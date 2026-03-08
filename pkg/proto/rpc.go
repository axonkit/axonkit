package proto

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
)

// RPC method constants for datachannel communication.
const (
	// Client -> Server: Lifecycle
	MethodReady = "ready"
	MethodLeave = "leave"
	MethodPing  = "ping"
	MethodPong  = "pong"

	// Client -> Server: Token
	MethodTokenRefresh = "token.refresh"

	// Client -> Server: Track operations
	MethodTrackPublish   = "track.publish"
	MethodTrackUnpublish = "track.unpublish"
	MethodTrackMute      = "track.mute"
	MethodTrackUpdate    = "track.update"

	// Client -> Server: Data track operations
	MethodDataTrackPublish   = "dataTrack.publish"
	MethodDataTrackUnpublish = "dataTrack.unpublish"

	// Client -> Server: Subscription operations
	MethodSubscriptionPause              = "subscription.pause"
	MethodSubscriptionResume             = "subscription.resume"
	MethodSubscriptionSetPreferredLayers = "subscription.setPreferredLayers"
	MethodSubscriptionSetPriority        = "subscription.setPriority"
	MethodSubscriptionRequestKeyFrame    = "subscription.requestKeyFrame"
	MethodSubscriptionClose              = "subscription.close"

	// Client -> Server: Data subscription operations
	MethodDataSubscriptionClose = "dataSubscription.close"

	// Client -> Server: Transport
	MethodTransportGetStats = "transport.getStats"

	// Client -> Server: Participant
	MethodParticipantUpdate = "participant.update"

	// Client -> Server: Stats (notification, no response)
	MethodStatsReport = "stats.report"

	// Client -> Server: Manual subscription control
	MethodSubscriptionSubscribe   = "subscription.subscribe"
	MethodSubscriptionUnsubscribe = "subscription.unsubscribe"

	// Client -> Server: Chat, P2P RPC (notifications, server relays)
	MethodChatMessage = "chat.message"
	MethodRpcRequest  = "rpc.request"
	MethodRpcResponse = "rpc.response"

	// Server -> Client: P2P RPC relay
	MethodRpcInvoke = "rpc.invoke"

	// Server -> Client: Subscription lifecycle (RPC, requires response)
	MethodSubscriptionCreate     = "subscription.create"
	MethodDataSubscriptionCreate = "dataSubscription.create"

	// Server -> Client: Notifications (one-way)
	MethodSubscriptionClosed             = "subscription.closed"
	MethodDataSubscriptionClosed         = "dataSubscription.closed"
	MethodParticipantJoined              = "participant.joined"
	MethodParticipantLeft                = "participant.left"
	MethodParticipantUpdated             = "participant.updated"
	MethodTrackPublished                 = "track.published"
	MethodTrackUnpublished               = "track.unpublished"
	MethodTrackMuted                     = "track.muted"
	MethodTrackSubscribedQualityChanged  = "track.subscribedQualityChanged"
	MethodTrackSubscriptionStatusChanged = "track.subscriptionStatusChanged"
	MethodActiveSpeakersChanged          = "activeSpeakers.changed"
	MethodRoomUpdated                    = "room.updated"
	MethodDisconnect                     = "disconnect"
)

// Status codes, HTTP-compatible.
const (
	StatusOK            = 200
	StatusBadRequest    = 400
	StatusUnauthorized  = 401
	StatusForbidden     = 403
	StatusNotFound      = 404
	StatusConflict      = 409
	StatusInternalError = 500
)

// Well-known header keys.
const (
	HdrError    = "Error"
	HdrEncoding = "Encoding"
)

const (
	protoLine = "AXON/1.0\r\n"
	crlf      = "\r\n"
)

var (
	protoLineBytes = []byte(protoLine)
	crlfBytes      = []byte(crlf)
	crlf2Bytes     = []byte(crlf + crlf)

	ErrInvalidMessage = errors.New("invalid message")
	ErrBadHeader      = errors.New("invalid message header")
)

// Header is an HTTP-style header map for RPC metadata.
type Header map[string][]string

func (h Header) Get(key string) string {
	if v := h[key]; len(v) > 0 {
		return v[0]
	}
	return ""
}

func (h Header) Set(key, value string) { h[key] = []string{value} }
func (h Header) Add(key, value string) { h[key] = append(h[key], value) }
func (h Header) Del(key string)        { delete(h, key) }

// MessageType identifies the kind of RPC message on the wire.
type MessageType byte

const (
	TypeRequest      MessageType = 'Q' // REQ
	TypeResponse     MessageType = 'S' // RES
	TypeNotification MessageType = 'M' // MSG
)

// Message is the wire format for datachannel RPC.
//
// Wire format (text protocol inspired by NATS):
//
//	REQ <method> <logId>\r\n        -- request (logId required)
//	RES <logId> <status>\r\n        -- response (logId required)
//	MSG <method> [<logId>]\r\n      -- notification (logId optional, for tracing)
//	[AXON/1.0\r\n                   -- optional headers
//	Key: Value\r\n
//	\r\n]
//	<body bytes>                    -- raw bytes (JSON, protobuf, etc.)
type Message struct {
	Type   MessageType // REQ / RES / MSG — set from wire, not inferred
	Method string      // set for REQ and MSG
	LogId  string      // correlation / trace ID
	Status int         // response status code, set for RES
	Header Header      // optional metadata
	Data   []byte      // raw body bytes
}

func (m *Message) IsRequest() bool      { return m.Type == TypeRequest }
func (m *Message) IsResponse() bool     { return m.Type == TypeResponse }
func (m *Message) IsNotification() bool { return m.Type == TypeNotification }

func (m *Message) Error() string {
	if m.Header != nil {
		return m.Header.Get(HdrError)
	}
	return ""
}

// Encode serializes the message to wire format.
func (m *Message) Encode() ([]byte, error) {
	var buf bytes.Buffer

	switch m.Type {
	case TypeRequest:
		buf.WriteString("REQ ")
		buf.WriteString(m.Method)
		buf.WriteString(" ")
		buf.WriteString(m.LogId)
	case TypeResponse:
		buf.WriteString("RES ")
		buf.WriteString(m.LogId)
		buf.WriteString(" ")
		buf.WriteString(strconv.Itoa(m.Status))
	default:
		buf.WriteString("MSG ")
		buf.WriteString(m.Method)
		if m.LogId != "" {
			buf.WriteString(" ")
			buf.WriteString(m.LogId)
		}
	}
	buf.WriteString(crlf)

	if len(m.Header) > 0 {
		buf.WriteString(protoLine)
		_ = http.Header(m.Header).Write(&buf)
		buf.Write(crlfBytes)
	}

	buf.Write(m.Data)
	return buf.Bytes(), nil
}

// DecodeMessage parses a wire-format message from raw bytes.
func DecodeMessage(data []byte) (*Message, error) {
	idx := bytes.Index(data, crlfBytes)
	if idx < 0 {
		return nil, ErrInvalidMessage
	}

	line := string(data[:idx])
	parts := strings.SplitN(line, " ", 4)
	msg := &Message{Header: make(Header)}

	switch parts[0] {
	case "REQ":
		if len(parts) != 3 {
			return nil, ErrInvalidMessage
		}
		msg.Type = TypeRequest
		msg.Method = parts[1]
		msg.LogId = parts[2]
	case "RES":
		if len(parts) != 3 {
			return nil, ErrInvalidMessage
		}
		msg.Type = TypeResponse
		msg.LogId = parts[1]
		status, err := strconv.Atoi(parts[2])
		if err != nil {
			return nil, ErrInvalidMessage
		}
		msg.Status = status
	case "MSG":
		if len(parts) < 2 || len(parts) > 3 {
			return nil, ErrInvalidMessage
		}
		msg.Type = TypeNotification
		msg.Method = parts[1]
		if len(parts) == 3 {
			msg.LogId = parts[2]
		}
	default:
		return nil, ErrInvalidMessage
	}

	rest := data[idx+len(crlfBytes):]

	if bytes.HasPrefix(rest, protoLineBytes) {
		hdrEnd := bytes.Index(rest, crlf2Bytes)
		if hdrEnd < 0 {
			return nil, ErrInvalidMessage
		}
		hdr, err := decodeHeaders(rest[:hdrEnd+len(crlf2Bytes)])
		if err != nil {
			return nil, err
		}
		msg.Header = hdr
		msg.Data = rest[hdrEnd+len(crlf2Bytes):]
	} else {
		msg.Data = rest
	}

	return msg, nil
}

func decodeHeaders(data []byte) (Header, error) {
	br := bufio.NewReaderSize(bytes.NewReader(data), 128)
	tp := textproto.NewReader(br)
	l, err := tp.ReadLine()
	if err != nil || !strings.HasPrefix(l, "AXON/") {
		return nil, ErrBadHeader
	}
	mh, err := tp.ReadMIMEHeader()
	if err != nil {
		return nil, ErrBadHeader
	}
	return Header(mh), nil
}

// ============================================================
// Constructors (raw bytes body)
// ============================================================

func NewRequest(method, logId string, data []byte) *Message {
	return &Message{Type: TypeRequest, Method: method, LogId: logId, Header: make(Header), Data: data}
}

func NewResponse(logId string, status int, data []byte) *Message {
	return &Message{Type: TypeResponse, LogId: logId, Status: status, Header: make(Header), Data: data}
}

func NewErrorResponse(logId string, status int, errMsg string) *Message {
	h := make(Header)
	h.Set(HdrError, errMsg)
	return &Message{Type: TypeResponse, LogId: logId, Status: status, Header: h}
}

func NewNotification(method string, data []byte) *Message {
	return &Message{Type: TypeNotification, Method: method, Header: make(Header), Data: data}
}

// ============================================================
// JSON convenience constructors
// ============================================================

func NewJSONRequest(method, logId string, body any) (*Message, error) {
	data, err := marshalBody(body)
	if err != nil {
		return nil, err
	}
	return NewRequest(method, logId, data), nil
}

func NewJSONResponse(logId string, status int, body any) (*Message, error) {
	data, err := marshalBody(body)
	if err != nil {
		return nil, err
	}
	return NewResponse(logId, status, data), nil
}

func NewJSONNotification(method string, body any) (*Message, error) {
	data, err := marshalBody(body)
	if err != nil {
		return nil, err
	}
	return NewNotification(method, data), nil
}

func marshalBody(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}
