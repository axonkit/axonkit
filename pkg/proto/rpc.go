package proto

import "encoding/json"

// RPC method constants for datachannel communication.
// Methods are organized by domain: track, subscription, transport, participant, etc.
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

	// Server -> Client: Subscription lifecycle (RPC, requires response)
	MethodSubscriptionCreate     = "subscription.create"
	MethodDataSubscriptionCreate = "dataSubscription.create"

	// Server -> Client: Notifications (one-way)
	MethodSubscriptionClosed     = "subscription.closed"
	MethodDataSubscriptionClosed = "dataSubscription.closed"
	MethodParticipantJoined      = "participant.joined"
	MethodParticipantLeft        = "participant.left"
	MethodParticipantUpdated     = "participant.updated"
	MethodTrackPublished         = "track.published"
	MethodTrackUnpublished       = "track.unpublished"
	MethodTrackMuted             = "track.muted"
	MethodActiveSpeakersChanged  = "activeSpeakers.changed"
	MethodRoomUpdated            = "room.updated"
	MethodDisconnect             = "disconnect"
)

// RPC status codes, loosely inspired by HTTP.
const (
	StatusOK            = 0
	StatusBadRequest    = 400
	StatusUnauthorized  = 401
	StatusForbidden     = 403
	StatusNotFound      = 404
	StatusConflict      = 409
	StatusInternalError = 500
)

// RPCHeader carries metadata for each datachannel RPC message.
// Design is inspired by the NATS protocol: compact, self-describing headers
// that support request/response correlation, content negotiation, and tracing.
type RPCHeader struct {
	Method     string `json:"method"`
	LogId      string `json:"logId"`
	Encoding   string `json:"encoding,omitempty"`
	Compressed bool   `json:"compressed,omitempty"`
	IsReply    bool   `json:"isReply,omitempty"`
	Status     int    `json:"status,omitempty"`
	Error      string `json:"error,omitempty"`
}

// RPCMessage is the envelope for all datachannel RPC communication.
type RPCMessage struct {
	Header RPCHeader       `json:"header"`
	Body   json.RawMessage `json:"body,omitempty"`
}

func NewRequest(method, logId string, body any) (*RPCMessage, error) {
	msg := &RPCMessage{
		Header: RPCHeader{
			Method: method,
			LogId:  logId,
		},
	}
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		msg.Body = b
	}
	return msg, nil
}

func NewResponse(method, logId string, status int, errMsg string, body any) (*RPCMessage, error) {
	msg := &RPCMessage{
		Header: RPCHeader{
			Method:  method,
			LogId:   logId,
			IsReply: true,
			Status:  status,
			Error:   errMsg,
		},
	}
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		msg.Body = b
	}
	return msg, nil
}

func NewOKResponse(method, logId string, body any) (*RPCMessage, error) {
	return NewResponse(method, logId, StatusOK, "", body)
}

func NewErrorResponse(method, logId string, status int, errMsg string) (*RPCMessage, error) {
	return NewResponse(method, logId, status, errMsg, nil)
}

func NewNotification(method string, body any) (*RPCMessage, error) {
	return NewRequest(method, "", body)
}
