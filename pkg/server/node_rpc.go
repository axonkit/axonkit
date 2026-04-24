package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/rtc"
	"github.com/axonkit/axonkit/pkg/rtc/whip"
	"github.com/axonkit/axonkit/pkg/types"
	"github.com/nats-io/nats.go"
)

// Inter-node request subject: rtc.rpc.<nodeID>
const nodeRPCSubjectPrefix = "rtc.rpc."

// Well-known NodeRPCRequest.Action values. Register handlers with the same names.
const (
	NodeRPCActionJoin               = "join"
	NodeRPCActionReconnect          = "reconnect"
	NodeRPCActionConnect            = "connect"
	NodeRPCActionRequestBackupCodec = "request_backup_codec"
	NodeRPCActionWHIPPatch          = "whip_patch"
	NodeRPCActionWHIPDelete         = "whip_delete"
	NodeRPCActionWHEPPatch          = "whep_patch"
	NodeRPCActionWHEPDelete         = "whep_delete"
)

// NodeRPCHandler handles one action for the local node. Returns HTTP-style status and JSON body.
type NodeRPCHandler func(payload json.RawMessage) (status int, body json.RawMessage)

// NodeRPCService routes rtc.rpc.<nodeId> request/reply: register handlers, dispatch incoming
// messages, and call Request() for remote or invokeLocal for this node.
type NodeRPCService struct {
	nc       *nats.Conn
	myNodeID types.NodeID
	timeout  time.Duration

	mu       sync.RWMutex
	handlers map[string]NodeRPCHandler
}

// NewNodeRPCService creates an empty registry. Call Register, then Subscribe(nc, subject, svc.Handle).
func NewNodeRPCService(nc *nats.Conn, myNodeID types.NodeID) *NodeRPCService {
	return &NodeRPCService{
		nc:       nc,
		myNodeID: myNodeID,
		timeout:  5 * time.Second,
		handlers: make(map[string]NodeRPCHandler),
	}
}

// Register adds or replaces the handler for an action name.
func (s *NodeRPCService) Register(action string, h NodeRPCHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[action] = h
}

// Handle is the NATS subscription callback (request/reply).
func (s *NodeRPCService) Handle(msg *nats.Msg) {
	var req proto.NodeRPCRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		s.respond(msg, http.StatusBadRequest, rpcErrPayload(proto.ErrCodeInvalidRequest, err.Error()))
		return
	}

	s.mu.RLock()
	h, ok := s.handlers[req.Action]
	s.mu.RUnlock()
	if !ok {
		s.respond(msg, http.StatusBadRequest, rpcErrPayload(proto.ErrCodeInvalidRequest, "unknown action: "+req.Action))
		return
	}

	status, body := h(req.Payload)
	if status == 0 {
		status = http.StatusOK
	}
	if len(body) == 0 {
		body = json.RawMessage("{}")
	}
	s.respond(msg, status, body)
}

func (s *NodeRPCService) respond(msg *nats.Msg, status int, body json.RawMessage) {
	data, err := json.Marshal(proto.NodeRPCResponse{Status: status, Payload: body})
	if err != nil {
		data, _ = json.Marshal(proto.NodeRPCResponse{
			Status:  http.StatusInternalServerError,
			Payload: rpcErrPayload(proto.ErrCodeInternal, err.Error()),
		})
	}
	_ = msg.Respond(data)
}

// Request sends NodeRPCRequest to the target node, or runs the handler locally if target is this node or empty.
func (s *NodeRPCService) Request(targetNodeID, action string, payload json.RawMessage) (proto.NodeRPCResponse, error) {
	if targetNodeID == "" || types.NodeID(targetNodeID) == s.myNodeID {
		return s.invokeLocal(action, payload)
	}
	if s.nc == nil {
		return proto.NodeRPCResponse{}, fmt.Errorf("nats unavailable")
	}
	rpcReq := proto.NodeRPCRequest{Action: action, Payload: payload}
	data, err := json.Marshal(rpcReq)
	if err != nil {
		return proto.NodeRPCResponse{}, err
	}
	msg, err := s.nc.Request(nodeRPCSubjectPrefix+targetNodeID, data, s.timeout)
	if err != nil {
		return proto.NodeRPCResponse{}, err
	}
	var rpcResp proto.NodeRPCResponse
	if err := json.Unmarshal(msg.Data, &rpcResp); err != nil {
		return proto.NodeRPCResponse{}, fmt.Errorf("invalid node RPC response: %w", err)
	}
	return rpcResp, nil
}

func (s *NodeRPCService) invokeLocal(action string, payload json.RawMessage) (proto.NodeRPCResponse, error) {
	s.mu.RLock()
	h, ok := s.handlers[action]
	s.mu.RUnlock()
	if !ok {
		return proto.NodeRPCResponse{
			Status:  http.StatusBadRequest,
			Payload: rpcErrPayload(proto.ErrCodeInvalidRequest, "unknown action: "+action),
		}, nil
	}
	status, body := h(payload)
	if status == 0 {
		status = http.StatusOK
	}
	if len(body) == 0 {
		body = json.RawMessage("{}")
	}
	return proto.NodeRPCResponse{Status: status, Payload: body}, nil
}

func rpcErrPayload(code proto.ErrorCode, msg string) json.RawMessage {
	b, _ := json.Marshal(proto.ErrorResponse{Code: code, Error: msg})
	return b
}

// rpcResponseRecorder implements http.ResponseWriter for reusing HTTPServer handlers that write JSON bodies.
type rpcResponseRecorder struct {
	headers http.Header
	status  int
	body    json.RawMessage
}

func (r *rpcResponseRecorder) Header() http.Header {
	if r.headers == nil {
		r.headers = make(http.Header)
	}
	return r.headers
}

func (r *rpcResponseRecorder) WriteHeader(status int) {
	r.status = status
}

func (r *rpcResponseRecorder) Write(data []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	r.body = data
	return len(data), nil
}

func (r *rpcResponseRecorder) statusAndBody() (int, json.RawMessage) {
	st := r.status
	if st == 0 {
		st = http.StatusOK
	}
	body := r.body
	if len(body) == 0 {
		body = json.RawMessage("{}")
	}
	return st, body
}

// registerDefaultNodeRPCHandlers wires all built-in inter-node actions. Call after NewHTTPServer.
func registerDefaultNodeRPCHandlers(svc *NodeRPCService, api *HTTPServer, rm *rtc.RoomManager) {
	svc.Register(NodeRPCActionJoin, func(p json.RawMessage) (int, json.RawMessage) {
		var joinReq proto.JoinRequest
		if err := json.Unmarshal(p, &joinReq); err != nil {
			return http.StatusBadRequest, rpcErrPayload(proto.ErrCodeInvalidRequest, err.Error())
		}
		rec := &rpcResponseRecorder{}
		api.handleJoinLocal(rec, &joinReq, "")
		return rec.statusAndBody()
	})

	svc.Register(NodeRPCActionReconnect, func(p json.RawMessage) (int, json.RawMessage) {
		var joinReq proto.JoinRequest
		if err := json.Unmarshal(p, &joinReq); err != nil {
			return http.StatusBadRequest, rpcErrPayload(proto.ErrCodeInvalidRequest, err.Error())
		}
		rec := &rpcResponseRecorder{}
		api.handleReconnectLocal(rec, &joinReq, "")
		return rec.statusAndBody()
	})

	svc.Register(NodeRPCActionConnect, func(p json.RawMessage) (int, json.RawMessage) {
		var connReq proto.ConnectRequest
		if err := json.Unmarshal(p, &connReq); err != nil {
			return http.StatusBadRequest, rpcErrPayload(proto.ErrCodeInvalidRequest, err.Error())
		}
		participant := rm.GetParticipantBySid(connReq.ParticipantSid)
		if participant == nil {
			return http.StatusNotFound, rpcErrPayload(proto.ErrCodeNotFound, "participant not found")
		}
		rec := &rpcResponseRecorder{}
		api.handleConnectLocal(rec, &connReq, participant, "")
		return rec.statusAndBody()
	})

	svc.Register(NodeRPCActionWHIPPatch, func(p json.RawMessage) (int, json.RawMessage) {
		var req whipRPCRequest
		if err := json.Unmarshal(p, &req); err != nil {
			return http.StatusBadRequest, rpcErrPayload(proto.ErrCodeInvalidRequest, err.Error())
		}
		etag, err := api.patchWHIPParticipantLocal(req.RoomName, req.ResourceID, req.IfMatch, req.Body)
		if err != nil {
			if errors.Is(err, whip.ErrSessionNotFound) {
				return http.StatusNotFound, rpcErrPayload(proto.ErrCodeNotFound, err.Error())
			}
			return http.StatusInternalServerError, rpcErrPayload(proto.ErrCodeInternal, err.Error())
		}
		body, _ := json.Marshal(struct {
			ETag string `json:"etag,omitempty"`
		}{ETag: etag})
		return http.StatusNoContent, body
	})

	svc.Register(NodeRPCActionWHIPDelete, func(p json.RawMessage) (int, json.RawMessage) {
		var req whipRPCRequest
		if err := json.Unmarshal(p, &req); err != nil {
			return http.StatusBadRequest, rpcErrPayload(proto.ErrCodeInvalidRequest, err.Error())
		}
		if err := api.deleteWHIPParticipantLocal(req.RoomName, req.ResourceID); err != nil {
			if errors.Is(err, whip.ErrSessionNotFound) {
				return http.StatusNotFound, rpcErrPayload(proto.ErrCodeNotFound, err.Error())
			}
			return http.StatusInternalServerError, rpcErrPayload(proto.ErrCodeInternal, err.Error())
		}
		return http.StatusOK, json.RawMessage("{}")
	})

	svc.Register(NodeRPCActionWHEPPatch, func(p json.RawMessage) (int, json.RawMessage) {
		var req whipRPCRequest
		if err := json.Unmarshal(p, &req); err != nil {
			return http.StatusBadRequest, rpcErrPayload(proto.ErrCodeInvalidRequest, err.Error())
		}
		etag, err := api.patchWHEPSessionLocal(req.RoomName, req.ResourceID, req.IfMatch, req.Body)
		if err != nil {
			if errors.Is(err, whip.ErrSessionNotFound) {
				return http.StatusNotFound, rpcErrPayload(proto.ErrCodeNotFound, err.Error())
			}
			return http.StatusInternalServerError, rpcErrPayload(proto.ErrCodeInternal, err.Error())
		}
		body, _ := json.Marshal(struct {
			ETag string `json:"etag,omitempty"`
		}{ETag: etag})
		return http.StatusNoContent, body
	})

	svc.Register(NodeRPCActionWHEPDelete, func(p json.RawMessage) (int, json.RawMessage) {
		var req whipRPCRequest
		if err := json.Unmarshal(p, &req); err != nil {
			return http.StatusBadRequest, rpcErrPayload(proto.ErrCodeInvalidRequest, err.Error())
		}
		if err := api.deleteWHEPSessionLocal(req.RoomName, req.ResourceID); err != nil {
			if errors.Is(err, whip.ErrSessionNotFound) {
				return http.StatusNotFound, rpcErrPayload(proto.ErrCodeNotFound, err.Error())
			}
			return http.StatusInternalServerError, rpcErrPayload(proto.ErrCodeInternal, err.Error())
		}
		return http.StatusOK, json.RawMessage("{}")
	})
	svc.Register(NodeRPCActionRequestBackupCodec, func(p json.RawMessage) (int, json.RawMessage) {
		var bcReq proto.RequestBackupCodecRPCRequest
		if err := json.Unmarshal(p, &bcReq); err != nil {
			return http.StatusBadRequest, rpcErrPayload(proto.ErrCodeInvalidRequest, err.Error())
		}
		if err := rm.RequestBackupCodecOnPublisher(bcReq.PublisherSid, bcReq.TrackSid); err != nil {
			st := http.StatusInternalServerError
			if strings.Contains(err.Error(), "not found") {
				st = http.StatusNotFound
			}
			b, _ := json.Marshal(proto.ErrorResponse{Code: proto.ErrCodeInternal, Error: err.Error()})
			return st, b
		}
		return http.StatusOK, json.RawMessage("{}")
	})
}

// ForwardRequestBackupCodecRPC is a thin wrapper for rtc: same-node or NATS via NodeRPCService.
func ForwardRequestBackupCodecRPC(svc *NodeRPCService, publisherSid, trackSid, publisherNodeID string) error {
	if svc == nil {
		return fmt.Errorf("node rpc not configured")
	}
	payload, err := json.Marshal(proto.RequestBackupCodecRPCRequest{
		PublisherSid: publisherSid,
		TrackSid:     trackSid,
	})
	if err != nil {
		return err
	}
	resp, err := svc.Request(publisherNodeID, NodeRPCActionRequestBackupCodec, payload)
	if err != nil {
		return fmt.Errorf("node rpc request: %w", err)
	}
	if resp.Status != http.StatusOK {
		var er proto.ErrorResponse
		if json.Unmarshal(resp.Payload, &er) == nil && er.Error != "" {
			return fmt.Errorf("%s", er.Error)
		}
		return fmt.Errorf("publisher node returned status %d", resp.Status)
	}
	return nil
}
