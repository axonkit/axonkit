package server

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/axonkit/axonkit/pkg/proto"
)

// forward-participant supports the "co-host" flow for live streaming (see
// the design plan at .cursor/plans/co-host-forwarding-design_*.plan.md):
// a business-layer admin asks axonkit to mirror hostB from roomB into
// roomA so hostA and hostB appear to share a single RTC room while each
// hostX stays connected to their own room. No media re-encoding — the
// RoomStore KV is updated and subscribers in the destination room pick
// up the forwarded tracks via the existing FIB + media.Manager pipeline.
//
// Routing note: these handlers never require a node hint. The shared
// RoomStore KV is authoritative, so any node can start or tear down a
// forward; the owning node reacts via its own store watch.

// ForwardParticipantRequest is the body for POST /rtc/forward-participant.
type ForwardParticipantRequest struct {
	// SrcRoom is the room that currently hosts the source participant.
	SrcRoom string `json:"srcRoom"`
	// SrcIdentity is the identity of the participant to forward. Identity
	// is preferred over SID because it is stable across reconnects.
	SrcIdentity string `json:"srcIdentity"`
	// DstRoom is the room that should receive the forwarded participant.
	DstRoom string `json:"dstRoom"`
}

// ForwardParticipantResponse is the successful response body. Callers
// interact with the forwarded participant the same way they would with an
// ordinary one — in particular, RemoveParticipant(DstRoom, Participant.Identity)
// tears the forward down.
type ForwardParticipantResponse struct {
	Participant *proto.ParticipantInfo `json:"participant"`
}

// RemoveParticipantRequest is the body for POST /rtc/remove-participant.
// It removes a participant from a room by identity. It uniformly handles
// both real participants (equivalent to an admin kick) and forwarded
// participants previously created via /rtc/forward-participant.
type RemoveParticipantRequest struct {
	Room     string `json:"room"`
	Identity string `json:"identity"`
}

func (s *HTTPServer) handleForwardParticipant(w http.ResponseWriter, r *http.Request) {
	logId, _ := r.Context().Value(logIdKey{}).(string)

	var req ForwardParticipantRequest
	if err := readRequest(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: err.Error()})
		return
	}

	if _, err := s.authorizeAdmin(r, req.SrcRoom, "anon_"); err != nil {
		err.write(w)
		return
	}

	info, err := s.roomManager.ForwardParticipant(req.SrcRoom, req.SrcIdentity, req.DstRoom)
	if err != nil {
		status, code := forwardErrorToStatus(err)
		writeJSON(w, status, proto.ErrorResponse{Code: code, Error: err.Error()})
		return
	}

	w.Header().Set("X-Node-Id", string(s.node.ID))
	w.Header().Set("X-Log-Id", logId)
	writeJSON(w, http.StatusOK, ForwardParticipantResponse{Participant: info})

	slog.Info("Participant forwarded",
		"srcRoom", req.SrcRoom, "srcIdentity", req.SrcIdentity,
		"dstRoom", req.DstRoom, "logId", logId)
}

func (s *HTTPServer) handleRemoveParticipant(w http.ResponseWriter, r *http.Request) {
	logId, _ := r.Context().Value(logIdKey{}).(string)

	var req RemoveParticipantRequest
	if err := readRequest(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: err.Error()})
		return
	}

	if _, err := s.authorizeAdmin(r, req.Room, "anon_"); err != nil {
		err.write(w)
		return
	}

	if err := s.roomManager.RemoveParticipantFromRoom(req.Room, req.Identity); err != nil {
		status, code := forwardErrorToStatus(err)
		writeJSON(w, status, proto.ErrorResponse{Code: code, Error: err.Error()})
		return
	}
	w.Header().Set("X-Log-Id", logId)
	writeJSON(w, http.StatusOK, struct{}{})
	slog.Info("Participant remove requested",
		"room", req.Room, "identity", req.Identity, "logId", logId)
}

// forwardErrorToStatus maps a ForwardParticipant / RemoveParticipantFromRoom
// error to an HTTP status + error code.
func forwardErrorToStatus(err error) (int, proto.ErrorCode) {
	if err == nil {
		return http.StatusOK, ""
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found"):
		return http.StatusNotFound, proto.ErrCodeNotFound
	case strings.Contains(msg, "invalid"),
		strings.Contains(msg, "required"),
		strings.Contains(msg, "must differ"):
		return http.StatusBadRequest, proto.ErrCodeInvalidRequest
	default:
		return http.StatusInternalServerError, proto.ErrCodeInternal
	}
}
