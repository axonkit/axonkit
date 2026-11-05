package server

import (
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/axonkit/axonkit/pkg/broadcast"
	"github.com/axonkit/axonkit/pkg/proto"
)

// registerBroadcastRoutes mounts the /rtc/broadcast surface:
//
//	POST   /rtc/broadcast           create a new broadcast
//	GET    /rtc/broadcast/{id}      snapshot status
//	DELETE /rtc/broadcast/{id}      stop the broadcast
//	POST   /rtc/broadcast/{id}/whip open mix ingest (SDP offer/answer)
//	PATCH  /rtc/broadcast/{id}/whip ICE restart / trickle on mix ingest
//	DELETE /rtc/broadcast/{id}/whip drop mix ingest, flip back to live
//
// Authentication reuses authorizeAdmin (RoomGrant.Admin).
func (s *HTTPServer) registerBroadcastRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /rtc/broadcast", s.handleBroadcastPost)
	mux.HandleFunc("GET /rtc/broadcast/{id}", s.handleBroadcastGet)
	mux.HandleFunc("DELETE /rtc/broadcast/{id}", s.handleBroadcastDelete)

	mux.HandleFunc("POST /rtc/broadcast/{id}/whip", s.handleBroadcastMixPost)
	mux.HandleFunc("PATCH /rtc/broadcast/{id}/whip", s.handleBroadcastMixPatch)
	mux.HandleFunc("DELETE /rtc/broadcast/{id}/whip", s.handleBroadcastMixDelete)

	mux.HandleFunc("OPTIONS /rtc/broadcast", writeCORSOK)
	mux.HandleFunc("OPTIONS /rtc/broadcast/{tail...}", writeCORSOK)
}

func (s *HTTPServer) handleBroadcastPost(w http.ResponseWriter, r *http.Request) {
	var req broadcast.CreateRequest
	if err := readRequest(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: err.Error(),
		})
		return
	}
	if _, errResp := s.authorizeAdmin(r, req.Room); errResp != nil {
		errResp.write(w)
		return
	}

	b, err := s.broadcastMgr.Create(&req)
	if err != nil {
		writeBroadcastError(w, err)
		return
	}

	w.Header().Set("X-Node-Id", string(s.node.ID))
	writeJSON(w, http.StatusCreated, b.Status())
	slog.Info("broadcast created",
		"id", b.ID(), "room", b.Room(), "liveIdentity", b.LiveIdentity(),
	)
}

func (s *HTTPServer) handleBroadcastGet(w http.ResponseWriter, r *http.Request) {
	if _, errResp := s.authorizeAdmin(r, ""); errResp != nil {
		errResp.write(w)
		return
	}
	b := s.broadcastMgr.Get(r.PathValue("id"))
	if b == nil {
		writeBroadcastError(w, broadcast.ErrNotFound)
		return
	}
	writeJSON(w, http.StatusOK, b.Status())
}

func (s *HTTPServer) handleBroadcastDelete(w http.ResponseWriter, r *http.Request) {
	if _, errResp := s.authorizeAdmin(r, ""); errResp != nil {
		errResp.write(w)
		return
	}
	if err := s.broadcastMgr.Delete(r.PathValue("id")); err != nil {
		writeBroadcastError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// Mix ingest handlers reuse whip.Service.CreateWHIP under the hood so
// avflow can POST an SDP offer as with a normal WHIP publisher. The
// difference is that broadcast does not register the resulting
// producers as a room participant — they feed the switcher directly.

func (s *HTTPServer) handleBroadcastMixPost(w http.ResponseWriter, r *http.Request) {
	if _, errResp := s.authorizeAdmin(r, ""); errResp != nil {
		errResp.write(w)
		return
	}
	b := s.broadcastMgr.Get(r.PathValue("id"))
	if b == nil {
		writeBroadcastError(w, broadcast.ErrNotFound)
		return
	}
	if !isSDPContent(r) {
		http.Error(w, "expected Content-Type: application/sdp", http.StatusUnsupportedMediaType)
		return
	}
	offerSDP, err := readSDPBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	answer, etag, err := b.AttachMix(offerSDP)
	if err != nil {
		writeBroadcastError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/sdp")
	w.Header().Set("Location", b.MixWhipPath())
	if etag != "" {
		w.Header().Set("ETag", `"`+etag+`"`)
	}
	w.Header().Set("X-Node-Id", string(s.node.ID))
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(answer))
	slog.Info("broadcast mix attached", "id", b.ID(), "etag", etag)
}

func (s *HTTPServer) handleBroadcastMixPatch(w http.ResponseWriter, r *http.Request) {
	if _, errResp := s.authorizeAdmin(r, ""); errResp != nil {
		errResp.write(w)
		return
	}
	b := s.broadcastMgr.Get(r.PathValue("id"))
	if b == nil {
		writeBroadcastError(w, broadcast.ErrNotFound)
		return
	}
	ifMatch := r.Header.Get("If-Match")
	_, _ = io.Copy(io.Discard, r.Body)
	_ = r.Body.Close()
	etag, err := b.PatchMixICE(ifMatch)
	if err != nil {
		writeBroadcastError(w, err)
		return
	}
	if etag != "" {
		w.Header().Set("ETag", `"`+etag+`"`)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *HTTPServer) handleBroadcastMixDelete(w http.ResponseWriter, r *http.Request) {
	if _, errResp := s.authorizeAdmin(r, ""); errResp != nil {
		errResp.write(w)
		return
	}
	b := s.broadcastMgr.Get(r.PathValue("id"))
	if b == nil {
		writeBroadcastError(w, broadcast.ErrNotFound)
		return
	}
	if err := b.DetachMix(); err != nil {
		writeBroadcastError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeBroadcastError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, broadcast.ErrNotFound):
		writeJSON(w, http.StatusNotFound, proto.ErrorResponse{
			Code: proto.ErrCodeNotFound, Error: err.Error(),
		})
	case errors.Is(err, broadcast.ErrRoomNotFound):
		writeJSON(w, http.StatusNotFound, proto.ErrorResponse{
			Code: proto.ErrCodeNotFound, Error: err.Error(),
		})
	case errors.Is(err, broadcast.ErrIdentityNotPublishing):
		writeJSON(w, http.StatusConflict, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: err.Error(),
		})
	case errors.Is(err, broadcast.ErrNoDestinations),
		errors.Is(err, broadcast.ErrTooManyDestinations):
		writeJSON(w, http.StatusBadRequest, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: err.Error(),
		})
	case errors.Is(err, broadcast.ErrTerminated),
		errors.Is(err, broadcast.ErrMixAlreadyConnected):
		writeJSON(w, http.StatusConflict, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: err.Error(),
		})
	default:
		writeJSON(w, http.StatusBadRequest, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: err.Error(),
		})
	}
}
