package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/axonkit/axonkit/pkg/auth"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/rtc"
	"github.com/axonkit/axonkit/pkg/rtc/whip"
	"github.com/jiyeyuran/mediasoup-go/v2"
)

// whipRPCRequest is the on-wire payload for WHIP/WHEP inter-node forwards.
// Carried by NodeRPCActionWHIP{Patch,Delete} and NodeRPCActionWHEP{Patch,Delete}.
type whipRPCRequest struct {
	RoomName   string `json:"roomName"`
	ResourceID string `json:"resourceId"`
	IfMatch    string `json:"ifMatch,omitempty"`
	Body       string `json:"body,omitempty"`
}

// ============================================================
// WHIP HTTP Routes
// ============================================================

type whipTransportFactory struct {
	routers       []*mediasoup.Router
	webRtcServers []*mediasoup.WebRtcServer
}

func (f *whipTransportFactory) New(_ whip.Mode) (*mediasoup.Transport, *mediasoup.Router, error) {
	if len(f.routers) == 0 {
		return nil, nil, fmt.Errorf("no media routers available")
	}
	idx := int(time.Now().UnixNano() % int64(len(f.routers)))
	if idx < 0 {
		idx = -idx
	}
	router := f.routers[idx]
	webRtcServer := f.webRtcServers[idx]

	enableUdp := true
	transport, err := router.CreateWebRtcTransport(&mediasoup.WebRtcTransportOptions{
		WebRtcServer: webRtcServer,
		EnableUdp:    &enableUdp,
		EnableTcp:    true,
		PreferUdp:    true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("create transport: %w", err)
	}
	return transport, router, nil
}

func (s *HTTPServer) registerWHIPRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /whip/{room}", s.handleWHIPPost)
	mux.HandleFunc("PATCH /whip/{room}/{resourceId}", s.handleWHIPPatch)
	mux.HandleFunc("DELETE /whip/{room}/{resourceId}", s.handleWHIPDelete)
	mux.HandleFunc("OPTIONS /whip/{tail...}", writeCORSOK)

	// WHEP is intentionally not registered yet: mediasoup has no consumer-side
	// RtpParameters override (versatica/mediasoup#1783), so CreateWHEP cannot
	// reproduce the offer's payload types / fmtp in the answer and standard
	// clients reject the stream. See the TODO in pkg/rtc/whip/service.go and
	// the file header of whep.go. Uncomment once mediasoup-go ships that field.
	// s.registerWHEPRoutes(mux)
}

func writeCORSOK(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }

func (s *HTTPServer) handleWHIPPost(w http.ResponseWriter, r *http.Request) {
	roomName := r.PathValue("room")
	if !isSDPContent(r) {
		http.Error(w, "expected Content-Type: application/sdp", http.StatusUnsupportedMediaType)
		return
	}
	offerSDP, err := readSDPBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	claims, errResp := s.authorizeAdmin(r, roomName)
	if errResp != nil {
		errResp.write(w)
		return
	}

	participant, answer, etag, err := s.createWHIPParticipant(roomName, claims, offerSDP)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, proto.ErrorResponse{Code: proto.ErrCodeInternal, Error: err.Error()})
		return
	}

	s.writeWHIPResponse(w, "whip", roomName, participant.Sid(), answer, etag)
}

func (s *HTTPServer) handleWHIPPatch(w http.ResponseWriter, r *http.Request) {
	roomName, id, ifMatch, body, ok := s.readPatchRequest(w, r)
	if !ok {
		return
	}
	etag, err := s.patchWHIPParticipant(roomName, id, ifMatch, body)
	writePatchResponse(w, etag, err)
}

func (s *HTTPServer) handleWHIPDelete(w http.ResponseWriter, r *http.Request) {
	roomName, id, ok := s.readDeleteRequest(w, r)
	if !ok {
		return
	}
	writeDeleteResponse(w, s.deleteWHIPParticipant(roomName, id))
}

// readPatchRequest parses the shared PATCH envelope (path + auth + body).
// Returns ok=false after writing the error response.
func (s *HTTPServer) readPatchRequest(w http.ResponseWriter, r *http.Request) (roomName, id, ifMatch, body string, ok bool) {
	roomName = r.PathValue("room")
	if _, errResp := s.authorizeAdmin(r, roomName); errResp != nil {
		errResp.write(w)
		return "", "", "", "", false
	}
	id = r.PathValue("resourceId")
	ifMatch = r.Header.Get("If-Match")
	raw, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()
	return roomName, id, ifMatch, string(raw), true
}

// readDeleteRequest is the DELETE counterpart of readPatchRequest.
func (s *HTTPServer) readDeleteRequest(w http.ResponseWriter, r *http.Request) (roomName, id string, ok bool) {
	roomName = r.PathValue("room")
	if _, errResp := s.authorizeAdmin(r, roomName); errResp != nil {
		errResp.write(w)
		return "", "", false
	}
	id = r.PathValue("resourceId")
	return roomName, id, true
}

func writePatchResponse(w http.ResponseWriter, etag string, err error) {
	if err != nil {
		if errors.Is(err, whip.ErrSessionNotFound) {
			http.Error(w, "resource not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if etag != "" {
		w.Header().Set("ETag", `"`+etag+`"`)
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeDeleteResponse(w http.ResponseWriter, err error) {
	if err != nil {
		if errors.Is(err, whip.ErrSessionNotFound) {
			http.Error(w, "resource not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func isSDPContent(r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		return false
	}
	head := strings.SplitN(ct, ";", 2)[0]
	return strings.EqualFold(strings.TrimSpace(head), "application/sdp")
}

func readSDPBody(r *http.Request) (string, error) {
	body, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		return "", fmt.Errorf("read body: %w", err)
	}
	if len(body) == 0 {
		return "", fmt.Errorf("empty body")
	}
	return string(body), nil
}

type whipErr struct {
	status int
	body   string
}

func (e *whipErr) write(w http.ResponseWriter) {
	http.Error(w, e.body, e.status)
}

func extractBearerToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		const prefix = "Bearer "
		if strings.HasPrefix(h, prefix) {
			return strings.TrimSpace(h[len(prefix):])
		}
	}
	if v := r.URL.Query().Get("token"); v != "" {
		return v
	}
	return ""
}

func (s *HTTPServer) createWHIPParticipant(roomName string, claims *auth.Claims, offerSDP string) (*rtc.Participant, string, string, error) {
	result, err := s.whipSvc.CreateWHIP(whip.CreateWHIPOptions{OfferSDP: offerSDP})
	if err != nil {
		return nil, "", "", err
	}
	participant, err := s.attachPublishSession(result, claims, roomName)
	if err != nil {
		// Tear down the transport the Service handed us; closing the
		// transport cascades into producer close. No Session/OnClose to
		// untangle — the Participant is the sole owner once attached.
		result.Transport.Close()
		return nil, "", "", fmt.Errorf("attach to room: %w", err)
	}
	return participant, result.Answer, result.ETag, nil
}

// ============================================================
// WHIP routing: distributed lookup via the room participant store
// ============================================================

// patchWHIPParticipant is the distributed-aware entrypoint. The resource
// id is the participant sid; we resolve its owning node via the room store
// and forward if it lives elsewhere.
func (s *HTTPServer) patchWHIPParticipant(roomName, participantSid, ifMatch, body string) (string, error) {
	nodeID, err := s.resolveWHIPNode(roomName, participantSid)
	if err != nil {
		return "", err
	}
	if nodeID != "" && nodeID != string(s.node.ID) {
		return s.forwardRPCPatch(nodeID, NodeRPCActionWHIPPatch, roomName, participantSid, ifMatch, body)
	}
	return s.patchWHIPParticipantLocal(roomName, participantSid, ifMatch, body)
}

func (s *HTTPServer) deleteWHIPParticipant(roomName, participantSid string) error {
	nodeID, err := s.resolveWHIPNode(roomName, participantSid)
	if err != nil {
		return err
	}
	if nodeID != "" && nodeID != string(s.node.ID) {
		return s.forwardRPCDelete(nodeID, NodeRPCActionWHIPDelete, roomName, participantSid)
	}
	return s.deleteWHIPParticipantLocal(roomName, participantSid)
}

// resolveWHIPNode finds the NodeId that owns a WHIP resource. Looks up
// first the local runtime then the room's store-synced participant cache.
// Returns (nodeID, nil) on success, ("", ErrSessionNotFound) when absent.
func (s *HTTPServer) resolveWHIPNode(roomName, participantSid string) (string, error) {
	if s.roomManager == nil {
		return "", fmt.Errorf("room manager not configured")
	}
	room := s.roomManager.GetRoom(roomName)
	if room == nil {
		return "", whip.ErrSessionNotFound
	}
	info := room.LookupParticipantInfo(participantSid)
	if info == nil || info.State == proto.ParticipantInfo_DISCONNECTED {
		return "", whip.ErrSessionNotFound
	}
	return info.NodeId, nil
}

// patchWHIPParticipantLocal is invoked when we (or an inbound RPC) know
// this node owns the participant.
func (s *HTTPServer) patchWHIPParticipantLocal(roomName, participantSid, ifMatch, body string) (string, error) {
	participant := s.findLocalParticipantBySid(roomName, participantSid)
	if participant == nil {
		return "", whip.ErrSessionNotFound
	}
	transport := participant.SendTransport()
	if transport == nil {
		return "", fmt.Errorf("participant %q has no send transport", participantSid)
	}
	if ifMatch == "*" {
		iceParams, err := transport.RestartIce()
		if err != nil {
			return "", fmt.Errorf("ice restart: %w", err)
		}
		return iceParams.UsernameFragment, nil
	}
	_ = body
	return "", nil
}

func (s *HTTPServer) deleteWHIPParticipantLocal(roomName, participantSid string) error {
	participant := s.findLocalParticipantBySid(roomName, participantSid)
	if participant == nil {
		return whip.ErrSessionNotFound
	}
	participant.Close(proto.DisconnectReason_CLIENT_INITIATED)
	return nil
}

// ============================================================
// Shared NodeRPC forwarders
// ============================================================

func (s *HTTPServer) forwardRPCPatch(nodeID, action, roomName, resourceID, ifMatch, body string) (string, error) {
	if s.nodeRPC == nil {
		return "", fmt.Errorf("node rpc not configured")
	}
	payload, _ := json.Marshal(whipRPCRequest{RoomName: roomName, ResourceID: resourceID, IfMatch: ifMatch, Body: body})
	resp, err := s.nodeRPC.Request(nodeID, action, payload)
	if err != nil {
		return "", fmt.Errorf("node rpc request: %w", err)
	}
	if resp.Status != http.StatusNoContent && resp.Status != http.StatusOK {
		return "", decodeRPCError(resp)
	}
	var out struct {
		ETag string `json:"etag,omitempty"`
	}
	_ = json.Unmarshal(resp.Payload, &out)
	return out.ETag, nil
}

func (s *HTTPServer) forwardRPCDelete(nodeID, action, roomName, resourceID string) error {
	if s.nodeRPC == nil {
		return fmt.Errorf("node rpc not configured")
	}
	payload, _ := json.Marshal(whipRPCRequest{RoomName: roomName, ResourceID: resourceID})
	resp, err := s.nodeRPC.Request(nodeID, action, payload)
	if err != nil {
		return fmt.Errorf("node rpc request: %w", err)
	}
	if resp.Status != http.StatusOK {
		return decodeRPCError(resp)
	}
	return nil
}

func decodeRPCError(resp proto.NodeRPCResponse) error {
	var er proto.ErrorResponse
	if json.Unmarshal(resp.Payload, &er) == nil && er.Error != "" {
		if resp.Status == http.StatusNotFound {
			return whip.ErrSessionNotFound
		}
		return errors.New(er.Error)
	}
	if resp.Status == http.StatusNotFound {
		return whip.ErrSessionNotFound
	}
	return fmt.Errorf("remote node returned status %d", resp.Status)
}

func (s *HTTPServer) findLocalParticipantBySid(roomName, participantSid string) *rtc.Participant {
	if s.roomManager == nil {
		return nil
	}
	room := s.roomManager.GetRoom(roomName)
	if room == nil {
		return nil
	}
	return room.GetParticipant(participantSid)
}

func (s *HTTPServer) attachPublishSession(result *whip.CreateWHIPResult, claims *auth.Claims, roomName string) (*rtc.Participant, error) {
	// WHIP itself does not carry a track-source hint, but downstream SDKs
	// commonly pivot on `source` (camera/mic) to pick which <video>/<audio>
	// element to attach to. Default to the obvious value per kind so demos
	// and dashboards render WHIP ingest out of the box.
	attached := make([]AttachedProducer, 0, len(result.Producers))
	for _, producer := range result.Producers {
		source := proto.TrackSource_UNKNOWN
		switch producer.Kind() {
		case mediasoup.MediaKindAudio:
			source = proto.TrackSource_MICROPHONE
		case mediasoup.MediaKindVideo:
			source = proto.TrackSource_CAMERA
		}
		attached = append(attached, AttachedProducer{Producer: producer, Source: source})
	}

	// Do NOT flip state to JOINED here. WHIP has no Ready request, but
	// media is not actually flowing until the client receives this answer
	// and ICE completes. The participant's ICE watchdog will promote the
	// state to JOINED on the first IceStateConnected/Completed event (see
	// onIceSourceUp), and the setup watchdog will close out the
	// participant if ICE never reaches that state within setupTimeout.
	//
	// Participant is also the sole owner of transport and producers after
	// this point. ICE disconnect is handled by the participant's own ICE
	// watchdog, which closes the participant (and thus the transport) on
	// timeout — no Session / OnClose plumbing needed.
	return s.AttachPublishParticipant(AttachPublishParticipantOptions{
		SidPrefix:     "whip_",
		RoomName:      roomName,
		Claims:        claims,
		Router:        result.Router,
		Transport:     result.Transport,
		TransportKind: TransportKindSend,
		Producers:     attached,
	})
}

func (s *HTTPServer) writeWHIPResponse(w http.ResponseWriter, prefix, roomName, resourceID, answer, etag string) {
	location := "/" + prefix + "/" + roomName + "/" + resourceID
	w.Header().Set("Content-Type", "application/sdp")
	w.Header().Set("Location", location)
	if etag != "" {
		w.Header().Set("ETag", `"`+etag+`"`)
	}
	w.Header().Set("X-Node-Id", string(s.node.ID))
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(answer))

	slog.Info("WHIP/WHEP session created", "mode", prefix, "resourceId", resourceID, "location", location)
}
