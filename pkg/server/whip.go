package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/axonkit/axonkit/pkg/auth"
	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/rtc"
	"github.com/axonkit/axonkit/pkg/rtc/whip"
	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/rs/xid"
)

// whipRPCRequest is the on-wire payload for WHIP/WHEP inter-node forwards.
// Carried by NodeRPCActionWHIP{Patch,Delete} and NodeRPCActionWHEP{Patch,Delete}.
type whipRPCRequest struct {
	RoomName   string `json:"roomName"`
	ResourceID string `json:"resourceId"`
	IfMatch    string `json:"ifMatch,omitempty"`
	Body       string `json:"body,omitempty"`
}

// whepLocBucket is the NATS KV bucket name for the "kv" WHEP routing scheme.
// It stores resourceID → nodeID with a short TTL refreshed by each session's
// keepalive.
const whepLocBucket = "WHEP_LOC"

// whepLocTTL is the TTL for WHEP_LOC entries. Must be comfortably larger
// than whip.DefaultKeepaliveInterval (10s) so a single missed refresh does
// not evict a live session from discovery.
const whepLocTTL = 30 * time.Second

// ============================================================
// WHIP / WHEP HTTP Routes
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

type whipProducerSource struct {
	server *HTTPServer
}

func (s *whipProducerSource) ProducersForParticipant(roomName, participantIdentity string) []*mediasoup.Producer {
	if s.server.roomManager == nil {
		return nil
	}
	room := s.server.roomManager.GetRoom(roomName)
	if room == nil {
		return nil
	}
	p := room.GetParticipantByIdentity(participantIdentity)
	if p == nil {
		return nil
	}
	return selectWHEPTracks(p.LiveTracks())
}

func selectWHEPTracks(tracks []rtc.LiveTrack) []*mediasoup.Producer {
	videoRank := map[proto.TrackSource]int{
		proto.TrackSource_SCREEN_SHARE: 3,
		proto.TrackSource_CAMERA:       2,
		proto.TrackSource_UNKNOWN:      1,
	}
	audioRank := map[proto.TrackSource]int{
		proto.TrackSource_SCREEN_SHARE_AUDIO: 3,
		proto.TrackSource_MICROPHONE:         2,
		proto.TrackSource_UNKNOWN:            1,
	}

	var bestVideo, bestAudio *mediasoup.Producer
	var videoScore, audioScore int
	for _, t := range tracks {
		if t.Producer == nil {
			continue
		}
		switch t.Kind {
		case mediasoup.MediaKindVideo:
			score, ok := videoRank[t.Source]
			if !ok {
				score = 1
			}
			if bestVideo == nil || score > videoScore {
				bestVideo = t.Producer
				videoScore = score
			}
		case mediasoup.MediaKindAudio:
			score, ok := audioRank[t.Source]
			if !ok {
				score = 1
			}
			if bestAudio == nil || score > audioScore {
				bestAudio = t.Producer
				audioScore = score
			}
		}
	}

	out := make([]*mediasoup.Producer, 0, 2)
	if bestAudio != nil {
		out = append(out, bestAudio)
	}
	if bestVideo != nil {
		out = append(out, bestVideo)
	}
	return out
}

// whepPipeResolver returns a whip.CreateWHEPOptions.ResolveProducerID hook
// that routes each producer through the publisher's RouterPipe cache when
// the destination router differs. When publisher is nil (e.g. the track
// is only visible via the cross-node store) the resolver is a no-op; the
// WHEP service then falls back to producer.Id() verbatim, which matches
// pre-pipe behavior for single-worker deployments.
func whepPipeResolver(publisher *rtc.Participant) func(*mediasoup.Router, *mediasoup.Producer) (string, error) {
	if publisher == nil {
		return nil
	}
	return func(dest *mediasoup.Router, p *mediasoup.Producer) (string, error) {
		return publisher.PipeProducerTo(p.Id(), dest)
	}
}

func (s *HTTPServer) registerWHIPRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /whip/{room}", s.handleWHIPPost)
	mux.HandleFunc("PATCH /whip/{room}/{resourceId}", s.handleWHIPPatch)
	mux.HandleFunc("DELETE /whip/{room}/{resourceId}", s.handleWHIPDelete)
	mux.HandleFunc("OPTIONS /whip/{tail...}", writeCORSOK)

	mux.HandleFunc("POST /whep/{room}/{participantId}", s.handleWHEPPost)
	mux.HandleFunc("PATCH /whep/{room}/{resourceId}", s.handleWHEPPatch)
	mux.HandleFunc("DELETE /whep/{room}/{resourceId}", s.handleWHEPDelete)
	mux.HandleFunc("OPTIONS /whep/{tail...}", writeCORSOK)
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

func (s *HTTPServer) handleWHEPPost(w http.ResponseWriter, r *http.Request) {
	roomName := r.PathValue("room")
	participantID := r.PathValue("participantId")
	if !isSDPContent(r) {
		http.Error(w, "expected Content-Type: application/sdp", http.StatusUnsupportedMediaType)
		return
	}
	offerSDP, err := readSDPBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, errResp := s.authorizeAdmin(r, roomName); errResp != nil {
		errResp.write(w)
		return
	}

	publisher, _, err := s.findParticipantByIdentity(roomName, participantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	producers := s.whipSvc.Sources.ProducersForParticipant(roomName, participantID)
	if len(producers) == 0 {
		http.Error(w, fmt.Sprintf("no active producers for participant %q in room %q", participantID, roomName), http.StatusNotFound)
		return
	}

	// Pre-compute the resource id according to the configured WHEP routing
	// scheme. For "prefix", we encode our NodeID up front so any other
	// node that receives a later PATCH/DELETE can route without consulting
	// the KV. For "kv", the id is a plain xid and discovery is handled by
	// the whepLocPut Keepalive hook below.
	nodeID := string(s.node.ID)
	resourceID := s.newWHEPResourceID(nodeID)

	// keepalive refreshes the WHEP_LOC KV entry under the "kv" scheme; for
	// "prefix" it is a no-op so the supervisor can avoid the extra goroutine
	// overhead.
	keepalive := s.whepKeepaliveFunc(resourceID, nodeID)

	// Producers are Router-scoped: each mediasoup worker runs its own
	// Router, and Transport.Consume(producerId) only resolves ids from the
	// same Router. When the WHEP factory happens to pick a different
	// worker than the one hosting the publisher, bridge the two via
	// PipeToRouter so the consumer transport sees a local copy of the
	// producer. The same cache that powers SFU cross-router subscribe
	// (rtc.Participant.PipeProducerTo) is reused here so a single pipe
	// is shared across both signalling protocols.
	result, err := s.whipSvc.CreateWHEP(whip.CreateWHEPOptions{
		OfferSDP:          offerSDP,
		Producers:         producers,
		ResourceID:        resourceID,
		Keepalive:         keepalive,
		ResolveProducerID: whepPipeResolver(publisher),
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, proto.ErrorResponse{Code: proto.ErrCodeInternal, Error: err.Error()})
		return
	}
	result.Session.RoomName = roomName
	// On session close we must drop the discovery entry so a re-created
	// session at a new node doesn't hit a stale record first. For "prefix"
	// scheme this is a no-op.
	result.Session.OnClose = s.whepCleanupFunc(result.Session.ID)

	s.writeWHIPResponse(w, "whep", roomName, result.Session.ID, result.Answer, result.ETag)
}

func (s *HTTPServer) handleWHIPPatch(w http.ResponseWriter, r *http.Request) {
	roomName, id, ifMatch, body, ok := s.readPatchRequest(w, r)
	if !ok {
		return
	}
	etag, err := s.patchWHIPParticipant(roomName, id, ifMatch, body)
	writePatchResponse(w, etag, err)
}

func (s *HTTPServer) handleWHEPPatch(w http.ResponseWriter, r *http.Request) {
	roomName, id, ifMatch, body, ok := s.readPatchRequest(w, r)
	if !ok {
		return
	}
	etag, err := s.patchWHEPSession(roomName, id, ifMatch, body)
	writePatchResponse(w, etag, err)
}

func (s *HTTPServer) handleWHIPDelete(w http.ResponseWriter, r *http.Request) {
	roomName, id, ok := s.readDeleteRequest(w, r)
	if !ok {
		return
	}
	writeDeleteResponse(w, s.deleteWHIPParticipant(roomName, id))
}

func (s *HTTPServer) handleWHEPDelete(w http.ResponseWriter, r *http.Request) {
	roomName, id, ok := s.readDeleteRequest(w, r)
	if !ok {
		return
	}
	writeDeleteResponse(w, s.deleteWHEPSession(roomName, id))
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

func anonymousClaims(roomName string, publish bool) *auth.Claims {
	identity := "anon_" + fmt.Sprintf("%d", time.Now().UnixNano())
	boolTrue := true
	grants := &proto.ClaimGrants{
		Identity: identity,
		Media: &proto.MediaGrant{
			CanPublish:   &boolTrue,
			CanSubscribe: &boolTrue,
		},
		Room: &proto.RoomGrant{Name: roomName, Join: true, Admin: true},
	}
	_ = publish
	return &auth.Claims{Sub: identity, Name: identity, Grants: grants}
}

func extractBearerToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		const prefix = "Bearer "
		if strings.HasPrefix(h, prefix) {
			return strings.TrimSpace(h[len(prefix):])
		}
	}
	if v := r.URL.Query().Get("access_token"); v != "" {
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
// WHEP routing: owner-node lookup via resourceID prefix or KV bucket
// ============================================================

func (s *HTTPServer) patchWHEPSession(roomName, resourceID, ifMatch, body string) (string, error) {
	nodeID := s.resolveWHEPNode(resourceID)
	if nodeID != "" && nodeID != string(s.node.ID) {
		return s.forwardRPCPatch(nodeID, NodeRPCActionWHEPPatch, roomName, resourceID, ifMatch, body)
	}
	return s.patchWHEPSessionLocal(roomName, resourceID, ifMatch, body)
}

func (s *HTTPServer) deleteWHEPSession(roomName, resourceID string) error {
	nodeID := s.resolveWHEPNode(resourceID)
	if nodeID != "" && nodeID != string(s.node.ID) {
		return s.forwardRPCDelete(nodeID, NodeRPCActionWHEPDelete, roomName, resourceID)
	}
	return s.deleteWHEPSessionLocal(roomName, resourceID)
}

// patchWHEPSessionLocal / deleteWHEPSessionLocal are invoked when the
// routing layer already decided the session lives on this node, either
// natively or via inbound node RPC.
func (s *HTTPServer) patchWHEPSessionLocal(roomName, resourceID, ifMatch, body string) (string, error) {
	sess := s.whipSvc.Whep.Get(resourceID)
	if sess == nil || sess.RoomName != roomName {
		return "", whip.ErrSessionNotFound
	}
	return s.whipSvc.PatchWHEP(resourceID, ifMatch, body)
}

func (s *HTTPServer) deleteWHEPSessionLocal(roomName, resourceID string) error {
	sess := s.whipSvc.Whep.Get(resourceID)
	if sess == nil || sess.RoomName != roomName {
		return whip.ErrSessionNotFound
	}
	return s.whipSvc.DeleteWHEP(resourceID)
}

// resolveWHEPNode picks the routing strategy per WhipConfig.WhepRouting:
//   - "prefix": parse the nodeID prefix out of resourceID ("{nodeID}.{xid}").
//   - "kv":     consult the WHEP_LOC bucket.
//
// Returns "" when the owning node is unknown; callers then fall back to
// local handling, which will 404 if the session truly does not exist.
func (s *HTTPServer) resolveWHEPNode(resourceID string) string {
	if idx := strings.IndexByte(resourceID, '.'); idx > 0 {
		return resourceID[:idx]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	entry, err := s.whepLocKV.Get(ctx, resourceID)
	if err != nil || entry == nil {
		return ""
	}
	return string(entry.Value())
}

// whepRoutingScheme returns a normalised scheme name, defaulting to prefix.
func (s *HTTPServer) whepRoutingScheme() string {
	if s.cfg == nil {
		return config.WhepRoutingPrefix
	}
	switch strings.ToLower(strings.TrimSpace(s.cfg.Whip.WhepRouting)) {
	case config.WhepRoutingKV:
		return config.WhepRoutingKV
	default:
		return config.WhepRoutingPrefix
	}
}

// newWHEPResourceID generates a resourceID according to the selected scheme.
// Prefix scheme embeds the owner NodeID so peer nodes can route without KV.
// KV scheme returns a plain xid since routing is done via whepLocKV.
func (s *HTTPServer) newWHEPResourceID(nodeID string) string {
	id := xid.New().String()
	if s.whepRoutingScheme() == config.WhepRoutingPrefix && nodeID != "" {
		return nodeID + "." + id
	}
	return id
}

// whepKeepaliveFunc returns a Session.Keepalive callback for the KV scheme
// that periodically refreshes the WHEP_LOC entry. For the prefix scheme it
// returns nil so the supervisor avoids needless KV writes.
func (s *HTTPServer) whepKeepaliveFunc(resourceID, nodeID string) func() {
	if s.whepLocKV == nil {
		return nil
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.whepLocKV.Put(ctx, resourceID, []byte(nodeID)); err != nil {
			slog.Warn("WHEP_LOC put failed", "resourceId", resourceID, "err", err)
		}
	}
}

// whepCleanupFunc returns a Session.OnClose callback that drops the
// WHEP_LOC entry for the KV scheme. Under the prefix scheme it is a no-op.
func (s *HTTPServer) whepCleanupFunc(resourceID string) func() {
	if s.whepLocKV == nil {
		return func() {}
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.whepLocKV.Delete(ctx, resourceID); err != nil {
			slog.Debug("WHEP_LOC delete failed", "resourceId", resourceID, "err", err)
		}
	}
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

func (s *HTTPServer) findParticipantByIdentity(roomName, participantID string) (*rtc.Participant, *proto.ParticipantInfo, error) {
	if s.roomManager == nil {
		return nil, nil, fmt.Errorf("room manager not configured")
	}
	room := s.roomManager.GetRoom(roomName)
	if room == nil {
		return nil, nil, fmt.Errorf("room %q unavailable", roomName)
	}
	if p := room.GetParticipantByIdentity(participantID); p != nil {
		info := p.ToProto()
		return p, info, nil
	}
	for _, info := range room.GetAllParticipantInfos("") {
		if info.Identity == participantID && info.State != proto.ParticipantInfo_DISCONNECTED {
			return nil, info, nil
		}
	}
	return nil, nil, fmt.Errorf("participant %q not found in room %q", participantID, roomName)
}

func (s *HTTPServer) attachPublishSession(result *whip.CreateWHIPResult, claims *auth.Claims, roomName string) (*rtc.Participant, error) {
	if s.roomManager == nil {
		return nil, fmt.Errorf("room manager not configured")
	}
	roomCfg := s.cfg.Room
	room := s.roomManager.GetOrCreateRoom(roomName, roomCfg.MaxParticipants, "")
	if room == nil {
		return nil, fmt.Errorf("room %q unavailable", roomName)
	}

	identity := claims.Identity()
	if identity == "" {
		identity = claims.Name
	}
	if identity == "" {
		return nil, fmt.Errorf("participant identity required")
	}
	name := claims.Name
	if name == "" {
		name = identity
	}

	participant := rtc.NewParticipant(rtc.ParticipantParams{
		Sid:           "whip_" + xid.New().String(),
		Identity:      identity,
		Name:          name,
		Grants:        claims.Grants,
		Router:        result.Router,
		TokenVerifier: s.verifier,
		NodeId:        string(s.node.ID),
	})
	participant.SetTransports(result.Transport, nil, nil)
	// Headless signaling: WHIP has no datachannel. SetSignal with all nils
	// latches signalHeadless so broadcast fan-out silently drops messages
	// instead of logging "signal channel not established" per update, and
	// messageLoop is never started for this participant.
	participant.SetSignal(nil, nil, nil, nil)

	if err := room.AddParticipant(participant); err != nil {
		return nil, fmt.Errorf("add participant: %w", err)
	}

	// Do NOT flip state to JOINED here. WHIP has no Ready request, but
	// media is not actually flowing until the client receives this answer
	// and ICE completes. The participant's ICE watchdog will promote the
	// state to JOINED on the first IceStateConnected/Completed event (see
	// onIceSourceUp), and the setup watchdog will close out the
	// participant if ICE never reaches that state within setupTimeout.
	for _, producer := range result.Producers {
		trackSid := fmt.Sprintf("TR_%d", time.Now().UnixNano())
		if err := participant.RegisterWHIPProducer(trackSid, producer); err != nil {
			slog.Warn("WHIP register producer failed", "err", err, "trackSid", trackSid)
			continue
		}
	}

	// Participant is now the sole owner of transport and producers. ICE
	// disconnect is handled by the participant's own ICE watchdog, which
	// closes the participant (and thus the transport) on timeout — no
	// Session / OnClose plumbing needed.
	return participant, nil
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
