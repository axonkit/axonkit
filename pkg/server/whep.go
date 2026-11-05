package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/rtc"
	"github.com/axonkit/axonkit/pkg/rtc/whip"
	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/rs/xid"
)

// WHEP (subscribe-side WHIP) is deliberately not exposed over HTTP yet.
//
// The blocker is the consumer-side RTP parameters override in mediasoup.
// whip.Service.CreateWHEP wants to feed Transport.Consume the subscriber's
// own m-section parameters, but mediasoup has not merged that capability
// upstream (https://github.com/versatica/mediasoup/pull/1783), and
// mediasoup-go has no ConsumerOptions.RtpParameters field as a result. See
// the TODO in pkg/rtc/whip/service.go.
//
// Without the override Consume negotiates from the router's
// RtpCapabilities, so the answer carries mediasoup-chosen payload types and
// fmtp lines instead of the ones declared in the offer. Standard clients
// (browsers, ffmpeg) reject such an answer or silently fail to decode, which
// makes WHEP unusable in practice.
//
// Everything below is kept compiling so it stays in sync with the package
// while the route registration is disabled in registerWHIPRoutes; it only
// needs the single call re-enabled once the upstream PR lands.

// whepLocBucket is the NATS KV bucket name for the "kv" WHEP routing scheme.
// It stores resourceID → nodeID with a short TTL refreshed by each session's
// keepalive.
const whepLocBucket = "WHEP_LOC"

// whepLocTTL is the TTL for WHEP_LOC entries. Must be comfortably larger
// than whip.DefaultKeepaliveInterval (10s) so a single missed refresh does
// not evict a live session from discovery.
const whepLocTTL = 30 * time.Second

// ============================================================
// WHEP HTTP Routes
// ============================================================

// registerWHEPRoutes is currently not called; see registerWHIPRoutes and the
// file header for why.
func (s *HTTPServer) registerWHEPRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /whep/{room}/{participantId}", s.handleWHEPPost)
	mux.HandleFunc("PATCH /whep/{room}/{resourceId}", s.handleWHEPPatch)
	mux.HandleFunc("DELETE /whep/{room}/{resourceId}", s.handleWHEPDelete)
	mux.HandleFunc("OPTIONS /whep/{tail...}", writeCORSOK)
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
	// is shared across both signaling protocols.
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

func (s *HTTPServer) handleWHEPPatch(w http.ResponseWriter, r *http.Request) {
	roomName, id, ifMatch, body, ok := s.readPatchRequest(w, r)
	if !ok {
		return
	}
	etag, err := s.patchWHEPSession(roomName, id, ifMatch, body)
	writePatchResponse(w, etag, err)
}

func (s *HTTPServer) handleWHEPDelete(w http.ResponseWriter, r *http.Request) {
	roomName, id, ok := s.readDeleteRequest(w, r)
	if !ok {
		return
	}
	writeDeleteResponse(w, s.deleteWHEPSession(roomName, id))
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

	// whepLocKV is only provisioned for the "kv" scheme; under the default
	// "prefix" scheme it is nil. Calling Get on a nil store.Store interface
	// would panic, so bail out and let the caller fall back to local handling.
	if s.whepLocKV == nil {
		return ""
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
