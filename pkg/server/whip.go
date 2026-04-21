package server

import (
	"context"
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
	"github.com/axonkit/axonkit/pkg/types"
	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/rs/xid"
)

// ============================================================
// WHIP / WHEP HTTP Routes
// ============================================================
//
// URL layout:
//
//   POST   /whip                            - standalone publish
//                                              (resourceId := JWT identity;
//                                              the stream is advertised via
//                                              FIB + WHIP_RESOURCES KV so
//                                              other nodes can pull it via
//                                              /whep/{resourceId}).
//   POST   /whip/{room}                     - publish as a Room Participant.
//   PATCH  /whip/{resourceId}               - ICE restart (If-Match: *) / trickle.
//   DELETE /whip/{resourceId}               - tear down the publish session.
//
//   POST   /whep/{resourceId}               - subscribe to a WHIP standalone
//                                              publisher anywhere in the mesh.
//   POST   /whep/{room}/{participantId}     - subscribe to a specific
//                                              participant inside a room
//                                              WITHOUT joining that room;
//                                              at most one audio + one video
//                                              track are returned, with
//                                              screen-share prioritised over
//                                              camera/microphone.
//   PATCH  /whep/{resourceId}               - ICE restart / trickle.
//   DELETE /whep/{resourceId}               - tear down the subscribe session.
//
// Auth:
//   - Bearer token in "Authorization: Bearer <jwt>" OR ?access_token=… query.
//   - Standalone (no {room}):  requires CanPublish (WHIP) / CanSubscribe (WHEP).
//   - In-room ({room}):        additionally requires RoomGrant.Join and
//     (RoomGrant.Name == "" || == {room}). WHEP-by-participant does NOT
//     require Join (it's an observer path).
//
// Lifetime:
//   Each session runs a single supervisor goroutine — see whip.Session.Start.
//   It (a) monitors mediasoup ICE state and tears down after
//   DefaultICEDisconnectedTimeout of uninterrupted "disconnected"; and
//   (b) ticks the Keepalive hook (used here to refresh the WHIP_RESOURCES
//   KV record so other nodes keep seeing the resource as alive). When the
//   same JWT identity publishes again the old session is evicted and
//   replaced — there is no 409 response on the happy path.

// whipTransportFactory is the glue between the whip.Service and the HTTP
// layer: it picks a router/webRtcServer pair and creates a WebRtcTransport.
type whipTransportFactory struct {
	routers       []*mediasoup.Router
	webRtcServers []*mediasoup.WebRtcServer
}

// New implements whip.TransportFactory.
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

// whipProducerSource implements whip.ProducerSource: it serves Producers
// for both "WHIP standalone resourceId" and "room participant" lookups.
type whipProducerSource struct {
	server *HTTPServer
}

// ProducersForResource walks the WHIP registry to find producers published
// by the given resourceId on THIS node. Cross-node resolution (pipe the
// stream in via FIB) is handled by HTTPServer.producersForStandaloneResource
// — this interface method only reports what's already in memory.
func (s *whipProducerSource) ProducersForResource(resourceID string) []*mediasoup.Producer {
	if resourceID == "" {
		return nil
	}
	sess := s.server.whipSvc.Whip.Get(resourceID)
	if sess == nil {
		return nil
	}
	// Session.Producers is append-only during CreateWHIP and never
	// mutated afterwards; it is cleared only by Close (guarded by
	// closeOnce). A concurrent Close is tolerated: consumer creation
	// on a closed producer simply fails, which surfaces as a 500.
	out := make([]*mediasoup.Producer, 0, len(sess.Producers))
	out = append(out, sess.Producers...)
	return out
}

// ProducersForParticipant returns at most two Producers (one audio + one
// video) selected from the named participant's currently published tracks.
// Priority:
//
//	video  : TrackSource_SCREEN_SHARE > TrackSource_CAMERA > anything else
//	audio  : TrackSource_SCREEN_SHARE_AUDIO > TrackSource_MICROPHONE > anything else
//
// Muted tracks are still eligible — we prefer delivering a muted stream
// over returning nothing so the subscriber's player layout stays stable.
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

// selectWHEPTracks implements the priority rules from
// ProducersForParticipant. Separated for direct testing without needing
// a full participant.
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
				score = 1 // lowest tier; beats "no pick yet"
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

// registerWHIPRoutes plumbs the WHIP/WHEP endpoints into the existing
// http.ServeMux that the HTTPServer owns. Called from RegisterRoutes.
func (s *HTTPServer) registerWHIPRoutes(mux *http.ServeMux) {
	// --- WHIP
	mux.HandleFunc("POST /whip", s.handleWHIPPostStandalone)
	mux.HandleFunc("POST /whip/{room}", s.handleWHIPPostWithRoom)
	mux.HandleFunc("PATCH /whip/{resourceId}", s.handleWHIPPatch)
	mux.HandleFunc("DELETE /whip/{resourceId}", s.handleWHIPDelete)
	mux.HandleFunc("OPTIONS /whip", writeCORSOK)
	mux.HandleFunc("OPTIONS /whip/{tail...}", writeCORSOK)

	// --- WHEP
	mux.HandleFunc("POST /whep/{resourceId}", s.handleWHEPPostByResource)
	mux.HandleFunc("POST /whep/{room}/{participantId}", s.handleWHEPPostByParticipant)
	mux.HandleFunc("PATCH /whep/{resourceId}", s.handleWHEPPatch)
	mux.HandleFunc("DELETE /whep/{resourceId}", s.handleWHEPDelete)
	mux.HandleFunc("OPTIONS /whep", writeCORSOK)
	mux.HandleFunc("OPTIONS /whep/{tail...}", writeCORSOK)
}

func writeCORSOK(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }

// ============================================================
// WHIP POST
// ============================================================

func (s *HTTPServer) handleWHIPPostStandalone(w http.ResponseWriter, r *http.Request) {
	s.handleWHIPPost(w, r, "")
}

func (s *HTTPServer) handleWHIPPostWithRoom(w http.ResponseWriter, r *http.Request) {
	s.handleWHIPPost(w, r, r.PathValue("room"))
}

func (s *HTTPServer) handleWHIPPost(w http.ResponseWriter, r *http.Request, roomName string) {
	if !isSDPContent(r) {
		http.Error(w, "expected Content-Type: application/sdp", http.StatusUnsupportedMediaType)
		return
	}
	offerSDP, err := readSDPBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	claims, errResp := s.authorizeWHIP(r, roomName, true)
	if errResp != nil {
		errResp.write(w)
		return
	}

	resourceID := whipResourceIDFor(claims)

	// Build the keepalive closure early so both the standalone and in-room
	// paths can reuse it. For in-room WHIP we still publish the record —
	// a WHEP subscriber can reach the participant via /whep/{room}/{id}
	// but also via /whep/{resourceId} if they prefer to address the
	// publisher directly.
	//
	// The closure is a no-op when WHIPKV is not wired (tests).
	keepalive := s.makeWHIPKeepalive(resourceID)

	result, err := s.whipSvc.CreateWHIP(whip.CreateWHIPOptions{
		OfferSDP:        offerSDP,
		ResourceID:      resourceID,
		ReplaceExisting: true, // same identity reconnecting wins
		Keepalive:       keepalive,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, proto.ErrorResponse{
			Code: proto.ErrCodeInternal, Error: err.Error()})
		return
	}

	// Wire distributed advertising (KV + FIB) for standalone; for in-room
	// the participant flow handles cascade already, so we only need the
	// KV record (which keepalive maintains on its own).
	streamIDs := s.whipStreamIDs(result.Session)
	s.putWHIPRecord(resourceID, streamIDs)
	if roomName == "" {
		s.advertiseStandaloneSession(result.Session, streamIDs)
	} else if err := s.attachPublishSession(result.Session, claims, roomName); err != nil {
		_ = s.whipSvc.Delete(whip.ModeWHIP, result.Session.ID)
		writeJSON(w, http.StatusInternalServerError, proto.ErrorResponse{
			Code: proto.ErrCodeInternal, Error: fmt.Sprintf("attach to room: %v", err)})
		return
	}

	// Delete the KV record on session teardown (watchdog or explicit
	// DELETE). Chained with any previously installed OnClose so we don't
	// drop room cleanup.
	s.installResourceTeardown(result.Session, resourceID)

	s.writeWHIPResponse(w, "whip", result.Session, result.Answer, result.ETag)
}

// ============================================================
// WHEP POST
// ============================================================

func (s *HTTPServer) handleWHEPPostByResource(w http.ResponseWriter, r *http.Request) {
	resourceID := r.PathValue("resourceId")
	if !isSDPContent(r) {
		http.Error(w, "expected Content-Type: application/sdp", http.StatusUnsupportedMediaType)
		return
	}
	offerSDP, err := readSDPBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, errResp := s.authorizeWHIP(r, "", false); errResp != nil {
		errResp.write(w)
		return
	}

	producers, err := s.producersForStandaloneResource(resourceID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	result, err := s.whipSvc.CreateWHEP(offerSDP, producers)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, proto.ErrorResponse{
			Code: proto.ErrCodeInternal, Error: err.Error()})
		return
	}
	s.writeWHIPResponse(w, "whep", result.Session, result.Answer, result.ETag)
}

func (s *HTTPServer) handleWHEPPostByParticipant(w http.ResponseWriter, r *http.Request) {
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

	// This flow does NOT join the room — CanSubscribe + optional room
	// scope match is enough. RoomGrant.Join is intentionally NOT required.
	if _, errResp := s.authorizeWHEPForRoom(r, roomName); errResp != nil {
		errResp.write(w)
		return
	}

	producers := s.whipSvc.Sources.ProducersForParticipant(roomName, participantID)
	if len(producers) == 0 {
		http.Error(w, fmt.Sprintf("no active producers for participant %q in room %q", participantID, roomName), http.StatusNotFound)
		return
	}

	result, err := s.whipSvc.CreateWHEP(offerSDP, producers)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, proto.ErrorResponse{
			Code: proto.ErrCodeInternal, Error: err.Error()})
		return
	}
	s.writeWHIPResponse(w, "whep", result.Session, result.Answer, result.ETag)
}

// producersForStandaloneResource resolves a WHIP resourceId to live
// mediasoup Producers on THIS node. If the publisher lives on another node
// we consult the WHIP_RESOURCES KV to find its StreamIDs and ask the media
// manager to pipe them in before returning.
func (s *HTTPServer) producersForStandaloneResource(resourceID string) ([]*mediasoup.Producer, error) {
	// Fast path: local registry has the publisher.
	if local := s.whipSvc.Sources.ProducersForResource(resourceID); len(local) > 0 {
		return local, nil
	}

	// Cross-node path: consult KV + media manager. If either piece is not
	// wired (tests), treat as "not found".
	if s.whipResources == nil || s.mediaMgr == nil {
		return nil, fmt.Errorf("whip resource %q not hosted on this node", resourceID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rec, err := s.whipResources.Get(ctx, resourceID)
	if err != nil || rec == nil {
		return nil, fmt.Errorf("whip resource %q not found: %v", resourceID, err)
	}
	if rec.NodeID == string(s.node.ID) {
		// Record says we own it but in-memory registry says otherwise —
		// the owning session just got closed. Treat as 404.
		return nil, fmt.Errorf("whip resource %q locally absent despite KV record", resourceID)
	}

	out := make([]*mediasoup.Producer, 0, len(rec.StreamIDs))
	for _, raw := range rec.StreamIDs {
		streamID := types.StreamID(raw)
		lp, err := s.mediaMgr.EnsureProducer(streamID)
		if err != nil {
			return nil, fmt.Errorf("pipe %s: %w", streamID, err)
		}
		if lp != nil && lp.Producer != nil {
			out = append(out, lp.Producer)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("whip resource %q has no reachable producers", resourceID)
	}
	return out, nil
}

// whipStreamIDs returns the canonical StreamID for each producer in the
// session, using the project-wide routerId/producerId encoding.
func (s *HTTPServer) whipStreamIDs(sess *whip.Session) []types.StreamID {
	if sess == nil || sess.Router == nil {
		return nil
	}
	routerID := sess.Router.Id()
	out := make([]types.StreamID, 0, len(sess.Producers))
	for _, p := range sess.Producers {
		out = append(out, types.NewStreamID(routerID, p.Id()))
	}
	return out
}

// makeWHIPKeepalive returns a closure safe to call on every supervisor
// tick. It refreshes the WHIP_RESOURCES KV TTL under the publisher's
// resourceId so that remote WHEP nodes keep seeing it as alive. The
// StreamID list is re-derived from the session's producers each call —
// WHIP producers are created once at session setup and never change, so
// the list is stable for the session's lifetime; we still re-read it
// lazily so Keepalive can be wired before CreateWHIP returns.
func (s *HTTPServer) makeWHIPKeepalive(resourceID string) func() {
	if s.whipResources == nil {
		return nil
	}
	return func() {
		sess := s.whipSvc.Whip.Get(resourceID)
		if sess == nil {
			return
		}
		s.putWHIPRecord(resourceID, s.whipStreamIDs(sess))
	}
}

// putWHIPRecord writes or refreshes a resource record in KV. Safe to call
// with nil store (no-op) and with an empty streamIDs slice (records the
// publisher's node membership even before the first producer appears —
// rare because producers are created synchronously in CreateWHIP).
func (s *HTTPServer) putWHIPRecord(resourceID string, streamIDs []types.StreamID) {
	if s.whipResources == nil || resourceID == "" {
		return
	}
	ids := make([]string, 0, len(streamIDs))
	for _, sid := range streamIDs {
		ids = append(ids, string(sid))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.whipResources.Put(ctx, whip.ResourceRecord{
		ResourceID: resourceID,
		NodeID:     string(s.node.ID),
		StreamIDs:  ids,
	}); err != nil {
		slog.Warn("WHIP KV put failed", "resourceId", resourceID, "err", err)
	}
}

// installResourceTeardown chains a KV delete onto the session's OnClose so
// that a clean DELETE (or watchdog teardown) doesn't leave a stale record
// lingering until the bucket TTL elapses.
func (s *HTTPServer) installResourceTeardown(sess *whip.Session, resourceID string) {
	if sess == nil {
		return
	}
	prev := sess.OnClose
	sess.OnClose = func() {
		if prev != nil {
			prev()
		}
		if s.whipResources != nil && resourceID != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := s.whipResources.Delete(ctx, resourceID); err != nil {
				slog.Warn("WHIP KV delete failed", "resourceId", resourceID, "err", err)
			}
		}
	}
}

// advertiseStandaloneSession registers every producer from a WHIP standalone
// session in the node's media manager. RegisterProducer also advertises the
// stream via RIB so remote nodes can discover and pipe it on demand.
// Teardown is wired through Session.OnClose.
func (s *HTTPServer) advertiseStandaloneSession(sess *whip.Session, streamIDs []types.StreamID) {
	if sess == nil || s.mediaMgr == nil {
		return
	}
	for i, sid := range streamIDs {
		if i >= len(sess.Producers) {
			break
		}
		s.mediaMgr.RegisterProducer(sid, sess.Producers[i])
	}

	prev := sess.OnClose
	sess.OnClose = func() {
		for _, sid := range streamIDs {
			s.mediaMgr.UnregisterProducer(sid)
		}
		if prev != nil {
			prev()
		}
	}
}

// ============================================================
// WHIP / WHEP PATCH + DELETE
// ============================================================

func (s *HTTPServer) handleWHIPPatch(w http.ResponseWriter, r *http.Request) {
	s.handlePatch(w, r, whip.ModeWHIP)
}

func (s *HTTPServer) handleWHEPPatch(w http.ResponseWriter, r *http.Request) {
	s.handlePatch(w, r, whip.ModeWHEP)
}

func (s *HTTPServer) handlePatch(w http.ResponseWriter, r *http.Request, mode whip.Mode) {
	id := r.PathValue("resourceId")
	ifMatch := r.Header.Get("If-Match")
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()

	etag, err := s.whipSvc.Patch(mode, id, ifMatch, string(body))
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

func (s *HTTPServer) handleWHIPDelete(w http.ResponseWriter, r *http.Request) {
	s.handleDelete(w, r, whip.ModeWHIP)
}

func (s *HTTPServer) handleWHEPDelete(w http.ResponseWriter, r *http.Request) {
	s.handleDelete(w, r, whip.ModeWHEP)
}

func (s *HTTPServer) handleDelete(w http.ResponseWriter, r *http.Request, mode whip.Mode) {
	id := r.PathValue("resourceId")
	if err := s.whipSvc.Delete(mode, id); err != nil {
		if errors.Is(err, whip.ErrSessionNotFound) {
			http.Error(w, "resource not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ============================================================
// Helpers
// ============================================================

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

// authorizeWHIP verifies the Bearer token and checks permissions. publish=true
// means WHIP (needs CanPublish); publish=false means WHEP (needs CanSubscribe).
// When roomName is non-empty, we additionally require RoomGrant.Join and a
// scope match — appropriate for WHIP-in-room.
func (s *HTTPServer) authorizeWHIP(r *http.Request, roomName string, publish bool) (*auth.Claims, *whipErr) {
	token := extractBearerToken(r)
	if token == "" && s.cfg.Auth.AllowAnonymous {
		return anonymousClaims(roomName, publish), nil
	}
	if token == "" {
		return nil, &whipErr{status: http.StatusUnauthorized, body: "missing Authorization: Bearer <token>"}
	}
	claims, err := s.verifier.Verify(token)
	if err != nil {
		return nil, &whipErr{status: http.StatusUnauthorized, body: err.Error()}
	}
	if claims.Grants == nil {
		return nil, &whipErr{status: http.StatusForbidden, body: "token has no grants"}
	}

	if publish {
		if claims.Grants.Media == nil || !claims.Grants.Media.GetCanPublish() {
			return nil, &whipErr{status: http.StatusForbidden, body: "token does not grant publish permission"}
		}
	} else {
		if claims.Grants.Media == nil || !claims.Grants.Media.GetCanSubscribe() {
			return nil, &whipErr{status: http.StatusForbidden, body: "token does not grant subscribe permission"}
		}
	}

	if roomName != "" {
		rg := claims.Grants.Room
		if rg == nil || !rg.Join {
			return nil, &whipErr{status: http.StatusForbidden, body: "token does not grant room join permission"}
		}
		if rg.Name != "" && rg.Name != roomName {
			return nil, &whipErr{status: http.StatusForbidden, body: "token is scoped to a different room"}
		}
	}

	return claims, nil
}

// authorizeWHEPForRoom validates WHEP /whep/{room}/{participant} requests:
// CanSubscribe is required, and if the token is scoped to a specific room
// the path must match. This does NOT require RoomGrant.Join — subscribing
// to a room participant via WHEP is explicitly an observer flow.
func (s *HTTPServer) authorizeWHEPForRoom(r *http.Request, roomName string) (*auth.Claims, *whipErr) {
	token := extractBearerToken(r)
	if token == "" && s.cfg.Auth.AllowAnonymous {
		return anonymousClaims(roomName, false), nil
	}
	if token == "" {
		return nil, &whipErr{status: http.StatusUnauthorized, body: "missing Authorization: Bearer <token>"}
	}
	claims, err := s.verifier.Verify(token)
	if err != nil {
		return nil, &whipErr{status: http.StatusUnauthorized, body: err.Error()}
	}
	if claims.Grants == nil {
		return nil, &whipErr{status: http.StatusForbidden, body: "token has no grants"}
	}
	if claims.Grants.Media == nil || !claims.Grants.Media.GetCanSubscribe() {
		return nil, &whipErr{status: http.StatusForbidden, body: "token does not grant subscribe permission"}
	}
	if rg := claims.Grants.Room; rg != nil && rg.Name != "" && rg.Name != roomName {
		return nil, &whipErr{status: http.StatusForbidden, body: "token is scoped to a different room"}
	}
	return claims, nil
}

func anonymousClaims(roomName string, publish bool) *auth.Claims {
	identity := "anon_" + xid.New().String()
	boolTrue := true
	grants := &proto.ClaimGrants{
		Identity: identity,
		Media: &proto.MediaGrant{
			CanPublish:   &boolTrue,
			CanSubscribe: &boolTrue,
		},
	}
	if roomName != "" {
		grants.Room = &proto.RoomGrant{Name: roomName, Join: true}
	}
	_ = publish
	return &auth.Claims{
		Sub:    identity,
		Name:   identity,
		Grants: grants,
	}
}

// whipResourceIDFor picks the WHIP resource id for the given claims. We
// prefer the explicit Identity grant; fall back to JWT Sub; fall back to a
// random id if both are empty (anonymous edge case).
func whipResourceIDFor(claims *auth.Claims) string {
	if claims != nil && claims.Grants != nil && claims.Grants.Identity != "" {
		return claims.Grants.Identity
	}
	if claims != nil && claims.Sub != "" {
		return claims.Sub
	}
	return "whip_" + xid.New().String()
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

// attachPublishSession turns a standalone WHIP publish session into a Room
// Participant. The participant owns the Producer(s) for lifecycle purposes;
// when the WHIP session is DELETEd, we close the participant too.
func (s *HTTPServer) attachPublishSession(sess *whip.Session, claims *auth.Claims, roomName string) error {
	if s.roomManager == nil {
		return fmt.Errorf("room manager not configured")
	}
	roomCfg := s.cfg.Room
	room := s.roomManager.GetOrCreateRoom(roomName, roomCfg.MaxParticipants, "")
	if room == nil {
		return fmt.Errorf("room %q unavailable", roomName)
	}

	identity := claims.Sub
	if identity == "" {
		identity = "whip_" + xid.New().String()
	}
	name := claims.Name
	if name == "" {
		name = identity
	}

	participant := rtc.NewParticipant(rtc.ParticipantParams{
		Sid:           "PA_" + xid.New().String(),
		Identity:      identity,
		Name:          name,
		Grants:        claims.Grants,
		Router:        sess.Router,
		TokenVerifier: s.verifier,
		NodeId:        string(s.node.ID),
	})
	participant.SetTransports(sess.Transport, nil, nil)
	participant.SetSignal(nil, nil, nil, nil)

	if err := room.AddParticipant(participant); err != nil {
		return fmt.Errorf("add participant: %w", err)
	}

	sess.RoomName = roomName

	for _, producer := range sess.Producers {
		sid := "TR_" + xid.New().String()
		if err := participant.RegisterWHIPProducer(sid, producer); err != nil {
			slog.Warn("WHIP register producer failed", "err", err, "trackSid", sid)
			continue
		}
		sess.TrackSids = append(sess.TrackSids, sid)
	}

	// Tear down the participant when the WHIP session is destroyed (by
	// the client's DELETE or by the supervisor).
	prev := sess.OnClose
	sess.OnClose = func() {
		room.RemoveParticipant(participant.Sid(), proto.DisconnectReason_CLIENT_INITIATED)
		if prev != nil {
			prev()
		}
	}
	return nil
}

// writeWHIPResponse sends back the standard RFC 9725 response: 201 Created,
// Content-Type: application/sdp, Location: /whip|whep/{id}, ETag, body = answer.
func (s *HTTPServer) writeWHIPResponse(
	w http.ResponseWriter, prefix string,
	sess *whip.Session, answer string, etag string,
) {
	location := "/" + prefix + "/" + sess.ID

	w.Header().Set("Content-Type", "application/sdp")
	w.Header().Set("Location", location)
	if etag != "" {
		w.Header().Set("ETag", `"`+etag+`"`)
	}
	w.Header().Set("X-Node-Id", string(s.node.ID))
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(answer))

	slog.Info("WHIP/WHEP session created", "mode", prefix, "resourceId", sess.ID,
		"location", location)
}
