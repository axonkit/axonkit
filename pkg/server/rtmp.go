package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jiyeyuran/mediasoup-go/v2"

	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/rtc"
	rtmpingest "github.com/axonkit/axonkit/pkg/rtc/rtmp"
)

// rtmpAttachment is the opaque handle returned by OnSessionReady and
// echoed back via OnSessionClosed. It owns the Participant lifecycle and
// the set of producers already registered onto it.
//
// mu guards concurrent access between:
//   - the rtmp.Session's first-producer callback (runs on the go-rtmp
//     read goroutine when the first seq-header arrives);
//   - the additional-producer callback (same goroutine, later tag);
//   - the OnSessionClosed hook fired by the Handler on disconnect;
//   - the eviction watchdog goroutine that reacts to Participant.Done.
type rtmpAttachment struct {
	mu                    sync.Mutex
	participant           *rtc.Participant
	session               *rtmpingest.Session
	router                *mediasoup.Router
	roomName              string
	closed                bool
	registeredProducerIds map[string]struct{}
}

// buildRTMPHooks assembles the ingest callbacks the rtmp.Service needs.
// The server owns the participant lifecycle: when the RTMP connection
// goes away we remove the participant from its room so subscribers get
// TrackUnpublished + ParticipantDisconnected like any other ingest.
func (s *HTTPServer) buildRTMPHooks() rtmpingest.PublishHooks {
	return rtmpingest.PublishHooks{
		ResolveRouter:   s.resolveRTMPRouter,
		OnSessionReady:  s.onRTMPSessionReady,
		OnSessionClosed: s.onRTMPSessionClosed,
	}
}

// resolveRTMPRouter picks a router for the RTMP publisher. When the
// target room already exists and runs in SharedRouter mode we re-use its
// router so cross-router PipeToRouter is not needed between the ingest
// and the (WebRTC) viewers. Otherwise we pick round-robin across this
// node's routers — matching the heuristic in handleJoinLocal.
func (s *HTTPServer) resolveRTMPRouter(roomName string) (*mediasoup.Router, error) {
	if len(s.routers) == 0 {
		return nil, fmt.Errorf("no mediasoup routers available")
	}
	if s.roomManager != nil {
		if room := s.roomManager.GetRoom(roomName); room != nil && room.SharedRouter() {
			idx := room.RouterIndex()
			if idx >= 0 && idx < len(s.routers) {
				return s.routers[idx], nil
			}
		}
	}
	idx := int(time.Now().UnixNano() % int64(len(s.routers)))
	if idx < 0 {
		idx = -idx
	}
	return s.routers[idx], nil
}

// onRTMPSessionReady is invoked after the publisher authenticates. No
// producers exist yet — they are created lazily by rtmp.Session when
// the first seq-header tag arrives. We install the first/additional
// callbacks here so producer creation fans out into Participant
// attachment and extra RegisterIngestProducer calls.
func (s *HTTPServer) onRTMPSessionReady(ctx context.Context, info rtmpingest.PublishInfo, session *rtmpingest.Session) (any, error) {
	_ = ctx
	if s.roomManager == nil {
		return nil, fmt.Errorf("room manager not configured")
	}
	att := &rtmpAttachment{
		router:                session.Router(),
		session:               session,
		roomName:              info.RoomName,
		registeredProducerIds: make(map[string]struct{}),
	}
	session.SetCallbacks(
		func(leg *rtmpingest.MediaLeg, kind mediasoup.MediaKind) {
			if err := s.attachRTMPFirstProducer(att, info, leg, kind); err != nil {
				slog.Warn("rtmp first-producer attach failed",
					"room", info.RoomName, "identity", info.Claims.Identity(), "err", err)
				closeRTMPLeg(leg)
			}
		},
		func(leg *rtmpingest.MediaLeg, kind mediasoup.MediaKind) {
			if err := s.attachRTMPAdditionalProducer(att, leg, kind); err != nil {
				slog.Warn("rtmp additional-producer attach failed",
					"room", info.RoomName, "kind", kind, "err", err)
				closeRTMPLeg(leg)
			}
		},
	)
	return att, nil
}

// attachRTMPFirstProducer mints the Participant the first time a producer
// becomes available (audio or video, whichever got its seq-header
// first). We re-use AttachPublishParticipant so the bookkeeping (sid
// minting, room.AddParticipant, RegisterIngestProducer) stays in one
// place with WHIP. AutoJoin=true promotes the participant straight to
// JOINED so the setupTimeout watchdog does not evict a healthy session.
func (s *HTTPServer) attachRTMPFirstProducer(att *rtmpAttachment, info rtmpingest.PublishInfo, leg *rtmpingest.MediaLeg, kind mediasoup.MediaKind) error {
	att.mu.Lock()
	if att.closed {
		att.mu.Unlock()
		return fmt.Errorf("session already closed")
	}
	if att.participant != nil {
		// Defensive: the session should only fire OnFirstProducer once.
		att.mu.Unlock()
		return fmt.Errorf("participant already attached")
	}
	att.mu.Unlock()

	source := rtmpSourceForKind(kind)
	participant, err := s.AttachPublishParticipant(AttachPublishParticipantOptions{
		SidPrefix:     "rtmp_",
		RoomName:      info.RoomName,
		Claims:        info.Claims,
		Router:        att.router,
		Transport:     leg.Transport,
		TransportKind: TransportKindDirect,
		Producers: []AttachedProducer{
			{Producer: leg.Producer, Source: source},
		},
		AutoJoin: true,
	})
	if err != nil {
		return fmt.Errorf("attach participant: %w", err)
	}

	att.mu.Lock()
	att.participant = participant
	att.registeredProducerIds[leg.Producer.Id()] = struct{}{}
	att.mu.Unlock()

	go s.watchRTMPParticipant(att)

	slog.Info("RTMP participant attached",
		"room", info.RoomName, "identity", participant.Identity(),
		"sid", participant.Sid(), "firstKind", kind)
	return nil
}

// watchRTMPParticipant tears down the RTMP TCP connection as soon as
// the attached Participant is closed (client disconnect, room eviction,
// admin kick, …). Without this, an ingested publisher would keep
// pushing frames into a DirectTransport whose Producer has already
// been closed, wasting bandwidth and confusing operators. The
// teardown is idempotent: if OnSessionClosed already fired we return
// early; otherwise we close the RTMP conn, which triggers go-rtmp's
// OnClose which in turn calls OnSessionClosed harmlessly.
func (s *HTTPServer) watchRTMPParticipant(att *rtmpAttachment) {
	att.mu.Lock()
	participant := att.participant
	session := att.session
	att.mu.Unlock()

	if participant == nil {
		return
	}
	<-participant.Done()

	att.mu.Lock()
	if att.closed {
		att.mu.Unlock()
		return
	}
	att.closed = true
	att.mu.Unlock()

	if session != nil {
		session.Close()
		session.CloseConn()
	}
	slog.Info("RTMP connection closed due to participant exit",
		"room", att.roomName, "identity", participant.Identity(), "sid", participant.Sid())
}

// attachRTMPAdditionalProducer registers the second producer onto the
// existing participant, triggering TrackPublished fan-out to
// subscribers. The producer's DirectTransport is not wired into the
// participant's transport slots (there's only a single direct slot and
// it's already occupied by the first leg); Participant.Close won't
// tear it down automatically so we rely on rtmp.Session.Close (fired
// by the Handler) to close the second DirectTransport + Producer when
// the connection goes away.
func (s *HTTPServer) attachRTMPAdditionalProducer(att *rtmpAttachment, leg *rtmpingest.MediaLeg, kind mediasoup.MediaKind) error {
	att.mu.Lock()
	defer att.mu.Unlock()
	if att.closed {
		return fmt.Errorf("session already closed")
	}
	if att.participant == nil {
		return fmt.Errorf("participant not yet attached")
	}
	if _, ok := att.registeredProducerIds[leg.Producer.Id()]; ok {
		return nil
	}

	source := rtmpSourceForKind(kind)
	if err := att.participant.RegisterIngestProducer(source, leg.Producer); err != nil {
		return fmt.Errorf("register producer: %w", err)
	}
	att.registeredProducerIds[leg.Producer.Id()] = struct{}{}

	slog.Info("RTMP additional track registered",
		"room", att.roomName, "identity", att.participant.Identity(),
		"kind", kind, "producerId", leg.Producer.Id())
	return nil
}

// onRTMPSessionClosed tears down the participant when the RTMP
// connection goes away. rtmp.Handler calls Session.Close() first, which
// closes producers + DirectTransports; we then drop the participant from
// the room so the room-side state (tracks, caches, WHEP bridging) all
// converge.
func (s *HTTPServer) onRTMPSessionClosed(attached any) {
	att, ok := attached.(*rtmpAttachment)
	if !ok || att == nil {
		return
	}
	att.mu.Lock()
	if att.closed {
		att.mu.Unlock()
		return
	}
	att.closed = true
	participant := att.participant
	att.mu.Unlock()

	if participant == nil {
		return
	}
	participant.Close(proto.DisconnectReason_CLIENT_INITIATED)
	slog.Info("RTMP participant closed",
		"room", att.roomName, "identity", participant.Identity(), "sid", participant.Sid())
}

// closeRTMPLeg is the error-path teardown for producer+transport pairs
// the callback couldn't wire into a participant. It runs on the rare
// failure branch (e.g. room full); happy path teardown comes from
// Participant.Close (first leg) and Session.Close (additional legs).
func closeRTMPLeg(leg *rtmpingest.MediaLeg) {
	if leg == nil {
		return
	}
	if leg.Producer != nil {
		_ = leg.Producer.Close()
	}
	if leg.Transport != nil {
		leg.Transport.Close()
	}
}

// rtmpSourceForKind maps mediasoup kind → TrackSource. RTMP has no
// explicit camera/mic hint so we default to the obvious value per kind.
func rtmpSourceForKind(kind mediasoup.MediaKind) proto.TrackSource {
	switch kind {
	case mediasoup.MediaKindAudio:
		return proto.TrackSource_MICROPHONE
	case mediasoup.MediaKindVideo:
		return proto.TrackSource_CAMERA
	}
	return proto.TrackSource_UNKNOWN
}
