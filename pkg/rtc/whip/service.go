package whip

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/jiyeyuran/mediasoup-go/v2"
)

// TransportFactory hands out a fresh WebRtcTransport for a new WHIP/WHEP
// session. The factory is expected to encapsulate router/webRtcServer
// selection so that the whip package does not need to know the higher-level
// allocation policy.
type TransportFactory interface {
	// New returns a freshly created WebRtcTransport along with the router
	// that owns it. Kind is a hint: "whip" (publish) or "whep" (subscribe).
	New(mode Mode) (*mediasoup.Transport, *mediasoup.Router, error)
}

// ProducerSource supplies the mediasoup Producer IDs that should be consumed
// during a WHEP session for a specific participant in a room.
type ProducerSource interface {
	// ProducersForParticipant returns the Producers of a specific
	// participant (by identity) inside a named room. At most one audio
	// and one video Producer are returned — the caller is expected to
	// apply source-priority selection (e.g. screenShare > camera) before
	// returning.
	ProducersForParticipant(roomName, participantIdentity string) []*mediasoup.Producer
}

// Service is the stateful orchestrator for WHIP/WHEP sessions. It holds the
// transport factory, maintains the WHEP session registry, and exposes
// high-level Create / Patch / Delete entry points the HTTP layer calls.
//
// WHIP does not use a service-owned registry: the WHIP session is bound to
// a rtc.Participant, and the participant is the source of truth (it lives
// in the room, is published to the distributed room store with a NodeId,
// and is cleaned up via room.RemoveParticipant). Having an extra in-memory
// map for WHIP would duplicate that lifecycle and diverge on eviction.
type Service struct {
	Factory TransportFactory
	Sources ProducerSource
	// Whep tracks subscribe-side sessions — they have no participant
	// backing, so the service owns their lifecycle directly.
	Whep   *Registry
	Logger *slog.Logger

	// ICEDisconnectedTimeout is propagated to every new Session's
	// watchdog. If zero the Session falls back to
	// DefaultICEDisconnectedTimeout.
	ICEDisconnectedTimeout time.Duration
}

// NewService wires a new Service. Logger defaults to slog.Default when nil.
func NewService(factory TransportFactory, sources ProducerSource, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default().With("component", "whip.Service")
	}
	return &Service{
		Factory: factory,
		Sources: sources,
		Whep:    NewRegistry(),
		Logger:  logger,
	}
}

// CreateWHIPOptions holds the inputs for a new WHIP publish session.
type CreateWHIPOptions struct {
	OfferSDP string
}

// CreateWHIPResult carries the raw artifacts a WHIP publish flow needs to
// attach onto a rtc.Participant and respond to the POST. There is no
// *Session here on purpose: for WHIP the Participant is the owner of the
// transport lifecycle and of the ICE watchdog. A service-owned Session
// would duplicate both and force cross-layer OnClose plumbing.
type CreateWHIPResult struct {
	Transport *mediasoup.Transport
	Router    *mediasoup.Router
	Producers []*mediasoup.Producer
	Answer    string
	ETag      string
}

// CreateWHIP creates a new publish session from an offer SDP.
//
// Steps:
//  1. Parse offer and validate.
//  2. Ask the factory for a WebRtcTransport + router.
//  3. Connect the transport using the client's DTLS fingerprint.
//  4. Create one Producer per sendonly/sendrecv m-section using the offer's
//     RtpParameters.
//  5. Build answer SDP from the transport's ICE/DTLS + the original offer PTs.
func (s *Service) CreateWHIP(opts CreateWHIPOptions) (*CreateWHIPResult, error) {
	if s.Factory == nil {
		return nil, fmt.Errorf("no transport factory configured")
	}

	transport, router, err := s.Factory.New(ModeWHIP)
	if err != nil {
		return nil, fmt.Errorf("factory: %w", err)
	}

	// closeOnErr is used to tear down partial setup before the caller has
	// a chance to take ownership. Once we return success the caller owns
	// transport and must Close it (via Participant.Close) on teardown.
	closeOnErr := func() { transport.Close() }

	caps := router.RtpCapabilities()
	parsed, err := ParseOffer(opts.OfferSDP, caps)
	if err != nil {
		closeOnErr()
		return nil, fmt.Errorf("parse offer: %w", err)
	}

	answerSetup, remoteDtlsRole := negotiateDtlsRole(parsed.DtlsSetup)
	if err := transport.Connect(&mediasoup.TransportConnectOptions{
		DtlsParameters: &mediasoup.DtlsParameters{
			// Role here is the *remote* peer's DTLS role; mediasoup takes the
			// opposite. Must stay consistent with the `a=setup` we answer
			// with, otherwise both sides wait for a ClientHello (→ timeout)
			// or both send one (→ handshake failure). The `a=setup:passive`
			// + `Role=Client` default works for browsers/OBS (they offer
			// actpass) but breaks ffmpeg, which offers `setup:passive` and
			// expects *us* to initiate.
			Role:         remoteDtlsRole,
			Fingerprints: []mediasoup.DtlsFingerprint{parsed.DtlsFingerprint},
		},
	}); err != nil {
		closeOnErr()
		return nil, fmt.Errorf("transport connect: %w", err)
	}

	var producers []*mediasoup.Producer
	// Produce one mediasoup Producer per publishable m-section.
	for _, pm := range parsed.Medias {
		if pm.Direction != mediasoup.MediaDirectionSendonly && pm.Direction != mediasoup.MediaDirectionSendrecv {
			continue
		}
		producer, err := transport.Produce(&mediasoup.ProducerOptions{
			Kind:          pm.Kind,
			RtpParameters: pm.RtpParameters,
		})
		if err != nil {
			closeOnErr()
			return nil, fmt.Errorf("produce %s mid=%q: %w", pm.Kind, pm.Mid, err)
		}
		producers = append(producers, producer)
	}
	if len(producers) == 0 {
		closeOnErr()
		return nil, fmt.Errorf("offer has no publishable media")
	}

	// Build the answer.
	td := transport.Data()
	if td.WebRtcTransportData == nil {
		closeOnErr()
		return nil, fmt.Errorf("transport has no webrtc data")
	}
	data := td.WebRtcTransportData

	medias := make([]*MediaAnswer, 0, len(parsed.Medias))
	for _, pm := range parsed.Medias {
		medias = append(medias, &MediaAnswer{
			Mid:        pm.Mid,
			Kind:       pm.Kind,
			Direction:  pm.Direction,
			RtpParams:  pm.RtpParameters,
			OfferPTs:   pm.OfferPTs,
			OfferProto: pm.OfferPortProto,
			Msid:       pm.Msid,
		})
	}
	answer, err := BuildAnswer(&AnswerParams{
		IceParameters:  &data.IceParameters,
		IceCandidates:  data.IceCandidates,
		DtlsParameters: &data.DtlsParameters,
		DtlsSetup:      answerSetup,
		Medias:         medias,
	})
	if err != nil {
		closeOnErr()
		return nil, fmt.Errorf("build answer: %w", err)
	}

	s.Logger.Info("WHIP published", "producers", len(producers), "dtlsSetup", answerSetup)

	return &CreateWHIPResult{
		Transport: transport,
		Router:    router,
		Producers: producers,
		Answer:    answer,
		ETag:      data.IceParameters.UsernameFragment,
	}, nil
}

// CreateWHEPOptions packages the inputs for a new WHEP subscribe session.
type CreateWHEPOptions struct {
	OfferSDP  string
	Producers []*mediasoup.Producer
	// ResourceID, when non-empty, is used verbatim as the session id.
	// This lets the HTTP layer encode routing info (e.g. owner NodeID)
	// into the id for distributed discovery. If empty, a random xid is
	// generated by the registry.
	ResourceID string
	// Keepalive, if non-nil, is invoked on every supervisor tick so the
	// HTTP layer can refresh external state (e.g. a "WHEP_LOC" KV entry
	// used for cross-node PATCH/DELETE routing).
	Keepalive func()
	// ResolveProducerID, when non-nil, is called once per consumed
	// producer just before Transport.Consume, with the destination router
	// chosen by the factory. Implementations return the producer id that
	// is reachable from dest — typically by invoking PipeToRouter when
	// the source router differs. When nil the service falls back to
	// producer.Id() verbatim (which only works when all producers are
	// co-located with the destination router — true of single-worker
	// deployments but not of the default multi-worker setup).
	//
	// Kind() matching against the SDP offer continues to use the original
	// producer handle, because kind is invariant across PipeToRouter.
	ResolveProducerID func(dest *mediasoup.Router, producer *mediasoup.Producer) (string, error)
}

// CreateWHEPResult is the data needed to respond to a WHEP POST.
type CreateWHEPResult struct {
	Session *Session
	Answer  string
	ETag    string
}

// CreateWHEP creates a new subscribe session. The caller supplies one or
// more Producers to consume; if the offer's m-section count doesn't match,
// the extra producers are ignored and extra m-sections become inactive.
func (s *Service) CreateWHEP(opts CreateWHEPOptions) (*CreateWHEPResult, error) {
	if s.Factory == nil {
		return nil, fmt.Errorf("no transport factory configured")
	}
	producers := opts.Producers
	if len(producers) == 0 {
		return nil, fmt.Errorf("no producers to consume")
	}

	transport, router, err := s.Factory.New(ModeWHEP)
	if err != nil {
		return nil, fmt.Errorf("factory: %w", err)
	}

	caps := router.RtpCapabilities()
	parsed, err := ParseOffer(opts.OfferSDP, caps)
	if err != nil {
		transport.Close()
		return nil, fmt.Errorf("parse offer: %w", err)
	}
	answerSetup, remoteDtlsRole := negotiateDtlsRole(parsed.DtlsSetup)
	if err := transport.Connect(&mediasoup.TransportConnectOptions{
		DtlsParameters: &mediasoup.DtlsParameters{
			Role:         remoteDtlsRole,
			Fingerprints: []mediasoup.DtlsFingerprint{parsed.DtlsFingerprint},
		},
	}); err != nil {
		transport.Close()
		return nil, fmt.Errorf("transport connect: %w", err)
	}

	sess := &Session{
		ID:                     opts.ResourceID,
		Transport:              transport,
		ICEDisconnectedTimeout: s.ICEDisconnectedTimeout,
		Keepalive:              opts.Keepalive,
		logger:                 s.Logger,
	}

	// Match producers to m-sections by kind order.
	remaining := append([]*mediasoup.Producer{}, producers...)
	medias := make([]*MediaAnswer, 0, len(parsed.Medias))
	for _, pm := range parsed.Medias {
		if pm.Direction != mediasoup.MediaDirectionRecvonly && pm.Direction != mediasoup.MediaDirectionSendrecv {
			medias = append(medias, blankInactive(pm))
			continue
		}
		idx := pickProducer(remaining, pm.Kind)
		if idx < 0 {
			medias = append(medias, blankInactive(pm))
			continue
		}
		producer := remaining[idx]
		remaining = append(remaining[:idx], remaining[idx+1:]...)

		// Producers are Router-scoped: if the factory handed us a
		// transport on a router other than the producer's, the raw
		// producer.Id() won't resolve inside Transport.Consume. The
		// caller (typically the HTTP layer, which owns cross-router
		// piping via rtc.RouterPipe) translates the id up-front.
		producerID := producer.Id()
		if opts.ResolveProducerID != nil {
			resolved, err := opts.ResolveProducerID(router, producer)
			if err != nil {
				sess.Close()
				return nil, fmt.Errorf("resolve producer=%q mid=%q: %w", producer.Id(), pm.Mid, err)
			}
			producerID = resolved
		}

		consumer, err := transport.Consume(&mediasoup.ConsumerOptions{
			ProducerId:      producerID,
			RtpCapabilities: chooseConsumerCaps(pm.RtpParameters, caps),
			Paused:          false,
			Mid:             pm.Mid,
		})
		if err != nil {
			sess.Close()
			return nil, fmt.Errorf("consume producer=%q mid=%q: %w", producerID, pm.Mid, err)
		}
		sess.Consumers = append(sess.Consumers, consumer)
		// Proactively close the session once every consumer has seen its
		// upstream producer go away, so we don't keep a dead WHEP resource
		// alive for the full ICE watchdog window.
		consumer.OnProducerClose(sess.HandleConsumerProducerClosed)

		ma := &MediaAnswer{
			Mid:        pm.Mid,
			Kind:       pm.Kind,
			Direction:  pm.Direction,
			RtpParams:  consumer.RtpParameters(),
			OfferPTs:   pm.OfferPTs,
			OfferProto: pm.OfferPortProto,
			Msid:       pm.Msid,
		}
		if enc := ma.RtpParams.Encodings; len(enc) > 0 {
			ma.ServerSsrc = enc[0].Ssrc
			if enc[0].Rtx != nil {
				ma.ServerRtxSsrc = enc[0].Rtx.Ssrc
			}
		}
		if ma.ServerSsrc == 0 {
			ma.ServerSsrc = rand.Uint32N(0x7FFFFFFE) + 1
		}
		if ma.RtpParams.Rtcp != nil {
			ma.ServerCname = ma.RtpParams.Rtcp.Cname
		}
		medias = append(medias, ma)
	}

	td := transport.Data()
	data := td.WebRtcTransportData

	answer, err := BuildAnswer(&AnswerParams{
		IceParameters:  &data.IceParameters,
		IceCandidates:  data.IceCandidates,
		DtlsParameters: &data.DtlsParameters,
		DtlsSetup:      answerSetup,
		Medias:         medias,
	})
	if err != nil {
		sess.Close()
		return nil, fmt.Errorf("build answer: %w", err)
	}

	sess.ETag = data.IceParameters.UsernameFragment
	id, err := s.Whep.Add(sess)
	if err != nil {
		sess.Close()
		return nil, err
	}

	sess.Start(func(sessionID string) {
		if err := s.Whep.Remove(sessionID); err != nil && err != ErrSessionNotFound {
			s.Logger.Warn("supervisor remove failed", "sessionId", sessionID, "err", err)
		}
	})

	s.Logger.Info("WHEP session created",
		"sessionId", id, "consumers", len(sess.Consumers))

	return &CreateWHEPResult{Session: sess, Answer: answer, ETag: sess.ETag}, nil
}

// PatchWHEP performs an ICE restart (if the client sent `If-Match: *`) or a
// trickle ICE update on a WHEP session. Returns the new ETag (IceUfrag)
// when an ICE restart happened.
//
// WHIP PATCH is intentionally not handled here — it operates on the
// publisher's participant transport (owned by rtc.Participant), so the
// HTTP layer drives it directly.
func (s *Service) PatchWHEP(sessionID, ifMatch, sdpFragment string) (string, error) {
	sess := s.Whep.Get(sessionID)
	if sess == nil {
		return "", ErrSessionNotFound
	}

	if ifMatch == "*" {
		iceParams, err := sess.Transport.RestartIce()
		if err != nil {
			return "", fmt.Errorf("ice restart: %w", err)
		}
		sess.mu.Lock()
		sess.ETag = iceParams.UsernameFragment
		sess.mu.Unlock()
		// Client successfully reconnected ICE; clear the disconnect
		// marker so the supervisor doesn't tear down a healthy session
		// on the next tick.
		sess.ICERecovered()
		return sess.ETag, nil
	}

	// Non-restart PATCH is trickle-ICE — we accept and ignore for now since
	// mediasoup WebRtcTransport ingests candidates from the offer/answer
	// exchange and does not need per-candidate push. Returning empty string
	// keeps the existing ETag.
	_ = sdpFragment
	return sess.ETag, nil
}

// DeleteWHEP closes a WHEP session and removes it from the registry.
// WHIP DELETE goes through the participant lifecycle (see server layer).
func (s *Service) DeleteWHEP(sessionID string) error {
	return s.Whep.Remove(sessionID)
}

// blankInactive builds an inactive placeholder m-section, reusing the offer's
// codec parameters so the answer layout matches. Without this helper, a
// client with audio-only WHEP against a video-only room would crash.
func blankInactive(pm *ParsedMedia) *MediaAnswer {
	return &MediaAnswer{
		Mid:        pm.Mid,
		Kind:       pm.Kind,
		Direction:  mediasoup.MediaDirectionInactive,
		RtpParams:  pm.RtpParameters,
		OfferPTs:   pm.OfferPTs,
		OfferProto: pm.OfferPortProto,
	}
}

// pickProducer returns the index of the first producer whose Kind matches.
func pickProducer(producers []*mediasoup.Producer, kind mediasoup.MediaKind) int {
	for i, p := range producers {
		if p.Kind() == kind {
			return i
		}
	}
	return -1
}

// chooseConsumerCaps constructs the minimum RtpCapabilities we need to pass
// to Consume so mediasoup does codec matching for us. We reuse the offer's
// codecs because that represents the client's capabilities. Missing fields
// fall back to the router's full capabilities.
func chooseConsumerCaps(offerParams *mediasoup.RtpParameters, routerCaps *mediasoup.RtpCapabilities) *mediasoup.RtpCapabilities {
	if offerParams == nil || len(offerParams.Codecs) == 0 {
		return routerCaps
	}
	caps := &mediasoup.RtpCapabilities{
		HeaderExtensions: routerCaps.HeaderExtensions,
	}
	for _, c := range offerParams.Codecs {
		caps.Codecs = append(caps.Codecs, &mediasoup.RtpCodecCapability{
			Kind:                 mediasoup.MediaKind(splitKindFromMime(c.MimeType)),
			MimeType:             c.MimeType,
			PreferredPayloadType: c.PayloadType,
			ClockRate:            c.ClockRate,
			Channels:             c.Channels,
			Parameters:           c.Parameters,
			RtcpFeedback:         c.RtcpFeedback,
		})
	}
	return caps
}

func splitKindFromMime(mime string) string {
	if i := indexByte(mime, '/'); i > 0 {
		return mime[:i]
	}
	return ""
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// negotiateDtlsRole derives the local DTLS setup (for the `a=setup:` line in
// the answer) and the remote peer's DTLS role (for mediasoup's
// TransportConnectOptions) from the offer's `a=setup` attribute.
//
// RFC 5763 §5: only one side can be the DTLS client. The offerer proposes a
// role, the answerer picks the complement:
//
//	offer=active   → remote is DTLS client → answer setup:passive, role=client
//	offer=passive  → remote is DTLS server → answer setup:active,  role=server
//	offer=actpass  → we choose; prefer passive for parity with historical
//	                 behaviour (works with all browsers, OBS, GStreamer).
//	offer=""       → treated as actpass; some minimal WHIP clients omit setup.
//
// Keeping the answer's `a=setup` and mediasoup's `DtlsRole` in lockstep is
// critical: a mismatch makes mediasoup silently act opposite to what the peer
// expects, manifesting as a 5s DTLS handshake timeout (typical ffmpeg
// symptom: `DTLS handshake timeout=5000ms ... state=6`).
func negotiateDtlsRole(offerSetup string) (answerSetup string, remoteRole mediasoup.DtlsRole) {
	switch offerSetup {
	case "active":
		return "passive", mediasoup.DtlsRoleClient
	case "passive":
		return "active", mediasoup.DtlsRoleServer
	default: // "actpass" or unspecified
		return "passive", mediasoup.DtlsRoleClient
	}
}
