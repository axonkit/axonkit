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
// during a WHEP session. Implementations are expected to cover both
// "producers from a WHIP resourceId" (standalone subscribe) and "producers
// from a given participant in a room" (in-room subscribe without joining).
type ProducerSource interface {
	// ProducersForResource returns the Producers published by a WHIP
	// standalone session identified by resourceID. An empty slice means
	// "not found / no media"; the caller will then answer with inactive
	// m-sections (or reject the request at the HTTP layer).
	ProducersForResource(resourceID string) []*mediasoup.Producer
	// ProducersForParticipant returns the Producers of a specific
	// participant (by identity) inside a named room. At most one audio
	// and one video Producer are returned — the caller is expected to
	// apply source-priority selection (e.g. screenShare > camera) before
	// returning.
	ProducersForParticipant(roomName, participantIdentity string) []*mediasoup.Producer
}

// Service is the stateful orchestrator for WHIP/WHEP sessions. It holds the
// transport factory, maintains separate session registries for WHIP and
// WHEP, and exposes high-level Create / Patch / Delete entry points the
// HTTP layer calls.
type Service struct {
	Factory TransportFactory
	Sources ProducerSource
	// Whip and Whep are intentionally separate registries: a WHIP
	// resourceId derived from the publisher's identity must not collide
	// with a WHEP resourceId (which is arbitrary), and tearing down a
	// WHEP session must never accidentally close its underlying WHIP
	// publish session.
	Whip   *Registry
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
		Whip:    NewRegistry(ModeWHIP),
		Whep:    NewRegistry(ModeWHEP),
		Logger:  logger,
	}
}

// CreateWHIPOptions holds the inputs for a new WHIP publish session.
type CreateWHIPOptions struct {
	OfferSDP string
	// ResourceID is the desired resource id for this WHIP session — the
	// HTTP layer typically sets it to the JWT Identity so that a given
	// publisher only holds one standalone WHIP resource at a time.
	// If empty, a random id is generated.
	ResourceID string

	// ReplaceExisting, if true, forces eviction of any session already
	// holding the same ResourceID before installing the new one. Used for
	// "same identity publishes again" — the old session is closed and the
	// new one takes its place instead of returning an error.
	ReplaceExisting bool

	// Keepalive, if set, is stored on the Session and invoked by the
	// supervisor on every tick (including immediately after start). Use
	// this to refresh KV TTLs for distributed discovery.
	Keepalive func()
}

// CreateWHIPResult is the data needed to respond to a WHIP POST: the
// resource id (becomes /whip/{id}), ETag value and the answer SDP.
type CreateWHIPResult struct {
	Session *Session
	Answer  string
	ETag    string
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
	closeOnErr := transport

	caps := router.RtpCapabilities()
	parsed, err := ParseOffer(opts.OfferSDP, caps)
	if err != nil {
		closeOnErr.Close()
		return nil, fmt.Errorf("parse offer: %w", err)
	}

	if err := transport.Connect(&mediasoup.TransportConnectOptions{
		DtlsParameters: &mediasoup.DtlsParameters{
			Role:         mediasoup.DtlsRoleClient, // client side of DTLS
			Fingerprints: []mediasoup.DtlsFingerprint{parsed.DtlsFingerprint},
		},
	}); err != nil {
		closeOnErr.Close()
		return nil, fmt.Errorf("transport connect: %w", err)
	}

	sess := &Session{
		ID:                     opts.ResourceID,
		Mode:                   ModeWHIP,
		Transport:              transport,
		Router:                 router,
		ICEDisconnectedTimeout: s.ICEDisconnectedTimeout,
		Keepalive:              opts.Keepalive,
		logger:                 s.Logger,
	}

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
			sess.Close()
			return nil, fmt.Errorf("produce %s mid=%q: %w", pm.Kind, pm.Mid, err)
		}
		sess.Producers = append(sess.Producers, producer)
	}
	if len(sess.Producers) == 0 {
		sess.Close()
		return nil, fmt.Errorf("offer has no publishable media")
	}

	// Build the answer.
	td := transport.Data()
	if td.WebRtcTransportData == nil {
		sess.Close()
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
		DtlsSetup:      "passive", // server is DTLS server in WHIP
		Medias:         medias,
	})
	if err != nil {
		sess.Close()
		return nil, fmt.Errorf("build answer: %w", err)
	}

	sess.ETag = data.IceParameters.UsernameFragment

	// If the client requested "replace existing", evict any prior session
	// under the same resource id before inserting. This is the preferred
	// behaviour for WHIP when the resource id is derived from the
	// publisher's JWT identity: a reconnecting client that forgot to
	// DELETE the old resource (crash, network loss, …) would otherwise be
	// permanently blocked by an already-expired-but-not-yet-reaped
	// session.
	if opts.ReplaceExisting && sess.ID != "" {
		if old := s.Whip.Get(sess.ID); old != nil {
			_ = s.Whip.Remove(sess.ID)
			s.Logger.Info("WHIP resource replaced (same identity reconnected)",
				"sessionId", sess.ID, "oldCreatedAt", old.CreatedAt)
		}
	}

	id, err := s.Whip.Add(sess)
	if err != nil {
		sess.Close()
		return nil, err
	}

	// Start the supervisor: single goroutine that drives (a) ICE disconnect
	// watchdog + (b) external keepalive (KV TTL refresh). onExpire closes
	// through the registry so the map stays consistent.
	sess.Start(func(sessionID string) {
		if err := s.Whip.Remove(sessionID); err != nil && err != ErrSessionNotFound {
			s.Logger.Warn("supervisor remove failed", "sessionId", sessionID, "err", err)
		}
	})

	s.Logger.Info("WHIP session created",
		"sessionId", id, "producers", len(sess.Producers))

	return &CreateWHIPResult{Session: sess, Answer: answer, ETag: sess.ETag}, nil
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
func (s *Service) CreateWHEP(offerSDP string, producers []*mediasoup.Producer) (*CreateWHEPResult, error) {
	if s.Factory == nil {
		return nil, fmt.Errorf("no transport factory configured")
	}
	if len(producers) == 0 {
		return nil, fmt.Errorf("no producers to consume")
	}

	transport, router, err := s.Factory.New(ModeWHEP)
	if err != nil {
		return nil, fmt.Errorf("factory: %w", err)
	}

	caps := router.RtpCapabilities()
	parsed, err := ParseOffer(offerSDP, caps)
	if err != nil {
		transport.Close()
		return nil, fmt.Errorf("parse offer: %w", err)
	}
	if err := transport.Connect(&mediasoup.TransportConnectOptions{
		DtlsParameters: &mediasoup.DtlsParameters{
			Role:         mediasoup.DtlsRoleClient,
			Fingerprints: []mediasoup.DtlsFingerprint{parsed.DtlsFingerprint},
		},
	}); err != nil {
		transport.Close()
		return nil, fmt.Errorf("transport connect: %w", err)
	}

	sess := &Session{
		Mode:                   ModeWHEP,
		Transport:              transport,
		Router:                 router,
		ICEDisconnectedTimeout: s.ICEDisconnectedTimeout,
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

		consumer, err := transport.Consume(&mediasoup.ConsumerOptions{
			ProducerId:      producer.Id(),
			RtpCapabilities: chooseConsumerCaps(pm.RtpParameters, caps),
			Paused:          false,
			Mid:             pm.Mid,
		})
		if err != nil {
			sess.Close()
			return nil, fmt.Errorf("consume producer=%q mid=%q: %w", producer.Id(), pm.Mid, err)
		}
		sess.Consumers = append(sess.Consumers, consumer)

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
		DtlsSetup:      "passive",
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

// Patch performs an ICE restart (if the client sent `If-Match: *`) or a
// trickle ICE update. Returns the new ETag (IceUfrag) when restart happened.
// mode selects which registry to consult — WHIP and WHEP resource IDs do
// not overlap.
func (s *Service) Patch(mode Mode, sessionID, ifMatch, sdpFragment string) (string, error) {
	sess := s.lookup(mode, sessionID)
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

// Delete closes a session and removes it from the registry.
func (s *Service) Delete(mode Mode, sessionID string) error {
	switch mode {
	case ModeWHIP:
		return s.Whip.Remove(sessionID)
	case ModeWHEP:
		return s.Whep.Remove(sessionID)
	}
	return fmt.Errorf("unknown mode %q", mode)
}

// lookup resolves a session in the registry for the given mode.
func (s *Service) lookup(mode Mode, sessionID string) *Session {
	switch mode {
	case ModeWHIP:
		return s.Whip.Get(sessionID)
	case ModeWHEP:
		return s.Whep.Get(sessionID)
	}
	return nil
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
