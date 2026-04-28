package broadcast

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/axonkit/axonkit/pkg/rtc"
	"github.com/axonkit/axonkit/pkg/rtc/whip"
	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/pion/rtp"
)

// RoomLookup is the minimal surface Broadcast needs from the room
// registry: find a room by name, then find a participant by identity.
// The concrete *rtc.RoomManager satisfies it (GetRoom → *rtc.Room which
// has GetParticipantByIdentity). Kept as an interface so tests can stub
// without spinning up mediasoup routers.
type RoomLookup interface {
	GetRoom(name string) *rtc.Room
}

// Params is the constructor input for a new Broadcast. Assembled by
// Manager.Create from the HTTP request; passed through here mainly so
// unit tests can build a Broadcast without going through the manager.
type Params struct {
	// ID is the broadcast's public identifier ("bc_" + xid). Also used
	// as the resource id for PATCH/DELETE /rtc/broadcast/{id}/whip.
	ID string
	// Room is the source room name; the bound live identity is
	// resolved inside it.
	Room string
	// LiveIdentity is the room participant whose producers are pushed
	// as the "live" source. Must be currently publishing at creation
	// time — Manager.Create returns ErrIdentityNotPublishing otherwise.
	// If the participant's producers later close, the whole broadcast
	// is torn down with reason TerminationLiveIdentityGone.
	LiveIdentity string
	// Destinations is the fanout list (1..MaxDestinations). Already
	// validated by Validate() before reaching Params.
	Destinations []Destination
	// Rooms gives us read-only access to live participants so we can
	// find the bound producer (and react to its close).
	Rooms RoomLookup
	// WhipService is used to materialize the mix ingest transport +
	// producers on POST /rtc/broadcast/{id}/whip. Optional — when
	// nil the mix endpoints return 501, the live path still works.
	WhipService *whip.Service
	// Logger is scoped to this broadcast (component=broadcast, id=...).
	Logger *slog.Logger
}

// Status is the JSON shape returned by GET /rtc/broadcast/{id}.
type Status struct {
	ID               string              `json:"id"`
	Room             string              `json:"room"`
	LiveIdentity     string              `json:"liveIdentity"`
	Source           SourceKind          `json:"source"`
	MixConnected     bool                `json:"mixConnected"`
	MixIceState      string              `json:"mixIceState,omitempty"`
	Destinations     []DestinationStatus `json:"destinations"`
	Terminated       bool                `json:"terminated"`
	TerminatedReason TerminationReason   `json:"terminatedReason,omitempty"`
	TerminatedAt     time.Time           `json:"terminatedAt,omitempty"`
	CreatedAt        time.Time           `json:"createdAt"`
	MixWhipPath      string              `json:"mixWhipPath"`
}

// DestinationStatus is one entry in Status.Destinations. Byte counters
// are best-effort (atomic counters inside the pusher) and may slightly
// trail reality under contention.
type DestinationStatus struct {
	Type      DestinationType `json:"type"`
	URL       string          `json:"url"`
	Connected bool            `json:"connected"`
	BytesOut  uint64          `json:"bytesOut"`
	LastError string          `json:"lastError,omitempty"`
}

// mixIngress abstracts the two implementations of the mix-source side
// of a Broadcast: the standard mediasoup-routed WHIP transport
// (mixSource) and the pion-terminated H.265 bypass (h265MixSource).
// Broadcast holds whichever one was created at AttachMix time and only
// ever speaks to it through this surface.
//
// Method semantics:
//   - Close: tear down all owned plumbing (transports, pion PC, etc.).
//     Idempotent. Must be safe to call concurrently with the watchdog
//     goroutine that may already be firing OnGone.
//   - VideoDepack: returns the video depacketizer the switcher consults
//     to fetch the codec configuration record (avcC / vpcC / av1C /
//     hvcC) when synthesizing RTMP SeqHeader on every flip.
//   - RestartIce: optional ICE restart triggered by PATCH /whip with
//     If-Match: *. Implementations that do not support restart return
//     a non-nil error — the HTTP layer turns that into a 4xx.
type mixIngress interface {
	Close()
	VideoDepack() videoDepacketizer
	RestartIce() (etag string, err error)
}

// Broadcast is the runtime object for one "push room user → N sinks"
// task. A Broadcast owns a mediasoup Router-dedicated DirectTransport
// pair (one for live, one for mix — created lazily in later milestones
// when those sources are actually wired up) and an optional mix WHIP
// session.
//
// Concurrency: all mutable fields are guarded by mu. Hot-path RTP
// forwarding goes through the Switcher (next milestone) which owns its
// own locking; the Broadcast-level mu only protects control-plane
// state transitions (start/terminate/mix-connect/mix-disconnect).
type Broadcast struct {
	id           string
	room         string
	liveIdentity string
	destinations []Destination
	rooms        RoomLookup
	whipSvc      *whip.Service
	logger       *slog.Logger

	createdAt time.Time

	mu           sync.RWMutex
	mixConnected bool
	// mixIceState is the human-readable ICE state of the active mix
	// ingress. Both the mediasoup path (mediasoup.IceState values:
	// "connected" / "completed" / "disconnected" / "closed") and the
	// pion bypass (mirrored to the same vocabulary in h265MixSource)
	// write here, so consumers of Status() never need to know which
	// implementation is running.
	mixIceState      string
	mixETag          string
	terminated       bool
	terminatedReason TerminationReason
	terminatedAt     time.Time
	// onTerminate, when set, is invoked once from inside terminate()
	// after the broadcast has been marked terminated but while still
	// holding mu, so the Manager can drop the id from its registry
	// atomically with the state flip. Safe to be nil.
	onTerminate func(reason TerminationReason)

	// Runtime plumbing attached by Start. Owned entirely by this
	// Broadcast; Terminate tears every field down in order
	// (pushers → live source → mix source / transport).
	ctx      context.Context
	cancel   context.CancelFunc
	live     *liveSource
	mix      mixIngress
	pushers  []destinationPusher
	switcher *switcher
}

// New constructs a Broadcast but does NOT start any goroutines or
// mediasoup plumbing. Use Manager.Create to go through the full
// lifecycle (validation + producer wiring + Start).
func New(p Params) *Broadcast {
	logger := p.Logger
	if logger == nil {
		logger = slog.Default().With("component", "broadcast", "id", p.ID)
	}
	return &Broadcast{
		id:           p.ID,
		room:         p.Room,
		liveIdentity: p.LiveIdentity,
		destinations: p.Destinations,
		rooms:        p.Rooms,
		whipSvc:      p.WhipService,
		logger:       logger,
		createdAt:    time.Now(),
	}
}

// ID returns the broadcast's public identifier.
func (b *Broadcast) ID() string { return b.id }

// Room returns the bound source room name.
func (b *Broadcast) Room() string { return b.room }

// LiveIdentity returns the bound room participant's identity.
func (b *Broadcast) LiveIdentity() string { return b.liveIdentity }

// MixWhipPath returns the HTTP path a WHIP client (avflow) should POST
// to in order to supply the mix source. Also used as the Location
// header returned from that POST.
func (b *Broadcast) MixWhipPath() string {
	return fmt.Sprintf("/rtc/broadcast/%s/whip", b.id)
}

// Terminated reports whether terminate() has already run. Callers
// should not invoke state-changing operations once this is true.
func (b *Broadcast) Terminated() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.terminated
}

// Status snapshots the broadcast into a JSON-safe struct.
func (b *Broadcast) Status() Status {
	b.mu.RLock()
	defer b.mu.RUnlock()
	dsts := make([]DestinationStatus, len(b.destinations))
	for i, d := range b.destinations {
		dst := DestinationStatus{Type: d.Type, URL: d.URL}
		if i < len(b.pushers) && b.pushers[i] != nil {
			dst.Connected = b.pushers[i].Connected()
			dst.BytesOut = b.pushers[i].BytesOut()
			dst.LastError = b.pushers[i].LastError()
		}
		dsts[i] = dst
	}
	source := SourceLive
	if b.switcher != nil {
		source = b.switcher.activeSource()
	}
	st := Status{
		ID:               b.id,
		Room:             b.room,
		LiveIdentity:     b.liveIdentity,
		Source:           source,
		MixConnected:     b.mixConnected,
		MixIceState:      b.mixIceState,
		Destinations:     dsts,
		Terminated:       b.terminated,
		TerminatedReason: b.terminatedReason,
		TerminatedAt:     b.terminatedAt,
		CreatedAt:        b.createdAt,
		MixWhipPath:      b.MixWhipPath(),
	}
	return st
}

// Start validates the live identity is currently publishing and
// prepares the broadcast for forwarding. It creates the outbound
// pushers first (so connection errors fail the POST), then wires the
// live source's DirectTransport + Consumers and starts dispatching
// RTP. The mix path is wired separately via POST /rtc/broadcast/{id}/whip
// in a later milestone.
//
// Errors:
//   - ErrRoomNotFound:          room does not exist on this node
//   - ErrIdentityNotPublishing: identity has no live producer yet
//   - ErrUnsupportedCodec:      producer uses a codec outside H264/Opus
//   - misc wrapped errors:      mediasoup Consume / URL parse failures
func (b *Broadcast) Start() error {
	room := b.rooms.GetRoom(b.room)
	if room == nil {
		return ErrRoomNotFound
	}
	participant := room.GetParticipantByIdentity(b.liveIdentity)
	if participant == nil || len(participant.LiveTracks()) == 0 {
		return ErrIdentityNotPublishing
	}

	audio, video, videoCodec, err := pickLiveProducers(participant)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrIdentityNotPublishing, err)
	}

	if err := validateDestinationsForCodec(b.destinations, videoCodec); err != nil {
		return err
	}

	pushers, err := b.buildPushers()
	if err != nil {
		return err
	}

	b.ctx, b.cancel = context.WithCancel(context.Background())
	for _, p := range pushers {
		p.Start(b.ctx)
	}

	b.pushers = pushers

	// Switcher owns the live/mix fan-in. It is created in live-active
	// state with empty extradata hooks; bindLive / bindMix wire those
	// up once the corresponding source exists.
	b.switcher = newSwitcher(b.logger.With("component", "switcher"))
	b.switcher.bindEmitters(b.fanOutSample, b.fanOutPacket)
	b.switcher.setTransitionHooks(
		func() { b.onFlipToLive() },
		func() { b.onFlipToMix() },
	)

	liveSampleSink := sampleSink(func(s MediaSample) {
		b.switcher.feedSample(SourceLive, s)
	})
	livePacketSink := packetSink(func(kind SampleKind, pkt *rtp.Packet) {
		b.switcher.feedPacket(SourceLive, kind, pkt)
	})

	var live *liveSource
	live, err = newLiveSource(liveSourceParams{
		Participant:    participant,
		AudioProducer:  audio,
		VideoProducer:  video,
		VideoCodec:     videoCodec,
		Sink:           liveSampleSink,
		PacketSink:     livePacketSink,
		OnProducerGone: func() { b.terminate(TerminationLiveIdentityGone) },
		Logger:         b.logger.With("source", "live"),
	})
	if err != nil {
		for _, p := range pushers {
			p.Stop()
		}
		b.cancel()
		return fmt.Errorf("live source: %w", err)
	}
	b.live = live
	b.switcher.bindLive(func() []byte { return live.videoDepack.ConfigRecord() })
	live.Resume()

	b.logger.Info("broadcast started",
		"room", b.room,
		"liveIdentity", b.liveIdentity,
		"videoCodec", videoCodec,
		"destinations", len(b.destinations),
	)
	return nil
}

// buildPushers turns the validated destination list into a slice of
// pushers. Both RTMP and WHIP pushers are created eagerly (URL parsed
// now, connection in background); so any URL-level error surfaces to
// the POST response rather than spinning up a broadcast that never
// sends a byte.
func (b *Broadcast) buildPushers() ([]destinationPusher, error) {
	pushers := make([]destinationPusher, 0, len(b.destinations))
	for _, d := range b.destinations {
		var (
			pp  destinationPusher
			err error
		)
		switch d.Type {
		case DestinationRTMP:
			var rp *rtmpPusher
			rp, err = newRTMPPusher(d.URL, b.logger.With("dst", d.URL), rtmpPusherOptions{
				DTSReorderMode: d.DTSReorderMode(),
			})
			if err == nil {
				pp = &rtmpPusherAdapter{inner: rp}
			}
		case DestinationWHIP:
			var wp *whipPusher
			wp, err = newWHIPPusher(d.URL, b.logger.With("dst", d.URL))
			if err == nil {
				pp = &whipPusherAdapter{inner: wp}
			}
		default:
			err = fmt.Errorf("unknown destination type %q", d.Type)
		}
		if err != nil {
			for _, existing := range pushers {
				existing.Stop()
			}
			return nil, fmt.Errorf("destination %q: %w", d.URL, err)
		}
		pushers = append(pushers, pp)
	}
	return pushers, nil
}

// fanOutSample forwards one depacketized sample to every pusher.
// RTMP pushers use the sample; WHIP pushers ignore it (they only care
// about the raw RTP stream delivered via fanOutPacket). Extradata is
// passed through from the switcher (which knows which source is
// active).
func (b *Broadcast) fanOutSample(s MediaSample, extradata []byte) {
	if b.Terminated() {
		return
	}
	b.mu.RLock()
	pushers := b.pushers
	b.mu.RUnlock()
	if len(pushers) == 0 {
		return
	}
	for _, p := range pushers {
		p.WriteSample(s, extradata)
	}
}

// fanOutPacket forwards one raw RTP packet to every pusher. Only WHIP
// pushers consume it; RTMP pushers are decoupled from RTP entirely.
func (b *Broadcast) fanOutPacket(kind SampleKind, pkt *rtp.Packet) {
	if b.Terminated() {
		return
	}
	b.mu.RLock()
	pushers := b.pushers
	b.mu.RUnlock()
	for _, p := range pushers {
		p.WriteRTP(kind, pkt)
	}
}

// onFlipToLive is the switcher's "we are back on live" hook. Drain
// the trailing window of mix's frames out of every pusher's DTS-
// reorder buffer first, then reset each pusher's video seq header
// so the new source's SPS/PPS land ahead of its first IDR, and ask
// the live consumer for a fresh IDR so the wait-for-keyframe window
// on the output side is short. The drain-before-reset order matters:
// the buffered mix frames must reach the FLV ahead of live's
// SeqHeader tag — otherwise the receiver applies live's avcC to
// trailing mix frames whose codec config is different and the
// recording develops the same "reference count overflow" cascade as
// the mix-flip path described on onFlipToMix.
func (b *Broadcast) onFlipToLive() {
	b.mu.RLock()
	pushers := b.pushers
	live := b.live
	b.mu.RUnlock()
	for _, p := range pushers {
		p.FlushVideoReorder()
		p.ResetVideoHeader()
	}
	if live != nil {
		live.RequestKeyFrame()
	}
}

// onFlipToMix is the switcher's "we are now on mix" hook. Drain the
// trailing window of live's frames out of every pusher's DTS-reorder
// buffer FIRST so they reach the FLV ahead of mix's SeqHeader tag,
// then reset each pusher's video seq header so mix's SPS/PPS go out
// before its first keyframe. Without the drain step the SeqHeader
// (mix avcC, e.g. baseline profile + max_num_ref_frames=1) lands in
// the FLV file ahead of 1–4 trailing high-profile B-frames from the
// outgoing live source — the receiver re-configures its decoder for
// the new avcC and then immediately tries to decode those B-frames
// against it, producing the "reference count overflow" /
// "modification_of_pic_nums_idc out of range" / "corrupted
// macroblock" cascade verify-rtmp.sh's strict gate caught.
//
// The switcher itself gated the flip on an already-arrived mix
// keyframe so no extra nudge is needed on the mix side.
func (b *Broadcast) onFlipToMix() {
	b.mu.RLock()
	pushers := b.pushers
	b.mu.RUnlock()
	for _, p := range pushers {
		p.FlushVideoReorder()
		p.ResetVideoHeader()
	}
}

// setOnTerminate is called by Manager right after New so that terminate
// can atomically drop the broadcast from the registry.
func (b *Broadcast) setOnTerminate(cb func(reason TerminationReason)) {
	b.mu.Lock()
	b.onTerminate = cb
	b.mu.Unlock()
}

// Terminate is the public entry for DELETE /rtc/broadcast/{id}.
func (b *Broadcast) Terminate() {
	b.terminate(TerminationDeleted)
}

// terminate is the single "stop everything" path. Idempotent: a
// second call is a no-op. Ordering mirrors existing WHIP teardown:
// stop the source (so no more samples arrive), then the pushers (so
// downstream sees a clean EOS rather than a half-written tag), then
// drop the broadcast from the manager registry via the onTerminate
// callback. Mix ingest (milestone 4) is torn down before the live
// source so avflow receives a clean WHEP DELETE too.
func (b *Broadcast) terminate(reason TerminationReason) {
	b.mu.Lock()
	if b.terminated {
		b.mu.Unlock()
		return
	}
	b.terminated = true
	b.terminatedReason = reason
	b.terminatedAt = time.Now()
	cb := b.onTerminate
	cancel := b.cancel
	live := b.live
	mix := b.mix
	sw := b.switcher
	pushers := b.pushers
	b.pushers = nil
	b.live = nil
	b.mix = nil
	b.mu.Unlock()

	b.logger.Info("broadcast terminating", "reason", reason)

	if sw != nil {
		sw.terminate()
	}
	if mix != nil {
		mix.Close()
	}
	if live != nil {
		live.Close()
	}
	for _, p := range pushers {
		p.Stop()
	}
	if cancel != nil {
		cancel()
	}

	if cb != nil {
		cb(reason)
	}
}

// AttachMix is invoked by the HTTP layer on POST /rtc/broadcast/{id}
// /whip with avflow's SDP offer. It creates a mediasoup WHIP
// transport, produces whatever m-sections avflow sent us, and wires
// the resulting producers into the switcher as the "mix" source.
// Returns the WHIP answer SDP + ICE ETag.
//
// Routing: SDP offers whose only m=video codec is H.265 cannot be
// terminated by mediasoup-go (its supported RTP capability table
// omits H.265 by design — see pkg/media/caps.go). For those offers we
// take the "pion bypass" path: a stand-alone webrtc.PeerConnection
// terminates the WHIP session, OnTrack hands raw RTP to the switcher,
// and the existing Switcher/Pusher pipeline pushes it out via E-RTMP
// (`hvc1`) or WHIP egress unchanged. Every other offer (H.264 / VP9 /
// AV1 / VP8, mixed or alone) keeps the original mediasoup route.
//
// Errors:
//   - ErrTerminated:          broadcast is already torn down
//   - ErrMixAlreadyConnected: a mix ingest is already attached
//   - various wrapped errors from whip.Service.CreateWHIP
func (b *Broadcast) AttachMix(offerSDP string) (answer, etag string, err error) {
	b.mu.Lock()
	if b.terminated {
		b.mu.Unlock()
		return "", "", ErrTerminated
	}
	if b.mix != nil {
		b.mu.Unlock()
		return "", "", ErrMixAlreadyConnected
	}
	sw := b.switcher
	b.mu.Unlock()

	if offerNeedsH265Bypass(offerSDP) {
		return b.attachMixH265Bypass(offerSDP, sw)
	}

	b.mu.RLock()
	whipSvc := b.whipSvc
	b.mu.RUnlock()
	if whipSvc == nil {
		return "", "", fmt.Errorf("broadcast: no WHIP service configured")
	}

	result, err := whipSvc.CreateWHIP(whip.CreateWHIPOptions{OfferSDP: offerSDP})
	if err != nil {
		return "", "", fmt.Errorf("mix whip create: %w", err)
	}

	audio, video, perr := pickMixProducers(result.Producers)
	if perr != nil {
		result.Transport.Close()
		return "", "", fmt.Errorf("mix producers: %w", perr)
	}

	mixLogger := b.logger.With("source", "mix")
	ms, err := newMixSource(mixSourceParams{
		Transport:     result.Transport,
		Router:        result.Router,
		AudioProducer: audio,
		VideoProducer: video,
		SampleSink:    func(s MediaSample) { sw.feedSample(SourceMix, s) },
		PacketSink:    func(kind SampleKind, pkt *rtp.Packet) { sw.feedPacket(SourceMix, kind, pkt) },
		OnReady:       func() { b.handleMixReady() },
		OnGone:        func(reason string) { b.handleMixGone(reason) },
		OnIceState:    func(state mediasoup.IceState) { b.setMixIceState(string(state)) },
		Logger:        mixLogger,
	})
	if err != nil {
		result.Transport.Close()
		return "", "", fmt.Errorf("mix source: %w", err)
	}

	b.mu.Lock()
	if b.terminated {
		b.mu.Unlock()
		ms.Close()
		return "", "", ErrTerminated
	}
	b.mix = ms
	b.mixConnected = true
	b.mixETag = result.ETag
	b.mu.Unlock()

	sw.bindMix(func() []byte { return ms.VideoDepack().ConfigRecord() })

	b.logger.Info("mix attached", "etag", result.ETag, "producers", len(result.Producers))
	return result.Answer, result.ETag, nil
}

// attachMixH265Bypass is the H.265 ingress path. avflow (or any WHIP
// publisher whose only m=video codec is H.265) terminates DTLS/SRTP
// directly against a pion PeerConnection that we own here; the
// resulting RTP feeds the Switcher and pushers exactly like the
// mediasoup mix source does, except the entire mediasoup leg is
// skipped. Lifecycle, OnReady, OnGone, ICE-state mirroring and
// flip-to-live behavior all stay identical so neither the switcher
// nor the HTTP layer can tell the two paths apart.
func (b *Broadcast) attachMixH265Bypass(offerSDP string, sw *switcher) (answer, etag string, err error) {
	mixLogger := b.logger.With("source", "mix", "ingress", "pion-h265")
	ms, err := newH265MixSource(h265MixSourceParams{
		OfferSDP:   offerSDP,
		SampleSink: func(s MediaSample) { sw.feedSample(SourceMix, s) },
		PacketSink: func(kind SampleKind, pkt *rtp.Packet) { sw.feedPacket(SourceMix, kind, pkt) },
		OnReady:    func() { b.handleMixReady() },
		OnGone:     func(reason string) { b.handleMixGone(reason) },
		OnIceState: func(state string) { b.setMixIceState(state) },
		Logger:     mixLogger,
	})
	if err != nil {
		return "", "", fmt.Errorf("mix h265 bypass: %w", err)
	}

	b.mu.Lock()
	if b.terminated {
		b.mu.Unlock()
		ms.Close()
		return "", "", ErrTerminated
	}
	b.mix = ms
	b.mixConnected = true
	b.mixETag = ms.ETag()
	b.mu.Unlock()

	sw.bindMix(func() []byte { return ms.VideoDepack().ConfigRecord() })

	b.logger.Info("mix attached (h265 bypass)", "etag", ms.ETag())
	return ms.Answer(), ms.ETag(), nil
}

// DetachMix is the DELETE /rtc/broadcast/{id}/whip fast-path: flip
// back to live immediately and tear the mix ingest down.
func (b *Broadcast) DetachMix() error {
	b.mu.Lock()
	if b.terminated {
		b.mu.Unlock()
		return ErrTerminated
	}
	mix := b.mix
	sw := b.switcher
	b.mix = nil
	b.mixConnected = false
	b.mixETag = ""
	b.mixIceState = ""
	b.mu.Unlock()

	if mix == nil {
		return nil
	}
	if sw != nil {
		sw.flipToLive("mix deleted")
		sw.unbindMix()
	}
	mix.Close()
	b.logger.Info("mix detached (explicit)")
	return nil
}

// PatchMixICE handles PATCH /rtc/broadcast/{id}/whip. Only If-Match: *
// (ICE restart) is supported; trickle-ICE is accepted but ignored.
// Returns the new IceUfrag on restart, or the existing one otherwise.
func (b *Broadcast) PatchMixICE(ifMatch, _ string) (string, error) {
	b.mu.RLock()
	mix := b.mix
	etag := b.mixETag
	b.mu.RUnlock()
	if mix == nil {
		return "", ErrNotFound
	}
	if ifMatch != "*" {
		return etag, nil
	}
	newETag, err := mix.RestartIce()
	if err != nil {
		return "", fmt.Errorf("mix ice restart: %w", err)
	}
	b.mu.Lock()
	b.mixETag = newETag
	etag = b.mixETag
	b.mu.Unlock()
	return etag, nil
}

// MixETag exposes the current mix ingest ICE ufrag for the HTTP
// handler's ETag header. Returns "" when no mix is attached.
func (b *Broadcast) MixETag() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.mixETag
}

// setMixIceState is wired to the mix transport's OnIceStateChange (or
// the pion bypass's OnICEConnectionStateChange) so Status() always
// reflects the live state. Both callers pass mediasoup-vocabulary
// strings ("connected"/"completed"/"disconnected"/"closed") to keep
// the API consumer-facing semantics independent of which ingress is
// active.
func (b *Broadcast) setMixIceState(state string) {
	b.mu.Lock()
	b.mixIceState = state
	b.mu.Unlock()
}

// handleMixReady runs on the mix consumer goroutine once the first
// video IDR has been depacketized. Flags the switcher so it can flip
// on the next keyframe boundary.
func (b *Broadcast) handleMixReady() {
	b.mu.RLock()
	sw := b.switcher
	b.mu.RUnlock()
	if sw != nil {
		sw.markMixReady()
	}
}

// handleMixGone runs when the mix ICE watchdog expires or producers
// close. Performs the "连麦 → 直播" fallback and tears down the mix
// source. Idempotent with DetachMix (the mixSource fires OnGone at
// most once).
func (b *Broadcast) handleMixGone(reason string) {
	b.mu.Lock()
	if b.terminated {
		b.mu.Unlock()
		return
	}
	mix := b.mix
	sw := b.switcher
	b.mix = nil
	b.mixConnected = false
	b.mixETag = ""
	b.mu.Unlock()

	if sw != nil {
		sw.flipToLive(reason)
		sw.unbindMix()
	}
	if mix != nil {
		go mix.Close() // async: mix.Close blocks on watchdog exit
	}
	b.logger.Warn("mix detached (watchdog)", "reason", reason)
}

// pickMixProducers splits a whip.CreateWHIPResult.Producers slice into
// (audio, video). avflow is expected to publish exactly one of each;
// additional producers are rejected so configuration drift surfaces
// early rather than causing confusing A/V-sync bugs later. Any video
// codec axonkit's pushers support via mediasoup (H.264 / VP9 / AV1 /
// VP8) is accepted — avflow pushing the same codec the live publisher
// uses lets the RTMP egress pass bitstreams through untouched. H.265
// is intentionally absent here: see producerVideoCodec for why
// mediasoup cannot surface it.
func pickMixProducers(producers []*mediasoup.Producer) (audio, video *mediasoup.Producer, err error) {
	for _, p := range producers {
		switch p.Kind() {
		case mediasoup.MediaKindAudio:
			if audio != nil {
				return nil, nil, fmt.Errorf("mix ingest published more than one audio producer")
			}
			if !isOpusProducer(p) {
				return nil, nil, fmt.Errorf("mix ingest audio must be Opus")
			}
			audio = p
		case mediasoup.MediaKindVideo:
			if video != nil {
				return nil, nil, fmt.Errorf("mix ingest published more than one video producer")
			}
			if producerVideoCodec(p) == CodecUnknown {
				return nil, nil, fmt.Errorf("mix ingest video must be H264, VP9, AV1 or VP8")
			}
			video = p
		}
	}
	if audio == nil {
		return nil, nil, fmt.Errorf("mix ingest missing audio producer")
	}
	if video == nil {
		return nil, nil, fmt.Errorf("mix ingest missing video producer")
	}
	return audio, video, nil
}

// validateDestinationsForCodec enforces the subset of (codec,
// destination protocol) combinations the pushers can serialize.
// Today the only hard reject is "live codec is VP8 + at least one
// RTMP destination": Enhanced-RTMP v2 does not register a FourCC
// for VP8, so the RTMP receiver would either close the connection
// or display garbage. A WHIP destination remains valid because the
// pusher passes RTP through verbatim. See sample.go::CodecVP8 for
// the full rationale.
func validateDestinationsForCodec(dsts []Destination, videoCodec Codec) error {
	if videoCodec != CodecVP8 {
		return nil
	}
	for _, d := range dsts {
		if d.Type == DestinationRTMP {
			return fmt.Errorf("broadcast: VP8 cannot be pushed to RTMP destinations (Enhanced-RTMP v2 has no FourCC for VP8); use a WHIP destination or republish in H.264/VP9/AV1")
		}
	}
	return nil
}
