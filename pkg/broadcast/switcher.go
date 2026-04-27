package broadcast

import (
	"log/slog"
	"sync"

	"github.com/pion/rtp"
)

// switchState encodes which source is currently driving the outbound
// fan-out. When a switch is pending (e.g. mix was attached and we are
// waiting for its first IDR before swapping over), the active kind
// stays on the previous source and a `pending*` flag is raised so the
// video path can observe it without extra locking.
type switchState uint8

const (
	sourceLiveActive switchState = iota
	sourceMixActive
)

// defaultAudioTick is the Opus frame duration in 48 kHz samples. The
// switcher uses it as the "catch-up" delta when the first sample after
// a switch arrives — we cannot know the exact gap because neither
// source guarantees back-to-back timestamps, but 20 ms is the
// canonical Opus ptime and matches what both livekit and avflow emit.
const defaultAudioTick = 960

// defaultVideoTick is 33 ms at 90 kHz (≈ 30 fps). Same rationale as
// above: we need *some* positive delta to keep FLV/RTP timestamps
// monotonic across a source change.
const defaultVideoTick = 3000

// rebaser turns a noisy per-source RTP-clock into a continuous output
// clock that preserves the input clock's intra-source PTS shape —
// crucially including the per-frame backward steps a B-frame source
// produces. The rebaser anchors each source's first sample at a
// monotonically-advancing output base and emits everything else as
// `outBase + (inTS - srcBase)` (signed delta). The hwm running
// maximum of all outputs is what advances on a source flip so the new
// source's first sample lands above every value the old source ever
// emitted, while the within-source PTS reordering is left untouched
// for downstream dts-reorder logic to recover the (DTS, CTS) pair.
//
// Not thread-safe — the switcher serializes access by delivering audio
// and video on separate goroutines (one consumer per kind) and holding
// no lock between rebase and emit.
type rebaser struct {
	// initialized is true once the rebaser has seen any sample at
	// all. Distinct from hasBase so the very first sample produces
	// output 0 instead of catchup.
	initialized bool
	// hasBase indicates the current source has contributed at least
	// one sample under this rebaser; used to distinguish the "first
	// sample after a source flip" from the steady state.
	hasBase bool
	// srcBase is the in-TS of the current source's first sample
	// since the last flip. NOT updated on every frame — only on the
	// initial sample of a source — so the signed delta below
	// (inTS - srcBase) preserves intra-source PTS reordering.
	srcBase uint32
	// outBase is the output TS that corresponds to srcBase (i.e. the
	// output emitted on the source's very first sample). Set on
	// init / source flip; constant across the rest of that source.
	outBase uint32
	// hwm is the modular running maximum of all outputs ever
	// emitted. catchup is applied to hwm — never to a possibly
	// out-of-order outBase + sdelta — so a source flip lands
	// strictly above every previously emitted timestamp even if the
	// last sample of the prior source was a B-frame whose output
	// was below the local high-water mark.
	hwm uint32
	// catchup is the tick count added on top of hwm when a new
	// source's first sample arrives. Audio uses 960 (one Opus
	// ptime), video 3000 (~33 ms at 90 kHz).
	catchup uint32
}

// rewrite returns the output TS for an incoming sample. Output is
// monotonic *across* source flips (driven by hwm + catchup) but is
// allowed to step backwards *within* a source whenever the input PTS
// does — exactly the case for an H.264 B-frame whose presentation
// timestamp precedes the prior P-frame in arrival order. The previous
// implementation collapsed this into a strictly-monotonic sequence by
// treating any backward delta as 32-bit wraparound, which silently
// erased the CompositionTime offset the egress dts-reorder buffer
// needs to rebuild a playable recording.
func (r *rebaser) rewrite(inTS uint32) uint32 {
	if !r.initialized {
		r.initialized = true
		r.hasBase = true
		r.srcBase = inTS
		r.outBase = 0
		r.hwm = 0
		return 0
	}
	if !r.hasBase {
		r.hasBase = true
		r.srcBase = inTS
		// catchup is applied to hwm, not outBase, so the new
		// source starts strictly above the old source's furthest
		// emitted timestamp regardless of where outBase + sdelta
		// landed on the prior source's last sample.
		r.outBase = r.hwm + r.catchup
		r.hwm = r.outBase
		return r.outBase
	}
	// Signed delta from this source's anchor. For the canonical
	// IBPBP… mediasoup feed the anchor is the source's first IDR,
	// so all subsequent samples have inTS >= srcBase by construction
	// (every GOP sample's PTS lies inside or after the IDR). B-frame
	// reorder shows up as sdelta moving backwards relative to the
	// prior sample but never crossing zero.
	sdelta := int32(inTS - r.srcBase) //nolint:gosec // intentional uint32→int32 modular cast
	out := uint32(int64(r.outBase) + int64(sdelta))
	// Modular comparison so wraparound past 2^32 is handled too.
	if int32(out-r.hwm) > 0 {
		r.hwm = out
	}
	return out
}

// onSourceFlip invalidates the current source base so the next sample
// becomes the "first sample after a flip" (catchup tick applied).
func (r *rebaser) onSourceFlip() { r.hasBase = false }

// switcher owns the "which source is active, and where is its clock?"
// decision. Every sample and packet goes through here before reaching
// the pushers; the switcher drops samples from the inactive source so
// subscribers see exactly one continuous stream.
//
// Concurrency: live+mix each deliver on their own mediasoup consumer
// goroutines (audio + video). The switcher uses `mu` for the control-
// plane state (active, pending) and fine-grained per-kind rebasers
// that do not race because mediasoup emits one RTP packet at a time
// per consumer. `mu` is always taken briefly; no push-side work
// happens under the lock.
type switcher struct {
	logger *slog.Logger

	mu         sync.RWMutex
	state      switchState
	terminated bool
	// mixReady flips true once mix has emitted its first video
	// keyframe (or the fallback grace has elapsed). Until then the
	// switcher keeps serving live.
	mixReady bool

	// Sample and packet paths each have their own per-kind rebaser.
	// They are intentionally NOT shared: liveSource feeds a single
	// RTP packet into packetSink(feedPacket) AND — after the
	// depacketizer assembles a sample — into sampleSink(feedSample).
	// If we shared one rebaser, feedPacket would rewrite
	// pkt.Timestamp in place, the depack would carry the rebased TS
	// into sample.RTPTimestamp, and feedSample would try to rebase
	// an already-rebased value against the original srcBase — a
	// uint32 underflow that produced hours of bogus FLV timestamps
	// in early verify-broadcast runs. Splitting gives each sink its
	// own monotonic output clock.
	audioSample rebaser
	videoSample rebaser
	audioPacket rebaser
	videoPacket rebaser

	// emit* are the downstream hooks; they are the Broadcast's
	// fanOutSample / fanOutPacket bound at construction.
	emitSample func(s MediaSample, extradata []byte)
	emitPacket func(kind SampleKind, pkt *rtp.Packet)

	// liveExtradataFn / mixExtradataFn return the AVCC extradata
	// for their respective source. Set via bindLive / bindMix.
	liveExtradataFn func() []byte
	mixExtradataFn  func() []byte

	// onFlipToLive / onFlipToMix are fired once per transition so
	// Broadcast can request a fresh keyframe, reset pusher seq
	// headers, update Status(), etc.
	onFlipToLive func()
	onFlipToMix  func()
}

// newSwitcher constructs a switcher in the live-active state with
// per-kind rebasers primed with the default tick values. The returned
// switcher is ready to accept live samples immediately; mix fan-in
// starts only after bindMix + markMixReady.
func newSwitcher(logger *slog.Logger) *switcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &switcher{
		logger:      logger,
		state:       sourceLiveActive,
		audioSample: rebaser{catchup: defaultAudioTick},
		videoSample: rebaser{catchup: defaultVideoTick},
		audioPacket: rebaser{catchup: defaultAudioTick},
		videoPacket: rebaser{catchup: defaultVideoTick},
	}
}

// bindEmitters sets the downstream sinks. Called once by Broadcast
// right after the pusher list is frozen.
func (s *switcher) bindEmitters(
	emitSample func(s MediaSample, extradata []byte),
	emitPacket func(kind SampleKind, pkt *rtp.Packet),
) {
	s.emitSample = emitSample
	s.emitPacket = emitPacket
}

// bindLive / bindMix register the extradata accessors so the switcher
// can pull the correct SPS/PPS blob for whichever source is active on
// the current video path.
func (s *switcher) bindLive(extradataFn func() []byte) {
	s.mu.Lock()
	s.liveExtradataFn = extradataFn
	s.mu.Unlock()
}

func (s *switcher) bindMix(extradataFn func() []byte) {
	s.mu.Lock()
	s.mixExtradataFn = extradataFn
	s.mixReady = false
	s.mu.Unlock()
}

// unbindMix tears the mix accessor down and marks mix no longer ready.
// If we are currently emitting mix samples the caller must also call
// flipToLive so the active source falls back.
func (s *switcher) unbindMix() {
	s.mu.Lock()
	s.mixExtradataFn = nil
	s.mixReady = false
	s.mu.Unlock()
}

// setTransitionHooks wires the per-flip callbacks.
func (s *switcher) setTransitionHooks(onLive, onMix func()) {
	s.mu.Lock()
	s.onFlipToLive = onLive
	s.onFlipToMix = onMix
	s.mu.Unlock()
}

// terminate stops further emission. Safe to call many times.
func (s *switcher) terminate() {
	s.mu.Lock()
	s.terminated = true
	s.mu.Unlock()
}

// activeSource reports the current active source for Status()
// consumers. Does not reveal the "pending" bit — it's an internal
// optimisation only the video fast-path cares about.
func (s *switcher) activeSource() SourceKind {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.state == sourceMixActive {
		return SourceMix
	}
	return SourceLive
}

// feedSample is the entry point for depacketized samples. The source
// parameter lets us drop samples coming from the non-active stream; we
// also perform the keyframe-aligned "pending → active" transition when
// the first mix IDR arrives.
func (s *switcher) feedSample(src SourceKind, sample MediaSample) {
	s.mu.RLock()
	if s.terminated {
		s.mu.RUnlock()
		return
	}
	state := s.state
	mixReady := s.mixReady
	s.mu.RUnlock()

	switch sample.Kind {
	case SampleKindVideo:
		s.feedVideoSample(src, sample, state, mixReady)
	case SampleKindAudio:
		s.feedAudioSample(src, sample, state)
	}
}

// feedPacket is the entry point for raw RTP packets (WHIP egress). We
// apply the same source gating as feedSample and produce a *copy* of
// pkt with the rewritten output-clock timestamp.
//
// Crucially: we do NOT mutate the caller's pkt.Timestamp. The same
// *rtp.Packet pointer is then fed into the source's depacketizer by
// the caller (liveSource.handleVideo / mixSource.handleVideo), and
// the depacketizer stamps its assembled MediaSample with
// pkt.Timestamp. If we rewrote pkt in place the downstream
// sampleSink path would receive already-rebased timestamps and try
// to rebase them again against the sample rebaser's baseline — the
// uint32 underflow bug that produced FLV streams with hours of
// virtual duration and broke the verify-broadcast color probes.
func (s *switcher) feedPacket(src SourceKind, kind SampleKind, pkt *rtp.Packet) {
	s.mu.RLock()
	if s.terminated {
		s.mu.RUnlock()
		return
	}
	state := s.state
	mixReady := s.mixReady
	s.mu.RUnlock()

	if !s.sourceEmits(src, kind, state, mixReady, pkt.Marker, false) {
		return
	}
	var r *rebaser
	switch kind {
	case SampleKindAudio:
		r = &s.audioPacket
	case SampleKindVideo:
		r = &s.videoPacket
	default:
		return
	}
	newTS := r.rewrite(pkt.Timestamp)
	if s.emitPacket == nil {
		return
	}
	outPkt := *pkt
	outPkt.Timestamp = newTS
	s.emitPacket(kind, &outPkt)
}

// sourceEmits decides whether a sample from `src` should reach the
// pushers given the current switcher state. Video samples may also
// trigger an internal state flip (live→mix on first mix keyframe);
// that flip happens in feedVideoSample which is where we have enough
// context to handle extradata+header reset atomically.
func (s *switcher) sourceEmits(
	src SourceKind,
	kind SampleKind,
	state switchState,
	mixReady bool,
	_ bool, // marker (unused)
	_ bool, // isKeyframe (unused at this level)
) bool {
	activeIsMix := state == sourceMixActive
	switch src {
	case SourceLive:
		return !activeIsMix
	case SourceMix:
		// Audio switches as soon as mix is marked ready so the
		// viewer doesn't hear live audio leak into the mixed
		// scene. Video is gated by the ready flag too, but the
		// keyframe alignment is done in feedVideoSample.
		if kind == SampleKindAudio {
			return activeIsMix || mixReady
		}
		return activeIsMix
	}
	return false
}

// feedAudioSample forwards audio immediately without keyframe wait.
// When mix goes ready while we are still on live, the very first mix
// audio sample triggers the state flip.
func (s *switcher) feedAudioSample(src SourceKind, sample MediaSample, state switchState) {
	activeIsMix := state == sourceMixActive
	switch src {
	case SourceLive:
		if activeIsMix {
			return
		}
	case SourceMix:
		if !activeIsMix {
			if !s.tryFlipToMixOnAudio() {
				return
			}
		}
	}
	sample.RTPTimestamp = s.audioSample.rewrite(sample.RTPTimestamp)
	if s.emitSample != nil {
		s.emitSample(sample, nil)
	}
}

// feedVideoSample implements keyframe-aligned switching. While we are
// on live and mix is ready we wait for the first mix IDR; while we are
// on mix and mix goes away we fall back to live and request a fresh
// key frame so the next emitted video tag is a proper GOP start.
func (s *switcher) feedVideoSample(src SourceKind, sample MediaSample, state switchState, mixReady bool) {
	activeIsMix := state == sourceMixActive
	switch src {
	case SourceLive:
		if activeIsMix {
			return
		}
	case SourceMix:
		if !activeIsMix {
			if !mixReady || !sample.IsKeyFrame {
				return
			}
			if !s.tryFlipToMixOnKeyframe() {
				return
			}
		}
	}

	s.emitVideo(src, sample)
}

// tryFlipToMixOnAudio swaps active to mix when triggered by an audio
// sample. Returns true if the caller should continue emitting (i.e.
// the flip succeeded or was already done).
func (s *switcher) tryFlipToMixOnAudio() bool {
	s.mu.Lock()
	if s.terminated {
		s.mu.Unlock()
		return false
	}
	if s.state == sourceMixActive {
		s.mu.Unlock()
		return true
	}
	// Only flip when mix is marked ready; until then audio from mix
	// is also withheld so we don't mix two streams' audio.
	if !s.mixReady {
		s.mu.Unlock()
		return false
	}
	s.state = sourceMixActive
	s.audioSample.onSourceFlip()
	s.audioPacket.onSourceFlip()
	// Video rebasers are flipped inside tryFlipToMixOnKeyframe when
	// the first mix IDR arrives — but if audio wins the race we
	// flip them here too so the first video sample/packet is
	// treated as first-after-switch.
	s.videoSample.onSourceFlip()
	s.videoPacket.onSourceFlip()
	hook := s.onFlipToMix
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	s.logger.Info("switcher: flipped live → mix (audio-first)")
	return true
}

// tryFlipToMixOnKeyframe is the canonical path: we are on live, a mix
// keyframe arrived, time to swap. Returns true on success.
func (s *switcher) tryFlipToMixOnKeyframe() bool {
	s.mu.Lock()
	if s.terminated {
		s.mu.Unlock()
		return false
	}
	if s.state == sourceMixActive {
		s.mu.Unlock()
		return true
	}
	if !s.mixReady {
		s.mu.Unlock()
		return false
	}
	s.state = sourceMixActive
	s.videoSample.onSourceFlip()
	s.videoPacket.onSourceFlip()
	s.audioSample.onSourceFlip()
	s.audioPacket.onSourceFlip()
	hook := s.onFlipToMix
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	s.logger.Info("switcher: flipped live → mix (keyframe)")
	return true
}

// flipToLive is triggered externally when mix ingest goes away (ICE
// failure / DELETE). Keyframe alignment on the *live* side is handled
// by the caller via liveSource.RequestKeyFrame; we simply swap state
// and flip the rebasers so the next live sample is treated as first-
// after-switch.
func (s *switcher) flipToLive(reason string) {
	s.mu.Lock()
	if s.terminated {
		s.mu.Unlock()
		return
	}
	if s.state == sourceLiveActive {
		s.mu.Unlock()
		return
	}
	s.state = sourceLiveActive
	s.mixReady = false
	s.audioSample.onSourceFlip()
	s.audioPacket.onSourceFlip()
	s.videoSample.onSourceFlip()
	s.videoPacket.onSourceFlip()
	hook := s.onFlipToLive
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	s.logger.Info("switcher: flipped mix → live", "reason", reason)
}

// markMixReady is called once the mix source has seen its ICE
// Connected transition AND its first video keyframe is known to be
// either in-flight (we'll swap on it) or the source itself triggers
// the swap. The mixReady flag is observed by the video feed for
// switching and by audio for dropping leakage.
func (s *switcher) markMixReady() {
	s.mu.Lock()
	if !s.terminated {
		s.mixReady = true
	}
	s.mu.Unlock()
}

// emitVideo rewrites the sample's RTP timestamp and forwards it to the
// downstream fan-out, with the correct extradata pulled from whichever
// source is currently emitting.
func (s *switcher) emitVideo(src SourceKind, sample MediaSample) {
	sample.RTPTimestamp = s.videoSample.rewrite(sample.RTPTimestamp)
	var extradata []byte
	if sample.IsKeyFrame {
		extradata = s.extradataFor(src)
	}
	if s.emitSample != nil {
		s.emitSample(sample, extradata)
	}
}

func (s *switcher) extradataFor(src SourceKind) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	switch src {
	case SourceLive:
		if s.liveExtradataFn != nil {
			return s.liveExtradataFn()
		}
	case SourceMix:
		if s.mixExtradataFn != nil {
			return s.mixExtradataFn()
		}
	}
	return nil
}
