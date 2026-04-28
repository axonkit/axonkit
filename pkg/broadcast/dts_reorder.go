package broadcast

import "log/slog"

// dtsReorder reconstructs a per-frame (DTS, CTS) pair from a stream of
// access units that arrive in decode order but only carry their PTS
// in the RTP timestamp.
//
// # Why this is needed
//
// RFC 6184 §5.1 defines the H.264 RTP timestamp as the sampling /
// presentation time of the frame, so the value attached to each
// packet is the PTS, not the DTS. With B-frames the encoder emits
// frames in decode order but their PTS sequence — observed in
// arrival order — is non-monotonic. For example libx264 with
// `-bf 2` produces:
//
//	decode order:  I    P    B    B    P    B    B    P
//	PTS (ms):     67   200  134  167  334  267  300  467
//
// FLV's tag timestamp is DTS by spec. Writing the raw PTS into that
// field with CTS=0 (as the egress used to do) leaves the muxed MP4
// with PTS == DTS over a B-frame bitstream. The decoder still
// reorders correctly, but the player sorts samples by PTS so the
// screen shows them in *decode* order: frames jump back to pts=134
// after pts=200, which is exactly the visual symptom users see.
//
// # Lookahead algorithm (buffered mode)
//
// dtsReorder buffers a sliding window of `window` arrivals, then on
// every subsequent arrival pops the FIFO head and assigns it a DTS
// equal to `min(head.PTS, min(rest of window))`. The CTS is the
// (PTS - DTS) skew — 0 when the head's own PTS is the smallest in
// the window (the canonical case for monotonic input or when the
// head is the next frame in display order), positive when a later
// B-frame in the lookahead is going to display before the head.
//
// A strict-monotonicity guard nudges DTS forward by one tick when
// two consecutive emits would land on the same DTS (which happens
// when a B-frame's PTS equals an earlier P-frame's chosen DTS or on
// 90 kHz / ms rounding edges). The CTS is then clamped to ≥ 0; the
// resulting one-tick (~11 µs) presentation-jitter is well below
// human-perceptible thresholds and avoids triggering FFmpeg's MP4
// muxer's PTS<DTS reject path.
//
// # Adaptive auto mode
//
// In auto mode the buffer additionally watches the steady-state
// behavior of the input. Every emit produced by the lookahead step
// is classified:
//   - If head.PTS turned out to be the minimum of the entire window
//     (CTS=0, no monotonicity nudge applied), the emit is "trivial":
//     no reorder happened, the frame would have been emitted
//     identically by the direct path. Trivial emits increment the
//     stableCount counter.
//   - Any non-trivial emit (positive CTS, or guard nudge) resets
//     stableCount.
//
// Once stableCount reaches dtsReorderAutoStableFrames the buffer
// flips to passthrough: the effective window shrinks to 0 so each
// subsequent Push pops one emit, and the residual `cap` entries the
// flip left behind drain at one frame per Push through the same
// lookahead path. After the queue empties Push emits each new
// arrival immediately with CTS=0.
//
// Passthrough mode keeps watching the input. Any arrival whose PTS
// is less than the most recently emitted PTS is the moment we know
// the upstream just turned on B-frames after all. That arrival is
// pushed back into the queue (instead of emitted), the effective
// window is restored to `cap`, and from there steady-state lookahead
// resumes. The frame that triggered the flip-back may pay a one-tick
// CTS clamp from the monotonicity guard — but the alternative
// (silently dropping the reorder signal) is the worse choice.
//
// # Properties
//
// Given any decode-order PTS stream this algorithm produces:
//   - Strictly monotonic DTS in the source 90 kHz clock.
//   - CTS ≥ 0 for every emitted access unit.
//   - PTS = DTS + CTS recoverable on the receiver up to the
//     occasional ≤ 1 ms guard slip on duplicated PTSs or the single
//     auto-mode flip-back boundary.
//
// # Window size
//
// dtsReorderWindow=4 covers libx264 -bf 3 (max_b_frames=3) and every
// other production encoder we ship against. Steady-state buffering
// delay is window×frame_duration ≈ 133 ms at 30 fps — invisible in a
// recording pipeline and acceptable on live broadcast egress.
const dtsReorderWindow = 4

// dtsReorderAutoStableFrames is the number of consecutive trivial
// emits required for auto mode to flip into passthrough. Sized large
// enough to ride out the worst realistic warm-up pattern (a static
// scene of pure I/P frames before the first B-frame appears) yet
// small enough that a stream genuinely free of B-frames pays at most
// ~2 s of buffer latency before the latency drops to zero. libx264
// with default `-bf 3` emits its first B-frame within the first GOP
// (typically frame index 1 or 2), so any encoder that has B-frames
// at all will trip this counter back to zero well within 60 frames.
const dtsReorderAutoStableFrames = 60

// dtsReorderGuardTicks is the minimum 90 kHz tick spacing the
// monotonicity guard inserts between consecutive emits. Sized to
// 90 (= 1 ms) so the FLV tag's millisecond timestamp field stays
// strictly increasing after the (ticks*1000/90000) integer
// truncation the egress applies. A +1-tick guard would collapse
// three consecutive emits whose lookahead picked the same rawDTS
// into a single FLV millisecond — manifesting as duplicate DTS
// rows in any downstream MP4 mux and tripping ffmpeg's "reference
// picture missing" decoder path on B-frames whose recovery key
// frame is hidden behind the duplicate-DTS cluster.
const dtsReorderGuardTicks = 90

// dtsReorderMode is internal: the rtmpPusher selects one when it
// constructs the buffer. The public Destination.DTSReorder pointer
// maps onto these values via Destination.DTSReorderMode.
type dtsReorderMode uint8

const (
	dtsReorderModeAlways dtsReorderMode = iota
	dtsReorderModeAuto
)

type dtsReorder struct {
	cap   int
	queue []dtsReorderEntry

	mode dtsReorderMode

	// lastDTS / haveLastDTS form the strict-monotonicity guard.
	// Maintained across mode flips so passthrough emits inherit the
	// last buffered DTS as their floor.
	lastDTS     uint32
	haveLastDTS bool

	// lastEmitPTS / haveLastEmitPTS track the most recently emitted
	// PTS. In passthrough mode we use this to detect a regression
	// (PTS went backward → B-frame just appeared → flip back into
	// buffered mode).
	lastEmitPTS     uint32
	haveLastEmitPTS bool

	// auto-mode-only state.
	stableCount int  // consecutive trivial emits since last reorder
	passthrough bool // currently in passthrough mode

	// logger is optional; when set the buffer emits a one-shot Info
	// line on each buffered↔passthrough transition so operators can
	// trace per-destination reorder behavior without enabling the
	// per-frame Debug stream. nil-safe (every call site checks).
	logger *slog.Logger
}

type dtsReorderEntry struct {
	pts90k uint32
	sample MediaSample
}

// dtsReorderEmit is one access unit ready to be muxed out.
//   - dts90k is the decode timestamp in the source 90 kHz clock; the
//     rtmp pusher subtracts its per-stream epoch and converts to
//     milliseconds for the FLV tag header.
//   - ctsMs is the (PTS - DTS) offset in milliseconds, written into
//     the FLV CompositionTime field. Always ≥ 0.
type dtsReorderEmit struct {
	sample MediaSample
	dts90k uint32
	ctsMs  int32
}

func newDTSReorder(window int) *dtsReorder {
	return newDTSReorderMode(window, dtsReorderModeAlways)
}

// newDTSReorderMode is the option-aware constructor. mode picks the
// behavior described in the file-level docstring.
func newDTSReorderMode(window int, mode dtsReorderMode) *dtsReorder {
	if window < 1 {
		window = 1
	}
	return &dtsReorder{
		cap:   window,
		queue: make([]dtsReorderEntry, 0, window+1),
		mode:  mode,
	}
}

// SetLogger attaches a structured logger used for the buffered↔
// passthrough transition Info lines. Optional; an unset logger leaves
// the buffer silent (the algorithm itself is unchanged either way).
// Callers typically pass the same per-destination logger they hand to
// the rtmp pusher so transition events join the rest of the egress
// log fields under one common scope.
func (r *dtsReorder) SetLogger(logger *slog.Logger) {
	r.logger = logger
}

// Push enqueues one sample and returns zero or more emits ready for
// the egress muxer. Returning a slice (instead of an `(emit, bool)`
// pair) lets the auto-mode flip drain the residual lookahead buffer
// in the same Push call that triggers it; in steady state the slice
// is either nil (warm-up / regression) or a singleton.
//
// Behavior depends on the current mode:
//   - Buffered (always-on, or auto warming up / recovering): while
//     the queue has not yet reached cap entries returns nil; once
//     full each arrival pops the FIFO head and returns its emit.
//     If that emit also trips the auto-mode passthrough threshold,
//     the residual queue is drained into the same return slice so
//     the egress immediately catches up to wall-clock latency.
//   - Passthrough (only reachable in auto mode): each arrival emits
//     immediately with CTS=0 and DTS=max(PTS, lastDTS+1). If the
//     incoming PTS is less than the last emitted PTS the sample is
//     instead pushed back into the queue and the buffer flips back
//     to buffered so the upstream's newly-introduced B-frames get
//     reordered correctly; that Push returns no emit (the buffered
//     path needs `cap` arrivals to refill its lookahead).
func (r *dtsReorder) Push(s MediaSample) []dtsReorderEmit {
	if r.passthrough && r.haveLastEmitPTS &&
		int32(s.RTPTimestamp-r.lastEmitPTS) < 0 { //nolint:gosec // modular comparison
		// PTS regressed — upstream just turned on B-frames. Drop
		// out of passthrough; the next `cap` arrivals (including
		// this one) refill the lookahead window before any emit
		// fires.
		r.passthrough = false
		r.stableCount = 0
		r.queue = append(r.queue, dtsReorderEntry{pts90k: s.RTPTimestamp, sample: s})
		if r.logger != nil {
			r.logger.Info("dts reorder switched back to buffered",
				"reason", "pts_regression",
				"incoming_pts90k", s.RTPTimestamp,
				"last_emit_pts90k", r.lastEmitPTS,
				"window", r.cap,
			)
		}
		return nil
	}

	if r.passthrough {
		// Steady-state passthrough: queue is empty (drained on
		// flip and never refilled). Direct emit, zero added
		// latency, and we still pass through the monotonicity
		// guard so the DTS series remains strictly increasing
		// across mode flips.
		return []dtsReorderEmit{r.directEmit(s)}
	}

	// Buffered path.
	r.queue = append(r.queue, dtsReorderEntry{pts90k: s.RTPTimestamp, sample: s})
	if len(r.queue) <= r.cap {
		return nil
	}
	out := []dtsReorderEmit{r.emit()}
	// emit() may have just flipped the buffer into passthrough
	// mode after a long trivial-emit streak. Drain the residual
	// queue in the same Push call so the steady-state latency
	// goes immediately to zero rather than dragging `cap` frames
	// behind real time forever.
	for r.passthrough && len(r.queue) > 0 {
		out = append(out, r.emit())
	}
	return out
}

// Drain flushes everything still in the buffer; called on shutdown so
// the trailing window of frames does not get lost when the publisher
// ends. Emits are returned in decode (FIFO) order with monotonic DTS
// computed the same way as in steady state.
func (r *dtsReorder) Drain() []dtsReorderEmit {
	if len(r.queue) == 0 {
		return nil
	}
	out := make([]dtsReorderEmit, 0, len(r.queue))
	for len(r.queue) > 0 {
		out = append(out, r.emit())
	}
	return out
}

// directEmit produces a passthrough emit for s: DTS is the sample's
// PTS unless the strict-monotonicity guard needs to nudge it forward
// by one ms-equivalent (dtsReorderGuardTicks), and CTS is always 0
// (no B-frame, no reorder happening). Maintained as a separate path
// from emit() so passthrough enjoys real zero-latency operation: no
// append into r.queue, no lookahead loop, no auto-mode counter math.
func (r *dtsReorder) directEmit(s MediaSample) dtsReorderEmit {
	dts := s.RTPTimestamp
	if r.haveLastDTS && int32(dts-r.lastDTS) < dtsReorderGuardTicks { //nolint:gosec // modular comparison
		dts = r.lastDTS + dtsReorderGuardTicks
	}
	r.lastDTS = dts
	r.haveLastDTS = true
	r.lastEmitPTS = s.RTPTimestamp
	r.haveLastEmitPTS = true
	return dtsReorderEmit{sample: s, dts90k: dts, ctsMs: 0}
}

// emit pops the FIFO head and computes its (DTS, CTS). DTS is the
// minimum of head.PTS and any remaining buffered PTS — small enough
// that head's frame can decode immediately, but never smaller than
// the previous emit's DTS (monotonicity guard). CTS = PTS - DTS,
// clamped at 0.
//
// In auto mode this also drives the stability counter that decides
// when to flip into passthrough.
func (r *dtsReorder) emit() dtsReorderEmit {
	head := r.queue[0]
	r.queue = r.queue[1:]
	rawDTS := head.pts90k
	for _, e := range r.queue {
		if int32(e.pts90k-rawDTS) < 0 { //nolint:gosec // modular comparison
			rawDTS = e.pts90k
		}
	}
	dts := rawDTS
	guardApplied := false
	// Guard step is one millisecond (dtsReorderGuardTicks=90 at
	// 90 kHz) instead of a single tick so the downstream FLV tag's
	// integer-millisecond timestamp stays strictly monotonic after
	// the (ticks*1000/90000) truncation. A 1-tick guard collapses
	// three consecutive lookahead-tied emits into the same FLV ms
	// — see dts_reorder_test.go's TestEmitsAreStrictlyMonotonicInMs
	// for the regression we're protecting against here.
	if r.haveLastDTS && int32(dts-r.lastDTS) < dtsReorderGuardTicks { //nolint:gosec // modular comparison
		dts = r.lastDTS + dtsReorderGuardTicks
		guardApplied = true
	}
	r.lastDTS = dts
	r.haveLastDTS = true
	r.lastEmitPTS = head.pts90k
	r.haveLastEmitPTS = true

	ctsTicks := int32(head.pts90k - dts) //nolint:gosec // bounded by window span
	if ctsTicks < 0 {
		// Monotonicity nudge can push DTS past head.PTS by one tick
		// on duplicated-PTS edge cases. Clamping yields PTS=DTS for
		// that single frame — correct decode order with at most one
		// tick (~11 µs) of presentation jitter.
		ctsTicks = 0
	}
	ctsMs := ctsTicks * 1000 / 90000

	// Auto mode bookkeeping: a "trivial" emit is one where the
	// lookahead chose head's own PTS (head was already the smallest
	// in the window) AND the monotonicity guard didn't have to
	// nudge. Anything else means reorder is actively useful.
	// Already-passthrough emits stop counting (the counter is moot).
	if r.mode == dtsReorderModeAuto && !r.passthrough {
		trivial := !guardApplied && rawDTS == head.pts90k && ctsMs == 0
		if trivial {
			r.stableCount++
			if r.stableCount >= dtsReorderAutoStableFrames {
				r.passthrough = true
				r.stableCount = 0
				if r.logger != nil {
					r.logger.Info("dts reorder switched to passthrough",
						"reason", "no_b_frames_detected",
						"stable_frames", dtsReorderAutoStableFrames,
						"window", r.cap,
					)
				}
			}
		} else {
			r.stableCount = 0
		}
	}

	return dtsReorderEmit{
		sample: head.sample,
		dts90k: dts,
		ctsMs:  ctsMs,
	}
}
