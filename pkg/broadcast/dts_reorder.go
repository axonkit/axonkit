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
// # Sorted-PTS-pop algorithm (buffered mode)
//
// dtsReorder buffers a sliding window of `window` arrivals, alongside
// a sortedPTS slice that mirrors the queue's contents in ascending
// PTS order. On every arrival past the warm-up window the FIFO head
// pops and is assigned `DTS = sortedPTS.pop_front()` — i.e. the
// smallest PTS still buffered. This is the same rule mp4 muxers use
// to derive DTS from PTS ("DTS[i in decode order] = sorted_pts[i]")
// and it produces a strictly-monotonic, fps-equidistant DTS series
// in steady state. The CTS is then `head.PTS - DTS` — 0 in the
// canonical case where head is the next-to-display frame (head.PTS
// == sortedPTS[0]), and positive whenever a later B-frame in the
// window is going to display before the head.
//
// Two safety nets backstop the result:
//
//   - CTS-clamp: if sortedPTS[0] ends up greater than head.PTS (head
//     is itself the smallest still-buffered PTS but the sort lookup
//     happens to return a different equal-value entry, or the queue
//     just lost its only future frame), DTS clamps down to head.PTS
//     so CTS = PTS - DTS stays ≥ 0. FLV's CompositionTime field is
//     signed SI24 so negative CTS is in-spec, but mainstream players
//     (ffmpeg's flv demuxer included) treat it as unsigned, so we
//     keep the existing CTS≥0 contract.
//
//   - Strict-monotonicity guard: when the candidate DTS would equal
//     or precede lastDTS (typical at a P→B cluster boundary, where
//     the next B-frame's PTS equals the P-frame's DTS we just
//     emitted), nudge DTS forward by one ms-equivalent tick
//     (dtsReorderGuardTicks=90 at 90 kHz) so the FLV tag's
//     ms-truncated header stays strictly increasing. The CTS is
//     clamped to 0 on the same emit. In steady state this fires
//     once per cluster boundary at most — a single +1 ms slip per
//     cluster, well below human-perceptible thresholds.
//
// The previous "lookahead-min" implementation chose `min(head.PTS,
// rest of window)` for every emit; on an N-B-frame cluster that
// pinned all N+1 emits to the same rawDTS and forced N consecutive
// +1 ms guards, producing N-element 1 ms DTS clusters in the FLV
// timestamp series. The sorted-pop rule yields at most one +1 ms
// nudge per cluster boundary regardless of cluster size.
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
// emits onto the same FLV millisecond — manifesting as duplicate
// DTS rows in any downstream MP4 mux and tripping ffmpeg's
// "reference picture missing" decoder path on B-frames whose
// recovery key frame is hidden behind the duplicate-DTS cluster.
// With the sorted-PTS-pop algorithm the guard fires at most once
// per cluster boundary in steady state.
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

	// sortedPTS holds the 90 kHz PTS of every sample currently in
	// queue, kept in ascending order. Push appends one PTS via
	// insertSorted; emit pops sortedPTS[0] as the next DTS candidate
	// (i.e. "the smallest PTS still buffered"). This is the
	// equivalent of mp4 muxer's "DTS[i in decode order] =
	// sorted_pts[i]" rule and yields a strictly-monotonic, fps-
	// equidistant DTS series for every steady-state B-frame cluster.
	//
	// Invariant: len(sortedPTS) == len(queue) at all times outside an
	// in-progress emit() call.
	//
	// Comparison is modular (int32 signed delta) so a 47.7-hour 90 kHz
	// wrap never confuses the sort — the buffer width is bounded at
	// cap*frame_duration ≈ 133 ms, dwarfed by the wrap period.
	sortedPTS []uint32

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
		cap:       window,
		queue:     make([]dtsReorderEntry, 0, window+1),
		sortedPTS: make([]uint32, 0, window+1),
		mode:      mode,
	}
}

// insertSorted places v at its ascending position inside r.sortedPTS,
// using modular int32 comparison so a 90 kHz timestamp wrap (every
// 47.7 h) does not corrupt the order. The slice stays small (≤ cap+1
// entries, in practice 4–8) so the linear scan beats any binary-
// search overhead.
func (r *dtsReorder) insertSorted(v uint32) {
	idx := 0
	for idx < len(r.sortedPTS) && int32(r.sortedPTS[idx]-v) < 0 { //nolint:gosec // modular comparison
		idx++
	}
	r.sortedPTS = append(r.sortedPTS, 0)
	copy(r.sortedPTS[idx+1:], r.sortedPTS[idx:])
	r.sortedPTS[idx] = v
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
		r.insertSorted(s.RTPTimestamp)
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
	r.insertSorted(s.RTPTimestamp)
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

// emit pops the FIFO head and computes its (DTS, CTS).
//
// Algorithm: the next DTS is the smallest PTS still buffered (i.e.
// sortedPTS[0]). This is the same rule mp4 muxers use to derive DTS
// from PTS — "DTS[i in decode order] = sorted_pts[i]" — and it
// produces a strictly-monotonic, fps-equidistant DTS series in steady
// state. Two safety nets guard the result:
//
//  1. CTS-clamp: if the popped sortedPTS[0] is greater than head.PTS
//     (head ran out of "future" frames whose PTS could lend it a
//     smaller DTS), clamp DTS down to head.PTS so CTS = PTS - DTS
//     stays ≥ 0. FLV's CompositionTime field is signed SI24 so the
//     protocol layer would in principle accept a negative CTS, but
//     mainstream players (ffmpeg's flv demuxer included) treat the
//     field as unsigned and present the resulting frames at the wrong
//     time, so we keep the existing CTS≥0 contract.
//
//  2. Strict-monotonicity guard: if the candidate DTS is ≤ lastDTS
//     (typical at a P→B cluster boundary, where the B-frame's PTS
//     equals the P-frame's DTS we just emitted), nudge DTS forward by
//     dtsReorderGuardTicks (=90 at 90 kHz, i.e. one FLV millisecond)
//     so the FLV-tag-header's ms-truncated timestamp remains strictly
//     increasing. This single +1ms nudge replaces the legacy
//     "lookahead-min + N ms-guards per cluster" output, where N was
//     the number of B-frames in the cluster. Steady state now sees
//     at most one +1ms nudge per cluster boundary.
//
// In auto mode this also drives the stability counter that decides
// when to flip into passthrough.
func (r *dtsReorder) emit() dtsReorderEmit {
	head := r.queue[0]
	r.queue = r.queue[1:]
	rawDTS := r.sortedPTS[0]
	r.sortedPTS = r.sortedPTS[1:]

	dts := rawDTS
	clampedToHead := false
	if int32(dts-head.pts90k) > 0 { //nolint:gosec // modular comparison
		dts = head.pts90k
		clampedToHead = true
	}

	guardApplied := false
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
		// Guard nudge can push DTS past head.PTS by one ms tick on
		// the cluster-boundary case described above. Clamping
		// yields PTS=DTS for that single frame — correct decode
		// order with at most one millisecond of presentation
		// jitter, which mainstream players absorb without artifact.
		ctsTicks = 0
	}
	ctsMs := ctsTicks * 1000 / 90000

	// Auto mode bookkeeping: a "trivial" emit is one where neither
	// safety net fired AND the chosen DTS equals head's own PTS.
	// That combination means the buffer found no out-of-order frame
	// in the window, so reorder added no value for this sample.
	// Already-passthrough emits stop counting (the counter is moot).
	if r.mode == dtsReorderModeAuto && !r.passthrough {
		trivial := !guardApplied && !clampedToHead && rawDTS == head.pts90k && ctsMs == 0
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
