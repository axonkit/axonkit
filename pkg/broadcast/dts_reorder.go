package broadcast

import "log/slog"

// dtsReorder reconstructs the FLV-facing (DTS, CTS) pair for H.264 /
// H.265 access units that arrive in decode order while carrying PTS in
// the RTP timestamp. B-frame streams make that PTS sequence
// non-monotonic, but FLV tag timestamps are DTS.
//
// Buffered mode keeps a FIFO queue plus the same PTS values sorted. On
// each emit it assigns the FIFO head the smallest still-buffered PTS as
// its DTS, then clamps to keep CTS >= 0 and nudges by one FLV millisecond
// when needed to keep DTS strictly increasing after RTMP ms truncation.
//
// Auto mode starts buffered, switches to passthrough after a stable run
// of trivial emits, and switches back if a later PTS regression reveals
// B-frames.
const dtsReorderWindow = 4

// dtsReorderAutoStableFrames is the number of consecutive trivial
// emits required for auto mode to flip into passthrough. At 30 fps this
// bounds the warm-up latency for genuinely B-frame-free streams at
// roughly two seconds.
const dtsReorderAutoStableFrames = 60

// dtsReorderGuardTicks is the minimum 90 kHz tick spacing the
// monotonicity guard inserts between consecutive emits. Sized to
// 90 (= 1 ms) so the FLV tag's millisecond timestamp field stays
// strictly increasing after the egress converts ticks to integer ms.
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

	// logger is optional and only used for buffered↔passthrough
	// transition logs.
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

// SetLogger attaches the optional transition logger.
func (r *dtsReorder) SetLogger(logger *slog.Logger) {
	r.logger = logger
}

// Push enqueues one sample and returns emits ready for the muxer. The
// slice is nil during buffered warm-up, usually length 1 in steady
// state, and may be longer when auto mode flips to passthrough and
// drains its residual queue.
func (r *dtsReorder) Push(s MediaSample) []dtsReorderEmit {
	if r.passthrough && r.haveLastEmitPTS &&
		int32(s.RTPTimestamp-r.lastEmitPTS) < 0 { //nolint:gosec // modular comparison
		// PTS regressed: keep this sample and refill the lookahead
		// window before emitting again.
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
		// Passthrough keeps zero added latency while still honoring
		// the DTS monotonicity floor carried over from buffered mode.
		return []dtsReorderEmit{r.directEmit(s)}
	}

	// Buffered path.
	r.queue = append(r.queue, dtsReorderEntry{pts90k: s.RTPTimestamp, sample: s})
	r.insertSorted(s.RTPTimestamp)
	if len(r.queue) <= r.cap {
		return nil
	}
	out := []dtsReorderEmit{r.emit()}
	// If this emit triggered passthrough, drain the residual lookahead
	// now so future pushes are truly zero-latency.
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

// directEmit produces a passthrough emit with CTS=0, applying only the
// monotonicity floor.
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

// emit pops the FIFO head and computes its (DTS, CTS). The candidate
// DTS is sortedPTS[0]; CTS clamp and the FLV-ms monotonicity guard keep
// the public invariants intact. In auto mode this also updates the
// passthrough stability counter.
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
		// A one-ms guard can push DTS just past head.PTS; clamp CTS so
		// FLV readers never see a negative composition offset.
		ctsTicks = 0
	}
	ctsMs := ctsTicks * 1000 / 90000

	// A trivial emit means the window found no out-of-order frame, so
	// enough of them in a row lets auto mode drop to passthrough.
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
