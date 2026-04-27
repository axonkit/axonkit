package broadcast

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// TestDTSReorderIBBPStream simulates the libx264 -bf 3 reorder pattern
// from the file-level docstring. Steady-state behavior: every emit
// has CTS ≥ 0 and the DTS series is strictly monotonic in the 90 kHz
// clock — the two properties downstream MP4 muxers (and ffmpeg's
// FLV→MP4 transcoder) enforce. Reconstructed PTS may differ by ≤ 1 ms
// from the input on B-frames whose PTS was equal to the previous
// emit's chosen DTS (the monotonicity guard nudges DTS by one tick
// to keep the series strictly increasing).
func TestDTSReorderIBBPStream(t *testing.T) {
	// PTS sequence in arrival (decode) order for IPBBBPBBB at 30 fps:
	// the encoder displays I0 B1 B2 B3 P4 B5 B6 B7 P8, so it has to
	// emit them as I0 P4 B1 B2 B3 P8 B5 B6 B7. Values in ms,
	// converted to 90 kHz ticks to match what mediasoup surfaces on
	// RTP after the ingress fix forwards PTS instead of DTS.
	ptsMs := []uint32{0, 132, 33, 66, 99, 264, 165, 198, 231}
	pictTypes := []byte{'I', 'P', 'B', 'B', 'B', 'P', 'B', 'B', 'B'}

	r := newDTSReorder(dtsReorderWindow)
	emits := make([]dtsReorderEmit, 0, len(ptsMs))
	for _, ms := range ptsMs {
		s := MediaSample{
			Kind:         SampleKindVideo,
			Codec:        CodecH264,
			RTPTimestamp: ms * 90,
		}
		emits = append(emits, r.Push(s)...)
	}
	emits = append(emits, r.Drain()...)

	if got, want := len(emits), len(ptsMs); got != want {
		t.Fatalf("emit count = %d, want %d", got, want)
	}

	for i, e := range emits {
		// Heads come out in the arrival (decode) order.
		if e.sample.RTPTimestamp != ptsMs[i]*90 {
			t.Errorf("emit[%d].pts = %d, want %d", i, e.sample.RTPTimestamp, ptsMs[i]*90)
		}
		// CTS must always be ≥ 0 (PTS ≥ DTS) — the property that
		// distinguishes the new algorithm from the legacy lookahead
		// + sorted-pop approach which produced negative CTS on
		// IPBB-style streams.
		if e.ctsMs < 0 {
			t.Errorf("emit[%d] (%c) cts=%d ms, expected ≥ 0",
				i, pictTypes[i], e.ctsMs)
		}
		// Reconstructed PTS must equal input within 1 ms (the
		// monotonicity-guard slip).
		dtsMs := uint32(uint64(e.dts90k) * 1000 / 90000)
		gotPTSms := int64(dtsMs) + int64(e.ctsMs)
		if delta := gotPTSms - int64(ptsMs[i]); delta < -1 || delta > 1 {
			t.Errorf("emit[%d] (%c) reconstructed pts=%d ms, want %d ms (delta %d)",
				i, pictTypes[i], gotPTSms, ptsMs[i], delta)
		}
	}

	// Strict DTS monotonicity in the source 90 kHz clock.
	for i := 1; i < len(emits); i++ {
		if int32(emits[i].dts90k-emits[i-1].dts90k) <= 0 {
			t.Errorf("emit[%d].dts (%d) <= emit[%d].dts (%d) — not monotonic",
				i, emits[i].dts90k, i-1, emits[i-1].dts90k)
		}
	}
}

// TestDTSReorderIPBBStream covers the libx264 -bf 3 -b_strategy 1
// adaptive output we observe on the testsrc input, which settles on
// max_b_frames=2 (IPBB cycles). The legacy "FIFO + sorted pop_min"
// reorder produced CTS=-33 here because P (decoded second) consumed
// the smallest unassigned PTS before the B frames could claim it.
// The new algorithm must yield CTS ≥ 0 for every frame.
func TestDTSReorderIPBBStream(t *testing.T) {
	ptsMs := []uint32{67, 200, 134, 167, 334, 267, 300, 467, 400, 434, 600}

	r := newDTSReorder(dtsReorderWindow)
	emits := make([]dtsReorderEmit, 0, len(ptsMs))
	for _, ms := range ptsMs {
		s := MediaSample{
			Kind:         SampleKindVideo,
			Codec:        CodecH264,
			RTPTimestamp: ms * 90,
		}
		emits = append(emits, r.Push(s)...)
	}
	emits = append(emits, r.Drain()...)
	if len(emits) != len(ptsMs) {
		t.Fatalf("emit count = %d, want %d", len(emits), len(ptsMs))
	}

	for i, e := range emits {
		if e.sample.RTPTimestamp != ptsMs[i]*90 {
			t.Errorf("emit[%d].pts = %d, want %d", i, e.sample.RTPTimestamp, ptsMs[i]*90)
		}
		if e.ctsMs < 0 {
			t.Errorf("emit[%d] cts=%d, expected ≥ 0", i, e.ctsMs)
		}
		dtsMs := uint32(uint64(e.dts90k) * 1000 / 90000)
		gotPTSms := int64(dtsMs) + int64(e.ctsMs)
		if delta := gotPTSms - int64(ptsMs[i]); delta < -1 || delta > 1 {
			t.Errorf("emit[%d] reconstructed pts=%d ms, want %d ms (delta %d)",
				i, gotPTSms, ptsMs[i], delta)
		}
	}
	for i := 1; i < len(emits); i++ {
		if int32(emits[i].dts90k-emits[i-1].dts90k) <= 0 {
			t.Errorf("emit[%d].dts (%d) <= emit[%d].dts (%d) — not monotonic",
				i, emits[i].dts90k, i-1, emits[i-1].dts90k)
		}
	}
}

// TestDTSReorderNoBFrames covers the SFU passthrough hot path: a
// strictly-monotonic PTS stream must yield CTS=0 throughout (the
// head's own PTS is always the smallest in any lookahead window).
func TestDTSReorderNoBFrames(t *testing.T) {
	r := newDTSReorder(dtsReorderWindow)
	const frames = 16
	emits := make([]dtsReorderEmit, 0, frames)
	for i := uint32(0); i < frames; i++ {
		s := MediaSample{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: i * 3000}
		emits = append(emits, r.Push(s)...)
	}
	emits = append(emits, r.Drain()...)
	if len(emits) != frames {
		t.Fatalf("emit count = %d, want %d", len(emits), frames)
	}
	for i, e := range emits {
		if e.ctsMs != 0 {
			t.Errorf("monotonic input emits[%d] cts=%d, want 0", i, e.ctsMs)
		}
	}
	for i := 1; i < len(emits); i++ {
		if int32(emits[i].dts90k-emits[i-1].dts90k) <= 0 {
			t.Errorf("dts not strictly monotonic at i=%d: %d <= %d",
				i, emits[i].dts90k, emits[i-1].dts90k)
		}
	}
}

// TestDTSReorderEqualPTSGuard confirms the strict-monotonicity guard
// for ms-rounding edge cases. Two frames with the same 90 kHz
// timestamp must yield two different DTS values one tick apart.
func TestDTSReorderEqualPTSGuard(t *testing.T) {
	r := newDTSReorder(2)
	emits := make([]dtsReorderEmit, 0, 4)
	pts := []uint32{1000, 1000, 2000, 3000}
	for _, p := range pts {
		s := MediaSample{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: p}
		emits = append(emits, r.Push(s)...)
	}
	emits = append(emits, r.Drain()...)
	for i := 1; i < len(emits); i++ {
		if int32(emits[i].dts90k-emits[i-1].dts90k) <= 0 {
			t.Errorf("dts not strictly monotonic at i=%d: %d <= %d",
				i, emits[i].dts90k, emits[i-1].dts90k)
		}
	}
}

// TestDTSReorderAutoFlipsToPassthroughOnMonotonic feeds the auto-mode
// buffer enough strictly-monotonic frames to trip the stable-streak
// counter, then asserts that the buffer (a) drains its residual
// lookahead window the moment it flips so steady-state latency is
// zero, and (b) emits each subsequent arrival immediately. This is
// the exact behavior callers asked for when they leave
// Destination.DTSReorder unset on a stream that has no B-frames.
func TestDTSReorderAutoFlipsToPassthroughOnMonotonic(t *testing.T) {
	r := newDTSReorderMode(dtsReorderWindow, dtsReorderModeAuto)

	// During warmup the first `cap` arrivals fill the queue (no
	// emits), and the next `dtsReorderAutoStableFrames` produce one
	// trivial emit each, the last of which should trigger the flip
	// and drain the residual `cap` entries in the same Push.
	emits := make([]dtsReorderEmit, 0)
	totalToPush := dtsReorderWindow + dtsReorderAutoStableFrames
	for i := 0; i < totalToPush; i++ {
		emits = append(emits, r.Push(MediaSample{
			Kind:         SampleKindVideo,
			Codec:        CodecH264,
			RTPTimestamp: uint32(i) * 3000, //nolint:gosec // small bounded loop
		})...)
	}
	if !r.passthrough {
		t.Fatalf("expected passthrough = true after %d trivial emits, got false", dtsReorderAutoStableFrames)
	}
	if got, want := len(emits), totalToPush; got != want {
		t.Fatalf("emit count post-flip = %d, want %d (drain must be inline)", got, want)
	}
	if n := len(r.queue); n != 0 {
		t.Fatalf("queue residual after flip = %d, want 0 (zero steady-state latency)", n)
	}

	// Push three more monotonic frames; each should produce exactly
	// one emit with CTS=0 and DTS=PTS.
	for i := totalToPush; i < totalToPush+3; i++ {
		got := r.Push(MediaSample{
			Kind:         SampleKindVideo,
			Codec:        CodecH264,
			RTPTimestamp: uint32(i) * 3000, //nolint:gosec // small bounded loop
		})
		if len(got) != 1 {
			t.Fatalf("post-flip Push %d returned %d emits, want 1", i, len(got))
		}
		if got[0].ctsMs != 0 {
			t.Errorf("post-flip emit cts=%d, want 0", got[0].ctsMs)
		}
		if got[0].dts90k != uint32(i)*3000 { //nolint:gosec // small bounded loop
			t.Errorf("post-flip emit dts=%d, want %d", got[0].dts90k, uint32(i)*3000) //nolint:gosec // small bounded loop
		}
	}
}

// TestDTSReorderAutoStaysBufferedOnBFrames replays an IPBB stream
// through auto mode and asserts the buffer never flips: each B-frame
// resets stableCount because head.PTS isn't the minimum of the
// lookahead window. This is the safety property that justifies
// shipping auto as the default — encoders that emit B-frames must
// never be silently degraded to the passthrough path.
func TestDTSReorderAutoStaysBufferedOnBFrames(t *testing.T) {
	r := newDTSReorderMode(dtsReorderWindow, dtsReorderModeAuto)

	// Long IPBB stream: 4 cycles of (P, B, B), each cycle 33 ms
	// apart, repeated for >> dtsReorderAutoStableFrames frames.
	// The first frame is an IDR.
	const cycles = 80
	ptsMs := []uint32{0}
	t0 := uint32(33)
	for c := 0; c < cycles; c++ {
		base := uint32(c)*100 + t0     //nolint:gosec // small bounded loop
		ptsMs = append(ptsMs, base+66) // P (display 3rd in cycle)
		ptsMs = append(ptsMs, base+0)  // B (display 1st)
		ptsMs = append(ptsMs, base+33) // B (display 2nd)
	}

	for _, ms := range ptsMs {
		_ = r.Push(MediaSample{
			Kind:         SampleKindVideo,
			Codec:        CodecH264,
			RTPTimestamp: ms * 90,
		})
		if r.passthrough {
			t.Fatalf("auto mode flipped to passthrough on a B-frame stream "+
				"(ptsMs=%d, stableCount=%d)", ms, r.stableCount)
		}
	}
}

// TestDTSReorderAutoFlipsBackOnRegression exercises the recovery
// path: after stabilizing and flipping into passthrough, a sudden
// PTS regression (encoder mid-stream B-frames re-enabled) must flip
// the buffer back into buffered mode. The first B-frame after the
// flip-back may pay the monotonicity-guard one-tick CTS clamp; what
// must NOT happen is silently dropping the reorder signal.
func TestDTSReorderAutoFlipsBackOnRegression(t *testing.T) {
	r := newDTSReorderMode(dtsReorderWindow, dtsReorderModeAuto)

	// Phase 1: feed enough monotonic frames to flip into passthrough.
	totalToPush := dtsReorderWindow + dtsReorderAutoStableFrames
	for i := 0; i < totalToPush; i++ {
		_ = r.Push(MediaSample{
			Kind:         SampleKindVideo,
			Codec:        CodecH264,
			RTPTimestamp: uint32(i) * 3000, //nolint:gosec // small bounded loop
		})
	}
	if !r.passthrough {
		t.Fatalf("phase 1 failed: did not enter passthrough")
	}

	// Phase 2: feed a regressed PTS that simulates a new B-frame
	// arriving — its PTS goes backward relative to the last emit.
	// Phase 1's last emit was at (totalToPush-1)*3000; pick a value
	// strictly less so the regression detector trips.
	regressed := uint32(totalToPush-1)*3000 - 1500 //nolint:gosec // small bounded computation
	got := r.Push(MediaSample{
		Kind:         SampleKindVideo,
		Codec:        CodecH264,
		RTPTimestamp: regressed,
	})
	if len(got) != 0 {
		t.Fatalf("regression Push returned %d emits, want 0 (must rebuffer)", len(got))
	}
	if r.passthrough {
		t.Fatalf("buffer still in passthrough after regression")
	}
	if len(r.queue) != 1 {
		t.Fatalf("queue depth after regression = %d, want 1 (the regressed frame)", len(r.queue))
	}
}

// TestDTSReorderTransitionLogs guards the only egress-side observability
// the buffer exposes: one Info line per buffered↔passthrough flip.
// Every other log line on the reorder path was deliberately dropped
// (per-frame Debug pollution); these two transitions are what
// operators rely on to confirm "auto mode actually adapted to my
// stream", so accidentally muting them in a future refactor must
// fail loud rather than silently regress observability.
func TestDTSReorderTransitionLogs(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	r := newDTSReorderMode(dtsReorderWindow, dtsReorderModeAuto)
	r.SetLogger(logger)

	// Phase 1: feed monotonic frames to trip the passthrough flip.
	totalToPush := dtsReorderWindow + dtsReorderAutoStableFrames
	for i := 0; i < totalToPush; i++ {
		_ = r.Push(MediaSample{
			Kind:         SampleKindVideo,
			Codec:        CodecH264,
			RTPTimestamp: uint32(i) * 3000, //nolint:gosec // small bounded loop
		})
	}
	out := buf.String()
	if !strings.Contains(out, "dts reorder switched to passthrough") {
		t.Fatalf("expected passthrough-flip log, got:\n%s", out)
	}
	if !strings.Contains(out, "no_b_frames_detected") {
		t.Fatalf("expected reason=no_b_frames_detected on flip log, got:\n%s", out)
	}

	// Phase 2: regress the PTS — same condition as the flip-back test.
	buf.Reset()
	regressed := uint32(totalToPush-1)*3000 - 1500 //nolint:gosec // small bounded computation
	_ = r.Push(MediaSample{
		Kind:         SampleKindVideo,
		Codec:        CodecH264,
		RTPTimestamp: regressed,
	})
	out = buf.String()
	if !strings.Contains(out, "dts reorder switched back to buffered") {
		t.Fatalf("expected buffered-flip log, got:\n%s", out)
	}
	if !strings.Contains(out, "pts_regression") {
		t.Fatalf("expected reason=pts_regression on flip-back log, got:\n%s", out)
	}
}
