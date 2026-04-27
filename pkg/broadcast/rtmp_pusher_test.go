package broadcast

import (
	"bytes"
	"testing"
)

// newTestPusher constructs an rtmpPusher with a stub URL but without
// calling Start, so enqueue just appends to the internal channel and
// tests can assert on what was produced. The channel is sized like the
// production default so a burst of enqueues doesn't block.
//
// DTSReorderMode defaults to "always" (pin buffered) so tests that
// only care about the seq-header / muxing logic don't accidentally
// trip the auto-mode passthrough flip after dtsReorderAutoStableFrames
// trivial emits. Tests that exercise specific modes go through
// newTestPusherOpts.
func newTestPusher(t *testing.T) *rtmpPusher {
	t.Helper()
	return newTestPusherOpts(t, rtmpPusherOptions{DTSReorderMode: DTSReorderAlways})
}

// newTestPusherOpts is the option-bearing variant for tests that
// need to flip knobs (currently only DTSReorderMode).
func newTestPusherOpts(t *testing.T, opts rtmpPusherOptions) *rtmpPusher {
	t.Helper()
	p, err := newRTMPPusher("rtmp://localhost/app/stream", nil, opts)
	if err != nil {
		t.Fatalf("newRTMPPusher: %v", err)
	}
	return p
}

// drainSeqHeaderBodies returns the bodies of the AVC-style SeqHeader
// tags currently sitting in the queue. It also counts NALU tags so the
// test can assert ordering (seq header before any frame data).
func drainSeqHeaderBodies(t *testing.T, p *rtmpPusher) (seqBodies [][]byte, nalTags int) {
	t.Helper()
	for {
		select {
		case f := <-p.queue:
			if f.kind != SampleKindVideo {
				continue
			}
			if len(f.body) < 2 {
				continue
			}
			// Classic FLV: byte 1 = AVCPacketType (0=seq, 1=NALU, 2=end).
			// E-RTMP: byte 0 IsExHeader bit set; byte 0 low 4 bits = packetType.
			if f.body[0]&0x80 != 0 {
				packetType := f.body[0] & 0x0F
				if packetType == flvExVideoPacketSeqStart {
					seqBodies = append(seqBodies, append([]byte{}, f.body...))
				} else {
					nalTags++
				}
				continue
			}
			switch f.body[1] {
			case flvAVCPacketTypeSeqHeader:
				seqBodies = append(seqBodies, append([]byte{}, f.body...))
			case flvAVCPacketTypeNALU:
				nalTags++
			}
		default:
			return
		}
	}
}

// TestEnsureVideoSeqHeader_SkipIdenticalAfterReset verifies the
// recorder-stability contract: after ResetVideoHeader (issued by the
// switcher on every live↔mix flip), an EnsureVideoSeqHeader call with
// byte-identical AVCC bytes must NOT enqueue a second SeqHeader tag.
// ffmpeg's RTMP demuxer drops the TCP connection when it sees a
// mid-stream SeqHeader, so the skip path is what keeps verify.sh's
// recorder loop on a single FLV segment.
func TestEnsureVideoSeqHeader_SkipIdenticalAfterReset(t *testing.T) {
	p := newTestPusher(t)
	defer close(p.queue)

	avcc := []byte{0x01, 0x42, 0xC0, 0x1E, 0xFF, 0xE1, 0x00, 0x05, 0x67, 0x42, 0xC0, 0x1E, 0x99}

	if !p.EnsureVideoSeqHeader(avcc) {
		t.Fatalf("first EnsureVideoSeqHeader: want true, got false")
	}
	p.ResetVideoHeader()
	if p.EnsureVideoSeqHeader(avcc) {
		t.Fatalf("second EnsureVideoSeqHeader with identical bytes: want false (skipped), got true")
	}

	seqBodies, _ := drainSeqHeaderBodies(t, p)
	if len(seqBodies) != 1 {
		t.Fatalf("expected exactly 1 SeqHeader tag, got %d", len(seqBodies))
	}
}

// TestEnsureVideoSeqHeader_EmitOnChangeAfterReset covers the opposite
// leg: when the live↔mix flip carries a genuinely different codec
// config, a fresh SeqHeader tag must still be enqueued so decoders
// reconfigure before the first post-flip keyframe is written out.
func TestEnsureVideoSeqHeader_EmitOnChangeAfterReset(t *testing.T) {
	p := newTestPusher(t)
	defer close(p.queue)

	first := []byte{0x01, 0x42, 0xC0, 0x1E, 0xFF, 0xE1, 0x00, 0x05, 0x67, 0x42, 0xC0, 0x1E, 0x99}
	second := []byte{0x01, 0x64, 0x00, 0x1F, 0xFF, 0xE1, 0x00, 0x05, 0x67, 0x64, 0x00, 0x1F, 0xAB}

	if !p.EnsureVideoSeqHeader(first) {
		t.Fatalf("first EnsureVideoSeqHeader: want true")
	}
	p.ResetVideoHeader()
	if !p.EnsureVideoSeqHeader(second) {
		t.Fatalf("second EnsureVideoSeqHeader with different bytes: want true, got false")
	}

	seqBodies, _ := drainSeqHeaderBodies(t, p)
	if len(seqBodies) != 2 {
		t.Fatalf("expected 2 SeqHeader tags, got %d", len(seqBodies))
	}
	if bytes.Equal(seqBodies[0], seqBodies[1]) {
		t.Fatalf("two SeqHeader tags should differ in body bytes")
	}
}

// TestEnsureExVideoSeqHeader_SkipIdenticalAfterReset is the E-RTMP
// counterpart of the AVC skip test — VP9 / AV1 SequenceStart tags are
// structured differently but the skip-on-reset contract is identical.
func TestEnsureExVideoSeqHeader_SkipIdenticalAfterReset(t *testing.T) {
	p := newTestPusher(t)
	defer close(p.queue)

	config := []byte{0x00, 0x00, 0x82, 0x01, 0x01, 0x01, 0x00, 0x00}

	if !p.EnsureExVideoSeqHeader(flvExVideoFourCCVP9, config) {
		t.Fatalf("first EnsureExVideoSeqHeader: want true")
	}
	p.ResetVideoHeader()
	if p.EnsureExVideoSeqHeader(flvExVideoFourCCVP9, config) {
		t.Fatalf("second EnsureExVideoSeqHeader with identical bytes: want false (skipped)")
	}
	seqBodies, _ := drainSeqHeaderBodies(t, p)
	if len(seqBodies) != 1 {
		t.Fatalf("expected exactly 1 E-RTMP SeqHeader tag, got %d", len(seqBodies))
	}
}

// TestEnsureExVideoSeqHeader_EmitOnFourCCChange makes sure a codec
// change (e.g. VP9 → AV1) is treated as "emit fresh SeqHeader" even
// when the config bytes happen to collide — the FourCC difference is
// what the pusher keys off.
func TestEnsureExVideoSeqHeader_EmitOnFourCCChange(t *testing.T) {
	p := newTestPusher(t)
	defer close(p.queue)

	config := []byte{0x00, 0x00, 0x82, 0x01, 0x01, 0x01, 0x00, 0x00}

	if !p.EnsureExVideoSeqHeader(flvExVideoFourCCVP9, config) {
		t.Fatalf("first EnsureExVideoSeqHeader (vp09): want true")
	}
	p.ResetVideoHeader()
	if !p.EnsureExVideoSeqHeader(flvExVideoFourCCAV1, config) {
		t.Fatalf("second EnsureExVideoSeqHeader (av01, same config bytes): want true, got false")
	}
	seqBodies, _ := drainSeqHeaderBodies(t, p)
	if len(seqBodies) != 2 {
		t.Fatalf("expected 2 SeqHeader tags on FourCC change, got %d", len(seqBodies))
	}
}

// TestDestinationDTSReorderModeDefault pins the public helper's
// "absent → auto, true → always, false → never" mapping. Other code
// in this package (and any API client doing pre-flight validation)
// reads this value, so an accidental remap would silently change
// behavior for every existing caller.
func TestDestinationDTSReorderModeDefault(t *testing.T) {
	cases := []struct {
		name string
		val  *bool
		want DTSReorderMode
	}{
		{"nil-default", nil, DTSReorderAuto},
		{"explicit-true", boolPtr(true), DTSReorderAlways},
		{"explicit-false", boolPtr(false), DTSReorderNever},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			d := Destination{Type: DestinationRTMP, URL: "rtmp://x/y", DTSReorder: c.val}
			if got := d.DTSReorderMode(); got != c.want {
				t.Fatalf("DTSReorderMode = %v, want %v", got, c.want)
			}
		})
	}
}

// TestWriteSampleH264_DTSReorderNever exercises the direct egress
// path: every H.264 sample lands in the queue immediately (no window
// delay), the AVC tag has CTS=0, and the FLV tag DTS equals the
// sample's RTP timestamp expressed in milliseconds-from-first.
//
// This is the steady-state path operators get when they pin
// DTSReorder=false on a destination — it must avoid the reorder
// buffer entirely so latency-sensitive pipelines feeding
// B-frame-free encoders don't pay the buffering tax.
func TestWriteSampleH264_DTSReorderNever(t *testing.T) {
	p := newTestPusherOpts(t, rtmpPusherOptions{DTSReorderMode: DTSReorderNever})
	defer close(p.queue)

	if p.videoReorder != nil {
		t.Fatalf("videoReorder should be nil when DTSReorderMode=Never")
	}

	// Push three frames at 90 kHz: 0 ms, 33 ms, 67 ms. Decode order
	// matches presentation order (no B-frames, the reason callers
	// disable reorder).
	samples := []MediaSample{
		{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 0, IsKeyFrame: true, Data: []byte{0xAA}},
		{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 2970, IsKeyFrame: false, Data: []byte{0xBB}},
		{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 6030, IsKeyFrame: false, Data: []byte{0xCC}},
	}
	for _, s := range samples {
		p.WriteSample(s)
	}

	wantTSMs := []uint32{0, 33, 67}
	for i := range samples {
		select {
		case f := <-p.queue:
			if f.kind != SampleKindVideo {
				t.Fatalf("frame %d: kind = %v, want video", i, f.kind)
			}
			if got := f.timestampMs; got != wantTSMs[i] {
				t.Fatalf("frame %d: timestampMs = %d, want %d (PTS→DTS direct map)", i, got, wantTSMs[i])
			}
			// FLV AVC NALU body layout: [0]=FrameType|CodecID,
			// [1]=AVCPacketType, [2..5)=CTS SI24 BE.
			if f.body[1] != flvAVCPacketTypeNALU {
				t.Fatalf("frame %d: AVCPacketType = %#x, want NALU(%#x)", i, f.body[1], flvAVCPacketTypeNALU)
			}
			cts := int32(uint32(f.body[2])<<16 | uint32(f.body[3])<<8 | uint32(f.body[4]))
			if cts != 0 {
				t.Fatalf("frame %d: CTS = %d, want 0 (no reorder)", i, cts)
			}
		default:
			t.Fatalf("frame %d: queue empty, expected immediate emit (no window delay)", i)
		}
	}

	// No trailing tags should remain — the direct path emits
	// exactly one tag per WriteSample.
	select {
	case f := <-p.queue:
		t.Fatalf("unexpected extra frame in queue: kind=%v body=%v", f.kind, f.body)
	default:
	}
}

// TestWriteSampleH264_DTSReorderAlwaysBuffersWindow guards the
// "always" mode: the default operator-pinned reorder must absorb the
// first `dtsReorderWindow` samples before emitting anything. Tests in
// dts_reorder_test.go already cover the algorithm itself; this one
// just makes sure newRTMPPusher actually wires the buffer in when
// the mode is Always.
func TestWriteSampleH264_DTSReorderAlwaysBuffersWindow(t *testing.T) {
	p := newTestPusherOpts(t, rtmpPusherOptions{DTSReorderMode: DTSReorderAlways})
	defer close(p.queue)

	if p.videoReorder == nil {
		t.Fatalf("videoReorder should be non-nil when DTSReorderMode=Always")
	}

	// Push exactly window samples — buffer is filling, no emits.
	for i := 0; i < dtsReorderWindow; i++ {
		p.WriteSample(MediaSample{
			Kind:         SampleKindVideo,
			Codec:        CodecH264,
			RTPTimestamp: uint32(i) * 3000, //nolint:gosec // small bounded loop
			IsKeyFrame:   i == 0,
			Data:         []byte{byte(i)},
		})
	}
	select {
	case f := <-p.queue:
		t.Fatalf("expected no frame after window samples, got kind=%v ts=%d", f.kind, f.timestampMs)
	default:
	}

	// (window+1)-th sample pops the head.
	p.WriteSample(MediaSample{
		Kind: SampleKindVideo, Codec: CodecH264,
		RTPTimestamp: uint32(dtsReorderWindow) * 3000, //nolint:gosec // small bounded value
		Data:         []byte{0xFF},
	})
	select {
	case f := <-p.queue:
		if f.kind != SampleKindVideo {
			t.Fatalf("first emit kind = %v, want video", f.kind)
		}
	default:
		t.Fatalf("expected one emit after window+1 samples, queue empty")
	}
}

// TestWriteSampleH265_DTSReorderNeverWritesCodedFrames mirrors the
// H.264 direct-path test for HEVC. Egress must wrap each sample in
// the Enhanced-RTMP CodedFrames (PacketType=1) tag with a SI24 CTS=0
// field and a "hvc1" FourCC, and the FLV millisecond timestamp must
// equal the per-stream PTS→ms mapping (no buffering). The wire layout
// is what makes the recording side ffmpeg parse the HEVC stream the
// same way it parses an AVC stream — getting the header byte wrong
// would silently flip the recorder over to CodedFramesX.
func TestWriteSampleH265_DTSReorderNeverWritesCodedFrames(t *testing.T) {
	p := newTestPusherOpts(t, rtmpPusherOptions{DTSReorderMode: DTSReorderNever})
	defer close(p.queue)

	if p.videoReorder != nil {
		t.Fatalf("videoReorder should be nil when DTSReorderMode=Never")
	}

	samples := []MediaSample{
		{Kind: SampleKindVideo, Codec: CodecH265, RTPTimestamp: 0, IsKeyFrame: true, Data: []byte{0x40, 0x01}},
		{Kind: SampleKindVideo, Codec: CodecH265, RTPTimestamp: 2970, IsKeyFrame: false, Data: []byte{0x42, 0x02}},
		{Kind: SampleKindVideo, Codec: CodecH265, RTPTimestamp: 6030, IsKeyFrame: false, Data: []byte{0x42, 0x03}},
	}
	for _, s := range samples {
		p.WriteSample(s)
	}

	wantTSMs := []uint32{0, 33, 67}
	wantHeaderByte := []byte{0x91, 0xA1, 0xA1} // key|CodedFrames, inter|CodedFrames, inter|CodedFrames
	for i, s := range samples {
		select {
		case f := <-p.queue:
			if f.kind != SampleKindVideo {
				t.Fatalf("frame %d: kind = %v, want video", i, f.kind)
			}
			if got := f.timestampMs; got != wantTSMs[i] {
				t.Fatalf("frame %d: timestampMs = %d, want %d (PTS→DTS direct map)", i, got, wantTSMs[i])
			}
			if f.body[0] != wantHeaderByte[i] {
				t.Fatalf("frame %d: header byte = 0x%02X, want 0x%02X", i, f.body[0], wantHeaderByte[i])
			}
			if string(f.body[1:5]) != flvExVideoFourCCH265 {
				t.Fatalf("frame %d: FourCC = %q, want %q", i, string(f.body[1:5]), flvExVideoFourCCH265)
			}
			cts := int32(uint32(f.body[5])<<16 | uint32(f.body[6])<<8 | uint32(f.body[7]))
			if cts != 0 {
				t.Fatalf("frame %d: CTS = %d, want 0 (no reorder)", i, cts)
			}
			if !bytes.Equal(f.body[8:], s.Data) {
				t.Fatalf("frame %d: payload mismatch", i)
			}
		default:
			t.Fatalf("frame %d: queue empty, expected immediate emit (no window delay)", i)
		}
	}

	select {
	case f := <-p.queue:
		t.Fatalf("unexpected extra frame in queue: kind=%v body=%v", f.kind, f.body)
	default:
	}
}

// TestWriteSampleH265_DTSReorderAlwaysBuffersWindow guards that the
// reorder buffer is wired up for HEVC the same way it is for AVC:
// pinned-Always mode must absorb the first dtsReorderWindow samples
// before the first emit appears in the queue, regardless of codec.
// dts_reorder_test.go covers the algorithm itself; this is the
// codec-routing assertion.
func TestWriteSampleH265_DTSReorderAlwaysBuffersWindow(t *testing.T) {
	p := newTestPusherOpts(t, rtmpPusherOptions{DTSReorderMode: DTSReorderAlways})
	defer close(p.queue)

	if p.videoReorder == nil {
		t.Fatalf("videoReorder should be non-nil when DTSReorderMode=Always")
	}

	for i := 0; i < dtsReorderWindow; i++ {
		p.WriteSample(MediaSample{
			Kind:         SampleKindVideo,
			Codec:        CodecH265,
			RTPTimestamp: uint32(i) * 3000, //nolint:gosec // small bounded loop
			IsKeyFrame:   i == 0,
			Data:         []byte{0x40, byte(i)},
		})
	}
	select {
	case f := <-p.queue:
		t.Fatalf("expected no frame after window samples, got kind=%v ts=%d", f.kind, f.timestampMs)
	default:
	}

	p.WriteSample(MediaSample{
		Kind: SampleKindVideo, Codec: CodecH265,
		RTPTimestamp: uint32(dtsReorderWindow) * 3000, //nolint:gosec // small bounded value
		Data:         []byte{0x42, 0xFF},
	})
	select {
	case f := <-p.queue:
		if f.kind != SampleKindVideo {
			t.Fatalf("first emit kind = %v, want video", f.kind)
		}
		// The first emit must use the HEVC CodedFrames layout, not
		// the AVC layout — proves the codec branch survived the
		// reorder buffer round trip.
		if f.body[0]&0x80 == 0 {
			t.Fatalf("first emit header byte = 0x%02X, want IsExHeader bit set (HEVC CodedFrames)", f.body[0])
		}
		if string(f.body[1:5]) != flvExVideoFourCCH265 {
			t.Fatalf("first emit FourCC = %q, want %q", string(f.body[1:5]), flvExVideoFourCCH265)
		}
	default:
		t.Fatalf("expected one emit after window+1 samples, queue empty")
	}
}

func boolPtr(b bool) *bool { return &b }
