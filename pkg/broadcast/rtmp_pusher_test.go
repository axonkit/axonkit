package broadcast

import (
	"bytes"
	"testing"
)

// newTestPusher constructs an rtmpPusher with a stub URL but without
// calling Start, so enqueue just appends to the internal channel and
// tests can assert on what was produced. The channel is sized like the
// production default so a burst of enqueues doesn't block.
func newTestPusher(t *testing.T) *rtmpPusher {
	t.Helper()
	p, err := newRTMPPusher("rtmp://localhost/app/stream", nil)
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
