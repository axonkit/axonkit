package broadcast

import (
	"encoding/binary"
	"testing"

	"github.com/pion/rtp"
)

func TestH264DepacketizerAssemblesSingleNALU(t *testing.T) {
	var d H264Depacketizer

	// Build a single NALU (non-IDR slice, type=1) as an RTP payload. pion's
	// H264Packet.Unmarshal returns Annex-B bytes for single-NALU packets.
	nalu := []byte{0x01, 0xDE, 0xAD, 0xBE, 0xEF} // header byte: type=1 (slice)
	pkt := &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 9000},
		Payload: nalu,
	}

	s, ok := d.Push(pkt)
	if !ok {
		t.Fatalf("expected sample, got none")
	}
	if s.Codec != CodecH264 {
		t.Errorf("codec = %v, want H264", s.Codec)
	}
	if s.RTPTimestamp != 9000 {
		t.Errorf("ts = %d, want 9000", s.RTPTimestamp)
	}
	if s.IsKeyFrame {
		t.Errorf("non-IDR should not be key")
	}
	// Expect AVCC framing: 4-byte length prefix + NALU bytes.
	if len(s.Data) != 4+len(nalu) {
		t.Fatalf("data len = %d, want %d", len(s.Data), 4+len(nalu))
	}
	length := binary.BigEndian.Uint32(s.Data[:4])
	if int(length) != len(nalu) {
		t.Errorf("length prefix = %d, want %d", length, len(nalu))
	}
}

func TestH264DepacketizerExtradataBuildsAfterSPSPPS(t *testing.T) {
	var d H264Depacketizer

	// Construct SPS (type=7): header=0x67 + minimal profile bytes.
	sps := []byte{0x67, 0x42, 0x00, 0x1F, 0x00, 0x11}
	// PPS (type=8): header=0x68.
	pps := []byte{0x68, 0xCE, 0x3C, 0x80}
	// IDR (type=5): header=0x65.
	idr := []byte{0x65, 0xAA, 0xBB, 0xCC}

	for _, payload := range [][]byte{sps, pps, idr} {
		pkt := &rtp.Packet{
			Header:  rtp.Header{Marker: false, Timestamp: 9000},
			Payload: payload,
		}
		_, _ = d.Push(pkt)
	}
	// Marker on last packet to flush the AU.
	pkt := &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 9000},
		Payload: []byte{0x65, 0xDE, 0xAD}, // trailing IDR
	}
	s, ok := d.Push(pkt)
	if !ok {
		t.Fatalf("no sample emitted")
	}
	if !s.IsKeyFrame {
		t.Errorf("expected key frame")
	}

	avcc := d.ExtradataAVCC()
	if avcc == nil {
		t.Fatalf("expected avcC extradata")
	}
	if avcc[0] != 0x01 {
		t.Errorf("avcC version = 0x%02X, want 0x01", avcc[0])
	}
	if !d.HasConfig() {
		t.Errorf("HasConfig should be true")
	}
	if !d.SeenIDR() {
		t.Errorf("SeenIDR should be true")
	}
}
