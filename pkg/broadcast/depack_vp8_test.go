package broadcast

import (
	"testing"

	"github.com/pion/rtp"
)

// TestVP8DepacketizerKeyframeFromSinglePacket exercises the most
// common shape: one VP8 frame fits in a single RTP packet, S=1 PID=0,
// marker=true. The first byte of the VP8 frame's tag has bit 0 = 0
// for keyframes (RFC 6386 §9.1).
func TestVP8DepacketizerKeyframeFromSinglePacket(t *testing.T) {
	var d VP8Depacketizer

	// VP8 RTP payload descriptor (1 byte): X=0 R=0 N=0 S=1 R=0 PID=0
	// → 0x10. Followed by a tiny keyframe payload whose first byte's
	// LSB is 0 (key) and rest is filler.
	pkt := &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 12345},
		Payload: []byte{0x10, 0x00, 0xDE, 0xAD, 0xBE, 0xEF},
	}

	s, ok := d.Push(pkt)
	if !ok {
		t.Fatalf("expected sample, got none")
	}
	if s.Codec != CodecVP8 {
		t.Errorf("codec=%v, want CodecVP8", s.Codec)
	}
	if s.RTPTimestamp != 12345 {
		t.Errorf("ts=%d, want 12345", s.RTPTimestamp)
	}
	if !s.IsKeyFrame {
		t.Errorf("expected key frame")
	}
	if len(s.Data) != 5 {
		t.Errorf("data len=%d, want 5", len(s.Data))
	}
	if !d.HasConfig() || !d.SeenIDR() {
		t.Errorf("HasConfig/SeenIDR = %v/%v, want both true", d.HasConfig(), d.SeenIDR())
	}
	if rec := d.ConfigRecord(); len(rec) == 0 {
		t.Errorf("ConfigRecord should be non-empty after a keyframe")
	}
}

// TestVP8DepacketizerInterFrame ensures that an interframe (LSB=1 on
// the first VP8 frame byte) is reported as IsKeyFrame=false, but
// HasConfig stays true because we already saw a key frame earlier.
func TestVP8DepacketizerInterFrame(t *testing.T) {
	var d VP8Depacketizer

	// Prime with a keyframe first.
	keyPkt := &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 1000},
		Payload: []byte{0x10, 0x00, 0x01, 0x02},
	}
	if _, ok := d.Push(keyPkt); !ok {
		t.Fatalf("priming keyframe did not flush")
	}

	// Inter-frame: same descriptor, but the frame byte's LSB is 1.
	interPkt := &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 4000},
		Payload: []byte{0x10, 0x01, 0xAA},
	}
	s, ok := d.Push(interPkt)
	if !ok {
		t.Fatalf("expected sample for interframe")
	}
	if s.IsKeyFrame {
		t.Errorf("interframe must not be flagged as key")
	}
	if s.RTPTimestamp != 4000 {
		t.Errorf("ts=%d, want 4000", s.RTPTimestamp)
	}
	if !d.SeenIDR() {
		t.Errorf("SeenIDR should remain true after observing earlier keyframe")
	}
}

// TestVP8DepacketizerMultiPacketFrame ensures fragments concatenate
// across RTP packets and only flush on Marker.
func TestVP8DepacketizerMultiPacketFrame(t *testing.T) {
	var d VP8Depacketizer

	// First fragment: S=1 PID=0, no marker. 0x10 descriptor, frame
	// byte bit 0 = 0 (key), then a few bytes.
	frag1 := &rtp.Packet{
		Header:  rtp.Header{Marker: false, Timestamp: 9000},
		Payload: []byte{0x10, 0x00, 0xAA, 0xBB},
	}
	if s, ok := d.Push(frag1); ok {
		t.Fatalf("first fragment must not flush, got sample %+v", s)
	}

	// Second (continuation) fragment: S=0 PID=0 = 0x00 descriptor.
	// Marker=true terminates the frame.
	frag2 := &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 9000},
		Payload: []byte{0x00, 0xCC, 0xDD, 0xEE},
	}
	s, ok := d.Push(frag2)
	if !ok {
		t.Fatalf("second fragment should flush on Marker")
	}
	if !s.IsKeyFrame {
		t.Errorf("expected key frame")
	}
	// Frame body = frag1 payload (after descriptor) + frag2 payload (after descriptor).
	want := []byte{0x00, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE}
	if string(s.Data) != string(want) {
		t.Errorf("data=% X, want % X", s.Data, want)
	}
}
