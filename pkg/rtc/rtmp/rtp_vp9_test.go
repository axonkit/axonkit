package rtmp

import (
	"testing"

	"github.com/pion/rtp"
)

func TestParseVPCodecConfigurationRecord_Empty(t *testing.T) {
	rec, err := ParseVPCodecConfigurationRecord(nil)
	if err != nil {
		t.Fatalf("empty vpcC should parse: %v", err)
	}
	if rec.Profile != 0 {
		t.Fatalf("empty vpcC profile = %d, want 0", rec.Profile)
	}
}

func TestParseVPCodecConfigurationRecord_Profile2(t *testing.T) {
	// Each byte follows the vpcC layout documented on
	// VPCodecConfigurationRecord. codecInitializationDataSize (u16 BE)
	// is required to be zero by the current spec.
	data := []byte{
		0x02,
		0x1F,
		0xA2,
		0x09,
		0x10,
		0x09,
		0x00, 0x00,
	}
	rec, err := ParseVPCodecConfigurationRecord(data)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if rec.Profile != 2 {
		t.Fatalf("profile = %d, want 2", rec.Profile)
	}
	if rec.BitDepth != 10 {
		t.Fatalf("bit_depth = %d, want 10", rec.BitDepth)
	}
	if rec.ColorPrimaries != 9 {
		t.Fatalf("color_primaries = %d, want 9", rec.ColorPrimaries)
	}
}

func TestParseVPCodecConfigurationRecord_Short(t *testing.T) {
	// <8 bytes but non-empty: reject explicitly so the session does not
	// silently skip a malformed publisher.
	if _, err := ParseVPCodecConfigurationRecord([]byte{0x00, 0x1F}); err == nil {
		t.Fatalf("short non-empty vpcC should error")
	}
}

// makeVP9InterFrame returns a byte slice whose first byte is a minimal
// valid VP9 non-keyframe header so pion's non-flexible VP9Payloader
// accepts it. The remaining bytes are opaque filler for the test.
//
// Bitstream per VP9 spec v0.6 §6.2.1 uncompressed_header():
//
//	frame_marker(2b)=10, profile_low(1b)=0, profile_high(1b)=0,
//	show_existing_frame(1b)=0, non_key_frame(1b)=1, show_frame(1b)=1,
//	error_resilient_mode(1b)=0  →  10 0 0 0 1 1 0 = 0x86
func makeVP9InterFrame(size int) []byte {
	if size < 1 {
		size = 1
	}
	out := make([]byte, size)
	out[0] = 0x86
	for i := 1; i < size; i++ {
		out[i] = byte(i)
	}
	return out
}

func TestVP9Packetizer_SinglePacket(t *testing.T) {
	pk := NewVP9Packetizer(0xDEADBEEF, 98)
	pk.SetMTU(1200)
	frame := makeVP9InterFrame(42)
	pkts, err := pk.Packetize(42_000, false, frame)
	if err != nil {
		t.Fatalf("packetize: %v", err)
	}
	if len(pkts) != 1 {
		t.Fatalf("small frame should fit one packet, got %d", len(pkts))
	}
	var p rtp.Packet
	if err := p.Unmarshal(pkts[0]); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if p.SSRC != 0xDEADBEEF || p.PayloadType != 98 || p.Timestamp != 42_000 {
		t.Fatalf("bad header: %+v", p.Header)
	}
	if !p.Marker {
		t.Fatalf("single-packet frame must have Marker=true")
	}
}

func TestVP9Packetizer_FragmentsSetMarkerOnlyOnLast(t *testing.T) {
	pk := NewVP9Packetizer(1, 98)
	// MTU=40 → ~37 bytes of payload per fragment (3-byte VP9 RTP header).
	pk.SetMTU(40)
	frame := makeVP9InterFrame(500)
	pkts, err := pk.Packetize(0, false, frame)
	if err != nil {
		t.Fatalf("packetize: %v", err)
	}
	if len(pkts) < 2 {
		t.Fatalf("expected multiple fragments, got %d", len(pkts))
	}
	for i, raw := range pkts {
		var p rtp.Packet
		if err := p.Unmarshal(raw); err != nil {
			t.Fatalf("unmarshal[%d]: %v", i, err)
		}
		isLast := i == len(pkts)-1
		if p.Marker != isLast {
			t.Fatalf("packet[%d].Marker = %v, want %v", i, p.Marker, isLast)
		}
	}
}

func TestVP9Packetizer_SequenceNumbersAdvance(t *testing.T) {
	pk := NewVP9Packetizer(1, 98)
	_, _ = pk.Packetize(0, false, makeVP9InterFrame(20))
	startSeq := pk.SequenceNumber()
	_, _ = pk.Packetize(90_000, false, makeVP9InterFrame(20))
	if pk.SequenceNumber() == startSeq {
		t.Fatalf("sequence did not advance")
	}
}
