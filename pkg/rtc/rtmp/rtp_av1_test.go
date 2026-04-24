package rtmp

import (
	"testing"

	"github.com/pion/rtp"
)

func TestParseAV1CodecConfigurationRecord_Main(t *testing.T) {
	// Main profile (0), level 5.1 (13), 8-bit 4:2:0, no presentation-delay.
	data := []byte{
		0x81, // marker=1 | version=1
		0x0D, // seq_profile=0, seq_level_idx_0=13
		0x0C, // tier=0, high_bitdepth=0, twelve=0, mono=0, subX=1, subY=1, pos=0
		0x00, // no initial_presentation_delay_present
	}
	rec, err := ParseAV1CodecConfigurationRecord(data)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if rec.SeqProfile != 0 {
		t.Fatalf("seq_profile = %d, want 0", rec.SeqProfile)
	}
	if rec.SeqLevelIdx0 != 13 {
		t.Fatalf("seq_level_idx_0 = %d, want 13", rec.SeqLevelIdx0)
	}
	if !rec.ChromaSubsamplingX || !rec.ChromaSubsamplingY {
		t.Fatalf("want 4:2:0 chroma, got subX=%v subY=%v", rec.ChromaSubsamplingX, rec.ChromaSubsamplingY)
	}
	if rec.InitialPresentationDelayPresent {
		t.Fatalf("did not expect initial_presentation_delay_present")
	}
}

func TestParseAV1CodecConfigurationRecord_PresentationDelay(t *testing.T) {
	data := []byte{
		0x81,
		0x21, // seq_profile=1, seq_level_idx_0=1
		0x00,
		0x1A, // initial_presentation_delay_present=1, value=10
	}
	rec, err := ParseAV1CodecConfigurationRecord(data)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if rec.SeqProfile != 1 {
		t.Fatalf("seq_profile = %d, want 1", rec.SeqProfile)
	}
	if !rec.InitialPresentationDelayPresent {
		t.Fatalf("initial_presentation_delay_present = false")
	}
	if rec.InitialPresentationDelayMinus1 != 10 {
		t.Fatalf("initial_presentation_delay_minus_one = %d, want 10", rec.InitialPresentationDelayMinus1)
	}
}

func TestParseAV1CodecConfigurationRecord_BadVersion(t *testing.T) {
	data := []byte{0x82, 0x00, 0x00, 0x00} // version=2 → unsupported
	if _, err := ParseAV1CodecConfigurationRecord(data); err == nil {
		t.Fatalf("expected error for av1C version 2")
	}
}

// makeAV1Frame returns a minimal valid AV1 low-overhead OBU bitstream
// containing a single FRAME OBU (type=6) with has_size_field=1 so
// pion's OBU parser accepts it.
//
//	byte 0 : obu header = 0 0110 0 1 0
//	         forbidden=0, type=6, extension=0, has_size=1, reserved=0 → 0x32
//	byte 1 : leb128 size
//	bytes 2..: opaque frame payload (size bytes)
func makeAV1Frame(size int) []byte {
	out := make([]byte, 2+size)
	out[0] = 0x32
	out[1] = byte(size)
	for i := 0; i < size; i++ {
		out[2+i] = byte(i + 1)
	}
	return out
}

func TestAV1Packetizer_SingleFrame(t *testing.T) {
	pk := NewAV1Packetizer(0xFEEDC0DE, 99)
	pk.SetMTU(1200)
	pkts, err := pk.Packetize(100_000, true, makeAV1Frame(50))
	if err != nil {
		t.Fatalf("packetize: %v", err)
	}
	if len(pkts) != 1 {
		t.Fatalf("want 1 packet, got %d", len(pkts))
	}
	var p rtp.Packet
	if err := p.Unmarshal(pkts[0]); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if p.SSRC != 0xFEEDC0DE || p.PayloadType != 99 || p.Timestamp != 100_000 {
		t.Fatalf("bad header: %+v", p.Header)
	}
	if !p.Marker {
		t.Fatalf("single-packet frame must have Marker=true")
	}
}

func TestAV1Packetizer_FragmentsSetMarkerOnlyOnLast(t *testing.T) {
	pk := NewAV1Packetizer(1, 99)
	// Force fragmentation — AV1 aggregation header is 1 byte, so MTU=30
	// leaves ~29 bytes per fragment.
	pk.SetMTU(30)
	pkts, err := pk.Packetize(0, true, makeAV1Frame(400))
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
