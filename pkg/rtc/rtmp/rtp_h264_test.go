package rtmp

import (
	"encoding/binary"
	"testing"

	"github.com/pion/rtp"
)

// writeAVCC builds a fake AVCC NALU with the given NALU bytes so tests can
// drive H264Packetizer.Packetize without pulling in a real encoder.
func writeAVCC(nalus ...[]byte) []byte {
	var out []byte
	for _, n := range nalus {
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(n)))
		out = append(out, lenBuf[:]...)
		out = append(out, n...)
	}
	return out
}

func TestH264Packetizer_KeyframeEmitsSTAPA(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00, 0x1F} // NALU type 7 (SPS)
	pps := []byte{0x68, 0xCE, 0x38, 0x80} // NALU type 8 (PPS)
	idr := []byte{0x65, 0x88, 0x84, 0x00} // NALU type 5 (IDR)

	pk := NewH264Packetizer(0xCAFEBABE, 125)
	pk.SetParameterSets(sps, pps)
	pk.SetMTU(1200)

	pkts, err := pk.Packetize(90_000, true, writeAVCC(idr))
	if err != nil {
		t.Fatalf("packetize: %v", err)
	}
	if len(pkts) < 2 {
		t.Fatalf("expected STAP-A + IDR packets, got %d", len(pkts))
	}

	// First packet should be STAP-A (NALU type 24 = 0x18). The STAP-A
	// header masks to 0x1F → 0x18.
	var first rtp.Packet
	if err := first.Unmarshal(pkts[0]); err != nil {
		t.Fatalf("unmarshal first: %v", err)
	}
	if got := first.Payload[0] & 0x1F; got != 24 {
		t.Fatalf("first payload nalType = %d, want 24 (STAP-A)", got)
	}
	if first.SSRC != 0xCAFEBABE {
		t.Fatalf("ssrc = %x", first.SSRC)
	}
	if first.PayloadType != 125 {
		t.Fatalf("pt = %d", first.PayloadType)
	}

	// Last packet must have Marker=true (end of access unit).
	var last rtp.Packet
	if err := last.Unmarshal(pkts[len(pkts)-1]); err != nil {
		t.Fatalf("unmarshal last: %v", err)
	}
	if !last.Marker {
		t.Fatalf("last packet should have Marker=true")
	}
}

func TestH264Packetizer_SequenceNumbersMonotonic(t *testing.T) {
	pk := NewH264Packetizer(1, 100)
	pk.SetParameterSets([]byte{0x67, 0x42, 0x00, 0x1F}, []byte{0x68, 0xCE})
	// Same tiny frame twice → at least two packets with two distinct seq#s.
	_, err := pk.Packetize(0, true, writeAVCC([]byte{0x65, 0x88}))
	if err != nil {
		t.Fatalf("packetize #1: %v", err)
	}
	startSeq := pk.SequenceNumber()
	pkts, err := pk.Packetize(90_000, false, writeAVCC([]byte{0x41, 0xE1}))
	if err != nil {
		t.Fatalf("packetize #2: %v", err)
	}
	if len(pkts) == 0 {
		t.Fatalf("second frame produced no packets")
	}
	if pk.SequenceNumber() == startSeq {
		t.Fatalf("sequence did not advance after second frame")
	}
}
