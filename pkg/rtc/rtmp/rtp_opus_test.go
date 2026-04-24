package rtmp

import (
	"testing"

	"github.com/pion/rtp"
)

func TestOpusPacketizerSequence(t *testing.T) {
	pk := NewOpusPacketizer(0xBEEFCAFE, 111)
	a, err := pk.PacketizeFrame(0, []byte{0x01, 0x02, 0x03})
	if err != nil {
		t.Fatalf("packetize: %v", err)
	}
	b, err := pk.PacketizeFrame(960, []byte{0x04, 0x05})
	if err != nil {
		t.Fatalf("packetize: %v", err)
	}

	var p1, p2 rtp.Packet
	if err := p1.Unmarshal(a); err != nil {
		t.Fatalf("unmarshal a: %v", err)
	}
	if err := p2.Unmarshal(b); err != nil {
		t.Fatalf("unmarshal b: %v", err)
	}

	if p1.PayloadType != 111 || p1.SSRC != 0xBEEFCAFE {
		t.Fatalf("bad header: pt=%d ssrc=%x", p1.PayloadType, p1.SSRC)
	}
	if p2.SequenceNumber != p1.SequenceNumber+1 {
		t.Fatalf("seq not monotonic: %d vs %d", p1.SequenceNumber, p2.SequenceNumber)
	}
	if p2.Timestamp-p1.Timestamp != 960 {
		t.Fatalf("ts delta = %d", p2.Timestamp-p1.Timestamp)
	}
}
