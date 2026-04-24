package rtmp

import (
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
)

// DefaultVP9MTU mirrors DefaultH264MTU: keep RTP payloads safely under
// the 1500-byte Ethernet MTU after UDP/IP/SRTP overhead. Direct transport
// does not use UDP, but mediasoup re-payloads for WebRTC consumers, so
// we stay within the standard WebRTC budget for compatibility.
const DefaultVP9MTU = 1200

// VP9Packetizer wraps pion's VP9Payloader to fragment VP9 elementary
// bitstream frames into RTP packets ready for DirectTransport.Producer.
// VP9 frames are self-describing (no SPS/PPS equivalent is injected at
// the RTP layer), so the packetizer state is just RTP bookkeeping.
//
// Construction: NewVP9Packetizer(ssrc, payloadType). For every frame:
//
//	pkts, err := pk.Packetize(pts90k, isKey, frameBytes)
//
// The isKey argument is ignored by pion's VP9Payloader (VP9 frames mark
// their own keyframe status inside the bitstream) but is kept to satisfy
// the VideoPacketizer interface.
type VP9Packetizer struct {
	ssrc        uint32
	payloadType uint8
	sequence    uint16
	payloader   codecs.VP9Payloader
	mtu         uint16
}

// NewVP9Packetizer constructs a VP9 packetizer. PayloadType must match
// the RtpCodecParameters.PayloadType the mediasoup producer was created
// with; otherwise mediasoup drops the RTP as "unknown PT".
func NewVP9Packetizer(ssrc uint32, payloadType uint8) *VP9Packetizer {
	return &VP9Packetizer{
		ssrc:        ssrc,
		payloadType: payloadType,
		mtu:         DefaultVP9MTU,
	}
}

// SetMTU overrides the payload-size target pion uses for fragmentation.
// Useful for tests that want deterministic packet counts.
func (p *VP9Packetizer) SetMTU(mtu uint16) { p.mtu = mtu }

// Packetize fragments one VP9 frame (raw elementary bitstream) into
// marshaled RTP packets. isKey is accepted for interface symmetry with
// H264Packetizer but unused — pion's VP9Payloader does not inspect it.
// The Marker bit is set on the last packet of the frame so receivers
// know when the picture is complete.
func (p *VP9Packetizer) Packetize(pts90k uint32, _ bool, frame []byte) ([][]byte, error) {
	fragments := p.payloader.Payload(p.mtu, frame)
	if len(fragments) == 0 {
		return nil, nil
	}
	packets := make([][]byte, 0, len(fragments))
	for i, frag := range fragments {
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    p.payloadType,
				SequenceNumber: p.sequence,
				Timestamp:      pts90k,
				SSRC:           p.ssrc,
				Marker:         i == len(fragments)-1,
			},
			Payload: frag,
		}
		b, err := pkt.Marshal()
		if err != nil {
			return nil, err
		}
		packets = append(packets, b)
		p.sequence++
	}
	return packets, nil
}

// SequenceNumber returns the next RTP sequence number that will be
// emitted. Exposed for tests that assert wrap-around behavior.
func (p *VP9Packetizer) SequenceNumber() uint16 { return p.sequence }

var _ VideoPacketizer = (*VP9Packetizer)(nil)
