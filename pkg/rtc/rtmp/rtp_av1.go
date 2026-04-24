package rtmp

import (
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
)

// DefaultAV1MTU keeps RTP payloads under the WebRTC budget. Mirrors the
// VP9 choice so all video packetizers in the package share MTU policy.
const DefaultAV1MTU = 1200

// AV1Packetizer wraps pion's AV1Payloader to fragment a low-overhead
// OBU bitstream (as shipped inside Enhanced RTMP "av01" CodedFrames /
// CodedFramesX tags) into RTP packets for DirectTransport.Producer.
//
// Construction: NewAV1Packetizer(ssrc, payloadType). For every access
// unit:
//
//	pkts, err := pk.Packetize(pts90k, isKey, obuStream)
//
// Pion's AV1Payloader does not consume an "isKey" argument — AV1
// frame type is encoded inside the OBU header — so the flag is kept
// only to satisfy the VideoPacketizer interface.
type AV1Packetizer struct {
	ssrc        uint32
	payloadType uint8
	sequence    uint16
	payloader   codecs.AV1Payloader
	mtu         uint16
}

// NewAV1Packetizer constructs an AV1 packetizer. PayloadType must match
// the mediasoup producer's RtpCodecParameters.PayloadType.
func NewAV1Packetizer(ssrc uint32, payloadType uint8) *AV1Packetizer {
	return &AV1Packetizer{
		ssrc:        ssrc,
		payloadType: payloadType,
		mtu:         DefaultAV1MTU,
	}
}

// SetMTU overrides the payload-size target pion uses for fragmentation.
func (p *AV1Packetizer) SetMTU(mtu uint16) { p.mtu = mtu }

// Packetize fragments one AV1 temporal unit (an OBU stream) into
// marshaled RTP packets. Marker is set on the last packet of the unit
// so receivers know the access unit is complete.
//
// If the OBU stream cannot be parsed (e.g. garbled publisher output)
// pion returns an empty slice; we surface that as (nil, nil) so the
// session drops the frame without killing the leg.
func (p *AV1Packetizer) Packetize(pts90k uint32, _ bool, obuStream []byte) ([][]byte, error) {
	fragments := p.payloader.Payload(p.mtu, obuStream)
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
// emitted. Exposed for tests.
func (p *AV1Packetizer) SequenceNumber() uint16 { return p.sequence }

var _ VideoPacketizer = (*AV1Packetizer)(nil)
