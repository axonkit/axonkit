package rtmp

import "github.com/pion/rtp"

// OpusPacketizer turns single Opus frames into RTP packets. Opus packs
// one frame per RTP packet (FLV/RTMP publishers emit one frame per audio
// tag), so there is no fragmentation logic to worry about.
type OpusPacketizer struct {
	ssrc        uint32
	payloadType uint8
	sequence    uint16
}

// NewOpusPacketizer constructs a packetizer. PayloadType must align with
// the RtpCodecParameters.PayloadType for the Opus producer mediasoup
// creates; otherwise mediasoup tags the packet as unknown PT and drops it.
func NewOpusPacketizer(ssrc uint32, payloadType uint8) *OpusPacketizer {
	return &OpusPacketizer{ssrc: ssrc, payloadType: payloadType}
}

// PacketizeFrame builds a single RTP packet for the given Opus payload.
// ts48k is the 48 kHz RTP timestamp; DirectTransport forwards it to
// mediasoup verbatim. Marker is always false for Opus in RTP (no access
// unit boundaries beyond the frame itself, matching WebRTC convention).
func (p *OpusPacketizer) PacketizeFrame(ts48k uint32, payload []byte) ([]byte, error) {
	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    p.payloadType,
			SequenceNumber: p.sequence,
			Timestamp:      ts48k,
			SSRC:           p.ssrc,
		},
		Payload: payload,
	}
	b, err := pkt.Marshal()
	if err != nil {
		return nil, err
	}
	p.sequence++
	return b, nil
}

// SequenceNumber exposes the next sequence# for tests.
func (p *OpusPacketizer) SequenceNumber() uint16 { return p.sequence }
