package rtmp

import (
	"encoding/binary"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
)

// DefaultH264MTU is the MTU pion's H264 payloader fragments against. We
// use 1200 to leave headroom for UDP/IP/RTP headers below the common 1500
// Ethernet MTU. Direct transport does not involve UDP, but mediasoup will
// re-packetise for consumers on real WebRTC paths, so we still follow the
// WebRTC norm so consumer-side assemblers see familiar sizes.
const DefaultH264MTU = 1200

// DefaultOpusMTU is the packet-length budget for one Opus RTP payload.
// Opus frames at 20 ms @48 kHz typically weigh < 200 bytes so this is
// effectively an upper bound, not a fragmentation threshold.
const DefaultOpusMTU = 1200

// H264Packetizer turns AVCC-formatted H.264 payloads into RTP packets
// ready for Producer.Send(). It is a thin wrapper around
// codecs.H264Payloader with RTP sequence/timestamp bookkeeping plus
// "inject SPS/PPS before each keyframe" housekeeping.
//
// Construction: NewH264Packetizer(ssrc, payloadType). Callers then:
//
//  1. Call SetParameterSets(sps, pps) exactly once on AVC seq header.
//  2. For every NALU access unit, call Packetize(pts90k, isKey, data).
//
// Packetize returns fully marshaled RTP packets; callers hand them to
// DirectTransport.Producer.Send verbatim.
//
// H264Packetizer satisfies the package's VideoPacketizer interface so
// the RTMP session can dispatch uniformly across AVC/VP9/AV1 legs.
type H264Packetizer struct {
	ssrc        uint32
	payloadType uint8
	sequence    uint16

	sps, pps  []byte
	payloader codecs.H264Payloader
	mtu       uint16
	hasSPSPPS bool
}

// NewH264Packetizer constructs a packetizer. PayloadType must match the
// RtpCodecParameters.PayloadType for the H.264 producer mediasoup is
// about to create; otherwise mediasoup drops the RTP as "unknown PT".
func NewH264Packetizer(ssrc uint32, payloadType uint8) *H264Packetizer {
	return &H264Packetizer{
		ssrc:        ssrc,
		payloadType: payloadType,
		mtu:         DefaultH264MTU,
	}
}

// Compile-time assertion: every codec packetizer in this package must
// satisfy the common VideoPacketizer contract so the session can treat
// them uniformly.
var _ VideoPacketizer = (*H264Packetizer)(nil)

// SetMTU overrides the payload size the fragmenter targets. Useful for
// tests that want deterministic fragment counts.
func (p *H264Packetizer) SetMTU(mtu uint16) {
	p.mtu = mtu
}

// SetParameterSets stores the SPS and PPS NALUs so the payloader can
// re-emit them as a STAP-A ahead of each keyframe. Callers decode them
// from the AVCDecoderConfigurationRecord.
func (p *H264Packetizer) SetParameterSets(sps, pps []byte) {
	p.sps = append(p.sps[:0], sps...)
	p.pps = append(p.pps[:0], pps...)
	p.hasSPSPPS = len(sps) > 0 && len(pps) > 0
}

// Packetize fragments an AVCC access unit (4-byte length prefixed
// NALUs) into RTP packets. pts90k is the 90 kHz RTP timestamp (derived
// by the session from FLV milliseconds). When isKey is true the payloader
// injects SPS+PPS so receivers can start decoding from this packet alone.
//
// Returned slices are freshly allocated and safe for the caller to pass
// to mediasoup (which also copies internally before sending). The caller
// must marshal each packet via Packet.Marshal() before handing to
// Producer.Send — we do that here so the session only deals with bytes.
func (p *H264Packetizer) Packetize(pts90k uint32, isKey bool, avcc []byte) ([][]byte, error) {
	annexB := convertAVCCToAnnexB(avcc, isKey && p.hasSPSPPS, p.sps, p.pps)
	fragments := p.payloader.Payload(p.mtu, annexB)
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

// SequenceNumber returns the next sequence number that will be assigned.
// Exposed for tests that assert wrap-around behavior.
func (p *H264Packetizer) SequenceNumber() uint16 { return p.sequence }

// convertAVCCToAnnexB rewrites [u32 length | NALU bytes...]+ into
// [0 0 0 1 | NALU bytes...]+. When insertSPSPPS is true SPS and PPS are
// prepended (also in Annex-B form) so the H264 payloader's cache picks
// them up and emits a STAP-A before the next NALU.
func convertAVCCToAnnexB(avcc []byte, insertSPSPPS bool, sps, pps []byte) []byte {
	// Pre-compute final length to avoid multiple allocations. Each NALU
	// grows by 0 bytes (length prefix is 4, start code is also 4); SPS
	// and PPS prepending adds 4 + len(sps) + 4 + len(pps).
	total := len(avcc)
	if insertSPSPPS {
		total += 4 + len(sps) + 4 + len(pps)
	}
	out := make([]byte, 0, total)
	startCode := []byte{0x00, 0x00, 0x00, 0x01}

	if insertSPSPPS {
		out = append(out, startCode...)
		out = append(out, sps...)
		out = append(out, startCode...)
		out = append(out, pps...)
	}

	for len(avcc) >= 4 {
		length := binary.BigEndian.Uint32(avcc[:4])
		if uint32(len(avcc)-4) < length {
			break
		}
		out = append(out, startCode...)
		out = append(out, avcc[4:4+length]...)
		avcc = avcc[4+length:]
	}
	return out
}
