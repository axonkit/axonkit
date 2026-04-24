package broadcast

import "github.com/pion/rtp"

// OpusDepacketizer lifts an RTP payload directly into a MediaSample.
// Opus over RTP (RFC 7587) uses a minimal payload format: each RTP
// packet carries exactly one Opus packet in its payload (an Opus packet
// itself may contain multiple Opus frames under Opus framing rules,
// but that is transparent to us — we forward the whole payload).
//
// Unlike H264Depacketizer this is trivial and stateless; we keep it a
// struct for symmetry and to leave room for DTX/PLC tracking later.
type OpusDepacketizer struct{}

// Push turns one RTP packet into one MediaSample. Always returns
// ok=true for non-empty payloads; FEC/PLC samples with empty payload
// are skipped so the pusher does not emit a zero-length FLV tag.
func (OpusDepacketizer) Push(pkt *rtp.Packet) (MediaSample, bool) {
	if len(pkt.Payload) == 0 {
		return MediaSample{}, false
	}
	data := make([]byte, len(pkt.Payload))
	copy(data, pkt.Payload)
	return MediaSample{
		Kind:         SampleKindAudio,
		Codec:        CodecOpus,
		RTPTimestamp: pkt.Timestamp,
		Data:         data,
	}, true
}
