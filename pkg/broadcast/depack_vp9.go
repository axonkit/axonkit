package broadcast

import (
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
)

// VP9Depacketizer reassembles VP9 RTP packets (RFC 8741) into complete
// access units ready to be wrapped in Enhanced-RTMP CodedFramesX tags.
//
// Only the subset we actually receive from real-time publishers is
// handled: single spatial layer, single temporal layer, picture ID may
// or may not be present, scalability-structure blocks are parsed and
// skipped. Multi-layer SVC would require splitting per-SID frames and
// is out of scope for the broadcast passthrough path.
//
// Not thread-safe: Push is expected to be called from a single
// Consumer.OnRtp goroutine (mediasoup delivers per-consumer callbacks
// serially).
type VP9Depacketizer struct {
	unmarshaller codecs.VP9Packet

	frame   []byte
	isKey   bool
	seenKey bool
}

// Push delivers one RTP packet and returns (sample, true) when a
// complete VP9 frame has been assembled. The sample.Data is a fresh
// allocation the caller may retain.
func (d *VP9Depacketizer) Push(pkt *rtp.Packet) (MediaSample, bool) {
	payload, err := d.unmarshaller.Unmarshal(pkt.Payload)
	if err != nil {
		if pkt.Marker {
			return d.flush(pkt.Timestamp)
		}
		return MediaSample{}, false
	}
	if d.unmarshaller.B {
		d.frame = d.frame[:0]
		// RTP VP9 header bit P — "inter-picture predicted" — is the
		// inverse of the keyframe flag: P=0 means a keyframe (no
		// reference to previous pictures).
		d.isKey = !d.unmarshaller.P
	}
	d.frame = append(d.frame, payload...)
	if !pkt.Marker {
		return MediaSample{}, false
	}
	return d.flush(pkt.Timestamp)
}

func (d *VP9Depacketizer) flush(ts uint32) (MediaSample, bool) {
	if len(d.frame) == 0 {
		return MediaSample{}, false
	}
	data := make([]byte, len(d.frame))
	copy(data, d.frame)
	d.frame = d.frame[:0]
	if d.isKey {
		d.seenKey = true
	}
	return MediaSample{
		Kind:         SampleKindVideo,
		Codec:        CodecVP9,
		RTPTimestamp: ts,
		IsKeyFrame:   d.isKey,
		Data:         data,
	}, true
}

// HasConfig reports whether the depacketizer has observed enough of
// the bitstream to generate a VPCodecConfigurationRecord. For VP9 any
// keyframe is sufficient — the record fields (profile/level/colour)
// we emit today are static defaults.
func (d *VP9Depacketizer) HasConfig() bool { return d.seenKey }

// SeenIDR is the H.264-symmetric name for "have we seen a keyframe
// yet?" — exported for Broadcast.Start's gating logic, which treats
// every codec the same way up to this point.
func (d *VP9Depacketizer) SeenIDR() bool { return d.seenKey }

// ConfigRecord returns the VPCodecConfigurationRecord for the E-RTMP
// SequenceStart tag. We default to profile 0 / 4:2:0 8-bit / BT.709
// colour — the dominant configuration for WebRTC VP9 publishers.
// Parsing the uncompressed VP9 frame header to fill this out properly
// is tracked as a follow-up for HDR / 10-bit streams.
func (d *VP9Depacketizer) ConfigRecord() []byte {
	return buildVP9ConfigRecord(0, 0)
}
