package broadcast

import (
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
)

// VP8Depacketizer reassembles VP8 RTP packets (RFC 7741) into complete
// frames. Mirrors the public surface of VP9Depacketizer / AV1Depacketizer
// so the switcher and pusher plumbing can stay codec-agnostic, but the
// resulting samples have a couple of caveats relative to the rest of
// the suite:
//
//  1. Enhanced-RTMP v2 does NOT register a FourCC for VP8; the only
//     FourCCs the spec enumerates for video are avc1 / vp09 / av01 /
//     hvc1. As a consequence Broadcast.Start rejects a VP8 live
//     producer the moment any of the configured destinations is RTMP
//     (validateDestinationsForCodec). The depacketizer's
//     ConfigRecord() therefore exists only to satisfy the
//     videoDepacketizer interface; today it is never enqueued by the
//     RTMP pusher.
//  2. WHIP egress passes the RTP through verbatim — no depacketization
//     would be required at all. We still run the depacketizer so the
//     switcher can perform key-frame-aligned source flips on the same
//     contract as the other codecs.
//
// Not thread-safe: Push is expected to be called from a single
// Consumer.OnRtp goroutine.
type VP8Depacketizer struct {
	unmarshaller codecs.VP8Packet

	frame   []byte
	isKey   bool
	seenKey bool
}

// Push delivers one RTP packet and returns (sample, true) on the
// packet that completes a VP8 frame (RTP marker bit set).
//
// Frame boundaries:
//   - VP8 RTP packets carry a payload descriptor whose S bit marks
//     the start of a new frame partition; combined with PID==0 it
//     uniquely identifies the start of a VP8 frame (RFC 7741 §4.4).
//   - Key vs. inter is the LSB of the first uncompressed VP8 byte
//     (RFC 6386 §9.1): bit 0 = 0 → keyframe, 1 → interframe.
func (d *VP8Depacketizer) Push(pkt *rtp.Packet) (MediaSample, bool) {
	payload, err := d.unmarshaller.Unmarshal(pkt.Payload)
	if err != nil {
		if pkt.Marker {
			return d.flush(pkt.Timestamp)
		}
		return MediaSample{}, false
	}
	if d.unmarshaller.S == 1 && d.unmarshaller.PID == 0 {
		d.frame = d.frame[:0]
		if len(payload) > 0 {
			d.isKey = (payload[0] & 0x01) == 0
		}
	}
	d.frame = append(d.frame, payload...)
	if !pkt.Marker {
		return MediaSample{}, false
	}
	return d.flush(pkt.Timestamp)
}

func (d *VP8Depacketizer) flush(ts uint32) (MediaSample, bool) {
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
		Codec:        CodecVP8,
		RTPTimestamp: ts,
		IsKeyFrame:   d.isKey,
		Data:         data,
	}, true
}

// HasConfig reports whether the depacketizer has observed a keyframe.
// Same semantics as the VP9 / AV1 implementations.
func (d *VP8Depacketizer) HasConfig() bool { return d.seenKey }

// SeenIDR is the H.264-symmetric name for "have we seen a keyframe
// yet?" — exported for the switcher's gating logic.
func (d *VP8Depacketizer) SeenIDR() bool { return d.seenKey }

// ConfigRecord returns a VPCodecConfigurationRecord with VP8-friendly
// defaults (profile=0, level=0). VP8 is never actually wrapped into
// an E-RTMP SequenceStart in production today (see type doc), but
// returning a non-nil byte slice keeps the videoDepacketizer interface
// uniform across codecs and unblocks future use cases (e.g. an
// internal axonkit-to-axonkit chain that agrees on FourCC "vp08").
func (d *VP8Depacketizer) ConfigRecord() []byte {
	if !d.seenKey {
		return nil
	}
	return buildVP9ConfigRecord(0, 0)
}
