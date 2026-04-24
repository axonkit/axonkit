package broadcast

// SampleKind tags a MediaSample as audio or video. Kept as a tiny
// enum instead of reusing mediasoup.MediaKind so downstream code
// (RTMP muxer, WHIP pusher) never needs to import mediasoup-go.
type SampleKind uint8

const (
	SampleKindAudio SampleKind = iota
	SampleKindVideo
)

// Codec is the codec carried by a MediaSample. Only the subset the
// outbound pushers can serialize is declared — unknown codecs surface
// as ErrUnsupportedCodec at broadcast creation time.
type Codec uint8

const (
	CodecUnknown Codec = iota
	CodecH264
	CodecOpus
	// CodecVP9 and CodecAV1 use the Enhanced-RTMP (E-RTMP) FLV tag
	// layout, pairing a FourCC ("vp09" / "av01") with a packet-type
	// code instead of the legacy CodecID nibble. Publishers that
	// negotiate these codecs are routed through the E-RTMP writer in
	// flv.go and the VP9 / AV1 depacketizers.
	CodecVP9
	CodecAV1
)

// IsVideo reports whether a Codec carries video payload. Used by
// higher-level code (e.g. sequence-header emission) to avoid
// duplicating case lists for every new codec.
func (c Codec) IsVideo() bool {
	switch c {
	case CodecH264, CodecVP9, CodecAV1:
		return true
	}
	return false
}

// MediaSample is one encoded access unit ready to be muxed out. It is
// the hand-off format between the source-specific depacketizers
// (source_live.go, source_mix.go in later milestones) and the outbound
// pushers (rtmp_pusher.go, whip_pusher.go).
//
// One MediaSample ≈ one video frame OR one Opus audio frame. The
// RTP timestamp is carried verbatim so pushers can translate into
// their own clock domain without losing precision.
type MediaSample struct {
	// Kind is audio or video.
	Kind SampleKind
	// Codec names the payload format inside Data.
	Codec Codec
	// RTPTimestamp is the original packet.Timestamp (90 kHz for
	// H264/video, 48 kHz for Opus). Pushers convert this to their
	// destination clock (RTMP ms, WHIP ssrc-local ts) using a
	// per-stream base captured on the first sample.
	RTPTimestamp uint32
	// IsKeyFrame is true for H264 IDR/SPS access units and false for
	// every audio frame and non-IDR video frame. The switcher uses
	// this to key-frame-align video source flips.
	IsKeyFrame bool
	// Data is codec-specific:
	//   - H264: AVCC-formatted access unit (4-byte BE length + NALU)*,
	//     i.e. ready for the FLV body or Annex-B conversion.
	//   - Opus: a single Opus packet (what the RTP payload already is).
	Data []byte
}
