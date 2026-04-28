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
	// CodecVP8 covers WebRTC's most common legacy video codec.
	// Enhanced-RTMP v2 does NOT assign a FourCC for VP8 (the spec
	// only blesses avc1 / vp09 / av01 / hvc1), so a Broadcast that
	// has any RTMP destination is rejected at Start() time when
	// the live codec is VP8 — see validateDestinationsForCodec.
	// VP8 is forwarded verbatim over WHIP egress.
	CodecVP8
	// CodecH265 (HEVC) uses E-RTMP FourCC "hvc1" with an
	// HEVCDecoderConfigurationRecord. mediasoup-go's supported
	// capability table does not include H.265 (registering it
	// fails Router validation), so the live source can never
	// negotiate H.265. The path is reachable from a future
	// pion-terminated mix WHIP ingress that bypasses mediasoup
	// (see docs/patent/broadcast-source-switch.md figure 5);
	// the egress plumbing here is ready ahead of that work.
	CodecH265
)

// IsVideo reports whether a Codec carries video payload. Used by
// higher-level code (e.g. sequence-header emission) to avoid
// duplicating case lists for every new codec.
func (c Codec) IsVideo() bool {
	switch c {
	case CodecH264, CodecVP9, CodecAV1, CodecVP8, CodecH265:
		return true
	}
	return false
}

// MediaSample is one encoded access unit ready to be muxed out. It is
// the hand-off format between the source-specific depacketizers
// (source_live.go, source_mix.go, source_mix_h265.go) and the outbound
// pushers.
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
