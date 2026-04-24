package rtmp

// VideoCodec identifies the concrete video codec carried inside an FLV
// video tag. The RTMP ingest pipeline uses it to select:
//
//   - the RtpCodecParameters advertised on the mediasoup Producer
//     (mime type, profile parameters, RTCP feedback),
//   - the codec-specific bitstream transform (AVCC→Annex-B for H.264;
//     passthrough for VP9/AV1 elementary bitstreams),
//   - the pion RTP payloader to fragment each access unit into RTP
//     packets below the MTU.
//
// The value is set once per session on the first SequenceStart tag and
// must not change mid-stream. Session guards against codec swaps
// because mediasoup refuses to re-negotiate a running Producer.
type VideoCodec uint8

const (
	// VideoCodecUnknown is the zero value used before SequenceStart
	// has arrived. Steady-state NALU/frame tags with this codec are
	// silently dropped until the first config record is seen.
	VideoCodecUnknown VideoCodec = 0
	// VideoCodecH264 covers both classic RTMP (CodecID=7) and
	// Enhanced RTMP (FourCC "avc1"). Payload shape is AVCC.
	VideoCodecH264 VideoCodec = 1
	// VideoCodecVP9 is Enhanced RTMP only, FourCC "vp09". Payload
	// shape is the VP9 elementary bitstream; config record is vpcC.
	VideoCodecVP9 VideoCodec = 2
	// VideoCodecAV1 is Enhanced RTMP only, FourCC "av01". Payload
	// shape is the AV1 low-overhead OBU stream; config record is av1C.
	VideoCodecAV1 VideoCodec = 3
)

// String returns the short lowercase codec label we log in attach /
// leg-ready messages. Unknown renders as "unknown".
func (c VideoCodec) String() string {
	switch c {
	case VideoCodecH264:
		return "h264"
	case VideoCodecVP9:
		return "vp9"
	case VideoCodecAV1:
		return "av1"
	default:
		return "unknown"
	}
}

// videoCodecFromFourCC maps an Enhanced RTMP video FourCC to the
// corresponding VideoCodec, returning VideoCodecUnknown for codecs
// outside the project's v1 scope (most notably "hvc1" HEVC).
func videoCodecFromFourCC(fourCC string) VideoCodec {
	switch fourCC {
	case "avc1":
		return VideoCodecH264
	case "vp09":
		return VideoCodecVP9
	case "av01":
		return VideoCodecAV1
	default:
		return VideoCodecUnknown
	}
}

// codecCarriesCTS reports whether a given codec's Enhanced RTMP
// CodedFrames (PacketType=1) body starts with a 24-bit composition
// time offset before the codec payload.
//
// Per Enhanced RTMP v2 §ExVideoTagBody the field is present only for
// B-frame-capable codecs (AVC / HEVC / VVC); AV1 and VP9 deliberately
// omit it because their DTS always equals PTS. Parsers that read 3
// bytes unconditionally would corrupt the first OBU / VP9 frame header.
func codecCarriesCTS(c VideoCodec) bool {
	return c == VideoCodecH264
}

// VideoPacketizer fragments one codec access unit into a list of
// marshaled RTP packets ready for mediasoup's Producer.Send.
//
// The payload shape is codec-specific and matches what arrives in the
// steady-state VideoTag.Data after the session has finished negotiating
// the codec on the first SequenceStart:
//
//   - H.264: AVCC-framed bytes (4-byte BE length + NALU, repeated).
//     The packetizer is responsible for the AVCC→Annex-B rewrite and
//     the STAP-A injection of SPS/PPS on keyframes.
//   - VP9: raw VP9 elementary bitstream for a single frame. No
//     fragmentation other than what pion's VP9Payloader performs for
//     MTU-budgeted packets.
//   - AV1: low-overhead OBU bitstream for one temporal unit. pion's
//     AV1Payloader handles OBU-level fragmentation (RFC 9381).
//
// Implementations keep per-producer RTP state (sequence number, SSRC,
// payload type). They are intentionally not safe for concurrent use
// because the RTMP handler single-threads tag dispatch already.
type VideoPacketizer interface {
	// Packetize returns the RTP packets (already marshaled) for one
	// access unit. pts90k is the 90 kHz RTP timestamp derived from the
	// FLV timestamp; isKey marks the access unit as a refresh so the
	// packetizer can inject parameter sets when required (H.264 only).
	Packetize(pts90k uint32, isKey bool, payload []byte) ([][]byte, error)
	// SequenceNumber returns the next RTP sequence number that will be
	// emitted. Exposed mostly for tests that verify wrap-around.
	SequenceNumber() uint16
}
