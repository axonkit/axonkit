// Package rtmp implements RTMP push ingest for axonkit. It supports both
// classic RTMP and the Enhanced RTMP (ExAudio / ExVideo) container
// extensions so publishers can push H.264 / VP9 / AV1 video plus Opus
// audio without any transcoding.
//
// This file defines the minimal FLV video-tag parser. It understands two
// framings:
//
//   - Classic AVC (CodecID=7): one byte "frame type | codec id", then
//     AVCPacketType, 3-byte composition time, then either an
//     AVCDecoderConfigurationRecord (seq header) or AVCC NALUs.
//
//   - Enhanced RTMP (ExVideoTagHeader, veovera E-RTMP v1/v2): first byte
//     has bit7=1 (IsExHeader), bits4-6 carry FrameType, bits0-3 carry
//     PacketType; then a 4-byte FourCC identifies the codec. Supported
//     FourCCs are "avc1" (H.264), "vp09" (VP9) and "av01" (AV1); anything
//     else (notably "hvc1" HEVC) is soft-dropped because mediasoup-go's
//     supported_rtp_capabilities table does not cover those codecs yet.
//
//     PacketType numbering follows the Enhanced RTMP spec exactly:
//     0 SequenceStart        — body is a codec config record
//     1 CodedFrames          — body is [CTS SI24 BE][codec payload]
//     2 SequenceEnd          — empty body
//     3 CodedFramesX         — body is the raw codec payload (no CTS)
//     4 Metadata             — ignored
//     5 MPEG2TSSequenceStart — ignored
//     6 Multitrack (v2)      — ignored
//     7 ModEx (v2)           — ignored
//
// The parser never allocates more than the input slice length + a small
// header; callers (the RTMP handler) are responsible for holding onto
// the returned payload bytes across RTP dispatch.
package rtmp

import (
	"encoding/binary"
	"fmt"
)

// VideoFrameType (FLV v10.1 §E.4.3.1 FrameType) — we only surface KEY and
// INTER because the SFU only cares whether the frame is a refresh.
type VideoFrameType uint8

const (
	VideoFrameKey       VideoFrameType = 1 // keyframe (IDR in AVC)
	VideoFrameInter     VideoFrameType = 2 // inter frame
	VideoFrameDisposInt VideoFrameType = 3 // disposable inter frame (H.263 only)
	VideoFrameGenerated VideoFrameType = 4 // generated keyframe (reserved)
	VideoFrameInfo      VideoFrameType = 5 // video info / command frame
)

// VideoPacketType describes how to interpret the FLV video payload.
//
// Classic AVCPacketType values and the normalised enum used by callers
// share the same constants: ParseVideoTag rewrites Enhanced RTMP packet
// types into these three so the H.264 pipeline only sees one vocabulary.
type VideoPacketType uint8

const (
	// VideoPacketSeqHeader carries an AVCDecoderConfigurationRecord (AVC)
	// or a codec-specific config record (ExVideo). The SFU turns it into
	// RtpParameters.codecs and feeds sps/pps into the H264 payloader.
	VideoPacketSeqHeader VideoPacketType = 0
	// VideoPacketNALU carries concatenated AVCC-prefixed NAL units (AVC)
	// or codec-specific frame payload. This is the steady-state tag.
	VideoPacketNALU VideoPacketType = 1
	// VideoPacketEndOfSeq signals the end of a video sequence; no payload.
	VideoPacketEndOfSeq VideoPacketType = 2
)

// VideoTag is the parsed form of an FLV VIDEODATA tag body.
//
// Data is the codec-specific payload:
//   - H.264: AVCC-formatted NALUs (4-byte BE length + NALU bytes) for
//     NALU packets, or the raw AVCDecoderConfigurationRecord when
//     PacketType == VideoPacketSeqHeader.
//   - VP9: raw VP9 elementary bitstream frame bytes, or the vpcC
//     configuration record bytes on SequenceStart.
//   - AV1: low-overhead OBU bitstream for frames, or the av1C
//     configuration record bytes on SequenceStart.
//
// CompositionTimeMs is the signed 24-bit DTS→PTS offset, non-zero only
// on classic AVC and Enhanced CodedFrames (PacketType=1) tags. The SFU
// does not re-order frames, so the value is surfaced mostly for
// downstream consumers that care about B-frames.
type VideoTag struct {
	Codec             VideoCodec
	FrameType         VideoFrameType
	PacketType        VideoPacketType
	IsKeyFrame        bool
	CompositionTimeMs int32
	Data              []byte
}

// UnsupportedVideoError is returned for tags we deliberately drop (e.g.
// non-AVC codec id, ExVideo with FourCC other than avc1). The RTMP handler
// treats this as a soft drop, not a fatal error, so unexpected codecs do
// not terminate the ingest connection.
type UnsupportedVideoError struct {
	Reason string
}

func (e *UnsupportedVideoError) Error() string { return "unsupported video tag: " + e.Reason }

// IsUnsupportedVideo reports whether err is a soft-drop sentinel so the
// RTMP handler can distinguish "codec not in scope" from real parse bugs.
func IsUnsupportedVideo(err error) bool {
	_, ok := err.(*UnsupportedVideoError)
	return ok
}

// ParseVideoTag decodes an FLV video tag body. The input is the raw payload
// delivered by go-rtmp (i.e. the bytes after the FLV 11-byte tag header).
//
// Layout (classic):
//
//	byte 0        : FrameType (hi 4 bits) | CodecID (lo 4 bits)
//	byte 1        : AVCPacketType
//	bytes 2..4    : CompositionTime (24-bit signed BE)
//	bytes 5..    : payload
//
// Layout (Enhanced RTMP, CodecID high bit = 1):
//
//	byte 0        : 1 (hi) | PacketType (4 bits) | FrameType (3 bits)
//	bytes 1..4    : FourCC
//	bytes 5..    : payload
func ParseVideoTag(body []byte) (*VideoTag, error) {
	if len(body) < 1 {
		return nil, fmt.Errorf("video tag too short: %d bytes", len(body))
	}

	head := body[0]
	if head&0x80 != 0 {
		return parseEnhancedVideoTag(body)
	}
	return parseClassicVideoTag(body)
}

func parseClassicVideoTag(body []byte) (*VideoTag, error) {
	if len(body) < 5 {
		return nil, fmt.Errorf("classic video tag too short: %d bytes", len(body))
	}
	frameType := VideoFrameType((body[0] >> 4) & 0x0F)
	codecID := body[0] & 0x0F

	const codecAVC = 7
	if codecID != codecAVC {
		return nil, &UnsupportedVideoError{Reason: fmt.Sprintf("classic codec id %d not AVC", codecID)}
	}

	pktType := VideoPacketType(body[1])
	cts := decodeSignedInt24BE(body[2:5])
	payload := body[5:]

	return &VideoTag{
		Codec:             VideoCodecH264,
		FrameType:         frameType,
		PacketType:        pktType,
		IsKeyFrame:        frameType == VideoFrameKey,
		CompositionTimeMs: cts,
		Data:              payload,
	}, nil
}

// Enhanced RTMP raw packet types as defined by veovera's E-RTMP v1-2023-12
// §8.1 (video). These are the wire values carried in the low nibble of
// the ExVideoTagHeader's first byte; parseEnhancedVideoTag normalizes
// them into the classic VideoPacket* enum for downstream dispatch.
const (
	exVideoPacketSequenceStart        uint8 = 0
	exVideoPacketCodedFrames          uint8 = 1 // body starts with 24-bit signed CTS
	exVideoPacketSequenceEnd          uint8 = 2
	exVideoPacketCodedFramesX         uint8 = 3 // same as CodedFrames but no CTS
	exVideoPacketMetadata             uint8 = 4
	exVideoPacketMPEG2TSSequenceStart uint8 = 5
	exVideoPacketMultitrack           uint8 = 6 // E-RTMP v2
	exVideoPacketModEx                uint8 = 7 // E-RTMP v2
)

func parseEnhancedVideoTag(body []byte) (*VideoTag, error) {
	if len(body) < 5 {
		return nil, fmt.Errorf("ex-video tag too short: %d bytes", len(body))
	}
	head := body[0]
	// Enhanced RTMP v1-2023-12 §8.1: bit7=1 (ex), bits4-6=FrameType,
	// bits0-3=PacketType.
	rawPacketType := head & 0x0F
	frameType := VideoFrameType((head >> 4) & 0x07)
	fourCC := string(body[1:5])

	codec := videoCodecFromFourCC(fourCC)
	if codec == VideoCodecUnknown {
		return nil, &UnsupportedVideoError{Reason: "ex-video FourCC " + fourCC + " not in v1 scope"}
	}

	// Normalize to the classic enum so downstream dispatch only has to
	// branch on one value. PacketTypes we cannot act on (Metadata,
	// MPEG2TSSequenceStart, Multitrack, ModEx) are surfaced as soft
	// drops so the handler logs the reason without closing the session.
	var normalized VideoPacketType
	switch rawPacketType {
	case exVideoPacketSequenceStart:
		normalized = VideoPacketSeqHeader
	case exVideoPacketCodedFrames:
		normalized = VideoPacketNALU
	case exVideoPacketSequenceEnd:
		normalized = VideoPacketEndOfSeq
	case exVideoPacketCodedFramesX:
		normalized = VideoPacketNALU
	case exVideoPacketMetadata:
		return nil, &UnsupportedVideoError{Reason: "ex-video Metadata"}
	case exVideoPacketMPEG2TSSequenceStart:
		return nil, &UnsupportedVideoError{Reason: "ex-video MPEG2TSSequenceStart"}
	case exVideoPacketMultitrack:
		return nil, &UnsupportedVideoError{Reason: "ex-video Multitrack"}
	case exVideoPacketModEx:
		return nil, &UnsupportedVideoError{Reason: "ex-video ModEx"}
	default:
		return nil, &UnsupportedVideoError{Reason: fmt.Sprintf("ex-video packet type %d", rawPacketType)}
	}

	offset := 5
	var cts int32
	// Enhanced RTMP v2 §ExVideoTagBody: the 24-bit composition time
	// offset is only present on PacketType=CodedFrames AND only for the
	// B-frame-capable codecs (AVC / HEVC / VVC). AV1 and VP9 have no
	// B-frames, so the spec deliberately omits the CTS field for them —
	// reading 3 bytes here would consume the first OBU / frame header
	// and leave pion's packetizer with mangled bitstream (which is
	// exactly the symptom we hit: half of the AV1 tags coming out of
	// ffmpeg are only 3 bytes of OBU after the FourCC, so the parser
	// was eating the entire payload).
	//
	// PacketType=CodedFramesX (3) never carries CTS for any codec, so it
	// is handled by the "no branch" path below.
	if rawPacketType == exVideoPacketCodedFrames && codecCarriesCTS(codec) &&
		len(body) >= offset+3 {
		cts = decodeSignedInt24BE(body[offset : offset+3])
		offset += 3
	}

	return &VideoTag{
		Codec:             codec,
		FrameType:         frameType,
		PacketType:        normalized,
		IsKeyFrame:        frameType == VideoFrameKey,
		CompositionTimeMs: cts,
		Data:              body[offset:],
	}, nil
}

// decodeSignedInt24BE decodes a big-endian, 24-bit signed integer. Used
// for the FLV composition time field (PTS - DTS in milliseconds).
func decodeSignedInt24BE(b []byte) int32 {
	if len(b) < 3 {
		return 0
	}
	v := uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
	if v&0x00800000 != 0 {
		v |= 0xFF000000
	}
	return int32(v) //nolint:gosec // deliberate sign extension
}

// IterAVCCNALUs invokes fn for every NALU in an AVCC-formatted payload
// (4-byte big-endian length prefix + NALU bytes). It returns the number of
// NALUs visited. Malformed or truncated entries stop iteration without an
// error so partial media can still be forwarded where safe.
func IterAVCCNALUs(data []byte, fn func(nalu []byte)) int {
	n := 0
	for len(data) >= 4 {
		length := binary.BigEndian.Uint32(data[:4])
		if uint32(len(data)-4) < length {
			return n
		}
		fn(data[4 : 4+length])
		data = data[4+length:]
		n++
	}
	return n
}
