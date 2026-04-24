// Package rtmp implements RTMP push ingest for axonkit. It supports both
// classic RTMP and the Enhanced RTMP (ExAudio / ExVideo) container
// extensions so publishers can push H.264 video plus Opus audio without
// any transcoding.
//
// This file defines the minimal FLV video-tag parser. It understands two
// framings only:
//
//   - Classic AVC (CodecID=7): one byte "frame type | codec id", then
//     AVCPacketType, 3-byte composition time, then either an
//     AVCDecoderConfigurationRecord (seq header) or AVCC NALUs.
//   - Enhanced RTMP (ExVideoData): first byte high bit set, PacketType in
//     the low nibble, FourCC ("hvc1", "av01", "vp09", "avc1"), and then
//     codec-specific payload. We only forward FourCC "avc1" to stay aligned
//     with the project's v1 codec scope; anything else is dropped.
//
// The parser never allocates more than the input slice length + a small
// header; callers (the RTMP handler) are responsible for holding onto the
// returned NALU bytes across RTP dispatch.
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
// Data is AVCC-format (4-byte big-endian length + NALU bytes) for AVC
// NALU packets, or the raw AVCDecoderConfigurationRecord bytes when
// PacketType == VideoPacketSeqHeader / VideoPacketExSeqStart.
//
// CompositionTimeMs is the signed 24-bit DTS→PTS offset (AVC only). The
// SFU does not re-order frames, so the value is carried through only
// when the caller wants to compute PTS for downstream consumers that
// care about B-frames.
type VideoTag struct {
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
		FrameType:         frameType,
		PacketType:        pktType,
		IsKeyFrame:        frameType == VideoFrameKey,
		CompositionTimeMs: cts,
		Data:              payload,
	}, nil
}

// ExVideoFourCC* are the Enhanced RTMP 4-byte codec identifiers we care
// about. The spec reserves several more but we only decode AVC for v1.
const (
	ExVideoFourCCAVC = "avc1"
)

func parseEnhancedVideoTag(body []byte) (*VideoTag, error) {
	if len(body) < 5 {
		return nil, fmt.Errorf("ex-video tag too short: %d bytes", len(body))
	}
	head := body[0]
	// Enhanced RTMP v1-2023-12 §8.1: bit7=1 (ex), bits4-6=FrameType,
	// bits0-3=PacketType.
	rawPacketType := VideoPacketType(head & 0x0F)
	frameType := VideoFrameType((head >> 4) & 0x07)
	fourCC := string(body[1:5])

	if fourCC != ExVideoFourCCAVC {
		return nil, &UnsupportedVideoError{Reason: "ex-video FourCC " + fourCC + " not AVC"}
	}

	// Normalise to the classic enum so downstream RTP code can branch on
	// one value regardless of which FLV dialect delivered the tag. Unknown
	// ex packet types (e.g. metadata) become soft drops.
	var normalized VideoPacketType
	switch rawPacketType {
	case VideoPacketType(0): // CodedFrames w/ composition time
		normalized = VideoPacketNALU
	case VideoPacketType(1): // SequenceStart
		normalized = VideoPacketSeqHeader
	case VideoPacketType(2): // CodedFramesX (no composition time)
		normalized = VideoPacketNALU
	case VideoPacketType(3): // MPEG2TSSequenceStart — ignored
		return nil, &UnsupportedVideoError{Reason: "ex-video MPEG2TSSequenceStart"}
	case VideoPacketType(4): // Multitrack — ignored
		return nil, &UnsupportedVideoError{Reason: "ex-video Multitrack"}
	case VideoPacketType(5): // ModEx
		return nil, &UnsupportedVideoError{Reason: "ex-video ModEx"}
	case VideoPacketType(6): // MetaData — ignored
		return nil, &UnsupportedVideoError{Reason: "ex-video MetaData"}
	case VideoPacketType(7): // SequenceEnd
		normalized = VideoPacketEndOfSeq
	default:
		return nil, &UnsupportedVideoError{Reason: fmt.Sprintf("ex-video packet type %d", rawPacketType)}
	}

	offset := 5
	var cts int32
	// PacketTypeCodedFrames (0) carries a 24-bit composition time offset
	// in big-endian form right after the FourCC. PacketTypeCodedFramesX
	// (2) omits it entirely for lower overhead when B-frames are absent.
	if rawPacketType == VideoPacketType(0) && len(body) >= offset+3 {
		cts = decodeSignedInt24BE(body[offset : offset+3])
		offset += 3
	}

	return &VideoTag{
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
