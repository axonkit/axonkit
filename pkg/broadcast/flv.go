package broadcast

import "encoding/binary"

// FLV tag byte-layout builders used by the RTMP pusher. The counterpart
// to these is pkg/rtc/rtmp/flv_video.go / flv_audio.go which parse the
// same bytes on ingress. The two paths deliberately do not share types
// — the pkg/rtc/rtmp direction models inbound semantics (codec
// detection, soft-drops, etc.), while this file is a minimal writer
// that only emits what our supported egress codecs (H264, Opus) need.

// FLV video tag constants (classic AVC framing). These mirror the
// `FrameType<<4 | CodecID` and `AVCPacketType` + 3-byte composition
// time field an FLV 10.1 video tag header specifies.
const (
	flvFrameTypeKey   = 0x10 // high nibble: 1 (key)
	flvFrameTypeInter = 0x20 // high nibble: 2 (inter)
	flvVideoCodecAVC  = 0x07 // low nibble: 7 (AVC/H.264)

	flvAVCPacketTypeSeqHeader byte = 0x00
	flvAVCPacketTypeNALU      byte = 0x01
)

// buildAVCSeqHeaderTag writes the FLV AVC seq header body:
//
//	[FrameType<<4 | CodecID] [AVCPacketType=0] [CTS SI24 BE = 0] [avcC record]
//
// Receivers need this before any AVC NALU tags.
func buildAVCSeqHeaderTag(avcc []byte) []byte {
	out := make([]byte, 5+len(avcc))
	out[0] = flvFrameTypeKey | flvVideoCodecAVC // 0x17
	out[1] = flvAVCPacketTypeSeqHeader
	// CTS=0 (3 bytes); out[2..5] are already zero.
	copy(out[5:], avcc)
	return out
}

// buildAVCFrameTag writes an FLV AVC NALU tag body:
//
//	[FrameType<<4 | CodecID] [AVCPacketType=1] [CTS SI24 BE] [AVCC access unit]
//
// CTS is the (PTS-DTS) offset in milliseconds. For pure passthrough
// from live/mix WebRTC producers we have no separate DTS so we always
// write CTS=0; B-frames are rare in real-time video and SFU publishers
// typically disable them.
func buildAVCFrameTag(isKey bool, avccAccessUnit []byte) []byte {
	out := make([]byte, 5+len(avccAccessUnit))
	if isKey {
		out[0] = flvFrameTypeKey | flvVideoCodecAVC // 0x17
	} else {
		out[0] = flvFrameTypeInter | flvVideoCodecAVC // 0x27
	}
	out[1] = flvAVCPacketTypeNALU
	// out[2..5] are CTS (signed 24-bit BE) — all zero.
	copy(out[5:], avccAccessUnit)
	return out
}

// Enhanced RTMP (E-RTMP) video tag layout — veovera
// "enhanced-rtmp-v2" §7. Used for VP9 and AV1 where the classic
// 1-byte (FrameType<<4 | CodecID) header cannot represent the codec.
//
// Byte 0 layout:
//
//	bit 7      : IsExHeader = 1
//	bits 6..4  : FrameType (1=key, 2=inter)
//	bits 3..0  : PacketType
//
// Bytes 1..4 : FourCC identifier (e.g. "vp09", "av01", "avc1", "hvc1")
// Bytes 5..  : codec + packet-type specific payload
//
// PacketTypes we emit:
//
//	0 SequenceStart  : codec configuration record
//	1 CodedFrames    : [SI24 CTS] [data]   (H.264/HEVC only)
//	2 SequenceEnd
//	3 CodedFramesX   : [data]              (no composition offset)
const (
	flvExVideoIsExHeader byte = 0x80

	flvExVideoFrameTypeKey   byte = 0x10 // (1 << 4)
	flvExVideoFrameTypeInter byte = 0x20 // (2 << 4)

	flvExVideoPacketSeqStart  byte = 0x00
	flvExVideoPacketCodedFrmX byte = 0x03

	flvExVideoFourCCVP9 = "vp09"
	flvExVideoFourCCAV1 = "av01"
)

func exVideoHeaderByte(frameKey bool, packetType byte) byte {
	ft := flvExVideoFrameTypeInter
	if frameKey {
		ft = flvExVideoFrameTypeKey
	}
	return flvExVideoIsExHeader | ft | (packetType & 0x0F)
}

// buildExVideoSeqHeaderTag constructs a SequenceStart E-RTMP video
// tag: [header byte] [FourCC] [codec config record].
func buildExVideoSeqHeaderTag(fourCC string, config []byte) []byte {
	out := make([]byte, 5+len(config))
	out[0] = exVideoHeaderByte(true, flvExVideoPacketSeqStart)
	copy(out[1:5], []byte(fourCC))
	copy(out[5:], config)
	return out
}

// buildExVideoFrameTag constructs a CodedFramesX E-RTMP video tag for
// a VP9/AV1 access unit — no composition time, one header byte, one
// FourCC, then the raw frame bytes.
func buildExVideoFrameTag(fourCC string, isKey bool, accessUnit []byte) []byte {
	out := make([]byte, 5+len(accessUnit))
	out[0] = exVideoHeaderByte(isKey, flvExVideoPacketCodedFrmX)
	copy(out[1:5], []byte(fourCC))
	copy(out[5:], accessUnit)
	return out
}

// buildVP9ConfigRecord produces a VPCodecConfigurationRecord (ISO/IEC
// 14496-15 §10, vpcC box). E-RTMP re-uses the same 8-byte record.
//
// We default to profile 0 / 4:2:0 8-bit / BT.709 color — the same
// parameters WebRTC stacks (Chrome, pion) overwhelmingly produce for
// real-time VP9. If a publisher negotiates HDR we'd need to derive
// these from the uncompressed VP9 frame header; for the broadcast
// passthrough path today the static record is accurate enough for
// ffmpeg and the majority of E-RTMP-aware players.
func buildVP9ConfigRecord(profile, level uint8) []byte {
	var out [8]byte
	out[0] = profile
	out[1] = level
	// bitDepth=8 (upper 4 bits), chromaSubsampling=1 (next 3 bits),
	// videoFullRangeFlag=0 (lowest bit).
	out[2] = (8 << 4) | (1 << 1)
	out[3] = 1 // colorPrimaries = BT.709
	out[4] = 1 // transferCharacteristics = BT.709
	out[5] = 1 // matrixCoefficients = BT.709
	// codecIntializationDataSize = 0 (bytes 6..7 already zero)
	return out[:]
}

// buildAV1ConfigRecord wraps an AV1 Sequence Header OBU into an
// AV1CodecConfigurationRecord (AV1-ISOBMFF §2.2.1). configOBUs must
// be the Sequence Header OBU bytes (including the OBU header), which
// the AV1 depacketizer captures on the first keyframe.
//
// For 8-bit 4:2:0 profile 0 streams the configuration fields collapse
// to a well-known prefix; we still parse the seq header to pick up
// seq_profile and seq_level_idx so downstream decoders that key on
// them (vlc, some ExoPlayer builds) behave correctly.
func buildAV1ConfigRecord(seqHeaderOBU []byte, seqProfile, seqLevel, seqTier uint8, highBitDepth, monochrome, chromaSubX, chromaSubY bool) []byte {
	out := make([]byte, 4+len(seqHeaderOBU))
	// Byte 0: marker(1) = 1, version(7) = 1.
	out[0] = 0x81
	// Byte 1: seq_profile(3), seq_level_idx_0(5).
	out[1] = ((seqProfile & 0x07) << 5) | (seqLevel & 0x1F)
	// Byte 2: seq_tier_0, high_bitdepth, twelve_bit, monochrome,
	// chroma_subsampling_x, chroma_subsampling_y, chroma_sample_position(2).
	var b byte
	if seqTier != 0 {
		b |= 1 << 7
	}
	if highBitDepth {
		b |= 1 << 6
	}
	// twelve_bit left 0 for 8/10-bit streams
	if monochrome {
		b |= 1 << 4
	}
	if chromaSubX {
		b |= 1 << 3
	}
	if chromaSubY {
		b |= 1 << 2
	}
	out[2] = b
	// Byte 3: reserved(3)=0, initial_presentation_delay_present(1)=0,
	// reserved(4)=0.
	out[3] = 0
	copy(out[4:], seqHeaderOBU)
	return out
}

// Enhanced RTMP audio (veovera v1-2023-12 §8.2) constants. We emit Opus
// via the Ex-Audio path because classic FLV has no codec slot for Opus
// that every receiver agrees on (ffmpeg understands both, but nginx-rtmp
// and some downstream consumers only accept the Ex path).
//
// Layout we emit:
//
//	byte 0   : 0x9X — high nibble 9 = Ex-Audio marker; low nibble = packet type
//	bytes 1-4: FourCC = "Opus"
//	bytes 5..: body (OpusHead for SequenceStart, Opus packet for CodedFrames)
const (
	flvExAudioPacketSeqStart    byte = 0x00
	flvExAudioPacketCodedFrames byte = 0x01

	flvExAudioFourCCOpus = "Opus"
)

// buildOpusSeqHeaderTag builds the Ex-Audio SequenceStart body with an
// OpusHead (RFC 7845 §5.1). Called once before any Opus frame tag so
// receivers know channel count / sample rate up front.
func buildOpusSeqHeaderTag(channels uint8) []byte {
	// OpusHead: 19 bytes, magic "OpusHead" + version 1 + channels +
	// preSkip(LE16) + inputSampleRate(LE32) + outputGain(LE16) +
	// mappingFamily(1).
	opusHead := make([]byte, 19)
	copy(opusHead[0:], "OpusHead")
	opusHead[8] = 0x01 // version
	opusHead[9] = channels
	binary.LittleEndian.PutUint16(opusHead[10:], 0)     // preSkip
	binary.LittleEndian.PutUint32(opusHead[12:], 48000) // input sample rate
	// opusHead[16..17] outputGain = 0 (left zero)
	// opusHead[18]     mappingFamily = 0 (mono/stereo, left zero)

	// Assemble full body: [0x90][FourCC "Opus"][OpusHead].
	out := make([]byte, 5+len(opusHead))
	out[0] = 0x90 | flvExAudioPacketSeqStart
	copy(out[1:5], []byte(flvExAudioFourCCOpus))
	copy(out[5:], opusHead)
	return out
}

// buildOpusFrameTag wraps one Opus packet in an Ex-Audio CodedFrames
// body:  [0x91][FourCC "Opus"][opus packet].
func buildOpusFrameTag(opusPacket []byte) []byte {
	out := make([]byte, 5+len(opusPacket))
	out[0] = 0x90 | flvExAudioPacketCodedFrames
	copy(out[1:5], []byte(flvExAudioFourCCOpus))
	copy(out[5:], opusPacket)
	return out
}
