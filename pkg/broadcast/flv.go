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
// ctsMs is the PTS-DTS offset in milliseconds, computed by the RTMP
// pusher's reorder path when B-frames are present.
func buildAVCFrameTag(isKey bool, ctsMs int32, avccAccessUnit []byte) []byte {
	out := make([]byte, 5+len(avccAccessUnit))
	if isKey {
		out[0] = flvFrameTypeKey | flvVideoCodecAVC // 0x17
	} else {
		out[0] = flvFrameTypeInter | flvVideoCodecAVC // 0x27
	}
	out[1] = flvAVCPacketTypeNALU
	encodeSignedInt24BE(out[2:5], ctsMs)
	copy(out[5:], avccAccessUnit)
	return out
}

// encodeSignedInt24BE writes a 24-bit signed big-endian integer into
// the first 3 bytes of buf. Out-of-range values are wrapped modulo
// 2^24, matching how go's int→uint conversion truncates: the
// FLV/E-RTMP CTS field cannot exceed ±2^23 milliseconds (~2.3 hours)
// in practice, so the truncation only matters as a defensive bound.
func encodeSignedInt24BE(buf []byte, v int32) {
	u := uint32(v) & 0x00FFFFFF
	buf[0] = byte(u >> 16)
	buf[1] = byte(u >> 8)
	buf[2] = byte(u)
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

	flvExVideoPacketSeqStart    byte = 0x00
	flvExVideoPacketCodedFrames byte = 0x01
	flvExVideoPacketCodedFrmX   byte = 0x03

	flvExVideoFourCCVP9  = "vp09"
	flvExVideoFourCCAV1  = "av01"
	flvExVideoFourCCH265 = "hvc1"
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
//
// Used for codecs whose RTP path does not surface a (PTS, DTS) skew
// (VP9, AV1 today). HEVC takes the CodedFrames variant below so its
// B-frame composition offset is preserved end-to-end.
func buildExVideoFrameTag(fourCC string, isKey bool, accessUnit []byte) []byte {
	out := make([]byte, 5+len(accessUnit))
	out[0] = exVideoHeaderByte(isKey, flvExVideoPacketCodedFrmX)
	copy(out[1:5], []byte(fourCC))
	copy(out[5:], accessUnit)
	return out
}

// buildExVideoCodedFramesTag constructs a CodedFrames (PacketType=1)
// E-RTMP video tag carrying an HEVC (or future H.264-via-FourCC)
// access unit:
//
//	[header byte] [FourCC] [SI24 CTS] [access unit]
//
// ctsMs is the (PTS-DTS) offset in milliseconds, encoded the same way
// as buildAVCFrameTag's classic AVC tag — so the egress reorder
// buffer in rtmp_pusher.go can hand HEVC samples to this builder
// unchanged. For passthrough callers that do not maintain a (DTS,
// CTS) split, ctsMs=0 produces the same wire layout as if the
// CodedFramesX form had been used (modulo the extra 3 zero bytes),
// which is the correct semantic: PTS == DTS.
func buildExVideoCodedFramesTag(fourCC string, isKey bool, ctsMs int32, accessUnit []byte) []byte {
	out := make([]byte, 8+len(accessUnit))
	out[0] = exVideoHeaderByte(isKey, flvExVideoPacketCodedFrames)
	copy(out[1:5], []byte(fourCC))
	encodeSignedInt24BE(out[5:8], ctsMs)
	copy(out[8:], accessUnit)
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

// buildHEVCConfigRecord constructs an HEVCDecoderConfigurationRecord
// (ISO/IEC 14496-15 §8.3.3.1.2) carried in the E-RTMP "hvc1"
// SequenceStart tag. Layout:
//
//	configurationVersion (1) = 1
//	general_profile_space(2) | general_tier_flag(1) | general_profile_idc(5)
//	general_profile_compatibility_flags (32 bits)
//	general_constraint_indicator_flags (48 bits)
//	general_level_idc (8)
//	0xF0 | min_spatial_segmentation_idc>>8 (4 bits = 0)
//	min_spatial_segmentation_idc lo (8) = 0
//	0xFC | parallelismType(2) = 0
//	0xFC | chromaFormat(2)        — default 1 (4:2:0)
//	0xF8 | bitDepthLumaMinus8(3)  — default 0 (8-bit)
//	0xF8 | bitDepthChromaMinus8(3)— default 0 (8-bit)
//	avgFrameRate (16) = 0
//	constantFrameRate(2) | numTemporalLayers(3)=1 | temporalIdNested(1)=1 | lengthSizeMinusOne(2)=3
//	numOfArrays (8) = 3 (VPS / SPS / PPS)
//	for each array:
//	    [array_completeness(1)=1 | reserved(1)=0 | NAL_unit_type(6)]
//	    numNalus (16) = 1
//	    nalUnitLength (16) | nalUnit bytes
//
// general_profile_idc / general_tier_flag / general_level_idc are
// extracted from the SPS profile_tier_level block; everything else
// uses Main profile / 4:2:0 / 8-bit defaults that match avflow's
// HEVC encoder configuration today.
func buildHEVCConfigRecord(vps, sps, pps []byte) []byte {
	if len(vps) < 2 || len(sps) < 14 || len(pps) < 2 {
		return nil
	}

	// SPS payload structure (after the 2-byte NAL header):
	//   sps_video_parameter_set_id (4) | sps_max_sub_layers_minus1 (3) | sps_temporal_id_nesting_flag (1) = 1 byte
	//   profile_tier_level(profilePresentFlag=1, sps_max_sub_layers_minus1):
	//     general_profile_space(2) | general_tier_flag(1) | general_profile_idc(5)  = 1 byte
	//     general_profile_compatibility_flag[0..31]                                 = 4 bytes
	//     general_constraint_indicator_flags                                        = 6 bytes
	//     general_level_idc                                                         = 1 byte
	// We read 12 bytes starting at sps[2+1] to capture the static profile_tier_level prefix.
	const spsHeader = 2
	ptlOffset := spsHeader + 1
	if len(sps) < ptlOffset+12 {
		return nil
	}
	ptl := sps[ptlOffset : ptlOffset+12]
	profileSpaceTierFlagProfileIdc := ptl[0]
	profileCompatFlags := ptl[1:5]
	constraintFlags := ptl[5:11]
	levelIdc := ptl[11]

	out := make([]byte, 0, 64+len(vps)+len(sps)+len(pps))
	// configurationVersion fixed to 0x01 per ISO/IEC 14496-15.
	out = append(out, 0x01)
	// PTL prefix: profile_space(2) tier_flag(1) profile_idc(5).
	out = append(out, profileSpaceTierFlagProfileIdc)
	// profile_compatibility_flags spans 32 bits.
	out = append(out, profileCompatFlags...)
	// general_constraint_indicator_flags spans 48 bits.
	out = append(out, constraintFlags...)
	// general_level_idc byte.
	out = append(out, levelIdc)
	// reserved 4 bits set + min_spatial_segmentation_idc fixed to 0.
	out = append(out, 0xF0, 0x00)
	// reserved 6 bits set + parallelismType fixed to 0.
	out = append(out, 0xFC)
	// reserved 6 bits set + chromaFormat fixed to 1 (4:2:0).
	out = append(out, 0xFD)
	// reserved 5 bits set + bitDepthLumaMinus8 fixed to 0 (8-bit).
	out = append(out, 0xF8)
	// reserved 5 bits set + bitDepthChromaMinus8 fixed to 0 (8-bit).
	out = append(out, 0xF8)
	// avgFrameRate fixed to 0 (16 bits).
	out = append(out, 0x00, 0x00)
	// CFR(2)=0 | numTemporalLayers(3)=1 | temporalIdNested(1)=1 |
	// lengthSizeMinusOne(2)=3.
	out = append(out, byte((0<<6)|(1<<3)|(1<<2)|0x03))
	// numOfArrays packs three NAL types: VPS, SPS, PPS.
	out = append(out, 0x03)

	const (
		hevcNALUTypeVPS = 32
		hevcNALUTypeSPS = 33
		hevcNALUTypePPS = 34
	)
	for _, e := range []struct {
		nalType byte
		nal     []byte
	}{
		{hevcNALUTypeVPS, vps},
		{hevcNALUTypeSPS, sps},
		{hevcNALUTypePPS, pps},
	} {
		// array_completeness(1)=1 | reserved(1)=0 | NAL_unit_type(6)
		out = append(out, 0x80|(e.nalType&0x3F))
		var numNalus [2]byte
		binary.BigEndian.PutUint16(numNalus[:], 1)
		out = append(out, numNalus[:]...)
		var nalLen [2]byte
		binary.BigEndian.PutUint16(nalLen[:], uint16(len(e.nal)))
		out = append(out, nalLen[:]...)
		out = append(out, e.nal...)
	}
	return out
}
