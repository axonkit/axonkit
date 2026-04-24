package rtmp

import "fmt"

// VPCodecConfigurationRecord is the minimal parsed form of the vpcC
// box defined by ISO/IEC 14496-15 §VP sample entry (also used by
// WebM/Matroska). Enhanced RTMP ships this record as the
// SequenceStart payload for FourCC "vp09".
//
// Layout (12 fixed bytes + variable codecInitializationData):
//
//	byte 0     : profile
//	byte 1     : level
//	byte 2     : bit_depth (hi 4 bits) | chroma_subsampling (bits 1-3) |
//	             video_full_range_flag (bit 0)
//	byte 3     : color_primaries
//	byte 4     : transfer_characteristics
//	byte 5     : matrix_coefficients
//	bytes 6..7 : codecInitializationDataSize (uint16 BE, always 0 per
//	             the current spec — reserved for future HDR metadata)
//	bytes 8..  : codecInitializationData (unused, skipped)
//
// VP9 profile values map to mediasoup's video/VP9 capability entries:
//
//	0: 8-bit 4:2:0  (default Chrome offer)
//	1: 8-bit 4:2:2 or 4:4:4
//	2: 10/12-bit 4:2:0 (HDR)
//	3: 10/12-bit 4:2:2 or 4:4:4
//
// The RTMP ingest only passes the profile through to
// RtpCodecParameters so the router announces a compatible VP9 PT.
type VPCodecConfigurationRecord struct {
	Profile                 uint8
	Level                   uint8
	BitDepth                uint8
	ChromaSubsampling       uint8
	VideoFullRangeFlag      uint8
	ColorPrimaries          uint8
	TransferCharacteristics uint8
	MatrixCoefficients      uint8
}

// ParseVPCodecConfigurationRecord decodes the vpcC record bytes that
// Enhanced RTMP ships as the SequenceStart payload for FourCC "vp09".
//
// Empty payload is tolerated — some publishers (ffmpeg's librtmp-only
// builds in particular) emit a zero-length vpcC and rely on the first
// few VP9 frames to let the decoder probe the bitstream. In that case
// we default to profile=0 (the most common 8-bit 4:2:0) and let the
// session select the matching mediasoup capability entry.
func ParseVPCodecConfigurationRecord(data []byte) (*VPCodecConfigurationRecord, error) {
	if len(data) == 0 {
		return &VPCodecConfigurationRecord{}, nil
	}
	if len(data) < 8 {
		return nil, fmt.Errorf("vpcC too short: %d bytes", len(data))
	}
	rec := &VPCodecConfigurationRecord{
		Profile:                 data[0],
		Level:                   data[1],
		BitDepth:                data[2] >> 4,
		ChromaSubsampling:       (data[2] >> 1) & 0x07,
		VideoFullRangeFlag:      data[2] & 0x01,
		ColorPrimaries:          data[3],
		TransferCharacteristics: data[4],
		MatrixCoefficients:      data[5],
	}
	// codecInitializationDataSize is mandated to be zero by the current
	// spec; we do not consume the trailing bytes even if present.
	return rec, nil
}
