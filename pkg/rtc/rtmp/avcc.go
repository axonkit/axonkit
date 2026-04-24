package rtmp

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// AVCDecoderConfigurationRecord is the minimal parsed form of ISO 14496-15
// §5.2.4.1 records used by RTMP/FLV and MP4. axonkit needs it to:
//
//   - extract SPS/PPS for the H.264 RTP payloader to emit STAP-A keyframes;
//   - compute the RFC 6184 `profile-level-id` string that mediasoup wants
//     in RtpCodecParameters so the routing layer picks the right H.264
//     variant.
//
// The record layout is:
//
//	byte 0          : configurationVersion (always 0x01)
//	byte 1          : AVCProfileIndication
//	byte 2          : profile_compatibility
//	byte 3          : AVCLevelIndication
//	byte 4          : (lengthSizeMinusOne) in low 2 bits; must be 3 (4-byte)
//	byte 5          : (numOfSequenceParameterSets) in low 5 bits
//	bytes 6..       : numSPS * (uint16 length + SPS bytes)
//	byte N          : numOfPictureParameterSets
//	bytes N+1..     : numPPS * (uint16 length + PPS bytes)
type AVCDecoderConfigurationRecord struct {
	ProfileIdc      uint8
	ProfileCompat   uint8
	LevelIdc        uint8
	LengthSizeMinus uint8 // always 3 in practice (4-byte AVCC length)
	SPS             [][]byte
	PPS             [][]byte
}

// ProfileLevelID returns the 6-hex-char string mediasoup passes through
// in RtpCodecParameters.Parameters.ProfileLevelId (e.g. "42e01f"). It is
// the concatenation of profile_idc, profile_compatibility, level_idc.
func (r *AVCDecoderConfigurationRecord) ProfileLevelID() string {
	return hex.EncodeToString([]byte{r.ProfileIdc, r.ProfileCompat, r.LevelIdc})
}

// ParseAVCDecoderConfigurationRecord decodes the AVCDecoderConfiguration-
// Record bytes that FLV ships as the seq-header payload.
func ParseAVCDecoderConfigurationRecord(data []byte) (*AVCDecoderConfigurationRecord, error) {
	if len(data) < 7 {
		return nil, fmt.Errorf("avcC too short: %d bytes", len(data))
	}
	if data[0] != 0x01 {
		return nil, fmt.Errorf("avcC configurationVersion %d not 1", data[0])
	}

	rec := &AVCDecoderConfigurationRecord{
		ProfileIdc:      data[1],
		ProfileCompat:   data[2],
		LevelIdc:        data[3],
		LengthSizeMinus: data[4] & 0x03,
	}

	numSPS := int(data[5] & 0x1F)
	offset := 6
	for i := 0; i < numSPS; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("avcC SPS header truncated at offset %d", offset)
		}
		length := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+length > len(data) {
			return nil, fmt.Errorf("avcC SPS payload truncated: need %d, have %d", length, len(data)-offset)
		}
		rec.SPS = append(rec.SPS, data[offset:offset+length])
		offset += length
	}

	if offset >= len(data) {
		return nil, fmt.Errorf("avcC missing PPS count")
	}
	numPPS := int(data[offset])
	offset++
	for i := 0; i < numPPS; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("avcC PPS header truncated at offset %d", offset)
		}
		length := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+length > len(data) {
			return nil, fmt.Errorf("avcC PPS payload truncated: need %d, have %d", length, len(data)-offset)
		}
		rec.PPS = append(rec.PPS, data[offset:offset+length])
		offset += length
	}

	if rec.LengthSizeMinus != 3 {
		return nil, fmt.Errorf("avcC lengthSizeMinusOne=%d, only 3 (4-byte) is supported", rec.LengthSizeMinus)
	}
	if len(rec.SPS) == 0 || len(rec.PPS) == 0 {
		return nil, fmt.Errorf("avcC missing SPS or PPS (sps=%d pps=%d)", len(rec.SPS), len(rec.PPS))
	}
	return rec, nil
}
