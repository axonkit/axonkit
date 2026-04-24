package rtmp

import "fmt"

// AV1CodecConfigurationRecord is the minimal parsed form of the av1C
// configuration box defined by AV1-ISOBMFF §2.3. Enhanced RTMP ships
// this record as the SequenceStart payload for FourCC "av01".
//
// Fixed 4-byte layout (configOBUs that follow are not parsed here):
//
//	byte 0 : marker(1)=1 | version(7)=1
//	byte 1 : seq_profile(3) | seq_level_idx_0(5)
//	byte 2 : seq_tier_0(1) | high_bitdepth(1) | twelve_bit(1) |
//	         monochrome(1) | chroma_subsampling_x(1) | chroma_subsampling_y(1) |
//	         chroma_sample_position(2)
//	byte 3 : reserved(3) | initial_presentation_delay_present(1) |
//	         initial_presentation_delay_minus_one(4) or reserved(4)
//
// The RTMP ingest only needs profile / level / tier to advertise the
// right video/AV1 capability entry; the remaining bits are surfaced so
// test assertions can sanity-check parsing.
type AV1CodecConfigurationRecord struct {
	Version                         uint8
	SeqProfile                      uint8
	SeqLevelIdx0                    uint8
	SeqTier0                        uint8
	HighBitdepth                    bool
	TwelveBit                       bool
	Monochrome                      bool
	ChromaSubsamplingX              bool
	ChromaSubsamplingY              bool
	ChromaSamplePosition            uint8
	InitialPresentationDelayPresent bool
	InitialPresentationDelayMinus1  uint8
}

// ParseAV1CodecConfigurationRecord decodes the fixed 4-byte header of
// the av1C record (configOBUs — if any — are intentionally ignored).
// Returns an error only when the input is shorter than 4 bytes or the
// marker / version bits are wrong, so test vectors caught mid-stream
// are still parseable.
func ParseAV1CodecConfigurationRecord(data []byte) (*AV1CodecConfigurationRecord, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("av1C too short: %d bytes", len(data))
	}
	if data[0]&0x80 == 0 {
		return nil, fmt.Errorf("av1C marker bit not set: 0x%02X", data[0])
	}
	if version := data[0] & 0x7F; version != 1 {
		return nil, fmt.Errorf("av1C version %d not supported", version)
	}

	rec := &AV1CodecConfigurationRecord{
		Version:              data[0] & 0x7F,
		SeqProfile:           (data[1] >> 5) & 0x07,
		SeqLevelIdx0:         data[1] & 0x1F,
		SeqTier0:             (data[2] >> 7) & 0x01,
		HighBitdepth:         (data[2]>>6)&0x01 != 0,
		TwelveBit:            (data[2]>>5)&0x01 != 0,
		Monochrome:           (data[2]>>4)&0x01 != 0,
		ChromaSubsamplingX:   (data[2]>>3)&0x01 != 0,
		ChromaSubsamplingY:   (data[2]>>2)&0x01 != 0,
		ChromaSamplePosition: data[2] & 0x03,
	}
	rec.InitialPresentationDelayPresent = (data[3]>>4)&0x01 != 0
	if rec.InitialPresentationDelayPresent {
		rec.InitialPresentationDelayMinus1 = data[3] & 0x0F
	}
	return rec, nil
}
