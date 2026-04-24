package broadcast

import (
	"encoding/binary"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
)

// H264 NALU types we key on. Kept here (instead of reusing the RTMP
// ingest package) so this package has no dependency on pkg/rtc/rtmp —
// the coupling would be purely nominal and obscure the fact that the
// egress path only needs a 3-item enum.
const (
	naluTypeSPS  byte = 7
	naluTypePPS  byte = 8
	naluTypeIDR  byte = 5
	naluTypeAUD  byte = 9
	naluTypeSEI  byte = 6
	naluTypeMask byte = 0x1F
)

// H264Depacketizer reassembles RTP payloads into H264 access units in
// AVCC format (4-byte BE length prefix + NALU bytes, repeated), which
// is the shape FLV and MP4 want.
//
// The assembler is intentionally simple: it collects NALUs in a scratch
// buffer, flushes on Marker bit (end of access unit), and caches the
// most recent SPS/PPS so the pusher can emit an AVCDecoderConfiguration
// Record at connection start.
//
// Not thread-safe: Push is expected to be called from a single
// Consumer.OnRtp goroutine (mediasoup delivers per-consumer callbacks
// serially).
type H264Depacketizer struct {
	unmarshaller codecs.H264Packet

	au         []byte
	auIsKey    bool
	auHasSPS   bool
	auHasPPS   bool
	sps        []byte
	pps        []byte
	seenIDR    bool
}

// Push delivers one RTP packet and, if that packet completes an access
// unit, returns (sample, true). The returned sample.Data is owned by
// the depacketizer and reused on the next Push — callers that need to
// retain the bytes must copy them before the next call.
func (d *H264Depacketizer) Push(pkt *rtp.Packet) (MediaSample, bool) {
	nalu, err := d.unmarshaller.Unmarshal(pkt.Payload)
	if err != nil || len(nalu) == 0 {
		if pkt.Marker {
			return d.flush(pkt.Timestamp)
		}
		return MediaSample{}, false
	}

	// codecs.H264Packet returns Annex-B bytes (concatenated NALUs
	// prefixed with [0,0,0,1] start codes). Split on start code,
	// capture SPS/PPS/IDR bits, then append each NALU as AVCC into
	// the access-unit scratch buffer.
	for _, singleNALU := range splitAnnexBNALUs(nalu) {
		if len(singleNALU) == 0 {
			continue
		}
		typ := singleNALU[0] & naluTypeMask
		switch typ {
		case naluTypeSPS:
			d.sps = append(d.sps[:0], singleNALU...)
			d.auHasSPS = true
		case naluTypePPS:
			d.pps = append(d.pps[:0], singleNALU...)
			d.auHasPPS = true
		case naluTypeIDR:
			d.auIsKey = true
			d.seenIDR = true
		case naluTypeAUD:
			// Access Unit Delimiters are redundant inside AVCC
			// framing (FLV already delimits access units with its
			// tag structure). Dropping AUDs keeps the payload
			// tight and avoids confusing decoders like
			// ffmpeg's flvenc.
			continue
		case naluTypeSEI:
			// SEI is optional and occasionally carries publisher
			// metadata (e.g. timestamps). Forward as-is; some
			// decoders ignore it, none reject it.
		}
		// AVCC: [u32 length BE][NALU bytes]
		avccHeader := [4]byte{}
		binary.BigEndian.PutUint32(avccHeader[:], uint32(len(singleNALU)))
		d.au = append(d.au, avccHeader[:]...)
		d.au = append(d.au, singleNALU...)
	}

	if !pkt.Marker {
		return MediaSample{}, false
	}
	return d.flush(pkt.Timestamp)
}

// flush returns the accumulated access unit as a MediaSample and
// resets the scratch state. Returns ok=false for empty AUs (typical on
// FU-A fragments that were partially reassembled when a timeout hit).
func (d *H264Depacketizer) flush(ts uint32) (MediaSample, bool) {
	if len(d.au) == 0 {
		d.auIsKey = false
		d.auHasSPS = false
		d.auHasPPS = false
		return MediaSample{}, false
	}
	// Emit a copy so Data outlives our scratch buffer.
	data := make([]byte, len(d.au))
	copy(data, d.au)
	s := MediaSample{
		Kind:         SampleKindVideo,
		Codec:        CodecH264,
		RTPTimestamp: ts,
		IsKeyFrame:   d.auIsKey,
		Data:         data,
	}
	d.au = d.au[:0]
	d.auIsKey = false
	d.auHasSPS = false
	d.auHasPPS = false
	return s, true
}

// ExtradataAVCC returns a freshly-allocated AVCDecoderConfigurationRecord
// built from the most recently seen SPS/PPS, or nil if the depacketizer
// has not yet observed both. The record is what the RTMP pusher writes
// as its first video tag (FLV AVC seq header).
func (d *H264Depacketizer) ExtradataAVCC() []byte {
	if len(d.sps) == 0 || len(d.pps) == 0 {
		return nil
	}
	return buildAVCDecoderConfigurationRecord(d.sps, d.pps)
}

// ConfigRecord is the videoDepacketizer-interface alias of
// ExtradataAVCC — kept as a thin wrapper so H264 satisfies the same
// contract as the VP9 / AV1 depacketizers without introducing a
// renaming churn in callers that already reach for the legacy name.
func (d *H264Depacketizer) ConfigRecord() []byte { return d.ExtradataAVCC() }

// HasConfig reports whether we have captured enough NALUs (SPS+PPS) to
// build AVCC extradata. Pushers should not start emitting frame data
// until this is true AND an IDR has been observed.
func (d *H264Depacketizer) HasConfig() bool {
	return len(d.sps) > 0 && len(d.pps) > 0
}

// SeenIDR reports whether the depacketizer has observed an IDR frame
// yet. Pushers gate their "first frame" logic on this to avoid sending
// broken GOPs that start with a P-frame.
func (d *H264Depacketizer) SeenIDR() bool { return d.seenIDR }

// splitAnnexBNALUs splits a byte slice at H.264 Annex-B start codes
// ([0,0,0,1] or [0,0,1]) and returns the bytes between them. The
// start codes themselves are dropped; an empty slice in → no output.
func splitAnnexBNALUs(buf []byte) [][]byte {
	var out [][]byte
	i := 0
	start := -1
	for i < len(buf) {
		// Find next start code.
		if i+3 < len(buf) && buf[i] == 0 && buf[i+1] == 0 &&
			buf[i+2] == 0 && buf[i+3] == 1 {
			if start >= 0 {
				out = append(out, buf[start:i])
			}
			i += 4
			start = i
			continue
		}
		if i+2 < len(buf) && buf[i] == 0 && buf[i+1] == 0 && buf[i+2] == 1 {
			if start >= 0 {
				out = append(out, buf[start:i])
			}
			i += 3
			start = i
			continue
		}
		i++
	}
	if start >= 0 && start < len(buf) {
		out = append(out, buf[start:])
	}
	return out
}

// buildAVCDecoderConfigurationRecord constructs the ISO 14496-15 §5.2.4.1
// record that FLV ships as the AVC seq header body. Only one SPS and
// one PPS are supplied — mediasoup/pion produce streams with exactly one
// of each, and carrying more buys us nothing. The length prefix in the
// SPS/PPS entries is uint16 BE per spec.
func buildAVCDecoderConfigurationRecord(sps, pps []byte) []byte {
	// avcC layout:
	//   [0]    configurationVersion = 1
	//   [1]    AVCProfileIndication = SPS[1]
	//   [2]    profile_compatibility = SPS[2]
	//   [3]    AVCLevelIndication = SPS[3]
	//   [4]    0xFF (lengthSizeMinusOne = 3 in low 2 bits, reserved bits 1)
	//   [5]    0xE0 | numOfSPS (low 5 bits) = 0xE1
	//   [6-7]  spsLen BE
	//   [...]  SPS bytes
	//   [N]    numOfPPS = 1
	//   [N+1-N+2] ppsLen BE
	//   [...]  PPS bytes
	if len(sps) < 4 {
		return nil
	}
	out := make([]byte, 0, 11+len(sps)+len(pps))
	out = append(out,
		0x01,            // configurationVersion
		sps[1],          // profile_idc
		sps[2],          // profile_compatibility
		sps[3],          // level_idc
		0xFF,            // 6 reserved bits '111111' + lengthSizeMinusOne '11' = 4-byte NALU length
		0xE1,            // 3 reserved bits '111' + numOfSequenceParameterSets = 1
	)
	var spsLen [2]byte
	binary.BigEndian.PutUint16(spsLen[:], uint16(len(sps)))
	out = append(out, spsLen[:]...)
	out = append(out, sps...)
	out = append(out, 0x01) // numOfPictureParameterSets = 1
	var ppsLen [2]byte
	binary.BigEndian.PutUint16(ppsLen[:], uint16(len(pps)))
	out = append(out, ppsLen[:]...)
	out = append(out, pps...)
	return out
}
