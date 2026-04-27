package broadcast

import (
	"encoding/binary"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
)

// H.265 NAL unit types we key on (ITU-T H.265 §7.4.2). The 6-bit
// nal_unit_type lives in bits 1..6 of NAL header byte 0:
//
//	nal_unit_type = (byte0 >> 1) & 0x3F
const (
	hevcNALUTypeTrailR       byte = 1
	hevcNALUTypeBlaWLp       byte = 16
	hevcNALUTypeBlaWRadl     byte = 17
	hevcNALUTypeBlaNLp       byte = 18
	hevcNALUTypeIdrWRadl     byte = 19
	hevcNALUTypeIdrNLp       byte = 20
	hevcNALUTypeCraNut       byte = 21
	hevcNALUTypeRsvIrapVcl22 byte = 22
	hevcNALUTypeRsvIrapVcl23 byte = 23
	hevcNALUTypeVpsNut       byte = 32
	hevcNALUTypeSpsNut       byte = 33
	hevcNALUTypePpsNut       byte = 34
	hevcNALUTypeAudNut       byte = 35
	hevcNALUTypePrefSEI      byte = 39
	hevcNALUTypeSufSEI       byte = 40
)

// H265Depacketizer reassembles H.265 RTP packets (RFC 7798) into
// access units in HVCC framing (4-byte BE length prefix per NALU,
// repeated), suitable for E-RTMP "hvc1" CodedFramesX tags or MP4
// `hvc1` sample data.
//
// Pion's codecs.H265Depacketizer hides the AP / FU / DONL bookkeeping
// and emits Annex-B bytes; we split on start codes, capture
// VPS/SPS/PPS for the HEVCDecoderConfigurationRecord, and detect IRAP
// (BLA/IDR/CRA) NALUs to flag the access unit as a key frame.
//
// The live source can never reach this depacketizer today —
// mediasoup-go's RTP capability table omits H.265, so the SFU
// rejects any router that advertises it (see pkg/media/caps.go
// comments). The plumbing is staged here so the future pion-based
// mix WHIP ingress (figure 5 of the patent doc) can reuse it
// without further changes to switcher / pusher.
//
// Not thread-safe: Push is expected to be called from a single
// goroutine.
type H265Depacketizer struct {
	unmarshaller codecs.H265Depacketizer

	au       []byte
	auIsKey  bool
	auHasVPS bool
	auHasSPS bool
	auHasPPS bool
	vps      []byte
	sps      []byte
	pps      []byte
	seenKey  bool
}

// Push delivers one RTP packet. Returns (sample, true) when the
// packet's marker bit signals end-of-access-unit. The returned sample
// owns its Data slice (fresh allocation per access unit).
func (d *H265Depacketizer) Push(pkt *rtp.Packet) (MediaSample, bool) {
	payload, err := d.unmarshaller.Unmarshal(pkt.Payload)
	if err != nil || len(payload) == 0 {
		if pkt.Marker {
			return d.flush(pkt.Timestamp)
		}
		return MediaSample{}, false
	}

	for _, nal := range splitAnnexBNALUs(payload) {
		if len(nal) < 2 {
			continue
		}
		typ := (nal[0] >> 1) & 0x3F
		switch typ {
		case hevcNALUTypeVpsNut:
			d.vps = append(d.vps[:0], nal...)
			d.auHasVPS = true
		case hevcNALUTypeSpsNut:
			d.sps = append(d.sps[:0], nal...)
			d.auHasSPS = true
		case hevcNALUTypePpsNut:
			d.pps = append(d.pps[:0], nal...)
			d.auHasPPS = true
		case hevcNALUTypeBlaWLp, hevcNALUTypeBlaWRadl, hevcNALUTypeBlaNLp,
			hevcNALUTypeIdrWRadl, hevcNALUTypeIdrNLp, hevcNALUTypeCraNut,
			hevcNALUTypeRsvIrapVcl22, hevcNALUTypeRsvIrapVcl23:
			d.auIsKey = true
		case hevcNALUTypeAudNut:
			// Drop AUDs — FLV / hvc1 framing already delimits AUs.
			continue
		case hevcNALUTypePrefSEI, hevcNALUTypeSufSEI:
			// SEI is informational; forward verbatim.
		}
		var lenPrefix [4]byte
		binary.BigEndian.PutUint32(lenPrefix[:], uint32(len(nal)))
		d.au = append(d.au, lenPrefix[:]...)
		d.au = append(d.au, nal...)
	}

	if !pkt.Marker {
		return MediaSample{}, false
	}
	return d.flush(pkt.Timestamp)
}

func (d *H265Depacketizer) flush(ts uint32) (MediaSample, bool) {
	if len(d.au) == 0 {
		d.auIsKey = false
		d.auHasVPS = false
		d.auHasSPS = false
		d.auHasPPS = false
		return MediaSample{}, false
	}
	data := make([]byte, len(d.au))
	copy(data, d.au)
	isKey := d.auIsKey
	if isKey {
		d.seenKey = true
	}
	d.au = d.au[:0]
	d.auIsKey = false
	d.auHasVPS = false
	d.auHasSPS = false
	d.auHasPPS = false
	return MediaSample{
		Kind:         SampleKindVideo,
		Codec:        CodecH265,
		RTPTimestamp: ts,
		IsKeyFrame:   isKey,
		Data:         data,
	}, true
}

// HasConfig reports whether VPS+SPS+PPS have all been observed,
// which is what hvcC needs.
func (d *H265Depacketizer) HasConfig() bool {
	return len(d.vps) > 0 && len(d.sps) > 0 && len(d.pps) > 0
}

// SeenIDR reports whether at least one IRAP (BLA / IDR / CRA) access
// unit has been emitted. Pushers should not start framing video
// until this is true.
func (d *H265Depacketizer) SeenIDR() bool { return d.seenKey }

// ConfigRecord returns the HEVCDecoderConfigurationRecord (hvcC) the
// E-RTMP "hvc1" SequenceStart tag wraps. nil until VPS+SPS+PPS have
// been captured.
func (d *H265Depacketizer) ConfigRecord() []byte {
	if !d.HasConfig() {
		return nil
	}
	return buildHEVCConfigRecord(d.vps, d.sps, d.pps)
}
