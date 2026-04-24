package broadcast

import (
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/rtp/codecs/av1/obu"
)

// AV1Depacketizer reassembles AV1 RTP packets (AV1 RTP spec v1.0,
// aomediacodec.github.io/av1-rtp-spec) into complete temporal units
// (one access unit) suitable for Enhanced-RTMP CodedFramesX tags.
//
// The heavy lifting — parsing the aggregation header, handling
// fragmented OBUs, writing obu_size_field per OBU — is delegated to
// pion's codecs.AV1Depacketizer. On top of that we:
//
//   - aggregate per-packet OBU streams until the RTP marker bit
//     signals the end of the temporal unit;
//   - scan for OBU_SEQUENCE_HEADER to (a) flag the unit as a key frame
//     and (b) stash the Sequence Header OBU for the
//     AV1CodecConfigurationRecord emitted at SequenceStart.
//
// Not thread-safe: Push is expected to be called from a single
// Consumer.OnRtp goroutine.
type AV1Depacketizer struct {
	depack codecs.AV1Depacketizer

	frame []byte
	isKey bool

	seqHeaderOBU []byte
	seenKey      bool
}

// Push delivers one RTP packet. Returns (sample, true) on the packet
// carrying the marker bit that terminates a temporal unit.
func (d *AV1Depacketizer) Push(pkt *rtp.Packet) (MediaSample, bool) {
	obus, err := d.depack.Unmarshal(pkt.Payload)
	if err != nil {
		if pkt.Marker {
			return d.flush(pkt.Timestamp)
		}
		return MediaSample{}, false
	}
	d.scanSequenceHeader(obus)
	d.frame = append(d.frame, obus...)
	if !pkt.Marker {
		return MediaSample{}, false
	}
	return d.flush(pkt.Timestamp)
}

func (d *AV1Depacketizer) flush(ts uint32) (MediaSample, bool) {
	if len(d.frame) == 0 {
		return MediaSample{}, false
	}
	data := make([]byte, len(d.frame))
	copy(data, d.frame)
	d.frame = d.frame[:0]
	isKey := d.isKey
	d.isKey = false
	if isKey {
		d.seenKey = true
	}
	return MediaSample{
		Kind:         SampleKindVideo,
		Codec:        CodecAV1,
		RTPTimestamp: ts,
		IsKeyFrame:   isKey,
		Data:         data,
	}, true
}

// scanSequenceHeader walks the low-overhead OBU bitstream produced by
// pion (every OBU has obu_has_size_field=1 in its header and a leb128
// size prefix). Every Sequence Header OBU in a temporal unit marks
// the unit as a key frame; the first one we see is cached verbatim
// for the AV1CodecConfigurationRecord.
func (d *AV1Depacketizer) scanSequenceHeader(buf []byte) {
	i := 0
	for i < len(buf) {
		hdr, err := obu.ParseOBUHeader(buf[i:])
		if err != nil {
			return
		}
		headerSize := hdr.Size()
		if i+headerSize > len(buf) {
			return
		}
		// pion writes low-overhead bitstream (HasSizeField=true).
		// Defensive: if a future codepath injects a raw OBU, bail.
		if !hdr.HasSizeField {
			return
		}
		size, sizeBytes, err := obu.ReadLeb128(buf[i+headerSize:])
		if err != nil {
			return
		}
		obuStart := i + headerSize + int(sizeBytes)
		obuEnd := obuStart + int(size)
		if obuEnd > len(buf) {
			return
		}
		if hdr.Type == obu.OBUSequenceHeader {
			d.isKey = true
			if len(d.seqHeaderOBU) == 0 {
				// Stash the complete OBU (header + size + payload) so
				// the config record can embed it verbatim.
				d.seqHeaderOBU = append([]byte{}, buf[i:obuEnd]...)
			}
		}
		i = obuEnd
	}
}

// HasConfig reports whether we have captured an OBU_SEQUENCE_HEADER,
// which is a prerequisite for emitting the SequenceStart tag.
func (d *AV1Depacketizer) HasConfig() bool {
	return len(d.seqHeaderOBU) > 0
}

// SeenIDR is the H.264-symmetric name for "have we seen a keyframe
// yet?" — exported for Broadcast.Start's gating logic.
func (d *AV1Depacketizer) SeenIDR() bool { return d.seenKey }

// ConfigRecord returns an AV1CodecConfigurationRecord wrapping the
// cached Sequence Header OBU. The per-field metadata (seq_profile,
// seq_level_idx, bit depth, chroma subsampling) is filled with values
// that match a WebRTC svt-av1 / libaom 4:2:0 8-bit stream — enough
// for ffmpeg and libdav1d-based players to decode without re-parsing
// the OBU. Future HDR publishers would need a proper OBU parser to
// fill these in from the real sequence header.
func (d *AV1Depacketizer) ConfigRecord() []byte {
	if len(d.seqHeaderOBU) == 0 {
		return nil
	}
	return buildAV1ConfigRecord(
		d.seqHeaderOBU,
		0,     // seq_profile = 0 (4:2:0 8-bit)
		0,     // seq_level_idx = 0 (2.0)
		0,     // seq_tier = 0 (Main)
		false, // high_bitdepth
		false, // monochrome
		true,  // chroma_subsampling_x = 1 (4:2:0)
		true,  // chroma_subsampling_y = 1 (4:2:0)
	)
}
