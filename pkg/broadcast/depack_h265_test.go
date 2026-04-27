package broadcast

import (
	"encoding/binary"
	"testing"

	"github.com/pion/rtp"
)

// hevcNAL builds a synthetic H.265 NAL unit:
//
//	byte 0: forbidden_zero_bit(1)=0 | nal_unit_type(6) | nuh_layer_id_msb(1)=0
//	byte 1: nuh_layer_id_lsb(5)=0 | nuh_temporal_id_plus1(3)=1
//	body  : caller-supplied
func hevcNAL(nalType byte, body ...byte) []byte {
	out := make([]byte, 2+len(body))
	out[0] = (nalType & 0x3F) << 1
	out[1] = 0x01
	copy(out[2:], body)
	return out
}

// hevcSPSWithPTL builds a tiny SPS NAL with a fixed profile_tier_level
// block. SPS body layout (after NAL header):
//
//	byte 0: sps_video_parameter_set_id(4)=0 | sps_max_sub_layers_minus1(3)=0 | sps_temporal_id_nesting_flag(1)=0
//	bytes 1..12: profile_tier_level (12 bytes)
//
// We pick general_profile_idc=1 (Main), tier=0, level_idc=120 (4.0)
// so the resulting hvcC has recognizable profile/level values.
func hevcSPSWithPTL() []byte {
	body := []byte{
		// sps_video_parameter_set_id(4)=0 | sps_max_sub_layers_minus1(3)=0 | sps_temporal_id_nesting_flag(1)=0
		0x00,
		// profile_space(2)=0 | tier_flag(1)=0 | profile_idc(5)=1 (Main)
		0x21,
		// profile_compatibility_flags (4 bytes)
		0x60, 0x00, 0x00, 0x00,
		// general_constraint_indicator_flags (6 bytes)
		0x90, 0x00, 0x00, 0x00, 0x00, 0x00,
		// general_level_idc = 120 (level 4.0)
		120,
	}
	return hevcNAL(33, body...)
}

// TestH265DepacketizerCapturesVPSSPSPPSAndKey feeds an RTP burst of
// VPS / SPS / PPS / IDR with the marker on the IDR. Verifies the
// access unit is reported as a key frame, ConfigRecord() builds an
// hvcC whose configurationVersion == 1 and embeds the supplied
// VPS/SPS/PPS in NAL arrays.
func TestH265DepacketizerCapturesVPSSPSPPSAndKey(t *testing.T) {
	var d H265Depacketizer

	vps := hevcNAL(32, 0xAA, 0xBB)
	sps := hevcSPSWithPTL()
	pps := hevcNAL(34, 0xC0, 0xDE)
	idr := hevcNAL(19, 0xDE, 0xAD, 0xBE, 0xEF)

	for i, nal := range [][]byte{vps, sps, pps, idr} {
		marker := i == 3
		pkt := &rtp.Packet{
			Header:  rtp.Header{Marker: marker, Timestamp: 90000},
			Payload: nal,
		}
		_, _ = d.Push(pkt)
	}

	if !d.HasConfig() {
		t.Fatalf("HasConfig should be true after VPS+SPS+PPS")
	}
	if !d.SeenIDR() {
		t.Fatalf("SeenIDR should be true after IDR")
	}
	rec := d.ConfigRecord()
	if len(rec) == 0 {
		t.Fatalf("ConfigRecord should be non-empty")
	}
	if rec[0] != 0x01 {
		t.Errorf("hvcC version = 0x%02X, want 0x01", rec[0])
	}
	// general_profile_space(2)=0 | general_tier_flag(1)=0 | general_profile_idc(5)=1 → 0x21
	if rec[1] != 0x21 {
		t.Errorf("hvcC profile_idc byte = 0x%02X, want 0x21", rec[1])
	}
	// general_level_idc lives at offset 1+1+4+6 = 12 in our hvcC.
	if rec[12] != 120 {
		t.Errorf("hvcC level_idc = %d, want 120", rec[12])
	}
}

// TestH265DepacketizerEmitsKeyOnlyForIRAP feeds a non-IRAP TRAIL_R
// frame and verifies IsKeyFrame stays false even when the access
// unit completes successfully.
func TestH265DepacketizerEmitsKeyOnlyForIRAP(t *testing.T) {
	var d H265Depacketizer

	trail := hevcNAL(hevcNALUTypeTrailR, 0x11, 0x22, 0x33)
	pkt := &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 30000},
		Payload: trail,
	}

	s, ok := d.Push(pkt)
	if !ok {
		t.Fatalf("expected sample for TRAIL_R")
	}
	if s.IsKeyFrame {
		t.Errorf("TRAIL_R must not be flagged as key")
	}
}

// TestBuildHEVCConfigRecordEncodesNALArrays builds an hvcC from
// hand-rolled bytes and asserts the array section parses back to
// 3 NAL types (VPS / SPS / PPS) with the original lengths.
func TestBuildHEVCConfigRecordEncodesNALArrays(t *testing.T) {
	vps := hevcNAL(32, 0xAA, 0xBB)
	sps := hevcSPSWithPTL()
	pps := hevcNAL(34, 0xC0, 0xDE)

	rec := buildHEVCConfigRecord(vps, sps, pps)
	if len(rec) == 0 {
		t.Fatalf("hvcC build returned empty bytes")
	}

	// hvcC fixed header: configurationVersion(1) + ptl(12) +
	// minSpatialSeg(2) + parallelismType(1) + chromaFormat(1) +
	// bitDepthLuma(1) + bitDepthChroma(1) + avgFrameRate(2) +
	// CFR/temporal/length(1) + numOfArrays(1) = 23 bytes.
	const nalArrayStart = 23
	if int(rec[nalArrayStart-1]) != 3 {
		t.Fatalf("numOfArrays at offset %d = %d, want 3",
			nalArrayStart-1, rec[nalArrayStart-1])
	}

	pos := nalArrayStart
	wantTypes := []byte{32, 33, 34}
	wantBodies := [][]byte{vps, sps, pps}
	for i, want := range wantTypes {
		if pos+3 > len(rec) {
			t.Fatalf("ran off hvcC at array %d", i)
		}
		if got := rec[pos] & 0x3F; got != want {
			t.Errorf("array[%d] type = %d, want %d", i, got, want)
		}
		numNalus := binary.BigEndian.Uint16(rec[pos+1:])
		if numNalus != 1 {
			t.Errorf("array[%d] numNalus = %d, want 1", i, numNalus)
		}
		pos += 3
		nalLen := int(binary.BigEndian.Uint16(rec[pos:]))
		pos += 2
		if pos+nalLen > len(rec) {
			t.Fatalf("array[%d] nalLen overflow", i)
		}
		if string(rec[pos:pos+nalLen]) != string(wantBodies[i]) {
			t.Errorf("array[%d] body mismatch", i)
		}
		pos += nalLen
	}
	if pos != len(rec) {
		t.Errorf("extra trailing bytes in hvcC: pos=%d total=%d", pos, len(rec))
	}
}
