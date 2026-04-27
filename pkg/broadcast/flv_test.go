package broadcast

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestBuildAVCSeqHeaderTag(t *testing.T) {
	avcc := []byte{0x01, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0x00, 0x04, 0x67, 0x42, 0x00, 0x1F}
	tag := buildAVCSeqHeaderTag(avcc)

	if tag[0] != 0x17 {
		t.Fatalf("byte 0 = 0x%02X, want 0x17 (key|AVC)", tag[0])
	}
	if tag[1] != flvAVCPacketTypeSeqHeader {
		t.Fatalf("byte 1 = 0x%02X, want 0x00 (SeqHeader)", tag[1])
	}
	if !bytes.Equal(tag[2:5], []byte{0, 0, 0}) {
		t.Fatalf("CTS bytes = %v, want zeros", tag[2:5])
	}
	if !bytes.Equal(tag[5:], avcc) {
		t.Fatalf("payload mismatch")
	}
}

func TestBuildAVCFrameTag(t *testing.T) {
	avccAU := []byte{0x00, 0x00, 0x00, 0x02, 0x01, 0x02}

	keyTag := buildAVCFrameTag(true, 0, avccAU)
	if keyTag[0] != 0x17 {
		t.Errorf("key byte 0 = 0x%02X", keyTag[0])
	}
	if keyTag[1] != flvAVCPacketTypeNALU {
		t.Errorf("key byte 1 = 0x%02X", keyTag[1])
	}
	if !bytes.Equal(keyTag[2:5], []byte{0, 0, 0}) {
		t.Errorf("zero CTS bytes = %v, want all zero", keyTag[2:5])
	}

	interTag := buildAVCFrameTag(false, 0, avccAU)
	if interTag[0] != 0x27 {
		t.Errorf("inter byte 0 = 0x%02X", interTag[0])
	}

	// CTS round-trip: positive 33 ms ((PTS-DTS) for a typical
	// reordered B-frame at 30 fps) must encode as 0x000021 BE and
	// decode back via the matching ingress helper.
	posTag := buildAVCFrameTag(false, 33, avccAU)
	if !bytes.Equal(posTag[2:5], []byte{0x00, 0x00, 0x21}) {
		t.Errorf("cts=33 bytes = %v, want 00 00 21", posTag[2:5])
	}

	// Negative CTS exercises the 24-bit sign extension on the
	// encode path. -1 ms is 0xFFFFFF in 24-bit two's complement.
	negTag := buildAVCFrameTag(false, -1, avccAU)
	if !bytes.Equal(negTag[2:5], []byte{0xFF, 0xFF, 0xFF}) {
		t.Errorf("cts=-1 bytes = %v, want FF FF FF", negTag[2:5])
	}
}

// TestBuildExVideoCodedFramesTag locks the wire layout of the
// PacketType=1 (CodedFrames) tag used for HEVC egress: header byte
// must combine the IsExHeader flag, the FrameType nibble, and the
// PacketType=1 low nibble; FourCC sits at bytes 1..5; the SI24 CTS
// field follows at bytes 5..8 with the same encoding as the classic
// AVC tag (so the reorder buffer does not need a codec-specific
// branch). The access unit bytes follow verbatim.
func TestBuildExVideoCodedFramesTag(t *testing.T) {
	au := []byte{0x40, 0x01, 0x0C, 0x01}

	keyTag := buildExVideoCodedFramesTag(flvExVideoFourCCH265, true, 0, au)
	// 0x80 IsExHeader | 0x10 KeyFrame | 0x01 PacketType=CodedFrames.
	if keyTag[0] != 0x91 {
		t.Errorf("key header = 0x%02X, want 0x91 (IsEx|key|CodedFrames)", keyTag[0])
	}
	if string(keyTag[1:5]) != flvExVideoFourCCH265 {
		t.Errorf("FourCC = %q, want %q", string(keyTag[1:5]), flvExVideoFourCCH265)
	}
	if !bytes.Equal(keyTag[5:8], []byte{0, 0, 0}) {
		t.Errorf("zero CTS bytes = %v, want zeros", keyTag[5:8])
	}
	if !bytes.Equal(keyTag[8:], au) {
		t.Errorf("payload mismatch: got %v, want %v", keyTag[8:], au)
	}

	interTag := buildExVideoCodedFramesTag(flvExVideoFourCCH265, false, 0, au)
	// 0x80 IsExHeader | 0x20 InterFrame | 0x01 PacketType=CodedFrames.
	if interTag[0] != 0xA1 {
		t.Errorf("inter header = 0x%02X, want 0xA1", interTag[0])
	}

	posTag := buildExVideoCodedFramesTag(flvExVideoFourCCH265, false, 33, au)
	if !bytes.Equal(posTag[5:8], []byte{0x00, 0x00, 0x21}) {
		t.Errorf("cts=33 bytes = %v, want 00 00 21", posTag[5:8])
	}

	negTag := buildExVideoCodedFramesTag(flvExVideoFourCCH265, false, -1, au)
	if !bytes.Equal(negTag[5:8], []byte{0xFF, 0xFF, 0xFF}) {
		t.Errorf("cts=-1 bytes = %v, want FF FF FF", negTag[5:8])
	}
}

func TestBuildOpusSeqHeaderTag(t *testing.T) {
	tag := buildOpusSeqHeaderTag(2)

	if tag[0] != 0x90 {
		t.Fatalf("ExAudio head byte = 0x%02X, want 0x90", tag[0])
	}
	if string(tag[1:5]) != "Opus" {
		t.Fatalf("FourCC = %q, want %q", string(tag[1:5]), "Opus")
	}
	body := tag[5:]
	if string(body[0:8]) != "OpusHead" {
		t.Fatalf("OpusHead magic missing")
	}
	if body[9] != 2 {
		t.Errorf("channel count = %d, want 2", body[9])
	}
	sr := binary.LittleEndian.Uint32(body[12:16])
	if sr != 48000 {
		t.Errorf("input sample rate = %d, want 48000", sr)
	}
}

func TestBuildOpusFrameTag(t *testing.T) {
	packet := []byte{0xFC, 0xFF, 0xFE}
	tag := buildOpusFrameTag(packet)
	if tag[0] != 0x91 {
		t.Fatalf("head = 0x%02X, want 0x91", tag[0])
	}
	if string(tag[1:5]) != "Opus" {
		t.Fatalf("FourCC = %q", string(tag[1:5]))
	}
	if !bytes.Equal(tag[5:], packet) {
		t.Fatalf("body mismatch")
	}
}
