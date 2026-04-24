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

	keyTag := buildAVCFrameTag(true, avccAU)
	if keyTag[0] != 0x17 {
		t.Errorf("key byte 0 = 0x%02X", keyTag[0])
	}
	if keyTag[1] != flvAVCPacketTypeNALU {
		t.Errorf("key byte 1 = 0x%02X", keyTag[1])
	}

	interTag := buildAVCFrameTag(false, avccAU)
	if interTag[0] != 0x27 {
		t.Errorf("inter byte 0 = 0x%02X", interTag[0])
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
