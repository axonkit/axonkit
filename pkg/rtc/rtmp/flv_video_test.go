package rtmp

import (
	"encoding/binary"
	"testing"
)

func TestParseClassicAVCSeqHeader(t *testing.T) {
	body := []byte{
		0x17,             // frameType=1 (key), codecID=7 (AVC)
		0x00,             // AVCPacketType seq header
		0x00, 0x00, 0x00, // composition time = 0
		// trailing bytes are the AVCDecoderConfigurationRecord placeholder
		0x01, 0x64, 0x00, 0x1F, 0xFF,
	}
	tag, err := ParseVideoTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tag.PacketType != VideoPacketSeqHeader {
		t.Fatalf("want seq header, got %d", tag.PacketType)
	}
	if !tag.IsKeyFrame {
		t.Fatalf("seq header tagged as key frame")
	}
	if len(tag.Data) != 5 {
		t.Fatalf("payload length = %d, want 5", len(tag.Data))
	}
}

func TestParseClassicAVCNALU(t *testing.T) {
	body := []byte{
		0x27,             // frameType=2 (inter), codecID=7 (AVC)
		0x01,             // AVCPacketType NALU
		0x00, 0x00, 0x00, // composition time = 0
		0x00, 0x00, 0x00, 0x02, 0x65, 0x88, // AVCC NALU
	}
	tag, err := ParseVideoTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tag.PacketType != VideoPacketNALU {
		t.Fatalf("want NALU, got %d", tag.PacketType)
	}
	if tag.IsKeyFrame {
		t.Fatalf("inter tagged as keyframe")
	}
	var visited int
	IterAVCCNALUs(tag.Data, func(n []byte) {
		visited++
		if len(n) != 2 || n[0] != 0x65 {
			t.Fatalf("unexpected NALU %x", n)
		}
	})
	if visited != 1 {
		t.Fatalf("visited %d NALUs", visited)
	}
}

func TestParseNonAVCClassicDropped(t *testing.T) {
	body := []byte{0x12, 0x00, 0, 0, 0} // frameType=1, codecID=2 (Sorenson H.263)
	_, err := ParseVideoTag(body)
	if !IsUnsupportedVideo(err) {
		t.Fatalf("expected UnsupportedVideoError, got %v", err)
	}
}

func TestParseEnhancedVideoAVC_CodedFrames(t *testing.T) {
	// ExRTMP: bit7=1 (ex), bits4-6=FrameType, bits0-3=PacketType.
	// Here PacketType=0 (CodedFrames w/ composition time), FrameType=1 (key).
	head := byte(0x80) | (byte(VideoFrameKey) << 4)
	body := []byte{head, 'a', 'v', 'c', '1', 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x02, 0x65, 0x88,
	}
	tag, err := ParseVideoTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !tag.IsKeyFrame || tag.PacketType != VideoPacketNALU {
		t.Fatalf("bad parse: key=%v pt=%d", tag.IsKeyFrame, tag.PacketType)
	}
	if tag.CompositionTimeMs != 0 {
		t.Fatalf("cts should be 0, got %d", tag.CompositionTimeMs)
	}
	count := IterAVCCNALUs(tag.Data, func(n []byte) {})
	if count != 1 {
		t.Fatalf("nalus = %d", count)
	}
}

func TestParseEnhancedVideoAVC_CodedFramesX(t *testing.T) {
	// PacketType=2 (CodedFramesX, no composition time), FrameType=2 (inter).
	head := byte(0x80) | (byte(VideoFrameInter) << 4) | 0x02
	body := []byte{head, 'a', 'v', 'c', '1',
		0x00, 0x00, 0x00, 0x02, 0x65, 0x88,
	}
	tag, err := ParseVideoTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tag.IsKeyFrame || tag.PacketType != VideoPacketNALU {
		t.Fatalf("bad parse: key=%v pt=%d", tag.IsKeyFrame, tag.PacketType)
	}
	count := IterAVCCNALUs(tag.Data, func(n []byte) {})
	if count != 1 {
		t.Fatalf("nalus = %d", count)
	}
}

func TestParseEnhancedVideoAVC_SequenceStart(t *testing.T) {
	head := byte(0x80) | (byte(VideoFrameKey) << 4) | 0x01 // SequenceStart
	body := []byte{head, 'a', 'v', 'c', '1', 0x01, 0x64, 0x00, 0x1F}
	tag, err := ParseVideoTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tag.PacketType != VideoPacketSeqHeader {
		t.Fatalf("want seq header, got %d", tag.PacketType)
	}
}

func TestParseEnhancedVideoUnknownFourCC(t *testing.T) {
	head := byte(0x80) | (byte(VideoFrameKey) << 4)
	body := []byte{head, 'h', 'v', 'c', '1', 0, 0, 0, 0x00}
	_, err := ParseVideoTag(body)
	if !IsUnsupportedVideo(err) {
		t.Fatalf("expected unsupported-video drop, got %v", err)
	}
}

func TestIterAVCCNALUs_StopsOnTruncation(t *testing.T) {
	nal := []byte{0x65, 0x01, 0x02}
	buf := make([]byte, 4+len(nal)+2)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(nal)))
	copy(buf[4:], nal)
	buf[4+len(nal)] = 0xFF
	buf[4+len(nal)+1] = 0xFF
	// trailing 2 bytes are a truncated next length; iter should emit 1 and stop.
	count := IterAVCCNALUs(buf, func(n []byte) {})
	if count != 1 {
		t.Fatalf("nalus = %d, want 1", count)
	}
}
