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

// exHead builds an ExVideoTagHeader first byte: bit7=1 marks the tag as
// Enhanced RTMP, bits4-6 carry FrameType, bits0-3 carry PacketType.
func exHead(frameType VideoFrameType, packetType uint8) byte {
	return 0x80 | byte(frameType<<4) | (packetType & 0x0F)
}

func TestParseEnhancedVideoAVC_SequenceStart(t *testing.T) {
	// PacketType=0 (SequenceStart), FrameType=1 (key). The payload is a
	// stand-in AVCDecoderConfigurationRecord; the parser must not consume
	// any CTS bytes here.
	body := []byte{exHead(VideoFrameKey, 0), 'a', 'v', 'c', '1', 0x01, 0x64, 0x00, 0x1F}
	tag, err := ParseVideoTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tag.PacketType != VideoPacketSeqHeader {
		t.Fatalf("want seq header, got %d", tag.PacketType)
	}
	if tag.Codec != VideoCodecH264 {
		t.Fatalf("want codec=h264, got %s", tag.Codec)
	}
	if len(tag.Data) != 4 || tag.Data[0] != 0x01 {
		t.Fatalf("unexpected seq-header payload: % x", tag.Data)
	}
}

func TestParseEnhancedVideoAVC_CodedFrames(t *testing.T) {
	// PacketType=1 (CodedFrames, +CTS). Spec says 3 bytes of signed CTS
	// follow the FourCC. The remainder is the AVCC payload.
	body := []byte{
		exHead(VideoFrameKey, 1),
		'a', 'v', 'c', '1',
		0x00, 0x00, 0x21, // CTS = 33 ms
		0x00, 0x00, 0x00, 0x02, 0x65, 0x88, // 1 NALU: [len=2][0x65 0x88]
	}
	tag, err := ParseVideoTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !tag.IsKeyFrame || tag.PacketType != VideoPacketNALU {
		t.Fatalf("bad parse: key=%v pt=%d", tag.IsKeyFrame, tag.PacketType)
	}
	if tag.Codec != VideoCodecH264 {
		t.Fatalf("want codec=h264, got %s", tag.Codec)
	}
	if tag.CompositionTimeMs != 33 {
		t.Fatalf("cts should be 33, got %d", tag.CompositionTimeMs)
	}
	count := IterAVCCNALUs(tag.Data, func(n []byte) {
		if len(n) != 2 || n[0] != 0x65 {
			t.Errorf("unexpected NALU % x", n)
		}
	})
	if count != 1 {
		t.Fatalf("nalus = %d", count)
	}
}

func TestParseEnhancedVideoAVC_CodedFramesX(t *testing.T) {
	// PacketType=3 (CodedFramesX, no CTS). This is a regression guard
	// that the parser must not read 3 bytes of CTS out of the payload —
	// the whole post-FourCC body is the AVCC frame data.
	body := []byte{
		exHead(VideoFrameInter, 3),
		'a', 'v', 'c', '1',
		0x00, 0x00, 0x00, 0x02, 0x65, 0x88,
	}
	tag, err := ParseVideoTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tag.IsKeyFrame || tag.PacketType != VideoPacketNALU {
		t.Fatalf("bad parse: key=%v pt=%d", tag.IsKeyFrame, tag.PacketType)
	}
	if tag.CompositionTimeMs != 0 {
		t.Fatalf("cts should be 0 (no CTS field), got %d", tag.CompositionTimeMs)
	}
	if len(tag.Data) != 6 {
		t.Fatalf("payload len = %d, want 6 (parser must not consume CTS bytes)", len(tag.Data))
	}
	count := IterAVCCNALUs(tag.Data, func(n []byte) {})
	if count != 1 {
		t.Fatalf("nalus = %d", count)
	}
}

func TestParseEnhancedVideoAVC_SequenceEnd(t *testing.T) {
	// PacketType=2 (SequenceEnd): empty body after FourCC.
	body := []byte{exHead(VideoFrameKey, 2), 'a', 'v', 'c', '1'}
	tag, err := ParseVideoTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tag.PacketType != VideoPacketEndOfSeq {
		t.Fatalf("want end-of-seq, got %d", tag.PacketType)
	}
	if len(tag.Data) != 0 {
		t.Fatalf("end-of-seq should have empty data, got % x", tag.Data)
	}
}

func TestParseEnhancedVideoVP9_SequenceStart(t *testing.T) {
	// vpcC is 8 fixed bytes (profile, level, bit_depth/chroma/range,
	// color_primaries, transfer, matrix, codecInitializationDataSize).
	// The parser does not decode these — it just surfaces the whole
	// post-FourCC payload as the config record.
	payload := []byte{
		0x00, 0x1F,
		0x80,
		0x01,
		0x01,
		0x01,
		0x00, 0x00,
	}
	body := append([]byte{exHead(VideoFrameKey, 0), 'v', 'p', '0', '9'}, payload...)
	tag, err := ParseVideoTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tag.Codec != VideoCodecVP9 {
		t.Fatalf("want codec=vp9, got %s", tag.Codec)
	}
	if tag.PacketType != VideoPacketSeqHeader {
		t.Fatalf("want seq header, got %d", tag.PacketType)
	}
	if len(tag.Data) != len(payload) {
		t.Fatalf("vpcC payload len = %d, want %d", len(tag.Data), len(payload))
	}
}

func TestParseEnhancedVideoAV1_CodedFramesX(t *testing.T) {
	// Verify AV1 frames go through CodedFramesX (the common case: no CTS
	// for low-latency encoders). The payload is an opaque OBU stream.
	payload := []byte{0x0a, 0x0b, 0x0c, 0x0d}
	body := append([]byte{exHead(VideoFrameKey, 3), 'a', 'v', '0', '1'}, payload...)
	tag, err := ParseVideoTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tag.Codec != VideoCodecAV1 {
		t.Fatalf("want codec=av1, got %s", tag.Codec)
	}
	if tag.PacketType != VideoPacketNALU {
		t.Fatalf("want NALU, got %d", tag.PacketType)
	}
	if len(tag.Data) != len(payload) {
		t.Fatalf("payload len = %d, want %d", len(tag.Data), len(payload))
	}
}

func TestParseEnhancedVideoAV1_CodedFramesNoCTS(t *testing.T) {
	// Regression guard: Enhanced RTMP v2 §ExVideoTagBody says AV1
	// (and VP9) do NOT carry a 3-byte CTS on PacketType=CodedFrames,
	// because they have no B-frames. Reading CTS unconditionally used
	// to strip the first 3 bytes of the OBU stream and corrupted every
	// frame pushed by ffmpeg. The real-world body that triggered this
	// looked like `a1 61 76 30 31 1a 01 f8`, where `1a 01 f8` is a
	// legal 3-byte FRAME_HEADER OBU — not a CTS field.
	body := []byte{exHead(VideoFrameInter, 1), 'a', 'v', '0', '1', 0x1a, 0x01, 0xf8}
	tag, err := ParseVideoTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tag.Codec != VideoCodecAV1 {
		t.Fatalf("want codec=av1, got %s", tag.Codec)
	}
	if tag.PacketType != VideoPacketNALU {
		t.Fatalf("want NALU, got %d", tag.PacketType)
	}
	if tag.CompositionTimeMs != 0 {
		t.Fatalf("cts should be 0 for AV1 CodedFrames, got %d", tag.CompositionTimeMs)
	}
	want := []byte{0x1a, 0x01, 0xf8}
	if len(tag.Data) != len(want) || tag.Data[0] != want[0] || tag.Data[1] != want[1] || tag.Data[2] != want[2] {
		t.Fatalf("payload = % x, want % x (parser must not consume CTS for AV1)", tag.Data, want)
	}
}

func TestParseEnhancedVideoVP9_CodedFramesNoCTS(t *testing.T) {
	// Same regression as AV1: VP9 has no B-frames, so Enhanced RTMP
	// omits the CTS field on PacketType=CodedFrames. Reading 3 bytes
	// unconditionally would corrupt the first bytes of every VP9
	// frame header.
	payload := []byte{0x82, 0x49, 0x83, 0x42, 0x00}
	body := append([]byte{exHead(VideoFrameInter, 1), 'v', 'p', '0', '9'}, payload...)
	tag, err := ParseVideoTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tag.Codec != VideoCodecVP9 {
		t.Fatalf("want codec=vp9, got %s", tag.Codec)
	}
	if tag.PacketType != VideoPacketNALU {
		t.Fatalf("want NALU, got %d", tag.PacketType)
	}
	if tag.CompositionTimeMs != 0 {
		t.Fatalf("cts should be 0 for VP9 CodedFrames, got %d", tag.CompositionTimeMs)
	}
	if len(tag.Data) != len(payload) {
		t.Fatalf("payload len = %d, want %d (parser must not consume CTS for VP9)", len(tag.Data), len(payload))
	}
}

func TestParseEnhancedVideoUnknownFourCC(t *testing.T) {
	// "hvc1" (HEVC) is outside v1 scope; must drop softly so the handler
	// keeps the session alive.
	body := []byte{exHead(VideoFrameKey, 0), 'h', 'v', 'c', '1', 0, 0, 0, 0x00}
	_, err := ParseVideoTag(body)
	if !IsUnsupportedVideo(err) {
		t.Fatalf("expected unsupported-video drop, got %v", err)
	}
}

func TestParseEnhancedVideoDropsMetadata(t *testing.T) {
	// PacketType=4 (Metadata) is supposed to be dropped softly so the
	// handler does not tear down the session just because the publisher
	// sent an in-band metadata frame we cannot consume.
	body := []byte{exHead(VideoFrameInter, 4), 'a', 'v', 'c', '1', 0x00}
	_, err := ParseVideoTag(body)
	if !IsUnsupportedVideo(err) {
		t.Fatalf("expected metadata drop, got %v", err)
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
