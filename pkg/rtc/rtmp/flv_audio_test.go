package rtmp

import "testing"

func TestParseClassicOpusTag(t *testing.T) {
	// soundFormat=13 (Opus), stereo bit set, payload = 0xAA 0xBB.
	body := []byte{(13 << 4) | 0x01, 0xAA, 0xBB}
	tag, err := ParseAudioTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tag.Codec != AudioCodecOpus {
		t.Fatalf("codec = %v", tag.Codec)
	}
	if tag.ChannelCount != 2 {
		t.Fatalf("channels = %d", tag.ChannelCount)
	}
	if tag.PacketType != AudioPacketOpusFrame {
		t.Fatalf("pkt = %d", tag.PacketType)
	}
	if len(tag.Data) != 2 || tag.Data[0] != 0xAA {
		t.Fatalf("payload = %x", tag.Data)
	}
}

func TestParseClassicAACDropped(t *testing.T) {
	body := []byte{(10 << 4) | 0x01, 0x00, 0x01, 0x02}
	_, err := ParseAudioTag(body)
	if !IsUnsupportedAudio(err) {
		t.Fatalf("expected unsupported-audio drop, got %v", err)
	}
}

func TestParseEnhancedOpusFrame(t *testing.T) {
	// head: soundFormat=9 (ExAudio marker) in high nibble, pktType=1 in low.
	body := []byte{(9 << 4) | 0x01, 'O', 'p', 'u', 's', 0xAA, 0xBB}
	tag, err := ParseAudioTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tag.PacketType != AudioPacketOpusFrame {
		t.Fatalf("pkt = %d", tag.PacketType)
	}
	if len(tag.Data) != 2 {
		t.Fatalf("payload = %x", tag.Data)
	}
}

func TestParseEnhancedOpusSequenceStart(t *testing.T) {
	head := []byte("OpusHead\x01\x02\x00\x00\x80\xBB\x00\x00\x00\x00\x00")
	// soundFormat=9 (hi-nibble) marks this as the E-RTMP ExAudioHeader with
	// FourCC "Opus"; low-nibble is zero for seq-header packets.
	body := append([]byte{9 << 4, 'O', 'p', 'u', 's'}, head...)
	tag, err := ParseAudioTag(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if tag.PacketType != AudioPacketOpusHeader {
		t.Fatalf("pkt = %d", tag.PacketType)
	}
	oh, err := ParseOpusHead(tag.Data)
	if err != nil {
		t.Fatalf("opus head: %v", err)
	}
	if oh.ChannelCount != 2 {
		t.Fatalf("channels = %d", oh.ChannelCount)
	}
	if oh.InputSampleRate != 48000 {
		t.Fatalf("rate = %d", oh.InputSampleRate)
	}
}

func TestParseEnhancedUnknownFourCC(t *testing.T) {
	body := []byte{(9 << 4) | 0x01, 'A', 'A', 'C', ' ', 0xAA}
	_, err := ParseAudioTag(body)
	if !IsUnsupportedAudio(err) {
		t.Fatalf("expected unsupported-audio drop, got %v", err)
	}
}
