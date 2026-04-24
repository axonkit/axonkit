package rtmp

import (
	"fmt"
)

// AudioCodec classifies the encoded payload in an FLV audio tag. axonkit
// v1 only ingests Opus; everything else (AAC, MP3, G.711…) is turned into
// an UnsupportedAudioError that the RTMP handler treats as a soft drop.
type AudioCodec uint8

const (
	AudioCodecUnknown AudioCodec = iota
	AudioCodecOpus
)

// AudioPacketType categorizes the Opus payload we emit.
type AudioPacketType uint8

const (
	// AudioPacketOpusHeader carries an OpusHead (Ogg-encapsulated Opus
	// identification header) when the publisher negotiated the Enhanced
	// RTMP "Opus" FourCC. Classic FLV `soundFormat=13` never signals a
	// header — receivers derive channels/rate from FLV metadata instead.
	AudioPacketOpusHeader AudioPacketType = 0
	// AudioPacketOpusFrame carries one Opus frame (always the steady state
	// after any optional header).
	AudioPacketOpusFrame AudioPacketType = 1
)

// AudioTag is the decoded form of an FLV AUDIODATA tag.
type AudioTag struct {
	Codec      AudioCodec
	PacketType AudioPacketType
	// ChannelCount is 1 or 2 (derived from FLV soundType bit or from the
	// Enhanced RTMP per-tag channel field). Downstream Opus RTP packing
	// does not actually care about this value — it is only consumed by the
	// session when computing RtpCodecParameters.Channels.
	ChannelCount uint8
	// SampleRateHz is the Opus sample rate as signaled by the publisher
	// (48000 in all practical FLV setups). axonkit rewrites to 48000 for
	// RTP timestamps regardless: Opus is always 48 kHz clock in RTP.
	SampleRateHz uint32
	Data         []byte
}

// UnsupportedAudioError signals audio that axonkit cannot ingest (AAC,
// MP3, etc.). Callers should drop the tag and continue the session rather
// than terminating the RTMP connection.
type UnsupportedAudioError struct {
	Reason string
}

func (e *UnsupportedAudioError) Error() string { return "unsupported audio tag: " + e.Reason }

// IsUnsupportedAudio reports whether err is a soft-drop sentinel.
func IsUnsupportedAudio(err error) bool {
	_, ok := err.(*UnsupportedAudioError)
	return ok
}

// ParseAudioTag decodes an FLV AUDIODATA body. It supports two dialects:
//
//  1. Enhanced RTMP (official, Adobe/YouTube 2024 spec): the first byte
//     high nibble is 9 (the ExAudioHeader marker). The low nibble is the
//     AudioPacketType. The 4 bytes that follow are a FourCC; FourCC="Opus"
//     selects our Opus ingest. For PacketType=SequenceStart (0) we emit
//     AudioPacketOpusHeader; for PacketType=CodedFrames (1) we emit
//     AudioPacketOpusFrame.
//
//  2. Classic FLV (non-standard YouTube extension): byte 0 encodes
//     soundFormat in bits 4-7. soundFormat==13 means "Opus" (the only
//     value we accept). The remaining bits describe sample rate/size/type.
//     The Opus frame follows immediately.
//
// Any other combination returns an *UnsupportedAudioError so the ingest
// handler can soft-drop the tag and keep the RTMP session alive.
func ParseAudioTag(body []byte) (*AudioTag, error) {
	if len(body) < 1 {
		return nil, fmt.Errorf("audio tag too short: %d bytes", len(body))
	}
	soundFormat := (body[0] >> 4) & 0x0F
	// soundFormat==9 is the Enhanced RTMP ExAudioHeader marker. It is
	// reserved in classic FLV so there is no legacy collision.
	if soundFormat == 9 {
		return parseEnhancedAudioTag(body)
	}
	return parseClassicAudioTag(body)
}

func parseClassicAudioTag(body []byte) (*AudioTag, error) {
	head := body[0]
	soundFormat := (head >> 4) & 0x0F
	// 13 == Opus (YouTube/RTMP extension). All other values are codecs we
	// intentionally drop (notably 10=AAC).
	const soundFormatOpus = 13
	if soundFormat != soundFormatOpus {
		return nil, &UnsupportedAudioError{Reason: fmt.Sprintf("classic soundFormat %d", soundFormat)}
	}
	// soundType bit 0: 0=mono, 1=stereo. sampleRate/sampleSize bits are
	// ignored because Opus is always 48 kHz / 16-bit in RTP.
	channels := uint8(1)
	if head&0x01 == 1 {
		channels = 2
	}
	if len(body) < 2 {
		return nil, fmt.Errorf("classic opus tag missing payload")
	}
	return &AudioTag{
		Codec:        AudioCodecOpus,
		PacketType:   AudioPacketOpusFrame,
		ChannelCount: channels,
		SampleRateHz: 48000,
		Data:         body[1:],
	}, nil
}

// ExAudioFourCC* are the Enhanced RTMP 4-byte audio codec identifiers we
// accept. Only Opus is in scope for v1.
const (
	ExAudioFourCCOpus = "Opus"
)

func parseEnhancedAudioTag(body []byte) (*AudioTag, error) {
	if len(body) < 5 {
		return nil, fmt.Errorf("ex-audio tag too short: %d bytes", len(body))
	}
	head := body[0]
	rawPacketType := head & 0x0F
	fourCC := string(body[1:5])
	if fourCC != ExAudioFourCCOpus {
		return nil, &UnsupportedAudioError{Reason: "ex-audio FourCC " + fourCC}
	}

	var pktType AudioPacketType
	switch rawPacketType {
	case 0: // SequenceStart → codec header (OpusHead)
		pktType = AudioPacketOpusHeader
	case 1: // CodedFrames
		pktType = AudioPacketOpusFrame
	case 2: // SequenceEnd — handled as soft drop; no producer state change.
		return nil, &UnsupportedAudioError{Reason: "ex-audio SequenceEnd"}
	case 3: // MultichannelConfig — ignored (Opus handles channels itself)
		return nil, &UnsupportedAudioError{Reason: "ex-audio MultichannelConfig"}
	case 4: // Multitrack — out of scope for v1
		return nil, &UnsupportedAudioError{Reason: "ex-audio Multitrack"}
	case 5: // ModEx — out of scope for v1
		return nil, &UnsupportedAudioError{Reason: "ex-audio ModEx"}
	default:
		return nil, &UnsupportedAudioError{Reason: fmt.Sprintf("ex-audio packet type %d", rawPacketType)}
	}

	return &AudioTag{
		Codec:        AudioCodecOpus,
		PacketType:   pktType,
		ChannelCount: 0, // caller reads from OpusHead when pktType==OpusHeader
		SampleRateHz: 48000,
		Data:         body[5:],
	}, nil
}

// OpusHead is the decoded form of the Ogg/Opus identification header that
// the Enhanced RTMP "Opus" FourCC delivers as SequenceStart. We decode
// only the fields that matter for mediasoup RtpCodecParameters: the
// pre-skip is useful for diagnostics but not required.
//
// Layout (RFC 7845 §5.1):
//
//	magic[8]        "OpusHead"
//	version         1 byte (0x01)
//	channelCount    1 byte
//	preSkip         2 bytes little-endian
//	inputSampleRate 4 bytes little-endian
//	outputGain      2 bytes little-endian
//	mappingFamily   1 byte
type OpusHead struct {
	ChannelCount    uint8
	InputSampleRate uint32
	PreSkip         uint16
	OutputGain      int16
	MappingFamily   uint8
}

// ParseOpusHead decodes the fixed-size Opus identification header. The
// optional channel mapping table (families >= 1) is not surfaced because
// axonkit only forwards; mediasoup does not need it.
func ParseOpusHead(data []byte) (*OpusHead, error) {
	if len(data) < 19 {
		return nil, fmt.Errorf("opus head too short: %d bytes", len(data))
	}
	if string(data[0:8]) != "OpusHead" {
		return nil, fmt.Errorf("opus head magic mismatch")
	}
	return &OpusHead{
		ChannelCount:    data[9],
		PreSkip:         uint16(data[10]) | uint16(data[11])<<8,
		InputSampleRate: uint32(data[12]) | uint32(data[13])<<8 | uint32(data[14])<<16 | uint32(data[15])<<24,
		OutputGain:      int16(uint16(data[16]) | uint16(data[17])<<8), //nolint:gosec // two's complement
		MappingFamily:   data[18],
	}, nil
}
