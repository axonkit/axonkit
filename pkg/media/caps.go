package media

import "github.com/jiyeyuran/mediasoup-go/v2"

var rtpCodecCapabilities = []*mediasoup.RtpCodecCapability{
	{
		Kind:      mediasoup.MediaKindAudio,
		MimeType:  "audio/opus",
		ClockRate: 48000,
		Channels:  2,
	},
	{
		Kind:      mediasoup.MediaKindVideo,
		MimeType:  "video/VP8",
		ClockRate: 90000,
	},
	{
		Kind:      mediasoup.MediaKindVideo,
		MimeType:  "video/H264",
		ClockRate: 90000,
		Parameters: mediasoup.RtpCodecSpecificParameters{
			LevelAsymmetryAllowed: 1,
			PacketizationMode:     1,
			ProfileLevelId:        "4d0032",
		},
	},
}

var consumerDeviceCapabilities = &mediasoup.RtpCapabilities{
	Codecs: []*mediasoup.RtpCodecCapability{
		{
			MimeType:             "audio/opus",
			Kind:                 "audio",
			PreferredPayloadType: 100,
			ClockRate:            48000,
			Channels:             2,
			RtcpFeedback: []*mediasoup.RtcpFeedback{
				{Type: "nack"},
			},
		},
		{
			MimeType:             "video/H264",
			Kind:                 "video",
			PreferredPayloadType: 101,
			ClockRate:            90000,
			Parameters: mediasoup.RtpCodecSpecificParameters{
				LevelAsymmetryAllowed: 1,
				PacketizationMode:     1,
				ProfileLevelId:        "4d0032",
			},
			RtcpFeedback: []*mediasoup.RtcpFeedback{
				{Type: "nack", Parameter: ""},
				{Type: "nack", Parameter: "pli"},
				{Type: "ccm", Parameter: "fir"},
				{Type: "goog-remb", Parameter: ""},
			},
		},
		{
			MimeType:             "video/rtx",
			Kind:                 "video",
			PreferredPayloadType: 102,
			ClockRate:            90000,
			Parameters: mediasoup.RtpCodecSpecificParameters{
				Apt: 101,
			},
		},
	},
	HeaderExtensions: []*mediasoup.RtpHeaderExtension{
		{
			Kind:             "audio",
			Uri:              "urn:ietf:params:rtp-hdrext:sdes:mid",
			PreferredId:      1,
			PreferredEncrypt: false,
		},
		{
			Kind:             "video",
			Uri:              "urn:ietf:params:rtp-hdrext:sdes:mid",
			PreferredId:      1,
			PreferredEncrypt: false,
		},
		{
			Kind:             "video",
			Uri:              "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id",
			PreferredId:      2,
			PreferredEncrypt: false,
		},
		{
			Kind:             "audio",
			Uri:              "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time", // eslint-disable-line max-len
			PreferredId:      4,
			PreferredEncrypt: false,
		},
		{
			Kind:             "video",
			Uri:              "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time", // eslint-disable-line max-len
			PreferredId:      4,
			PreferredEncrypt: false,
		},
		{
			Kind:             "audio",
			Uri:              "urn:ietf:params:rtp-hdrext:ssrc-audio-level",
			PreferredId:      6,
			PreferredEncrypt: false,
		},
		{
			Kind:             "video",
			Uri:              "urn:3gpp:video-orientation",
			PreferredId:      8,
			PreferredEncrypt: false,
		},
		{
			Kind:             "video",
			Uri:              "urn:ietf:params:rtp-hdrext:toffset",
			PreferredId:      9,
			PreferredEncrypt: false,
		},
	},
}
