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
		Parameters: mediasoup.RtpCodecSpecificParameters{
			XGoogleStartBitrate: 1000,
		},
	},
	{
		Kind:      mediasoup.MediaKindVideo,
		MimeType:  "video/VP9",
		ClockRate: 90000,
		Parameters: mediasoup.RtpCodecSpecificParameters{
			ProfileId:           2,
			XGoogleStartBitrate: 1000,
		},
	},
	{
		Kind:      mediasoup.MediaKindVideo,
		MimeType:  "video/AV1",
		ClockRate: 90000,
		Parameters: mediasoup.RtpCodecSpecificParameters{
			XGoogleStartBitrate: 1000,
		},
	},
	{
		Kind:      mediasoup.MediaKindVideo,
		MimeType:  "video/H264",
		ClockRate: 90000,
		Parameters: mediasoup.RtpCodecSpecificParameters{
			PacketizationMode:     1,
			ProfileLevelId:        "42e01f",
			LevelAsymmetryAllowed: 1,
			XGoogleStartBitrate:   1000,
		},
	},
	{
		Kind:      mediasoup.MediaKindVideo,
		MimeType:  "video/H264",
		ClockRate: 90000,
		Parameters: mediasoup.RtpCodecSpecificParameters{
			PacketizationMode:     1,
			ProfileLevelId:        "4d0032",
			LevelAsymmetryAllowed: 1,
			XGoogleStartBitrate:   1000,
		},
	},
}
