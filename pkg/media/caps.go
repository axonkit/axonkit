package media

import "github.com/jiyeyuran/mediasoup-go/v2"

// rtpCodecCapabilities is what the Router announces to the rest of the stack.
// mediasoup-go merges these against its internal `supportedRtpCapabilities`
// table (see mediasoup-go/supported_rtp_capabilities.go) and inherits the
// standard RtcpFeedback (nack/pli/fir/goog-remb/transport-cc for video, nack/
// transport-cc for opus), so we only declare the codec identity + a few
// negotiation knobs here.
//
// Guiding rules:
//   - List every codec/profile a modern Chromium/Firefox/Safari offer may
//     prefer, otherwise the WHIP parser drops those PTs (see
//     whip/parse.go::matchCapability). Narrow caps = fewer publish failures
//     reported to users as "no negotiable codecs".
//   - Do NOT list codecs the worker cannot actually forward (H.265 is absent
//     from mediasoup-go's supported table — advertising it causes validate
//     errors at Router creation time).
//   - Prefer packetization-mode=1 for H.264. Single-NAL (mode=0) is only
//     needed by legacy SIP/telepresence gear; browsers always offer both but
//     are happy to pick mode=1 when we also offer it.
//   - For VP9/H.264 each profile is a *separate* codec from mediasoup's
//     perspective; listing multiple entries for the same mimeType is the
//     idiomatic way to advertise multi-profile support.
var rtpCodecCapabilities = []*mediasoup.RtpCodecCapability{
	// ---- Audio --------------------------------------------------------------
	{
		Kind:      mediasoup.MediaKindAudio,
		MimeType:  "audio/opus",
		ClockRate: 48000,
		Channels:  2,
	},
	// G.722 / PCMU / PCMA: telephony interop (WHIP from SIP gateways, or
	// WebRTC clients falling back when opus isn't available). Harmless to
	// advertise; browsers only pick them if nothing else fits.
	{
		Kind:                 mediasoup.MediaKindAudio,
		MimeType:             "audio/G722",
		PreferredPayloadType: 9,
		ClockRate:            8000,
	},
	{
		Kind:                 mediasoup.MediaKindAudio,
		MimeType:             "audio/PCMU",
		PreferredPayloadType: 0,
		ClockRate:            8000,
	},
	{
		Kind:                 mediasoup.MediaKindAudio,
		MimeType:             "audio/PCMA",
		PreferredPayloadType: 8,
		ClockRate:            8000,
	},

	// ---- Video --------------------------------------------------------------
	{
		Kind:      mediasoup.MediaKindVideo,
		MimeType:  "video/VP8",
		ClockRate: 90000,
		Parameters: mediasoup.RtpCodecSpecificParameters{
			XGoogleStartBitrate: 1000,
		},
	},
	// VP9 profile-id=0: 8-bit 4:2:0, Chrome's default VP9 offer.
	{
		Kind:      mediasoup.MediaKindVideo,
		MimeType:  "video/VP9",
		ClockRate: 90000,
		Parameters: mediasoup.RtpCodecSpecificParameters{
			ProfileId:           0,
			XGoogleStartBitrate: 1000,
		},
	},
	// VP9 profile-id=2: 10/12-bit 4:2:0, used by HDR pipelines and some
	// screenshare content.
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
	// H.264 Constrained Baseline (most compatible; required by WebRTC spec).
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
	// H.264 Baseline (cs1=0). Chromium still emits this PT; without it the
	// parser drops Chrome's 42001f PTs.
	{
		Kind:      mediasoup.MediaKindVideo,
		MimeType:  "video/H264",
		ClockRate: 90000,
		Parameters: mediasoup.RtpCodecSpecificParameters{
			PacketizationMode:     1,
			ProfileLevelId:        "42001f",
			LevelAsymmetryAllowed: 1,
			XGoogleStartBitrate:   1000,
		},
	},
	// H.264 Main @ L3.1 (common from software encoders incl. x264 default).
	{
		Kind:      mediasoup.MediaKindVideo,
		MimeType:  "video/H264",
		ClockRate: 90000,
		Parameters: mediasoup.RtpCodecSpecificParameters{
			PacketizationMode:     1,
			ProfileLevelId:        "4d001f",
			LevelAsymmetryAllowed: 1,
			XGoogleStartBitrate:   1000,
		},
	},
	// H.264 Main @ L5.0 (higher-res screen share).
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
	// H.264 High @ L3.1 (better compression; Chrome offers by default).
	{
		Kind:      mediasoup.MediaKindVideo,
		MimeType:  "video/H264",
		ClockRate: 90000,
		Parameters: mediasoup.RtpCodecSpecificParameters{
			PacketizationMode:     1,
			ProfileLevelId:        "64001f",
			LevelAsymmetryAllowed: 1,
			XGoogleStartBitrate:   1000,
		},
	},
}
