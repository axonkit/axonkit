package whip

import (
	"strings"
	"testing"

	"github.com/jiyeyuran/mediasoup-go/v2"
)

const chromeWHIPOffer = `v=0
o=- 4611731400430051336 2 IN IP4 127.0.0.1
s=-
t=0 0
a=group:BUNDLE 0 1
a=extmap-allow-mixed
a=msid-semantic: WMS abc
m=audio 9 UDP/TLS/RTP/SAVPF 111 63 9 0 8 13 110 126
c=IN IP4 0.0.0.0
a=rtcp:9 IN IP4 0.0.0.0
a=ice-ufrag:aBcD
a=ice-pwd:secretpwdsecretpwdsecretpwd12
a=ice-options:trickle
a=fingerprint:sha-256 11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00
a=setup:actpass
a=mid:0
a=extmap:1 urn:ietf:params:rtp-hdrext:ssrc-audio-level
a=extmap:3 http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01
a=sendonly
a=msid:abc audio0
a=rtcp-mux
a=rtpmap:111 opus/48000/2
a=rtcp-fb:111 transport-cc
a=fmtp:111 minptime=10;useinbandfec=1
a=rtpmap:63 red/48000/2
a=rtpmap:9 G722/8000
a=rtpmap:0 PCMU/8000
a=rtpmap:8 PCMA/8000
a=rtpmap:13 CN/8000
a=rtpmap:110 telephone-event/48000
a=rtpmap:126 telephone-event/8000
a=ssrc:3735928559 cname:someCname
a=ssrc:3735928559 msid:abc audio0
m=video 9 UDP/TLS/RTP/SAVPF 96 97 102 103
c=IN IP4 0.0.0.0
a=rtcp:9 IN IP4 0.0.0.0
a=ice-ufrag:aBcD
a=ice-pwd:secretpwdsecretpwdsecretpwd12
a=ice-options:trickle
a=fingerprint:sha-256 11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00
a=setup:actpass
a=mid:1
a=extmap:2 urn:ietf:params:rtp-hdrext:toffset
a=extmap:3 http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01
a=sendonly
a=msid:abc video0
a=rtcp-mux
a=rtcp-rsize
a=rtpmap:96 VP8/90000
a=rtcp-fb:96 goog-remb
a=rtcp-fb:96 transport-cc
a=rtcp-fb:96 ccm fir
a=rtcp-fb:96 nack
a=rtcp-fb:96 nack pli
a=rtpmap:97 rtx/90000
a=fmtp:97 apt=96
a=rtpmap:102 H264/90000
a=rtcp-fb:102 goog-remb
a=rtcp-fb:102 transport-cc
a=rtcp-fb:102 ccm fir
a=rtcp-fb:102 nack
a=rtcp-fb:102 nack pli
a=fmtp:102 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f
a=rtpmap:103 rtx/90000
a=fmtp:103 apt=102
a=ssrc-group:FID 2222222222 3333333333
a=ssrc:2222222222 cname:someCname
a=ssrc:2222222222 msid:abc video0
a=ssrc:3333333333 cname:someCname
a=ssrc:3333333333 msid:abc video0
`

// mockCaps provides a minimal mediasoup RtpCapabilities slice covering the
// codecs used in the Chrome fixture, so we can exercise negotiation.
func mockCaps() *mediasoup.RtpCapabilities {
	return &mediasoup.RtpCapabilities{
		Codecs: []*mediasoup.RtpCodecCapability{
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
				MimeType:  "video/rtx",
				ClockRate: 90000,
			},
			{
				Kind:      mediasoup.MediaKindVideo,
				MimeType:  "video/H264",
				ClockRate: 90000,
				Parameters: mediasoup.RtpCodecSpecificParameters{
					PacketizationMode: 1,
					ProfileLevelId:    "42e01f",
				},
			},
			{
				Kind:      mediasoup.MediaKindVideo,
				MimeType:  "video/rtx",
				ClockRate: 90000,
			},
		},
	}
}

func TestParseOffer_Chrome(t *testing.T) {
	po, err := ParseOffer(chromeWHIPOffer, mockCaps())
	if err != nil {
		t.Fatalf("ParseOffer: %v", err)
	}
	if po.IceUfrag != "aBcD" || po.IcePwd != "secretpwdsecretpwdsecretpwd12" {
		t.Fatalf("ice params not extracted: %+v %+v", po.IceUfrag, po.IcePwd)
	}
	if po.DtlsFingerprint.Algorithm != "sha-256" {
		t.Fatalf("unexpected fingerprint algo: %q", po.DtlsFingerprint.Algorithm)
	}
	if po.DtlsSetup != "actpass" {
		t.Fatalf("unexpected setup: %q", po.DtlsSetup)
	}
	if len(po.BundleMids) != 2 || po.BundleMids[0] != "0" || po.BundleMids[1] != "1" {
		t.Fatalf("bundle mids = %v", po.BundleMids)
	}
	if len(po.Medias) != 2 {
		t.Fatalf("expected 2 media, got %d", len(po.Medias))
	}
	audio := po.Medias[0]
	video := po.Medias[1]
	if audio.Kind != mediasoup.MediaKindAudio || audio.Mid != "0" {
		t.Fatalf("audio media: %+v", audio)
	}
	if video.Kind != mediasoup.MediaKindVideo || video.Mid != "1" {
		t.Fatalf("video media: %+v", video)
	}

	foundOpus := false
	for _, c := range audio.RtpParameters.Codecs {
		if strings.EqualFold(c.MimeType, "audio/opus") && c.PayloadType == 111 {
			foundOpus = true
		}
	}
	if !foundOpus {
		t.Fatalf("audio codecs negotiation missed opus: %+v", audio.RtpParameters.Codecs)
	}

	hasVP8, hasRTX, hasH264 := false, false, false
	for _, c := range video.RtpParameters.Codecs {
		switch {
		case strings.EqualFold(c.MimeType, "video/VP8"):
			hasVP8 = true
		case strings.EqualFold(c.MimeType, "video/rtx"):
			hasRTX = true
		case strings.EqualFold(c.MimeType, "video/H264"):
			hasH264 = true
		}
	}
	if !hasVP8 || !hasRTX || !hasH264 {
		t.Fatalf("video codec negotiation: vp8=%v rtx=%v h264=%v", hasVP8, hasRTX, hasH264)
	}

	// Encodings should carry primary ssrc + rtx ssrc.
	if len(video.RtpParameters.Encodings) != 1 {
		t.Fatalf("expected 1 video encoding, got %d", len(video.RtpParameters.Encodings))
	}
	enc := video.RtpParameters.Encodings[0]
	if enc.Ssrc != 2222222222 {
		t.Fatalf("video ssrc: %d", enc.Ssrc)
	}
	if enc.Rtx == nil || enc.Rtx.Ssrc != 3333333333 {
		t.Fatalf("video rtx ssrc: %+v", enc.Rtx)
	}
	if video.RtpParameters.Rtcp == nil || video.RtpParameters.Rtcp.Cname != "someCname" {
		t.Fatalf("video rtcp cname: %+v", video.RtpParameters.Rtcp)
	}
}

const simulcastOffer = `v=0
o=- 1 2 IN IP4 127.0.0.1
s=-
t=0 0
a=group:BUNDLE 0
a=msid-semantic: WMS abc
m=video 9 UDP/TLS/RTP/SAVPF 96 97
c=IN IP4 0.0.0.0
a=ice-ufrag:aBcD
a=ice-pwd:secretpwdsecretpwdsecretpwd12
a=fingerprint:sha-256 11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00
a=setup:actpass
a=mid:0
a=sendonly
a=rtcp-mux
a=rtpmap:96 VP8/90000
a=rtpmap:97 rtx/90000
a=fmtp:97 apt=96
a=rid:q send
a=rid:h send
a=rid:f send
a=simulcast:send q;h;f
`

func TestParseOffer_Simulcast(t *testing.T) {
	po, err := ParseOffer(simulcastOffer, mockCaps())
	if err != nil {
		t.Fatalf("ParseOffer: %v", err)
	}
	if len(po.Medias) != 1 {
		t.Fatalf("expected 1 video media, got %d", len(po.Medias))
	}
	m := po.Medias[0]
	if len(m.Simulcast) != 3 || m.Simulcast[0] != "q" || m.Simulcast[2] != "f" {
		t.Fatalf("simulcast rids: %v", m.Simulcast)
	}
	if len(m.RtpParameters.Encodings) != 3 {
		t.Fatalf("expected 3 encodings, got %d", len(m.RtpParameters.Encodings))
	}
	for i, want := range []string{"q", "h", "f"} {
		if m.RtpParameters.Encodings[i].Rid != want {
			t.Fatalf("encoding[%d].Rid = %q, want %q", i, m.RtpParameters.Encodings[i].Rid, want)
		}
	}
}

// firefoxOffer is shaped like a real Firefox 122 WHIP offer: ufrag/pwd are
// tighter, fmtp parameters are uppercase-commas, and rtcp-fb wildcards.
const firefoxOffer = `v=0
o=mozilla...THIS_IS_SDPARTA-99.0 1111 0 IN IP4 0.0.0.0
s=-
t=0 0
a=sendrecv
a=fingerprint:sha-256 A1:B2:C3:D4:E5:F6:07:18:29:3A:4B:5C:6D:7E:8F:90:A1:B2:C3:D4:E5:F6:07:18:29:3A:4B:5C:6D:7E:8F:90
a=group:BUNDLE 0 1
a=ice-options:trickle
a=msid-semantic:WMS *
m=audio 9 UDP/TLS/RTP/SAVPF 109
c=IN IP4 0.0.0.0
a=sendonly
a=extmap:1 urn:ietf:params:rtp-hdrext:sdes:mid
a=fmtp:109 maxplaybackrate=48000;stereo=1;useinbandfec=1
a=ice-pwd:fixfirefoxicepasswordofvalidlen
a=ice-ufrag:ffUfrag
a=mid:0
a=msid:{stream} {track}
a=rtcp-mux
a=rtpmap:109 opus/48000/2
a=setup:actpass
a=ssrc:1111111 cname:{firefox-cname}
m=video 9 UDP/TLS/RTP/SAVPF 120 124
c=IN IP4 0.0.0.0
a=sendonly
a=extmap:1 urn:ietf:params:rtp-hdrext:sdes:mid
a=fmtp:124 apt=120
a=ice-pwd:fixfirefoxicepasswordofvalidlen
a=ice-ufrag:ffUfrag
a=mid:1
a=msid:{stream} {track-video}
a=rtcp-fb:120 nack
a=rtcp-fb:120 nack pli
a=rtcp-fb:120 ccm fir
a=rtcp-fb:120 goog-remb
a=rtcp-mux
a=rtpmap:120 VP8/90000
a=rtpmap:124 rtx/90000
a=setup:actpass
a=ssrc-group:FID 3000001 3000002
a=ssrc:3000001 cname:{firefox-cname}
a=ssrc:3000002 cname:{firefox-cname}
`

func TestParseOffer_Firefox(t *testing.T) {
	po, err := ParseOffer(firefoxOffer, mockCaps())
	if err != nil {
		t.Fatalf("ParseOffer: %v", err)
	}
	if po.IceUfrag != "ffUfrag" {
		t.Fatalf("ufrag: %q", po.IceUfrag)
	}
	if len(po.Medias) != 2 {
		t.Fatalf("expected 2 m-lines, got %d", len(po.Medias))
	}
	v := po.Medias[1]
	if v.Kind != mediasoup.MediaKindVideo || v.Mid != "1" {
		t.Fatalf("video mid/kind: %+v", v)
	}
	if len(v.RtpParameters.Encodings) != 1 {
		t.Fatalf("expected 1 encoding")
	}
	enc := v.RtpParameters.Encodings[0]
	if enc.Ssrc != 3000001 || enc.Rtx == nil || enc.Rtx.Ssrc != 3000002 {
		t.Fatalf("bad ssrc/rtx: %+v rtx=%+v", enc, enc.Rtx)
	}
}

// obsOffer is shaped like an OBS 31 WHIP client: no BUNDLE group, ICE at
// session level, just one video m-section.
const obsOffer = `v=0
o=- 12345 2 IN IP4 0.0.0.0
s=OBS
t=0 0
a=ice-ufrag:obsUf
a=ice-pwd:obsPasswordHasAtLeast22Chrs
a=fingerprint:sha-256 77:66:55:44:33:22:11:00:FF:EE:DD:CC:BB:AA:99:88:77:66:55:44:33:22:11:00:FF:EE:DD:CC:BB:AA:99:88
a=setup:actpass
a=group:BUNDLE 0
m=video 9 UDP/TLS/RTP/SAVPF 96
c=IN IP4 0.0.0.0
a=mid:0
a=sendonly
a=rtcp-mux
a=rtpmap:96 VP8/90000
a=fmtp:96 max-fs=12288;max-fr=60
a=rtcp-fb:96 nack
a=rtcp-fb:96 nack pli
a=rtcp-fb:96 ccm fir
a=ssrc:99999 cname:obs
`

func TestParseOffer_OBS(t *testing.T) {
	po, err := ParseOffer(obsOffer, mockCaps())
	if err != nil {
		t.Fatalf("ParseOffer: %v", err)
	}
	if len(po.Medias) != 1 {
		t.Fatalf("expected 1 m-line, got %d", len(po.Medias))
	}
	m := po.Medias[0]
	if m.Kind != mediasoup.MediaKindVideo {
		t.Fatalf("kind: %v", m.Kind)
	}
	if len(m.RtpParameters.Codecs) != 1 || !strings.EqualFold(m.RtpParameters.Codecs[0].MimeType, "video/VP8") {
		t.Fatalf("codecs: %+v", m.RtpParameters.Codecs)
	}
	if len(m.RtpParameters.Encodings) != 1 || m.RtpParameters.Encodings[0].Ssrc != 99999 {
		t.Fatalf("ssrc: %+v", m.RtpParameters.Encodings)
	}
}

func TestParseOffer_MissingFingerprint(t *testing.T) {
	bad := strings.ReplaceAll(chromeWHIPOffer, "a=fingerprint:sha-256", "a=ignored:")
	if _, err := ParseOffer(bad, mockCaps()); err == nil {
		t.Fatalf("expected error for missing fingerprint")
	}
}

// ffmpegOffer mirrors what `ffmpeg -f whip` emits for an MP4 source: video
// only (AAC audio is silently dropped by the WHIP muxer) with a Constrained
// Baseline H.264 profile-level-id whose *level* (Level 2.2 → 0x16) differs
// from the one we advertise in caps (Level 3.1 → 0x1f). We still have to
// negotiate successfully because the profiles are identical.
const ffmpegOffer = `v=0
o=FFmpeg 4489045141692799359 2 IN IP4 127.0.0.1
s=FFmpegPublishSession
t=0 0
a=group:BUNDLE 1
a=extmap-allow-mixed
a=msid-semantic: WMS
m=video 9 UDP/TLS/RTP/SAVPF 106 105
c=IN IP4 0.0.0.0
a=ice-ufrag:f6b0c82c
a=ice-pwd:cf12d2f7cdf9745ce6bb53c24da87975
a=fingerprint:sha-256 AC:BA:B7:A4:92:84:43:FC:D0:F2:C8:33:AC:03:7D:FC:CC:AD:F9:4F:F7:9C:1B:20:90:42:ED:45:33:2A:87:25
a=setup:passive
a=mid:1
a=sendonly
a=msid:FFmpeg video
a=rtcp-mux
a=rtcp-rsize
a=rtpmap:106 H264/90000
a=fmtp:106 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=424016
a=rtcp-fb:106 nack
a=rtpmap:105 rtx/90000
a=fmtp:105 apt=106
a=ssrc-group:FID 2872498389 2872498390
a=ssrc:2872498389 cname:FFmpeg
a=ssrc:2872498389 msid:FFmpeg video
`

func TestParseOffer_FFmpegH264LevelMismatch(t *testing.T) {
	po, err := ParseOffer(ffmpegOffer, mockCaps())
	if err != nil {
		t.Fatalf("ParseOffer: %v", err)
	}
	if len(po.Medias) != 1 {
		t.Fatalf("expected 1 m-line, got %d", len(po.Medias))
	}
	v := po.Medias[0]
	if v.Kind != mediasoup.MediaKindVideo {
		t.Fatalf("kind: %v", v.Kind)
	}
	// Both H.264 (PT=106) and its rtx (PT=105) should survive negotiation.
	var haveH264, haveRtx bool
	for _, c := range v.RtpParameters.Codecs {
		switch strings.ToLower(c.MimeType) {
		case "video/h264":
			haveH264 = true
			if c.Parameters.ProfileLevelId != "424016" {
				t.Fatalf("expected offer PLI to survive, got %q", c.Parameters.ProfileLevelId)
			}
		case "video/rtx":
			haveRtx = true
		}
	}
	if !haveH264 || !haveRtx {
		t.Fatalf("codec set incomplete: %+v", v.RtpParameters.Codecs)
	}
}

func TestH264ProfileCompatible(t *testing.T) {
	cases := []struct {
		name  string
		a, b  string
		match bool
	}{
		{"same-pli", "42e01f", "42e01f", true},
		{"cb-different-level", "424016", "42e01f", true}, // offer L2.2 vs cap L3.1, both Constrained Baseline.
		{"cb-vs-baseline", "42001f", "42e01f", false},    // plain Baseline vs Constrained Baseline.
		{"main-vs-cb", "4d001f", "42e01f", false},        // Main vs Constrained Baseline.
		{"constrained-high-vs-high", "640c1f", "64001f", false},
		{"empty-offer", "", "42e01f", true},
		{"empty-cap", "42e01f", "", true},
		{"malformed", "zz", "42e01f", false}, // falls back to literal compare.
	}
	for _, tc := range cases {
		if got := h264ProfileCompatible(tc.a, tc.b); got != tc.match {
			t.Errorf("%s: h264ProfileCompatible(%q,%q) = %v, want %v", tc.name, tc.a, tc.b, got, tc.match)
		}
	}
}
