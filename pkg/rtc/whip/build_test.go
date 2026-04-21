package whip

import (
	"strings"
	"testing"

	"github.com/jiyeyuran/mediasoup-go/v2"
)

func TestBuildAnswer_RoundTrip(t *testing.T) {
	po, err := ParseOffer(chromeWHIPOffer, mockCaps())
	if err != nil {
		t.Fatalf("ParseOffer: %v", err)
	}

	medias := make([]*MediaAnswer, 0, len(po.Medias))
	for _, m := range po.Medias {
		medias = append(medias, &MediaAnswer{
			Mid:        m.Mid,
			Kind:       m.Kind,
			Direction:  m.Direction,
			RtpParams:  m.RtpParameters,
			OfferPTs:   m.OfferPTs,
			OfferProto: m.OfferPortProto,
			Msid:       m.Msid,
		})
	}

	ans, err := BuildAnswer(&AnswerParams{
		IceParameters: &mediasoup.IceParameters{
			UsernameFragment: "srvUfrag",
			Password:         "serverIcePasswordAtLeast22chars",
			IceLite:          true,
		},
		IceCandidates: []mediasoup.IceCandidate{
			{
				Foundation: "udpcandidate",
				Priority:   2130706431,
				Address:    "10.0.0.1",
				Protocol:   mediasoup.TransportProtocolUDP,
				Port:       40001,
				Type:       "host",
			},
		},
		DtlsParameters: &mediasoup.DtlsParameters{
			Role: mediasoup.DtlsRoleServer,
			Fingerprints: []mediasoup.DtlsFingerprint{
				{Algorithm: "sha-256", Value: "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88"},
			},
		},
		DtlsSetup: "passive",
		Medias:    medias,
	})
	if err != nil {
		t.Fatalf("BuildAnswer: %v", err)
	}
	if !strings.Contains(ans, "a=group:BUNDLE 0 1") {
		t.Fatalf("missing bundle group:\n%s", ans)
	}
	if !strings.Contains(ans, "a=ice-ufrag:srvUfrag") {
		t.Fatalf("missing ice ufrag:\n%s", ans)
	}
	if !strings.Contains(ans, "a=setup:passive") {
		t.Fatalf("missing dtls setup:\n%s", ans)
	}
	if !strings.Contains(ans, "a=rtpmap:111 opus/48000/2") {
		t.Fatalf("missing opus rtpmap:\n%s", ans)
	}
	if !strings.Contains(ans, "a=rtpmap:96 VP8/90000") {
		t.Fatalf("missing vp8 rtpmap:\n%s", ans)
	}
	if !strings.Contains(ans, "a=fmtp:102 ") || !strings.Contains(ans, "profile-level-id=42e01f") {
		t.Fatalf("missing h264 fmtp:\n%s", ans)
	}
	if !strings.Contains(ans, "a=recvonly") {
		t.Fatalf("expected recvonly direction:\n%s", ans)
	}
	if !strings.Contains(ans, "a=candidate:udpcandidate 1 UDP 2130706431 10.0.0.1 40001 typ host") {
		t.Fatalf("missing candidate:\n%s", ans)
	}
	if !strings.Contains(ans, "a=end-of-candidates") {
		t.Fatalf("missing end-of-candidates:\n%s", ans)
	}

	// The answer should parse back cleanly.
	if _, err := ParseOffer(ans, mockCaps()); err != nil {
		t.Fatalf("ParseOffer round-trip: %v", err)
	}
}

func TestBuildAnswer_WHEP(t *testing.T) {
	codecs := []*mediasoup.RtpCodecParameters{
		{
			MimeType:    "video/VP8",
			PayloadType: 96,
			ClockRate:   90000,
			RtcpFeedback: []*mediasoup.RtcpFeedback{
				{Type: "nack"},
				{Type: "nack", Parameter: "pli"},
			},
		},
		{
			MimeType:    "video/rtx",
			PayloadType: 97,
			ClockRate:   90000,
			Parameters:  mediasoup.RtpCodecSpecificParameters{Apt: 96},
		},
	}
	ans, err := BuildAnswer(&AnswerParams{
		IceParameters:  &mediasoup.IceParameters{UsernameFragment: "uf", Password: "pwdpwdpwdpwdpwdpwdpwdpwd"},
		DtlsParameters: &mediasoup.DtlsParameters{Fingerprints: []mediasoup.DtlsFingerprint{{Algorithm: "sha-256", Value: "AA:BB"}}},
		DtlsSetup:      "active",
		Medias: []*MediaAnswer{{
			Mid:           "0",
			Kind:          mediasoup.MediaKindVideo,
			Direction:     mediasoup.MediaDirectionRecvonly, // client is recvonly in WHEP
			RtpParams:     &mediasoup.RtpParameters{Mid: "0", Codecs: codecs},
			ServerSsrc:    444444444,
			ServerRtxSsrc: 555555555,
			ServerCname:   "axonkit",
			Msid:          "stream track",
		}},
	})
	if err != nil {
		t.Fatalf("BuildAnswer: %v", err)
	}
	if !strings.Contains(ans, "a=sendonly") {
		t.Fatalf("WHEP answer should be sendonly from server:\n%s", ans)
	}
	if !strings.Contains(ans, "a=ssrc:444444444 cname:axonkit") {
		t.Fatalf("missing server ssrc:\n%s", ans)
	}
	if !strings.Contains(ans, "a=ssrc-group:FID 444444444 555555555") {
		t.Fatalf("missing FID group:\n%s", ans)
	}
}
