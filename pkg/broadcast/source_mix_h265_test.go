package broadcast

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// TestOfferNeedsH265Bypass covers the SDP classification rule:
//
//   - H.265-only video → bypass.
//   - H.264 / VP9 / AV1 / VP8 (with or without H.265 alongside) →
//     stay on the mediasoup path so the existing pipeline keeps
//     working when avflow offers a multi-codec menu.
//   - Garbage / non-video offers → false (we never want to silently
//     route garbage through pion either).
func TestOfferNeedsH265Bypass(t *testing.T) {
	cases := []struct {
		name    string
		offer   string
		want    bool
		comment string
	}{
		{
			name: "h265 only with audio",
			offer: minimalOffer(
				videoSection("116 H265/90000"),
				audioSection("111 opus/48000/2"),
			),
			want:    true,
			comment: "single H.265 video PT → bypass",
		},
		{
			name: "h265 plus rtx still bypass",
			offer: minimalOffer(
				videoSection("116 H265/90000", "117 rtx/90000"),
				audioSection("111 opus/48000/2"),
			),
			want:    true,
			comment: "RTX is plumbing, not a primary codec",
		},
		{
			name: "h264 plus h265 stays mediasoup",
			offer: minimalOffer(
				videoSection("96 H264/90000", "116 H265/90000"),
				audioSection("111 opus/48000/2"),
			),
			want:    false,
			comment: "mediasoup negotiates H.264 — keep it",
		},
		{
			name: "vp9 only is mediasoup",
			offer: minimalOffer(
				videoSection("98 VP9/90000"),
				audioSection("111 opus/48000/2"),
			),
			want:    false,
			comment: "no H265 string at all",
		},
		{
			name:    "audio only is not bypass",
			offer:   minimalOffer(audioSection("111 opus/48000/2")),
			want:    false,
			comment: "no m=video, no bypass",
		},
		{
			name:    "garbage is not bypass",
			offer:   "v=0\r\nthis is not sdp\r\n",
			want:    false,
			comment: "let mediasoup return the parse error",
		},
		{
			name:    "literal H265 in non-rtpmap text",
			offer:   "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=H265\r\n",
			want:    false,
			comment: "no actual rtpmap → nothing to bypass",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := offerNeedsH265Bypass(tc.offer)
			if got != tc.want {
				t.Fatalf("offerNeedsH265Bypass = %v, want %v\n%s\n--- offer:\n%s",
					got, tc.want, tc.comment, tc.offer)
			}
		})
	}
}

// TestExtractIceUfragFindsValueIgnoringWhitespace pins the ETag
// extraction. The answer SDP can use \r\n or \n and we accept either.
func TestExtractIceUfragFindsValueIgnoringWhitespace(t *testing.T) {
	cases := []struct {
		name string
		sdp  string
		want string
	}{
		{"crlf", "v=0\r\na=ice-ufrag:abcd1234\r\n", "abcd1234"},
		{"lf", "v=0\na=ice-ufrag:xyz\n", "xyz"},
		{"missing", "v=0\r\n", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := extractIceUfrag(tc.sdp); got != tc.want {
				t.Fatalf("extractIceUfrag = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestH265MixSourceEndToEnd is the full pion-as-publisher integration
// check: build an H.265 sendonly publisher, hand its offer to the
// bypass, exchange answer, push a synthetic H.265 RTP burst (VPS /
// SPS / PPS / IDR with marker on the IDR), and verify both sample +
// packet sinks fire and OnReady runs exactly once.
//
// We deliberately do NOT exercise audio here: avflow always sends
// audio, but adding a second track would force us to also synthesize
// Opus (which is non-trivial without an encoder dependency). The
// audio path shares its read loop and depacketizer with mixSource,
// which is already covered by the existing depack_*_test.go suite.
//
// Test runtime: ICE/DTLS negotiation in-process via pion typically
// completes in well under a second; we cap with a 5s timeout.
func TestH265MixSourceEndToEnd(t *testing.T) {
	publisher, videoTrack := newPublisherPC(t)
	defer func() { _ = publisher.Close() }()

	offer, err := publisher.CreateOffer(nil)
	if err != nil {
		t.Fatalf("publisher CreateOffer: %v", err)
	}
	gather := webrtc.GatheringCompletePromise(publisher)
	if err := publisher.SetLocalDescription(offer); err != nil {
		t.Fatalf("publisher SetLocalDescription: %v", err)
	}
	select {
	case <-gather:
	case <-time.After(5 * time.Second):
		t.Fatalf("publisher ICE gather timeout")
	}

	var (
		sampleCount   atomic.Int32
		packetCount   atomic.Int32
		readyFired    atomic.Int32
		goneFired     atomic.Int32
		lastSampleKey atomic.Bool
	)

	ms, err := newH265MixSource(h265MixSourceParams{
		OfferSDP: publisher.LocalDescription().SDP,
		SampleSink: func(s MediaSample) {
			sampleCount.Add(1)
			if s.IsKeyFrame {
				lastSampleKey.Store(true)
			}
		},
		PacketSink: func(_ SampleKind, _ *rtp.Packet) {
			packetCount.Add(1)
		},
		OnReady:    func() { readyFired.Add(1) },
		OnGone:     func(string) { goneFired.Add(1) },
		OnIceState: func(string) {},
		Logger:     nil,
	})
	if err != nil {
		t.Fatalf("newH265MixSource: %v", err)
	}
	defer ms.Close()

	if !strings.Contains(ms.Answer(), "H265/90000") {
		t.Fatalf("answer missing H265 rtpmap:\n%s", ms.Answer())
	}
	if ms.ETag() == "" {
		t.Fatalf("answer ETag (ICE ufrag) is empty:\n%s", ms.Answer())
	}

	if err := publisher.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  ms.Answer(),
	}); err != nil {
		t.Fatalf("publisher SetRemoteDescription: %v", err)
	}

	connected := waitConnected(t, publisher, 5*time.Second)
	if !connected {
		t.Fatalf("publisher peer connection failed to reach Connected")
	}

	// Drain the answerer's RTCP so pion's interceptor pipeline keeps
	// flowing — the publisher side must respond to NACK/PLI requests.
	for _, sender := range publisher.GetSenders() {
		go func(s *webrtc.RTPSender) {
			buf := make([]byte, 1500)
			for {
				if _, _, err := s.Read(buf); err != nil {
					return
				}
			}
		}(sender)
	}

	pushSyntheticH265IDR(t, videoTrack)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if readyFired.Load() > 0 && lastSampleKey.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if readyFired.Load() != 1 {
		t.Errorf("OnReady fired %d times, want 1", readyFired.Load())
	}
	if !lastSampleKey.Load() {
		t.Errorf("did not observe a key-frame sample (samples=%d packets=%d)",
			sampleCount.Load(), packetCount.Load())
	}
	if packetCount.Load() == 0 {
		t.Errorf("no RTP packets reached PacketSink")
	}
	if goneFired.Load() != 0 {
		t.Errorf("OnGone fired prematurely: %d", goneFired.Load())
	}
}

// TestH265MixSourceCloseIsIdempotent checks the supervise goroutine
// exits cleanly on Close() even when no track has connected. The
// constructor still completes (offer is valid; ICE gather just times
// out in 5 s) so we feed it a self-built bare offer and tear it down.
func TestH265MixSourceCloseIsIdempotent(t *testing.T) {
	publisher, _ := newPublisherPC(t)
	defer func() { _ = publisher.Close() }()

	offer, err := publisher.CreateOffer(nil)
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	gather := webrtc.GatheringCompletePromise(publisher)
	if err := publisher.SetLocalDescription(offer); err != nil {
		t.Fatalf("SetLocalDescription: %v", err)
	}
	<-gather

	ms, err := newH265MixSource(h265MixSourceParams{
		OfferSDP:   publisher.LocalDescription().SDP,
		SampleSink: func(MediaSample) {},
		PacketSink: func(SampleKind, *rtp.Packet) {},
		OnReady:    func() {},
		OnGone:     func(string) {},
		OnIceState: func(string) {},
	})
	if err != nil {
		t.Fatalf("newH265MixSource: %v", err)
	}

	done := make(chan struct{})
	go func() {
		ms.Close()
		ms.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("Close did not return within 3s — supervise goroutine likely deadlocked")
	}
}

// TestH265MixSourceRestartIceReturnsError pins the contract that the
// pion bypass cannot satisfy a WHIP PATCH ICE-restart and surfaces a
// descriptive error so PatchMixICE returns 4xx instead of fake-OK.
func TestH265MixSourceRestartIceReturnsError(t *testing.T) {
	ms := &h265MixSource{}
	if _, err := ms.RestartIce(); err == nil {
		t.Fatalf("RestartIce should return an error on the bypass")
	}
}

// --- helpers below ---

// minimalOffer assembles the smallest plausible SDP offer with the
// caller-supplied m-sections. Just enough syntactic shape for
// pion/sdp/v3 to UnmarshalString successfully.
func minimalOffer(sections ...string) string {
	var b strings.Builder
	b.WriteString("v=0\r\n")
	b.WriteString("o=- 1 1 IN IP4 127.0.0.1\r\n")
	b.WriteString("s=test\r\n")
	b.WriteString("t=0 0\r\n")
	for _, s := range sections {
		b.WriteString(s)
	}
	return b.String()
}

func videoSection(rtpmaps ...string) string {
	var b strings.Builder
	b.WriteString("m=video 9 UDP/TLS/RTP/SAVPF")
	for _, r := range rtpmaps {
		pt, _, _ := strings.Cut(r, " ")
		b.WriteString(" ")
		b.WriteString(pt)
	}
	b.WriteString("\r\n")
	for _, r := range rtpmaps {
		b.WriteString("a=rtpmap:")
		b.WriteString(r)
		b.WriteString("\r\n")
	}
	return b.String()
}

func audioSection(rtpmap string) string {
	pt, _, _ := strings.Cut(rtpmap, " ")
	return "m=audio 9 UDP/TLS/RTP/SAVPF " + pt + "\r\n" +
		"a=rtpmap:" + rtpmap + "\r\n"
}

// newPublisherPC creates a sendonly H.265 publisher PeerConnection
// suitable for driving the bypass under test. Returns the PC and the
// H.265 track the test pushes RTP into.
func newPublisherPC(t *testing.T) (*webrtc.PeerConnection, *webrtc.TrackLocalStaticRTP) {
	t.Helper()

	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeH265,
			ClockRate: 90000,
		},
		PayloadType: 116,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		t.Fatalf("publisher register h265: %v", err)
	}
	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(me, ir); err != nil {
		t.Fatalf("publisher interceptors: %v", err)
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(ir))
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("publisher NewPeerConnection: %v", err)
	}

	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH265, ClockRate: 90000},
		"video", "axonkit-h265-test",
	)
	if err != nil {
		t.Fatalf("publisher new track: %v", err)
	}
	if _, err := pc.AddTransceiverFromTrack(track, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	}); err != nil {
		t.Fatalf("publisher add transceiver: %v", err)
	}
	return pc, track
}

// waitConnected blocks until the PC reaches Connected or the timeout
// fires. Returns true on Connected, false otherwise.
func waitConnected(t *testing.T, pc *webrtc.PeerConnection, timeout time.Duration) bool {
	t.Helper()
	once := sync.Once{}
	connectedCh := make(chan struct{})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateConnected {
			once.Do(func() { close(connectedCh) })
		}
	})
	if pc.ConnectionState() == webrtc.PeerConnectionStateConnected {
		return true
	}
	select {
	case <-connectedCh:
		return true
	case <-time.After(timeout):
		return false
	}
}

// pushSyntheticH265IDR writes a tiny VPS+SPS+PPS+IDR burst into the
// publisher's track. Each NAL goes out as its own RTP packet with
// monotonically increasing sequence numbers; the IDR carries marker=1
// so the depacketizer flushes the access unit. RTP timestamps are
// fixed at 90000 (one frame's worth at 90 kHz / 30 fps).
func pushSyntheticH265IDR(t *testing.T, track *webrtc.TrackLocalStaticRTP) {
	t.Helper()
	nals := [][]byte{
		hevcNAL(32, 0xAA, 0xBB),
		hevcSPSWithPTL(),
		hevcNAL(34, 0xC0, 0xDE),
		hevcNAL(19, 0xDE, 0xAD, 0xBE, 0xEF),
	}
	seq := uint16(1000)
	for i, nal := range nals {
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    116,
				SequenceNumber: seq,
				Timestamp:      90000,
				SSRC:           0xCAFEBABE,
				Marker:         i == len(nals)-1,
			},
			Payload: nal,
		}
		seq++
		if err := track.WriteRTP(pkt); err != nil {
			t.Fatalf("write rtp[%d]: %v", i, err)
		}
		// Tiny gap so pion's pacer/jitter buffer sees the packets in
		// order — running them too tight in one tight loop is fine,
		// but a microsecond lets the read loop on the bypass side
		// schedule between packets.
		time.Sleep(2 * time.Millisecond)
	}
}
