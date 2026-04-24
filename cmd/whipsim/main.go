// whipsim is the WHIP publisher used by the broadcast verify scripts
// to exercise H.264 / VP9 / AV1 ingest end-to-end. ffmpeg's built-in
// -f whip muxer only speaks H.264, so the VP9 / AV1 codec coverage
// required by the Enhanced-RTMP output path cannot be covered by
// ffmpeg alone; whipsim plugs that gap by consuming IVF video + Ogg
// audio produced by ffmpeg and pushing them as real-time WebRTC media
// over the WHIP signaling handshake.
//
// Usage (as invoked by test/broadcast/lib.sh):
//
//	whipsim \
//	  -url        http://127.0.0.1:17990/whip/<room> \
//	  -token      <bearer> \
//	  -video-ivf  /tmp/userA-video.ivf \
//	  -audio-ogg  /tmp/userA-audio.ogg \
//	  -video-codec vp9 \
//	  -duration   45s
//
// The ffmpeg producer is expected to write IVF + Ogg fragments into
// regular files (they may be named pipes — the readers stream the
// input live). whipsim keeps the PeerConnection up until either
// duration elapses, the pipe EOFs, or a SIGTERM is received. On exit
// it issues a WHIP DELETE against the Location header returned by
// the POST so axonkit's resource counters stay balanced.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/ivfreader"
	"github.com/pion/webrtc/v4/pkg/media/oggreader"
)

func main() {
	var (
		url         = flag.String("url", "", "WHIP endpoint URL (POST target)")
		token       = flag.String("token", "", "Bearer token for Authorization header")
		videoPath   = flag.String("video-ivf", "", "Path to IVF file (VP8/VP9/AV1) providing video")
		audioPath   = flag.String("audio-ogg", "", "Path to Ogg/Opus file providing audio")
		videoCodec  = flag.String("video-codec", "vp9", "Video codec hint: h264 | vp9 | av1 (must match IVF FourCC)")
		duration    = flag.Duration("duration", 0, "Stop after this duration (0 = run until EOF or signal)")
		extraHeader = flag.String("extra-header", "", "Optional single extra HTTP header 'Name: Value' to send on POST/DELETE")
	)
	flag.Parse()

	if *url == "" {
		log.Fatalf("whipsim: -url is required")
	}
	if *videoPath == "" || *audioPath == "" {
		log.Fatalf("whipsim: -video-ivf and -audio-ogg are required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigCh
		log.Printf("whipsim: got signal %s, shutting down", s)
		cancel()
	}()

	if *duration > 0 {
		go func() {
			select {
			case <-ctx.Done():
			case <-time.After(*duration):
				log.Printf("whipsim: duration %s elapsed", *duration)
				cancel()
			}
		}()
	}

	if err := run(ctx, *url, *token, *videoPath, *audioPath, *videoCodec, *extraHeader); err != nil {
		log.Fatalf("whipsim: %v", err)
	}
}

// run orchestrates the peer-connection lifecycle: build tracks for the
// chosen codec, post the offer to the WHIP endpoint, stream media from
// disk until ctx is done, then DELETE the session. It blocks until
// ctx.Done() is observed so callers can `wait` on the process.
func run(ctx context.Context, whipURL, token, videoPath, audioPath, videoCodec, extraHeader string) error {
	mime, clockRate, err := codecMime(videoCodec)
	if err != nil {
		return err
	}

	m := &webrtc.MediaEngine{}
	if err := registerCodecs(m, videoCodec); err != nil {
		return fmt.Errorf("register codecs: %w", err)
	}

	// The broadcast verify scripts drive axonkit over loopback with
	// webrtc-announced-ip=127.0.0.1. pion's ICE agent filters out
	// loopback interfaces by default, so without this call we would
	// only advertise public IPv4/IPv6 addresses that axonkit has no
	// route back to, and ICE would stall at "checking → failed".
	s := webrtc.SettingEngine{}
	s.SetIncludeLoopbackCandidate(true)

	api := webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithSettingEngine(s))

	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return fmt.Errorf("new peer connection: %w", err)
	}
	defer pc.Close()

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Printf("whipsim: peer connection state -> %s", state)
	})
	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Printf("whipsim: ice connection state -> %s", state)
	})

	videoTrack, err := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{
		MimeType:  mime,
		ClockRate: clockRate,
	}, "video", "whipsim")
	if err != nil {
		return fmt.Errorf("new video track: %w", err)
	}
	if _, err := pc.AddTrack(videoTrack); err != nil {
		return fmt.Errorf("add video track: %w", err)
	}

	audioTrack, err := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeOpus,
		ClockRate: 48000,
		Channels:  2,
	}, "audio", "whipsim")
	if err != nil {
		return fmt.Errorf("new audio track: %w", err)
	}
	if _, err := pc.AddTrack(audioTrack); err != nil {
		return fmt.Errorf("add audio track: %w", err)
	}

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("create offer: %w", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return fmt.Errorf("set local description: %w", err)
	}
	// WHIP: wait for ICE gathering to complete so the POST carries a
	// fully-trickled offer (no ICE restart loop).
	<-webrtc.GatheringCompletePromise(pc)

	answerSDP, location, err := doWhipPost(ctx, whipURL, token, extraHeader, pc.LocalDescription().SDP)
	if err != nil {
		return fmt.Errorf("whip post: %w", err)
	}
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answerSDP,
	}); err != nil {
		return fmt.Errorf("set remote description: %w", err)
	}
	log.Printf("whipsim: WHIP session established, location=%s", location)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := pumpVideo(ctx, videoPath, videoTrack); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("whipsim: video pump stopped: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := pumpAudio(ctx, audioPath, audioTrack); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("whipsim: audio pump stopped: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("whipsim: context done, tearing down")
	wg.Wait()

	if location != "" {
		if err := doWhipDelete(context.Background(), location, token, extraHeader); err != nil {
			log.Printf("whipsim: DELETE %s failed: %v", location, err)
		}
	}
	return nil
}

// codecMime maps the human-readable -video-codec flag to pion's MIME
// constants. ClockRate is always 90 kHz for our three supported video
// codecs; the helper returns it anyway so callers don't hard-code.
func codecMime(codec string) (string, uint32, error) {
	switch strings.ToLower(codec) {
	case "h264":
		return webrtc.MimeTypeH264, 90000, nil
	case "vp9":
		return webrtc.MimeTypeVP9, 90000, nil
	case "av1":
		return webrtc.MimeTypeAV1, 90000, nil
	default:
		return "", 0, fmt.Errorf("unsupported video codec %q (want h264|vp9|av1)", codec)
	}
}

// registerCodecs populates the pion MediaEngine with only the codecs
// we need. We register the one chosen video codec (so SDP negotiation
// picks exactly it, matching axonkit's router capabilities) plus Opus
// for audio. Static PT values come from the pion default profiles.
func registerCodecs(m *webrtc.MediaEngine, videoCodec string) error {
	if err := m.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "minptime=10;useinbandfec=1",
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return err
	}
	switch strings.ToLower(videoCodec) {
	case "h264":
		return m.RegisterCodec(webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:    webrtc.MimeTypeH264,
				ClockRate:   90000,
				SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
			},
			PayloadType: 102,
		}, webrtc.RTPCodecTypeVideo)
	case "vp9":
		return m.RegisterCodec(webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:    webrtc.MimeTypeVP9,
				ClockRate:   90000,
				SDPFmtpLine: "profile-id=0",
			},
			PayloadType: 98,
		}, webrtc.RTPCodecTypeVideo)
	case "av1":
		return m.RegisterCodec(webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeAV1,
				ClockRate: 90000,
			},
			PayloadType: 45,
		}, webrtc.RTPCodecTypeVideo)
	}
	return fmt.Errorf("unsupported video codec %q", videoCodec)
}

// doWhipPost sends the SDP offer to the WHIP endpoint and returns the
// answer SDP + Location header (used to send a later DELETE). axonkit's
// broadcast mix ingest wants the bearer token in Authorization; the
// optional extra header slot covers endpoints that expect an additional
// header (X-Axonkit-*, etc.).
func doWhipPost(ctx context.Context, url, token, extraHeader, offer string) (answer, location string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(offer))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/sdp")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if extraHeader != "" {
		if n, v, ok := splitHeader(extraHeader); ok {
			req.Header.Set(n, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		buf, _ := io.ReadAll(resp.Body)
		return "", "", fmt.Errorf("POST %s: %s: %s", url, resp.Status, bytes.TrimSpace(buf))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("read answer: %w", err)
	}
	location = resp.Header.Get("Location")
	// Location may be relative to the POST URL (RFC 9725 allows both
	// absolute and relative). Resolve relative paths here so DELETE
	// later can hit it unchanged.
	if location != "" && !strings.HasPrefix(location, "http") {
		location = resolveURL(url, location)
	}
	return string(body), location, nil
}

// doWhipDelete is the cleanup counterpart of doWhipPost. Failures are
// logged by the caller; they're not fatal because the broadcast side
// will eventually GC the session via ICE timeout.
func doWhipDelete(ctx context.Context, location, token, extraHeader string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, location, http.NoBody)
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if extraHeader != "" {
		if n, v, ok := splitHeader(extraHeader); ok {
			req.Header.Set(n, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("DELETE %s: %s", location, resp.Status)
	}
	return nil
}

func splitHeader(s string) (name, value string, ok bool) {
	i := strings.Index(s, ":")
	if i <= 0 {
		return "", "", false
	}
	return strings.TrimSpace(s[:i]), strings.TrimSpace(s[i+1:]), true
}

// resolveURL joins a relative Location path to its POST URL base,
// mirroring http.Client's own behavior. Only the protocol+host
// prefix is preserved; query strings on the POST URL are dropped.
func resolveURL(base, rel string) string {
	// Very small implementation — WHIP Locations in practice are
	// either absolute or "/rtc/broadcast/<id>/whip/<sid>" relative.
	if strings.HasPrefix(rel, "/") {
		slash := strings.Index(base, "://")
		if slash < 0 {
			return rel
		}
		host := base[slash+3:]
		if i := strings.Index(host, "/"); i >= 0 {
			host = host[:i]
		}
		return base[:slash+3] + host + rel
	}
	// Relative (without leading slash): append to base directory.
	if i := strings.LastIndex(base, "/"); i > 0 {
		return base[:i+1] + rel
	}
	return base + "/" + rel
}

// pumpVideo streams frames from an IVF file as pion media.Samples at
// wall-clock rate derived from the IVF header. IVF carries VP8, VP9,
// AV1 and (as a de-facto convention) raw Annex-B H.264 nal streams in
// ffmpeg-produced files; ivfreader treats each frame as an opaque
// payload so all three codecs route through the same pump.
//
// When the IVF source ends (EOF) the pump returns so the caller can
// decide whether to cancel the context. We never loop — the verify
// scripts pass in a duration that outlives the longest mix phase.
func pumpVideo(ctx context.Context, path string, track *webrtc.TrackLocalStaticSample) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open video: %w", err)
	}
	defer f.Close()

	ivf, header, err := ivfreader.NewWith(f)
	if err != nil {
		return fmt.Errorf("ivf header: %w", err)
	}
	var frameDuration time.Duration
	if header.TimebaseDenominator > 0 && header.TimebaseNumerator > 0 {
		frameDuration = time.Duration(float64(time.Second) *
			float64(header.TimebaseNumerator) /
			float64(header.TimebaseDenominator))
	}
	if frameDuration <= 0 {
		frameDuration = time.Second / 30
	}
	log.Printf("whipsim: video pump FourCC=%s frame-duration=%s", header.FourCC, frameDuration)

	ticker := time.NewTicker(frameDuration)
	defer ticker.Stop()

	for {
		frame, _, err := ivf.ParseNextFrame()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("ivf parse: %w", err)
		}
		if err := track.WriteSample(media.Sample{
			Data:     frame,
			Duration: frameDuration,
		}); err != nil {
			return fmt.Errorf("write video sample: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// pumpAudio streams Opus packets from an Ogg/Opus file at 20 ms per
// packet (the rate ffmpeg emits for `-c:a libopus`). The Ogg stream
// begins with two metadata pages — OpusHead and OpusTags — that carry
// zero audio samples; feeding them to the peer connection pollutes the
// RTP stream with garbage packets that some decoders (including
// ffmpeg's opus parser, which is what the RTMP recorder feeds) refuse
// to recover from, leaving the final recording audio-less. We detect
// them by payload signature and skip them explicitly.
func pumpAudio(ctx context.Context, path string, track *webrtc.TrackLocalStaticSample) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open audio: %w", err)
	}
	defer f.Close()

	ogg, _, err := oggreader.NewWith(f)
	if err != nil {
		return fmt.Errorf("ogg header: %w", err)
	}
	const opusFrame = 20 * time.Millisecond
	ticker := time.NewTicker(opusFrame)
	defer ticker.Stop()

	var (
		lastGranule uint64
		audioFrames int
	)
	for {
		payload, header, err := ogg.ParseNextPage()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("ogg parse: %w", err)
		}
		if isOpusHeaderPage(payload) {
			// OpusHead / OpusTags — skip without touching the ticker
			// so the first real sample goes out immediately.
			lastGranule = header.GranulePosition
			continue
		}
		sampleCount := float64(header.GranulePosition - lastGranule)
		lastGranule = header.GranulePosition
		sampleDuration := time.Duration((sampleCount / 48000.0) * float64(time.Second))
		if sampleDuration <= 0 {
			sampleDuration = opusFrame
		}
		if err := track.WriteSample(media.Sample{
			Data:     payload,
			Duration: sampleDuration,
		}); err != nil {
			return fmt.Errorf("write audio sample: %w", err)
		}
		if audioFrames == 0 {
			log.Printf("whipsim: audio pump first frame sent (len=%d dur=%s)", len(payload), sampleDuration)
		}
		audioFrames++
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// isOpusHeaderPage reports whether an Ogg page payload is one of the
// two Opus metadata pages (OpusHead, OpusTags). Both are identified by
// a fixed 8-byte magic prefix at offset 0 per RFC 7845 §5.1.
func isOpusHeaderPage(payload []byte) bool {
	if len(payload) < 8 {
		return false
	}
	magic := string(payload[:8])
	return magic == "OpusHead" || magic == "OpusTags"
}
