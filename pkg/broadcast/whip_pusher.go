package broadcast

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// whipPusher forwards the broadcast's live/mix RTP into a remote WHIP
// ingest endpoint (RFC 9725) using a sendonly RTCPeerConnection. Each
// destination owns its own PeerConnection + two TrackLocalStaticRTP
// instances (audio + video); pion handles SSRC / sequence rewrite, so
// callers can hand off arbitrary RTP packets verbatim.
//
// Lifecycle and concurrency semantics mirror rtmpPusher:
//   - Start once, Stop once
//   - WriteRTP is safe to call from RTP hot-path goroutines
//   - Connection setup is handled on a background goroutine that the
//     hot path never blocks on
type whipPusher struct {
	url    string
	logger *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	pc          *webrtc.PeerConnection
	audioTrack  *webrtc.TrackLocalStaticRTP
	videoTrack  *webrtc.TrackLocalStaticRTP
	resourceURL string

	started    atomic.Bool
	connected  atomic.Bool
	bytesOut   atomic.Uint64
	dropped    atomic.Uint64
	lastErr    atomic.Pointer[string]
	done       chan struct{}
	setupDone  chan struct{}
	httpClient *http.Client
}

const (
	whipSetupTimeout = 15 * time.Second
	whipContentType  = "application/sdp"
)

// newWHIPPusher constructs a WHIP pusher but does not yet open the
// PeerConnection. Call Start before Write*.
func newWHIPPusher(destURL string, logger *slog.Logger) (*whipPusher, error) {
	if !strings.HasPrefix(destURL, "http://") && !strings.HasPrefix(destURL, "https://") {
		return nil, fmt.Errorf("whip url must start with http:// or https://, got %q", destURL)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &whipPusher{
		url:        destURL,
		logger:     logger.With("sink", "whip", "url", destURL),
		done:       make(chan struct{}),
		setupDone:  make(chan struct{}),
		httpClient: &http.Client{Timeout: whipSetupTimeout},
	}, nil
}

// Start builds the PeerConnection, creates sendonly tracks, POSTs the
// offer to the WHIP endpoint, and applies the answer. All network
// work happens on a background goroutine so creation returns quickly.
func (p *whipPusher) Start(ctx context.Context) {
	if !p.started.CompareAndSwap(false, true) {
		return
	}
	p.ctx, p.cancel = context.WithCancel(ctx)
	go p.runSetup()
}

// Stop closes the PeerConnection and DELETEs the remote resource (best
// effort). Idempotent.
func (p *whipPusher) Stop() {
	if !p.started.Load() {
		close(p.done)
		return
	}
	select {
	case <-p.done:
		return
	default:
	}
	if p.cancel != nil {
		p.cancel()
	}
	// Wait for setup to conclude so we have a resourceURL to DELETE
	// (or the setup error to skip DELETE cleanly).
	select {
	case <-p.setupDone:
	case <-time.After(500 * time.Millisecond):
	}
	if p.resourceURL != "" {
		p.deleteResource()
	}
	if p.pc != nil {
		_ = p.pc.Close()
	}
	close(p.done)
}

// BytesOut returns the approximate total bytes written into the pion
// tracks. pion does not surface over-the-wire counters directly, so
// this is a sum of RTP payload + header bytes we handed off.
func (p *whipPusher) BytesOut() uint64 { return p.bytesOut.Load() }

// Connected reports whether the ICE + DTLS negotiation succeeded AND
// the PeerConnection is in ConnectedState.
func (p *whipPusher) Connected() bool { return p.connected.Load() }

// LastError returns the last setup/negotiation error, or "" if none.
func (p *whipPusher) LastError() string {
	if s := p.lastErr.Load(); s != nil {
		return *s
	}
	return ""
}

// WriteRTP forwards one RTP packet into the matching sendonly track.
// Called from the source's OnRtp callback. Pion rewrites ssrc/seq/pt
// internally, so the caller does not need to renumber the packet.
func (p *whipPusher) WriteRTP(kind SampleKind, pkt *rtp.Packet) {
	// No-op until negotiation completes. Dropping packets during setup
	// is fine: the remote's decoder starts from our first key frame,
	// and our upstream Consumer keeps requesting IDRs for the first
	// second anyway.
	if !p.connected.Load() {
		p.dropped.Add(1)
		return
	}
	var track *webrtc.TrackLocalStaticRTP
	switch kind {
	case SampleKindAudio:
		track = p.audioTrack
	case SampleKindVideo:
		track = p.videoTrack
	}
	if track == nil {
		return
	}
	if err := track.WriteRTP(pkt); err != nil {
		if err == io.ErrClosedPipe {
			return
		}
		p.recordErr(err)
		return
	}
	p.bytesOut.Add(uint64(pkt.MarshalSize()))
}

func (p *whipPusher) runSetup() {
	defer close(p.setupDone)
	defer close(p.done)

	if err := p.connect(); err != nil {
		p.recordErr(err)
		p.logger.Error("whip pusher setup failed", "error", err)
		if p.pc != nil {
			_ = p.pc.Close()
		}
		return
	}
	p.logger.Info("whip pusher connected", "resource", p.resourceURL)

	// Block until the parent cancels or PeerConnection fails; Stop
	// will cancel us. The hot path is driven purely by WriteRTP
	// callbacks, so there is nothing periodic to do here.
	<-p.ctx.Done()
}

// connect runs the full WHIP negotiation: build PC → offer → POST →
// apply answer → wait for connected state (bounded by whipSetupTimeout).
func (p *whipPusher) connect() error {
	mediaEngine := &webrtc.MediaEngine{}
	// Register the exact codecs room users publish (H264 CBP level 3.1 +
	// Opus 48k stereo). These happen to be the mediasoup defaults which
	// every live source in our scope negotiates, so the WHIP answer we
	// get back (from avflow or another axonkit broadcast) lines up
	// without extra fmtp juggling.
	if err := mediaEngine.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeH264,
			ClockRate:   90000,
			SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
		},
		PayloadType: 96,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return fmt.Errorf("register h264: %w", err)
	}
	if err := mediaEngine.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeOpus,
			ClockRate: 48000,
			Channels:  2,
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return fmt.Errorf("register opus: %w", err)
	}

	api := webrtc.NewAPI(webrtc.WithMediaEngine(mediaEngine))
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return fmt.Errorf("new peer connection: %w", err)
	}
	p.pc = pc

	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "axonkit-broadcast",
	)
	if err != nil {
		return fmt.Errorf("new audio track: %w", err)
	}
	p.audioTrack = audioTrack

	videoTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000},
		"video", "axonkit-broadcast",
	)
	if err != nil {
		return fmt.Errorf("new video track: %w", err)
	}
	p.videoTrack = videoTrack

	if _, err := pc.AddTransceiverFromTrack(audioTrack, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	}); err != nil {
		return fmt.Errorf("add audio transceiver: %w", err)
	}
	if _, err := pc.AddTransceiverFromTrack(videoTrack, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	}); err != nil {
		return fmt.Errorf("add video transceiver: %w", err)
	}

	connectedCh := make(chan struct{}, 1)
	failedCh := make(chan error, 1)
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		p.logger.Info("whip peer connection state", "state", state)
		switch state {
		case webrtc.PeerConnectionStateConnected:
			p.connected.Store(true)
			select {
			case connectedCh <- struct{}{}:
			default:
			}
		case webrtc.PeerConnectionStateFailed,
			webrtc.PeerConnectionStateClosed,
			webrtc.PeerConnectionStateDisconnected:
			p.connected.Store(false)
			select {
			case failedCh <- fmt.Errorf("peer connection state=%s", state):
			default:
			}
		}
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("create offer: %w", err)
	}
	gatherComplete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		return fmt.Errorf("set local description: %w", err)
	}
	select {
	case <-gatherComplete:
	case <-time.After(5 * time.Second):
		p.logger.Warn("ICE gather timeout (proceeding with candidates gathered so far)")
	case <-p.ctx.Done():
		return p.ctx.Err()
	}

	answer, resourceURL, err := p.postOffer(pc.LocalDescription().SDP)
	if err != nil {
		return fmt.Errorf("post offer: %w", err)
	}
	p.resourceURL = resourceURL

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answer,
	}); err != nil {
		return fmt.Errorf("set remote description: %w", err)
	}

	select {
	case <-connectedCh:
		return nil
	case err := <-failedCh:
		return err
	case <-time.After(whipSetupTimeout):
		return fmt.Errorf("whip connect timeout after %s", whipSetupTimeout)
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

// postOffer sends the SDP to the WHIP server and returns the answer
// SDP plus the absolute Location URL (for PATCH/DELETE). Resolution
// of relative Location URLs is standard RFC 3986 — we just pass that
// work to net/url via http.Request.URL.Parse.
func (p *whipPusher) postOffer(sdp string) (string, string, error) {
	req, err := http.NewRequestWithContext(p.ctx, http.MethodPost, p.url, strings.NewReader(sdp))
	if err != nil {
		return "", "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", whipContentType)
	req.Header.Set("Accept", whipContentType)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("post sdp: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return "", "", fmt.Errorf("read answer: %w", err)
	}
	if resp.StatusCode != http.StatusCreated {
		return "", "", fmt.Errorf("whip POST returned status %d: %s", resp.StatusCode, bytes.TrimSpace(body))
	}

	loc := resp.Header.Get("Location")
	if loc == "" {
		return "", "", fmt.Errorf("whip response missing Location header")
	}
	// Resolve relative Location against the request URL (RFC 9725 §4).
	resource, err := req.URL.Parse(loc)
	if err != nil {
		return "", "", fmt.Errorf("parse Location %q: %w", loc, err)
	}
	return string(body), resource.String(), nil
}

// deleteResource issues the WHIP DELETE to clean up the remote
// session. Best-effort — a 404 or network failure during teardown is
// logged at debug level and not surfaced.
func (p *whipPusher) deleteResource() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, p.resourceURL, http.NoBody)
	if err != nil {
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		p.logger.Debug("whip delete failed", "error", err)
		return
	}
	_ = resp.Body.Close()
	p.logger.Info("whip resource deleted", "status", resp.StatusCode)
}

func (p *whipPusher) recordErr(err error) {
	s := err.Error()
	p.lastErr.Store(&s)
}
