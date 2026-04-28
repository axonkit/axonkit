package broadcast

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
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
// # Codec class change handling
//
// Unlike the RTMP path — which carries codec config inline (FLV
// SeqHeader / E-RTMP SeqStart) and can therefore re-publish a new
// FourCC over the same TCP socket once the receiver opens a new
// recording segment (see rtmp_pusher.go's requestReconnect) — WebRTC
// pins codec MimeType to the SDP-negotiated PT at offer/answer time
// and pion's TrackLocalStaticRTP capability is immutable after
// creation. There is no in-band escape hatch: once the SDP is
// finalised, every RTP packet the receiver decodes is interpreted
// against that PT's fmtp string.
//
// So a flip from H.264 live to H.265 mix (or any other class change)
// requires the full WHIP renegotiation dance:
//  1. DELETE the WHIP resource so the remote tears down its session,
//  2. close the local PeerConnection,
//  3. build a fresh MediaEngine + tracks for the new codec,
//  4. POST a new offer to the same WHIP endpoint and apply the answer.
//
// The pusher exposes NoteVideoCodec(codec) for this; the adapter calls
// it on every Broadcast.onFlipTo*. Same-codec calls are no-ops; a
// different codec enqueues a reconnect request that the runLoop drains
// at the next opportunity. WriteRTP gates on `connected`, so during
// the ~1–3 s reconnect window packets are silently dropped — the same
// behaviour the very first connect already exercises.
//
// # Lifecycle and concurrency
//
//   - Start once, Stop once.
//   - WriteRTP is safe to call from RTP hot-path goroutines.
//   - Connection setup and reconnects run on a background runLoop
//     that the hot path never blocks on.
//   - NoteVideoCodec is safe from the same source-flip goroutine
//     Broadcast.onFlipTo* runs on; it never blocks.
type whipPusher struct {
	url    string
	logger *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	// pcMu guards every field below it that the runLoop mutates
	// across reconnects. WriteRTP only reads videoTrack/audioTrack
	// while connected.Load()==true; runLoop ensures the tracks are
	// already swapped to the new PC's instances before flipping
	// connected back to true on a reconnect.
	pcMu        sync.Mutex
	pc          *webrtc.PeerConnection
	audioTrack  *webrtc.TrackLocalStaticRTP
	videoTrack  *webrtc.TrackLocalStaticRTP
	resourceURL string
	// activeCodec is the video codec the current PeerConnection's
	// SDP was negotiated against. Compared against the requested
	// codec on every NoteVideoCodec call to gate reconnects.
	activeCodec Codec

	// requestedCodec is the video codec the pusher should re-handshake
	// with on the next reconnect cycle. Written by NoteVideoCodec
	// under pcMu, read by runLoop right before each connect attempt.
	// Defaults to whatever caller passed to newWHIPPusher so the very
	// first connect already targets the live producer's codec.
	requestedCodec Codec

	started   atomic.Bool
	connected atomic.Bool
	bytesOut  atomic.Uint64
	lastErr   atomic.Pointer[string]
	done      chan struct{}
	// reconnectCh is a buffered=1 nudge: NoteVideoCodec writes to it
	// when the requested codec differs from the currently active one;
	// runLoop selects on it (alongside ctx.Done) inside its
	// idle-after-connect wait so a codec change drops out of the
	// idle wait, tears the PC down and dials again. Buffered to 1 so
	// concurrent NoteVideoCodec calls never block; the loop coalesces
	// them by re-reading requestedCodec at the top of each iteration.
	reconnectCh chan struct{}
	httpClient  *http.Client
}

const (
	whipSetupTimeout = 15 * time.Second
	whipContentType  = "application/sdp"
	// whipVideoPayloadType is the dynamic PT we register in every
	// MediaEngine. Pion will renegotiate to whatever the remote
	// answer prefers, so the value here is only the offer's hint.
	// 96 is the canonical "first dynamic" value the mediasoup default
	// table also uses, which keeps SDP diffs minimal across a flip.
	whipVideoPayloadType = 96
	whipAudioPayloadType = 111
)

// newWHIPPusher constructs a WHIP pusher but does not yet open the
// PeerConnection. Call Start before Write*.
//
// initialVideoCodec is the codec the live producer is currently
// encoding against; it picks the MimeType registered into the very
// first MediaEngine and the capability of the first TrackLocalStaticRTP.
// Subsequent NoteVideoCodec calls (issued by the adapter on every
// Broadcast source flip) replace it via the reconnect path.
func newWHIPPusher(destURL string, initialVideoCodec Codec, logger *slog.Logger) (*whipPusher, error) {
	if !strings.HasPrefix(destURL, "http://") && !strings.HasPrefix(destURL, "https://") {
		return nil, fmt.Errorf("whip url must start with http:// or https://, got %q", destURL)
	}
	if !initialVideoCodec.IsVideo() {
		return nil, fmt.Errorf("whip pusher: initial codec %v is not a video codec", initialVideoCodec)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &whipPusher{
		url:            destURL,
		logger:         logger.With("sink", "whip", "url", destURL),
		requestedCodec: initialVideoCodec,
		done:           make(chan struct{}),
		reconnectCh:    make(chan struct{}, 1),
		httpClient:     &http.Client{Timeout: whipSetupTimeout},
	}, nil
}

// Start spins up the runLoop which drives the initial connect and any
// subsequent reconnects (one per video codec class change). Returns
// immediately; the connect itself is async.
func (p *whipPusher) Start(ctx context.Context) {
	if !p.started.CompareAndSwap(false, true) {
		return
	}
	p.ctx, p.cancel = context.WithCancel(ctx)
	go p.runLoop()
}

// Stop closes the PeerConnection and DELETEs the remote resource (best
// effort). Idempotent.
func (p *whipPusher) Stop() {
	if !p.started.Load() {
		select {
		case <-p.done:
		default:
			close(p.done)
		}
		return
	}
	if p.cancel != nil {
		p.cancel()
	}
	// runLoop closes p.done after teardown; bound the wait so a
	// hung HTTP client never deadlocks the broadcast shutdown path.
	select {
	case <-p.done:
	case <-time.After(whipSetupTimeout + time.Second):
		p.logger.Warn("whip pusher Stop timed out waiting for runLoop")
	}
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

// NoteVideoCodec records the codec the next batch of video samples
// will arrive in. If it matches the codec the active PeerConnection
// was negotiated against the call is a silent no-op; otherwise the
// new codec is stashed in requestedCodec and the runLoop is nudged
// to tear down + re-handshake. Safe to call concurrently with
// WriteRTP — the comparison and the channel send are both lock-light.
func (p *whipPusher) NoteVideoCodec(codec Codec) {
	if !codec.IsVideo() {
		return
	}
	p.pcMu.Lock()
	if p.activeCodec == codec && p.requestedCodec == codec {
		p.pcMu.Unlock()
		return
	}
	p.requestedCodec = codec
	p.pcMu.Unlock()
	select {
	case p.reconnectCh <- struct{}{}:
	default:
		// Already a pending nudge; runLoop will pick up the
		// latest requestedCodec when it drains.
	}
	p.logger.Info("whip pusher: codec change requested", "codec", codec)
}

// WriteRTP forwards one RTP packet into the matching sendonly track.
// Called from the source's OnRtp callback. Pion rewrites ssrc/seq/pt
// internally, so the caller does not need to renumber the packet.
func (p *whipPusher) WriteRTP(kind SampleKind, pkt *rtp.Packet) {
	// No-op until negotiation completes (or during a reconnect).
	// Dropping packets during setup is fine: the remote's decoder
	// starts from our first key frame, and our upstream Consumer
	// keeps requesting IDRs for the first second anyway. The same
	// drop-while-disconnected behaviour covers the codec-change
	// reconnect window — receivers never decode mismatched payload
	// against a stale SDP fmtp.
	if !p.connected.Load() {
		return
	}
	// Snapshot the active tracks under pcMu so a concurrent
	// reconnect that just swapped them in cannot tear the WriteRTP
	// out from under us. The lock is held only across two pointer
	// reads; pion's TrackLocalStaticRTP.WriteRTP runs unlocked.
	p.pcMu.Lock()
	var track *webrtc.TrackLocalStaticRTP
	switch kind {
	case SampleKindAudio:
		track = p.audioTrack
	case SampleKindVideo:
		track = p.videoTrack
	}
	p.pcMu.Unlock()
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

// runLoop is the connect → idle → (codec change | ctx done) cycle.
// On a codec change it tears down the current PC + WHIP resource and
// starts a fresh negotiation with the new codec. On ctx done it tears
// down once and exits, closing p.done.
//
// Failures during connect are non-fatal: the error is recorded into
// lastErr and the loop blocks on the next reconnect nudge. This
// matches the "broadcast keeps running, operator inspects status"
// philosophy of the rest of the egress.
func (p *whipPusher) runLoop() {
	defer close(p.done)

	for {
		// Pick up the latest requested codec; NoteVideoCodec may
		// have been called multiple times before runLoop reached
		// here, so we always read the most recent value rather
		// than queue-of-codecs to avoid stale connects.
		p.pcMu.Lock()
		codec := p.requestedCodec
		p.pcMu.Unlock()

		if err := p.connect(codec); err != nil {
			p.recordErr(err)
			p.logger.Error("whip pusher setup failed", "codec", codec, "error", err)
			p.disconnect()
			// Wait for either a fresh codec change or
			// shutdown; we do not retry the same codec on
			// our own (the broadcast operator gets the
			// signal via Status.LastError).
			select {
			case <-p.reconnectCh:
				continue
			case <-p.ctx.Done():
				return
			}
		}
		p.logger.Info("whip pusher connected", "codec", codec, "resource", p.resourceURL)

		// Idle: WriteRTP is driving traffic. Block until either
		// (a) ctx is cancelled (Stop), or (b) a codec change
		// nudges the reconnect channel.
		select {
		case <-p.reconnectCh:
			p.logger.Info("whip pusher: tearing down for codec change")
			p.disconnect()
			// Fall through to the top of the loop; it
			// re-reads requestedCodec.
		case <-p.ctx.Done():
			p.disconnect()
			return
		}
	}
}

// connect runs the full WHIP negotiation for the given video codec:
// build MediaEngine → register codec(s) → build PC + tracks → offer
// → POST → apply answer → wait for connected state. Bounded by
// whipSetupTimeout. On success p.pc / p.audioTrack / p.videoTrack /
// p.resourceURL / p.activeCodec are populated under pcMu and
// p.connected is set to true.
func (p *whipPusher) connect(videoCodec Codec) error {
	mediaEngine := &webrtc.MediaEngine{}
	if err := registerWHIPVideoCodec(mediaEngine, videoCodec); err != nil {
		return err
	}
	if err := mediaEngine.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeOpus,
			ClockRate: 48000,
			Channels:  2,
		},
		PayloadType: whipAudioPayloadType,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return fmt.Errorf("register opus: %w", err)
	}

	api := webrtc.NewAPI(webrtc.WithMediaEngine(mediaEngine))
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return fmt.Errorf("new peer connection: %w", err)
	}

	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "axonkit-broadcast",
	)
	if err != nil {
		_ = pc.Close()
		return fmt.Errorf("new audio track: %w", err)
	}
	videoTrack, err := webrtc.NewTrackLocalStaticRTP(
		whipVideoTrackCapability(videoCodec),
		"video", "axonkit-broadcast",
	)
	if err != nil {
		_ = pc.Close()
		return fmt.Errorf("new video track: %w", err)
	}

	if _, err := pc.AddTransceiverFromTrack(audioTrack, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	}); err != nil {
		_ = pc.Close()
		return fmt.Errorf("add audio transceiver: %w", err)
	}
	if _, err := pc.AddTransceiverFromTrack(videoTrack, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	}); err != nil {
		_ = pc.Close()
		return fmt.Errorf("add video transceiver: %w", err)
	}

	connectedCh := make(chan struct{}, 1)
	failedCh := make(chan error, 1)
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		p.logger.Info("whip peer connection state", "state", state, "codec", videoCodec)
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
		_ = pc.Close()
		return fmt.Errorf("create offer: %w", err)
	}
	gatherComplete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		_ = pc.Close()
		return fmt.Errorf("set local description: %w", err)
	}
	select {
	case <-gatherComplete:
	case <-time.After(5 * time.Second):
		p.logger.Warn("ICE gather timeout (proceeding with candidates gathered so far)")
	case <-p.ctx.Done():
		_ = pc.Close()
		return p.ctx.Err()
	}

	answer, resourceURL, err := p.postOffer(pc.LocalDescription().SDP)
	if err != nil {
		_ = pc.Close()
		return fmt.Errorf("post offer: %w", err)
	}

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answer,
	}); err != nil {
		_ = pc.Close()
		// Best-effort: release the resource we just created.
		p.deleteResourceURL(resourceURL)
		return fmt.Errorf("set remote description: %w", err)
	}

	// Publish the new connection state under pcMu so a concurrent
	// WriteRTP picks up matching tracks atomically with connected.
	p.pcMu.Lock()
	p.pc = pc
	p.audioTrack = audioTrack
	p.videoTrack = videoTrack
	p.resourceURL = resourceURL
	p.activeCodec = videoCodec
	p.pcMu.Unlock()

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

// disconnect tears down the active session: mark disconnected, DELETE
// the WHIP resource (best-effort), close the PC, and clear the cached
// pointers so a stray WriteRTP can't dereference a closed track.
func (p *whipPusher) disconnect() {
	p.connected.Store(false)
	p.pcMu.Lock()
	pc := p.pc
	resource := p.resourceURL
	p.pc = nil
	p.audioTrack = nil
	p.videoTrack = nil
	p.resourceURL = ""
	p.activeCodec = CodecUnknown
	p.pcMu.Unlock()
	if resource != "" {
		p.deleteResourceURL(resource)
	}
	if pc != nil {
		_ = pc.Close()
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

// deleteResourceURL issues the WHIP DELETE for the given absolute
// resource URL. Best-effort — a 404 or network failure during teardown
// is logged at debug level and not surfaced.
func (p *whipPusher) deleteResourceURL(resourceURL string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, resourceURL, http.NoBody)
	if err != nil {
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		p.logger.Debug("whip delete failed", "error", err, "resource", resourceURL)
		return
	}
	_ = resp.Body.Close()
	p.logger.Info("whip resource deleted", "status", resp.StatusCode, "resource", resourceURL)
}

func (p *whipPusher) recordErr(err error) {
	s := err.Error()
	p.lastErr.Store(&s)
}

// registerWHIPVideoCodec registers the given video codec into a fresh
// MediaEngine with the dynamic PT whipVideoPayloadType. Each switch
// arm uses the canonical fmtp string for the codec; pion is liberal
// about fmtp matching at answer-time so we only have to land in the
// right ballpark to interop with the avflow / axonkit WHIP receivers
// we ship against.
func registerWHIPVideoCodec(me *webrtc.MediaEngine, codec Codec) error {
	cap, err := whipMediaEngineCapability(codec)
	if err != nil {
		return err
	}
	return me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: cap,
		PayloadType:        whipVideoPayloadType,
	}, webrtc.RTPCodecTypeVideo)
}

// whipMediaEngineCapability is the offer-side capability for a codec:
// MimeType + ClockRate + the canonical fmtp hint. The fmtp values
// chosen here are the lowest-common-denominator profiles the receivers
// we test against (avflow, another axonkit broadcast) accept; remote
// servers that demand a higher profile will negotiate it via the
// answer's fmtp re-write and pion accepts that.
func whipMediaEngineCapability(codec Codec) (webrtc.RTPCodecCapability, error) {
	switch codec {
	case CodecH264:
		return webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeH264,
			ClockRate:   90000,
			SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
		}, nil
	case CodecH265:
		// Main profile, level 3.1. tx-mode=SRST is the
		// single-stream ("not RTP-multicast / no temporal-id")
		// variant every modern WHIP receiver accepts; we never
		// publish multi-stream HEVC.
		return webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeH265,
			ClockRate:   90000,
			SDPFmtpLine: "level-id=93;profile-id=1;tier-flag=0;tx-mode=SRST",
		}, nil
	case CodecVP9:
		return webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeVP9,
			ClockRate:   90000,
			SDPFmtpLine: "profile-id=0",
		}, nil
	case CodecAV1:
		return webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeAV1,
			ClockRate: 90000,
		}, nil
	case CodecVP8:
		return webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeVP8,
			ClockRate: 90000,
		}, nil
	default:
		return webrtc.RTPCodecCapability{}, fmt.Errorf("whip pusher: unsupported video codec %v", codec)
	}
}

// whipVideoTrackCapability is the same MimeType / ClockRate pair
// without fmtp, suitable for TrackLocalStaticRTP. fmtp belongs only
// in the MediaEngine offer; the local track is identified by mime
// alone so the pion side picks the matching transceiver out of the
// remote's answer regardless of fmtp parameter ordering.
func whipVideoTrackCapability(codec Codec) webrtc.RTPCodecCapability {
	switch codec {
	case CodecH264:
		return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}
	case CodecH265:
		return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH265, ClockRate: 90000}
	case CodecVP9:
		return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP9, ClockRate: 90000}
	case CodecAV1:
		return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeAV1, ClockRate: 90000}
	case CodecVP8:
		return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}
	}
	// Unreachable: registerWHIPVideoCodec already rejected unknown
	// codecs before we reach NewTrackLocalStaticRTP. Returning H.264
	// here is the safest fallback — the connect path will fail at
	// the first packet write rather than silently mis-route RTP.
	return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}
}
