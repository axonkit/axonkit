package broadcast

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
)

// h265MixSource terminates the avflow → axonkit WHIP ingress for the
// H.265 case directly inside a stand-alone pion PeerConnection,
// bypassing mediasoup entirely.
//
// Why bypass: mediasoup-go's supported RTP capability table omits
// H.265 by design (validation rejects HEVC at Router creation, see
// pkg/media/caps.go). For every other codec (H.264 / VP9 / AV1 / VP8)
// the standard mixSource path stays in charge — only an offer whose
// only m=video codec is H.265 is routed here.
//
// Lifecycle, the four hooks (OnReady / OnGone / OnIceState / sample &
// packet sinks) and the supervise watchdog all match mixSource so the
// switcher and Broadcast control plane cannot tell the two apart.
type h265MixSource struct {
	logger *slog.Logger

	pc *webrtc.PeerConnection
	// answerSDP is the server's SDP answer assembled after ICE
	// gathering completes. Returned to the WHIP POST handler.
	answerSDP string
	// etag is the answer's ICE ufrag, used as the WHIP resource ETag.
	etag string

	videoDepack videoDepacketizer // *H265Depacketizer
	audioDepack OpusDepacketizer

	onceReady sync.Once
	onReady   func()

	onceGone sync.Once
	onGone   func(reason string)

	onIceState func(state string)

	sampleSink sampleSink
	packetSink packetSink

	mu             sync.Mutex
	disconnectedAt time.Time
	iceConnected   bool
	stopCh         chan struct{}
	watchdogDone   chan struct{}
	closed         bool
}

// h265MixSourceParams groups the inputs newH265MixSource needs.
// OfferSDP must be the raw avflow offer; everything else mirrors
// mixSourceParams so the dispatching code in Broadcast.AttachMix only
// needs to substitute the constructor.
type h265MixSourceParams struct {
	OfferSDP   string
	SampleSink sampleSink
	PacketSink packetSink
	OnReady    func()
	OnGone     func(reason string)
	OnIceState func(state string)
	Logger     *slog.Logger
}

// h265BypassOpusPT and h265BypassH265PT are the payload types we offer
// in the answer when the publisher's offer doesn't pin one. Pion will
// internally renumber to honor whatever the offer asked for, so these
// are only fallbacks for completely freeform offers.
const (
	h265BypassH265PT = 116
	h265BypassOpusPT = 111
)

// h265IceGrace mirrors mixIceDisconnectGrace; kept as a separate const
// so the bypass leg can be tuned independently if pion's ICE failure
// detection turns out to be more or less sensitive than mediasoup's.
const h265IceGrace = mixIceDisconnectGrace

// newH265MixSource wires a pion PeerConnection, registers H.265 +
// Opus, applies avflow's offer, builds an answer (waiting for ICE
// gather), starts the supervise goroutine and the per-track read
// loops. On any setup failure the partially-built PC is closed before
// returning the error.
func newH265MixSource(p h265MixSourceParams) (*h265MixSource, error) {
	if p.OfferSDP == "" {
		return nil, fmt.Errorf("h265bypass: empty offer SDP")
	}
	if p.Logger == nil {
		p.Logger = slog.Default().With("component", "broadcast.h265MixSource")
	}

	pc, err := buildH265BypassPC()
	if err != nil {
		return nil, err
	}

	depack, err := newVideoDepacketizer(CodecH265)
	if err != nil {
		_ = pc.Close()
		return nil, fmt.Errorf("h265bypass depacketizer: %w", err)
	}

	ms := &h265MixSource{
		logger:       p.Logger,
		pc:           pc,
		videoDepack:  depack,
		sampleSink:   p.SampleSink,
		packetSink:   p.PacketSink,
		onReady:      p.OnReady,
		onGone:       p.OnGone,
		onIceState:   p.OnIceState,
		stopCh:       make(chan struct{}),
		watchdogDone: make(chan struct{}),
	}

	pc.OnTrack(ms.handleTrack)
	pc.OnICEConnectionStateChange(ms.handleIceState)
	pc.OnConnectionStateChange(ms.handleConnectionState)

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  p.OfferSDP,
	}); err != nil {
		_ = pc.Close()
		return nil, fmt.Errorf("h265bypass set remote: %w", err)
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		_ = pc.Close()
		return nil, fmt.Errorf("h265bypass create answer: %w", err)
	}

	gather := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		_ = pc.Close()
		return nil, fmt.Errorf("h265bypass set local: %w", err)
	}

	// Bound ICE gather like whipPusher; non-trickle WHIP needs a
	// complete answer up front, but a tiny default grace is plenty
	// for loopback/UDP scenarios. avflow is on the same LAN in the
	// expected deployment.
	select {
	case <-gather:
	case <-time.After(5 * time.Second):
		ms.logger.Warn("h265 bypass ICE gather timeout (using candidates so far)")
	}

	local := pc.LocalDescription()
	if local == nil {
		_ = pc.Close()
		return nil, fmt.Errorf("h265bypass missing local description")
	}
	ms.answerSDP = local.SDP
	ms.etag = extractIceUfrag(local.SDP)

	go ms.supervise()
	return ms, nil
}

// buildH265BypassPC constructs the pion PeerConnection used for the
// bypass: a MediaEngine that knows H.265 + Opus and the default
// interceptor stack so RTCP feedback round-trips correctly.
func buildH265BypassPC() (*webrtc.PeerConnection, error) {
	me := &webrtc.MediaEngine{}

	videoFB := []webrtc.RTCPFeedback{
		{Type: "nack"},
		{Type: "nack", Parameter: "pli"},
		{Type: "ccm", Parameter: "fir"},
		{Type: "goog-remb"},
		{Type: "transport-cc"},
	}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:     webrtc.MimeTypeH265,
			ClockRate:    90000,
			RTCPFeedback: videoFB,
		},
		PayloadType: h265BypassH265PT,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return nil, fmt.Errorf("register h265: %w", err)
	}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeOpus,
			ClockRate: 48000,
			Channels:  2,
		},
		PayloadType: h265BypassOpusPT,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, fmt.Errorf("register opus: %w", err)
	}

	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(me, ir); err != nil {
		return nil, fmt.Errorf("register interceptors: %w", err)
	}

	api := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(ir))
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return nil, fmt.Errorf("new peer connection: %w", err)
	}
	return pc, nil
}

// handleTrack is the OnTrack callback. avflow publishes one m=video
// (H.265) and one m=audio (Opus); we spawn a read loop per track and
// also drain RTCP on the receiver so pion's interceptor pipeline
// keeps flowing. Anything else (extra video tracks, unknown codecs)
// is logged and ignored — silently dropping is preferred over killing
// the bypass since the publisher might add tracks defensively.
func (ms *h265MixSource) handleTrack(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	mime := strings.ToLower(track.Codec().MimeType)
	ssrc := track.SSRC()
	ms.logger.Info("h265 bypass track", "kind", track.Kind(), "mime", mime, "ssrc", ssrc)

	go ms.drainRTCP(receiver)

	switch track.Kind() {
	case webrtc.RTPCodecTypeVideo:
		if mime != strings.ToLower(webrtc.MimeTypeH265) {
			ms.logger.Warn("h265 bypass video track has unexpected codec", "mime", mime)
			return
		}
		go ms.readVideo(track)
	case webrtc.RTPCodecTypeAudio:
		if mime != strings.ToLower(webrtc.MimeTypeOpus) {
			ms.logger.Warn("h265 bypass audio track has unexpected codec", "mime", mime)
			return
		}
		go ms.readAudio(track)
	}
}

// drainRTCP keeps the receiver's RTCP buffer empty so pion's
// interceptor pipeline (NACK, TWCC, RTCP repair) keeps making
// forward progress. We don't act on RTCP directly here — the bypass
// is read-only — so a closed receiver simply ends the goroutine.
func (ms *h265MixSource) drainRTCP(receiver *webrtc.RTPReceiver) {
	for {
		if _, _, err := receiver.ReadRTCP(); err != nil {
			return
		}
	}
}

// readVideo is the H.265 RTP read loop. Each packet feeds the
// existing H265Depacketizer; the depacketizer's keyframe flag drives
// fireReady. Raw RTP also goes to packetSink for WHIP egress.
func (ms *h265MixSource) readVideo(track *webrtc.TrackRemote) {
	for {
		pkt, _, err := track.ReadRTP()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				ms.logger.Debug("h265 bypass video read", "error", err)
			}
			ms.fireGone(fmt.Sprintf("h265 video track ended: %v", err))
			return
		}
		if ms.packetSink != nil {
			ms.packetSink(SampleKindVideo, pkt)
		}
		if s, ok := ms.videoDepack.Push(pkt); ok {
			if s.IsKeyFrame {
				ms.fireReady()
			}
			if ms.sampleSink != nil {
				ms.sampleSink(s)
			}
		}
	}
}

// readAudio mirrors readVideo for the Opus track.
func (ms *h265MixSource) readAudio(track *webrtc.TrackRemote) {
	for {
		pkt, _, err := track.ReadRTP()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				ms.logger.Debug("h265 bypass audio read", "error", err)
			}
			ms.fireGone(fmt.Sprintf("h265 audio track ended: %v", err))
			return
		}
		if ms.packetSink != nil {
			ms.packetSink(SampleKindAudio, pkt)
		}
		if s, ok := ms.audioDepack.Push(pkt); ok && ms.sampleSink != nil {
			ms.sampleSink(s)
		}
	}
}

// handleIceState mirrors mixSource.handleIceState's vocabulary:
// connected/completed → arm sample flow + clear watchdog;
// disconnected/failed → arm watchdog (fireGone after grace);
// closed → fireGone immediately. Pion's "new" / "checking" are
// passthrough only so Status() reflects what the wire shows.
func (ms *h265MixSource) handleIceState(state webrtc.ICEConnectionState) {
	str := state.String()
	if ms.onIceState != nil {
		ms.onIceState(str)
	}
	ms.mu.Lock()
	switch state {
	case webrtc.ICEConnectionStateConnected, webrtc.ICEConnectionStateCompleted:
		ms.disconnectedAt = time.Time{}
		first := !ms.iceConnected
		ms.iceConnected = true
		ms.mu.Unlock()
		if first {
			ms.logger.Info("h265 bypass ICE connected")
		}
	case webrtc.ICEConnectionStateDisconnected, webrtc.ICEConnectionStateFailed:
		if ms.disconnectedAt.IsZero() {
			ms.disconnectedAt = time.Now()
			ms.logger.Warn("h265 bypass ICE disconnected; watchdog armed",
				"state", str, "grace", h265IceGrace)
		}
		ms.mu.Unlock()
	case webrtc.ICEConnectionStateClosed:
		ms.mu.Unlock()
		ms.fireGone("h265 bypass ICE closed")
	default:
		ms.mu.Unlock()
	}
}

// handleConnectionState backs up handleIceState: pion may transition
// the aggregate PeerConnection state (DTLS+ICE) to Failed/Closed
// independently of the ICE-only state machine, especially when DTLS
// fails. Treat both terminal states the same way handleIceState does.
func (ms *h265MixSource) handleConnectionState(state webrtc.PeerConnectionState) {
	switch state {
	case webrtc.PeerConnectionStateFailed:
		ms.fireGone("h265 bypass peer connection failed")
	case webrtc.PeerConnectionStateClosed:
		ms.fireGone("h265 bypass peer connection closed")
	}
}

// supervise drives the same once-per-second watchdog mixSource uses.
// On Close() the stopCh closes and we exit immediately; otherwise we
// trip onGone after h265IceGrace of continuous "disconnected".
func (ms *h265MixSource) supervise() {
	defer close(ms.watchdogDone)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ms.stopCh:
			return
		case <-ticker.C:
			ms.mu.Lock()
			disconnected := ms.disconnectedAt
			ms.mu.Unlock()
			if disconnected.IsZero() {
				continue
			}
			if time.Since(disconnected) >= h265IceGrace {
				ms.fireGone("h265 bypass ICE watchdog expired")
				return
			}
		}
	}
}

// fireReady is idempotent.
func (ms *h265MixSource) fireReady() {
	ms.onceReady.Do(func() {
		if ms.onReady != nil {
			ms.onReady()
		}
	})
}

// fireGone is idempotent and is the single mechanism by which the
// bypass tells Broadcast "fall back to live".
func (ms *h265MixSource) fireGone(reason string) {
	ms.onceGone.Do(func() {
		ms.logger.Warn("h265 bypass going away", "reason", reason)
		if ms.onGone != nil {
			ms.onGone(reason)
		}
	})
}

// VideoDepack implements mixIngress.VideoDepack.
func (ms *h265MixSource) VideoDepack() videoDepacketizer { return ms.videoDepack }

// VideoCodec returns the codec this bypass terminates. The pion-bypass
// path is reachable only when the avflow offer's only video m= line is
// H.265, so the answer is always CodecH265 — see Broadcast.attachMix
// dispatching in broadcast.go.
func (ms *h265MixSource) VideoCodec() Codec { return CodecH265 }

// Answer returns the SDP answer the WHIP POST handler must echo back
// to the publisher.
func (ms *h265MixSource) Answer() string { return ms.answerSDP }

// ETag returns the WHIP resource ETag (ICE ufrag of the answer).
func (ms *h265MixSource) ETag() string { return ms.etag }

// RestartIce implements mixIngress.RestartIce. The bypass does not
// (yet) support ICE restart: pion needs a fresh offer to generate one
// and the WHIP PATCH flow only supplies If-Match. Returning an error
// causes PatchMixICE to surface a 4xx — preferable to silently
// pretending success and leaving the publisher stuck.
func (ms *h265MixSource) RestartIce() (string, error) {
	return "", fmt.Errorf("h265 bypass: ICE restart not supported on pion ingress; reconnect by re-issuing POST /whip")
}

// Close tears the PeerConnection down. Idempotent and safe to call
// concurrently with the supervise goroutine — same once-flag pattern
// as mixSource.Close.
func (ms *h265MixSource) Close() {
	ms.mu.Lock()
	if ms.closed {
		ms.mu.Unlock()
		return
	}
	ms.closed = true
	close(ms.stopCh)
	ms.mu.Unlock()

	<-ms.watchdogDone

	if ms.pc != nil {
		_ = ms.pc.Close()
	}
}

// offerNeedsH265Bypass inspects the offer SDP and returns true iff
// every m=video section advertises H.265 only (after stripping
// retransmission / FEC entries). For mixed offers (H.264 + H.265,
// VP9 + H.265, etc.) we deliberately return false: the existing
// mediasoup path can negotiate the non-H.265 codec and avflow can
// fall back to that — no need to engage the bypass.
//
// The function is intentionally tolerant of malformed SDP: any parse
// error reports false, so a truly broken offer falls through to the
// existing mediasoup path which will return a precise parse error.
func offerNeedsH265Bypass(offerSDP string) bool {
	if !strings.Contains(strings.ToUpper(offerSDP), "H265") {
		return false
	}
	sess := &sdp.SessionDescription{}
	if err := sess.UnmarshalString(offerSDP); err != nil {
		return false
	}
	sawH265Only := false
	for _, md := range sess.MediaDescriptions {
		if md.MediaName.Media != "video" {
			continue
		}
		primary := primaryVideoCodecsFromMedia(md)
		if len(primary) == 0 {
			continue
		}
		for _, c := range primary {
			if c != "H265" {
				return false
			}
		}
		sawH265Only = true
	}
	return sawH265Only
}

// primaryVideoCodecsFromMedia returns the upper-cased rtpmap codec
// names of one m=video section, excluding RTX / RED / ULPFEC /
// FLEXFEC-03 / transport-cc placeholder entries — i.e. the actual
// candidate video codecs the publisher is offering.
func primaryVideoCodecsFromMedia(md *sdp.MediaDescription) []string {
	out := make([]string, 0, 4)
	for _, attr := range md.Attributes {
		if attr.Key != "rtpmap" {
			continue
		}
		_, codec, ok := splitRtpmap(attr.Value)
		if !ok {
			continue
		}
		up := strings.ToUpper(codec)
		switch up {
		case "RTX", "RED", "ULPFEC", "FLEXFEC-03":
			continue
		}
		out = append(out, up)
	}
	return out
}

// splitRtpmap parses "<pt> <name>/<clock>" returning (pt, name, true).
// Returns (0, "", false) on malformed input.
func splitRtpmap(value string) (uint8, string, bool) {
	ptStr, rest, ok := strings.Cut(value, " ")
	if !ok {
		return 0, "", false
	}
	pt, err := strconv.ParseUint(ptStr, 10, 8)
	if err != nil {
		return 0, "", false
	}
	name, _, _ := strings.Cut(rest, "/")
	if name == "" {
		return 0, "", false
	}
	return uint8(pt), name, true
}

// extractIceUfrag walks the answer SDP and returns the first
// a=ice-ufrag value. Used as the WHIP resource ETag, mirroring the
// way mediasoup's WebRtcTransport surfaces IceParameters.
// UsernameFragment.
func extractIceUfrag(sdpStr string) string {
	for _, line := range strings.Split(sdpStr, "\n") {
		line = strings.TrimSpace(line)
		if v, ok := strings.CutPrefix(line, "a=ice-ufrag:"); ok {
			return v
		}
	}
	return ""
}
