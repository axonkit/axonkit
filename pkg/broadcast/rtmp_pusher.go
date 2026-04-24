package broadcast

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	gortmp "github.com/yutopp/go-rtmp"
	rtmpmsg "github.com/yutopp/go-rtmp/message"
)

// rtmpFrame is one muxed FLV tag ready for Stream.Write. Decoupling the
// muxing (on the hot callback thread) from the RTMP send (on the
// background connect/publish goroutine) keeps the RTP hot path non-
// blocking: Enqueue is O(1) and never performs network I/O.
type rtmpFrame struct {
	kind SampleKind
	// timestampMs is the per-stream milliseconds-from-start value the
	// RTMP receiver will stamp on its FLV tag. Pusher maintains per-
	// kind zero points so it can accept samples whose RTP timestamps
	// are offset from an arbitrary origin.
	timestampMs uint32
	body        []byte
}

// rtmpPusher is one outbound RTMP push target. It buffers FLV tags
// internally, opens the RTMP connection lazily on the first Enqueue,
// and reconnects on connection-level errors (bounded retries).
//
// Lifecycle:
//
//	p := newRTMPPusher(url, logger)
//	p.Start(ctx)
//	p.WriteVideoSeqHeader(avcc)
//	p.WriteAudioSeqHeader(channels=2)
//	for sample := range samples {
//	    p.WriteSample(sample)
//	}
//	p.Stop()
//
// Not thread-safe per-method EXCEPT WriteSample + WriteVideo/AudioSeqHeader
// + Stop, which can be called from the RTP hot path concurrently with
// the background goroutine (the internal channel does the hand-off).
type rtmpPusher struct {
	url    string
	parsed *rtmpURL
	logger *slog.Logger

	// queue carries ready-to-send FLV frames from the hot path to the
	// background send goroutine. Bounded to rtmpPusherQueueDepth to
	// keep memory usage sane under back-pressure; drops are logged and
	// counted so operators see stream stalls.
	queue     chan *rtmpFrame
	dropped   atomic.Uint64
	bytesOut  atomic.Uint64
	started   atomic.Bool
	connected atomic.Bool
	lastErrMu sync.Mutex
	lastErr   string

	// videoStart / audioStart are the RTP timestamps of the first
	// sample on each stream; all subsequent timestamps are expressed
	// as a delta from these, so the RTMP receiver always sees a
	// stream that starts at (or near) ms=0.
	videoStart atomic.Uint64 // uint64(1<<32) | rtpTimestamp; bit 32 = "set"
	audioStart atomic.Uint64

	// videoHdrSent / audioHdrSent guard the "seq header exactly once"
	// contract. Flipped atomically so the fan-out path can stay
	// lock-free across the audio/video consumer goroutines.
	videoHdrSent atomic.Bool
	audioHdrSent atomic.Bool

	// videoSeq* caches the payload of the most recently emitted video
	// seq header so EnsureVideoSeqHeader / EnsureExVideoSeqHeader can
	// skip re-emitting an identical tag after ResetVideoHeader is
	// called on a live↔mix flip. Most RTMP demuxers (notably ffmpeg)
	// treat a mid-stream SeqHeader tag as a codec change and drop the
	// TCP connection; keeping the tag out when the codec config has
	// not actually changed preserves the single-connection recording.
	videoSeqMu     sync.Mutex
	videoSeqBody   []byte
	videoSeqFourCC string // "" for classic AVC, fourCC (e.g. "vp09") for E-RTMP

	done chan struct{}
}

const (
	rtmpPusherQueueDepth = 512
	rtmpRetryInterval    = 1 * time.Second
	rtmpMaxRetries       = 5
)

// newRTMPPusher constructs a pusher but does NOT open the connection.
// Start must be called before the first WriteSample; WriteSample is a
// no-op when the pusher is not started.
func newRTMPPusher(destURL string, logger *slog.Logger) (*rtmpPusher, error) {
	parsed, err := parseRTMPURL(destURL)
	if err != nil {
		return nil, fmt.Errorf("rtmp url: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &rtmpPusher{
		url:    destURL,
		parsed: parsed,
		logger: logger.With("sink", "rtmp", "url", destURL),
		queue:  make(chan *rtmpFrame, rtmpPusherQueueDepth),
		done:   make(chan struct{}),
	}, nil
}

// Start launches the background connect/publish/send loop. Safe to
// call exactly once; subsequent calls are no-ops.
func (p *rtmpPusher) Start(ctx context.Context) {
	if !p.started.CompareAndSwap(false, true) {
		return
	}
	go p.runLoop(ctx)
}

// Stop signals the background goroutine to exit, drops any buffered
// frames, and waits for the goroutine to finish. Idempotent.
func (p *rtmpPusher) Stop() {
	if !p.started.Load() {
		close(p.done)
		return
	}
	// Signal by closing the queue; runLoop drains any remaining
	// frames and then exits.
	select {
	case <-p.done:
		return
	default:
	}
	close(p.queue)
	<-p.done
}

// BytesOut returns the approximate number of FLV body bytes sent so
// far. Used for the destination status JSON.
func (p *rtmpPusher) BytesOut() uint64 { return p.bytesOut.Load() }

// Connected reports whether the pusher's RTMP session is currently in
// a live publishing state.
func (p *rtmpPusher) Connected() bool { return p.connected.Load() }

// LastError returns the most recent connection/publish error string,
// or "" if none.
func (p *rtmpPusher) LastError() string {
	p.lastErrMu.Lock()
	defer p.lastErrMu.Unlock()
	return p.lastErr
}

// EnsureVideoSeqHeader enqueues the AVC seq header tag the first time
// it is called and returns true; subsequent calls are no-ops that
// return false. After ResetVideoHeader the next call also skips the
// enqueue if the AVCC bytes are byte-identical to the previously
// emitted header — preventing a mid-stream SeqHeader tag that would
// otherwise cause ffmpeg (and similar RTMP demuxers) to drop the TCP
// connection even though the codec config hasn't changed.
func (p *rtmpPusher) EnsureVideoSeqHeader(avcc []byte) bool {
	if len(avcc) == 0 {
		return false
	}
	if !p.videoHdrSent.CompareAndSwap(false, true) {
		return false
	}
	p.videoSeqMu.Lock()
	if p.videoSeqFourCC == "" && bytes.Equal(p.videoSeqBody, avcc) {
		p.videoSeqMu.Unlock()
		return false
	}
	p.videoSeqBody = append(p.videoSeqBody[:0], avcc...)
	p.videoSeqFourCC = ""
	p.videoSeqMu.Unlock()
	p.enqueue(&rtmpFrame{
		kind:        SampleKindVideo,
		timestampMs: 0,
		body:        buildAVCSeqHeaderTag(avcc),
	})
	return true
}

// EnsureExVideoSeqHeader is the E-RTMP counterpart of
// EnsureVideoSeqHeader: used for codecs that live in the enhanced-RTMP
// video tag layout (VP9 + AV1 today). The caller supplies a FourCC
// plus the codec configuration record (VPCodecConfigurationRecord /
// AV1CodecConfigurationRecord); this method only guards the "send
// once per source" contract and skips the enqueue when both the
// FourCC and config bytes match the previously emitted header.
func (p *rtmpPusher) EnsureExVideoSeqHeader(fourCC string, config []byte) bool {
	if len(fourCC) != 4 || len(config) == 0 {
		return false
	}
	if !p.videoHdrSent.CompareAndSwap(false, true) {
		return false
	}
	p.videoSeqMu.Lock()
	if p.videoSeqFourCC == fourCC && bytes.Equal(p.videoSeqBody, config) {
		p.videoSeqMu.Unlock()
		return false
	}
	p.videoSeqBody = append(p.videoSeqBody[:0], config...)
	p.videoSeqFourCC = fourCC
	p.videoSeqMu.Unlock()
	p.enqueue(&rtmpFrame{
		kind:        SampleKindVideo,
		timestampMs: 0,
		body:        buildExVideoSeqHeaderTag(fourCC, config),
	})
	return true
}

// EnsureAudioSeqHeader is the audio counterpart of EnsureVideoSeqHeader.
func (p *rtmpPusher) EnsureAudioSeqHeader(channels uint8) bool {
	if !p.audioHdrSent.CompareAndSwap(false, true) {
		return false
	}
	p.enqueue(&rtmpFrame{
		kind:        SampleKindAudio,
		timestampMs: 0,
		body:        buildOpusSeqHeaderTag(channels),
	})
	return true
}

// ResetVideoHeader arms the next EnsureVideoSeqHeader call to
// re-evaluate whether a fresh seq header tag needs to go out. Used by
// the switcher when flipping sources: the new source's first keyframe
// brings a codec config record, which is compared byte-wise against
// the previously emitted one. Identical bytes → no tag is enqueued,
// so the RTMP receiver never sees a mid-stream SeqHeader and keeps
// the TCP connection open. Differing bytes → a fresh SeqHeader tag
// is enqueued before the next keyframe.
func (p *rtmpPusher) ResetVideoHeader() { p.videoHdrSent.Store(false) }

// WriteSample encodes one MediaSample into an FLV tag and enqueues it
// for send. Drops the sample (with a counter bump) if the send queue
// is full — this preserves hot-path latency under back-pressure, which
// matters more than occasional late A/V for live broadcast.
func (p *rtmpPusher) WriteSample(s MediaSample) {
	ts := p.timestampMs(s)
	var body []byte
	switch s.Codec {
	case CodecH264:
		body = buildAVCFrameTag(s.IsKeyFrame, s.Data)
	case CodecVP9:
		body = buildExVideoFrameTag(flvExVideoFourCCVP9, s.IsKeyFrame, s.Data)
	case CodecAV1:
		body = buildExVideoFrameTag(flvExVideoFourCCAV1, s.IsKeyFrame, s.Data)
	case CodecOpus:
		body = buildOpusFrameTag(s.Data)
	default:
		// Unsupported codecs should never reach WriteSample because
		// Broadcast.Start rejects them, but guard anyway so a future
		// addition does not silently corrupt the stream.
		return
	}
	p.enqueue(&rtmpFrame{kind: s.Kind, timestampMs: ts, body: body})
}

// enqueue pushes one frame onto the queue or drops it (with a log) if
// the queue is full. Tries a non-blocking send first, so the hot path
// is never blocked on the background goroutine.
func (p *rtmpPusher) enqueue(f *rtmpFrame) {
	select {
	case p.queue <- f:
	default:
		d := p.dropped.Add(1)
		if d == 1 || d%200 == 0 {
			p.logger.Warn("rtmp pusher queue full, dropping frame",
				"dropped", d, "kind", f.kind)
		}
	}
}

// timestampMs converts a sample's RTP timestamp into FLV-tag milliseconds,
// using the first observed sample of that kind as zero.
func (p *rtmpPusher) timestampMs(s MediaSample) uint32 {
	var startSlot *atomic.Uint64
	var clockHz uint32
	switch s.Kind {
	case SampleKindVideo:
		startSlot = &p.videoStart
		clockHz = 90000
	case SampleKindAudio:
		startSlot = &p.audioStart
		clockHz = 48000
	default:
		return 0
	}
	existing := startSlot.Load()
	if existing == 0 {
		// Store "set" flag in high bit so zero can be sentinel.
		startSlot.CompareAndSwap(0, uint64(1)<<32|uint64(s.RTPTimestamp))
		existing = startSlot.Load()
	}
	start := uint32(existing & 0xFFFFFFFF)
	// Unsigned subtraction handles RTP-timestamp wraparound
	// correctly for the 32-bit field.
	delta := s.RTPTimestamp - start
	return uint32(uint64(delta) * 1000 / uint64(clockHz))
}

// runLoop is the background goroutine: connect → publish → drain queue
// → retry on error. Exits once the queue is closed or the context is
// canceled.
func (p *rtmpPusher) runLoop(ctx context.Context) {
	defer close(p.done)

	for retries := 0; retries <= rtmpMaxRetries; retries++ {
		if err := ctx.Err(); err != nil {
			return
		}
		if err := p.connectAndDrain(ctx); err != nil {
			p.recordErr(err)
			p.connected.Store(false)
			p.logger.Error("rtmp publish failed",
				"error", err, "retry", retries, "max", rtmpMaxRetries,
			)
			select {
			case <-ctx.Done():
				return
			case <-time.After(rtmpRetryInterval):
			}
			continue
		}
		// Clean exit (queue closed).
		return
	}
	p.logger.Error("rtmp pusher gave up after retries")
}

func (p *rtmpPusher) connectAndDrain(ctx context.Context) error {
	p.logger.Info("connecting to rtmp server", "host", p.parsed.Host, "app", p.parsed.App)
	conn, err := gortmp.Dial(p.parsed.Scheme, p.parsed.Host, &gortmp.ConnConfig{})
	if err != nil {
		return fmt.Errorf("dial %s: %w", p.parsed.Host, err)
	}
	defer conn.Close()

	if err := conn.Connect(&rtmpmsg.NetConnectionConnect{
		Command: rtmpmsg.NetConnectionConnectCommand{App: p.parsed.App},
	}); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	stream, err := conn.CreateStream(&rtmpmsg.NetConnectionCreateStream{}, 4096)
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	if err := stream.Publish(&rtmpmsg.NetStreamPublish{
		PublishingName: p.parsed.StreamKey,
		PublishingType: "live",
	}); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	p.logger.Info("rtmp publish started", "streamKey", p.parsed.StreamKey)
	p.connected.Store(true)
	defer p.connected.Store(false)

	return p.pump(ctx, stream)
}

// pump reads from the queue and forwards every frame to the RTMP
// stream. Returns nil when the queue closes (clean shutdown), an error
// for any send failure so the caller can retry the connection.
func (p *rtmpPusher) pump(ctx context.Context, stream *gortmp.Stream) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case f, ok := <-p.queue:
			if !ok {
				return nil
			}
			if err := writeFLVFrame(stream, f); err != nil {
				return fmt.Errorf("write frame: %w", err)
			}
			p.bytesOut.Add(uint64(len(f.body)))
		}
	}
}

func (p *rtmpPusher) recordErr(err error) {
	p.lastErrMu.Lock()
	p.lastErr = err.Error()
	p.lastErrMu.Unlock()
}

// writeFLVFrame writes one pre-muxed FLV tag body on the given RTMP
// stream. Video / audio pick their corresponding go-rtmp message type;
// everything else is a programming error (enqueue filters by kind).
func writeFLVFrame(stream *gortmp.Stream, f *rtmpFrame) error {
	const chunkStreamID = 6
	switch f.kind {
	case SampleKindVideo:
		return stream.Write(chunkStreamID, f.timestampMs, &rtmpmsg.VideoMessage{
			Payload: bytes.NewReader(f.body),
		})
	case SampleKindAudio:
		return stream.Write(chunkStreamID, f.timestampMs, &rtmpmsg.AudioMessage{
			Payload: bytes.NewReader(f.body),
		})
	}
	return fmt.Errorf("unknown frame kind %d", f.kind)
}
