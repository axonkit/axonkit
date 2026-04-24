package broadcast

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/rtc"
	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/pion/rtp"
)

// sampleSink is the callback contract between a source and whatever
// consumes samples downstream. In milestone 2 that is directly the
// RTMP pusher; milestone 5 replaces this with Switcher.feedLive /
// Switcher.feedMix so two sources can fan into one set of pushers.
type sampleSink func(s MediaSample)

// packetSink hands off raw RTP packets to pushers that forward them
// as-is (WHIP egress). The kind argument lets the receiver sort
// audio from video without inspecting the packet payload.
type packetSink func(kind SampleKind, pkt *rtp.Packet)

// videoDepacketizer is the common contract the switcher + pusher
// plumbing uses when it needs to peek at "have we seen a config/idr
// yet" or pull the codec configuration record for the FLV sequence
// header. H264, VP9 and AV1 implementations all satisfy it.
type videoDepacketizer interface {
	Push(pkt *rtp.Packet) (MediaSample, bool)
	HasConfig() bool
	SeenIDR() bool
	ConfigRecord() []byte
}

// liveSource bundles the mediasoup resources that consume the bound
// room participant's tracks and deliver decoded MediaSamples to a
// downstream sink. One per Broadcast; always starts in the live state.
//
// Concurrency: the depacketizers are single-producer (mediasoup delivers
// consumer RTP serially per-consumer). sampleSink is invoked from the
// same goroutines; callers that fan samples out to multiple pushers
// should themselves be non-blocking on that path.
type liveSource struct {
	logger *slog.Logger

	// transport is the DirectTransport on the broadcast router that
	// owns our consumers. Owned by liveSource — Close() tears it down.
	transport *mediasoup.Transport
	// audioCons / videoCons are the Consumers feeding us the live
	// participant's Opus / H264|VP9|AV1 RTP. At most one of each; a
	// v1 broadcast requires both.
	audioCons *mediasoup.Consumer
	videoCons *mediasoup.Consumer

	// videoDepack is the codec-specific depacketizer chosen at
	// construction based on the producer's RTP capabilities. The
	// value type differs (H264Depacketizer, VP9Depacketizer,
	// AV1Depacketizer) but all satisfy videoDepacketizer.
	videoDepack videoDepacketizer
	// audioDepack is value (stateless for Opus) — kept explicit for
	// symmetry and to ease a future switch to a stateful variant.
	audioDepack OpusDepacketizer

	sink       sampleSink
	packetSink packetSink

	// onProducerGone is invoked exactly once the first time the
	// audio or video producer closes. Broadcast hooks it to
	// terminate("live_identity_gone").
	onceGone       sync.Once
	onProducerGone func()
}

// liveSourceParams groups the inputs liveSource needs so callers do
// not have to juggle a seven-argument constructor.
type liveSourceParams struct {
	Participant    *rtc.Participant
	AudioProducer  *mediasoup.Producer
	VideoProducer  *mediasoup.Producer
	// VideoCodec selects the depacketizer used to decode the video
	// producer. Must be one of CodecH264 / CodecVP9 / CodecAV1 and
	// must match the codec negotiated on VideoProducer.
	VideoCodec     Codec
	Sink           sampleSink
	PacketSink     packetSink
	OnProducerGone func()
	Logger         *slog.Logger
}

// newLiveSource wires DirectTransport+Consumers on the participant's
// router and starts dispatching RTP to the depacketizer. The returned
// source is live immediately; callers do not need to call a Start.
func newLiveSource(p liveSourceParams) (*liveSource, error) {
	if p.Participant == nil || p.AudioProducer == nil || p.VideoProducer == nil {
		return nil, fmt.Errorf("livesource: missing participant or producers")
	}
	router := p.Participant.Router()
	if router == nil {
		return nil, fmt.Errorf("livesource: participant has no router")
	}

	transport, err := router.CreateDirectTransport(&mediasoup.DirectTransportOptions{})
	if err != nil {
		return nil, fmt.Errorf("livesource direct transport: %w", err)
	}

	caps := router.RtpCapabilities()

	// Create both consumers paused so mediasoup does not start
	// delivering RTP to OnRtp before the caller has had a chance to
	// rebind `sink` (newLiveSource returns, then Broadcast patches in
	// the sink that knows about the concrete liveSource pointer).
	audioCons, err := transport.Consume(&mediasoup.ConsumerOptions{
		ProducerId:      p.AudioProducer.Id(),
		RtpCapabilities: caps,
		Paused:          true,
	})
	if err != nil {
		transport.Close()
		return nil, fmt.Errorf("livesource consume audio: %w", err)
	}

	videoCons, err := transport.Consume(&mediasoup.ConsumerOptions{
		ProducerId:      p.VideoProducer.Id(),
		RtpCapabilities: caps,
		Paused:          true,
	})
	if err != nil {
		audioCons.Close()
		transport.Close()
		return nil, fmt.Errorf("livesource consume video: %w", err)
	}

	depack, err := newVideoDepacketizer(p.VideoCodec)
	if err != nil {
		videoCons.Close()
		audioCons.Close()
		transport.Close()
		return nil, err
	}

	ls := &liveSource{
		logger:         p.Logger,
		transport:      transport,
		audioCons:      audioCons,
		videoCons:      videoCons,
		videoDepack:    depack,
		sink:           p.Sink,
		packetSink:     p.PacketSink,
		onProducerGone: p.OnProducerGone,
	}

	// RTP fan-in. mediasoup-go emits already-serialized RTP bytes;
	// unmarshal once per packet, hand to the codec depacketizer.
	audioCons.OnRtp(ls.handleAudio)
	videoCons.OnRtp(ls.handleVideo)

	audioCons.OnProducerClose(func(ctx context.Context) { ls.producerGone("audio") })
	videoCons.OnProducerClose(func(ctx context.Context) { ls.producerGone("video") })

	// Request an initial keyframe so pushers can start GOP-aligned
	// as soon as the first video consumer's RTP flows. Errors are
	// non-fatal: if the producer is actively streaming key frames
	// already we will pick one up anyway.
	if err := videoCons.RequestKeyFrame(); err != nil {
		ls.logger.Debug("initial RequestKeyFrame failed", "error", err)
	}

	return ls, nil
}

// Resume flips both consumers out of their initial paused state.
// Called by Broadcast.Start after it has finished patching in the
// sample sink, so the first RTP packet arrives on a fully wired
// pipeline.
func (ls *liveSource) Resume() {
	if ls == nil {
		return
	}
	if ls.audioCons != nil {
		_ = ls.audioCons.Resume()
	}
	if ls.videoCons != nil {
		_ = ls.videoCons.Resume()
	}
}

// Close tears down the consumers and the direct transport. Safe to
// call concurrently; only the transport close is meaningful — consumers
// are torn down via transport cascade.
func (ls *liveSource) Close() {
	if ls == nil {
		return
	}
	if ls.transport != nil {
		ls.transport.Close()
	}
}

// RequestKeyFrame asks the upstream H264 producer for a fresh IDR. The
// switcher will call this right before flipping back to live from mix
// so pushers pick the new source up on a key-frame boundary.
func (ls *liveSource) RequestKeyFrame() {
	if ls == nil || ls.videoCons == nil {
		return
	}
	if err := ls.videoCons.RequestKeyFrame(); err != nil {
		ls.logger.Debug("RequestKeyFrame failed", "error", err)
	}
}

func (ls *liveSource) handleAudio(data []byte) {
	pkt := &rtp.Packet{}
	if err := pkt.Unmarshal(data); err != nil {
		return
	}
	if ls.packetSink != nil {
		ls.packetSink(SampleKindAudio, pkt)
	}
	if s, ok := ls.audioDepack.Push(pkt); ok && ls.sink != nil {
		ls.sink(s)
	}
}

func (ls *liveSource) handleVideo(data []byte) {
	pkt := &rtp.Packet{}
	if err := pkt.Unmarshal(data); err != nil {
		return
	}
	if ls.packetSink != nil {
		ls.packetSink(SampleKindVideo, pkt)
	}
	if s, ok := ls.videoDepack.Push(pkt); ok && ls.sink != nil {
		ls.sink(s)
	}
}

func (ls *liveSource) producerGone(kind string) {
	ls.logger.Warn("live producer closed, terminating broadcast", "kind", kind)
	ls.onceGone.Do(func() {
		if ls.onProducerGone != nil {
			ls.onProducerGone()
		}
	})
}

// newVideoDepacketizer wires up the concrete depacketizer that matches
// the producer's negotiated codec. Non-video / unknown codecs yield an
// error so callers fail cleanly rather than silently dropping frames.
func newVideoDepacketizer(codec Codec) (videoDepacketizer, error) {
	switch codec {
	case CodecH264:
		return &H264Depacketizer{}, nil
	case CodecVP9:
		return &VP9Depacketizer{}, nil
	case CodecAV1:
		return &AV1Depacketizer{}, nil
	default:
		return nil, fmt.Errorf("broadcast: unsupported video codec %v", codec)
	}
}

// pickLiveProducers returns the audio + video producers for the
// participant, filtered to codecs the pushers can serialise. Returns
// the negotiated video codec so the caller can instantiate the right
// depacketizer. Prefers camera > other sources for the video leg.
func pickLiveProducers(p *rtc.Participant) (audio, video *mediasoup.Producer, videoCodec Codec, err error) {
	tracks := p.LiveTracks()
	for _, t := range tracks {
		if t.Producer == nil {
			continue
		}
		switch t.Kind {
		case mediasoup.MediaKindAudio:
			if audio == nil && isOpusProducer(t.Producer) {
				audio = t.Producer
			}
		case mediasoup.MediaKindVideo:
			c := producerVideoCodec(t.Producer)
			if c == CodecUnknown {
				continue
			}
			// Prefer camera > screen_share > anything else, so a
			// user with both publishes gets the "main" face camera
			// pushed out.
			if video == nil {
				video = t.Producer
				videoCodec = c
				continue
			}
			if t.Source == proto.TrackSource_CAMERA {
				video = t.Producer
				videoCodec = c
			}
		}
	}
	if audio == nil {
		return nil, nil, CodecUnknown, fmt.Errorf("broadcast: no Opus audio producer found for live identity")
	}
	if video == nil {
		return nil, nil, CodecUnknown, fmt.Errorf("broadcast: no supported video producer (H264/VP9/AV1) found for live identity")
	}
	return audio, video, videoCodec, nil
}

func isOpusProducer(p *mediasoup.Producer) bool {
	rp := p.RtpParameters()
	if rp == nil {
		return false
	}
	for _, c := range rp.Codecs {
		if mimeTypeIs(c.MimeType, "audio/opus") {
			return true
		}
	}
	return false
}

func isH264Producer(p *mediasoup.Producer) bool {
	return producerVideoCodec(p) == CodecH264
}

// producerVideoCodec maps a mediasoup video producer's negotiated
// codec MIME onto the broadcast Codec enum. Returns CodecUnknown if
// none of the supported codecs are present.
func producerVideoCodec(p *mediasoup.Producer) Codec {
	rp := p.RtpParameters()
	if rp == nil {
		return CodecUnknown
	}
	for _, c := range rp.Codecs {
		switch {
		case mimeTypeIs(c.MimeType, "video/h264"):
			return CodecH264
		case mimeTypeIs(c.MimeType, "video/vp9"):
			return CodecVP9
		case mimeTypeIs(c.MimeType, "video/av1"):
			return CodecAV1
		}
	}
	return CodecUnknown
}

// mimeTypeIs compares MIME types case-insensitively; mediasoup reports
// "audio/opus", WebRTC SDPs sometimes use "audio/OPUS", and we want to
// match regardless.
func mimeTypeIs(got, want string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := 0; i < len(got); i++ {
		a := got[i]
		b := want[i]
		if a >= 'A' && a <= 'Z' {
			a += 'a' - 'A'
		}
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		if a != b {
			return false
		}
	}
	return true
}
