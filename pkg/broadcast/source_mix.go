package broadcast

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/pion/rtp"
)

// mixIceDisconnectGrace is how long we let a disconnected mix ingest
// try to recover before we fall back to live. livekit's default ICE
// watchdog is a bit longer (30 s), but for broadcast the viewer is
// already seeing a static mix frame; a shorter window keeps the "mix
// → live" fall-back snappy without being twitchy on a transient
// hiccup.
const mixIceDisconnectGrace = 5 * time.Second

// mixSource consumes the audio + video producers created by a WHIP
// ingest session (the "avflow pushed mix") via a DirectTransport on
// the same router, turns the RTP into depacketized samples + raw RTP,
// and delivers both to the switcher.
//
// Lifecycle: attached by Broadcast.AttachMix (POST /rtc/broadcast/{id}
// /whip), torn down either by DetachMix (explicit DELETE) or by the
// ICE watchdog on irrecoverable disconnects. The watchdog is driven
// by a supervise goroutine that exits once Close() is called.
type mixSource struct {
	logger *slog.Logger

	// transport is the WHIP WebRtcTransport that ingested the mix.
	// Owned by the Broadcast once attached; closed on Detach/Close.
	transport *mediasoup.Transport
	// directTransport carries the Consumer pair that feeds our
	// depacketizers. A dedicated DirectTransport in the same router
	// is required to get raw RTP via Consumer.OnRtp.
	directTransport *mediasoup.Transport
	audioCons       *mediasoup.Consumer
	videoCons       *mediasoup.Consumer

	// Depacketizers run on the consumer goroutines so there is no
	// locking needed around them — each is single-producer.
	// videoDepack is codec-specific (H264/VP9/AV1); chosen at
	// construction based on the video producer's RTP capabilities
	// so live and mix can run on different codecs if needed.
	videoDepack videoDepacketizer
	audioDepack OpusDepacketizer

	// videoCodec snapshots the negotiated mix video codec so the
	// Broadcast can hand it to each destinationPusher on flip-to-
	// mix without re-deriving from the producer's RTP parameters
	// every time. Set once in newMixSource; never mutated.
	videoCodec Codec

	// onReady is fired exactly once, after the first video keyframe
	// has been seen AND ICE is confirmed connected. The switcher
	// uses it to mark mix ready.
	onceReady sync.Once
	onReady   func()

	// onGone is fired exactly once when the ICE watchdog decides
	// mix is dead (grace elapsed) or the consumer detects a
	// producer close. Broadcast uses it to flip back to live.
	onceGone sync.Once
	onGone   func(reason string)

	// onIceState mirrors the transport ICE handler so Broadcast.Status
	// can publish the current state without reaching into mixSource.
	onIceState func(state mediasoup.IceState)

	sampleSink sampleSink
	packetSink packetSink

	mu             sync.Mutex
	disconnectedAt time.Time
	iceConnected   bool
	stopCh         chan struct{}
	watchdogDone   chan struct{}
	closed         bool
}

// mixSourceParams groups the inputs for newMixSource. AudioProducer /
// VideoProducer must come from the same router as the WHIP transport.
type mixSourceParams struct {
	Transport     *mediasoup.Transport
	Router        *mediasoup.Router
	AudioProducer *mediasoup.Producer
	VideoProducer *mediasoup.Producer
	SampleSink    sampleSink
	PacketSink    packetSink
	OnReady       func()
	OnGone        func(reason string)
	// OnIceState, when set, is invoked synchronously from the
	// mediasoup event goroutine on every transport ICE transition.
	// Broadcast uses it to populate Status().MixIceState so HTTP
	// consumers can observe the live state of the mix ingest.
	OnIceState func(state mediasoup.IceState)
	Logger     *slog.Logger
}

// newMixSource wires the DirectTransport, consumers and ICE watchdog.
// On failure the caller is responsible for closing the WHIP transport;
// we close only the resources we successfully created here.
func newMixSource(p mixSourceParams) (*mixSource, error) {
	if p.Transport == nil || p.Router == nil || p.AudioProducer == nil || p.VideoProducer == nil {
		return nil, fmt.Errorf("mixsource: missing transport/router/producers")
	}
	if p.Logger == nil {
		p.Logger = slog.Default().With("component", "broadcast.mixSource")
	}

	dt, err := p.Router.CreateDirectTransport(&mediasoup.DirectTransportOptions{})
	if err != nil {
		return nil, fmt.Errorf("mixsource direct transport: %w", err)
	}

	caps := p.Router.RtpCapabilities()

	audioCons, err := dt.Consume(&mediasoup.ConsumerOptions{
		ProducerId:      p.AudioProducer.Id(),
		RtpCapabilities: caps,
		Paused:          true,
	})
	if err != nil {
		dt.Close()
		return nil, fmt.Errorf("mixsource consume audio: %w", err)
	}
	videoCons, err := dt.Consume(&mediasoup.ConsumerOptions{
		ProducerId:      p.VideoProducer.Id(),
		RtpCapabilities: caps,
		Paused:          true,
	})
	if err != nil {
		audioCons.Close()
		dt.Close()
		return nil, fmt.Errorf("mixsource consume video: %w", err)
	}

	videoCodec := producerVideoCodec(p.VideoProducer)
	depack, err := newVideoDepacketizer(videoCodec)
	if err != nil {
		videoCons.Close()
		audioCons.Close()
		dt.Close()
		return nil, fmt.Errorf("mixsource depacketizer: %w", err)
	}

	ms := &mixSource{
		logger:          p.Logger,
		transport:       p.Transport,
		directTransport: dt,
		audioCons:       audioCons,
		videoCons:       videoCons,
		videoDepack:     depack,
		videoCodec:      videoCodec,
		sampleSink:      p.SampleSink,
		packetSink:      p.PacketSink,
		onReady:         p.OnReady,
		onGone:          p.OnGone,
		onIceState:      p.OnIceState,
		stopCh:          make(chan struct{}),
		watchdogDone:    make(chan struct{}),
	}

	audioCons.OnRtp(ms.handleAudio)
	videoCons.OnRtp(ms.handleVideo)

	audioCons.OnProducerClose(func(ctx context.Context) { ms.fireGone("mix audio producer closed") })
	videoCons.OnProducerClose(func(ctx context.Context) { ms.fireGone("mix video producer closed") })

	// Hook transport ICE state for the mix watchdog. mediasoup does
	// not emit a dedicated "ICE failed" event — livekit-style the
	// watchdog tears the source down after the grace window if we
	// do not recover.
	p.Transport.OnIceStateChange(ms.handleIceState)

	// Ask the mix publisher for an immediate keyframe so the switch
	// over happens as soon as possible. Non-fatal if it fails
	// (avflow will soon emit one anyway).
	if err := videoCons.RequestKeyFrame(); err != nil {
		ms.logger.Debug("mix RequestKeyFrame failed", "error", err)
	}

	// Consumers start paused; Resume after ICE connected so we do
	// not buffer RTP arriving before the switcher has a chance to
	// observe state. The watchdog Resume path handles this.
	go ms.supervise()

	return ms, nil
}

// handleIceState is the single point of truth for the mix transport's
// ICE. Connected/Completed → resume consumers, fire onReady as soon as
// the first IDR also arrives. Disconnected → arm the watchdog. Closed
// → fire onGone immediately.
func (ms *mixSource) handleIceState(state mediasoup.IceState) {
	if ms.onIceState != nil {
		ms.onIceState(state)
	}
	ms.mu.Lock()
	switch state {
	case mediasoup.IceStateConnected, mediasoup.IceStateCompleted:
		ms.disconnectedAt = time.Time{}
		first := !ms.iceConnected
		ms.iceConnected = true
		ms.mu.Unlock()
		if first {
			ms.logger.Info("mix ICE connected, resuming consumers")
			if err := ms.audioCons.Resume(); err != nil {
				ms.logger.Debug("mix audio resume failed", "error", err)
			}
			if err := ms.videoCons.Resume(); err != nil {
				ms.logger.Debug("mix video resume failed", "error", err)
			}
		}
	case mediasoup.IceStateDisconnected:
		if ms.disconnectedAt.IsZero() {
			ms.disconnectedAt = time.Now()
			ms.logger.Warn("mix ICE disconnected; watchdog armed", "grace", mixIceDisconnectGrace)
		}
		ms.mu.Unlock()
	case mediasoup.IceStateClosed:
		ms.mu.Unlock()
		ms.fireGone("mix ICE closed")
	default:
		ms.mu.Unlock()
	}
}

// supervise runs the ICE watchdog. Wakes every second, fires onGone
// once `disconnectedAt` has been set for >= mixIceDisconnectGrace. A
// Close() call closes stopCh and the goroutine exits immediately on
// the next select without waiting for the tick.
func (ms *mixSource) supervise() {
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
			if time.Since(disconnected) >= mixIceDisconnectGrace {
				ms.fireGone("mix ICE watchdog expired")
				return
			}
		}
	}
}

// handleAudio unmarshals the audio RTP, delivers the raw packet for
// WHIP egress, and depacketizes for RTMP egress.
func (ms *mixSource) handleAudio(data []byte) {
	pkt := &rtp.Packet{}
	if err := pkt.Unmarshal(data); err != nil {
		return
	}
	if ms.packetSink != nil {
		ms.packetSink(SampleKindAudio, pkt)
	}
	if s, ok := ms.audioDepack.Push(pkt); ok && ms.sampleSink != nil {
		ms.sampleSink(s)
	}
}

// handleVideo handles the video RTP. In addition to the same fan-out
// as handleAudio, it inspects depacketized keyframes so we can fire
// onReady the first time mix has delivered one — which is what the
// switcher needs before flipping over.
func (ms *mixSource) handleVideo(data []byte) {
	pkt := &rtp.Packet{}
	if err := pkt.Unmarshal(data); err != nil {
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

// fireReady is idempotent. First call invokes onReady; subsequent calls
// are no-ops so the switcher's mixReady flag is only set once.
func (ms *mixSource) fireReady() {
	ms.onceReady.Do(func() {
		if ms.onReady != nil {
			ms.onReady()
		}
	})
}

// fireGone is idempotent. First call invokes onGone and then closes
// the source. Subsequent calls are no-ops.
func (ms *mixSource) fireGone(reason string) {
	ms.onceGone.Do(func() {
		ms.logger.Warn("mix source going away", "reason", reason)
		if ms.onGone != nil {
			ms.onGone(reason)
		}
	})
}

// VideoDepack exposes the depacketizer so the switcher can pull the
// codec configuration record (AVCC for H.264, vpcC for VP9, av1C for
// AV1) it needs to emit a fresh RTMP seq header on every mix → (mix
// or live) flip.
func (ms *mixSource) VideoDepack() videoDepacketizer { return ms.videoDepack }

// VideoCodec returns the codec the mix WHIP session negotiated for its
// video producer. Used by Broadcast.onFlipToMix to tell each destination
// pusher the codec the next batch of mix samples will be encoded with;
// WHIP destinations whose RTCPeerConnection was negotiated against a
// different codec re-handshake their session, RTMP destinations ignore
// the value (they re-derive the FourCC from each keyframe's avcC/hvcC).
func (ms *mixSource) VideoCodec() Codec { return ms.videoCodec }

// RestartIce implements mixIngress.RestartIce by delegating to the
// underlying mediasoup WebRtcTransport, which generates fresh ICE
// credentials and returns the new IceUfrag (used as the WHIP ETag).
// The pion bypass implements the same interface in source_mix_h265.go.
func (ms *mixSource) RestartIce() (string, error) {
	if ms.transport == nil {
		return "", fmt.Errorf("mixsource: transport already closed")
	}
	iceParams, err := ms.transport.RestartIce()
	if err != nil {
		return "", err
	}
	return iceParams.UsernameFragment, nil
}

// Close tears down consumers, DirectTransport and WHIP transport.
// Safe to call concurrently with the watchdog goroutine; the flags
// ensure exactly-once teardown. The WHIP transport close also tears
// down the producers below it.
func (ms *mixSource) Close() {
	ms.mu.Lock()
	if ms.closed {
		ms.mu.Unlock()
		return
	}
	ms.closed = true
	close(ms.stopCh)
	ms.mu.Unlock()

	// Wait for supervise() to actually exit so later Close calls
	// can't double-close the transports while the watchdog is mid-
	// fireGone.
	<-ms.watchdogDone

	if ms.directTransport != nil {
		ms.directTransport.Close()
	}
	if ms.transport != nil {
		ms.transport.Close()
	}
}
