package broadcast

import (
	"log/slog"
	"testing"

	"github.com/pion/rtp"
)

// fakeSink captures emitted samples + packets so tests can assert on
// what the switcher forwarded. All writes happen on the caller's
// goroutine; no synchronisation needed.
type fakeSink struct {
	samples []MediaSample
	packets []struct {
		kind SampleKind
		ts   uint32
	}
	lastExtradataLen int
}

func (f *fakeSink) emitSample(s MediaSample, extradata []byte) {
	f.samples = append(f.samples, s)
	f.lastExtradataLen = len(extradata)
}
func (f *fakeSink) emitPacket(kind SampleKind, pkt *rtp.Packet) {
	f.packets = append(f.packets, struct {
		kind SampleKind
		ts   uint32
	}{kind, pkt.Timestamp})
}

// newTestSwitcher builds a switcher wired to a fakeSink, with both
// extradata accessors returning a deterministic blob so tests can tell
// live from mix on the wire. live-active at construction, as in the
// real broadcast.
func newTestSwitcher() (*switcher, *fakeSink) {
	sink := &fakeSink{}
	sw := newSwitcher(slog.Default())
	sw.bindEmitters(sink.emitSample, sink.emitPacket)
	sw.bindLive(func() []byte { return []byte("LIVEAVCC") })
	sw.bindMix(func() []byte { return []byte("MIXAVCC") })
	return sw, sink
}

func TestSwitcher_LiveOnly_TimestampsRebasedFromZero(t *testing.T) {
	sw, sink := newTestSwitcher()

	// First live audio sample should land at output-TS 0, regardless
	// of the arbitrary source-TS.
	sw.feedSample(SourceLive, MediaSample{
		Kind:         SampleKindAudio,
		Codec:        CodecOpus,
		RTPTimestamp: 123456,
	})
	// +960 ticks of Opus
	sw.feedSample(SourceLive, MediaSample{
		Kind:         SampleKindAudio,
		Codec:        CodecOpus,
		RTPTimestamp: 124416,
	})

	if len(sink.samples) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(sink.samples))
	}
	if sink.samples[0].RTPTimestamp != 0 {
		t.Errorf("first sample TS = %d, want 0", sink.samples[0].RTPTimestamp)
	}
	if sink.samples[1].RTPTimestamp != 960 {
		t.Errorf("second sample TS = %d, want 960", sink.samples[1].RTPTimestamp)
	}
}

func TestSwitcher_DropsMixSamplesUntilReady(t *testing.T) {
	sw, sink := newTestSwitcher()

	// Mix samples before markMixReady must be dropped so we don't
	// interleave two streams.
	sw.feedSample(SourceMix, MediaSample{
		Kind:         SampleKindAudio,
		Codec:        CodecOpus,
		RTPTimestamp: 10_000,
	})
	sw.feedSample(SourceMix, MediaSample{
		Kind:         SampleKindVideo,
		Codec:        CodecH264,
		RTPTimestamp: 10_000,
		IsKeyFrame:   true,
	})

	if len(sink.samples) != 0 {
		t.Fatalf("expected 0 emitted samples, got %d", len(sink.samples))
	}
}

func TestSwitcher_FlipOnMixKeyframe(t *testing.T) {
	sw, sink := newTestSwitcher()

	// Prime the live stream so we have a non-zero output clock.
	sw.feedSample(SourceLive, MediaSample{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 1000, IsKeyFrame: true})
	sw.feedSample(SourceLive, MediaSample{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 4000})
	lastLiveTS := sink.samples[len(sink.samples)-1].RTPTimestamp

	// Mix becomes ready; a non-key mix frame should still be held
	// back (we wait for a keyframe boundary).
	sw.markMixReady()
	sw.feedSample(SourceMix, MediaSample{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 50_000})
	if sw.activeSource() != SourceLive {
		t.Fatalf("should still be on live after non-key mix frame")
	}

	// Now a mix keyframe: switcher flips to mix.
	sw.feedSample(SourceMix, MediaSample{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 50_000, IsKeyFrame: true})
	if sw.activeSource() != SourceMix {
		t.Fatalf("should have flipped to mix on keyframe")
	}
	// The flip sample itself should have been emitted with a TS
	// that strictly advances past the last live-emitted TS.
	got := sink.samples[len(sink.samples)-1].RTPTimestamp
	if got <= lastLiveTS {
		t.Errorf("post-flip TS %d must strictly advance past live TS %d", got, lastLiveTS)
	}

	// Subsequent live samples should be dropped.
	prev := len(sink.samples)
	sw.feedSample(SourceLive, MediaSample{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 5000})
	if len(sink.samples) != prev {
		t.Errorf("live samples should be dropped after flip; got %d new", len(sink.samples)-prev)
	}
}

func TestSwitcher_FlipBackToLive(t *testing.T) {
	sw, sink := newTestSwitcher()
	sw.markMixReady()
	sw.feedSample(SourceMix, MediaSample{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 100_000, IsKeyFrame: true})
	sw.feedSample(SourceMix, MediaSample{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 103_000})
	if sw.activeSource() != SourceMix {
		t.Fatalf("switcher should be on mix after first keyframe")
	}

	// Simulate avflow DELETE: external caller flips us back.
	sw.flipToLive("test delete")
	if sw.activeSource() != SourceLive {
		t.Fatalf("expected live after flipToLive")
	}

	// First live sample after flip keeps the output clock
	// monotonic (+video catchup of 3000).
	prev := sink.samples[len(sink.samples)-1].RTPTimestamp
	sw.feedSample(SourceLive, MediaSample{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 900, IsKeyFrame: true})
	got := sink.samples[len(sink.samples)-1].RTPTimestamp
	if got != prev+defaultVideoTick {
		t.Errorf("post-flipback TS = %d, want %d (prev=%d + tick)", got, prev+defaultVideoTick, prev)
	}
}

func TestSwitcher_PacketTimestampsRewritten(t *testing.T) {
	sw, sink := newTestSwitcher()

	// Push two live audio RTP packets. Output TS must be rebased
	// against the first packet (→ 0, 960).
	sw.feedPacket(SourceLive, SampleKindAudio, &rtp.Packet{
		Header: rtp.Header{Timestamp: 777_000},
	})
	sw.feedPacket(SourceLive, SampleKindAudio, &rtp.Packet{
		Header: rtp.Header{Timestamp: 777_960},
	})

	if len(sink.packets) != 2 {
		t.Fatalf("want 2 emitted packets, got %d", len(sink.packets))
	}
	if sink.packets[0].ts != 0 || sink.packets[1].ts != 960 {
		t.Errorf("packet TS sequence = %d,%d want 0,960",
			sink.packets[0].ts, sink.packets[1].ts)
	}
}
