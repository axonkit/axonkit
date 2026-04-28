package broadcast

import (
	"log/slog"
	"testing"

	"github.com/pion/rtp"
)

// fakeSink captures emitted samples + packets so tests can assert on
// what the switcher forwarded. All writes happen on the caller's
// goroutine; no synchronization needed.
type fakeSink struct {
	samples []MediaSample
	packets []struct {
		kind SampleKind
		ts   uint32
	}
}

func (f *fakeSink) emitSample(s MediaSample, extradata []byte) {
	f.samples = append(f.samples, s)
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

// TestSwitcher_FlipSharesAVAnchor reproduces the A/V desync bug we
// hit on verify-rtmp.sh's mid-stream live→mix flip: when the audio
// ingest pipeline runs ahead of the video ingest pipeline (audio
// hwm > video hwm, typical because B-frame reorder + ffmpeg's
// encoder buildup adds 1–2 s to video while audio stays near
// real-time), each per-kind rebaser used to anchor the post-flip
// source against ITS OWN hwm. The mix's first audio sample then
// landed ~1.5 s after the mix's first video sample, even though
// they came in back-to-back from the mix ingest.
//
// After the fix the audio rebaser's hwm should be propagated into
// the video rebaser at flip time (and vice versa), so the trailing
// kind catches up to the leading kind and both first-after-flip
// samples emerge with output PTS that line up in milliseconds.
func TestSwitcher_FlipSharesAVAnchor(t *testing.T) {
	sw, sink := newTestSwitcher()

	// Drive the live audio rebaser ~5 s into the future. 5 s @
	// 48 kHz = 240 000 ticks; we feed two consecutive Opus packets
	// so the hwm advances past 5 s.
	sw.feedSample(SourceLive, MediaSample{Kind: SampleKindAudio, Codec: CodecOpus, RTPTimestamp: 1_000_000})
	sw.feedSample(SourceLive, MediaSample{Kind: SampleKindAudio, Codec: CodecOpus, RTPTimestamp: 1_240_000})
	audioOutHWM := sink.samples[len(sink.samples)-1].RTPTimestamp
	if audioOutHWM == 0 {
		t.Fatalf("setup: live audio hwm did not advance, got %d", audioOutHWM)
	}
	audioOutMs := uint32(int64(audioOutHWM) * 1000 / int64(audioClockHz))

	// Drive the live video rebaser ~3.9 s into the future — this
	// is the verify-rtmp.sh shape where video lags audio by
	// ~1.1 s due to B-frame reorder + encoder buildup. 3.9 s @
	// 90 kHz = 351 000 ticks.
	sw.feedSample(SourceLive, MediaSample{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 50_000_000, IsKeyFrame: true})
	sw.feedSample(SourceLive, MediaSample{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 50_351_000})
	videoOutHWM := sink.samples[len(sink.samples)-1].RTPTimestamp
	videoOutMs := uint32(int64(videoOutHWM) * 1000 / int64(videoClockHz))
	if audioOutMs <= videoOutMs+500 {
		t.Fatalf("setup: expected audio leading video by >500 ms, got audio=%dms video=%dms", audioOutMs, videoOutMs)
	}

	// Mix arrives, switcher flips live → mix on the first mix
	// keyframe.
	sw.markMixReady()
	sw.feedSample(SourceMix, MediaSample{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 90_000_000, IsKeyFrame: true})
	if sw.activeSource() != SourceMix {
		t.Fatalf("expected mix-active after keyframe flip")
	}
	mixVideoFirstTS := sink.samples[len(sink.samples)-1].RTPTimestamp
	mixVideoFirstMs := uint32(int64(mixVideoFirstTS) * 1000 / int64(videoClockHz))

	// First mix audio sample arrives next.
	sw.feedSample(SourceMix, MediaSample{Kind: SampleKindAudio, Codec: CodecOpus, RTPTimestamp: 7_000_000})
	mixAudioFirstTS := sink.samples[len(sink.samples)-1].RTPTimestamp
	mixAudioFirstMs := uint32(int64(mixAudioFirstTS) * 1000 / int64(audioClockHz))

	// Pre-fix: mix audio first frame would have been audioHWM +
	// audioCatchup (~5020ms) and mix video first frame would have
	// been videoHWM + videoCatchup (~3933ms) — a 1+ second skew.
	// Post-fix the trailing video rebaser is rolled up to audio's
	// hwm before catchup, so both first-after-flip frames land
	// within one audio + one video tick of audio's hwm.
	const tolMs = 50 // covers the 20ms audio + 33ms video catchup spread
	skewMs := int64(mixAudioFirstMs) - int64(mixVideoFirstMs)
	if skewMs < 0 {
		skewMs = -skewMs
	}
	if skewMs > tolMs {
		t.Errorf("post-flip A/V anchor skew = %dms (audio=%dms video=%dms), want ≤ %dms",
			skewMs, mixAudioFirstMs, mixVideoFirstMs, tolMs)
	}

	// And neither kind should have rolled BACKWARDS past its own
	// pre-flip hwm — a buggy implementation that pinned both to
	// the trailing kind's hwm would do exactly that on audio.
	if int32(mixAudioFirstTS-audioOutHWM) <= 0 {
		t.Errorf("mix audio first TS %d not strictly past pre-flip audio hwm %d",
			mixAudioFirstTS, audioOutHWM)
	}
	if int32(mixVideoFirstTS-videoOutHWM) <= 0 {
		t.Errorf("mix video first TS %d not strictly past pre-flip video hwm %d",
			mixVideoFirstTS, videoOutHWM)
	}
}

// TestSwitcher_FlipBackPreservesLeadingKind covers the symmetric
// case: when video LEADS audio (e.g. mix had only video), the
// shared anchor must roll audio FORWARD without rolling video back.
func TestSwitcher_FlipBackPreservesLeadingKind(t *testing.T) {
	sw, sink := newTestSwitcher()
	sw.markMixReady()

	// Mix sends only video, hwm grows in video clock domain
	// without touching audio. After the flip back to live, an
	// incoming audio sample should be aligned to video's
	// (millisecond) anchor so it doesn't replay 1+ second behind.
	sw.feedSample(SourceMix, MediaSample{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 100_000, IsKeyFrame: true})
	sw.feedSample(SourceMix, MediaSample{Kind: SampleKindVideo, Codec: CodecH264, RTPTimestamp: 460_000}) // ~4 s later
	videoHWM := sink.samples[len(sink.samples)-1].RTPTimestamp
	videoHWMMs := uint32(int64(videoHWM) * 1000 / int64(videoClockHz))

	sw.flipToLive("test")

	// First live audio sample after the flip-back. Its output
	// timestamp converted to ms must land near videoHWMMs (within
	// one audio catchup tick = 20 ms), NOT at 0+catchup which is
	// what an unaligned audio rebaser would emit because the
	// audio rebaser was never used during the mix segment.
	sw.feedSample(SourceLive, MediaSample{Kind: SampleKindAudio, Codec: CodecOpus, RTPTimestamp: 999_999})
	audioFirstTS := sink.samples[len(sink.samples)-1].RTPTimestamp
	audioFirstMs := uint32(int64(audioFirstTS) * 1000 / int64(audioClockHz))

	// Aligned audio should be inside [videoHWMMs, videoHWMMs+50ms].
	skewMs := int64(audioFirstMs) - int64(videoHWMMs)
	if skewMs < 0 || skewMs > 50 {
		t.Errorf("flip-back audio anchor = %dms, video hwm = %dms (skew=%dms), want skew in [0,50]",
			audioFirstMs, videoHWMMs, skewMs)
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
