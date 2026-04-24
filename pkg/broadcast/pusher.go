package broadcast

import (
	"context"

	"github.com/pion/rtp"
)

// destinationPusher is the interface Broadcast talks to regardless of
// whether the outbound sink is RTMP or WHIP. Implementations:
//
//   - rtmpPusher — needs depacketized samples + extradata handshakes.
//   - whipPusher — needs raw RTP for pion's TrackLocalStaticRTP.
//
// Broadcast fans every sample + every packet out to every pusher; each
// pusher picks the subset it cares about (see the two adapters below).
type destinationPusher interface {
	Start(ctx context.Context)
	Stop()
	BytesOut() uint64
	Connected() bool
	LastError() string

	// WriteSample delivers a depacketized access unit. RTMP pushers
	// mux it into an FLV tag and forward; WHIP pushers ignore it.
	WriteSample(s MediaSample, extradata []byte)
	// WriteRTP delivers a raw RTP packet. WHIP pushers forward into
	// pion's TrackLocalStaticRTP; RTMP pushers ignore it.
	WriteRTP(kind SampleKind, pkt *rtp.Packet)

	// ResetVideoHeader asks the pusher to re-emit the video codec
	// sequence header on its next keyframe. The switcher calls this
	// on every source flip so the new source's SPS/PPS reach the
	// receiver before its first IDR. WHIP pushers (raw RTP) are
	// no-ops.
	ResetVideoHeader()
}

// rtmpPusherAdapter wraps rtmpPusher to match destinationPusher. It
// lives as a separate type so the inner pusher stays agnostic of the
// "extradata" plumbing the adapter needs to trigger the one-shot seq
// header emission.
type rtmpPusherAdapter struct {
	inner *rtmpPusher
}

func (a *rtmpPusherAdapter) Start(ctx context.Context) { a.inner.Start(ctx) }
func (a *rtmpPusherAdapter) Stop()                     { a.inner.Stop() }
func (a *rtmpPusherAdapter) BytesOut() uint64          { return a.inner.BytesOut() }
func (a *rtmpPusherAdapter) Connected() bool           { return a.inner.Connected() }
func (a *rtmpPusherAdapter) LastError() string         { return a.inner.LastError() }

// WriteSample gates on the "need seq header" flags held by rtmpPusher.
// For video it only emits on the first key frame once the codec
// configuration record is available; for audio it fires
// unconditionally on the first frame. The sample's Codec field picks
// between the classic AVC FLV tag and the E-RTMP "ExVideoTagHeader"
// variant used for VP9 / AV1.
func (a *rtmpPusherAdapter) WriteSample(s MediaSample, extradata []byte) {
	switch s.Kind {
	case SampleKindVideo:
		if !a.inner.videoHdrSent.Load() {
			if !s.IsKeyFrame || len(extradata) == 0 {
				return
			}
			switch s.Codec {
			case CodecH264:
				a.inner.EnsureVideoSeqHeader(extradata)
			case CodecVP9:
				a.inner.EnsureExVideoSeqHeader(flvExVideoFourCCVP9, extradata)
			case CodecAV1:
				a.inner.EnsureExVideoSeqHeader(flvExVideoFourCCAV1, extradata)
			default:
				return
			}
		}
	case SampleKindAudio:
		a.inner.EnsureAudioSeqHeader(2)
	}
	a.inner.WriteSample(s)
}

func (a *rtmpPusherAdapter) WriteRTP(SampleKind, *rtp.Packet) {}

func (a *rtmpPusherAdapter) ResetVideoHeader() { a.inner.ResetVideoHeader() }

// whipPusherAdapter wraps whipPusher to match destinationPusher.
type whipPusherAdapter struct {
	inner *whipPusher
}

func (a *whipPusherAdapter) Start(ctx context.Context) { a.inner.Start(ctx) }
func (a *whipPusherAdapter) Stop()                     { a.inner.Stop() }
func (a *whipPusherAdapter) BytesOut() uint64          { return a.inner.BytesOut() }
func (a *whipPusherAdapter) Connected() bool           { return a.inner.Connected() }
func (a *whipPusherAdapter) LastError() string         { return a.inner.LastError() }

func (a *whipPusherAdapter) WriteSample(MediaSample, []byte) {}

func (a *whipPusherAdapter) WriteRTP(kind SampleKind, pkt *rtp.Packet) {
	a.inner.WriteRTP(kind, pkt)
}

// ResetVideoHeader is a no-op for WHIP: raw RTP passthrough does not
// carry a codec sequence header. SPS/PPS arrive in-band on keyframes.
func (a *whipPusherAdapter) ResetVideoHeader() {}
