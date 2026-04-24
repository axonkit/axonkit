package rtmp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jiyeyuran/mediasoup-go/v2"
)

// MediaLeg owns the DirectTransport + Producer for one kind of media
// (audio or video). Legs are lazily created on the first payload tag of
// that kind so publishers that only send audio or only send video do not
// pay for an unused Producer slot.
//
// All fields are accessed under the session's mutex or during setup; the
// RTMP library single-threads OnAudio/OnVideo callbacks so the producer
// state machine itself does not need extra locking.
type MediaLeg struct {
	Transport *mediasoup.Transport
	Producer  *mediasoup.Producer
	SSRC      uint32
	// StartTimestampMs is the first FLV timestamp observed for this leg.
	// The session subtracts it before converting to RTP so the RTP clock
	// starts near zero (mediasoup tolerates arbitrary start values but
	// consumers get healthier NTP mappings with small offsets).
	StartTimestampMs uint32
	StartSeen        bool
}

// SessionParams configures the runtime state shared between the RTMP
// handler callbacks and the media legs.
type SessionParams struct {
	// Router hosts the DirectTransport for each leg. All legs on a session
	// share the same Router so the associated Participant's PipeProducerTo
	// cache picks them up verbatim.
	Router *mediasoup.Router

	// Logger receives per-connection diagnostics tagged with stream key,
	// room, and identity (filled in once the publisher authenticates).
	Logger *slog.Logger

	// OnFirstProducer is invoked exactly once after either audio or video
	// produces the first Producer. Callers use this to register the first
	// track onto the room-side Participant before the second leg arrives.
	// When nil, the session skips the hook (tests).
	OnFirstProducer func(leg *MediaLeg, kind mediasoup.MediaKind)

	// OnAdditionalProducer is invoked when a second Producer is created
	// (e.g. audio after video) so the server can attach the new track to
	// the existing Participant.
	OnAdditionalProducer func(leg *MediaLeg, kind mediasoup.MediaKind)
}

// Session owns the per-publisher state for one active RTMP connection.
// It is created by the Handler after OnPublish succeeds and torn down
// when the RTMP connection closes.
type Session struct {
	mu sync.Mutex

	router *mediasoup.Router
	logger *slog.Logger

	video    MediaLeg
	audio    MediaLeg
	videoPk  *H264Packetizer
	audioPk  *OpusPacketizer
	cfgRec   *AVCDecoderConfigurationRecord
	opusHead *OpusHead

	onFirst      func(leg *MediaLeg, kind mediasoup.MediaKind)
	onAdditional func(leg *MediaLeg, kind mediasoup.MediaKind)

	// closeConn closes the underlying RTMP TCP connection. The handler
	// populates it right after OnServe so the server can force the
	// publisher to disconnect when the participant leaves the room.
	closeConn func()

	// firstKindEmitted is the MediaKind of the first producer attached via
	// OnFirstProducer. Subsequent producers fire OnAdditionalProducer.
	firstKindEmitted mediasoup.MediaKind

	closed bool
}

// NewSession constructs a Session. The Router must already exist; the
// session creates DirectTransports lazily as media arrives.
func NewSession(p SessionParams) *Session {
	logger := p.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Session{
		router:       p.Router,
		logger:       logger,
		onFirst:      p.OnFirstProducer,
		onAdditional: p.OnAdditionalProducer,
	}
}

// SetCallbacks installs the first/additional-producer callbacks after the
// session has been constructed. The rtmp Handler creates a Session
// before it can know the server-side attach strategy, so the server
// invokes this from within its OnSessionReady hook. Calling after the
// first producer has already been emitted is a no-op for OnFirstProducer
// but still installs OnAdditionalProducer for later tags.
func (s *Session) SetCallbacks(first, additional func(leg *MediaLeg, kind mediasoup.MediaKind)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onFirst = first
	s.onAdditional = additional
}

// SetConnCloser registers the function the handler should invoke to
// tear down the RTMP TCP connection. Called by the handler once the
// go-rtmp connection is known; the server then uses CloseConn to force
// the publisher to disconnect when the participant is evicted.
func (s *Session) SetConnCloser(closer func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeConn = closer
}

// CloseConn closes the underlying TCP connection if a closer has been
// registered. It is safe to call multiple times; the registered closer
// (go-rtmp's Conn.Close) is itself idempotent.
func (s *Session) CloseConn() {
	s.mu.Lock()
	closer := s.closeConn
	s.mu.Unlock()
	if closer != nil {
		closer()
	}
}

// Close tears down all producers and transports owned by this session.
// Safe to call multiple times from different paths (RTMP close + outer
// shutdown race).
func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	if s.video.Producer != nil {
		_ = s.video.Producer.Close()
	}
	if s.audio.Producer != nil {
		_ = s.audio.Producer.Close()
	}
	if s.video.Transport != nil {
		s.video.Transport.Close()
	}
	if s.audio.Transport != nil {
		s.audio.Transport.Close()
	}
}

// Router exposes the mediasoup router attached to this session so
// callers (e.g. the server attach helper) can thread it through when
// creating the participant.
func (s *Session) Router() *mediasoup.Router { return s.router }

// VideoTransport returns the DirectTransport bound to the video leg (or
// nil if video has not started yet). Exposed so the server can pass the
// transport to Participant.SetTransports for lifecycle management.
func (s *Session) VideoTransport() *mediasoup.Transport { return s.video.Transport }

// AudioTransport returns the DirectTransport bound to the audio leg (nil
// before first audio tag).
func (s *Session) AudioTransport() *mediasoup.Transport { return s.audio.Transport }

// VideoProducer / AudioProducer expose the created producers after the
// legs are established.
func (s *Session) VideoProducer() *mediasoup.Producer { return s.video.Producer }
func (s *Session) AudioProducer() *mediasoup.Producer { return s.audio.Producer }

// IngestVideo handles one parsed FLV video tag. Lazy-initializes the
// video leg on the first SeqHeader and forwards NALU frames as RTP.
func (s *Session) IngestVideo(ctx context.Context, timestampMs uint32, tag *VideoTag) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("session closed")
	}

	switch tag.PacketType {
	case VideoPacketSeqHeader:
		return s.handleVideoSeqHeader(ctx, tag.Data)
	case VideoPacketNALU:
		if s.video.Producer == nil {
			// Arrived before seq header (rare but possible): drop silently;
			// the next keyframe after seq header will reset decoding.
			return nil
		}
		s.trackStartTs(&s.video, timestampMs)
		pts90k := convertMsToClock(timestampMs-s.video.StartTimestampMs, 90000)
		packets, err := s.videoPk.PacketizeAVCC(pts90k, tag.IsKeyFrame, tag.Data)
		if err != nil {
			return fmt.Errorf("packetize h264: %w", err)
		}
		for _, p := range packets {
			if err := s.video.Producer.SendContext(ctx, p); err != nil {
				return fmt.Errorf("producer.Send: %w", err)
			}
		}
		return nil
	case VideoPacketEndOfSeq:
		// Publisher signaled end-of-stream; wait for session close to
		// tear down so mid-stream EOS does not kill the connection.
		return nil
	}
	return nil
}

// IngestAudio handles one parsed FLV audio tag.
func (s *Session) IngestAudio(ctx context.Context, timestampMs uint32, tag *AudioTag) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("session closed")
	}

	switch tag.PacketType {
	case AudioPacketOpusHeader:
		// Parse OpusHead so we can pick up channel count for
		// RtpCodecParameters.Channels.
		oh, err := ParseOpusHead(tag.Data)
		if err != nil {
			return fmt.Errorf("opus head: %w", err)
		}
		s.opusHead = oh
		if tag.ChannelCount == 0 {
			tag.ChannelCount = oh.ChannelCount
		}
		return nil
	case AudioPacketOpusFrame:
		if s.audio.Producer == nil {
			channels := tag.ChannelCount
			if channels == 0 {
				channels = 2
			}
			if err := s.setupAudioLeg(ctx, channels); err != nil {
				return err
			}
		}
		s.trackStartTs(&s.audio, timestampMs)
		ts48k := convertMsToClock(timestampMs-s.audio.StartTimestampMs, 48000)
		pkt, err := s.audioPk.PacketizeFrame(ts48k, tag.Data)
		if err != nil {
			return fmt.Errorf("packetize opus: %w", err)
		}
		if err := s.audio.Producer.SendContext(ctx, pkt); err != nil {
			return fmt.Errorf("producer.Send: %w", err)
		}
		return nil
	}
	return nil
}

func (s *Session) handleVideoSeqHeader(ctx context.Context, data []byte) error {
	rec, err := ParseAVCDecoderConfigurationRecord(data)
	if err != nil {
		return fmt.Errorf("avcC: %w", err)
	}
	s.cfgRec = rec
	if s.video.Producer == nil {
		return s.setupVideoLeg(ctx, rec)
	}
	// Config can re-arrive mid-stream (re-configuration). We update the
	// SPS/PPS cache so subsequent keyframes carry the refreshed ones but
	// keep the producer alive to avoid tearing down room subscriptions.
	s.videoPk.SetParameterSets(rec.SPS[0], rec.PPS[0])
	return nil
}

func (s *Session) setupVideoLeg(ctx context.Context, rec *AVCDecoderConfigurationRecord) error {
	t, err := s.router.CreateDirectTransportContext(ctx, &mediasoup.DirectTransportOptions{})
	if err != nil {
		return fmt.Errorf("create direct transport (video): %w", err)
	}
	ssrc := randomSSRC()
	const payloadType uint8 = 125
	rtpParams := &mediasoup.RtpParameters{
		Codecs: []*mediasoup.RtpCodecParameters{{
			MimeType:    "video/H264",
			PayloadType: payloadType,
			ClockRate:   90000,
			Parameters: mediasoup.RtpCodecSpecificParameters{
				PacketizationMode:     1,
				ProfileLevelId:        rec.ProfileLevelID(),
				LevelAsymmetryAllowed: 1,
			},
			RtcpFeedback: []*mediasoup.RtcpFeedback{
				{Type: "nack"},
				{Type: "nack", Parameter: "pli"},
				{Type: "ccm", Parameter: "fir"},
				{Type: "goog-remb"},
				{Type: "transport-cc"},
			},
		}},
		Encodings: []*mediasoup.RtpEncodingParameters{{Ssrc: ssrc}},
		Rtcp:      &mediasoup.RtcpParameters{Cname: "rtmp-" + randomCName()},
	}
	producer, err := t.ProduceContext(ctx, &mediasoup.ProducerOptions{
		Kind:          mediasoup.MediaKindVideo,
		RtpParameters: rtpParams,
	})
	if err != nil {
		t.Close()
		return fmt.Errorf("produce h264: %w", err)
	}
	s.video.Transport = t
	s.video.Producer = producer
	s.video.SSRC = ssrc
	s.videoPk = NewH264Packetizer(ssrc, payloadType)
	s.videoPk.SetParameterSets(rec.SPS[0], rec.PPS[0])

	s.logger.Info("RTMP video leg ready",
		"producerId", producer.Id(), "profileLevelId", rec.ProfileLevelID())
	s.notifyNewProducer(&s.video, mediasoup.MediaKindVideo)
	return nil
}

func (s *Session) setupAudioLeg(ctx context.Context, channels uint8) error {
	t, err := s.router.CreateDirectTransportContext(ctx, &mediasoup.DirectTransportOptions{})
	if err != nil {
		return fmt.Errorf("create direct transport (audio): %w", err)
	}
	ssrc := randomSSRC()
	const payloadType uint8 = 111
	rtpParams := &mediasoup.RtpParameters{
		Codecs: []*mediasoup.RtpCodecParameters{{
			MimeType:    "audio/opus",
			PayloadType: payloadType,
			ClockRate:   48000,
			Channels:    channels,
			Parameters: mediasoup.RtpCodecSpecificParameters{
				Useinbandfec: 1,
			},
			RtcpFeedback: []*mediasoup.RtcpFeedback{
				{Type: "transport-cc"},
			},
		}},
		Encodings: []*mediasoup.RtpEncodingParameters{{Ssrc: ssrc}},
		Rtcp:      &mediasoup.RtcpParameters{Cname: "rtmp-" + randomCName()},
	}
	producer, err := t.ProduceContext(ctx, &mediasoup.ProducerOptions{
		Kind:          mediasoup.MediaKindAudio,
		RtpParameters: rtpParams,
	})
	if err != nil {
		t.Close()
		return fmt.Errorf("produce opus: %w", err)
	}
	s.audio.Transport = t
	s.audio.Producer = producer
	s.audio.SSRC = ssrc
	s.audioPk = NewOpusPacketizer(ssrc, payloadType)

	s.logger.Info("RTMP audio leg ready",
		"producerId", producer.Id(), "channels", channels)
	s.notifyNewProducer(&s.audio, mediasoup.MediaKindAudio)
	return nil
}

// trackStartTs records the first timestamp for a leg so subsequent
// conversions are anchored at zero. FLV timestamps typically start at 0
// anyway but DJI/OBS publishers sometimes begin mid-stream.
func (s *Session) trackStartTs(leg *MediaLeg, ts uint32) {
	if leg.StartSeen {
		return
	}
	leg.StartTimestampMs = ts
	leg.StartSeen = true
}

// notifyNewProducer invokes OnFirstProducer on the very first producer
// and OnAdditionalProducer on later ones. This two-phase callback lets
// the server attach the participant once and then register additional
// tracks inline.
func (s *Session) notifyNewProducer(leg *MediaLeg, kind mediasoup.MediaKind) {
	if s.firstKindEmitted == "" {
		s.firstKindEmitted = kind
		if s.onFirst != nil {
			s.onFirst(leg, kind)
		}
		return
	}
	if s.onAdditional != nil {
		s.onAdditional(leg, kind)
	}
}

// convertMsToClock converts a millisecond duration to the given RTP clock
// tick count with rounding. Overflow wraps modulo 2^32 like a normal RTP
// timestamp.
func convertMsToClock(ms, clock uint32) uint32 {
	// (ms * clock) / 1000. Use uint64 to avoid overflow for clock=90000
	// and multi-hour sessions (90000 * ~47 hours = 2^32 ms).
	return uint32((uint64(ms) * uint64(clock)) / 1000)
}

// randomSSRC returns a non-zero random 32-bit identifier. Producers on a
// DirectTransport still need SSRC values that are unique within the
// router namespace; failure to randomize causes mediasoup to drop
// duplicates from a second publisher.
func randomSSRC() uint32 {
	var buf [4]byte
	_, _ = rand.Read(buf[:])
	v := binary.BigEndian.Uint32(buf[:])
	if v == 0 {
		v = uint32(time.Now().UnixNano())
	}
	return v
}

// randomCName returns a short unique cname suffix.
func randomCName() string {
	var buf [6]byte
	_, _ = rand.Read(buf[:])
	return fmt.Sprintf("%x", buf)
}
