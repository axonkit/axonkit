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

	video MediaLeg
	audio MediaLeg

	// videoCodec is the codec negotiated on the first SequenceStart.
	// Subsequent SequenceStart tags with a different FourCC are dropped
	// because mediasoup refuses to re-negotiate a running Producer —
	// publishers that need to change codec must reconnect.
	videoCodec VideoCodec
	videoPk    VideoPacketizer

	audioPk  *OpusPacketizer
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

	// videoEmptyPacketize counts NALU tags for which pion returned an
	// empty packet slice (i.e. the bitstream was unparseable). Surfaced
	// in a periodic WARN log so silently-rejected inputs are visible
	// without flooding stdout.
	videoEmptyPacketize uint64

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
// video leg on the first SeqHeader and forwards coded frames as RTP.
// Dispatch is codec-agnostic: session.videoPk is a VideoPacketizer
// interface set to the concrete implementation (H264/VP9/AV1) selected
// when the first SequenceStart arrived.
func (s *Session) IngestVideo(ctx context.Context, timestampMs uint32, tag *VideoTag) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("session closed")
	}

	switch tag.PacketType {
	case VideoPacketSeqHeader:
		return s.handleVideoSeqHeader(ctx, tag)
	case VideoPacketNALU:
		if s.video.Producer == nil {
			// Arrived before seq header (rare but possible): drop silently;
			// the next keyframe after seq header will reset decoding.
			return nil
		}
		if s.videoCodec != VideoCodecUnknown && tag.Codec != VideoCodecUnknown && tag.Codec != s.videoCodec {
			// Mid-stream codec swap inside a single tag stream would
			// corrupt the already-running producer. Drop the frame and
			// wait for the publisher to either reconnect or return to
			// the negotiated codec.
			s.logger.Warn("RTMP video codec swap dropped",
				"negotiated", s.videoCodec.String(), "incoming", tag.Codec.String())
			return nil
		}
		s.trackStartTs(&s.video, timestampMs)
		pts90k := convertMsToClock(timestampMs-s.video.StartTimestampMs, 90000)
		packets, err := s.videoPk.Packetize(pts90k, tag.IsKeyFrame, tag.Data)
		if err != nil {
			return fmt.Errorf("packetize %s: %w", s.videoCodec, err)
		}
		if len(packets) == 0 {
			// pion's VP9/AV1 payloaders can silently return an empty
			// slice when they reject the bitstream. Count the drops so
			// malformed inputs surface in logs instead of showing the
			// subscriber a black frame with no explanation.
			s.videoEmptyPacketize++
			if s.videoEmptyPacketize <= 3 || s.videoEmptyPacketize%1000 == 0 {
				s.logger.Warn("rtmp video packetize produced no packets",
					"codec", s.videoCodec.String(),
					"isKey", tag.IsKeyFrame,
					"payloadBytes", len(tag.Data),
					"totalEmpty", s.videoEmptyPacketize)
			}
			return nil
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

// handleVideoSeqHeader reacts to a SequenceStart FLV tag. On the first
// SeqHeader it creates the DirectTransport + Producer + packetizer for
// the codec indicated by the tag. Subsequent SeqHeaders for the same
// codec refresh per-codec state (H.264's SPS/PPS cache); a SeqHeader
// announcing a *different* codec is dropped because producers cannot be
// re-negotiated once created.
func (s *Session) handleVideoSeqHeader(ctx context.Context, tag *VideoTag) error {
	if tag.Codec == VideoCodecUnknown {
		return fmt.Errorf("video seq header without codec")
	}
	if s.video.Producer == nil {
		return s.setupVideoLeg(ctx, tag.Codec, tag.Data)
	}
	if tag.Codec != s.videoCodec {
		s.logger.Warn("RTMP video seq header codec swap ignored",
			"negotiated", s.videoCodec.String(), "incoming", tag.Codec.String())
		return nil
	}
	// Refresh codec-specific state. Only H.264 has per-SeqHeader mutable
	// state worth caching (SPS/PPS for STAP-A injection on keyframes);
	// VP9 / AV1 carry parameter sets inside the bitstream and the
	// packetizer holds nothing to update.
	if pk, ok := s.videoPk.(*H264Packetizer); ok {
		rec, err := ParseAVCDecoderConfigurationRecord(tag.Data)
		if err != nil {
			return fmt.Errorf("avcC refresh: %w", err)
		}
		pk.SetParameterSets(rec.SPS[0], rec.PPS[0])
	}
	return nil
}

// Payload types the RTMP ingest uses when building Producer parameters.
// They only need to match within the session's RtpParameters; mediasoup
// re-maps PTs when forwarding to consumers, so these values do not
// collide with whatever the WHIP path negotiates.
const (
	videoH264PT uint8 = 125
	videoVP9PT  uint8 = 98
	videoAV1PT  uint8 = 99
)

// setupVideoLeg dispatches on the negotiated codec to build the
// DirectTransport + Producer + packetizer for one video leg. It is
// invoked exactly once per session on the first SequenceStart and keeps
// the locking contract of IngestVideo (caller already holds s.mu).
func (s *Session) setupVideoLeg(ctx context.Context, codec VideoCodec, cfg []byte) error {
	t, err := s.router.CreateDirectTransportContext(ctx, &mediasoup.DirectTransportOptions{})
	if err != nil {
		return fmt.Errorf("create direct transport (video): %w", err)
	}
	ssrc := randomSSRC()

	var (
		rtpParams *mediasoup.RtpParameters
		pk        VideoPacketizer
		label     []any
	)
	switch codec {
	case VideoCodecH264:
		rec, perr := ParseAVCDecoderConfigurationRecord(cfg)
		if perr != nil {
			t.Close()
			return fmt.Errorf("avcC: %w", perr)
		}
		rtpParams = buildH264RtpParameters(ssrc, videoH264PT, rec)
		h264 := NewH264Packetizer(ssrc, videoH264PT)
		h264.SetParameterSets(rec.SPS[0], rec.PPS[0])
		pk = h264
		label = []any{"codec", "h264", "profileLevelId", rec.ProfileLevelID()}
	case VideoCodecVP9:
		rec, perr := ParseVPCodecConfigurationRecord(cfg)
		if perr != nil {
			t.Close()
			return fmt.Errorf("vpcC: %w", perr)
		}
		rtpParams = buildVP9RtpParameters(ssrc, videoVP9PT, rec)
		pk = NewVP9Packetizer(ssrc, videoVP9PT)
		label = []any{"codec", "vp9", "profileId", rec.Profile}
	case VideoCodecAV1:
		rec, perr := ParseAV1CodecConfigurationRecord(cfg)
		if perr != nil {
			t.Close()
			return fmt.Errorf("av1C: %w", perr)
		}
		rtpParams = buildAV1RtpParameters(ssrc, videoAV1PT, rec)
		pk = NewAV1Packetizer(ssrc, videoAV1PT)
		label = []any{"codec", "av1", "profile", rec.SeqProfile, "level", rec.SeqLevelIdx0}
	default:
		t.Close()
		return fmt.Errorf("unsupported video codec %s", codec)
	}

	producer, err := t.ProduceContext(ctx, &mediasoup.ProducerOptions{
		Kind:          mediasoup.MediaKindVideo,
		RtpParameters: rtpParams,
	})
	if err != nil {
		t.Close()
		return fmt.Errorf("produce %s: %w", codec, err)
	}
	s.video.Transport = t
	s.video.Producer = producer
	s.video.SSRC = ssrc
	s.videoCodec = codec
	s.videoPk = pk

	s.logger.Info("RTMP video leg ready", append([]any{"producerId", producer.Id()}, label...)...)
	s.notifyNewProducer(&s.video, mediasoup.MediaKindVideo)
	return nil
}

// buildH264RtpParameters emits the RtpParameters mediasoup needs for a
// video/H264 Producer. ProfileLevelId must match an entry the Router
// declared in caps.go, otherwise mediasoup rejects the Producer with
// "no supported matching codec".
func buildH264RtpParameters(ssrc uint32, pt uint8, rec *AVCDecoderConfigurationRecord) *mediasoup.RtpParameters {
	return &mediasoup.RtpParameters{
		Codecs: []*mediasoup.RtpCodecParameters{{
			MimeType:    "video/H264",
			PayloadType: pt,
			ClockRate:   90000,
			Parameters: mediasoup.RtpCodecSpecificParameters{
				PacketizationMode:     1,
				ProfileLevelId:        rec.ProfileLevelID(),
				LevelAsymmetryAllowed: 1,
			},
			RtcpFeedback: videoRtcpFeedback(),
		}},
		Encodings: []*mediasoup.RtpEncodingParameters{{Ssrc: ssrc}},
		Rtcp:      &mediasoup.RtcpParameters{Cname: "rtmp-" + randomCName()},
	}
}

// buildVP9RtpParameters emits video/VP9 Producer parameters. We only
// forward profile 0 or 2 (the two entries caps.go advertises); any
// other profile is clamped to 0 because the router would otherwise
// refuse to create the producer.
func buildVP9RtpParameters(ssrc uint32, pt uint8, rec *VPCodecConfigurationRecord) *mediasoup.RtpParameters {
	profile := uint32(rec.Profile)
	if profile != 0 && profile != 2 {
		profile = 0
	}
	return &mediasoup.RtpParameters{
		Codecs: []*mediasoup.RtpCodecParameters{{
			MimeType:    "video/VP9",
			PayloadType: pt,
			ClockRate:   90000,
			Parameters: mediasoup.RtpCodecSpecificParameters{
				ProfileId: profile,
			},
			RtcpFeedback: videoRtcpFeedback(),
		}},
		Encodings: []*mediasoup.RtpEncodingParameters{{Ssrc: ssrc}},
		Rtcp:      &mediasoup.RtcpParameters{Cname: "rtmp-" + randomCName()},
	}
}

// buildAV1RtpParameters emits video/AV1 Producer parameters. mediasoup's
// video/AV1 capability is single-profile, so we surface seq_profile via
// RtpCodecSpecificParameters.ProfileId (shared between VP9 and AV1 in
// mediasoup-go v2) for logging and SDP faithfulness; level / tier
// currently have no typed field in the library so we omit them.
func buildAV1RtpParameters(ssrc uint32, pt uint8, rec *AV1CodecConfigurationRecord) *mediasoup.RtpParameters {
	return &mediasoup.RtpParameters{
		Codecs: []*mediasoup.RtpCodecParameters{{
			MimeType:    "video/AV1",
			PayloadType: pt,
			ClockRate:   90000,
			Parameters: mediasoup.RtpCodecSpecificParameters{
				ProfileId: uint32(rec.SeqProfile),
			},
			RtcpFeedback: videoRtcpFeedback(),
		}},
		Encodings: []*mediasoup.RtpEncodingParameters{{Ssrc: ssrc}},
		Rtcp:      &mediasoup.RtcpParameters{Cname: "rtmp-" + randomCName()},
	}
}

// videoRtcpFeedback is the RtcpFeedback set every video leg announces.
// Keeping it centralized means VP9 / AV1 inherit the same NACK/PLI/FIR
// behavior H.264 has been tested with.
func videoRtcpFeedback() []*mediasoup.RtcpFeedback {
	return []*mediasoup.RtcpFeedback{
		{Type: "nack"},
		{Type: "nack", Parameter: "pli"},
		{Type: "ccm", Parameter: "fir"},
		{Type: "goog-remb"},
		{Type: "transport-cc"},
	}
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
