package server

import (
	"fmt"
	"log/slog"

	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/rs/xid"

	"github.com/axonkit/axonkit/pkg/auth"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/rtc"
)

// This file hosts ingest-protocol-agnostic participant attach helpers
// shared by WHIP, RTMP, and any future push ingest flow. Keep it free
// of WHIP-specific code; protocol-specific glue lives in whip.go or
// rtmp.go.

// TransportKind enumerates the per-ingest binding between a freshly
// created mediasoup Transport and a Participant's transport slots.
// WHIP publishes through a WebRtcTransport in the "send" slot, while
// RTMP injects RTP through a DirectTransport in the "direct" slot.
type TransportKind string

const (
	TransportKindSend   TransportKind = "send"
	TransportKindDirect TransportKind = "direct"
)

// AttachPublishParticipantOptions describes a ready-to-attach ingest flow.
// Both WHIP (CreateWHIPResult) and RTMP (DirectTransport-based) populate
// one and hand it to HTTPServer.AttachPublishParticipant.
type AttachPublishParticipantOptions struct {
	// SidPrefix is the prefix used to mint the Participant.Sid (e.g.
	// "whip_" for WHIP, "rtmp_" for RTMP). Keep them disjoint so
	// operators can grep for the ingest kind in logs.
	SidPrefix string

	// RoomName is the target room; must be non-empty. The room is
	// created on demand when it does not yet exist.
	RoomName string

	// Claims carries Identity/Name/Grants for the participant. Identity
	// must be non-empty; when Name is empty the Identity is reused so
	// downstream UIs have something to render.
	Claims *auth.Claims

	// Router hosts the Producers that will be attached to the participant.
	Router *mediasoup.Router

	// Transport is the transport that owns the producers (WebRtcTransport
	// for WHIP, DirectTransport for RTMP). It is assigned via
	// SetTransports(send, recv=nil, direct=nil) for WHIP; RTMP assigns
	// via SetTransports(nil, nil, direct) instead so that
	// Participant.Close() tears down the DirectTransport.
	Transport *mediasoup.Transport

	// TransportKind selects which slot of SetTransports the Transport
	// goes into. TransportKindSend matches WHIP; TransportKindDirect
	// matches RTMP ingest.
	TransportKind TransportKind

	// Producers are the already-created, router-local producers to
	// register onto the participant. Source hints let the UI pick the
	// correct HTML element per track.
	Producers []AttachedProducer

	// AutoJoin, when true, promotes the participant to the JOINED state
	// immediately after it is added to the room. RTMP and SIP ingests
	// must set this because they do not have an ICE watchdog to trigger
	// the promotion on the first IceStateConnected event; WHIP leaves
	// it false and relies on the ICE watchdog (see onIceSourceUp).
	AutoJoin bool
}

// AttachedProducer binds a producer to a TrackSource hint so
// RegisterIngestProducer can tag the track for downstream subscribers.
type AttachedProducer struct {
	Producer *mediasoup.Producer
	Source   proto.TrackSource
}

// AttachPublishParticipant wires a fresh Participant onto a live transport
// + producers and joins it into the target room. Shared by WHIP and RTMP
// ingest so their participant bookkeeping stays identical.
//
// On success, the caller should NOT close the transport; the Participant
// now owns the transport's lifecycle (close cascades into producer close).
// On error the caller is responsible for tearing down the Transport
// (see HTTPServer.createWHIPParticipant for the pattern).
func (s *HTTPServer) AttachPublishParticipant(opts AttachPublishParticipantOptions) (*rtc.Participant, error) {
	if s.roomManager == nil {
		return nil, fmt.Errorf("room manager not configured")
	}
	if opts.RoomName == "" {
		return nil, fmt.Errorf("room name required")
	}
	if opts.Claims == nil {
		return nil, fmt.Errorf("auth claims required")
	}
	if opts.Router == nil || opts.Transport == nil {
		return nil, fmt.Errorf("router and transport required")
	}

	roomCfg := s.cfg.Room
	room := s.roomManager.GetOrCreateRoom(opts.RoomName, roomCfg.MaxParticipants, "")
	if room == nil {
		return nil, fmt.Errorf("room %q unavailable", opts.RoomName)
	}

	identity := opts.Claims.Identity()
	if identity == "" {
		identity = opts.Claims.Name
	}
	if identity == "" {
		return nil, fmt.Errorf("participant identity required")
	}
	name := opts.Claims.Name
	if name == "" {
		name = identity
	}

	sidPrefix := opts.SidPrefix
	if sidPrefix == "" {
		sidPrefix = "ingest_"
	}

	participant := rtc.NewParticipant(rtc.ParticipantParams{
		Sid:           sidPrefix + xid.New().String(),
		Identity:      identity,
		Name:          name,
		Grants:        opts.Claims.Grants,
		Router:        opts.Router,
		TokenVerifier: s.verifier,
		NodeId:        string(s.node.ID),
	})

	switch opts.TransportKind {
	case TransportKindDirect:
		participant.SetTransports(nil, nil, opts.Transport)
	default: // TransportKindSend and empty both mean WHIP-style
		participant.SetTransports(opts.Transport, nil, nil)
	}
	// Headless signaling: neither WHIP nor RTMP have a datachannel.
	// SetSignal with all nils latches signalHeadless so broadcast fan-out
	// silently drops messages instead of logging "signal channel not
	// established" per update, and messageLoop is never started for this
	// participant.
	participant.SetSignal(nil, nil, nil, nil)

	if err := room.AddParticipant(participant); err != nil {
		return nil, fmt.Errorf("add participant: %w", err)
	}

	for _, ap := range opts.Producers {
		if ap.Producer == nil {
			continue
		}
		if err := participant.RegisterIngestProducer(ap.Source, ap.Producer); err != nil {
			slog.Warn("ingest register producer failed", "err", err, "producerId", ap.Producer.Id(), "kind", ap.Producer.Kind())
			continue
		}
	}

	// RTMP / SIP / other non-ICE ingests need an explicit JOINED
	// transition — without it the setup watchdog (setupTimeout = 30s)
	// decides the publisher never finished handshake and closes the
	// participant, which in turn tears down the producers we just
	// published.
	if opts.AutoJoin {
		participant.SetState(proto.ParticipantInfo_JOINED)
	}
	return participant, nil
}
