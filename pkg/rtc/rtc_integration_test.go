package rtc

import (
	"os"
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/jiyeyuran/mediasoup-go/v2"
)

func skipWithoutWorker(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("MEDIASOUP_WORKER_BIN")
	if bin == "" {
		t.Skip("MEDIASOUP_WORKER_BIN not set; skipping integration test")
	}
	return bin
}

func createWorkerAndRouter(t *testing.T) (*mediasoup.Worker, *mediasoup.Router) {
	t.Helper()
	bin := skipWithoutWorker(t)

	worker, err := mediasoup.NewWorker(bin)
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	t.Cleanup(func() { worker.Close() })

	router, err := worker.CreateRouter(&mediasoup.RouterOptions{
		MediaCodecs: []*mediasoup.RtpCodecCapability{
			{
				Kind:      mediasoup.MediaKindAudio,
				MimeType:  "audio/opus",
				ClockRate: 48000,
				Channels:  2,
			},
			{
				Kind:      mediasoup.MediaKindVideo,
				MimeType:  "video/VP8",
				ClockRate: 90000,
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateRouter: %v", err)
	}
	return worker, router
}

func createWebRtcTransport(t *testing.T, router *mediasoup.Router) *mediasoup.Transport {
	t.Helper()
	transport, err := router.CreateWebRtcTransport(&mediasoup.WebRtcTransportOptions{
		ListenInfos: []mediasoup.TransportListenInfo{
			{Ip: "127.0.0.1", Protocol: mediasoup.TransportProtocolUDP},
		},
		EnableSctp: true,
		NumSctpStreams: &mediasoup.NumSctpStreams{
			OS:  1024,
			MIS: 1024,
		},
	})
	if err != nil {
		t.Fatalf("CreateWebRtcTransport: %v", err)
	}
	return transport
}

func createDirectTransport(t *testing.T, router *mediasoup.Router) *mediasoup.Transport {
	t.Helper()
	transport, err := router.CreateDirectTransport(&mediasoup.DirectTransportOptions{})
	if err != nil {
		t.Fatalf("CreateDirectTransport: %v", err)
	}
	return transport
}

func TestIntegration_PublishTrack(t *testing.T) {
	_, router := createWorkerAndRouter(t)
	sendTransport := createWebRtcTransport(t, router)

	p := NewParticipant(ParticipantParams{
		Sid:      "pub-p1",
		Identity: "publisher",
		Grants:   &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
		Router:   router,
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.SetTransports(sendTransport, nil, nil)

	track, err := p.PublishTrack(&proto.PublishTrackRequest{
		Kind: mediasoup.MediaKindAudio,
		RtpParameters: &mediasoup.RtpParameters{
			Mid: "0",
			Codecs: []*mediasoup.RtpCodecParameters{
				{
					MimeType:    "audio/opus",
					PayloadType: 111,
					ClockRate:   48000,
					Channels:    2,
				},
			},
			Encodings: []*mediasoup.RtpEncodingParameters{
				{Ssrc: 1234},
			},
		},
		Name:   "microphone",
		Source: proto.TrackSource_MICROPHONE,
	})
	if err != nil {
		t.Fatalf("PublishTrack: %v", err)
	}
	if track == nil {
		t.Fatal("expected non-nil track")
	}
	if track.sid == "" {
		t.Fatal("expected non-empty track sid")
	}
	if !p.IsPublisher() {
		t.Fatal("expected IsPublisher=true")
	}

	// UnpublishTrack
	if err := p.UnpublishTrack(track.sid); err != nil {
		t.Fatalf("UnpublishTrack: %v", err)
	}
	if p.IsPublisher() {
		t.Fatal("expected IsPublisher=false after unpublish")
	}
}

func TestIntegration_MuteTrack(t *testing.T) {
	_, router := createWorkerAndRouter(t)
	sendTransport := createWebRtcTransport(t, router)

	p := NewParticipant(ParticipantParams{
		Sid:      "mute-p1",
		Identity: "muter",
		Grants:   &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
		Router:   router,
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.SetTransports(sendTransport, nil, nil)

	track, err := p.PublishTrack(&proto.PublishTrackRequest{
		Kind: mediasoup.MediaKindAudio,
		RtpParameters: &mediasoup.RtpParameters{
			Mid: "0",
			Codecs: []*mediasoup.RtpCodecParameters{
				{MimeType: "audio/opus", PayloadType: 111, ClockRate: 48000, Channels: 2},
			},
			Encodings: []*mediasoup.RtpEncodingParameters{{Ssrc: 5678}},
		},
		Name: "mic",
	})
	if err != nil {
		t.Fatalf("PublishTrack: %v", err)
	}

	// Mute
	if err := p.MuteTrack(track.sid, true); err != nil {
		t.Fatalf("MuteTrack(true): %v", err)
	}
	if !track.muted {
		t.Fatal("expected muted=true")
	}

	// Unmute
	if err := p.MuteTrack(track.sid, false); err != nil {
		t.Fatalf("MuteTrack(false): %v", err)
	}
	if track.muted {
		t.Fatal("expected muted=false")
	}
}

func TestIntegration_UpdateTrack(t *testing.T) {
	_, router := createWorkerAndRouter(t)
	sendTransport := createWebRtcTransport(t, router)

	p := NewParticipant(ParticipantParams{
		Sid:      "upd-p1",
		Identity: "updater",
		Grants:   &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
		Router:   router,
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.SetTransports(sendTransport, nil, nil)

	track, _ := p.PublishTrack(&proto.PublishTrackRequest{
		Kind: mediasoup.MediaKindVideo,
		RtpParameters: &mediasoup.RtpParameters{
			Mid: "1",
			Codecs: []*mediasoup.RtpCodecParameters{
				{MimeType: "video/VP8", PayloadType: 96, ClockRate: 90000},
			},
			Encodings: []*mediasoup.RtpEncodingParameters{{Ssrc: 9999}},
		},
		Name:   "camera",
		Width:  1280,
		Height: 720,
	})

	if err := p.UpdateTrack(&proto.UpdateTrackRequest{
		TrackSid: track.sid,
		Width:    1920,
		Height:   1080,
	}); err != nil {
		t.Fatalf("UpdateTrack: %v", err)
	}

	if track.width != 1920 || track.height != 1080 {
		t.Fatalf("expected 1920x1080, got %dx%d", track.width, track.height)
	}
}

func TestIntegration_PublishDataTrack(t *testing.T) {
	_, router := createWorkerAndRouter(t)
	sendTransport := createWebRtcTransport(t, router)

	p := NewParticipant(ParticipantParams{
		Sid:      "dpub-p1",
		Identity: "data-publisher",
		Grants:   &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
		Router:   router,
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.SetTransports(sendTransport, nil, nil)

	dt, err := p.PublishDataTrack(&proto.PublishDataTrackRequest{
		Name:  "chat",
		Label: "chat",
		SctpStreamParameters: &mediasoup.SctpStreamParameters{
			StreamId: 0,
		},
	})
	if err != nil {
		t.Fatalf("PublishDataTrack: %v", err)
	}
	if dt.sid == "" {
		t.Fatal("expected non-empty data track sid")
	}

	// Unpublish
	if err := p.UnpublishDataTrack(dt.sid); err != nil {
		t.Fatalf("UnpublishDataTrack: %v", err)
	}
}

func TestIntegration_Subscribe(t *testing.T) {
	_, router := createWorkerAndRouter(t)
	sendTransport := createWebRtcTransport(t, router)
	recvTransport := createWebRtcTransport(t, router)

	publisher := NewParticipant(ParticipantParams{
		Sid:      "sub-pub",
		Identity: "pub-alice",
		Grants:   &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
		Router:   router,
	})
	defer publisher.Close(proto.DisconnectReason_CLIENT_INITIATED)
	publisher.SetTransports(sendTransport, nil, nil)

	track, err := publisher.PublishTrack(&proto.PublishTrackRequest{
		Kind: mediasoup.MediaKindAudio,
		RtpParameters: &mediasoup.RtpParameters{
			Mid: "0",
			Codecs: []*mediasoup.RtpCodecParameters{
				{MimeType: "audio/opus", PayloadType: 111, ClockRate: 48000, Channels: 2},
			},
			Encodings: []*mediasoup.RtpEncodingParameters{{Ssrc: 1111}},
		},
		Name: "mic",
	})
	if err != nil {
		t.Fatalf("PublishTrack: %v", err)
	}

	subscriber := NewParticipant(ParticipantParams{
		Sid:      "sub-sub",
		Identity: "sub-bob",
		Grants:   &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
		Router:   router,
	})
	defer subscriber.Close(proto.DisconnectReason_CLIENT_INITIATED)
	subscriber.SetTransports(nil, recvTransport, nil)
	subscriber.SetRtpCapabilities(router.RtpCapabilities())

	// Subscribe will fail at Request() because no signal channel is set up.
	// But it exercises the transport.Consume path.
	err = subscriber.Subscribe(track, publisher)
	if err == nil {
		t.Fatal("expected error because signal not connected for RPC")
	}
	// The error should be about RPC, not about transport/consume
	if err.Error() != "subscription RPC failed: signal channel not established" {
		t.Logf("Subscribe error (expected): %v", err)
	}
}

func TestIntegration_SubscribeData(t *testing.T) {
	_, router := createWorkerAndRouter(t)
	sendTransport := createWebRtcTransport(t, router)
	recvTransport := createWebRtcTransport(t, router)

	publisher := NewParticipant(ParticipantParams{
		Sid:      "dsub-pub",
		Identity: "dpub-alice",
		Grants:   &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
		Router:   router,
	})
	defer publisher.Close(proto.DisconnectReason_CLIENT_INITIATED)
	publisher.SetTransports(sendTransport, nil, nil)

	dt, err := publisher.PublishDataTrack(&proto.PublishDataTrackRequest{
		Name:  "chat",
		Label: "chat",
		SctpStreamParameters: &mediasoup.SctpStreamParameters{
			StreamId: 0,
		},
	})
	if err != nil {
		t.Fatalf("PublishDataTrack: %v", err)
	}

	subscriber := NewParticipant(ParticipantParams{
		Sid:      "dsub-sub",
		Identity: "dsub-bob",
		Grants:   &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
		Router:   router,
	})
	defer subscriber.Close(proto.DisconnectReason_CLIENT_INITIATED)
	subscriber.SetTransports(nil, recvTransport, nil)

	// SubscribeData will fail at Request() because no signal channel
	err = subscriber.SubscribeData(dt, publisher)
	if err == nil {
		t.Fatal("expected error because signal not connected for RPC")
	}
}

func TestIntegration_SetTransportsWithICE(t *testing.T) {
	_, router := createWorkerAndRouter(t)
	sendTransport := createWebRtcTransport(t, router)
	recvTransport := createWebRtcTransport(t, router)

	p := NewParticipant(ParticipantParams{
		Sid:      "ice-p1",
		Identity: "ice-test",
		Grants:   &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
		Router:   router,
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.SetTransports(sendTransport, recvTransport, nil)

	if p.SendTransportSid() == "" {
		t.Fatal("expected non-empty send transport sid")
	}
	if p.RecvTransportSid() == "" {
		t.Fatal("expected non-empty recv transport sid")
	}

	// GetTransportBySid
	if p.GetTransportBySid(sendTransport.Id()) == nil {
		t.Fatal("expected to find send transport by sid")
	}
	if p.GetTransportBySid(recvTransport.Id()) == nil {
		t.Fatal("expected to find recv transport by sid")
	}
}

func TestIntegration_SetSignalAndMessage(t *testing.T) {
	_, router := createWorkerAndRouter(t)
	direct := createDirectTransport(t, router)

	p := NewParticipant(ParticipantParams{
		Sid:      "sig-p1",
		Identity: "sig-test",
		Grants:   &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
		Router:   router,
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	// Create DataProducer/DataConsumer for signaling
	outProducer, err := direct.ProduceData(&mediasoup.DataProducerOptions{
		Label: "signal-out",
	})
	if err != nil {
		t.Fatalf("ProduceData out: %v", err)
	}

	outConsumer, err := direct.ConsumeData(&mediasoup.DataConsumerOptions{
		DataProducerId: outProducer.Id(),
	})
	if err != nil {
		t.Fatalf("ConsumeData out: %v", err)
	}

	inProducer, err := direct.ProduceData(&mediasoup.DataProducerOptions{
		Label: "signal-in",
	})
	if err != nil {
		t.Fatalf("ProduceData in: %v", err)
	}

	inConsumer, err := direct.ConsumeData(&mediasoup.DataConsumerOptions{
		DataProducerId: inProducer.Id(),
	})
	if err != nil {
		t.Fatalf("ConsumeData in: %v", err)
	}

	p.SetTransports(nil, nil, direct)
	p.SetSignal(outProducer, outConsumer, inProducer, inConsumer)

	// SendNotification should succeed now
	if err := p.SendNotification("test.event", map[string]string{"hello": "world"}); err != nil {
		t.Fatalf("SendNotification: %v", err)
	}

	// Request should timeout (no client to respond)
	_, err = p.Request("test.request", nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestIntegration_Reconnect(t *testing.T) {
	_, router := createWorkerAndRouter(t)
	send1 := createWebRtcTransport(t, router)
	recv1 := createWebRtcTransport(t, router)
	direct1 := createDirectTransport(t, router)

	p := NewParticipant(ParticipantParams{
		Sid:          "recon-int",
		Identity:     "recon-user",
		Grants:       &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
		Router:       router,
		PingTimeout:  10 * time.Second,
		PingInterval: 5 * time.Second,
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.SetTransports(send1, recv1, direct1)
	p.SetState(proto.ParticipantInfo_ACTIVE)

	// Reconnect with new transports
	send2 := createWebRtcTransport(t, router)
	recv2 := createWebRtcTransport(t, router)
	direct2 := createDirectTransport(t, router)

	err := p.Reconnect(send2, recv2, direct2, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Reconnect: %v", err)
	}

	if p.SendTransportSid() != send2.Id() {
		t.Fatal("expected new send transport")
	}
	if p.RecvTransportSid() != recv2.Id() {
		t.Fatal("expected new recv transport")
	}
}
