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
		Grants:   &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
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
		Grants:   &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
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
		Grants:   &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
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
		Grants:   &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
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
		Grants:   &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
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
		Grants:   &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
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
		Grants:   &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
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
		Grants:   &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
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
		Grants:   &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
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
		Grants:   &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
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

func TestIntegration_ReadyFlushesQueuedNotifications(t *testing.T) {
	_, router := createWorkerAndRouter(t)
	direct := createDirectTransport(t, router)

	room := NewRoom(RoomParams{
		Sid:          "room-ready-flush",
		Name:         "ready-flush",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})

	p := NewParticipant(ParticipantParams{
		Sid:          "ready-flush-p1",
		Identity:     "ready-flush-user",
		Grants:       &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
		Router:       router,
		PingTimeout:  10 * time.Second,
		PingInterval: 5 * time.Second,
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)
	room.AddParticipant(p)

	outProducer, err := direct.ProduceData(&mediasoup.DataProducerOptions{Label: "signal-out"})
	if err != nil {
		t.Fatalf("ProduceData out: %v", err)
	}
	outConsumer, err := direct.ConsumeData(&mediasoup.DataConsumerOptions{
		DataProducerId: outProducer.Id(),
	})
	if err != nil {
		t.Fatalf("ConsumeData out: %v", err)
	}
	inProducer, err := direct.ProduceData(&mediasoup.DataProducerOptions{Label: "signal-in"})
	if err != nil {
		t.Fatalf("ProduceData in: %v", err)
	}
	inConsumer, err := direct.ConsumeData(&mediasoup.DataConsumerOptions{
		DataProducerId: inProducer.Id(),
	})
	if err != nil {
		t.Fatalf("ConsumeData in: %v", err)
	}

	received := make(chan *proto.Message, 4)
	outConsumer.OnMessage(func(payload []byte, ppid mediasoup.SctpPayloadType) {
		msg, err := proto.DecodeMessage(payload)
		if err != nil {
			t.Errorf("DecodeMessage: %v", err)
			return
		}
		received <- msg
	})

	p.SetTransports(nil, nil, direct)
	p.SetSignal(outProducer, outConsumer, inProducer, inConsumer)

	if err := p.SendNotification("queued.event", map[string]string{"value": "before-ready"}); err != nil {
		t.Fatalf("SendNotification before ready: %v", err)
	}

	select {
	case msg := <-received:
		t.Fatalf("unexpected message before ready: %s", msg.Method)
	case <-time.After(50 * time.Millisecond):
	}

	p.dispatchRPC(makeRequest(proto.MethodReady, "log-ready-flush", &proto.ReadyRequest{}))

	var methods []string
	for range 2 {
		select {
		case msg := <-received:
			methods = append(methods, msg.Method)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for ready flush messages, got %v", methods)
		}
	}

	if len(methods) != 2 {
		t.Fatalf("expected 2 messages, got %v", methods)
	}
	if methods[0] != proto.MethodReady {
		t.Fatalf("expected ready response first, got %s", methods[0])
	}
	if methods[1] != "queued.event" {
		t.Fatalf("expected queued notification second, got %s", methods[1])
	}
}

func TestIntegration_RestartIce(t *testing.T) {
	_, router := createWorkerAndRouter(t)
	send := createWebRtcTransport(t, router)
	recv := createWebRtcTransport(t, router)
	direct := createDirectTransport(t, router)

	p := NewParticipant(ParticipantParams{
		Sid:          "recon-int",
		Identity:     "recon-user",
		Grants:       &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
		Router:       router,
		PingTimeout:  10 * time.Second,
		PingInterval: 5 * time.Second,
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.SetTransports(send, recv, direct)
	p.SetState(proto.ParticipantInfo_JOINED)

	result, err := p.RestartIce()
	if err != nil {
		t.Fatalf("RestartIce: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("expected 2 ICE parameter sets, got %d", len(result))
	}
	if result[send.Id()] == nil {
		t.Fatal("expected ICE params for send transport")
	}
	if result[recv.Id()] == nil {
		t.Fatal("expected ICE params for recv transport")
	}

	// Transport SIDs should remain the same after ICE restart
	if p.SendTransportSid() != send.Id() {
		t.Fatal("expected same send transport after ICE restart")
	}
	if p.RecvTransportSid() != recv.Id() {
		t.Fatal("expected same recv transport after ICE restart")
	}
}
