package rtc

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/proto"
)

func newSignalTestParticipant(t *testing.T) *Participant {
	t.Helper()
	cfg := defaultRoomConfig()
	cfg.AutoSubscribe = false
	room := NewRoom(RoomParams{
		Sid:          "room-sig",
		Name:         "signal-room",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	p := NewParticipant(ParticipantParams{
		Sid:           "sig-p1",
		Identity:      "sig-alice",
		Name:          "Alice",
		Grants:        &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
		AutoSubscribe: true,
		PingInterval:  5 * time.Second,
		PingTimeout:   15 * time.Second,
	})
	p.SetState(proto.ParticipantInfo_JOINED)
	room.AddParticipant(p)
	return p
}

func makeRequest(method, logId string, body any) *proto.Message {
	data, _ := json.Marshal(body)
	return proto.NewRequest(method, logId, data)
}

func TestDispatchRPC_UnknownMethod(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest("unknown.method", "log1", nil)
	p.dispatchRPC(msg) // should not panic, returns error response
}

func TestDispatchRPC_Ping(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodPing, "log-ping", &proto.PingRequest{
		Timestamp: time.Now().UnixMilli(),
	})
	p.dispatchRPC(msg)

	p.mu.RLock()
	last := p.lastPing
	p.mu.RUnlock()
	if last.IsZero() {
		t.Fatal("expected lastPing to be set after ping RPC")
	}
}

func TestDispatchRPC_Ping_BadJSON(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := proto.NewRequest(proto.MethodPing, "log-bad", []byte("not-json"))
	p.dispatchRPC(msg)
}

func TestDispatchRPC_Leave(t *testing.T) {
	p := newSignalTestParticipant(t)
	msg := makeRequest(proto.MethodLeave, "log-leave", nil)
	p.dispatchRPC(msg)

	time.Sleep(200 * time.Millisecond)
	if p.State() != proto.ParticipantInfo_DISCONNECTED {
		t.Fatal("expected DISCONNECTED after leave")
	}
}

func TestDispatchRPC_TrackPublish_NoTransport(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodTrackPublish, "log-pub", &proto.PublishTrackRequest{
		ClientTrackId: "client-1",
		Name:          "video",
	})
	p.dispatchRPC(msg)
}

func TestDispatchRPC_TrackPublish_BadJSON(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := proto.NewRequest(proto.MethodTrackPublish, "log-pub", []byte("{bad"))
	p.dispatchRPC(msg)
}

func TestDispatchRPC_TrackUnpublish(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodTrackUnpublish, "log-unpub", &proto.UnpublishTrackRequest{
		TrackSid: "nonexistent",
	})
	p.dispatchRPC(msg)
}

func TestDispatchRPC_TrackMute(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodTrackMute, "log-mute", &proto.MuteTrackRequest{
		TrackSid: "nonexistent",
		Muted:    true,
	})
	p.dispatchRPC(msg)
}

func TestDispatchRPC_TrackUpdate(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodTrackUpdate, "log-update", &proto.UpdateTrackRequest{
		TrackSid: "nonexistent",
	})
	p.dispatchRPC(msg)
}

func TestDispatchRPC_DataTrackPublish_NoTransport(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodDataTrackPublish, "log-dpub", &proto.PublishDataTrackRequest{
		ClientTrackId: "client-d1",
		Name:          "data",
	})
	p.dispatchRPC(msg)
}

func TestDispatchRPC_DataTrackUnpublish(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodDataTrackUnpublish, "log-dunpub", &proto.UnpublishDataTrackRequest{
		DataTrackSid: "nonexistent",
	})
	p.dispatchRPC(msg)
}

func TestDispatchRPC_SubscriptionPause(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodSubscriptionPause, "log-sp", &proto.PauseSubscriptionRequest{
		SubscriptionSid: "nonexistent",
	})
	p.dispatchRPC(msg)
}

func TestDispatchRPC_SubscriptionResume(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodSubscriptionResume, "log-sr", &proto.ResumeSubscriptionRequest{
		SubscriptionSid: "nonexistent",
	})
	p.dispatchRPC(msg)
}

func TestDispatchRPC_SubscriptionSetPreferredLayers(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodSubscriptionSetPreferredLayers, "log-spl", &proto.SetPreferredLayersRequest{
		SubscriptionSid: "nonexistent",
		SpatialLayer:    2,
	})
	p.dispatchRPC(msg)
}

func TestDispatchRPC_SubscriptionSetPriority(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodSubscriptionSetPriority, "log-spri", &proto.SetPriorityRequest{
		SubscriptionSid: "nonexistent",
		Priority:        5,
	})
	p.dispatchRPC(msg)
}

func TestDispatchRPC_SubscriptionRequestKeyFrame(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodSubscriptionRequestKeyFrame, "log-kf", &proto.RequestKeyFrameRequest{
		SubscriptionSid: "nonexistent",
	})
	p.dispatchRPC(msg)
}

func TestDispatchRPC_SubscriptionClose(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodSubscriptionClose, "log-sc", &proto.CloseSubscriptionRequest{
		SubscriptionSid: "nonexistent",
	})
	p.dispatchRPC(msg)
}

func TestDispatchRPC_DataSubscriptionClose(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodDataSubscriptionClose, "log-dsc", &proto.CloseDataSubscriptionRequest{
		DataSubscriptionSid: "nonexistent",
	})
	p.dispatchRPC(msg)
}

func TestDispatchRPC_TransportGetStats_NotFound(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodTransportGetStats, "log-stats", &proto.GetTransportStatsRequest{
		TransportSid: "nonexistent",
	})
	p.dispatchRPC(msg)
}

func TestDispatchRPC_TransportGetStats_BadJSON(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := proto.NewRequest(proto.MethodTransportGetStats, "log-stats", []byte("{bad"))
	p.dispatchRPC(msg)
}

func TestDispatchRPC_ParticipantUpdate(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)
	p.permission.CanUpdateMetadata = true

	meta := "updated"
	msg := makeRequest(proto.MethodParticipantUpdate, "log-pu", &proto.UpdateParticipantRequest{
		Metadata: &meta,
	})
	p.dispatchRPC(msg)

	if p.ToProto().Metadata != "updated" {
		t.Fatal("expected metadata to be updated")
	}
}

func TestDispatchRPC_ParticipantUpdate_NoPermission(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)
	p.permission.CanUpdateMetadata = false

	meta := "try-update"
	msg := makeRequest(proto.MethodParticipantUpdate, "log-pu-noperm", &proto.UpdateParticipantRequest{
		Metadata: &meta,
	})
	p.dispatchRPC(msg)

	if p.ToProto().Metadata == "try-update" {
		t.Fatal("should NOT have updated metadata without permission")
	}
}

func TestDispatchRPC_ParticipantUpdate_BadJSON(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := proto.NewRequest(proto.MethodParticipantUpdate, "log-pu-bad", []byte("{bad"))
	p.dispatchRPC(msg)
}

func TestDispatchRPC_Ready(t *testing.T) {
	cfg := config.RoomConfig{
		EmptyTimeout:     5 * time.Minute,
		DepartureTimeout: 30 * time.Second,
		MaxParticipants:  10,
		AutoSubscribe:    false,
	}
	room := NewRoom(RoomParams{
		Sid:          "room-ready",
		Name:         "ready-room",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	p := NewParticipant(ParticipantParams{
		Sid:           "ready-p1",
		Identity:      "ready-alice",
		Grants:        &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
		PingTimeout:   10 * time.Second,
		PingInterval:  5 * time.Second,
		AutoSubscribe: true,
	})
	room.AddParticipant(p)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodReady, "log-ready", &proto.ReadyRequest{})
	p.dispatchRPC(msg)

	if p.State() != proto.ParticipantInfo_JOINED {
		t.Fatalf("expected JOINED state after ready, got %d", p.State())
	}
}

func TestDispatchRPC_Ready_BadJSON(t *testing.T) {
	cfg := defaultRoomConfig()
	room := NewRoom(RoomParams{
		Sid:          "room-ready-bad",
		Name:         "ready-bad",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	p := NewParticipant(ParticipantParams{
		Sid: "ready-bad-p1", Identity: "ready-bad-alice",
		Grants: &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
	})
	room.AddParticipant(p)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := proto.NewRequest(proto.MethodReady, "log-ready-bad", []byte("{bad"))
	p.dispatchRPC(msg)
}

func TestDispatchRPC_Notification_NoResponse(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	data, _ := json.Marshal(&proto.PingRequest{Timestamp: 123})
	msg := proto.NewNotification(proto.MethodPing, data)
	p.dispatchRPC(msg) // notification, no response sent
}

func TestDispatchRPC_AllBadJSON(t *testing.T) {
	methods := []string{
		proto.MethodTrackUnpublish,
		proto.MethodTrackMute,
		proto.MethodTrackUpdate,
		proto.MethodDataTrackPublish,
		proto.MethodDataTrackUnpublish,
		proto.MethodSubscriptionPause,
		proto.MethodSubscriptionResume,
		proto.MethodSubscriptionSetPreferredLayers,
		proto.MethodSubscriptionSetPriority,
		proto.MethodSubscriptionRequestKeyFrame,
		proto.MethodSubscriptionClose,
		proto.MethodDataSubscriptionClose,
	}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			p := newSignalTestParticipant(t)
			defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

			msg := proto.NewRequest(method, "log-bad-"+method, []byte("{bad"))
			p.dispatchRPC(msg)
		})
	}
}

func TestProcessReconnectSync(t *testing.T) {
	cfg := defaultRoomConfig()
	cfg.AutoSubscribe = false
	room := NewRoom(RoomParams{
		Sid:          "room-sync",
		Name:         "sync-room",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	p := NewParticipant(ParticipantParams{
		Sid: "sync-p1", Identity: "sync-alice",
		Grants: &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
	})
	p.SetState(proto.ParticipantInfo_JOINED)
	room.AddParticipant(p)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.processReconnectSync(&proto.SyncState{
		SubscribedTrackSids:     []string{"old-track-1"},
		SubscribedDataTrackSids: []string{"old-data-1"},
	}, nil)
}

func TestProcessReconnectSync_NilRoom(t *testing.T) {
	p := NewParticipant(ParticipantParams{
		Sid: "sync-nilroom", Identity: "orphan",
		Grants: &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.processReconnectSync(&proto.SyncState{}, nil)
}

func TestHandleIncomingMessage_InvalidPayload(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.handleIncomingMessage([]byte("garbage"))
}

func TestHandleIncomingMessage_Request(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodPing, "log-hm", &proto.PingRequest{Timestamp: 123})
	data := msg.Encode()
	p.handleIncomingMessage(data)
	time.Sleep(50 * time.Millisecond)
}

func TestHandleIncomingMessage_Response(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	ch := make(chan *proto.Message, 1)
	p.mu.Lock()
	p.pendingRPC["test-log"] = ch
	p.mu.Unlock()

	resp := proto.NewResponse("test-log", 200, []byte(`{}`))
	data := resp.Encode()
	p.handleIncomingMessage(data)

	select {
	case got := <-ch:
		if got.Status != 200 {
			t.Fatalf("expected status 200, got %d", got.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("expected response on channel")
	}
}

func TestHandleIncomingMessage_ResponseUnknownLogId(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	resp := proto.NewResponse("unknown-log", 200, []byte(`{}`))
	data := resp.Encode()
	p.handleIncomingMessage(data) // no pending RPC, should not panic
}

func TestProcessReconnectSync_WithTracks(t *testing.T) {
	cfg := defaultRoomConfig()
	cfg.AutoSubscribe = false
	room := NewRoom(RoomParams{
		Sid:          "room-recon",
		Name:         "recon-room",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	p := NewParticipant(ParticipantParams{
		Sid: "recon-sub", Identity: "alice",
		Grants: &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)
	room.AddParticipant(p)

	pub := NewParticipant(ParticipantParams{
		Sid: "recon-pub", Identity: "bob",
		Grants: &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
	})
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	room.AddParticipant(pub)

	pub.mu.Lock()
	pub.publishedTracks["t-existing"] = &PublishedTrack{
		sid: "t-existing", name: "cam", kind: "video",
	}
	pub.dataPublishedTracks["dt-existing"] = &DataPublishedTrack{
		sid: "dt-existing", name: "chat",
	}
	pub.mu.Unlock()

	syncState := &proto.SyncState{
		SubscribedTrackSids:     []string{"t-old"},
		SubscribedDataTrackSids: []string{"dt-old"},
	}

	otherInfos := room.GetAllParticipantInfos(p.sid)
	p.processReconnectSync(syncState, otherInfos)
}

func TestHandleIncomingMessage_QueueFull(t *testing.T) {
	p := NewParticipant(ParticipantParams{
		Sid: "queue-full", Identity: "overflow",
		Grants: &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
	})

	// Close the done channel without calling Close, to stop messageLoop
	// Then fill the queue
	close(p.done)
	time.Sleep(20 * time.Millisecond)

	// Fill the message queue
	for i := 0; i < msgQueueSize; i++ {
		p.msgCh <- &proto.Message{Type: proto.TypeRequest, Method: "dummy", LogId: "x"}
	}

	// This message should hit the "queue full" branch
	msg := makeRequest(proto.MethodPing, "overflow-log", &proto.PingRequest{Timestamp: 1})
	data := msg.Encode()
	p.handleIncomingMessage(data)

	// Clean up
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
}
