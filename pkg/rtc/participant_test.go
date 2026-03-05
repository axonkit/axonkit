package rtc

import (
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/proto"
)

func TestParticipant_NewParticipant(t *testing.T) {
	p := NewParticipant(ParticipantParams{
		Sid:      "sid-1",
		Identity: "alice",
		Name:     "Alice",
		Metadata: "meta",
		Attributes: map[string]string{
			"role": "host",
		},
		Grants: &proto.ClaimGrants{
			Video: &proto.VideoGrant{RoomJoin: true},
		},
		PingInterval: 5 * time.Second,
		PingTimeout:  15 * time.Second,
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if p.Sid() != "sid-1" {
		t.Fatalf("expected sid-1, got %s", p.Sid())
	}
	if p.Identity() != "alice" {
		t.Fatalf("expected alice, got %s", p.Identity())
	}
	if p.Name() != "Alice" {
		t.Fatalf("expected Alice, got %s", p.Name())
	}
	if p.State() != proto.ParticipantInfo_JOINING {
		t.Fatalf("expected JOINING state, got %d", p.State())
	}
	if p.Room() != nil {
		t.Fatal("expected nil room")
	}
}

func TestParticipant_SetState(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.SetState(proto.ParticipantInfo_JOINED)
	if p.State() != proto.ParticipantInfo_JOINED {
		t.Fatalf("expected JOINED, got %d", p.State())
	}
}

func TestParticipant_IsReady(t *testing.T) {
	p := NewParticipant(ParticipantParams{
		Sid: "sid-1", Identity: "alice",
		Grants: &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if p.IsReady() {
		t.Fatal("JOINING should not be ready")
	}
	p.SetState(proto.ParticipantInfo_ACTIVE)
	if !p.IsReady() {
		t.Fatal("ACTIVE should be ready")
	}
	p.SetState(proto.ParticipantInfo_JOINED)
	if !p.IsReady() {
		t.Fatal("JOINED should be ready")
	}
	p.SetState(proto.ParticipantInfo_DISCONNECTED)
	if p.IsReady() {
		t.Fatal("DISCONNECTED should not be ready")
	}
}

func TestParticipant_IsPublisher(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if p.IsPublisher() {
		t.Fatal("should not be publisher with no tracks")
	}
}

func TestParticipant_Permissions(t *testing.T) {
	tests := []struct {
		name         string
		grants       *proto.ClaimGrants
		canPub       bool
		canSub       bool
		canPubData   bool
		canUpdateMd  bool
	}{
		{
			name:       "nil grants defaults to all true",
			grants:     nil,
			canPub:     true,
			canSub:     true,
			canPubData: true,
		},
		{
			name:       "nil video defaults to all true",
			grants:     &proto.ClaimGrants{},
			canPub:     true,
			canSub:     true,
			canPubData: true,
		},
		{
			name:   "explicit grant",
			grants: &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
			canPub: true, canSub: true, canPubData: true,
		},
		{
			name: "restricted publish",
			grants: func() *proto.ClaimGrants {
				v := &proto.VideoGrant{RoomJoin: true}
				v.SetCanPublish(false)
				return &proto.ClaimGrants{Video: v}
			}(),
			canPub: false, canSub: true, canPubData: false,
		},
		{
			name: "restricted subscribe",
			grants: func() *proto.ClaimGrants {
				v := &proto.VideoGrant{RoomJoin: true}
				v.SetCanSubscribe(false)
				return &proto.ClaimGrants{Video: v}
			}(),
			canPub: true, canSub: false, canPubData: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := NewParticipant(ParticipantParams{
				Sid: "sid-1", Identity: "test", Grants: tc.grants,
			})
			defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

			if p.CanPublish() != tc.canPub {
				t.Errorf("CanPublish = %v, want %v", p.CanPublish(), tc.canPub)
			}
			if p.CanSubscribe() != tc.canSub {
				t.Errorf("CanSubscribe = %v, want %v", p.CanSubscribe(), tc.canSub)
			}
		})
	}
}

func TestParticipant_TransportSids_Nil(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if p.SendTransportSid() != "" {
		t.Fatal("expected empty send transport sid")
	}
	if p.RecvTransportSid() != "" {
		t.Fatal("expected empty recv transport sid")
	}
}

func TestParticipant_GetTransportBySid_Nil(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if p.GetTransportBySid("") != nil {
		t.Fatal("expected nil for empty sid")
	}
	if p.GetTransportBySid("nonexistent") != nil {
		t.Fatal("expected nil for nonexistent sid")
	}
	if p.GetDirectTransport() != nil {
		t.Fatal("expected nil direct transport")
	}
}

func TestParticipant_ToProto(t *testing.T) {
	p := NewParticipant(ParticipantParams{
		Sid:      "sid-1",
		Identity: "alice",
		Name:     "Alice",
		Metadata: "test-meta",
		Attributes: map[string]string{
			"role": "speaker",
		},
		Grants: &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	info := p.ToProto()
	if info.Sid != "sid-1" || info.Identity != "alice" || info.Name != "Alice" {
		t.Fatalf("unexpected proto: %+v", info)
	}
	if info.Metadata != "test-meta" {
		t.Fatalf("expected test-meta, got %s", info.Metadata)
	}
	if info.State != proto.ParticipantInfo_JOINING {
		t.Fatalf("expected JOINING state")
	}
	if info.Attributes["role"] != "speaker" {
		t.Fatal("expected role=speaker attribute")
	}
	if info.Permission == nil || !info.Permission.CanPublish || !info.Permission.CanSubscribe {
		t.Fatal("expected default permissions")
	}
}

func TestParticipant_UpdateMetadata(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	meta := "new-meta"
	name := "New Name"
	p.UpdateMetadata(&meta, &name, map[string]string{"key": "val"})

	info := p.ToProto()
	if info.Metadata != "new-meta" {
		t.Fatalf("expected new-meta, got %s", info.Metadata)
	}
	if info.Name != "New Name" {
		t.Fatalf("expected New Name, got %s", info.Name)
	}
	if info.Version != 1 {
		t.Fatalf("expected version 1, got %d", info.Version)
	}

	// Partial update
	p.UpdateMetadata(nil, nil, map[string]string{"key2": "val2"})
	info = p.ToProto()
	if info.Metadata != "new-meta" {
		t.Fatal("metadata should not change with nil")
	}
	if info.Version != 2 {
		t.Fatalf("expected version 2, got %d", info.Version)
	}
}

func TestParticipant_Close_Idempotent(t *testing.T) {
	p := newTestParticipant("alice")
	p.Close(proto.DisconnectReason_CLIENT_INITIATED)
	p.Close(proto.DisconnectReason_CLIENT_INITIATED) // should not panic

	if p.State() != proto.ParticipantInfo_DISCONNECTED {
		t.Fatalf("expected DISCONNECTED state")
	}
}

func TestParticipant_HandlePing(t *testing.T) {
	p := NewParticipant(ParticipantParams{
		Sid:          "sid-1",
		Identity:     "alice",
		Grants:       &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
		PingTimeout:  500 * time.Millisecond,
		PingInterval: 200 * time.Millisecond,
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.HandlePing(&proto.PingRequest{Timestamp: time.Now().UnixMilli()})
	p.mu.RLock()
	last := p.lastPing
	p.mu.RUnlock()
	if last.IsZero() {
		t.Fatal("expected lastPing to be set")
	}
}

func TestParticipant_StartHeartbeat_Timeout(t *testing.T) {
	p := NewParticipant(ParticipantParams{
		Sid:          "sid-1",
		Identity:     "alice",
		Grants:       &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
		PingTimeout:  100 * time.Millisecond,
		PingInterval: 50 * time.Millisecond,
	})

	p.StartHeartbeat()
	time.Sleep(300 * time.Millisecond)

	if p.State() != proto.ParticipantInfo_DISCONNECTED {
		t.Fatal("expected disconnect after ping timeout")
	}
}

func TestParticipant_PublishTrack_NoPermission(t *testing.T) {
	v := &proto.VideoGrant{RoomJoin: true}
	v.SetCanPublish(false)
	p := NewParticipant(ParticipantParams{
		Sid:      "sid-1",
		Identity: "alice",
		Grants:   &proto.ClaimGrants{Video: v},
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	_, err := p.PublishTrack(&proto.PublishTrackRequest{})
	if err == nil {
		t.Fatal("expected error for no publish permission")
	}
}

func TestParticipant_PublishTrack_NoTransport(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	_, err := p.PublishTrack(&proto.PublishTrackRequest{})
	if err == nil {
		t.Fatal("expected error for nil transport")
	}
}

func TestParticipant_PublishDataTrack_NoPermission(t *testing.T) {
	v := &proto.VideoGrant{RoomJoin: true}
	v.SetCanPublish(false)
	p := NewParticipant(ParticipantParams{
		Sid:      "sid-1",
		Identity: "alice",
		Grants:   &proto.ClaimGrants{Video: v},
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	_, err := p.PublishDataTrack(&proto.PublishDataTrackRequest{})
	if err == nil {
		t.Fatal("expected error for no publish permission")
	}
}

func TestParticipant_PublishDataTrack_NoTransport(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	_, err := p.PublishDataTrack(&proto.PublishDataTrackRequest{})
	if err == nil {
		t.Fatal("expected error for nil transport")
	}
}

func TestParticipant_Subscribe_NotReady(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	err := p.Subscribe(nil, nil)
	if err == nil {
		t.Fatal("expected error for nil transport/caps")
	}
}

func TestParticipant_SubscribeData_NotReady(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	err := p.SubscribeData(nil, nil)
	if err == nil {
		t.Fatal("expected error for nil transport")
	}
}

func TestParticipant_CloseSubscription_NotFound(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if err := p.CloseSubscription("nonexistent"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParticipant_CloseDataSubscription_NotFound(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if err := p.CloseDataSubscription("nonexistent"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParticipant_PauseResumeSubscription_NotFound(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if err := p.PauseSubscription("nonexistent"); err == nil {
		t.Fatal("expected error")
	}
	if err := p.ResumeSubscription("nonexistent"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParticipant_SetPreferredLayers_NotFound(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if err := p.SetPreferredLayers(&proto.SetPreferredLayersRequest{SubscriptionSid: "nonexistent"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestParticipant_SetPriority_NotFound(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if err := p.SetPriority("nonexistent", 0); err == nil {
		t.Fatal("expected error")
	}
}

func TestParticipant_RequestKeyFrame_NotFound(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if err := p.RequestKeyFrame("nonexistent"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParticipant_MuteTrack_NotFound(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if err := p.MuteTrack("nonexistent", true); err == nil {
		t.Fatal("expected error")
	}
}

func TestParticipant_UpdateTrack_NotFound(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if err := p.UpdateTrack(&proto.UpdateTrackRequest{TrackSid: "nonexistent"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestParticipant_UnpublishTrack_NotFound(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if err := p.UnpublishTrack("nonexistent"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParticipant_UnpublishDataTrack_NotFound(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if err := p.UnpublishDataTrack("nonexistent"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParticipant_GetTracks_Empty(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if len(p.GetPublishedTracks()) != 0 {
		t.Fatal("expected empty tracks")
	}
	if len(p.GetDataPublishedTracks()) != 0 {
		t.Fatal("expected empty data tracks")
	}
}

func TestParticipant_SendNotification_NoSignal(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if err := p.SendNotification("test", nil); err == nil {
		t.Fatal("expected error when signal not set")
	}
}

// ---------------------
// PublishedTrack / Dynacast tests
// ---------------------

func TestPublishedTrack_ToProto(t *testing.T) {
	track := &PublishedTrack{
		sid:    "track-1",
		name:   "camera",
		source: proto.TrackSource_CAMERA,
		kind:   "video",
		muted:  true,
		width:  1920,
		height: 1080,
	}
	info := track.ToProto()
	if info.Sid != "track-1" || info.Name != "camera" {
		t.Fatalf("unexpected proto: %+v", info)
	}
	if info.Type != proto.TrackType_VIDEO {
		t.Fatal("expected VIDEO type")
	}
	if !info.Muted {
		t.Fatal("expected muted")
	}
	if info.Width != 1920 || info.Height != 1080 {
		t.Fatal("unexpected dimensions")
	}

	audioTrack := &PublishedTrack{
		sid:  "track-2",
		kind: "audio",
	}
	audioInfo := audioTrack.ToProto()
	if audioInfo.Type != proto.TrackType_AUDIO {
		t.Fatal("expected AUDIO type")
	}
}

func TestDataPublishedTrack_ToProto(t *testing.T) {
	dt := &DataPublishedTrack{
		sid:     "dtrack-1",
		name:    "chat",
		appData: map[string]any{"key": "val"},
	}
	info := dt.ToProto()
	if info.Sid != "dtrack-1" || info.Name != "chat" {
		t.Fatalf("unexpected proto: %+v", info)
	}
}

func TestPublishedTrack_AddRemoveConsumer(t *testing.T) {
	track := &PublishedTrack{
		sid:             "track-1",
		kind:            "video",
		activeConsumers: make(map[string]*consumerLayerState),
	}

	track.addConsumer("c1")
	track.mu.Lock()
	if _, ok := track.activeConsumers["c1"]; !ok {
		t.Fatal("expected consumer c1")
	}
	track.mu.Unlock()

	track.removeConsumer("c1")
	track.mu.Lock()
	if _, ok := track.activeConsumers["c1"]; ok {
		t.Fatal("consumer c1 should be removed")
	}
	track.mu.Unlock()
}

func TestPublishedTrack_AddConsumer_NilMap(t *testing.T) {
	track := &PublishedTrack{
		sid:             "track-1",
		activeConsumers: nil,
	}
	// Should not panic when activeConsumers is nil
	track.addConsumer("c1")
	track.removeConsumer("c1")
	track.updateConsumerLayers("c1", 2)
	track.updateConsumerPaused("c1", true)
}

func TestPublishedTrack_UpdateConsumerLayers(t *testing.T) {
	track := &PublishedTrack{
		sid:             "track-1",
		kind:            "video",
		activeConsumers: make(map[string]*consumerLayerState),
	}

	track.addConsumer("c1")
	track.updateConsumerLayers("c1", 2)

	track.mu.Lock()
	st := track.activeConsumers["c1"]
	if st.spatialLayer != 2 {
		t.Fatalf("expected spatial layer 2, got %d", st.spatialLayer)
	}
	track.mu.Unlock()

	// Non-existent consumer
	track.updateConsumerLayers("nonexistent", 1)
}

func TestPublishedTrack_UpdateConsumerPaused(t *testing.T) {
	track := &PublishedTrack{
		sid:             "track-1",
		kind:            "audio",
		activeConsumers: make(map[string]*consumerLayerState),
	}

	track.addConsumer("c1")
	track.updateConsumerPaused("c1", true)

	track.mu.Lock()
	st := track.activeConsumers["c1"]
	if !st.paused {
		t.Fatal("expected paused")
	}
	track.mu.Unlock()

	// Non-existent consumer
	track.updateConsumerPaused("nonexistent", false)
}

func TestPublishedTrack_RecalcLocked_NilPublisher(t *testing.T) {
	track := &PublishedTrack{
		sid:             "track-1",
		kind:            "video",
		activeConsumers: make(map[string]*consumerLayerState),
		publisher:       nil,
	}
	track.addConsumer("c1")
	// recalc should be called but skip because publisher is nil
	track.removeConsumer("c1")
}

func TestParticipant_HandleIceStateChange(t *testing.T) {
	p := NewParticipant(ParticipantParams{
		Sid:      "sid-1",
		Identity: "alice",
		Grants:   &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	// Connected -> should transition to ACTIVE
	p.handleIceStateChange("connected")
	if p.State() != proto.ParticipantInfo_ACTIVE {
		t.Fatalf("expected ACTIVE after connected, got %d", p.State())
	}

	// Completed (while ACTIVE, should stay ACTIVE)
	p.handleIceStateChange("completed")
	if p.State() != proto.ParticipantInfo_ACTIVE {
		t.Fatalf("expected ACTIVE after completed, got %d", p.State())
	}

	// Disconnected -> should log but not change state
	p.handleIceStateChange("disconnected")
	if p.State() != proto.ParticipantInfo_ACTIVE {
		t.Fatalf("expected ACTIVE after ice disconnected, got %d", p.State())
	}
}

func TestParticipant_HandleIceStateChange_Closed(t *testing.T) {
	p := NewParticipant(ParticipantParams{
		Sid:      "sid-ice",
		Identity: "bob",
		Grants:   &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
	})

	// ICE closed triggers participant close
	p.handleIceStateChange("closed")
	time.Sleep(50 * time.Millisecond)
	if p.State() != proto.ParticipantInfo_DISCONNECTED {
		t.Fatalf("expected DISCONNECTED after ice closed, got %d", p.State())
	}
}

func TestParticipant_HandleIceStateChange_Reconnecting(t *testing.T) {
	p := NewParticipant(ParticipantParams{
		Sid:      "sid-recon",
		Identity: "carol",
		Grants:   &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	// Set reconnecting flag
	p.mu.Lock()
	p.reconnecting = true
	p.mu.Unlock()

	// ICE changes should be ignored during reconnection
	p.handleIceStateChange("connected")
	if p.State() != proto.ParticipantInfo_JOINING {
		t.Fatal("state should not change during reconnection")
	}
}

func TestParticipant_SetTransports_Nil(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	// Setting nil transports should not panic
	p.SetTransports(nil, nil, nil)
}

func TestParticipant_SetSignal_Nil(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	// Setting nil signals should not panic
	p.SetSignal(nil, nil, nil, nil)
}

func TestParticipant_SetRtpCapabilities(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.SetRtpCapabilities(nil)
	p.mu.RLock()
	if p.rtpCapabilities != nil {
		t.Fatal("expected nil capabilities")
	}
	p.mu.RUnlock()
}

func TestParticipant_Request_NoSignal(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	_, err := p.Request("test.method", nil)
	if err == nil {
		t.Fatal("expected error when signal channel not set")
	}
}

func TestParticipant_Reconnect_Closed(t *testing.T) {
	p := newTestParticipant("alice")
	p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	err := p.Reconnect(nil, nil, nil, nil, nil, nil, nil, nil)
	if err == nil {
		t.Fatal("expected error when participant is closed")
	}
}

func TestParticipant_Reconnect_Basic(t *testing.T) {
	p := NewParticipant(ParticipantParams{
		Sid:      "sid-recon",
		Identity: "alice",
		Grants:   &proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}},
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.SetState(proto.ParticipantInfo_ACTIVE)

	err := p.Reconnect(nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Reconnect failed: %v", err)
	}

	// After reconnect, state should be JOINING
	if p.State() != proto.ParticipantInfo_JOINING {
		t.Fatalf("expected JOINING after reconnect, got %d", p.State())
	}
}

// Tests recalcLocked with a real publisher that can receive notifications
func TestPublishedTrack_RecalcLocked_WithPublisher(t *testing.T) {
	publisher := newTestParticipant("alice")
	defer publisher.Close(proto.DisconnectReason_CLIENT_INITIATED)

	// Audio track: tests subscription status changed notification
	audioTrack := &PublishedTrack{
		sid:             "audio-track",
		kind:            "audio",
		activeConsumers: make(map[string]*consumerLayerState),
		publisher:       publisher,
	}

	// Add a consumer -> recalc -> hasSubscriber=true
	audioTrack.addConsumer("c1")
	// Now remove it -> recalc -> hasSubscriber=false
	audioTrack.removeConsumer("c1")

	// Video track: tests subscribed quality changed notification
	videoTrack := &PublishedTrack{
		sid:             "video-track",
		kind:            "video",
		activeConsumers: make(map[string]*consumerLayerState),
		publisher:       publisher,
		lastQualities:   [3]bool{true, true, true},
	}

	// Add consumers at different spatial layers
	videoTrack.addConsumer("c1")
	videoTrack.updateConsumerLayers("c1", 0) // LOW only

	videoTrack.addConsumer("c2")
	videoTrack.updateConsumerLayers("c2", 2) // HIGH

	// Pause one consumer
	videoTrack.updateConsumerPaused("c1", true)

	// Remove all consumers
	videoTrack.removeConsumer("c2")
	videoTrack.removeConsumer("c1")
}

// Tests recalcLocked with unset spatial layer (-1, means all)
func TestPublishedTrack_RecalcLocked_UnsetLayer(t *testing.T) {
	publisher := newTestParticipant("bob")
	defer publisher.Close(proto.DisconnectReason_CLIENT_INITIATED)

	track := &PublishedTrack{
		sid:             "video-unset",
		kind:            "video",
		activeConsumers: make(map[string]*consumerLayerState),
		publisher:       publisher,
		lastQualities:   [3]bool{false, false, false},
	}

	// Consumer with default spatialLayer=-1 means "use all"
	track.addConsumer("c1")
	// recalc already called by addConsumer's removeConsumer path not,
	// but addConsumer doesn't call recalc. Let's trigger it by updating.
	track.updateConsumerPaused("c1", false)
}

func TestParticipant_ToProto_WithTracks(t *testing.T) {
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	// Add published tracks manually
	p.mu.Lock()
	p.publishedTracks["t1"] = &PublishedTrack{
		sid: "t1", name: "cam", kind: "video",
		width: 1280, height: 720,
	}
	p.dataPublishedTracks["dt1"] = &DataPublishedTrack{
		sid: "dt1", name: "chat",
	}
	p.mu.Unlock()

	info := p.ToProto()
	if len(info.Tracks) != 1 {
		t.Fatalf("expected 1 track, got %d", len(info.Tracks))
	}
	if len(info.DataTracks) != 1 {
		t.Fatalf("expected 1 data track, got %d", len(info.DataTracks))
	}
	if !info.IsPublisher {
		t.Fatal("expected IsPublisher=true")
	}
}
