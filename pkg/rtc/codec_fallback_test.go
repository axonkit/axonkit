package rtc

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/proto"
)

// ---------------------------------------------------------------------------
// Room.ShouldRegressCodec
// ---------------------------------------------------------------------------

func TestRoom_ShouldRegressCodec_ZeroThreshold(t *testing.T) {
	room := NewRoom(RoomParams{
		Sid:          "room-regress-0",
		Name:         "regress-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})
	if !room.ShouldRegressCodec() {
		t.Fatal("threshold=0 should always return true")
	}
}

func TestRoom_ShouldRegressCodec_BelowThreshold(t *testing.T) {
	cfg := defaultRoomConfig()
	cfg.CodecRegressionThreshold = 5
	room := NewRoom(RoomParams{
		Sid:          "room-regress-below",
		Name:         "regress-below",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	for i := 0; i < 3; i++ {
		p := newTestParticipant("p" + string(rune('a'+i)))
		defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)
		room.AddParticipant(p)
	}

	if !room.ShouldRegressCodec() {
		t.Fatal("3 participants < threshold 5: should return true")
	}
}

func TestRoom_ShouldRegressCodec_AboveThreshold(t *testing.T) {
	cfg := defaultRoomConfig()
	cfg.CodecRegressionThreshold = 3
	room := NewRoom(RoomParams{
		Sid:          "room-regress-above",
		Name:         "regress-above",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	for i := 0; i < 5; i++ {
		p := newTestParticipant("p" + string(rune('a'+i)))
		defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)
		room.AddParticipant(p)
	}

	if room.ShouldRegressCodec() {
		t.Fatal("5 participants >= threshold 3: should return false")
	}
}

func TestRoom_ShouldRegressCodec_ExactThreshold(t *testing.T) {
	cfg := defaultRoomConfig()
	cfg.CodecRegressionThreshold = 3
	room := NewRoom(RoomParams{
		Sid:          "room-regress-exact",
		Name:         "regress-exact",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	for i := 0; i < 3; i++ {
		p := newTestParticipant("exact" + string(rune('a'+i)))
		defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)
		room.AddParticipant(p)
	}

	if room.ShouldRegressCodec() {
		t.Fatal("count==threshold should return false (not strictly less than)")
	}
}

// ---------------------------------------------------------------------------
// PublishedTrack.enableRegression
// ---------------------------------------------------------------------------

func TestPublishedTrack_EnableRegression_NilPublisher(t *testing.T) {
	track := &PublishedTrack{
		sid:               "track-enable-nil",
		backupCodecPolicy: proto.BackupCodecPolicy_REGRESSION,
		publisher:         nil,
	}
	if track.enableRegression() {
		t.Fatal("nil publisher should return false")
	}
}

func TestPublishedTrack_EnableRegression_NilRoom(t *testing.T) {
	pub := newTestParticipant("pub-noroom")
	pub.room = nil
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)

	track := &PublishedTrack{
		sid:               "track-enable-noroom",
		backupCodecPolicy: proto.BackupCodecPolicy_REGRESSION,
		publisher:         pub,
	}
	if track.enableRegression() {
		t.Fatal("nil room should return false")
	}
}

func TestPublishedTrack_EnableRegression_PolicyREGRESSION(t *testing.T) {
	cfg := defaultRoomConfig()
	room := NewRoom(RoomParams{
		Sid: "room-er-reg", Name: "room-er-reg",
		Config: cfg, CreationTime: time.Now(),
	})
	pub := newTestParticipant("pub-regression")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	room.AddParticipant(pub)

	track := &PublishedTrack{
		sid:               "track-regression",
		backupCodecPolicy: proto.BackupCodecPolicy_REGRESSION,
		publisher:         pub,
	}
	if !track.enableRegression() {
		t.Fatal("REGRESSION policy should always return true")
	}
}

func TestPublishedTrack_EnableRegression_PolicySIMULCAST(t *testing.T) {
	cfg := defaultRoomConfig()
	room := NewRoom(RoomParams{
		Sid: "room-er-sim", Name: "room-er-sim",
		Config: cfg, CreationTime: time.Now(),
	})
	pub := newTestParticipant("pub-simulcast")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	room.AddParticipant(pub)

	track := &PublishedTrack{
		sid:               "track-simulcast",
		backupCodecPolicy: proto.BackupCodecPolicy_SIMULCAST,
		publisher:         pub,
	}
	if track.enableRegression() {
		t.Fatal("SIMULCAST policy should always return false")
	}
}

func TestPublishedTrack_EnableRegression_PolicyPREFER_REGRESSION(t *testing.T) {
	tests := []struct {
		name      string
		threshold int
		numParts  int
		expect    bool
	}{
		{"zero threshold always regress", 0, 5, true},
		{"below threshold regress", 5, 3, true},
		{"above threshold no regress", 3, 5, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultRoomConfig()
			cfg.CodecRegressionThreshold = tc.threshold
			room := NewRoom(RoomParams{
				Sid: "room-pr-" + tc.name, Name: "room-pr-" + tc.name,
				Config: cfg, CreationTime: time.Now(),
			})

			for i := 0; i < tc.numParts; i++ {
				p := newTestParticipant(tc.name + string(rune('a'+i)))
				defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)
				room.AddParticipant(p)
			}

			pub := room.GetParticipant("sid-" + tc.name + "a")
			track := &PublishedTrack{
				sid:               "track-pr",
				backupCodecPolicy: proto.BackupCodecPolicy_PREFER_REGRESSION,
				publisher:         pub,
			}

			got := track.enableRegression()
			if got != tc.expect {
				t.Fatalf("enableRegression() = %v, want %v", got, tc.expect)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// PublishedTrack.ToProto — includes BackupCodecPolicy
// ---------------------------------------------------------------------------

func TestPublishedTrack_ToProto_BackupCodecPolicy(t *testing.T) {
	track := &PublishedTrack{
		sid:               "track-proto-bcp",
		name:              "camera",
		kind:              "video",
		source:            proto.TrackSource_CAMERA,
		backupCodecPolicy: proto.BackupCodecPolicy_REGRESSION,
	}
	info := track.ToProto()
	if info.BackupCodecPolicy != proto.BackupCodecPolicy_REGRESSION {
		t.Fatalf("expected REGRESSION policy, got %d", info.BackupCodecPolicy)
	}

	track2 := &PublishedTrack{
		sid:               "track-proto-sim",
		name:              "screen",
		kind:              "video",
		backupCodecPolicy: proto.BackupCodecPolicy_SIMULCAST,
	}
	info2 := track2.ToProto()
	if info2.BackupCodecPolicy != proto.BackupCodecPolicy_SIMULCAST {
		t.Fatalf("expected SIMULCAST policy, got %d", info2.BackupCodecPolicy)
	}
}

// ---------------------------------------------------------------------------
// PublishedTrack backup codec model tests
// ---------------------------------------------------------------------------

func TestPublishedTrack_BackupConsumers_Map(t *testing.T) {
	track := &PublishedTrack{
		sid:             "track-bc-map",
		kind:            "video",
		backupCodec:     "vp8",
		backupConsumers: make(map[string]string),
	}

	track.backupConsumers["c1"] = "primary"
	track.backupConsumers["c2"] = "backup"
	track.backupConsumers["c3"] = "primary"

	hasBackup := false
	for _, codec := range track.backupConsumers {
		if codec == "backup" {
			hasBackup = true
			break
		}
	}
	if !hasBackup {
		t.Fatal("should detect backup consumer")
	}

	delete(track.backupConsumers, "c2")

	hasBackup = false
	for _, codec := range track.backupConsumers {
		if codec == "backup" {
			hasBackup = true
			break
		}
	}
	if hasBackup {
		t.Fatal("should not detect backup consumer after removal")
	}
}

func TestPublishedTrack_CheckCodecRegressionReversal_NoBackupCodec(t *testing.T) {
	pub := newTestParticipant("pub-no-backup")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)

	track := &PublishedTrack{
		sid:             "track-no-backup",
		kind:            "video",
		backupCodec:     "",
		backupConsumers: nil,
		publisher:       pub,
	}
	// Should return early without panic
	track.checkCodecRegressionReversal()
}

func TestPublishedTrack_CheckCodecRegressionReversal_NilConsumers(t *testing.T) {
	pub := newTestParticipant("pub-nil-consumers")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)

	track := &PublishedTrack{
		sid:             "track-nil-consumers",
		kind:            "video",
		backupCodec:     "vp8",
		backupConsumers: nil,
		publisher:       pub,
	}
	// Should return early without panic
	track.checkCodecRegressionReversal()
}

func TestPublishedTrack_CheckCodecRegressionReversal_HasBackupConsumer(t *testing.T) {
	pub := newTestParticipant("pub-has-backup")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)

	track := &PublishedTrack{
		sid:             "track-has-backup",
		kind:            "video",
		videoCodec:      "vp9",
		backupCodec:     "vp8",
		backupConsumers: map[string]string{"c1": "backup"},
		regressed:       true,
		publisher:       pub,
	}
	track.checkCodecRegressionReversal()

	track.mu.Lock()
	stillRegressed := track.regressed
	track.mu.Unlock()

	if !stillRegressed {
		t.Fatal("should remain regressed while backup consumers exist")
	}
}

func TestPublishedTrack_CheckCodecRegressionReversal_NoBackupConsumers_WasRegressed(t *testing.T) {
	pub := newTestParticipant("pub-revert")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)

	track := &PublishedTrack{
		sid:             "track-revert",
		kind:            "video",
		videoCodec:      "vp9",
		backupCodec:     "vp8",
		backupConsumers: map[string]string{"c1": "primary"},
		regressed:       true,
		publisher:       pub,
	}
	track.checkCodecRegressionReversal()

	track.mu.Lock()
	r := track.regressed
	track.mu.Unlock()

	if r {
		t.Fatal("should have reverted regression when no backup consumers remain")
	}
}

func TestPublishedTrack_CheckCodecRegressionReversal_RemoteSub(t *testing.T) {
	pub := newTestParticipant("pub-remote-sub")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)

	track := &PublishedTrack{
		sid:             "track-remote-sub",
		kind:            "video",
		videoCodec:      "vp9",
		backupCodec:     "vp8",
		backupConsumers: map[string]string{"c1": "primary"},
		regressed:       true,
		publisher:       pub,
		remoteSubscriptions: map[string]*TrackSubInfo{
			"node-2": {HasBackupSub: true},
		},
	}
	track.checkCodecRegressionReversal()

	track.mu.Lock()
	r := track.regressed
	track.mu.Unlock()

	if !r {
		t.Fatal("should remain regressed when remote node has backup sub")
	}
}

// ---------------------------------------------------------------------------
// PublishedTrack.requestBackupCodec
// ---------------------------------------------------------------------------

func TestPublishedTrack_RequestBackupCodec_NilPublisher(t *testing.T) {
	track := &PublishedTrack{
		sid:         "track-req-nil",
		backupCodec: "vp8",
		publisher:   nil,
	}
	// Should not panic
	track.requestBackupCodec()
}

func TestPublishedTrack_RequestBackupCodec_NoBackupCodec(t *testing.T) {
	pub := newTestParticipant("pub-no-bc")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)

	track := &PublishedTrack{
		sid:         "track-no-bc",
		backupCodec: "",
		publisher:   pub,
	}
	// Should return early
	track.requestBackupCodec()
}

// ---------------------------------------------------------------------------
// PublishedTrack.currentSubscribedQualities
// ---------------------------------------------------------------------------

func TestPublishedTrack_CurrentSubscribedQualities(t *testing.T) {
	track := &PublishedTrack{
		sid:           "track-csq",
		lastQualities: [3]bool{true, false, true},
	}
	qualities := track.currentSubscribedQualities()
	if len(qualities) != 3 {
		t.Fatalf("expected 3 qualities, got %d", len(qualities))
	}
	if !qualities[0].Enabled || qualities[1].Enabled || !qualities[2].Enabled {
		t.Fatal("quality states don't match lastQualities")
	}
	if qualities[0].Quality != proto.VideoQuality_LOW ||
		qualities[1].Quality != proto.VideoQuality_MEDIUM ||
		qualities[2].Quality != proto.VideoQuality_HIGH {
		t.Fatal("quality levels incorrect")
	}
}

// ---------------------------------------------------------------------------
// PublishedTrack.sendCodecQualityNotification
// ---------------------------------------------------------------------------

func TestPublishedTrack_SendCodecQualityNotification_NilPublisher(t *testing.T) {
	track := &PublishedTrack{
		sid:        "track-scqn-nil",
		videoCodec: "vp9",
		backupCodec: "vp8",
		publisher:  nil,
	}
	// Should not panic
	track.sendCodecQualityNotification(true, false)
}

func TestPublishedTrack_SendCodecQualityNotification_NoCodecs(t *testing.T) {
	pub := newTestParticipant("pub-scqn-empty")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)

	track := &PublishedTrack{
		sid:        "track-scqn-empty",
		videoCodec: "",
		backupCodec: "",
		publisher:  pub,
	}
	// Should not panic even with empty codecs
	track.sendCodecQualityNotification(true, true)
}

// ---------------------------------------------------------------------------
// PublishedTrack.findSubscriberByConsumer
// ---------------------------------------------------------------------------

func TestPublishedTrack_FindSubscriberByConsumer_NilPublisher(t *testing.T) {
	track := &PublishedTrack{
		sid:       "track-fsbc-nil",
		publisher: nil,
	}
	if track.findSubscriberByConsumer("c1") != nil {
		t.Fatal("should return nil for nil publisher")
	}
}

func TestPublishedTrack_FindSubscriberByConsumer_NilRoom(t *testing.T) {
	pub := newTestParticipant("pub-fsbc-noroom")
	pub.room = nil
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)

	track := &PublishedTrack{
		sid:       "track-fsbc-noroom",
		publisher: pub,
	}
	if track.findSubscriberByConsumer("c1") != nil {
		t.Fatal("should return nil for nil room")
	}
}

// ---------------------------------------------------------------------------
// PublishedTrack.onBackupProducerReady — empties pending queue
// ---------------------------------------------------------------------------

func TestPublishedTrack_OnBackupProducerReady_ClearsPending(t *testing.T) {
	cfg := defaultRoomConfig()
	room := NewRoom(RoomParams{
		Sid: "room-bpr", Name: "room-bpr",
		Config: cfg, CreationTime: time.Now(),
	})

	pub := newTestParticipant("pub-bpr")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	room.AddParticipant(pub)

	sub := newTestParticipant("sub-bpr")
	defer sub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	room.AddParticipant(sub)

	track := &PublishedTrack{
		sid:               "track-bpr",
		kind:              "video",
		videoCodec:        "vp9",
		backupCodec:       "vp8",
		backupCodecPolicy: proto.BackupCodecPolicy_SIMULCAST,
		backupConsumers:   make(map[string]string),
		publisher:         pub,
		pendingBackupSubs: []*pendingBackupSub{
			{subscriber: sub, publisher: pub},
		},
	}

	track.onBackupProducerReady()

	track.mu.Lock()
	remaining := len(track.pendingBackupSubs)
	track.mu.Unlock()

	if remaining != 0 {
		t.Fatalf("expected pending queue cleared, got %d", remaining)
	}
}

// ---------------------------------------------------------------------------
// PublishedTrack.performRegression — sets regressed flag
// ---------------------------------------------------------------------------

func TestPublishedTrack_PerformRegression_AlreadyRegressed(t *testing.T) {
	pub := newTestParticipant("pub-already-reg")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)

	track := &PublishedTrack{
		sid:             "track-already-reg",
		kind:            "video",
		videoCodec:      "vp9",
		backupCodec:     "vp8",
		regressed:       true,
		backupConsumers: make(map[string]string),
		publisher:       pub,
	}
	// Should return early without doing anything
	track.performRegression()

	track.mu.Lock()
	r := track.regressed
	track.mu.Unlock()
	if !r {
		t.Fatal("should still be regressed")
	}
}

func TestPublishedTrack_PerformRegression_SetsFlag(t *testing.T) {
	cfg := defaultRoomConfig()
	room := NewRoom(RoomParams{
		Sid: "room-perf-reg", Name: "room-perf-reg",
		Config: cfg, CreationTime: time.Now(),
	})

	pub := newTestParticipant("pub-perf-reg")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	room.AddParticipant(pub)

	track := &PublishedTrack{
		sid:             "track-perf-reg",
		kind:            "video",
		videoCodec:      "vp9",
		backupCodec:     "vp8",
		regressed:       false,
		backupConsumers: make(map[string]string),
		publisher:       pub,
	}
	track.performRegression()

	track.mu.Lock()
	r := track.regressed
	track.mu.Unlock()
	if !r {
		t.Fatal("performRegression should set regressed=true")
	}
}

// ---------------------------------------------------------------------------
// Config: CodecRegressionThreshold
// ---------------------------------------------------------------------------

func TestConfig_CodecRegressionThreshold_Default(t *testing.T) {
	cfg := config.RoomConfig{}
	if cfg.CodecRegressionThreshold != 0 {
		t.Fatalf("default threshold should be 0, got %d", cfg.CodecRegressionThreshold)
	}
}

func TestConfig_CodecRegressionThreshold_JSON(t *testing.T) {
	data := `{"codecRegressionThreshold": 10}`
	var cfg config.RoomConfig
	if err := json.Unmarshal([]byte(data), &cfg); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if cfg.CodecRegressionThreshold != 10 {
		t.Fatalf("expected 10, got %d", cfg.CodecRegressionThreshold)
	}
}

// ---------------------------------------------------------------------------
// Proto model tests
// ---------------------------------------------------------------------------

func TestProto_BackupCodecPolicy_Values(t *testing.T) {
	if proto.BackupCodecPolicy_PREFER_REGRESSION != 0 {
		t.Fatal("PREFER_REGRESSION should be 0")
	}
	if proto.BackupCodecPolicy_SIMULCAST != 1 {
		t.Fatal("SIMULCAST should be 1")
	}
	if proto.BackupCodecPolicy_REGRESSION != 2 {
		t.Fatal("REGRESSION should be 2")
	}
}

func TestProto_PublishTrackRequest_CodecFields_JSON(t *testing.T) {
	req := proto.PublishTrackRequest{
		ClientTrackId:     "ct-1",
		Name:              "camera",
		VideoCodec:        "vp9",
		BackupCodec:       "vp8",
		BackupCodecPolicy: proto.BackupCodecPolicy_REGRESSION,
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded proto.PublishTrackRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if decoded.VideoCodec != "vp9" {
		t.Fatalf("expected vp9, got %s", decoded.VideoCodec)
	}
	if decoded.BackupCodec != "vp8" {
		t.Fatalf("expected vp8, got %s", decoded.BackupCodec)
	}
	if decoded.BackupCodecPolicy != proto.BackupCodecPolicy_REGRESSION {
		t.Fatalf("expected REGRESSION, got %d", decoded.BackupCodecPolicy)
	}
}

func TestProto_PublishBackupTrackRequest_JSON(t *testing.T) {
	req := proto.PublishBackupTrackRequest{
		TrackSid:      "track-123",
		ClientTrackId: "ct-backup",
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded proto.PublishBackupTrackRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if decoded.TrackSid != "track-123" {
		t.Fatalf("expected track-123, got %s", decoded.TrackSid)
	}
	if decoded.ClientTrackId != "ct-backup" {
		t.Fatalf("expected ct-backup, got %s", decoded.ClientTrackId)
	}
}

func TestProto_PublishBackupTrackResponse_JSON(t *testing.T) {
	resp := proto.PublishBackupTrackResponse{
		BackupTrackSid: "backup-sid-abc",
	}
	data, _ := json.Marshal(resp)

	var decoded proto.PublishBackupTrackResponse
	json.Unmarshal(data, &decoded)
	if decoded.BackupTrackSid != "backup-sid-abc" {
		t.Fatalf("expected backup-sid-abc, got %s", decoded.BackupTrackSid)
	}
}

func TestProto_SubscribedQualityChangedNotification_WithCodecs(t *testing.T) {
	notif := proto.SubscribedQualityChangedNotification{
		TrackSid: "track-sq",
		SubscribedQualities: []proto.SubscribedQuality{
			{Quality: proto.VideoQuality_LOW, Enabled: true},
			{Quality: proto.VideoQuality_MEDIUM, Enabled: true},
			{Quality: proto.VideoQuality_HIGH, Enabled: false},
		},
		SubscribedCodecs: []proto.SubscribedCodec{
			{
				Codec: "vp9",
				Qualities: []proto.SubscribedQuality{
					{Quality: proto.VideoQuality_LOW, Enabled: true},
					{Quality: proto.VideoQuality_MEDIUM, Enabled: true},
					{Quality: proto.VideoQuality_HIGH, Enabled: true},
				},
			},
			{
				Codec: "vp8",
				Qualities: []proto.SubscribedQuality{
					{Quality: proto.VideoQuality_LOW, Enabled: true},
					{Quality: proto.VideoQuality_MEDIUM, Enabled: true},
					{Quality: proto.VideoQuality_HIGH, Enabled: true},
				},
			},
		},
	}

	data, err := json.Marshal(notif)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded proto.SubscribedQualityChangedNotification
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(decoded.SubscribedCodecs) != 2 {
		t.Fatalf("expected 2 codecs, got %d", len(decoded.SubscribedCodecs))
	}
	if decoded.SubscribedCodecs[0].Codec != "vp9" {
		t.Fatalf("expected vp9, got %s", decoded.SubscribedCodecs[0].Codec)
	}
	if decoded.SubscribedCodecs[1].Codec != "vp8" {
		t.Fatalf("expected vp8, got %s", decoded.SubscribedCodecs[1].Codec)
	}
	if len(decoded.SubscribedCodecs[0].Qualities) != 3 {
		t.Fatalf("expected 3 qualities per codec")
	}
}

func TestProto_SubscribedQualityChangedNotification_NoCodecs_BackwardCompat(t *testing.T) {
	notif := proto.SubscribedQualityChangedNotification{
		TrackSid: "track-compat",
		SubscribedQualities: []proto.SubscribedQuality{
			{Quality: proto.VideoQuality_LOW, Enabled: true},
		},
	}

	data, _ := json.Marshal(notif)
	var decoded proto.SubscribedQualityChangedNotification
	json.Unmarshal(data, &decoded)

	if decoded.SubscribedCodecs != nil {
		t.Fatal("subscribedCodecs should be nil when not provided")
	}
	if len(decoded.SubscribedQualities) != 1 {
		t.Fatal("subscribedQualities should remain intact")
	}
}

// ---------------------------------------------------------------------------
// TrackSubInfo — backup fields
// ---------------------------------------------------------------------------

func TestTrackSubInfo_BackupFields_JSON(t *testing.T) {
	info := TrackSubInfo{
		TrackSid:     "track-sub-bf",
		NodeId:       "node-1",
		Layers:       [3]bool{true, true, false},
		HasBackupSub: true,
		BackupLayers: [3]bool{true, true, true},
	}
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded TrackSubInfo
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if !decoded.HasBackupSub {
		t.Fatal("expected HasBackupSub=true")
	}
	if decoded.BackupLayers != [3]bool{true, true, true} {
		t.Fatalf("unexpected BackupLayers: %v", decoded.BackupLayers)
	}
}

func TestTrackSubInfo_NoBackup_JSON(t *testing.T) {
	info := TrackSubInfo{
		TrackSid: "track-sub-nb",
		NodeId:   "node-1",
		Layers:   [3]bool{true, false, false},
	}
	data, _ := json.Marshal(info)

	var decoded TrackSubInfo
	json.Unmarshal(data, &decoded)
	if decoded.HasBackupSub {
		t.Fatal("HasBackupSub should default to false")
	}
}

// ---------------------------------------------------------------------------
// RPC method constant
// ---------------------------------------------------------------------------

func TestRPC_MethodTrackPublishBackup(t *testing.T) {
	if proto.MethodTrackPublishBackup != "track.publishBackup" {
		t.Fatalf("unexpected method name: %s", proto.MethodTrackPublishBackup)
	}
}

// ---------------------------------------------------------------------------
// Dispatch RPC: track.publishBackup
// ---------------------------------------------------------------------------

func TestDispatchRPC_TrackPublishBackup_BadJSON(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := proto.NewRequest(proto.MethodTrackPublishBackup, "log-bkp-bad", []byte("{bad"))
	p.dispatchRPC(msg)
}

func TestDispatchRPC_TrackPublishBackup_TrackNotFound(t *testing.T) {
	p := newSignalTestParticipant(t)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodTrackPublishBackup, "log-bkp-nf", &proto.PublishBackupTrackRequest{
		TrackSid:      "nonexistent",
		ClientTrackId: "ct-backup",
	})
	p.dispatchRPC(msg)
}

func TestDispatchRPC_TrackPublishBackup_NoPermission(t *testing.T) {
	cfg := defaultRoomConfig()
	cfg.AutoSubscribe = false
	room := NewRoom(RoomParams{
		Sid:          "room-bkp-noperm",
		Name:         "bkp-noperm",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	m := &proto.MediaGrant{}
	m.SetCanPublish(false)
	p := NewParticipant(ParticipantParams{
		Sid:      "bkp-noperm-p",
		Identity: "bkp-noperm-alice",
		Grants:   &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}, Media: m},
	})
	p.SetState(proto.ParticipantInfo_CONNECTED)
	room.AddParticipant(p)
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	msg := makeRequest(proto.MethodTrackPublishBackup, "log-bkp-noperm", &proto.PublishBackupTrackRequest{
		TrackSid:      "some-track",
		ClientTrackId: "ct-backup",
	})
	p.dispatchRPC(msg)
}

// ---------------------------------------------------------------------------
// Participant.PublishBackupTrack — error paths
// ---------------------------------------------------------------------------

func TestParticipant_PublishBackupTrack_NoPermission(t *testing.T) {
	m := &proto.MediaGrant{}
	m.SetCanPublish(false)
	p := NewParticipant(ParticipantParams{
		Sid:      "bkp-np",
		Identity: "bkp-np",
		Grants:   &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}, Media: m},
	})
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	_, err := p.PublishBackupTrack(&proto.PublishBackupTrackRequest{TrackSid: "t1"})
	if err == nil {
		t.Fatal("expected error for no publish permission")
	}
}

func TestParticipant_PublishBackupTrack_TrackNotFound(t *testing.T) {
	p := newTestParticipant("pub-bkp-nf")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	_, err := p.PublishBackupTrack(&proto.PublishBackupTrackRequest{TrackSid: "nonexistent"})
	if err == nil {
		t.Fatal("expected error for track not found")
	}
}

func TestParticipant_PublishBackupTrack_NoSendTransport(t *testing.T) {
	p := newTestParticipant("pub-bkp-nt")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.mu.Lock()
	p.publishedTracks["t1"] = &PublishedTrack{
		sid:         "t1",
		kind:        "video",
		backupCodec: "vp8",
	}
	p.mu.Unlock()

	_, err := p.PublishBackupTrack(&proto.PublishBackupTrackRequest{TrackSid: "t1"})
	if err == nil {
		t.Fatal("expected error for nil send transport")
	}
}

func TestParticipant_PublishBackupTrack_NoBackupCodecConfigured(t *testing.T) {
	p := newTestParticipant("pub-bkp-nobc")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p.mu.Lock()
	p.publishedTracks["t1"] = &PublishedTrack{
		sid:         "t1",
		kind:        "video",
		backupCodec: "",
	}
	p.mu.Unlock()

	_, err := p.PublishBackupTrack(&proto.PublishBackupTrackRequest{TrackSid: "t1"})
	if err == nil {
		t.Fatal("expected error for no backup codec configured")
	}
}

// ---------------------------------------------------------------------------
// PublishedTrack model — pending backup subs
// ---------------------------------------------------------------------------

func TestPublishedTrack_PendingBackupSubs(t *testing.T) {
	sub1 := newTestParticipant("sub1")
	defer sub1.Close(proto.DisconnectReason_CLIENT_INITIATED)
	sub2 := newTestParticipant("sub2")
	defer sub2.Close(proto.DisconnectReason_CLIENT_INITIATED)
	pub := newTestParticipant("pub-pbs")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)

	track := &PublishedTrack{
		sid:               "track-pbs",
		kind:              "video",
		pendingBackupSubs: nil,
	}

	track.pendingBackupSubs = append(track.pendingBackupSubs, &pendingBackupSub{
		subscriber: sub1,
		publisher:  pub,
	})
	track.pendingBackupSubs = append(track.pendingBackupSubs, &pendingBackupSub{
		subscriber: sub2,
		publisher:  pub,
	})

	if len(track.pendingBackupSubs) != 2 {
		t.Fatalf("expected 2 pending subs, got %d", len(track.pendingBackupSubs))
	}

	track.pendingBackupSubs = nil
	if track.pendingBackupSubs != nil {
		t.Fatal("expected nil after clearing")
	}
}

// ---------------------------------------------------------------------------
// PublishedTrack backup fields initialization
// ---------------------------------------------------------------------------

func TestPublishedTrack_BackupFieldsInit(t *testing.T) {
	track := &PublishedTrack{
		sid:               "track-init",
		kind:              "video",
		videoCodec:        "av1",
		backupCodec:       "vp8",
		backupCodecPolicy: proto.BackupCodecPolicy_PREFER_REGRESSION,
		backupConsumers:   make(map[string]string),
	}

	if track.videoCodec != "av1" {
		t.Fatal("unexpected videoCodec")
	}
	if track.backupCodec != "vp8" {
		t.Fatal("unexpected backupCodec")
	}
	if track.backupCodecPolicy != proto.BackupCodecPolicy_PREFER_REGRESSION {
		t.Fatal("unexpected policy")
	}
	if track.regressed {
		t.Fatal("should not be regressed by default")
	}
	if track.backupRequested {
		t.Fatal("should not have requested backup by default")
	}
	if track.backupProducer != nil {
		t.Fatal("backup producer should be nil initially")
	}
	if track.backupSid != "" {
		t.Fatal("backup sid should be empty initially")
	}
}
