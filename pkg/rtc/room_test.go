package rtc

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/jiyeyuran/mediasoup-go/v2"
)

func defaultRoomConfig() config.RoomConfig {
	return config.RoomConfig{
		EmptyTimeout:     5 * time.Minute,
		DepartureTimeout: 200 * time.Millisecond,
		MaxParticipants:  100,
		AutoSubscribe:    true,
		Dynacast:         true,
	}
}

func newTestParticipant(identity string) *Participant {
	p := NewParticipant(ParticipantParams{
		Sid:      "sid-" + identity,
		Identity: identity,
		Name:     identity,
		Grants: &proto.ClaimGrants{
			Room: &proto.RoomGrant{
				Join: true,
			},
		},
		AutoSubscribe: true,
		PingInterval:  5 * time.Second,
		PingTimeout:   15 * time.Second,
	})
	p.SetState(proto.ParticipantInfo_CONNECTED)
	return p
}

func TestRoom_NewRoom(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})
	if r.Sid() != "sid-1" || r.Name() != "test-room" {
		t.Fatalf("unexpected sid/name: %s/%s", r.Sid(), r.Name())
	}
	if r.ParticipantCount() != 0 {
		t.Fatalf("expected 0 participants, got %d", r.ParticipantCount())
	}
}

func TestRoom_AddRemoveParticipant(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if err := r.AddParticipant(p); err != nil {
		t.Fatalf("AddParticipant failed: %v", err)
	}
	if r.ParticipantCount() != 1 {
		t.Fatalf("expected 1 participant, got %d", r.ParticipantCount())
	}
	if r.GetParticipant("sid-alice") == nil {
		t.Fatal("expected participant by sid")
	}
	if r.GetParticipantByIdentity("alice") == nil {
		t.Fatal("expected participant by identity")
	}

	r.RemoveParticipant("sid-alice", proto.DisconnectReason_CLIENT_INITIATED)
	if r.ParticipantCount() != 0 {
		t.Fatalf("expected 0 participants after remove")
	}
}

func TestRoom_AddParticipant_MaxParticipants(t *testing.T) {
	cfg := defaultRoomConfig()
	cfg.MaxParticipants = 1
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	p1 := newTestParticipant("alice")
	defer p1.Close(proto.DisconnectReason_CLIENT_INITIATED)
	p2 := newTestParticipant("bob")
	defer p2.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if err := r.AddParticipant(p1); err != nil {
		t.Fatalf("AddParticipant failed: %v", err)
	}
	if err := r.AddParticipant(p2); err != ErrRoomFull {
		t.Fatalf("expected ErrRoomFull, got %v", err)
	}
}

func TestRoom_AddParticipant_DuplicateIdentity(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})

	p1 := NewParticipant(ParticipantParams{
		Sid:      "sid-1",
		Identity: "alice",
		Name:     "alice",
		Grants:   &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
	})
	defer p1.Close(proto.DisconnectReason_CLIENT_INITIATED)

	p2 := NewParticipant(ParticipantParams{
		Sid:      "sid-2",
		Identity: "alice",
		Name:     "alice",
		Grants:   &proto.ClaimGrants{Room: &proto.RoomGrant{Join: true}},
	})
	defer p2.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if err := r.AddParticipant(p1); err != nil {
		t.Fatalf("AddParticipant p1 failed: %v", err)
	}
	if err := r.AddParticipant(p2); err != nil {
		t.Fatalf("AddParticipant p2 failed: %v", err)
	}
	// p1 should have been kicked
	time.Sleep(50 * time.Millisecond)
	if r.GetParticipant("sid-1") != nil {
		t.Fatal("expected old participant to be kicked")
	}
	if r.GetParticipant("sid-2") == nil {
		t.Fatal("expected new participant to exist")
	}
}

func TestRoom_AddParticipant_Closed(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})
	r.Close(proto.DisconnectReason_ROOM_CLOSED)

	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	if err := r.AddParticipant(p); err != ErrRoomClosed {
		t.Fatalf("expected ErrRoomClosed, got %v", err)
	}
}

func TestRoom_Close_Idempotent(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})
	r.Close(proto.DisconnectReason_ROOM_CLOSED)
	r.Close(proto.DisconnectReason_ROOM_CLOSED) // should not panic
}

func TestRoom_GetOtherParticipants(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})
	p1 := newTestParticipant("alice")
	p2 := newTestParticipant("bob")
	p3 := newTestParticipant("charlie")
	defer p1.Close(proto.DisconnectReason_CLIENT_INITIATED)
	defer p2.Close(proto.DisconnectReason_CLIENT_INITIATED)
	defer p3.Close(proto.DisconnectReason_CLIENT_INITIATED)

	r.AddParticipant(p1)
	r.AddParticipant(p2)
	r.AddParticipant(p3)

	others := r.GetOtherParticipants("sid-alice")
	if len(others) != 2 {
		t.Fatalf("expected 2 others, got %d", len(others))
	}
	for _, o := range others {
		if o.Sid() == "sid-alice" {
			t.Fatal("should not include self")
		}
	}
}

func TestRoom_RemoveParticipant_NotFound(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})
	r.RemoveParticipant("nonexistent", proto.DisconnectReason_CLIENT_INITIATED) // should not panic
}

func TestRoom_ToProto(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})
	p1 := newTestParticipant("alice")
	defer p1.Close(proto.DisconnectReason_CLIENT_INITIATED)

	r.AddParticipant(p1)
	info := r.ToProto()
	if info.Sid != "sid-1" || info.Name != "test-room" {
		t.Fatalf("unexpected proto: %+v", info)
	}
	if info.NumParticipants != 1 {
		t.Fatalf("expected 1 participant, got %d", info.NumParticipants)
	}
}

func TestRoom_EmptyTimer(t *testing.T) {
	cfg := defaultRoomConfig()
	cfg.DepartureTimeout = 100 * time.Millisecond
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	var closed atomic.Int32
	r.SetOnClose(func(*Room) {
		closed.Add(1)
	})

	p := newTestParticipant("alice")
	r.AddParticipant(p)
	r.RemoveParticipant("sid-alice", proto.DisconnectReason_CLIENT_INITIATED)
	p.Close(proto.DisconnectReason_CLIENT_INITIATED)

	time.Sleep(300 * time.Millisecond)
	if closed.Load() != 1 {
		t.Fatal("expected room to close after empty timer")
	}
}

func TestRoom_SetMetadata(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})
	r.SetMetadata("hello")
	if r.Metadata() != "hello" {
		t.Fatalf("expected metadata 'hello', got %q", r.Metadata())
	}
}

func TestRoom_Dynacast(t *testing.T) {
	cfg := defaultRoomConfig()
	cfg.Dynacast = true
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       cfg,
		CreationTime: time.Now(),
	})
	if !r.Dynacast() {
		t.Fatal("expected Dynacast = true")
	}
	cfg.Dynacast = false
	r2 := NewRoom(RoomParams{
		Sid:          "sid-2",
		Name:         "test-room-2",
		Config:       cfg,
		CreationTime: time.Now(),
	})
	if r2.Dynacast() {
		t.Fatal("expected Dynacast = false")
	}
}

func TestRoom_CascadeCallbacks_Publish(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})

	var published, unpublished []string
	r.cascade = CascadeCallbacks{
		OnTrackPublished:       func(sid string, p *mediasoup.Producer) { published = append(published, sid) },
		OnTrackUnpublished:     func(sid string) { unpublished = append(unpublished, sid) },
		OnDataTrackPublished:   func(sid string, dp *mediasoup.DataProducer) { published = append(published, sid) },
		OnDataTrackUnpublished: func(sid string) { unpublished = append(unpublished, sid) },
	}

	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)
	r.AddParticipant(p)

	r.OnTrackUnpublished(p, "stream-1")
	if len(unpublished) != 1 || unpublished[0] != "stream-1" {
		t.Fatalf("expected unpublished callback for stream-1")
	}

	r.OnDataTrackUnpublished(p, "data-stream-1")
	if len(unpublished) != 2 || unpublished[1] != "data-stream-1" {
		t.Fatalf("expected unpublished callback for data-stream-1")
	}
}

func TestRoom_Close_KicksParticipants(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})

	var onCloseInvoked atomic.Int32
	r.SetOnClose(func(*Room) {
		onCloseInvoked.Add(1)
	})

	p1 := newTestParticipant("alice")
	p2 := newTestParticipant("bob")
	r.AddParticipant(p1)
	r.AddParticipant(p2)

	r.Close(proto.DisconnectReason_ROOM_CLOSED)
	time.Sleep(50 * time.Millisecond)

	if r.ParticipantCount() != 0 {
		t.Fatalf("expected 0 participants after close")
	}
	if onCloseInvoked.Load() != 1 {
		t.Fatal("expected onClose to be invoked")
	}
}

func TestRoom_FindPublishedTrack(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})
	if r.FindPublishedTrack("nonexistent") != nil {
		t.Fatal("expected nil for nonexistent track")
	}
}

func TestRoom_OnTrackPublished(t *testing.T) {
	cfg := defaultRoomConfig()
	cfg.AutoSubscribe = false
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	var published []string
	r.cascade = CascadeCallbacks{
		OnTrackPublished: func(sid string, p *mediasoup.Producer) { published = append(published, sid) },
	}

	pub := newTestParticipant("alice")
	sub := newTestParticipant("bob")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	defer sub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	r.AddParticipant(pub)
	r.AddParticipant(sub)

	track := &PublishedTrack{sid: "track-1", name: "video"}
	r.OnTrackPublished(pub, track)

	if len(published) != 1 || published[0] != "track-1" {
		t.Fatal("expected cascade callback for OnTrackPublished")
	}
}

func TestRoom_OnTrackPublished_AutoSubscribe(t *testing.T) {
	cfg := defaultRoomConfig()
	cfg.AutoSubscribe = true
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	pub := newTestParticipant("alice")
	sub := newTestParticipant("bob")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	defer sub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	r.AddParticipant(pub)
	r.AddParticipant(sub)

	track := &PublishedTrack{sid: "track-1", name: "video"}
	r.OnTrackPublished(pub, track)
	// Auto-subscribe will fail (no mediasoup) but exercises the code path
	time.Sleep(50 * time.Millisecond)
}

func TestRoom_OnTrackMuted(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})
	pub := newTestParticipant("alice")
	sub := newTestParticipant("bob")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	defer sub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	r.AddParticipant(pub)
	r.AddParticipant(sub)

	r.OnTrackMuted(pub, "track-1", true)
	r.OnTrackMuted(pub, "track-1", false)
}

func TestRoom_OnDataTrackPublished(t *testing.T) {
	cfg := defaultRoomConfig()
	cfg.AutoSubscribe = true
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	var published []string
	r.cascade = CascadeCallbacks{
		OnDataTrackPublished: func(sid string, dp *mediasoup.DataProducer) { published = append(published, sid) },
	}

	pub := newTestParticipant("alice")
	sub := newTestParticipant("bob")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	defer sub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	r.AddParticipant(pub)
	r.AddParticipant(sub)

	track := &DataPublishedTrack{sid: "data-track-1", name: "data"}
	r.OnDataTrackPublished(pub, track)

	if len(published) != 1 || published[0] != "data-track-1" {
		t.Fatal("expected cascade callback for OnDataTrackPublished")
	}
	time.Sleep(50 * time.Millisecond)
}

func TestRoom_OnDataTrackPublished_NoAutoSubscribe(t *testing.T) {
	cfg := defaultRoomConfig()
	cfg.AutoSubscribe = false
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	pub := newTestParticipant("alice")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	r.AddParticipant(pub)

	track := &DataPublishedTrack{sid: "data-track-1"}
	r.OnDataTrackPublished(pub, track)
}

func TestRoom_SubscribeExistingTracks(t *testing.T) {
	cfg := defaultRoomConfig()
	cfg.AutoSubscribe = true
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	pub := newTestParticipant("alice")
	sub := newTestParticipant("bob")
	defer pub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	defer sub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	r.AddParticipant(pub)
	r.AddParticipant(sub)

	r.SubscribeExistingTracks(sub, nil) // no tracks, exercises path
}

func TestRoom_SubscribeExistingTracks_NoAutoSubscribe(t *testing.T) {
	cfg := defaultRoomConfig()
	cfg.AutoSubscribe = false
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	sub := newTestParticipant("alice")
	defer sub.Close(proto.DisconnectReason_CLIENT_INITIATED)
	r.AddParticipant(sub)

	r.SubscribeExistingTracks(sub, nil) // should return early
}

func TestRoom_FindPublishedTrack_MultiParticipant(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})

	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)
	r.AddParticipant(p)

	// Add a track manually (without mediasoup - just for lookup testing)
	track := &PublishedTrack{sid: "track-1", kind: "audio", name: "mic"}
	p.mu.Lock()
	p.publishedTracks["track-1"] = track
	p.mu.Unlock()

	found := r.FindPublishedTrack("track-1")
	if found == nil {
		t.Fatal("expected to find track-1")
	}
	if found.name != "mic" {
		t.Fatalf("expected name=mic, got %s", found.name)
	}

	// Track from different participant
	p2 := newTestParticipant("bob")
	defer p2.Close(proto.DisconnectReason_CLIENT_INITIATED)
	r.AddParticipant(p2)

	track2 := &PublishedTrack{sid: "track-2", kind: "video", name: "cam"}
	p2.mu.Lock()
	p2.publishedTracks["track-2"] = track2
	p2.mu.Unlock()

	found = r.FindPublishedTrack("track-2")
	if found == nil || found.name != "cam" {
		t.Fatal("expected to find track-2 from bob")
	}
}

func TestRoom_GetParticipantByIdentity(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)
	r.AddParticipant(p)

	found := r.GetParticipantByIdentity("alice")
	if found == nil || found.Sid() != "sid-alice" {
		t.Fatal("expected to find alice by identity")
	}
	if r.GetParticipantByIdentity("unknown") != nil {
		t.Fatal("expected nil for unknown identity")
	}
}

func TestRoom_CascadeCallbacks_Advertise(t *testing.T) {
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       defaultRoomConfig(),
		CreationTime: time.Now(),
	})

	published := make([]string, 0)
	unpublished := make([]string, 0)
	dataPublished := make([]string, 0)
	dataUnpublished := make([]string, 0)

	r.cascade = CascadeCallbacks{
		OnTrackPublished:       func(sid string, p *mediasoup.Producer) { published = append(published, sid) },
		OnTrackUnpublished:     func(sid string) { unpublished = append(unpublished, sid) },
		OnDataTrackPublished:   func(sid string, dp *mediasoup.DataProducer) { dataPublished = append(dataPublished, sid) },
		OnDataTrackUnpublished: func(sid string) { dataUnpublished = append(dataUnpublished, sid) },
	}

	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)
	r.AddParticipant(p)

	track := &PublishedTrack{sid: "track-1", kind: "audio"}
	r.OnTrackPublished(p, track)
	if len(published) != 1 || published[0] != "track-1" {
		t.Fatal("expected OnTrackPublished callback")
	}

	r.OnTrackUnpublished(p, "track-1")
	if len(unpublished) != 1 || unpublished[0] != "track-1" {
		t.Fatal("expected OnTrackUnpublished callback")
	}

	dt := &DataPublishedTrack{sid: "dtrack-1"}
	r.OnDataTrackPublished(p, dt)
	if len(dataPublished) != 1 || dataPublished[0] != "dtrack-1" {
		t.Fatal("expected OnDataTrackPublished callback")
	}

	r.OnDataTrackUnpublished(p, "dtrack-1")
	if len(dataUnpublished) != 1 || dataUnpublished[0] != "dtrack-1" {
		t.Fatal("expected OnDataTrackUnpublished callback")
	}
}

func TestRoom_TestHelpers(t *testing.T) {
	cfg := defaultRoomConfig()
	r := NewRoom(RoomParams{
		Sid:          "sid-1",
		Name:         "test-room",
		Config:       cfg,
		CreationTime: time.Now(),
	})

	// With no cascade callbacks, these should be no-ops
	if err := r.TestEnsureStream("stream-1"); err != nil {
		t.Fatalf("TestEnsureStream with nil callback: %v", err)
	}
	r.TestReleaseStream("stream-1")

	// With callbacks
	var ensured, released []string
	r.cascade = CascadeCallbacks{
		EnsureStream:  func(sid string) error { ensured = append(ensured, sid); return nil },
		ReleaseStream: func(sid string) { released = append(released, sid) },
	}
	if err := r.TestEnsureStream("stream-2"); err != nil {
		t.Fatalf("TestEnsureStream failed: %v", err)
	}
	r.TestReleaseStream("stream-2")
	if len(ensured) != 1 || ensured[0] != "stream-2" {
		t.Fatal("expected ensure callback")
	}
	if len(released) != 1 || released[0] != "stream-2" {
		t.Fatal("expected release callback")
	}
}
