package rtc

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/media"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/routing"
	"github.com/axonkit/axonkit/pkg/store"
	"github.com/axonkit/axonkit/pkg/types"
)

func defaultRMConfig() config.RoomConfig {
	return config.RoomConfig{
		EmptyTimeout:     5 * time.Minute,
		DepartureTimeout: 500 * time.Millisecond,
		MaxParticipants:  100,
		AutoSubscribe:    true,
		Dynacast:         true,
	}
}

func testRMParams(kv store.Store) RoomManagerParams {
	return RoomManagerParams{
		Config:  defaultRMConfig(),
		KV:      kv,
		LeaseKV: store.NewMemoryStore(0),
	}
}

func TestRoomManager_GetOrCreateRoom(t *testing.T) {
	rm := NewRoomManager(testRMParams(store.NewMemoryStore(0)))
	defer rm.Close()

	room := rm.GetOrCreateRoom("test-room", 0, "")
	if room == nil || room.Name() != "test-room" {
		t.Fatal("expected room")
	}

	room2 := rm.GetOrCreateRoom("test-room", 0, "")
	if room2.Sid() != room.Sid() {
		t.Fatal("expected same room on duplicate GetOrCreateRoom")
	}
}

func TestRoomManager_GetRoom(t *testing.T) {
	rm := NewRoomManager(testRMParams(store.NewMemoryStore(0)))
	defer rm.Close()

	if rm.GetRoom("nonexistent") != nil {
		t.Fatal("expected nil for nonexistent room")
	}

	room := rm.GetOrCreateRoom("test-room", 0, "")
	if rm.GetRoom("test-room") != room {
		t.Fatal("expected room by name")
	}
}

func TestRoomManager_GetRoomBySid(t *testing.T) {
	rm := NewRoomManager(testRMParams(store.NewMemoryStore(0)))
	defer rm.Close()

	if rm.GetRoomBySid("nonexistent") != nil {
		t.Fatal("expected nil")
	}

	room := rm.GetOrCreateRoom("test-room", 0, "")
	if rm.GetRoomBySid(room.Sid()) != room {
		t.Fatal("expected room by sid")
	}
}

func TestRoomManager_GetParticipantBySid(t *testing.T) {
	rm := NewRoomManager(testRMParams(store.NewMemoryStore(0)))
	defer rm.Close()

	if rm.GetParticipantBySid("nonexistent") != nil {
		t.Fatal("expected nil")
	}

	room := rm.GetOrCreateRoom("test-room", 0, "")
	p := newTestParticipant("alice")
	defer p.Close(proto.DisconnectReason_CLIENT_INITIATED)
	room.AddParticipant(p)

	found := rm.GetParticipantBySid("sid-alice")
	if found == nil || found.Identity() != "alice" {
		t.Fatal("expected to find alice")
	}
}

func TestRoomManager_RoomRemoveOnClose(t *testing.T) {
	rm := NewRoomManager(testRMParams(store.NewMemoryStore(0)))
	defer rm.Close()

	room := rm.GetOrCreateRoom("test-room", 0, "")
	room.Close(proto.DisconnectReason_ROOM_CLOSED)

	if rm.GetRoom("test-room") != nil {
		t.Fatal("expected room to be removed after close")
	}
	if rm.GetRoomBySid(room.Sid()) != nil {
		t.Fatal("expected room to be removed by sid after close")
	}
}

func TestRoomManager_Close(t *testing.T) {
	rm := NewRoomManager(testRMParams(store.NewMemoryStore(0)))
	rm.GetOrCreateRoom("room-1", 0, "")
	rm.GetOrCreateRoom("room-2", 0, "")

	rm.Close()

	if rm.GetRoom("room-1") != nil || rm.GetRoom("room-2") != nil {
		t.Fatal("expected all rooms closed")
	}
}

func TestRoomManager_MaxParticipantsOverride(t *testing.T) {
	cfg := defaultRMConfig()
	cfg.MaxParticipants = 50
	kv := store.NewMemoryStore(0)
	rm := NewRoomManager(RoomManagerParams{Config: cfg, KV: kv, LeaseKV: store.NewMemoryStore(0)})
	defer rm.Close()

	room := rm.GetOrCreateRoom("test-room", 10, "")
	if room.config.MaxParticipants != 10 {
		t.Fatalf("expected maxParticipants=10, got %d", room.config.MaxParticipants)
	}

	room2 := rm.GetOrCreateRoom("test-room-2", 0, "")
	if room2.config.MaxParticipants != 50 {
		t.Fatalf("expected maxParticipants=50 (default), got %d", room2.config.MaxParticipants)
	}
}

func TestRoomManager_CascadeCallbacks(t *testing.T) {
	rm := NewRoomManager(testRMParams(store.NewMemoryStore(0)))
	defer rm.Close()

	room := rm.GetOrCreateRoom("test-room", 0, "")

	// Verify cascade callbacks are wired
	if room.cascade.OnTrackPublished == nil {
		t.Fatal("expected OnTrackPublished callback")
	}
	if room.cascade.OnTrackUnpublished == nil {
		t.Fatal("expected OnTrackUnpublished callback")
	}
	if room.cascade.OnDataTrackPublished == nil {
		t.Fatal("expected OnDataTrackPublished callback")
	}
	if room.cascade.OnDataTrackUnpublished == nil {
		t.Fatal("expected OnDataTrackUnpublished callback")
	}
	if room.cascade.EnsureStream == nil {
		t.Fatal("expected EnsureStream callback")
	}
	if room.cascade.ReleaseStream == nil {
		t.Fatal("expected ReleaseStream callback")
	}
}

func TestRoomManager_CascadeEnsureStream_LocalSkip(t *testing.T) {
	rm := NewRoomManager(testRMParams(store.NewMemoryStore(0)))
	defer rm.Close()

	room := rm.GetOrCreateRoom("test-room", 0, "")
	// EnsureStream with no mediaMgr/fib should be no-op
	if err := room.cascade.EnsureStream("rt-xxx.pr-yyy"); err != nil {
		t.Fatalf("expected no error for nil deps, got %v", err)
	}
}

func TestRoomManager_CascadeReleaseStream_NilFIB(t *testing.T) {
	rm := NewRoomManager(testRMParams(store.NewMemoryStore(0)))
	defer rm.Close()

	room := rm.GetOrCreateRoom("test-room", 0, "")
	// ReleaseStream with no fib should be no-op (no panic)
	room.cascade.ReleaseStream("rt-xxx.pr-yyy")
}

func TestRoomManager_CascadeCallbacks_WithRIBAndMedia(t *testing.T) {
	ms := store.NewMemoryStore(0)
	defer ms.Close()

	nodeInfo := types.NodeInfo{ID: "node-1", Region: "us-east", Cluster: "c1"}
	rib := routing.NewRIB(ms, nodeInfo)
	defer rib.Close()

	mediaMgr := media.NewManager(nodeInfo.ID, nil, "", config.WebRtcConfig{})

	rm := NewRoomManager(RoomManagerParams{
		Config:       defaultRMConfig(),
		RIB:          rib,
		MediaManager: mediaMgr,
		KV:           store.NewMemoryStore(0),
		LeaseKV:      store.NewMemoryStore(0),
	})
	defer rm.Close()

	room := rm.GetOrCreateRoom("cascade-room", 0, "")

	// OnTrackPublished callback should register with media and RIB
	room.cascade.OnTrackPublished("rt-aaa.pr-bbb", nil)
	time.Sleep(100 * time.Millisecond)

	streamID := types.StreamID("rt-aaa.pr-bbb")
	if mediaMgr.GetLocalProducer(streamID) == nil {
		t.Fatal("expected local producer registered")
	}

	// OnTrackUnpublished callback should unregister
	room.cascade.OnTrackUnpublished("rt-aaa.pr-bbb")
	time.Sleep(100 * time.Millisecond)

	if mediaMgr.GetLocalProducer(streamID) != nil {
		t.Fatal("expected local producer unregistered")
	}

	// OnDataTrackPublished
	room.cascade.OnDataTrackPublished("rt-aaa.dp-ccc", nil)
	time.Sleep(100 * time.Millisecond)

	dStreamID := types.StreamID("rt-aaa.dp-ccc")
	if mediaMgr.GetLocalProducer(dStreamID) == nil {
		t.Fatal("expected data producer registered")
	}

	// OnDataTrackUnpublished
	room.cascade.OnDataTrackUnpublished("rt-aaa.dp-ccc")
}

func TestRoomManager_CascadeEnsureStream_WithLocalProducer(t *testing.T) {
	ms := store.NewMemoryStore(0)
	defer ms.Close()

	nodeInfo := types.NodeInfo{ID: "node-1", Region: "us-east", Cluster: "c1"}
	rib := routing.NewRIB(ms, nodeInfo)
	defer rib.Close()

	mediaMgr := media.NewManager(nodeInfo.ID, nil, "", config.WebRtcConfig{})
	fib := routing.NewFIB(rib, nodeInfo.ID)
	defer fib.Close()

	rm := NewRoomManager(RoomManagerParams{
		Config:       defaultRMConfig(),
		RIB:          rib,
		FIB:          fib,
		MediaManager: mediaMgr,
		KV:           store.NewMemoryStore(0),
		LeaseKV:      store.NewMemoryStore(0),
	})
	defer rm.Close()

	room := rm.GetOrCreateRoom("ensure-room", 0, "")

	// Register a local producer first
	streamID := types.StreamID("rt-xxx.pr-yyy")
	mediaMgr.RegisterProducer(streamID, nil)

	// EnsureStream should skip FIB because producer is local
	if err := room.cascade.EnsureStream(string(streamID)); err != nil {
		t.Fatalf("expected no error for local producer, got %v", err)
	}
}

func TestRoomManager_CascadeReleaseStream_WithLocalProducer(t *testing.T) {
	ms := store.NewMemoryStore(0)
	defer ms.Close()

	nodeInfo := types.NodeInfo{ID: "node-1", Region: "us-east", Cluster: "c1"}
	rib := routing.NewRIB(ms, nodeInfo)
	defer rib.Close()

	mediaMgr := media.NewManager(nodeInfo.ID, nil, "", config.WebRtcConfig{})
	fib := routing.NewFIB(rib, nodeInfo.ID)
	defer fib.Close()

	rm := NewRoomManager(RoomManagerParams{
		Config:       defaultRMConfig(),
		RIB:          rib,
		FIB:          fib,
		MediaManager: mediaMgr,
		KV:           store.NewMemoryStore(0),
		LeaseKV:      store.NewMemoryStore(0),
	})
	defer rm.Close()

	room := rm.GetOrCreateRoom("release-room", 0, "")

	// Register a local producer
	streamID := types.StreamID("rt-xxx.pr-yyy")
	mediaMgr.RegisterProducer(streamID, nil)

	// ReleaseStream should skip FIB because producer is local
	room.cascade.ReleaseStream(string(streamID))
}

func TestRoomManager_ConcurrentGetOrCreate(t *testing.T) {
	rm := NewRoomManager(testRMParams(store.NewMemoryStore(0)))
	defer rm.Close()

	var wg sync.WaitGroup
	rooms := make([]*Room, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rooms[idx] = rm.GetOrCreateRoom("shared-room", 0, "")
		}(i)
	}
	wg.Wait()

	sid := rooms[0].Sid()
	for _, r := range rooms {
		if r.Sid() != sid {
			t.Fatal("expected all goroutines to get the same room")
		}
	}
}

func TestValidateRoomName(t *testing.T) {
	for _, name := range []string{
		"r", "Room1", "test-room", "under_score", "a1B2c3",
		"中文", "会议室", "umlaut-ä", "Café_1",
	} {
		if err := ValidateRoomName(name); err != nil {
			t.Errorf("ValidateRoomName(%q): %v", name, err)
		}
	}
	for _, name := range []string{"", "bad.room", "bad room", "x!", "a@b", "emoji🔥"} {
		if err := ValidateRoomName(name); err == nil {
			t.Errorf("ValidateRoomName(%q): expected error", name)
		}
	}
	long := strings.Repeat("a", MaxRoomNameLen+1)
	if err := ValidateRoomName(long); err == nil {
		t.Fatal("expected error for length")
	}
	okLen := strings.Repeat("a", MaxRoomNameLen)
	if err := ValidateRoomName(okLen); err != nil {
		t.Fatal(err)
	}
	longUnicode := strings.Repeat("世", MaxRoomNameLen+1)
	if err := ValidateRoomName(longUnicode); err == nil {
		t.Fatal("expected error for unicode length")
	}
}

func TestMetaKeyRoomName(t *testing.T) {
	tests := []struct {
		key  string
		want string
		ok   bool
	}{
		{"room.foo.meta", "foo", true},
		{"room.a.b.meta", "a.b", true},
		{"room..meta", "", false},
		{"x.meta", "", false},
		{"room.foo.bar", "", false},
	}
	for _, tc := range tests {
		got, ok := MetaKeyRoomName(tc.key)
		if ok != tc.ok || got != tc.want {
			t.Errorf("MetaKeyRoomName(%q) = (%q, %v), want (%q, %v)", tc.key, got, ok, tc.want, tc.ok)
		}
	}
}

func TestRoomManager_SweeperPurgesMetaWithoutLease(t *testing.T) {
	kv := store.NewMemoryStore(0)
	leaseKV := store.NewMemoryStore(0)
	rm := NewRoomManager(RoomManagerParams{Config: defaultRMConfig(), KV: kv, LeaseKV: leaseKV})
	defer rm.Close()

	old := time.Now().Add(-time.Hour).UnixMilli()
	PutRoomMeta(kv, &RoomMeta{
		Name:         "orphan",
		Sid:          "rm_test",
		CreationTime: old,
		Config:       defaultRMConfig(),
	})
	ctx := context.Background()
	if err := kv.Put(ctx, "room.orphan.participant.p1", []byte("{}")); err != nil {
		t.Fatal(err)
	}

	rm.sweepOrphanRoomMeta(ctx)

	if _, err := GetRoomMeta(kv, "orphan"); err == nil {
		t.Fatal("expected meta purged")
	}
	keys, err := kv.Keys(ctx, "room.orphan.>")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("expected subkeys purged, got %v", keys)
	}
}

func TestRoomManager_SweeperSkipsFreshMetaWithoutLease(t *testing.T) {
	kv := store.NewMemoryStore(0)
	rm := NewRoomManager(RoomManagerParams{Config: defaultRMConfig(), KV: kv, LeaseKV: store.NewMemoryStore(0)})
	defer rm.Close()

	PutRoomMeta(kv, &RoomMeta{
		Name:         "brand-new",
		Sid:          "rm_new",
		CreationTime: time.Now().UnixMilli(),
		Config:       defaultRMConfig(),
	})
	ctx := context.Background()
	rm.sweepOrphanRoomMeta(ctx)

	if _, err := GetRoomMeta(kv, "brand-new"); err != nil {
		t.Fatal("expected meta kept during orphan grace window")
	}
}
