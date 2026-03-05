package discovery

import (
	"testing"

	"github.com/axonkit/axonkit/pkg/store"
	"github.com/axonkit/axonkit/pkg/types"
)

func TestSessionRegistry_RegisterLookup(t *testing.T) {
	ms := store.NewMemoryStore(0)
	defer ms.Close()

	sr := NewSessionRegistry(ms, "node-1")

	info := SessionInfo{
		NodeID:   "node-1",
		RoomName: "room-1",
		Identity: "alice",
	}

	if err := sr.Register("sid-1", info); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	got, err := sr.Lookup("sid-1")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if got.NodeID != "node-1" || got.Identity != "alice" || got.RoomName != "room-1" {
		t.Fatalf("unexpected session info: %+v", got)
	}
}

func TestSessionRegistry_LookupNotFound(t *testing.T) {
	ms := store.NewMemoryStore(0)
	defer ms.Close()

	sr := NewSessionRegistry(ms, "node-1")

	_, err := sr.Lookup("nonexistent")
	if err == nil {
		t.Fatal("expected error for missing session")
	}
}

func TestSessionRegistry_Unregister(t *testing.T) {
	ms := store.NewMemoryStore(0)
	defer ms.Close()

	sr := NewSessionRegistry(ms, "node-1")

	sr.Register("sid-1", SessionInfo{NodeID: "node-1", Identity: "alice"})

	if err := sr.Unregister("sid-1"); err != nil {
		t.Fatalf("Unregister failed: %v", err)
	}

	_, err := sr.Lookup("sid-1")
	if err == nil {
		t.Fatal("expected error after unregister")
	}
}

func TestSessionRegistry_KeepAlive(t *testing.T) {
	ms := store.NewMemoryStore(0)
	defer ms.Close()

	sr := NewSessionRegistry(ms, "node-1")

	info := SessionInfo{NodeID: "node-1", Identity: "bob"}
	sr.Register("sid-2", info)

	info.RoomName = "room-updated"
	if err := sr.KeepAlive("sid-2", info); err != nil {
		t.Fatalf("KeepAlive failed: %v", err)
	}

	got, _ := sr.Lookup("sid-2")
	if got.RoomName != "room-updated" {
		t.Fatalf("expected updated room name, got %s", got.RoomName)
	}
}

func TestSessionRegistry_Close(t *testing.T) {
	ms := store.NewMemoryStore(0)
	sr := NewSessionRegistry(ms, types.NodeID("node-1"))

	if err := sr.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}
