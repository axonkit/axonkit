package rtc

import (
	"strings"
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/store"
)

// seedParticipant writes a JOINED ParticipantInfo row for a given (room,
// identity) into the shared KV. It is the KV-only equivalent of a joined
// participant — good enough for the store-driven forwarding paths that
// ForwardParticipant / RemoveParticipantFromRoom operate on.
func seedParticipant(kv store.Store, room, sid, identity, nodeId string) *proto.ParticipantInfo {
	rs := NewRoomStore(kv, room, "seed")
	info := &proto.ParticipantInfo{
		Sid:      sid,
		Identity: identity,
		State:    proto.ParticipantInfo_JOINED,
		Name:     identity,
		NodeId:   nodeId,
	}
	rs.PutParticipant(info)
	return info
}

func TestRoomManager_ForwardParticipant_ValidationErrors(t *testing.T) {
	rm := NewRoomManager(testRMParams(store.NewMemoryStore(0)))
	defer rm.Close()

	cases := []struct {
		name               string
		src, identity, dst string
		wantErrSubstr      string
	}{
		{"empty srcRoom", "", "alice", "dst", "invalid srcRoom"},
		{"empty dstRoom", "src", "alice", "", "invalid dstRoom"},
		{"same room", "same", "alice", "same", "must differ"},
		{"empty identity", "src", "", "dst", "srcIdentity is required"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := rm.ForwardParticipant(c.src, c.identity, c.dst)
			if err == nil || !strings.Contains(err.Error(), c.wantErrSubstr) {
				t.Fatalf("want error containing %q, got %v", c.wantErrSubstr, err)
			}
		})
	}
}

func TestRoomManager_ForwardParticipant_NotFound(t *testing.T) {
	rm := NewRoomManager(testRMParams(store.NewMemoryStore(0)))
	defer rm.Close()

	_, err := rm.ForwardParticipant("src-room", "ghost", "dst-room")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("want 'not found' error, got %v", err)
	}
}

func TestRoomManager_ForwardParticipant_Success(t *testing.T) {
	kv := store.NewMemoryStore(0)
	rm := NewRoomManager(testRMParams(kv))
	defer rm.Close()

	src := seedParticipant(kv, "src-room", "pa_alice", "alice", "node-src")

	got, err := rm.ForwardParticipant("src-room", "alice", "dst-room")
	if err != nil {
		t.Fatalf("ForwardParticipant: %v", err)
	}
	if got == nil || got.Sid != src.Sid {
		t.Fatalf("expected forwarded participant to keep src sid %q, got %+v", src.Sid, got)
	}
	if got.Identity != "alice" {
		t.Fatalf("identity mismatch: %q", got.Identity)
	}
	if got.Attributes[ForwardedFromRoomAttr] != "src-room" {
		t.Fatalf("expected ForwardedFromRoomAttr=src-room, got %v", got.Attributes)
	}
	if keys := rm.ActiveForwarders(); len(keys) != 1 {
		t.Fatalf("expected 1 active forwarder, got %d (%v)", len(keys), keys)
	}
}

func TestRoomManager_ForwardParticipant_Idempotent(t *testing.T) {
	kv := store.NewMemoryStore(0)
	rm := NewRoomManager(testRMParams(kv))
	defer rm.Close()

	seedParticipant(kv, "src-room", "pa_alice", "alice", "node-src")

	first, err := rm.ForwardParticipant("src-room", "alice", "dst-room")
	if err != nil {
		t.Fatalf("first ForwardParticipant: %v", err)
	}
	second, err := rm.ForwardParticipant("src-room", "alice", "dst-room")
	if err != nil {
		t.Fatalf("second ForwardParticipant: %v", err)
	}
	if first.Sid != second.Sid {
		t.Fatalf("sid mismatch across calls: %q vs %q", first.Sid, second.Sid)
	}
	if n := len(rm.ActiveForwarders()); n != 1 {
		t.Fatalf("expected idempotent: 1 active forwarder, got %d", n)
	}
}

func TestRoomManager_RemoveParticipantFromRoom_ForwardedTearsDown(t *testing.T) {
	kv := store.NewMemoryStore(0)
	rm := NewRoomManager(testRMParams(kv))
	defer rm.Close()

	seedParticipant(kv, "src-room", "pa_alice", "alice", "node-src")

	if _, err := rm.ForwardParticipant("src-room", "alice", "dst-room"); err != nil {
		t.Fatalf("ForwardParticipant: %v", err)
	}

	if err := rm.RemoveParticipantFromRoom("dst-room", "alice"); err != nil {
		t.Fatalf("RemoveParticipantFromRoom: %v", err)
	}

	// The forwarder's runDst watcher should observe the DISCONNECTED write
	// and tear itself down, removing the mirrored participant and clearing
	// the RoomManager registry.
	deadline := time.Now().Add(time.Second)
	dstStore := NewRoomStore(kv, "dst-room", "verify")
	for time.Now().Before(deadline) {
		if len(rm.ActiveForwarders()) == 0 && dstStore.GetParticipant("pa_alice") == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("forwarder did not tear down: active=%v dst=%+v",
		rm.ActiveForwarders(), dstStore.GetParticipant("pa_alice"))
}

func TestRoomManager_RemoveParticipantFromRoom_WritesDisconnectedForReal(t *testing.T) {
	kv := store.NewMemoryStore(0)
	rm := NewRoomManager(testRMParams(kv))
	defer rm.Close()

	seedParticipant(kv, "my-room", "pa_alice", "alice", "node-1")

	if err := rm.RemoveParticipantFromRoom("my-room", "alice"); err != nil {
		t.Fatalf("RemoveParticipantFromRoom: %v", err)
	}

	rs := NewRoomStore(kv, "my-room", "verify")
	got := rs.GetParticipant("pa_alice")
	if got == nil {
		t.Fatal("expected participant row to still exist (marked DISCONNECTED)")
	}
	if got.State != proto.ParticipantInfo_DISCONNECTED {
		t.Fatalf("expected state=DISCONNECTED, got %v", got.State)
	}
	if got.DisconnectReason != proto.DisconnectReason_PARTICIPANT_REMOVED {
		t.Fatalf("expected reason=PARTICIPANT_REMOVED, got %v", got.DisconnectReason)
	}
}

func TestRoomManager_RemoveParticipantFromRoom_UnknownIsNoop(t *testing.T) {
	rm := NewRoomManager(testRMParams(store.NewMemoryStore(0)))
	defer rm.Close()

	if err := rm.RemoveParticipantFromRoom("nobody-home", "ghost"); err != nil {
		t.Fatalf("expected no error for unknown identity, got %v", err)
	}
}

func TestRoomManager_RemoveParticipantFromRoom_Validation(t *testing.T) {
	rm := NewRoomManager(testRMParams(store.NewMemoryStore(0)))
	defer rm.Close()

	if err := rm.RemoveParticipantFromRoom("", "alice"); err == nil {
		t.Fatal("expected error for empty room")
	}
	if err := rm.RemoveParticipantFromRoom("room", ""); err == nil {
		t.Fatal("expected error for empty identity")
	}
}
