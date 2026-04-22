package rtc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/store"
)

// waitFor polls fn until it returns true or the deadline elapses. Used to
// let KV watch events propagate asynchronously inside the forwarder.
func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func seedSrcParticipant(t *testing.T, srcStore *RoomStore, sid, identity string) *proto.ParticipantInfo {
	t.Helper()
	info := &proto.ParticipantInfo{
		Sid:      sid,
		Identity: identity,
		State:    proto.ParticipantInfo_JOINED,
		Name:     identity,
		NodeId:   "node-src",
	}
	srcStore.PutParticipant(info)
	return info
}

func seedSrcTrack(srcStore *RoomStore, sid, participantSid string) *proto.TrackInfo {
	info := &proto.TrackInfo{
		Sid:            sid,
		ParticipantSid: participantSid,
		Name:           sid,
	}
	srcStore.PutTrack(info)
	return info
}

func newStores(t *testing.T) (*RoomStore, *RoomStore) {
	t.Helper()
	kv := store.NewMemoryStore(0)
	t.Cleanup(func() { kv.Close() })
	return NewRoomStore(kv, "src-room", "node-test"), NewRoomStore(kv, "dst-room", "node-test")
}

func TestForwarderKey_Deterministic(t *testing.T) {
	a := forwarderKey("pa_alice", "roomA")
	b := forwarderKey("pa_alice", "roomA")
	if a != b {
		t.Fatalf("forwarderKey must be deterministic; got %s vs %s", a, b)
	}
	c := forwarderKey("pa_alice", "roomB")
	if a == c {
		t.Fatalf("different dstRoom must yield different key; both %s", a)
	}
	d := forwarderKey("pa_bob", "roomA")
	if a == d {
		t.Fatalf("different srcSid must yield different key; both %s", a)
	}
}

func TestCrossRoomForwarder_StartMirrorsParticipantAndTracks(t *testing.T) {
	src, dst := newStores(t)
	srcInfo := seedSrcParticipant(t, src, "pa_alice", "alice")
	seedSrcTrack(src, "rt-1.pr-1", srcInfo.Sid)
	seedSrcTrack(src, "rt-1.pr-2", srcInfo.Sid)

	fwd := NewCrossRoomForwarder(CrossRoomForwarderParams{
		SrcStore:       src,
		DstStore:       dst,
		SrcRoom:        "src-room",
		DstRoom:        "dst-room",
		SrcParticipant: srcInfo,
	})
	if err := fwd.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer fwd.Close()

	// Participant is mirrored under the SRC Sid (not a synthetic one).
	got := dst.GetParticipant(srcInfo.Sid)
	if got == nil {
		t.Fatalf("expected participant %q in dst store", srcInfo.Sid)
	}
	if got.Identity != srcInfo.Identity {
		t.Fatalf("identity mismatch: want %q got %q", srcInfo.Identity, got.Identity)
	}
	if got.Attributes[ForwardedFromRoomAttr] != "src-room" {
		t.Fatalf("expected ForwardedFromRoomAttr=src-room, got %v", got.Attributes)
	}

	// Both tracks mirrored verbatim.
	tracks := dst.GetAllTracks()[srcInfo.Sid]
	if len(tracks) != 2 {
		t.Fatalf("expected 2 mirrored tracks, got %d", len(tracks))
	}
}

func TestCrossRoomForwarder_StartFailsWhenSrcMissing(t *testing.T) {
	src, dst := newStores(t)
	fwd := NewCrossRoomForwarder(CrossRoomForwarderParams{
		SrcStore: src, DstStore: dst,
		SrcRoom: "src-room", DstRoom: "dst-room",
		SrcParticipant: &proto.ParticipantInfo{Sid: "pa_ghost", Identity: "ghost"},
	})
	if err := fwd.Start(context.Background()); err == nil {
		t.Fatal("expected error when src participant is absent")
	}
}

func TestCrossRoomForwarder_StartFailsWhenSrcDisconnected(t *testing.T) {
	src, dst := newStores(t)
	srcInfo := seedSrcParticipant(t, src, "pa_alice", "alice")
	srcInfo.State = proto.ParticipantInfo_DISCONNECTED
	src.PutParticipant(srcInfo)

	fwd := NewCrossRoomForwarder(CrossRoomForwarderParams{
		SrcStore: src, DstStore: dst,
		SrcRoom: "src-room", DstRoom: "dst-room",
		SrcParticipant: srcInfo,
	})
	if err := fwd.Start(context.Background()); err == nil {
		t.Fatal("expected error when src participant is already disconnected")
	}
}

func TestCrossRoomForwarder_MirrorsNewTrackPublishedAfterStart(t *testing.T) {
	src, dst := newStores(t)
	srcInfo := seedSrcParticipant(t, src, "pa_alice", "alice")

	fwd := NewCrossRoomForwarder(CrossRoomForwarderParams{
		SrcStore: src, DstStore: dst,
		SrcRoom: "src-room", DstRoom: "dst-room",
		SrcParticipant: srcInfo,
	})
	if err := fwd.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer fwd.Close()

	seedSrcTrack(src, "rt-1.pr-late", srcInfo.Sid)

	waitFor(t, time.Second, func() bool {
		return len(dst.GetAllTracks()[srcInfo.Sid]) == 1
	})
}

func TestCrossRoomForwarder_UnmirrorsTrackDeletedAfterStart(t *testing.T) {
	src, dst := newStores(t)
	srcInfo := seedSrcParticipant(t, src, "pa_alice", "alice")
	seedSrcTrack(src, "rt-1.pr-1", srcInfo.Sid)

	fwd := NewCrossRoomForwarder(CrossRoomForwarderParams{
		SrcStore: src, DstStore: dst,
		SrcRoom: "src-room", DstRoom: "dst-room",
		SrcParticipant: srcInfo,
	})
	if err := fwd.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer fwd.Close()

	// Verify track is initially mirrored, then remove it on the src side.
	waitFor(t, time.Second, func() bool { return len(dst.GetAllTracks()[srcInfo.Sid]) == 1 })

	src.DeleteTrack("rt-1.pr-1")

	waitFor(t, time.Second, func() bool { return len(dst.GetAllTracks()[srcInfo.Sid]) == 0 })
}

func TestCrossRoomForwarder_TearsDownOnSrcDisconnected(t *testing.T) {
	src, dst := newStores(t)
	srcInfo := seedSrcParticipant(t, src, "pa_alice", "alice")

	var closedKey atomic.Value
	fwd := NewCrossRoomForwarder(CrossRoomForwarderParams{
		SrcStore: src, DstStore: dst,
		SrcRoom: "src-room", DstRoom: "dst-room",
		SrcParticipant: srcInfo,
		OnClose:        func(k string) { closedKey.Store(k) },
	})
	if err := fwd.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	updated := cloneParticipantInfo(srcInfo)
	updated.State = proto.ParticipantInfo_DISCONNECTED
	updated.DisconnectReason = proto.DisconnectReason_CLIENT_INITIATED
	src.PutParticipant(updated)

	waitFor(t, time.Second, func() bool { return dst.GetParticipant(srcInfo.Sid) == nil })
	waitFor(t, time.Second, func() bool {
		v, _ := closedKey.Load().(string)
		return v == fwd.Key()
	})
}

func TestCrossRoomForwarder_TearsDownOnDstDisconnected(t *testing.T) {
	src, dst := newStores(t)
	srcInfo := seedSrcParticipant(t, src, "pa_alice", "alice")

	var closedKey atomic.Value
	fwd := NewCrossRoomForwarder(CrossRoomForwarderParams{
		SrcStore: src, DstStore: dst,
		SrcRoom: "src-room", DstRoom: "dst-room",
		SrcParticipant: srcInfo,
		OnClose:        func(k string) { closedKey.Store(k) },
	})
	if err := fwd.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Simulate RemoveParticipantFromRoom writing DISCONNECTED on the dst entry.
	kick := dst.GetParticipant(srcInfo.Sid)
	if kick == nil {
		t.Fatal("expected initial mirror in dst")
	}
	kick.State = proto.ParticipantInfo_DISCONNECTED
	kick.DisconnectReason = proto.DisconnectReason_PARTICIPANT_REMOVED
	dst.PutParticipant(kick)

	waitFor(t, time.Second, func() bool {
		v, _ := closedKey.Load().(string)
		return v == fwd.Key()
	})
	waitFor(t, time.Second, func() bool { return dst.GetParticipant(srcInfo.Sid) == nil })
}

func TestCrossRoomForwarder_CloseIdempotent(t *testing.T) {
	src, dst := newStores(t)
	srcInfo := seedSrcParticipant(t, src, "pa_alice", "alice")

	var calls atomic.Int32
	fwd := NewCrossRoomForwarder(CrossRoomForwarderParams{
		SrcStore: src, DstStore: dst,
		SrcRoom: "src-room", DstRoom: "dst-room",
		SrcParticipant: srcInfo,
		OnClose:        func(string) { calls.Add(1) },
	})
	if err := fwd.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	fwd.Close()
	fwd.Close()
	fwd.Close()
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected OnClose called exactly once, got %d", got)
	}
}
