package whip

import (
	"sync/atomic"
	"testing"
	"time"
)

// The supervisor goroutine is started via Start. We drive it without a real
// mediasoup Transport (Start is relaxed to support this for tests and for
// the Keepalive-only case); markDisconnected simulates the IceState hook
// that production code would otherwise call.

func TestSession_SupervisorFiresAfterDisconnect(t *testing.T) {
	var called atomic.Int32
	sess := &Session{
		ID:                     "test-session",
		ICEDisconnectedTimeout: 60 * time.Millisecond,
		KeepaliveInterval:      20 * time.Millisecond,
	}
	sess.Start(func(id string) {
		if id != "test-session" {
			t.Errorf("unexpected id %q", id)
		}
		called.Add(1)
	})
	defer sess.Close()

	sess.markDisconnected(time.Now())

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && called.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if called.Load() == 0 {
		t.Fatalf("supervisor did not fire onExpire within deadline")
	}
}

func TestSession_SupervisorICERecoveredDisarms(t *testing.T) {
	var called atomic.Int32
	sess := &Session{
		ID:                     "test-session",
		ICEDisconnectedTimeout: 80 * time.Millisecond,
		KeepaliveInterval:      20 * time.Millisecond,
	}
	sess.Start(func(id string) { called.Add(1) })
	defer sess.Close()

	sess.markDisconnected(time.Now())
	// Recover well before the timeout. No onExpire should ever fire.
	time.Sleep(20 * time.Millisecond)
	sess.ICERecovered()

	time.Sleep(200 * time.Millisecond)
	if called.Load() != 0 {
		t.Fatalf("supervisor fired after ICERecovered (called=%d)", called.Load())
	}
}

func TestSession_KeepaliveTicks(t *testing.T) {
	var count atomic.Int32
	sess := &Session{
		ID:                "ka-session",
		KeepaliveInterval: 20 * time.Millisecond,
		Keepalive: func() {
			count.Add(1)
		},
	}
	sess.Start(nil)
	defer sess.Close()

	// First call is immediate; then every KeepaliveInterval. After ~100ms
	// we expect at least 3 calls.
	time.Sleep(100 * time.Millisecond)
	if got := count.Load(); got < 3 {
		t.Fatalf("expected >= 3 keepalive ticks, got %d", got)
	}
}

func TestSession_CloseStopsSupervisor(t *testing.T) {
	var called atomic.Int32
	sess := &Session{
		ID:                     "close-session",
		ICEDisconnectedTimeout: 30 * time.Millisecond,
		KeepaliveInterval:      10 * time.Millisecond,
	}
	sess.Start(func(id string) { called.Add(1) })
	sess.markDisconnected(time.Now())
	sess.Close()

	time.Sleep(150 * time.Millisecond)
	if called.Load() != 0 {
		t.Fatalf("supervisor fired after Close (called=%d)", called.Load())
	}
}

func TestSession_CloseIdempotent(t *testing.T) {
	sess := &Session{ID: "idem", KeepaliveInterval: 50 * time.Millisecond}
	sess.Start(nil)
	sess.Close()
	sess.Close() // must not panic (close of closed channel would)
}
