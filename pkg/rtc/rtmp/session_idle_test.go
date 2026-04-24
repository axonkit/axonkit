package rtmp

import (
	"sync"
	"testing"
	"time"
)

// TestSessionIdleWatchdog_FiresCloser verifies the watchdog invokes the
// registered conn-closer when no media tag arrives within IdleTimeout.
//
// The watchdog must arm in SetConnCloser (the closer becoming available
// is what makes the timer meaningful in the first place) and must fire
// exactly once even if Close races the AfterFunc goroutine.
func TestSessionIdleWatchdog_FiresCloser(t *testing.T) {
	const idle = 40 * time.Millisecond

	var (
		mu     sync.Mutex
		fired  int
		doneCh = make(chan struct{}, 1)
	)

	s := NewSession(SessionParams{IdleTimeout: idle})
	s.SetConnCloser(func() {
		mu.Lock()
		fired++
		n := fired
		mu.Unlock()
		if n == 1 {
			doneCh <- struct{}{}
		}
	})

	select {
	case <-doneCh:
	case <-time.After(idle * 10):
		t.Fatalf("idle watchdog did not fire within %v", idle*10)
	}

	time.Sleep(idle * 3)

	mu.Lock()
	defer mu.Unlock()
	if fired != 1 {
		t.Fatalf("closer fired %d times, want 1", fired)
	}
}

// TestSessionIdleWatchdog_ResetOnActivity ensures a steady stream of
// tag ingest keeps the watchdog quiet. Close() must also stop the
// timer so no spurious close fires after the session is torn down.
func TestSessionIdleWatchdog_ResetOnActivity(t *testing.T) {
	const idle = 40 * time.Millisecond

	var (
		mu    sync.Mutex
		fired int
	)
	s := NewSession(SessionParams{IdleTimeout: idle})
	s.SetConnCloser(func() {
		mu.Lock()
		fired++
		mu.Unlock()
	})

	// Simulate continuous ingest by bumping the watchdog every idle/4
	// for a window that is comfortably larger than the timeout. Must
	// touch the watchdog through the same lock the ingest paths use.
	deadline := time.Now().Add(idle * 3)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		s.bumpIdleWatchdogLocked()
		s.mu.Unlock()
		time.Sleep(idle / 4)
	}

	s.Close()
	time.Sleep(idle * 2)

	mu.Lock()
	defer mu.Unlock()
	if fired != 0 {
		t.Fatalf("closer fired %d times during active ingest + after Close, want 0", fired)
	}
}

// TestSessionIdleWatchdog_DisabledWhenZero confirms IdleTimeout=0 means
// "never fire" — we rely on kernel keep-alive alone in that mode, and
// tests should be able to opt out deterministically.
func TestSessionIdleWatchdog_DisabledWhenZero(t *testing.T) {
	var (
		mu    sync.Mutex
		fired int
	)
	s := NewSession(SessionParams{IdleTimeout: 0})
	s.SetConnCloser(func() {
		mu.Lock()
		fired++
		mu.Unlock()
	})

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if fired != 0 {
		t.Fatalf("closer fired %d times with IdleTimeout=0, want 0", fired)
	}
	if s.idleTimer != nil {
		t.Fatalf("idleTimer must remain nil when IdleTimeout=0")
	}
}
