package ttl

import (
	"sync"
	"testing"
	"time"
)

func TestManager_Basic(t *testing.T) {
	var mu sync.Mutex
	expired := make(map[string]int)

	m := NewManager(func(key string, val int) {
		mu.Lock()
		expired[key] = val
		mu.Unlock()
	})
	defer m.Close()

	// Add item expiring in 100ms
	m.Add("k1", 1, 100*time.Millisecond)

	// Wait 200ms
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	if val, ok := expired["k1"]; !ok || val != 1 {
		t.Errorf("k1 should have expired with value 1, got %v (exists: %v)", val, ok)
	}
	mu.Unlock()
}

func TestManager_AddAt(t *testing.T) {
	var mu sync.Mutex
	expired := make(map[string]int)

	m := NewManager(func(key string, val int) {
		mu.Lock()
		expired[key] = val
		mu.Unlock()
	})
	defer m.Close()

	// Add item expiring at specific time
	target := time.Now().Add(100 * time.Millisecond)
	m.AddAt("k1", 1, target)

	// Update item with new time (extend)
	target2 := time.Now().Add(300 * time.Millisecond)
	m.AddAt("k1", 2, target2)

	// Wait 200ms - should NOT expire yet
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	if _, ok := expired["k1"]; ok {
		t.Error("k1 expired too early")
	}
	mu.Unlock()

	// Wait 200ms more (total 400ms) - should expire
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	if val, ok := expired["k1"]; !ok || val != 2 {
		t.Errorf("k1 should have expired with value 2, got %v", val)
	}
	mu.Unlock()
}

func TestManager_Remove(t *testing.T) {
	var mu sync.Mutex
	expired := make(map[string]int)

	m := NewManager(func(key string, val int) {
		mu.Lock()
		expired[key] = val
		mu.Unlock()
	})
	defer m.Close()

	m.Add("k1", 1, 100*time.Millisecond)
	m.Remove("k1")

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	if _, ok := expired["k1"]; ok {
		t.Error("k1 should not have expired after removal")
	}
	mu.Unlock()
}

func TestManager_Multiple(t *testing.T) {
	var mu sync.Mutex
	expired := make(map[string]int)
	wg := sync.WaitGroup{}

	count := 10
	wg.Add(count)

	m := NewManager(func(key string, val int) {
		mu.Lock()
		expired[key] = val
		mu.Unlock()
		wg.Done()
	})
	defer m.Close()

	// Add multiple items with different TTLs
	// Add in reverse order of expiration to test PQ sorting
	for i := 0; i < count; i++ {
		ttl := time.Duration(100*(count-i)) * time.Millisecond
		m.Add(string(rune('a'+i)), i, ttl)
	}

	// Wait for all to expire
	// Max TTL is 100 * 10 = 1s
	// Wait slightly longer with buffer

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All good
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for all items to expire")
	}

	mu.Lock()
	if len(expired) != count {
		t.Errorf("Expected %d expired items, got %d", count, len(expired))
	}
	mu.Unlock()
}
