package ttl

import (
	"container/heap"
	"sync"
	"time"

	"github.com/axonkit/axonkit/pkg/ds"
)

type Entry[K comparable, V any] struct {
	Key   K
	Value V
}

// Manager manages expiration of items using a priority queue.
type Manager[K comparable, V any] struct {
	mu       sync.Mutex
	pq       ds.PriorityQueue[*Entry[K, V]]
	items    map[K]*ds.Item[*Entry[K, V]]
	signalCh chan struct{}
	stopCh   chan struct{}
	onExpire func(K, V)
}

// NewManager creates a new TTL manager.
// onExpire is called when an item expires. It is called in a separate goroutine (the loop),
// so it should not block for too long.
func NewManager[K comparable, V any](onExpire func(K, V)) *Manager[K, V] {
	m := &Manager[K, V]{
		pq:       ds.NewPriorityQueue[*Entry[K, V]](0),
		items:    make(map[K]*ds.Item[*Entry[K, V]]),
		signalCh: make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
		onExpire: onExpire,
	}
	go m.loop()
	return m
}

// Add adds or updates an item with a TTL.
func (m *Manager[K, V]) Add(key K, value V, ttl time.Duration) {
	m.AddAt(key, value, time.Now().Add(ttl))
}

// AddAt adds or updates an item expiring at a specific time.
// Returns the previous value and true if the key already existed.
func (m *Manager[K, V]) AddAt(key K, value V, expireAt time.Time) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	priority := expireAt.UnixNano()

	if item, exists := m.items[key]; exists {
		oldVal := item.Value.Value
		item.Value.Value = value
		item.Priority = priority
		heap.Fix(&m.pq, item.Index)

		select {
		case m.signalCh <- struct{}{}:
		default:
		}
		return oldVal, true
	}

	entry := &Entry[K, V]{
		Key:   key,
		Value: value,
	}
	item := &ds.Item[*Entry[K, V]]{
		Value:    entry,
		Priority: priority,
	}
	heap.Push(&m.pq, item)
	m.items[key] = item

	select {
	case m.signalCh <- struct{}{}:
	default:
	}

	var zero V
	return zero, false
}

// Remove removes an item.
func (m *Manager[K, V]) Remove(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if item, exists := m.items[key]; exists {
		heap.Remove(&m.pq, item.Index)
		delete(m.items, key)

		// Signal loop to potentially reset timer (optional but good for cleanup)
		select {
		case m.signalCh <- struct{}{}:
		default:
		}
	}
}

// Close stops the manager.
func (m *Manager[K, V]) Close() {
	close(m.stopCh)
}

func (m *Manager[K, V]) loop() {
	timer := time.NewTimer(1 * time.Hour)
	defer timer.Stop()

	for {
		var delay time.Duration

		m.mu.Lock()
		if m.pq.Len() == 0 {
			delay = 1 * time.Hour
		} else {
			item := m.pq[0]
			now := time.Now().UnixNano()
			if now >= item.Priority {
				delay = 0
			} else {
				delay = time.Duration(item.Priority - now)
			}
		}
		m.mu.Unlock()

		timer.Reset(delay)

		select {
		case <-m.stopCh:
			return

		case <-m.signalCh:
			timer.Stop()
			continue

		case <-timer.C:
			m.mu.Lock()
			now := time.Now().UnixNano()
			// Process all expired items
			for m.pq.Len() > 0 {
				item := m.pq[0]
				if item.Priority > now {
					break
				}

				// Remove from heap and map
				heap.Pop(&m.pq)
				entry := item.Value
				delete(m.items, entry.Key)

				// Callback (unlock during callback to avoid deadlocks if callback calls Add/Remove)
				m.mu.Unlock()
				m.onExpire(entry.Key, entry.Value)
				m.mu.Lock()
			}
			m.mu.Unlock()
		}
	}
}
