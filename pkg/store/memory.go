package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/axonkit/axonkit/pkg/store/ttl"
)

// MemoryStore implements an in-memory Store.
type MemoryStore struct {
	mu       sync.RWMutex
	data     map[string][]byte
	watchers []*memoryWatcher
	ttl      time.Duration
	ttlMgr   *ttl.Manager[string, struct{}]
}

// NewMemoryStore creates a new in-memory store.
// If ttl > 0, keys will be automatically deleted after ttl.
func NewMemoryStore(ttlDuration time.Duration) *MemoryStore {
	ms := &MemoryStore{
		data: make(map[string][]byte),
		ttl:  ttlDuration,
	}

	if ttlDuration > 0 {
		ms.ttlMgr = ttl.NewManager(func(key string, _ struct{}) {
			// Expired
			ms.Delete(context.Background(), key)
		})
	}

	return ms
}

// memoryEntry implements Entry
type memoryEntry struct {
	key   string
	value []byte
	op    Operation
}

func (e *memoryEntry) Key() string          { return e.key }
func (e *memoryEntry) Value() []byte        { return e.value }
func (e *memoryEntry) Operation() Operation { return e.op }

// memoryWatcher implements Watcher
type memoryWatcher struct {
	store   *MemoryStore
	pattern string
	opts    WatchOptions
	ch      chan Entry
	stopCh  chan struct{}
}

func (w *memoryWatcher) Updates() <-chan Entry {
	return w.ch
}

func (w *memoryWatcher) Stop() {
	select {
	case <-w.stopCh:
		return // already stopped
	default:
		close(w.stopCh)
	}

	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	// Remove self from store.watchers
	for i, watcher := range w.store.watchers {
		if watcher == w {
			// Fast delete
			w.store.watchers[i] = w.store.watchers[len(w.store.watchers)-1]
			w.store.watchers = w.store.watchers[:len(w.store.watchers)-1]
			break
		}
	}
	close(w.ch)
}

func (s *MemoryStore) Put(ctx context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clone value to prevent external modification
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	s.data[key] = valCopy

	s.notify(key, valCopy, OpPut)
	return nil
}

func (s *MemoryStore) Get(ctx context.Context, key string) (Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	// Return copy
	valCopy := make([]byte, len(val))
	copy(valCopy, val)

	return &memoryEntry{
		key:   key,
		value: valCopy,
		op:    OpPut,
	}, nil
}

func (s *MemoryStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		return nil // NATS Delete is idempotent-ish usually, or we can return error
	}
	delete(s.data, key)

	s.notify(key, nil, OpDelete)
	return nil
}

func (s *MemoryStore) Keys(ctx context.Context, pattern string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var matched []string
	for k := range s.data {
		if matchSubject(pattern, k) {
			matched = append(matched, k)
		}
	}
	return matched, nil
}

func (s *MemoryStore) Watch(ctx context.Context, pattern string, opts ...WatchOption) (Watcher, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	options := GetWatchOptions(opts...)

	w := &memoryWatcher{
		store:   s,
		pattern: pattern,
		opts:    options,
		ch:      make(chan Entry, 100), // Buffer to avoid blocking notify
		stopCh:  make(chan struct{}),
	}

	s.watchers = append(s.watchers, w)

	// Deliver initial state synchronously under the lock so that it is
	// guaranteed to arrive before any subsequent Put/Delete notifications.
	for k, v := range s.data {
		if matchSubject(pattern, k) {
			valCopy := make([]byte, len(v))
			copy(valCopy, v)
			select {
			case w.ch <- &memoryEntry{key: k, value: valCopy, op: OpPut}:
			default:
			}
		}
	}

	return w, nil
}

func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Stop all timers
	if s.ttlMgr != nil {
		s.ttlMgr.Close()
	}

	// Stop all watchers
	for _, w := range s.watchers {
		select {
		case <-w.stopCh:
		default:
			close(w.stopCh)
			close(w.ch)
		}
	}
	s.watchers = nil
	s.data = nil
	return nil
}

// notify must be called with lock held
func (s *MemoryStore) notify(key string, value []byte, op Operation) {
	// Handle TTL globally (once per update)
	if s.ttlMgr != nil {
		switch op {
		case OpPut:
			s.ttlMgr.Add(key, struct{}{}, s.ttl)
		case OpDelete:
			s.ttlMgr.Remove(key)
		}
	}

	for _, w := range s.watchers {
		if w.opts.IgnoreDeletes && op == OpDelete {
			continue
		}

		if matchSubject(w.pattern, key) {
			e := &memoryEntry{
				key:   key,
				value: value,
				op:    op,
			}
			select {
			case w.ch <- e:
			case <-w.stopCh:
			}
		}
	}
}

// matchSubject implements basic NATS wildcard matching
// * matches any token
// > matches any remaining tokens
func matchSubject(pattern, subject string) bool {
	if pattern == ">" {
		return true
	}
	pTokens := strings.Split(pattern, ".")
	sTokens := strings.Split(subject, ".")

	for i, pToken := range pTokens {
		if pToken == ">" {
			return true
		}
		if i >= len(sTokens) {
			return false
		}
		sToken := sTokens[i]
		if pToken != "*" && pToken != sToken {
			return false
		}
	}
	return len(pTokens) == len(sTokens)
}
