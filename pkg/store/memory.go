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

	ms.ttlMgr = ttl.NewManager(func(key string, _ struct{}) {
		// Expired
		ms.Delete(context.Background(), key)
	})

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
	queue   queue[Entry]
	cancel  context.CancelFunc
}

func (w *memoryWatcher) Updates() <-chan Entry { return w.ch }

func (w *memoryWatcher) Stop() {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	// Remove self from store.watchers
	for i, watcher := range w.store.watchers {
		if watcher == w {
			// Fast delete
			w.store.watchers[i] = w.store.watchers[len(w.store.watchers)-1]
			w.store.watchers = w.store.watchers[:len(w.store.watchers)-1]
			watcher.cancel()
			break
		}
	}
}

func (s *MemoryStore) Put(ctx context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clone value to prevent external modification
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	s.data[key] = valCopy

	if s.ttl > 0 && s.ttlMgr != nil {
		s.ttlMgr.Add(key, struct{}{}, s.ttl)
	}

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

	s.ttlMgr.Remove(key)

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
	options := GetWatchOptions(opts...)
	ctx, cancel := context.WithCancel(ctx)

	w := &memoryWatcher{
		store:   s,
		pattern: pattern,
		opts:    options,
		ch:      make(chan Entry),
		queue:   newQueue[Entry](),
		cancel:  cancel,
	}

	s.mu.Lock()

	for k, v := range s.data {
		if matchSubject(pattern, k) {
			valCopy := make([]byte, len(v))
			copy(valCopy, v)
			w.queue.put(&memoryEntry{key: k, value: valCopy, op: OpPut})
		}
	}
	w.queue.put(nil) // sentinel: initial snapshot complete

	s.watchers = append(s.watchers, w)

	s.mu.Unlock()

	go func() {
		defer func() {
			close(w.ch)
			cancel()
		}()
		for {
			entry, err := w.queue.get(ctx)
			if err != nil {
				return
			}
			w.ch <- entry
		}
	}()

	return w, nil
}

func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Stop all timers
	s.ttlMgr.Close()

	// Stop all watchers
	for _, w := range s.watchers {
		w.cancel()
	}
	s.watchers = nil
	s.data = nil
	return nil
}

// notify must be called with lock held
func (s *MemoryStore) notify(key string, value []byte, op Operation) {
	for _, w := range s.watchers {
		if w.opts.IgnoreDeletes && op == OpDelete {
			continue
		}

		if matchSubject(w.pattern, key) {
			w.queue.put(&memoryEntry{
				key:   key,
				value: value,
				op:    op,
			})
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
		if pToken != "*" && pToken != sTokens[i] {
			return false
		}
	}
	return len(pTokens) == len(sTokens) || (len(pTokens) > 0 && pTokens[len(pTokens)-1] == ">")
}
