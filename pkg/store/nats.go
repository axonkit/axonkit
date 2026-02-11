package store

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jiyeyuran/omnistream/pkg/store/ttl"

	"github.com/nats-io/nats.go/jetstream"
)

type NatsStore struct {
	kv       jetstream.KeyValue
	ttl      time.Duration
	bucket   string
	logger   *slog.Logger
	ttlMgr   *ttl.Manager[string, uint64]
	watchers []*natsWatcher
	mu       sync.RWMutex
}

// NewNatsStore creates a new NATS-backed KV store.
// It ensures the bucket exists with the given configuration.
func NewNatsStore(ctx context.Context, js jetstream.JetStream, bucket string, ttlDuration time.Duration) (*NatsStore, error) {
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      bucket,
		Description: "Routing Information Base",
		TTL:         ttlDuration,
		History:     1,
	})
	if err != nil {
		return nil, err
	}

	s := &NatsStore{
		kv:     kv,
		ttl:    ttlDuration,
		bucket: bucket,
		logger: slog.Default().With("component", "NatsStore", "bucket", bucket),
	}

	if ttlDuration > 0 {
		// Use local TTL Manager
		s.ttlMgr = ttl.NewManager(func(key string, rev uint64) {
			// Check if key still exists
			if _, err := kv.Get(context.Background(), key); err == nil {
				s.logger.Warn("Key still exists, ignore expiration", "key", key, "rev", rev)
				return
			}
			s.logger.Info("Expired key", "key", key, "rev", rev)

			// Notify local watchers
			s.mu.RLock()
			defer s.mu.RUnlock()

			for _, w := range s.watchers {
				if matchSubject(w.pattern, key) {
					// Create local delete event
					e := &localEntry{
						key: key,
						op:  OpDelete,
					}

					// Attempt to send, but don't block forever
					// Note: nw.ch is unbuffered usually?
					// In Watch(), we created it with make(chan Entry). Unbuffered.
					// If the consumer is slow, we might block here.
					// But we hold RLock, so we block other operations?
					// Ideally we should use select with default or timeout.
					// But if we drop, the watcher misses the expiration.

					// Let's use a non-blocking send to be safe against deadlocks/blocking,
					// assuming watchers are consuming.
					select {
					case w.ch <- e:
					case <-time.After(100 * time.Millisecond):
						s.logger.Warn("Failed to notify watcher of expiration (timeout)", "key", key)
					}
				}
			}
		})
	}

	return s, nil
}

func (s *NatsStore) Put(ctx context.Context, key string, value []byte) error {
	_, err := s.kv.Put(ctx, key, value)
	return err
}

func (s *NatsStore) Get(ctx context.Context, key string) (Entry, error) {
	e, err := s.kv.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return &natsEntry{e}, nil
}

func (s *NatsStore) Delete(ctx context.Context, key string) error {
	return s.kv.Purge(ctx, key)
}

func (s *NatsStore) Watch(ctx context.Context, pattern string, opts ...WatchOption) (Watcher, error) {
	o := GetWatchOptions(opts...)
	var jsOpts []jetstream.WatchOpt
	if o.IgnoreDeletes {
		jsOpts = append(jsOpts, jetstream.IgnoreDeletes())
	}

	w, err := s.kv.Watch(ctx, pattern, jsOpts...)
	if err != nil {
		return nil, err
	}

	nw := &natsWatcher{
		w:       w,
		ch:      make(chan Entry, 100),
		pattern: pattern,
	}

	s.mu.Lock()
	s.watchers = append(s.watchers, nw)
	s.mu.Unlock()

	go func() {
		defer func() {
			s.mu.Lock()
			for i, w := range s.watchers {
				if w == nw {
					s.watchers = append(s.watchers[:i], s.watchers[i+1:]...)
					break
				}
			}
			s.mu.Unlock()

			close(nw.ch)
		}()
		for e := range w.Updates() {
			if e == nil {
				continue
			}

			// Handle TTL for externally watched keys
			if s.ttlMgr != nil {
				switch e.Operation() {
				case jetstream.KeyValuePut:
					s.ttlMgr.Add(e.Key(), e.Revision(), s.ttl+time.Second)
				case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
					s.ttlMgr.Remove(e.Key())
				}
			}

			nw.ch <- &natsEntry{e}
		}
	}()

	return nw, nil
}

func (s *NatsStore) Keys(ctx context.Context, pattern string) ([]string, error) {
	lister, err := s.kv.ListKeysFiltered(ctx, pattern)
	if err != nil {
		return nil, err
	}
	defer lister.Stop()

	var matched []string
	for k := range lister.Keys() {
		matched = append(matched, k)
	}
	return matched, nil
}

func (s *NatsStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Stop all watchers
	for _, w := range s.watchers {
		w.w.Stop()
	}
	s.watchers = nil

	return nil
}

// natsEntry implements Entry
type natsEntry struct {
	e jetstream.KeyValueEntry
}

func (n *natsEntry) Key() string   { return n.e.Key() }
func (n *natsEntry) Value() []byte { return n.e.Value() }
func (n *natsEntry) Operation() Operation {
	switch n.e.Operation() {
	case jetstream.KeyValuePut:
		return OpPut
	case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
		return OpDelete
	default:
		return OpPut
	}
}

// natsWatcher implements Watcher
type natsWatcher struct {
	w       jetstream.KeyWatcher
	ch      chan Entry
	pattern string
}

func (nw *natsWatcher) Updates() <-chan Entry {
	return nw.ch
}

func (nw *natsWatcher) Stop() {
	nw.w.Stop()
}

// localEntry implements Entry for locally generated events (like TTL expiration)
type localEntry struct {
	key   string
	value []byte
	op    Operation
}

func (e *localEntry) Key() string          { return e.key }
func (e *localEntry) Value() []byte        { return e.value }
func (e *localEntry) Operation() Operation { return e.op }
