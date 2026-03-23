package store

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type NatsStoreOptions struct {
	Description    string
	LimitMarkerTTL time.Duration
}

type NatsStoreOption func(*NatsStoreOptions)

func WithDescription(desc string) NatsStoreOption {
	return func(o *NatsStoreOptions) { o.Description = desc }
}

func WithLimitMarkerTTL(ttl time.Duration) NatsStoreOption {
	return func(o *NatsStoreOptions) { o.LimitMarkerTTL = ttl }
}

type NatsStore struct {
	kv       jetstream.KeyValue
	ttl      time.Duration
	bucket   string
	logger   *slog.Logger
	watchers []*natsWatcher
	mu       sync.RWMutex
}

// NewNatsStore creates a new NATS-backed KV store.
// It ensures the bucket exists with the given configuration.
func NewNatsStore(ctx context.Context, js jetstream.JetStream, bucket string, ttlDuration time.Duration, opts ...NatsStoreOption) (*NatsStore, error) {
	options := NatsStoreOptions{
		LimitMarkerTTL: 3 * time.Second,
	}
	for _, opt := range opts {
		opt(&options)
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         bucket,
		Description:    options.Description,
		TTL:            ttlDuration,
		History:        1,
		LimitMarkerTTL: options.LimitMarkerTTL,
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

	return s, nil
}

func (s *NatsStore) Create(ctx context.Context, key string, value []byte, opts ...CreateOption) error {
	var o CreateOptions
	for _, opt := range opts {
		opt(&o)
	}

	if o.TTL > 0 {
		_, err := s.kv.Create(ctx, key, value, jetstream.KeyTTL(o.TTL))
		return err
	}

	_, err := s.kv.Create(ctx, key, value)
	return err
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
