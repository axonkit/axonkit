package store

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type NatsStore struct {
	kv jetstream.KeyValue
}

// NewNatsStore creates a new NATS-backed KV store.
// It ensures the bucket exists with the given configuration.
func NewNatsStore(ctx context.Context, js jetstream.JetStream, bucket string, ttl time.Duration) (*NatsStore, error) {
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      bucket,
		Description: "Routing Information Base",
		TTL:         ttl,
		History:     1,
	})
	if err != nil {
		return nil, err
	}
	return &NatsStore{kv: kv}, nil
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
	return s.kv.Delete(ctx, key)
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
		w:  w,
		ch: make(chan Entry),
	}

	go func() {
		defer close(nw.ch)
		for e := range w.Updates() {
			if e == nil {
				return
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
	w  jetstream.KeyWatcher
	ch chan Entry
}

func (nw *natsWatcher) Updates() <-chan Entry {
	return nw.ch
}

func (nw *natsWatcher) Stop() {
	nw.w.Stop()
}
