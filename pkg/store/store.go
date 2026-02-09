package store

import "context"

type Operation int

const (
	OpPut Operation = iota
	OpDelete
)

type Entry interface {
	Key() string
	Value() []byte
	Operation() Operation
}

type Watcher interface {
	Updates() <-chan Entry
	Stop()
}

type WatchOption func(*watchOptions)

type watchOptions struct {
	IgnoreDeletes bool
}

func IgnoreDeletes() WatchOption {
	return func(o *watchOptions) {
		o.IgnoreDeletes = true
	}
}

// GetWatchOptions applies options and returns the struct
func GetWatchOptions(opts ...WatchOption) watchOptions {
	var o watchOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

type Store interface {
	Put(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) (Entry, error)
	Delete(ctx context.Context, key string) error
	Watch(ctx context.Context, pattern string, opts ...WatchOption) (Watcher, error)
	Close() error
}
