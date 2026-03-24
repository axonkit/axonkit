package store

import (
	"context"
)

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

type WatchOption func(*WatchOptions)

type WatchOptions struct {
	IgnoreDeletes bool
}

func IgnoreDeletes() WatchOption {
	return func(o *WatchOptions) {
		o.IgnoreDeletes = true
	}
}

// GetWatchOptions applies options and returns the struct
func GetWatchOptions(opts ...WatchOption) WatchOptions {
	var o WatchOptions
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
	Keys(ctx context.Context, pattern string) ([]string, error)
	Close() error
}
