package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"rib-control/pkg/store"
	"rib-control/pkg/types"
)

const (
	NodeBucketName = "NODES"
	NodeTTL        = 5 * time.Second
)

type Registry struct {
	store  store.Store
	local  types.NodeInfo
	logger *slog.Logger
	stopCh chan struct{}
	ttl    time.Duration
}

type RegistryOption func(*Registry)

func WithTTL(ttl time.Duration) RegistryOption {
	return func(r *Registry) {
		r.ttl = ttl
	}
}

func NewRegistry(s store.Store, local types.NodeInfo, opts ...RegistryOption) *Registry {
	r := &Registry{
		store:  s,
		local:  local,
		logger: slog.Default().With("component", "Registry"),
		stopCh: make(chan struct{}),
		ttl:    NodeTTL,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Start registers the node and starts the keepalive loop
func (r *Registry) Start() error {
	// Initial register
	if err := r.register(); err != nil {
		return err
	}

	go r.keepAliveLoop()
	return nil
}

func (r *Registry) register() error {
	data, err := json.Marshal(r.local)
	if err != nil {
		return err
	}
	return r.store.Put(context.Background(), string(r.local.ID), data)
}

func (r *Registry) keepAliveLoop() {
	ticker := time.NewTicker(r.ttl / 2)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			if err := r.register(); err != nil {
				r.logger.Error("Keepalive failed", "error", err)
			}
		}
	}
}

func (r *Registry) Close() {
	close(r.stopCh)
	// Graceful shutdown: delete self
	_ = r.store.Delete(context.Background(), string(r.local.ID))
}

// WatchNodes watches for node changes and calls callbacks
func (r *Registry) WatchNodes(cb func(nodeID types.NodeID, isDelete bool)) error {
	w, err := r.store.Watch(context.Background(), ">") // Watch all keys
	if err != nil {
		return err
	}

	// Watcher Loop
	go func() {
		defer w.Stop()

		lastSeen := make(map[types.NodeID][]byte)

		for {
			select {
			case <-r.stopCh:
				return
			case entry, ok := <-w.Updates():
				if !ok {
					return
				}
				if entry == nil {
					continue
				}

				nodeID := types.NodeID(entry.Key())
				if nodeID == r.local.ID {
					continue
				}

				if entry.Operation() == store.OpDelete {
					r.logger.Info("Node down detected", "node_id", nodeID)
					delete(lastSeen, nodeID)
					cb(nodeID, true)
				} else {
					// Notify Put/Update
					newValue := entry.Value()

					// Check if changed
					if oldValue, exists := lastSeen[nodeID]; exists {
						if bytes.Equal(oldValue, newValue) {
							continue
						}
					}

					lastSeen[nodeID] = newValue

					cb(nodeID, false)
				}
			}
		}
	}()

	return nil
}
