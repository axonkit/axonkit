package discovery

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"rib-control/pkg/store"
	"rib-control/pkg/types"
)

const (
	NodeBucketName    = "NODES"
	NodeTTL           = 5 * time.Second
	KeepAliveInterval = 2 * time.Second
)

type Registry struct {
	store  store.Store
	local  types.NodeInfo
	logger *slog.Logger
	stopCh chan struct{}
}

func NewRegistry(s store.Store, local types.NodeInfo) *Registry {
	return &Registry{
		store:  s,
		local:  local,
		logger: slog.Default().With("component", "Registry"),
		stopCh: make(chan struct{}),
	}
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
	ticker := time.NewTicker(KeepAliveInterval)
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
func (r *Registry) WatchNodes(onNodeDown func(types.NodeID)) error {
	w, err := r.store.Watch(context.Background(), ">") // Watch all keys
	if err != nil {
		return err
	}

	go func() {
		defer w.Stop()
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

				if entry.Operation() == store.OpDelete {
					nodeID := types.NodeID(entry.Key())
					if nodeID != r.local.ID {
						r.logger.Info("Node down detected", "node_id", nodeID)
						onNodeDown(nodeID)
					}
				}
			}
		}
	}()
	return nil
}
