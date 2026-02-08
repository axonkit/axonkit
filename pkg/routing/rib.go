package routing

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"rib-control/pkg/ds"
	"rib-control/pkg/types"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	RibBucketName = "RIB"
	RIBTTL        = 10 * time.Second
	RefreshRate   = 3 * time.Second
)

// adEntry represents a stream being advertised
type adEntry struct {
	StreamID    types.StreamID
	BaseCost    int
	PathVector  []types.NodeID
	RttToOrigin int64
}

type RIB struct {
	kv    jetstream.KeyValue
	js    jetstream.JetStream
	local types.NodeInfo
	ctx   context.Context

	// Keepalive management
	mu       sync.Mutex
	pq       ds.PriorityQueue[*adEntry]
	entryMap map[types.StreamID]*ds.Item[*adEntry]

	// Channels for keepalive loop
	signalCh chan struct{} // signal when new item added
	stopCh   chan struct{} // stop loop

	logger *slog.Logger
}

func NewRIB(js jetstream.JetStream, local types.NodeInfo) (*RIB, error) {
	ctx := context.Background()

	// Ensure Bucket Exists
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      RibBucketName,
		Description: "Routing Information Base",
		TTL:         RIBTTL,
		History:     1,
	})
	if err != nil {
		return nil, err
	}

	r := &RIB{
		kv:       kv,
		js:       js,
		local:    local,
		ctx:      ctx,
		pq:       ds.NewPriorityQueue[*adEntry](0),
		entryMap: make(map[types.StreamID]*ds.Item[*adEntry]),
		signalCh: make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
		logger:   slog.Default().With("component", "RIB"),
	}

	heap.Init(&r.pq)

	// Start keepalive scheduler
	go r.keepAliveLoop()

	return r, nil
}

// LocalInfo returns the local node information
func (r *RIB) LocalInfo() types.NodeInfo {
	return r.local
}

// SubscribeUpdates starts a watcher for a specific stream's routes
// It returns a cancel function to stop the watcher.
func (r *RIB) SubscribeUpdates(streamID types.StreamID, cb func(types.RouteKVValue)) func() {
	// Pattern: route.<StreamID>.>
	pattern := fmt.Sprintf("route.%s.>", streamID)

	// We use a separate context for the watcher so we can cancel it
	ctx, cancel := context.WithCancel(context.Background())

	// Watch includes deletes (we need to know if upstream is gone)
	watcher, err := r.kv.Watch(ctx, pattern)
	if err != nil {
		r.logger.Error("Failed to watch stream", "stream_id", streamID, "error", err)
		cancel()
		return func() {}
	}

	go func() {
		defer watcher.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case entry, ok := <-watcher.Updates():
				if !ok {
					return
				}
				if entry == nil {
					continue
				}

				// Handle Delete
				if entry.Operation() == jetstream.KeyValueDelete || entry.Operation() == jetstream.KeyValuePurge {
					// Key: route.<StreamID>...
					// We pass the Key as OriginNode (hack compatible with previous logic)
					cb(types.RouteKVValue{
						StreamID:   streamID,
						BaseCost:   -1,
						OriginNode: types.NodeID(entry.Key()),
					})
					continue
				}

				var val types.RouteKVValue
				if err := json.Unmarshal(entry.Value(), &val); err != nil {
					continue
				}
				cb(val)
			}
		}
	}()

	return cancel
}

// WatchDownstream registers a callback to track who depends on us
func (r *RIB) WatchDownstream(streamID types.StreamID, cb func(downstreamNode types.NodeID, isAdd bool)) {
	pattern := fmt.Sprintf("route.%s.>", streamID)

	// We use r.ctx (global) because FIB doesn't cancel WatchDownstream explicitly
	// This means these watchers run until RIB is closed.
	watcher, err := r.kv.Watch(r.ctx, pattern)
	if err != nil {
		r.logger.Error("Failed to watch downstream", "stream_id", streamID, "error", err)
		return
	}

	go func() {
		defer watcher.Stop()
		for {
			select {
			case <-r.stopCh: // Stop if RIB stops
				return
			case entry, ok := <-watcher.Updates():
				if !ok {
					return
				}
				if entry == nil {
					continue
				}

				// Handle Delete
				if entry.Operation() == jetstream.KeyValueDelete || entry.Operation() == jetstream.KeyValuePurge {
					// Assume node removed dependency
					cb(types.NodeID(entry.Key()), false)
					continue
				}

				var val types.RouteKVValue
				if err := json.Unmarshal(entry.Value(), &val); err != nil {
					continue
				}

				// Check if we are in the PathVector as the last hop (upstream)
				if len(val.PathVector) > 0 && val.PathVector[len(val.PathVector)-1] == r.local.ID {
					cb(val.OriginNode, true)
				}
			}
		}
	}()
}

// StartAdvertise registers a stream for keepalive
func (r *RIB) StartAdvertise(streamID types.StreamID, baseCost int, pathVector []types.NodeID, rttToOrigin int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.entryMap[streamID]; exists {
		return nil
	}

	val := &adEntry{
		StreamID:    streamID,
		BaseCost:    baseCost,
		PathVector:  pathVector,
		RttToOrigin: rttToOrigin,
	}

	// Initial advertise immediately
	if err := r.advertiseOnce(val); err != nil {
		return err
	}

	priority := time.Now().Add(RefreshRate).UnixNano()
	item := &ds.Item[*adEntry]{
		Value:    val,
		Priority: priority,
	}

	r.entryMap[streamID] = item
	heap.Push(&r.pq, item)

	// Signal keepAliveLoop
	select {
	case r.signalCh <- struct{}{}:
	default:
	}

	return nil
}

// StopAdvertise removes a stream from keepalive
func (r *RIB) StopAdvertise(streamID types.StreamID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	item, exists := r.entryMap[streamID]
	if !exists {
		return nil
	}

	heap.Remove(&r.pq, item.Index)
	delete(r.entryMap, streamID)

	return r.Unadvertise(streamID)
}

// keepAliveLoop manages the priority queue to schedule re-advertisements
func (r *RIB) keepAliveLoop() {
	timer := time.NewTimer(1 * time.Hour) // Long idle time
	defer timer.Stop()

	for {
		var delay time.Duration

		r.mu.Lock()
		if r.pq.Len() == 0 {
			delay = 1 * time.Hour
		} else {
			item := r.pq[0]
			now := time.Now().UnixNano()
			if now > item.Priority {
				delay = 0
			} else {
				delay = time.Duration(item.Priority - now)
			}
		}
		r.mu.Unlock()

		timer.Reset(delay)

		select {
		case <-r.stopCh:
			return

		case <-r.signalCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			continue

		case <-timer.C:
			r.mu.Lock()
			now := time.Now().UnixNano()
			if item, wait := r.pq.PeekAndShift(now); item != nil {
				go func(val *adEntry) {
					if err := r.advertiseOnce(val); err != nil {
						r.logger.Error("Advertise failed", "stream_id", val.StreamID, "error", err)
					}
				}(item.Value)

				item.Priority = time.Now().Add(RefreshRate).UnixNano()
				heap.Push(&r.pq, item)
			} else if wait > 0 {
				// Reset timer
			}
			r.mu.Unlock()
		}
	}
}

// Close cleans up resources and stops advertising all streams
func (r *RIB) Close() {
	// Stop keepAliveLoop
	close(r.stopCh)

	// Unadvertise all active streams
	r.mu.Lock()
	defer r.mu.Unlock()

	for streamID, item := range r.entryMap {
		r.logger.Info("Closing: Unadvertising stream", "stream_id", streamID)
		if err := r.Unadvertise(streamID); err != nil {
			r.logger.Error("Failed to unadvertise", "stream_id", streamID, "error", err)
		}
		delete(r.entryMap, streamID)
		heap.Remove(&r.pq, item.Index)
	}
}

// advertiseOnce writes to KV once
func (r *RIB) advertiseOnce(entry *adEntry) error {
	key := fmt.Sprintf("route.%s.%s.%s.%s", entry.StreamID, r.local.Region, r.local.Cluster, r.local.ID)

	val := types.RouteKVValue{
		StreamID:      entry.StreamID,
		OriginNode:    r.local.ID,
		OriginRegion:  r.local.Region,
		OriginCluster: r.local.Cluster,
		BaseCost:      entry.BaseCost,
		PathVector:    entry.PathVector,
		RttToOrigin:   entry.RttToOrigin,
		ExpireAt:      time.Now().Add(RIBTTL),
	}

	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	_, err = r.kv.Put(r.ctx, key, data)
	return err
}

// Unadvertise removes the entry directly
func (r *RIB) Unadvertise(streamID types.StreamID) error {
	key := fmt.Sprintf("route.%s.%s.%s.%s", streamID, r.local.Region, r.local.Cluster, r.local.ID)
	return r.kv.Delete(r.ctx, key)
}

// GetCandidates returns all available routes for a stream
func (r *RIB) GetCandidates(streamID types.StreamID) ([]types.RouteKVValue, error) {
	pattern := fmt.Sprintf("route.%s.>", streamID)

	watcher, err := r.kv.Watch(r.ctx, pattern, jetstream.IgnoreDeletes())
	if err != nil {
		return nil, err
	}
	defer watcher.Stop()

	var candidates []types.RouteKVValue

	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}

		var val types.RouteKVValue
		if err := json.Unmarshal(entry.Value(), &val); err != nil {
			continue
		}
		candidates = append(candidates, val)
	}

	return candidates, nil
}
