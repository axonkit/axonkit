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

type streamWatcher struct {
	streamID types.StreamID
	cancel   context.CancelFunc // Stop the NATS watcher

	mu          sync.RWMutex
	subscribers map[int64]func(types.RouteKVValue)
	downstreams map[int64]func(types.NodeID, bool)
	subSeq      int64
	dsSeq       int64
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

	// Stream Watchers
	watchers   map[types.StreamID]*streamWatcher
	watchersMu sync.Mutex
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
		watchers: make(map[types.StreamID]*streamWatcher),
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

func (r *RIB) getOrStartWatcher(streamID types.StreamID) (*streamWatcher, error) {
	r.watchersMu.Lock()
	defer r.watchersMu.Unlock()

	if sw, exists := r.watchers[streamID]; exists {
		return sw, nil
	}

	pattern := fmt.Sprintf("route.%s.>", streamID)
	// Use separate context for cancellation
	ctx, cancel := context.WithCancel(context.Background())

	natsWatcher, err := r.kv.Watch(ctx, pattern)
	if err != nil {
		cancel()
		return nil, err
	}

	sw := &streamWatcher{
		streamID:    streamID,
		cancel:      cancel,
		subscribers: make(map[int64]func(types.RouteKVValue)),
		downstreams: make(map[int64]func(types.NodeID, bool)),
	}
	r.watchers[streamID] = sw

	// Start dispatch loop
	go func() {
		defer natsWatcher.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.stopCh: // Also stop if RIB stops
				return
			case entry, ok := <-natsWatcher.Updates():
				if !ok {
					return
				}
				if entry == nil {
					continue
				}
				r.dispatchUpdate(sw, entry)
			}
		}
	}()

	return sw, nil
}

func (r *RIB) dispatchUpdate(sw *streamWatcher, entry jetstream.KeyValueEntry) {
	isDelete := entry.Operation() == jetstream.KeyValueDelete || entry.Operation() == jetstream.KeyValuePurge
	key := entry.Key()

	var val types.RouteKVValue
	if isDelete {
		val.BaseCost = -1
		val.Node = types.NodeID(key) // Hack: use Key as ID for delete
		// StreamID is known from sw
		val.StreamID = sw.streamID
	} else {
		if err := json.Unmarshal(entry.Value(), &val); err != nil {
			return
		}
	}

	sw.mu.RLock()
	defer sw.mu.RUnlock()

	// 1. Notify Subscribers
	for _, cb := range sw.subscribers {
		cb(val)
	}

	// 2. Notify Downstreams
	for _, cb := range sw.downstreams {
		if isDelete {
			cb(types.NodeID(key), false)
		} else {
			// Check if we are upstream
			if len(val.PathVector) > 0 && val.PathVector[len(val.PathVector)-1] == r.local.ID {
				cb(val.Node, true)
			}
		}
	}
}

func (r *RIB) cleanupWatcher(streamID types.StreamID, sw *streamWatcher) {
	r.watchersMu.Lock()
	defer r.watchersMu.Unlock()

	// Only clean up if it's the same watcher instance and empty
	if current, ok := r.watchers[streamID]; ok && current == sw {
		sw.mu.RLock()
		empty := len(sw.subscribers) == 0 && len(sw.downstreams) == 0
		sw.mu.RUnlock()

		if empty {
			sw.cancel()
			delete(r.watchers, streamID)
		}
	}
}

// SubscribeUpdates starts a watcher for a specific stream's routes
// It returns a cancel function to stop the watcher.
func (r *RIB) SubscribeUpdates(streamID types.StreamID, cb func(types.RouteKVValue)) func() {
	sw, err := r.getOrStartWatcher(streamID)
	if err != nil {
		r.logger.Error("Failed to watch stream", "stream_id", streamID, "error", err)
		return func() {}
	}

	sw.mu.Lock()
	sw.subSeq++
	id := sw.subSeq
	sw.subscribers[id] = cb
	sw.mu.Unlock()

	return func() {
		sw.mu.Lock()
		delete(sw.subscribers, id)
		sw.mu.Unlock()
		r.cleanupWatcher(streamID, sw)
	}
}

// WatchDownstream registers a callback to track who depends on us
func (r *RIB) WatchDownstream(streamID types.StreamID, cb func(downstreamNode types.NodeID, isAdd bool)) {
	sw, err := r.getOrStartWatcher(streamID)
	if err != nil {
		r.logger.Error("Failed to watch downstream", "stream_id", streamID, "error", err)
		return
	}

	sw.mu.Lock()
	sw.dsSeq++
	id := sw.dsSeq
	sw.downstreams[id] = cb
	sw.mu.Unlock()

	// No cleanup returned, persists until RIB close or implicit cleanup
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

	// Cancel all watchers
	r.watchersMu.Lock()
	for _, sw := range r.watchers {
		sw.cancel()
	}
	r.watchersMu.Unlock()

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
		StreamID:    entry.StreamID,
		Node:        r.local.ID,
		Region:      r.local.Region,
		Cluster:     r.local.Cluster,
		BaseCost:    entry.BaseCost,
		PathVector:  entry.PathVector,
		RttToOrigin: entry.RttToOrigin,
		ExpireAt:    time.Now().Add(RIBTTL),
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
