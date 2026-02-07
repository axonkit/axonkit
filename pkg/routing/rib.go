package routing

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
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
	WorkerCount   = 5
	QueueSize     = 100
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

	// Channels for coordinator
	signalCh chan struct{} // signal when new item added
	stopCh   chan struct{} // stop coordinator

	// Worker pool
	jobCh chan *adEntry

	// Global Watcher
	subscribers   map[types.StreamID][]func(types.RouteKVValue)
	subscribersMu sync.RWMutex

	// Downstream Watcher
	downstreamCallbacks   map[types.StreamID]func(types.NodeID, bool)
	downstreamCallbacksMu sync.RWMutex

	// Dynamic Consumer
	consumer         jetstream.Consumer
	consumeCtx       jetstream.ConsumeContext
	activeFilter     []string
	filterMu         sync.Mutex
	consumerUpdateCh chan struct{} // Signal to update consumer filter
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
		kv:                  kv,
		js:                  js,
		local:               local,
		ctx:                 ctx,
		pq:                  ds.NewPriorityQueue[*adEntry](0),
		entryMap:            make(map[types.StreamID]*ds.Item[*adEntry]),
		signalCh:            make(chan struct{}, 1),
		stopCh:              make(chan struct{}),
		jobCh:               make(chan *adEntry, QueueSize),
		subscribers:         make(map[types.StreamID][]func(types.RouteKVValue)),
		downstreamCallbacks: make(map[types.StreamID]func(types.NodeID, bool)),
		activeFilter:        []string{}, // Start empty
		consumerUpdateCh:    make(chan struct{}, 1),
	}

	heap.Init(&r.pq)

	// Start coordinator and workers
	go r.coordinator()
	for i := 0; i < WorkerCount; i++ {
		go r.worker(i)
	}

	// Start Dynamic Watcher (initially idle)
	// We will start it when first subscription happens or if we want to watch something default
	// But actually, Downstream Watcher needs "route.>" because we don't know which stream will have us as downstream.
	// This is the catch!
	// If we want to detect "Who depends on me?", we MUST watch "route.>".
	// Unless we require downstream to explicitly signal "I depend on you".

	// But wait, user said: "Subscribe all routes is wasteful".
	// If we optimize for subscribers, we can filter.
	// But for "Downstream Discovery", we scan everything.

	// Maybe we can optimize Downstream Discovery?
	// If downstream nodes use a specific subject to announce dependency? e.g. "link.US-A.US-B" -> "stream-1"
	// But that changes the protocol.

	// If we stick to KV Route Table:
	// We have to pay the price of scanning everything IF we want "Zero-Config Downstream Discovery".
	// OR, we accept that "route.>" is necessary for the router role.

	// BUT, if we are an Edge Node (Leaf), we might not have downstreams.
	// So we can start with Filtered Watcher.
	// If someone registers DownstreamWatcher, we switch to "route.>"?

	// Let's implement Dynamic Filter Update.
	// If activeFilter is empty or contains ">", we watch everything.
	// Initially, we might just watch nothing?
	// But globalWatcherLoop was watching "route.>".

	// Let's start with a filter that matches nothing? Or just don't start consume.
	// But we need to handle "WatchDownstream".
	// If WatchDownstream is called, we probably need full watch.

	// Implementation Note:
	// We use an Event-Driven Dynamic Watcher (manageConsumer).
	// It only watches streams that are explicitly subscribed or have active downstream watchers.
	// This assumes that we only care about downstream dependencies for streams we are already participating in.
	go r.manageConsumer()

	return r, nil
}

// updateConsumerFilter updates the NATS consumer filter subjects
func (r *RIB) updateConsumerFilter() {
	r.filterMu.Lock()
	defer r.filterMu.Unlock()

	// Gather all interested subjects
	subjects := make(map[string]bool)

	// 1. Explicit Subscribers
	r.subscribersMu.RLock()
	for streamID := range r.subscribers {
		// KV subjects are prefixed with $KV.<Bucket>.
		// Key: route.<StreamID>...
		// Subject: $KV.RIB.route.<StreamID>.>
		subj := fmt.Sprintf("$KV.%s.route.%s.>", RibBucketName, streamID)
		subjects[subj] = true
	}
	r.subscribersMu.RUnlock()

	// 2. Downstream Watchers
	r.downstreamCallbacksMu.RLock()
	if len(r.downstreamCallbacks) > 0 {
		// If we are watching for downstreams, we ideally need to see ALL routes
		// to check if they contain us in PathVector.
		// UNLESS we can filter by PathVector in NATS (we can't).
		// So if any downstream watcher is active, we must fall back to "route.>"
		// UNLESS we only care about specific streams.
		// The callbacks are per-streamID.
		// So we can just add those streams to filter!
		for streamID := range r.downstreamCallbacks {
			subj := fmt.Sprintf("$KV.%s.route.%s.>", RibBucketName, streamID)
			subjects[subj] = true
		}
	}
	r.downstreamCallbacksMu.RUnlock()

	// Convert to list
	var filterSubjects []string
	for s := range subjects {
		filterSubjects = append(filterSubjects, s)
	}

	if len(filterSubjects) == 0 {
		// No interest, stop consumer if running
		if r.consumeCtx != nil {
			r.consumeCtx.Stop()
			r.consumeCtx = nil
		}
		return
	}

	// Update or Create Consumer
	streamName := fmt.Sprintf("KV_%s", RibBucketName)

	cfg := jetstream.ConsumerConfig{
		Name:    fmt.Sprintf("RIB_Watcher_%s", r.local.ID),
		Durable: fmt.Sprintf("RIB_Watcher_%s", r.local.ID), // Durable to resume? Or Ephemeral?
		// Ephemeral is better for "current state". Durable might replay old stuff.
		// Actually for KV Watch, we want "Last Value" policy.
		// But JetStream FilterSubjects update on existing consumer is allowed?
		// Yes, for ephemeral.
		FilterSubjects: filterSubjects,
		DeliverPolicy:  jetstream.DeliverLastPerSubjectPolicy,
		AckPolicy:      jetstream.AckNonePolicy,
	}

	// We recreate consumer to apply new filters (simplest way)
	if r.consumeCtx != nil {
		r.consumeCtx.Stop()
	}

	// Note: CreateOrUpdateConsumer is needed
	// But KV backing stream is managed by NATS. We are just a consumer.
	cons, err := r.js.CreateOrUpdateConsumer(r.ctx, streamName, cfg)
	if err != nil {
		log.Printf("[RIB] Failed to update consumer: %v", err)
		return
	}

	r.consumer = cons
	cc, err := cons.Consume(r.handleMsg)
	if err != nil {
		log.Printf("[RIB] Failed to consume: %v", err)
		return
	}
	r.consumeCtx = cc
	log.Printf("[RIB] Updated watcher filters: %d subjects", len(filterSubjects))
}

func (r *RIB) handleMsg(msg jetstream.Msg) {
	// Parse KV Message
	// Subject: $KV.RIB.route.<StreamID>...
	// Payload: RouteKVValue

	// Extract Key from Subject
	// Prefix: $KV.RIB.
	prefix := fmt.Sprintf("$KV.%s.", RibBucketName)
	if !strings.HasPrefix(msg.Subject(), prefix) {
		return
	}
	key := msg.Subject()[len(prefix):]

	// Parse headers to detect delete
	// KV uses headers for delete markers (KV-Operation: DEL)
	op := msg.Headers().Get("KV-Operation")
	isDelete := op == "DEL" || op == "PURGE"

	var val types.RouteKVValue
	var streamID types.StreamID

	if !isDelete {
		if err := json.Unmarshal(msg.Data(), &val); err != nil {
			return
		}
		streamID = val.StreamID

		// Check Downstream Dependency
		if len(val.PathVector) > 0 && val.PathVector[len(val.PathVector)-1] == r.local.ID {
			r.downstreamCallbacksMu.RLock()
			if cb, ok := r.downstreamCallbacks[streamID]; ok {
				go cb(val.OriginNode, true)
			}
			r.downstreamCallbacksMu.RUnlock()
		}
	} else {
		// Handle Delete
		// Extract StreamID from Key: route.<StreamID>...
		// key: route.stream-1.CN...
		parts := strings.Split(key, ".")
		if len(parts) >= 2 {
			streamID = types.StreamID(parts[1])
		}

		// Notify Subscribers
		r.subscribersMu.RLock()
		if subs, ok := r.subscribers[streamID]; ok {
			evt := types.RouteKVValue{
				StreamID:   streamID,
				BaseCost:   -1,
				OriginNode: types.NodeID(key), // Key as NodeID placeholder
			}
			for _, cb := range subs {
				cb(evt)
			}
		}
		r.subscribersMu.RUnlock()

		// Notify Downstream Watchers
		r.downstreamCallbacksMu.RLock()
		if cb, ok := r.downstreamCallbacks[streamID]; ok {
			go cb(types.NodeID(key), false)
		}
		r.downstreamCallbacksMu.RUnlock()
	}

	if !isDelete {
		// Notify Subscribers
		r.subscribersMu.RLock()
		if subs, ok := r.subscribers[streamID]; ok {
			for _, cb := range subs {
				cb(val)
			}
		}
		r.subscribersMu.RUnlock()
	}
}

// manageConsumer manages the dynamic consumer lifecycle
func (r *RIB) manageConsumer() {
	// Initial update (debounce a bit to let initial subscriptions happen)
	time.Sleep(10 * time.Millisecond)
	r.updateConsumerFilter()

	for {
		select {
		case <-r.stopCh:
			return
		case <-r.consumerUpdateCh:
			// Debounce: Wait a bit to accumulate more updates
			time.Sleep(10 * time.Millisecond)

			// Drain remaining signals
		drain:
			for {
				select {
				case <-r.consumerUpdateCh:
					continue
				default:
					break drain
				}
			}

			// Check if we are stopped before updating
			select {
			case <-r.stopCh:
				return
			default:
			}

			r.updateConsumerFilter()
		}
	}
}

// SubscribeUpdates registers a callback to receive updates for a stream
func (r *RIB) SubscribeUpdates(streamID types.StreamID, cb func(types.RouteKVValue)) func() {
	r.subscribersMu.Lock()
	r.subscribers[streamID] = append(r.subscribers[streamID], cb)
	r.subscribersMu.Unlock()

	// Trigger Filter Update (Non-blocking)
	select {
	case r.consumerUpdateCh <- struct{}{}:
	default:
	}

	return func() {
		r.subscribersMu.Lock()
		defer r.subscribersMu.Unlock()
		delete(r.subscribers, streamID)

		// Trigger Filter Update (Non-blocking)
		select {
		case r.consumerUpdateCh <- struct{}{}:
		default:
		}
	}
}

// WatchDownstream registers a callback to track who depends on us
func (r *RIB) WatchDownstream(streamID types.StreamID, cb func(downstreamNode types.NodeID, isAdd bool)) {
	r.downstreamCallbacksMu.Lock()
	r.downstreamCallbacks[streamID] = cb
	r.downstreamCallbacksMu.Unlock()

	// Trigger Filter Update (Non-blocking)
	select {
	case r.consumerUpdateCh <- struct{}{}:
	default:
	}
}

// globalWatcherLoop is removed/replaced by dynamic consumer
func (r *RIB) globalWatcherLoop() {
	// Legacy method, kept empty or removed.
	// We use updateConsumerFilter to start/stop consumer.
}

// StartAdvertise registers a stream for keepalive
func (r *RIB) StartAdvertise(streamID types.StreamID, baseCost int, pathVector []types.NodeID, rttToOrigin int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.entryMap[streamID]; exists {
		return nil
	}

	// Initial advertise immediately (blocking or async? let's do sync for first time to catch errors)
	if err := r.advertiseOnce(streamID, baseCost, pathVector, rttToOrigin); err != nil {
		return err
	}

	val := &adEntry{
		StreamID:    streamID,
		BaseCost:    baseCost,
		PathVector:  pathVector,
		RttToOrigin: rttToOrigin,
	}

	priority := time.Now().Add(RefreshRate).UnixNano()
	item := &ds.Item[*adEntry]{
		Value:    val,
		Priority: priority,
	}

	r.entryMap[streamID] = item
	heap.Push(&r.pq, item)

	// Signal coordinator if this is the new earliest item (or just always signal)
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

// coordinator manages the priority queue
func (r *RIB) coordinator() {
	timer := time.NewTimer(1 * time.Hour) // Long idle time
	defer timer.Stop()

	for {
		var delay time.Duration

		r.mu.Lock()
		if r.pq.Len() == 0 {
			// No items, wait indefinitely (until signal)
			// We set a long delay, but rely on signalCh to wake us up
			delay = 1 * time.Hour
		} else {
			item := r.pq[0]
			now := time.Now().UnixNano()
			if now > item.Priority {
				// Due immediately
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
			// New item added, loop again to re-evaluate delay
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			continue

		case <-timer.C:
			// Timer fired, check if we have work
			r.mu.Lock()

			// Use PeekAndShift to try getting expired items
			now := time.Now().UnixNano()
			if item, wait := r.pq.PeekAndShift(now); item != nil {
				// Process item
				// Dispatch
				// NON-BLOCKING send to avoid deadlock if workers are slow/stuck
				select {
				case r.jobCh <- item.Value:
					// Scheduled
				default:
					log.Printf("[RIB] Worker queue full, skipping update for %s", item.Value.StreamID)
				}

				// Re-add to queue with new time
				item.Priority = time.Now().Add(RefreshRate).UnixNano()
				heap.Push(&r.pq, item)
			} else if wait > 0 {
				// Reset timer if we woke up too early?
			}

			r.mu.Unlock()
		}
	}
}

func (r *RIB) worker(id int) {
	for {
		select {
		case <-r.stopCh:
			return
		case item := <-r.jobCh:
			if err := r.advertiseOnce(item.StreamID, item.BaseCost, item.PathVector, item.RttToOrigin); err != nil {
				log.Printf("[RIB-Worker-%d] Advertise failed for %s: %v", id, item.StreamID, err)
			}
		}
	}
}

// Close cleans up resources and stops advertising all streams
func (r *RIB) Close() {
	// Stop coordinator and workers
	close(r.stopCh)

	// Stop Dynamic Consumer
	if r.consumeCtx != nil {
		r.consumeCtx.Stop()
	}

	// Unadvertise all active streams
	r.mu.Lock()
	defer r.mu.Unlock()

	for streamID, item := range r.entryMap {
		log.Printf("[RIB] Closing: Unadvertising stream %s", streamID)
		// We do best effort delete
		if err := r.Unadvertise(streamID); err != nil {
			log.Printf("[RIB] Failed to unadvertise %s: %v", streamID, err)
		}
		// Clean map (optional since we exit)
		delete(r.entryMap, streamID)
		heap.Remove(&r.pq, item.Index)
	}
}

// advertiseOnce writes to KV once
func (r *RIB) advertiseOnce(streamID types.StreamID, baseCost int, pathVector []types.NodeID, rttToOrigin int64) error {
	key := fmt.Sprintf("route.%s.%s.%s.%s", streamID, r.local.Region, r.local.Cluster, r.local.ID)

	val := types.RouteKVValue{
		StreamID:      streamID,
		OriginNode:    r.local.ID,
		OriginRegion:  r.local.Region,
		OriginCluster: r.local.Cluster,
		BaseCost:      baseCost,
		PathVector:    pathVector,
		RttToOrigin:   rttToOrigin,
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
