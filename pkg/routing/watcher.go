package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"rib-control/pkg/types"

	"github.com/nats-io/nats.go/jetstream"
)

type DynamicWatcher struct {
	kv     jetstream.KeyValue
	ctx    context.Context
	cancel context.CancelFunc

	updateCh chan struct{}

	mu sync.RWMutex
	// map[pattern]map[handleID]callback
	interests map[string]map[int64]func(types.RouteKVValue, bool)

	watcher jetstream.KeyWatcher

	debounceInterval time.Duration
	handleSeq        int64
}

func NewDynamicWatcher(kv jetstream.KeyValue) *DynamicWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	dw := &DynamicWatcher{
		kv:               kv,
		ctx:              ctx,
		cancel:           cancel,
		updateCh:         make(chan struct{}, 1),
		interests:        make(map[string]map[int64]func(types.RouteKVValue, bool)),
		debounceInterval: 100 * time.Millisecond, // Slightly larger debounce to aggregate updates
	}

	go dw.run()
	return dw
}

func (dw *DynamicWatcher) Close() {
	dw.cancel()
}

func (dw *DynamicWatcher) WatchStream(streamID types.StreamID, cb func(types.RouteKVValue, bool)) func() {
	dw.mu.Lock()
	pattern := fmt.Sprintf("route.%s.>", streamID)

	id := atomic.AddInt64(&dw.handleSeq, 1)

	if _, ok := dw.interests[pattern]; !ok {
		dw.interests[pattern] = make(map[int64]func(types.RouteKVValue, bool))
	}
	dw.interests[pattern][id] = cb
	dw.mu.Unlock()

	dw.triggerUpdate()

	return func() {
		dw.mu.Lock()
		if m, ok := dw.interests[pattern]; ok {
			delete(m, id)
			if len(m) == 0 {
				delete(dw.interests, pattern)
			}
		}
		dw.mu.Unlock()
		dw.triggerUpdate()
	}
}

func (dw *DynamicWatcher) triggerUpdate() {
	select {
	case dw.updateCh <- struct{}{}:
	default:
	}
}

func (dw *DynamicWatcher) run() {
	timer := time.NewTimer(dw.debounceInterval)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	var pendingUpdate bool

	for {
		select {
		case <-dw.ctx.Done():
			if dw.watcher != nil {
				dw.watcher.Stop()
			}
			return

		case <-dw.updateCh:
			if !pendingUpdate {
				pendingUpdate = true
				timer.Reset(dw.debounceInterval)
			}

		case <-timer.C:
			pendingUpdate = false
			dw.updateWatcher()
		}
	}
}

func (dw *DynamicWatcher) updateWatcher() {
	dw.mu.RLock()
	patterns := make([]string, 0, len(dw.interests))
	for p := range dw.interests {
		patterns = append(patterns, p)
	}
	dw.mu.RUnlock()

	// Stop old watcher
	if dw.watcher != nil {
		dw.watcher.Stop()
		dw.watcher = nil
	}

	if len(patterns) == 0 {
		return
	}

	// Start new watcher
	// WatchFiltered takes keys. Our patterns are like "route.stream1.>"
	// By default it includes deletes unless jetstream.IgnoreDeletes() is passed.
	w, err := dw.kv.WatchFiltered(dw.ctx, patterns)

	if err != nil {
		log.Printf("[DynamicWatcher] Failed to watch: %v", err)
		// Retry?
		go func() {
			time.Sleep(1 * time.Second)
			dw.triggerUpdate()
		}()
		return
	}

	dw.watcher = w

	// Start consumer loop
	go func(w jetstream.KeyWatcher) {
		defer w.Stop()
		for entry := range w.Updates() {
			if entry == nil {
				return
			}
			dw.dispatch(entry)
		}
	}(w)
}

func (dw *DynamicWatcher) dispatch(entry jetstream.KeyValueEntry) {
	key := entry.Key() // e.g. route.stream1.R1.C1.NodeA

	isDelete := entry.Operation() == jetstream.KeyValueDelete || entry.Operation() == jetstream.KeyValuePurge

	var val types.RouteKVValue
	if isDelete {
		val.BaseCost = -1
		val.OriginNode = types.NodeID(key)
		// Extract StreamID from key
		parts := strings.Split(key, ".")
		if len(parts) >= 2 {
			val.StreamID = types.StreamID(parts[1])
		}
	} else {
		if err := json.Unmarshal(entry.Value(), &val); err != nil {
			return
		}
	}

	// Find matching patterns
	dw.mu.RLock()
	defer dw.mu.RUnlock()

	// Optimization: We know the pattern is route.<StreamID>.>
	if val.StreamID == "" {
		return
	}
	pattern := fmt.Sprintf("route.%s.>", val.StreamID)

	if cbs, ok := dw.interests[pattern]; ok {
		for _, cb := range cbs {
			cb(val, isDelete)
		}
	}
}
