package routing

import (
	"fmt"
	"github.com/jiyeyuran/omnistream/pkg/types"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	// DefaultLingerDuration is the default time a pipe is kept alive after refcount drops to 0
	DefaultLingerDuration = 10 * time.Second

	// DefaultOptimizationInterval is the default interval for checking route optimization
	DefaultOptimizationInterval = 5 * time.Minute

	// DefaultOptimizationThreshold is the default cost improvement threshold to trigger switch
	DefaultOptimizationThreshold = 10
)

// FIB (Forwarding Information Base) manages local active connections
type FIB struct {
	entries map[types.StreamID]*types.FIBEntry
	mu      sync.RWMutex

	sf singleflight.Group

	rib         *RIB
	localNodeID types.NodeID

	// Callbacks for Pipe management
	ConnectPipe    func(streamID types.StreamID, upstream types.NodeID) (string, error)
	DisconnectPipe func(streamID types.StreamID, pipeID string, upstreamNode types.NodeID) error

	// Watcher management
	// watchers map[types.StreamID]jetstream.KeyWatcher // Old
	watchers   map[types.StreamID]func() // New: cancel func
	watchersMu sync.Mutex

	// Route Optimization Config
	EnableRouteOptimization bool
	OptimizationInterval    time.Duration
	OptimizationThreshold   int // Cost improvement threshold to trigger switch
	LingerDuration          time.Duration

	logger *slog.Logger
}

type FIBOption func(*FIB)

func WithRouteOptimization(enabled bool, interval time.Duration, threshold int) FIBOption {
	return func(f *FIB) {
		f.EnableRouteOptimization = enabled
		f.OptimizationInterval = interval
		f.OptimizationThreshold = threshold
	}
}

func WithLingerDuration(d time.Duration) FIBOption {
	return func(f *FIB) {
		f.LingerDuration = d
	}
}

func WithLogger(logger *slog.Logger) FIBOption {
	return func(f *FIB) {
		f.logger = logger
	}
}

func NewFIB(rib *RIB, localNodeID types.NodeID, opts ...FIBOption) *FIB {
	f := &FIB{
		entries:                 make(map[types.StreamID]*types.FIBEntry),
		rib:                     rib,
		localNodeID:             localNodeID,
		watchers:                make(map[types.StreamID]func()),
		EnableRouteOptimization: false,                        // Default disabled, enable via feature flag
		OptimizationInterval:    DefaultOptimizationInterval,  // Slow convergence
		OptimizationThreshold:   DefaultOptimizationThreshold, // Hysteresis
		LingerDuration:          DefaultLingerDuration,
		logger:                  slog.Default(),
	}

	for _, opt := range opts {
		opt(f)
	}

	f.logger = f.logger.With("component", "FIB")

	return f
}

// Subscribe increments ref count and establishes connection if needed
// Returns pipeID and upstream node
func (f *FIB) Subscribe(streamID types.StreamID, findPath func(types.StreamID) (*types.RouteKVValue, error)) (string, types.NodeID, error) {
	f.mu.Lock()
	entry, exists := f.entries[streamID]
	if exists {
		f.cancelTeardown(entry)
		entry.RefCount++
		f.logger.Info("Stream ref increased", "stream_id", streamID, "ref_count", entry.RefCount, "existing_pipe", entry.PipeID)
		f.mu.Unlock()
		return entry.PipeID, entry.UpstreamNode, nil
	}
	f.mu.Unlock()

	type subResult struct {
		pipeID string
		route  *types.RouteKVValue
	}

	// Singleflight for FindPath + ConnectPipe
	val, err, _ := f.sf.Do(string(streamID), func() (interface{}, error) {
		// 1. Find Path
		route, err := findPath(streamID)
		if err != nil {
			return nil, err
		}
		if route == nil {
			return nil, fmt.Errorf("no route found for %s", streamID)
		}

		f.logger.Info("Found best route", "stream_id", streamID, "via", route.Node, "cost", route.BaseCost)

		// 2. Connect Pipe (Mock)
		pipeID := fmt.Sprintf("pipe-%d", time.Now().UnixNano())
		if f.ConnectPipe != nil {
			var err error
			pipeID, err = f.ConnectPipe(streamID, route.Node)
			if err != nil {
				return nil, err
			}
		}
		return subResult{pipeID: pipeID, route: route}, nil
	})

	if err != nil {
		return "", "", err
	}

	res := val.(subResult)
	pipeID := res.pipeID
	route := res.route

	f.mu.Lock()
	defer f.mu.Unlock()

	// Check again in case created concurrently
	if entry, exists := f.entries[streamID]; exists {
		f.cancelTeardown(entry)
		entry.RefCount++
		f.logger.Info("Stream ref increased (concurrent)", "stream_id", streamID, "ref_count", entry.RefCount)
		return entry.PipeID, entry.UpstreamNode, nil
	}

	// 3. Create Entry
	entry = &types.FIBEntry{
		StreamID:         streamID,
		UpstreamNode:     route.Node,
		UpstreamIsOrigin: route.IsOrigin,
		PipeID:           pipeID,
		State:            "connected",
		RefCount:         1,
		DownstreamNodes:  make(map[types.NodeID]bool),
		FindPathFunc:     findPath,
	}
	f.entries[streamID] = entry

	// 4. Re-advertise (Propagate)
	newPathVector := append(route.PathVector, f.localNodeID)

	// Start Keepalive Advertise
	err = f.rib.StartAdvertise(AdvertiseOptions{
		StreamID:    streamID,
		BaseCost:    route.BaseCost + 10,
		PathVector:  newPathVector,
		RttToOrigin: route.RttToOrigin + 20,
		IsOrigin:    false,
	})
	if err != nil {
		f.logger.Error("Failed to start re-advertise", "stream_id", streamID, "error", err)
	} else {
		f.logger.Info("Re-advertised stream as new source", "stream_id", streamID)
	}

	// 5. Start Watching for Upstream Teardown AND Downstream Dependency
	f.monitorUpstream(streamID, route.Node, findPath)

	// 6. Schedule Periodic Optimization
	if f.EnableRouteOptimization {
		f.scheduleOptimization(entry)
	}

	// 7. Register Downstream Watcher
	f.rib.WatchDownstream(streamID, func(nodeID types.NodeID, isAdd bool) {
		f.handleDownstreamChange(streamID, nodeID, isAdd)
	})

	return pipeID, route.Node, nil
}

func (f *FIB) handleDownstreamChange(streamID types.StreamID, nodeID types.NodeID, isAdd bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	entry, exists := f.entries[streamID]
	if !exists {
		return
	}

	if isAdd {
		f.cancelTeardown(entry) // Cancel linger if active again
		if !entry.DownstreamNodes[nodeID] {
			entry.DownstreamNodes[nodeID] = true
			f.logger.Info("New downstream dependency", "stream_id", streamID, "downstream_node", nodeID, "total", len(entry.DownstreamNodes))
		}
	} else {
		if entry.DownstreamNodes[nodeID] {
			delete(entry.DownstreamNodes, nodeID)
			f.logger.Info("Downstream dependency removed", "stream_id", streamID, "downstream_node", nodeID, "total", len(entry.DownstreamNodes))

			// Check if we need to teardown
			// Logic: If RefCount == 0 AND len(DownstreamNodes) == 0 -> Teardown
			if entry.RefCount <= 0 && len(entry.DownstreamNodes) == 0 {
				f.logger.Info("Scheduling teardown (no users/downstreams)", "stream_id", streamID)
				f.scheduleTeardown(streamID)
			}
		}
	}
}

// monitorUpstream watches the RIB for changes to the upstream node
func (f *FIB) monitorUpstream(streamID types.StreamID, currentUpstream types.NodeID, findPath func(types.StreamID) (*types.RouteKVValue, error)) {
	cancel := f.rib.SubscribeUpdates(streamID, func(val types.RouteKVValue, deleted bool) {
		if deleted {
			// A route was deleted. Is it OUR upstream?
			if val.Node == currentUpstream {
				// 1. Check if we are connected to Origin (Fast check)
				f.mu.Lock()
				ent, ok := f.entries[streamID]
				isOrigin := ok && ent.UpstreamIsOrigin
				f.mu.Unlock()

				var candidates []types.RouteKVValue
				if !isOrigin {
					// 2. Not origin, so check alternatives (IO operation, outside lock)
					candidates, _ = f.rib.GetCandidates(streamID)
				}

				hasAlternatives := false
				if len(candidates) > 0 {
					// 3. Filter candidates (Requires lock for DownstreamNodes)
					f.mu.Lock()
					ent, ok = f.entries[streamID]
					for _, c := range candidates {
						if c.Node == f.localNodeID {
							continue
						}
						// Avoid loops through downstreams
						if ok && ent != nil && ent.DownstreamNodes[c.Node] {
							continue
						}
						hasAlternatives = true
						break
					}
					f.mu.Unlock()
				}

				if !hasAlternatives {
					f.logger.Info("No alternatives found, tearing down", "stream_id", streamID)
					go f.teardownUpstream(streamID, currentUpstream)
					return
				}

				// Trigger failover immediately.
				// handleFailover will check for alternatives and teardown if none found.
				f.logger.Warn("Upstream disappeared, triggering failover", "upstream", currentUpstream, "stream_id", streamID)
				go f.handleFailover(streamID, currentUpstream, findPath)
			}
		}
	})

	f.watchersMu.Lock()
	if oldCancel, ok := f.watchers[streamID]; ok {
		oldCancel() // Cleanup previous watcher
	}
	f.watchers[streamID] = cancel
	f.watchersMu.Unlock()

	defer func() {
		// No need to cancel here, it's done in Unsubscribe
		// Actually, this function exits immediately now (it's not a loop anymore).
		// So defer is useless/wrong here if we want to keep watching.
		// We should remove defer.
	}()

	f.logger.Info("Started monitoring upstream", "upstream", currentUpstream, "stream_id", streamID)
}

func (f *FIB) handleFailover(streamID types.StreamID, failedUpstream types.NodeID, findPath func(types.StreamID) (*types.RouteKVValue, error)) {
	f.mu.Lock()
	entry, exists := f.entries[streamID]
	if !exists {
		f.mu.Unlock()
		return // Already unsubscribed
	}

	// Ensure we are still connected to the failed upstream (race condition check)
	if entry.UpstreamNode != failedUpstream {
		f.mu.Unlock()
		return
	}

	// Disconnect old pipe
	if f.DisconnectPipe != nil {
		f.DisconnectPipe(streamID, entry.PipeID, entry.UpstreamNode)
	}
	f.mu.Unlock()

	f.logger.Info("Attempting to reroute stream", "stream_id", streamID)

	type subResult struct {
		pipeID string
		route  *types.RouteKVValue
	}

	// Use Singleflight for FindPath + ConnectPipe
	val, err, _ := f.sf.Do(string(streamID), func() (interface{}, error) {
		// 1. Try to find a NEW path
		newRoute, err := findPath(streamID)
		if err != nil || newRoute == nil {
			return nil, fmt.Errorf("no alternative path")
		}

		// 2. Connect new pipe
		newPipeID := fmt.Sprintf("pipe-%d-reroute", time.Now().UnixNano())
		if f.ConnectPipe != nil {
			pid, err := f.ConnectPipe(streamID, newRoute.Node)
			if err != nil {
				return nil, err
			}
			newPipeID = pid
		}
		return subResult{pipeID: newPipeID, route: newRoute}, nil
	})

	if err != nil {
		f.logger.Error("Reroute failed", "stream_id", streamID, "error", err)

		f.teardownUpstream(streamID, failedUpstream)
		return
	}

	res := val.(subResult)
	newRoute := res.route
	newPipeID := res.pipeID

	f.mu.Lock()
	// Re-check existence
	if entry, ok := f.entries[streamID]; ok {
		// Only update if we haven't already switched to this node
		if entry.UpstreamNode != newRoute.Node {
			f.logger.Info("Reroute successful", "from", failedUpstream, "to", newRoute.Node)

			entry.UpstreamNode = newRoute.Node
			entry.UpstreamIsOrigin = newRoute.IsOrigin
			entry.PipeID = newPipeID
		}
	}
	f.mu.Unlock()

	// Restart monitoring for NEW upstream
	go f.monitorUpstream(streamID, newRoute.Node, findPath)

	// Update Re-advertise (PathVector changed!)
	// We need to update our advertisement because the path vector is different now
	newPathVector := append(newRoute.PathVector, f.localNodeID)
	if err := f.rib.StartAdvertise(AdvertiseOptions{
		StreamID:    streamID,
		BaseCost:    newRoute.BaseCost + 10,
		PathVector:  newPathVector,
		RttToOrigin: newRoute.RttToOrigin + 20,
		IsOrigin:    false,
	}); err != nil {
		f.logger.Error("Failed to re-advertise during failover", "stream_id", streamID, "error", err)
	}
}

// teardownUpstream safely disconnects and clears the upstream state
func (f *FIB) teardownUpstream(streamID types.StreamID, expectedUpstream types.NodeID) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ent, ok := f.entries[streamID]; ok && ent.UpstreamNode == expectedUpstream {
		f.teardownLocked(streamID)
	}
}

// Unsubscribe decrements ref count and tears down if 0
func (f *FIB) Unsubscribe(streamID types.StreamID) {
	f.mu.Lock()
	defer f.mu.Unlock()

	entry, exists := f.entries[streamID]
	if !exists {
		return
	}

	// If forced (e.g. failover failed), we might want to clear all refs
	// But standard Unsubscribe just decs one.
	// For failover case, we might need a "ForceTeardown".
	// Assuming standard usage:
	entry.RefCount--
	f.logger.Info("Stream ref decreased", "stream_id", streamID, "ref_count", entry.RefCount)

	if entry.RefCount <= 0 && len(entry.DownstreamNodes) == 0 {
		// Stop Optimization if we are tearing down (or lingering)
		if entry.OptimizationTimer != nil {
			entry.OptimizationTimer.Stop()
			entry.OptimizationTimer = nil
		}
		f.scheduleTeardown(streamID)
	}
}

// scheduleTeardown schedules disconnection after linger time
func (f *FIB) scheduleTeardown(streamID types.StreamID) {
	entry, exists := f.entries[streamID]
	if !exists {
		return
	}

	if entry.TeardownTimer != nil {
		return // Already scheduled
	}

	entry.State = "lingering"
	f.logger.Info("Stream entered lingering state", "stream_id", streamID, "duration", f.LingerDuration)

	entry.TeardownTimer = time.AfterFunc(f.LingerDuration, func() {
		f.mu.Lock()
		defer f.mu.Unlock()

		// Re-check conditions
		// Note: entry pointer is still valid as long as we didn't delete it from map
		// But verify it's still the same entry in map
		currentEntry, ok := f.entries[streamID]
		if !ok || currentEntry != entry {
			return // Already deleted or replaced
		}

		if currentEntry.RefCount > 0 || len(currentEntry.DownstreamNodes) > 0 {
			// Resurrected during wait!
			currentEntry.TeardownTimer = nil
			currentEntry.State = "connected"
			f.logger.Info("Stream resurrected during linger", "stream_id", streamID)
			return
		}

		f.logger.Info("Stream linger expired, executing teardown", "stream_id", streamID)
		f.teardownLocked(streamID)
	})
}

// cancelTeardown stops the timer and restores state
func (f *FIB) cancelTeardown(entry *types.FIBEntry) {
	if entry.TeardownTimer != nil {
		entry.TeardownTimer.Stop()
		entry.TeardownTimer = nil
		entry.State = "connected"
		f.logger.Info("Stream linger cancelled (reused)", "stream_id", entry.StreamID)

		// Restart Optimization Timer if enabled
		if f.EnableRouteOptimization {
			f.scheduleOptimization(entry)
		}
	}
}

// teardownLocked performs the actual disconnection. caller must hold lock.
func (f *FIB) teardownLocked(streamID types.StreamID) {
	entry, exists := f.entries[streamID]
	if !exists {
		return
	}

	// Stop any pending timers
	if entry.TeardownTimer != nil {
		entry.TeardownTimer.Stop()
		entry.TeardownTimer = nil
	}
	if entry.OptimizationTimer != nil {
		entry.OptimizationTimer.Stop()
		entry.OptimizationTimer = nil
	}

	// Stop Watcher
	f.watchersMu.Lock()
	if cancel, ok := f.watchers[streamID]; ok {
		cancel()
		delete(f.watchers, streamID)
	}
	f.watchersMu.Unlock()

	// Teardown
	if f.DisconnectPipe != nil {
		f.DisconnectPipe(streamID, entry.PipeID, entry.UpstreamNode)
	}
	delete(f.entries, streamID)

	// Stop Advertise
	err := f.rib.StopAdvertise(streamID)
	if err != nil {
		f.logger.Error("Failed to stop advertise", "stream_id", streamID, "error", err)
	} else {
		f.logger.Info("Unadvertised stream", "stream_id", streamID)
	}
}

// Close cleans up all FIB resources
func (f *FIB) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.logger.Info("Closing: Cleaning up all streams")

	for streamID, entry := range f.entries {
		// Stop Timer
		if entry.TeardownTimer != nil {
			entry.TeardownTimer.Stop()
		}
		if entry.OptimizationTimer != nil {
			entry.OptimizationTimer.Stop()
		}

		// Disconnect Pipe
		if f.DisconnectPipe != nil {
			f.logger.Info("Disconnecting pipe", "stream_id", streamID)
			f.DisconnectPipe(streamID, entry.PipeID, entry.UpstreamNode)
		}

		// Stop Watcher
		f.watchersMu.Lock()
		if cancel, ok := f.watchers[streamID]; ok {
			cancel()
			delete(f.watchers, streamID)
		}
		f.watchersMu.Unlock()
	}

	// Clear map
	f.entries = make(map[types.StreamID]*types.FIBEntry)
}

// scheduleOptimization starts a timer to periodically check for better paths
func (f *FIB) scheduleOptimization(entry *types.FIBEntry) {
	if entry.OptimizationTimer != nil {
		entry.OptimizationTimer.Stop()
	}

	entry.OptimizationTimer = time.AfterFunc(f.OptimizationInterval, func() {
		f.optimizeRoute(entry.StreamID)
	})
}

// optimizeRoute checks if there is a significantly better path
func (f *FIB) optimizeRoute(streamID types.StreamID) {
	f.mu.Lock()
	entry, exists := f.entries[streamID]
	if !exists {
		f.mu.Unlock()
		return
	}

	// Reschedule optimization (periodic)
	defer f.scheduleOptimization(entry)

	currentUpstream := entry.UpstreamNode
	findPath := entry.FindPathFunc

	// Current Cost?
	// We don't store current cost in FIBEntry, but we can query RIB or guess.
	// Actually, we should probably just find the BEST path now and compare.
	f.mu.Unlock()

	// Find best path
	bestRoute, err := findPath(streamID)
	if err != nil || bestRoute == nil {
		return // No path found, keep current
	}

	if bestRoute.Node == currentUpstream {
		return // Same upstream, nothing to do
	}

	// We need to compare costs.
	// Issue: findPath usually returns the best route.
	// If it returns a different node, it implies that node is "better" (lower cost).
	// But we need to apply Hysteresis (Threshold).

	// To do this properly, we need the Cost of the CURRENT route.
	// We can try to fetch the candidate for current upstream from RIB.
	candidates, err := f.rib.GetCandidates(streamID)
	if err != nil {
		return
	}

	var currentRoute *types.RouteKVValue
	for _, c := range candidates {
		if c.Node == currentUpstream {
			currentRoute = &c
			break
		}
	}

	if currentRoute == nil {
		// Current upstream is gone?
		// If gone, we should have triggered failover already.
		// But maybe failover is slow or racing.
		// If we found a new best route, let's switch.
		f.logger.Info("Optimize: Current upstream not found, switching", "current", currentUpstream, "new", bestRoute.Node)
		f.switchUpstream(streamID, bestRoute)
		return
	}

	// Skip optimization if current upstream is in the same Cluster
	// Intra-cluster traffic is usually preferred/free, so don't switch away unless broken.
	if currentRoute.Cluster == f.rib.LocalInfo().Cluster {
		// log.Printf("[FIB] Optimize: Current upstream %s is in same cluster (%s). Skipping optimization.", currentUpstream, currentRoute.OriginCluster)
		return
	}

	// Apply Threshold
	// Switch if: NewCost < CurrentCost - Threshold
	if bestRoute.BaseCost < currentRoute.BaseCost-f.OptimizationThreshold {
		f.logger.Info("Optimize: Found better path", "new_node", bestRoute.Node, "new_cost", bestRoute.BaseCost, "current_node", currentUpstream, "current_cost", currentRoute.BaseCost)
		f.switchUpstream(streamID, bestRoute)
	}
}

// switchUpstream performs a Make-Before-Break switch to a new upstream
func (f *FIB) switchUpstream(streamID types.StreamID, newRoute *types.RouteKVValue) {
	f.mu.Lock()
	entry, exists := f.entries[streamID]
	if !exists {
		f.mu.Unlock()
		return
	}
	oldUpstream := entry.UpstreamNode
	oldPipeID := entry.PipeID
	f.mu.Unlock()

	f.logger.Info("Switching upstream", "stream_id", streamID, "from", oldUpstream, "to", newRoute.Node)

	// 1. Connect New Pipe
	var newPipeID string
	if f.ConnectPipe != nil {
		var err error
		newPipeID, err = f.ConnectPipe(streamID, newRoute.Node)
		if err != nil {
			f.logger.Error("Switch failed: ConnectPipe error", "error", err)
			return
		}
	}

	// 2. Update FIB Entry
	f.mu.Lock()
	// Re-check existence
	entry, exists = f.entries[streamID]
	if !exists {
		f.mu.Unlock()
		// Clean up new pipe
		if f.DisconnectPipe != nil {
			f.DisconnectPipe(streamID, newPipeID, newRoute.Node)
		}
		return
	}

	entry.UpstreamNode = newRoute.Node
	entry.UpstreamIsOrigin = newRoute.IsOrigin
	entry.PipeID = newPipeID
	f.mu.Unlock()

	// 3. Update Advertisement (Propagate new path)
	newPathVector := append(newRoute.PathVector, f.localNodeID)
	err := f.rib.StartAdvertise(AdvertiseOptions{
		StreamID:    streamID,
		BaseCost:    newRoute.BaseCost + 10,
		PathVector:  newPathVector,
		RttToOrigin: newRoute.RttToOrigin + 20,
		IsOrigin:    false,
	})
	if err != nil {
		f.logger.Error("Failed to update advertisement during switch", "error", err)
	}

	// 4. Update Watcher
	// We need to monitor the NEW upstream
	f.monitorUpstream(streamID, newRoute.Node, entry.FindPathFunc)

	// 5. Disconnect Old Pipe
	if f.DisconnectPipe != nil {
		f.DisconnectPipe(streamID, oldPipeID, oldUpstream)
	}

	f.logger.Info("Switch complete", "stream_id", streamID, "via", newRoute.Node)
}
