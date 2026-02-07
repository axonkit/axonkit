package routing

import (
	"fmt"
	"log"
	"rib-control/pkg/types"
	"sync"
	"time"
)

// FIB (Forwarding Information Base) manages local active connections
type FIB struct {
	entries map[types.StreamID]*types.FIBEntry
	mu      sync.RWMutex

	rib         *RIB
	localNodeID types.NodeID

	// Callbacks for Pipe management
	ConnectPipe    func(streamID types.StreamID, upstream types.NodeID) (string, error)
	DisconnectPipe func(pipeID string) error

	// Watcher management
	// watchers map[types.StreamID]jetstream.KeyWatcher // Old
	watchers   map[types.StreamID]func() // New: cancel func
	watchersMu sync.Mutex

	// Route Optimization Config
	EnableRouteOptimization bool
	OptimizationInterval    time.Duration
	OptimizationThreshold   int // Cost improvement threshold to trigger switch
}

func NewFIB(rib *RIB, localNodeID types.NodeID) *FIB {
	return &FIB{
		entries:                 make(map[types.StreamID]*types.FIBEntry),
		rib:                     rib,
		localNodeID:             localNodeID,
		watchers:                make(map[types.StreamID]func()),
		EnableRouteOptimization: false,           // Default disabled, enable via feature flag
		OptimizationInterval:    5 * time.Minute, // Slow convergence
		OptimizationThreshold:   10,              // Hysteresis
	}
}

// Subscribe increments ref count and establishes connection if needed
// Returns pipeID and upstream node
func (f *FIB) Subscribe(streamID types.StreamID, findPath func(types.StreamID) (*types.RouteKVValue, error)) (string, types.NodeID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	entry, exists := f.entries[streamID]
	if exists {
		f.cancelTeardown(entry)
		entry.RefCount++
		log.Printf("[FIB] Stream %s ref++ -> %d (Existing Pipe: %s)", streamID, entry.RefCount, entry.PipeID)
		return entry.PipeID, entry.UpstreamNode, nil
	}

	// New subscription
	// 1. Find Path
	route, err := findPath(streamID)
	if err != nil {
		return "", "", err
	}
	if route == nil {
		return "", "", fmt.Errorf("no route found for %s", streamID)
	}

	log.Printf("[FIB] Stream %s: Found best route via %s (Origin: %s, Cost: %d)",
		streamID, route.OriginNode, route.OriginNode, route.BaseCost)

	// 2. Connect Pipe (Mock)
	pipeID := fmt.Sprintf("pipe-%d", time.Now().UnixNano())
	if f.ConnectPipe != nil {
		var err error
		pipeID, err = f.ConnectPipe(streamID, route.OriginNode)
		if err != nil {
			return "", "", err
		}
	}

	// 3. Create Entry
	entry = &types.FIBEntry{
		StreamID:        streamID,
		UpstreamNode:    route.OriginNode,
		PipeID:          pipeID,
		State:           "connected",
		RefCount:        1,
		DownstreamNodes: make(map[types.NodeID]bool),
		FindPathFunc:    findPath,
	}
	f.entries[streamID] = entry

	// 4. Re-advertise (Propagate)
	newPathVector := append(route.PathVector, f.localNodeID)

	// Start Keepalive Advertise
	err = f.rib.StartAdvertise(streamID, route.BaseCost+10, newPathVector, route.RttToOrigin+20)
	if err != nil {
		log.Printf("[FIB] Failed to start re-advertise stream %s: %v", streamID, err)
	} else {
		log.Printf("[FIB] Re-advertised stream %s as new source (Keepalive started)", streamID)
	}

	// 5. Start Watching for Upstream Teardown AND Downstream Dependency
	f.monitorUpstream(streamID, route.OriginNode, findPath)

	// 6. Schedule Periodic Optimization
	if f.EnableRouteOptimization {
		f.scheduleOptimization(entry)
	}

	// 7. Register Downstream Watcher
	f.rib.WatchDownstream(streamID, func(nodeID types.NodeID, isAdd bool) {
		f.handleDownstreamChange(streamID, nodeID, isAdd)
	})

	return pipeID, route.OriginNode, nil
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
			log.Printf("[FIB] New downstream dependency for %s: %s (Total Downstreams: %d)", streamID, nodeID, len(entry.DownstreamNodes))
		}
	} else {
		if entry.DownstreamNodes[nodeID] {
			delete(entry.DownstreamNodes, nodeID)
			log.Printf("[FIB] Downstream dependency removed for %s: %s (Total Downstreams: %d)", streamID, nodeID, len(entry.DownstreamNodes))

			// Check if we need to teardown
			// Logic: If RefCount == 0 AND len(DownstreamNodes) == 0 -> Teardown
			if entry.RefCount <= 0 && len(entry.DownstreamNodes) == 0 {
				log.Printf("[FIB] No local users and no downstream dependencies for %s. Scheduling teardown.", streamID)
				f.scheduleTeardown(streamID)
			}
		}
	}
}

// monitorUpstream watches the RIB for changes to the upstream node
func (f *FIB) monitorUpstream(streamID types.StreamID, currentUpstream types.NodeID, findPath func(types.StreamID) (*types.RouteKVValue, error)) {
	cancel := f.rib.SubscribeUpdates(streamID, func(val types.RouteKVValue) {
		// Check for "Delete" event
		// We defined convention: BaseCost = -1 means deleted.
		// And OriginNode contains the Key (hack).

		if val.BaseCost == -1 {
			// A route was deleted. Is it OUR upstream?
			key := string(val.OriginNode) // We stored Key in OriginNode
			expectedSuffix := fmt.Sprintf(".%s", currentUpstream)

			if len(key) > len(expectedSuffix) && key[len(key)-len(expectedSuffix):] == expectedSuffix {
				log.Printf("[FIB] Upstream %s disappeared for stream %s! Triggering failover...", currentUpstream, streamID)

				// We need to trigger failover logic.
				// NOTE: This callback is executed in RIB's global watcher loop.
				// We MUST NOT block here or do heavy work.
				// We should spawn a goroutine or use a worker pool in FIB.
				// For now, spawn a goroutine to handle failover.
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

	log.Printf("[FIB] Started monitoring upstream %s for stream %s", currentUpstream, streamID)
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
	f.mu.Unlock()

	log.Printf("[FIB] Attempting to reroute stream %s...", streamID)

	// 1. Try to find a NEW path
	newRoute, err := findPath(streamID)
	if err != nil || newRoute == nil {
		log.Printf("[FIB] Reroute failed: No alternative path for %s. Tearing down downstream.", streamID)

		// Force stop advertising to prevent downstream from using us
		f.rib.StopAdvertise(streamID)

		// We should also disconnect the broken pipe
		if f.DisconnectPipe != nil {
			f.DisconnectPipe(entry.PipeID)
		}

		// We keep the entry (RefCount > 0), but state is broken.
		// Downstream nodes will see our StopAdvertise and trigger their own failover.
		// Local users will experience timeout/error.
		// Eventually users will Unsubscribe, reducing RefCount to 0.

		return
	}

	// 2. Found new path? Switch!
	log.Printf("[FIB] Reroute successful! Switching from %s to %s", failedUpstream, newRoute.OriginNode)

	// Disconnect old pipe
	if f.DisconnectPipe != nil {
		f.DisconnectPipe(entry.PipeID)
	}

	// Connect new pipe
	newPipeID := fmt.Sprintf("pipe-%d-reroute", time.Now().UnixNano())
	if f.ConnectPipe != nil {
		pid, err := f.ConnectPipe(streamID, newRoute.OriginNode)
		if err == nil {
			newPipeID = pid
		} else {
			log.Printf("[FIB] Failed to connect to new upstream: %v", err)
			f.Unsubscribe(streamID)
			return
		}
	}

	// Update FIB Entry
	f.mu.Lock()
	// Re-check existence
	if entry, ok := f.entries[streamID]; ok {
		entry.UpstreamNode = newRoute.OriginNode
		entry.PipeID = newPipeID
		// RefCount remains same
	}
	f.mu.Unlock()

	// Restart monitoring for NEW upstream
	go f.monitorUpstream(streamID, newRoute.OriginNode, findPath)

	// Update Re-advertise (PathVector changed!)
	// We need to update our advertisement because the path vector is different now
	newPathVector := append(newRoute.PathVector, f.localNodeID)
	f.rib.StartAdvertise(streamID, newRoute.BaseCost+10, newPathVector, newRoute.RttToOrigin+20)
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
	log.Printf("[FIB] Stream %s ref-- -> %d", streamID, entry.RefCount)

	if entry.RefCount <= 0 && len(entry.DownstreamNodes) == 0 {
		// Stop Optimization if we are tearing down (or lingering)
		if entry.OptimizationTimer != nil {
			entry.OptimizationTimer.Stop()
			entry.OptimizationTimer = nil
		}
		f.scheduleTeardown(streamID)
	}
}

const LingerDuration = 10 * time.Second

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
	log.Printf("[FIB] Stream %s entered lingering state (waiting %v)...", streamID, LingerDuration)

	entry.TeardownTimer = time.AfterFunc(LingerDuration, func() {
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
			log.Printf("[FIB] Stream %s resurrected during linger, cancelling teardown", streamID)
			return
		}

		log.Printf("[FIB] Stream %s linger expired. Executing physical teardown.", streamID)
		f.teardownLocked(streamID)
	})
}

// cancelTeardown stops the timer and restores state
func (f *FIB) cancelTeardown(entry *types.FIBEntry) {
	if entry.TeardownTimer != nil {
		entry.TeardownTimer.Stop()
		entry.TeardownTimer = nil
		entry.State = "connected"
		log.Printf("[FIB] Stream %s linger cancelled (reused)", entry.StreamID)

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
		f.DisconnectPipe(entry.PipeID)
	}
	delete(f.entries, streamID)

	// Stop Advertise
	err := f.rib.StopAdvertise(streamID)
	if err != nil {
		log.Printf("[FIB] Failed to stop advertise %s: %v", streamID, err)
	} else {
		log.Printf("[FIB] Unadvertised stream %s", streamID)
	}
}

// Close cleans up all FIB resources
func (f *FIB) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()

	log.Println("[FIB] Closing: Cleaning up all streams...")

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
			log.Printf("[FIB] Disconnecting pipe for %s", streamID)
			f.DisconnectPipe(entry.PipeID)
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

	if bestRoute.OriginNode == currentUpstream {
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
		if c.OriginNode == currentUpstream {
			currentRoute = &c
			break
		}
	}

	if currentRoute == nil {
		// Current upstream is gone?
		// If gone, we should have triggered failover already.
		// But maybe failover is slow or racing.
		// If we found a new best route, let's switch.
		log.Printf("[FIB] Optimize: Current upstream %s not found in RIB. Switching to %s.", currentUpstream, bestRoute.OriginNode)
		f.switchUpstream(streamID, bestRoute)
		return
	}

	// Skip optimization if current upstream is in the same Cluster
	// Intra-cluster traffic is usually preferred/free, so don't switch away unless broken.
	if currentRoute.OriginCluster == f.rib.LocalInfo().Cluster {
		// log.Printf("[FIB] Optimize: Current upstream %s is in same cluster (%s). Skipping optimization.", currentUpstream, currentRoute.OriginCluster)
		return
	}

	// Apply Threshold
	// Switch if: NewCost < CurrentCost - Threshold
	if bestRoute.BaseCost < currentRoute.BaseCost-f.OptimizationThreshold {
		log.Printf("[FIB] Optimize: Found better path via %s (Cost: %d) vs %s (Cost: %d). Switching.",
			bestRoute.OriginNode, bestRoute.BaseCost, currentUpstream, currentRoute.BaseCost)
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

	log.Printf("[FIB] Switching upstream for %s: %s -> %s", streamID, oldUpstream, newRoute.OriginNode)

	// 1. Connect New Pipe
	var newPipeID string
	if f.ConnectPipe != nil {
		var err error
		newPipeID, err = f.ConnectPipe(streamID, newRoute.OriginNode)
		if err != nil {
			log.Printf("[FIB] Switch failed: ConnectPipe error: %v", err)
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
			f.DisconnectPipe(newPipeID)
		}
		return
	}

	entry.UpstreamNode = newRoute.OriginNode
	entry.PipeID = newPipeID
	f.mu.Unlock()

	// 3. Update Advertisement (Propagate new path)
	newPathVector := append(newRoute.PathVector, f.localNodeID)
	err := f.rib.StartAdvertise(streamID, newRoute.BaseCost+10, newPathVector, newRoute.RttToOrigin+20)
	if err != nil {
		log.Printf("[FIB] Failed to update advertisement during switch: %v", err)
	}

	// 4. Update Watcher
	// We need to monitor the NEW upstream
	f.monitorUpstream(streamID, newRoute.OriginNode, entry.FindPathFunc)

	// 5. Disconnect Old Pipe
	if f.DisconnectPipe != nil {
		f.DisconnectPipe(oldPipeID)
	}

	log.Printf("[FIB] Switch complete: %s is now via %s", streamID, newRoute.OriginNode)
}
