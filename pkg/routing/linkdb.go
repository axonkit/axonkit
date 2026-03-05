package routing

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/axonkit/axonkit/pkg/types"
)

// LinkDB stores local perspective of link quality to other clusters and regions
type LinkDB struct {
	clusterStats map[types.ClusterID]types.LinkStats
	regionStats  map[types.RegionID]types.LinkStats
	mu           sync.RWMutex

	localNode     types.NodeInfo
	clusterNodes  map[types.ClusterID][]types.NodeID
	nodesMu       sync.RWMutex
	logger        *slog.Logger
	probeInterval time.Duration
	rttProvider   RTTProvider
}

type LinkDBOption func(*LinkDB)

type RTTProvider func(nodeID types.NodeID) (int64, bool)

func WithProbeInterval(d time.Duration) LinkDBOption {
	return func(db *LinkDB) {
		db.probeInterval = d
	}
}

func WithRTTProvider(provider RTTProvider) LinkDBOption {
	return func(db *LinkDB) {
		db.rttProvider = provider
	}
}

func NewLinkDB(localNode types.NodeInfo, opts ...LinkDBOption) *LinkDB {
	db := &LinkDB{
		clusterStats:  make(map[types.ClusterID]types.LinkStats),
		regionStats:   make(map[types.RegionID]types.LinkStats),
		localNode:     localNode,
		clusterNodes:  make(map[types.ClusterID][]types.NodeID),
		logger:        slog.Default().With("component", "LinkDB"),
		probeInterval: 2 * time.Second,
	}

	for _, opt := range opts {
		opt(db)
	}

	return db
}

func (db *LinkDB) SetRTTProvider(provider RTTProvider) {
	db.rttProvider = provider
}

// UpdateClusterLink updates stats for a cluster
func (db *LinkDB) UpdateClusterLink(cluster types.ClusterID, rtt int64, loss float64, cost int) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.clusterStats[cluster] = types.LinkStats{
		AvgRTT:     rtt,
		PacketLoss: loss,
		CostFactor: cost,
	}
}

// UpdateRegionLink updates stats for a region
func (db *LinkDB) UpdateRegionLink(region types.RegionID, rtt int64, loss float64, cost int) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.regionStats[region] = types.LinkStats{
		AvgRTT:     rtt,
		PacketLoss: loss,
		CostFactor: cost,
	}
}

// GetLink returns stats for a cluster, falling back to region if not found.
func (db *LinkDB) GetLink(cluster types.ClusterID, region types.RegionID) types.LinkStats {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 1. Try Cluster specific stats (More specific)
	if s, ok := db.clusterStats[cluster]; ok {
		return s
	}

	// 2. Try Region specific stats (Broader scope)
	if s, ok := db.regionStats[region]; ok {
		return s
	}

	// Default: high cost for unknown destinations
	return types.LinkStats{
		AvgRTT:     1000,
		PacketLoss: 0,
		CostFactor: 100,
	}
}

// AddNode adds a known node to the DB for probing
func (db *LinkDB) AddNode(node types.NodeInfo) {
	if node.ID == db.localNode.ID {
		return
	}
	// Ignore nodes in same cluster (RTT ~0)
	if node.Cluster == db.localNode.Cluster {
		return
	}

	db.nodesMu.Lock()
	defer db.nodesMu.Unlock()

	nodes := db.clusterNodes[node.Cluster]
	// Check duplicates
	for _, id := range nodes {
		if id == node.ID {
			return
		}
	}
	db.clusterNodes[node.Cluster] = append(nodes, node.ID)
}

// RemoveNode removes a node
func (db *LinkDB) RemoveNode(nodeID types.NodeID) {
	db.nodesMu.Lock()
	defer db.nodesMu.Unlock()

	for cluster, nodes := range db.clusterNodes {
		for i, id := range nodes {
			if id == nodeID {
				// Remove
				db.clusterNodes[cluster] = append(nodes[:i], nodes[i+1:]...)
				return
			}
		}
	}
}

// StartProbing starts the background probing loop
func (db *LinkDB) StartProbing(ctx context.Context) {
	go func() {
		// Probe immediately on start
		db.probeAllClusters()

		ticker := time.NewTicker(db.probeInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				db.probeAllClusters()
			}
		}
	}()
}

func (db *LinkDB) probeAllClusters() {
	db.nodesMu.RLock()
	clusters := make([]types.ClusterID, 0, len(db.clusterNodes))
	for c := range db.clusterNodes {
		clusters = append(clusters, c)
	}
	db.nodesMu.RUnlock()

	for _, cluster := range clusters {
		go db.probeCluster(cluster)
	}
}

func (db *LinkDB) probeCluster(cluster types.ClusterID) {
	if db.rttProvider == nil {
		return
	}

	// Pick random node
	db.nodesMu.RLock()
	nodes := db.clusterNodes[cluster]
	if len(nodes) == 0 {
		db.nodesMu.RUnlock()
		return
	}
	target := nodes[rand.Intn(len(nodes))]
	db.nodesMu.RUnlock()

	rtt, ok := db.rttProvider(target)
	if !ok {
		return
	}

	// Update stats
	// Simple moving average
	oldStats := db.GetLink(cluster, "")
	newRTT := rtt
	if oldStats.AvgRTT > 0 && oldStats.AvgRTT < 1000 { // 1000 is default high cost
		newRTT = (oldStats.AvgRTT*7 + rtt*3) / 10
	}

	db.UpdateClusterLink(cluster, newRTT, 0, oldStats.CostFactor)
	db.logger.Debug("Probed cluster", "cluster", cluster, "target", target, "rtt", rtt, "avg_rtt", newRTT)
}
