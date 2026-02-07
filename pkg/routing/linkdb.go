package routing

import (
	"rib-control/pkg/types"
	"sync"
)

// LinkDB stores local perspective of link quality to other regions
type LinkDB struct {
	stats map[types.RegionID]types.LinkStats
	mu    sync.RWMutex
}

func NewLinkDB() *LinkDB {
	return &LinkDB{
		stats: make(map[types.RegionID]types.LinkStats),
	}
}

// UpdateLink updates stats for a region
func (db *LinkDB) UpdateLink(region types.RegionID, rtt int64, loss float64, cost int) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.stats[region] = types.LinkStats{
		AvgRTT:     rtt,
		PacketLoss: loss,
		CostFactor: cost,
	}
}

// GetLink returns stats for a region. Returns default if not found.
func (db *LinkDB) GetLink(region types.RegionID) types.LinkStats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if s, ok := db.stats[region]; ok {
		return s
	}
	// Default: high cost for unknown regions
	return types.LinkStats{
		AvgRTT:     1000,
		PacketLoss: 0,
		CostFactor: 100,
	}
}
