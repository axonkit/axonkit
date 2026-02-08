package discovery

import (
	"rib-control/pkg/routing"
	"rib-control/pkg/types"
	"sort"
)

type Finder struct {
	rib         *routing.RIB
	linkDB      *routing.LinkDB
	localNodeID types.NodeID
}

func NewFinder(rib *routing.RIB, linkDB *routing.LinkDB, localNodeID types.NodeID) *Finder {
	return &Finder{
		rib:         rib,
		linkDB:      linkDB,
		localNodeID: localNodeID,
	}
}

type ScoredCandidate struct {
	Candidate types.RouteKVValue
	Score     float64
}

// FindPath queries RIB and calculates the best path based on LinkDB scores
func (f *Finder) FindPath(streamID types.StreamID) (*types.RouteKVValue, error) {
	candidates, err := f.rib.GetCandidates(streamID)
	if err != nil {
		return nil, err
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	var scored []ScoredCandidate

	for _, c := range candidates {
		// 1. Loop detection
		hasLoop := false
		for _, node := range c.PathVector {
			if node == f.localNodeID {
				hasLoop = true
				break
			}
		}
		if hasLoop {
			continue
		}

		// 2. Calculate Score
		// Formula: TotalScore = Candidate.BaseCost + MyLinkCost(RTT, Factor)
		// Lower score is better.

		linkStats := f.linkDB.GetLink(c.Region)

		// Heuristic:
		// BaseCost: Cost from origin to candidate
		// LinkRTT: Cost from candidate (Region) to me
		// Factor: Administrative cost (e.g. cross-region penalty)

		linkCost := float64(linkStats.AvgRTT) * 1.0 // 1ms = 1 score

		// Add penalty for packet loss
		if linkStats.PacketLoss > 0 {
			linkCost += linkStats.PacketLoss * 100
		}

		// Add administrative cost factor
		linkCost += float64(linkStats.CostFactor)

		totalScore := float64(c.BaseCost) + linkCost

		scored = append(scored, ScoredCandidate{
			Candidate: c,
			Score:     totalScore,
		})
	}

	if len(scored) == 0 {
		return nil, nil
	}

	// Sort by score (ascending)
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score < scored[j].Score
	})

	return &scored[0].Candidate, nil
}
