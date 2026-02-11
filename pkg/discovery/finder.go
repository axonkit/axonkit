package discovery

import (
	"github.com/jiyeyuran/omnistream/pkg/routing"
	"github.com/jiyeyuran/omnistream/pkg/types"
	"sort"
)

type Finder struct {
	rib       *routing.RIB
	linkDB    *routing.LinkDB
	localNode types.NodeInfo
}

func NewFinder(rib *routing.RIB, linkDB *routing.LinkDB, localNode types.NodeInfo) *Finder {
	return &Finder{
		rib:       rib,
		linkDB:    linkDB,
		localNode: localNode,
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
			if node == f.localNode.ID {
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

		// Try to get link stats for the specific cluster, fallback to region
		linkStats := f.linkDB.GetLink(c.Cluster, c.Region)

		// Heuristic:
		// BaseCost: Cost from origin to candidate
		// LinkRTT: Cost from candidate (Cluster) to me
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
		a := scored[i]
		b := scored[j]

		// 1. Same Cluster Priority
		aInCluster := a.Candidate.Cluster == f.localNode.Cluster
		bInCluster := b.Candidate.Cluster == f.localNode.Cluster
		if aInCluster != bInCluster {
			return aInCluster
		}

		// 2. Same Region Priority
		aInRegion := a.Candidate.Region == f.localNode.Region
		bInRegion := b.Candidate.Region == f.localNode.Region
		if aInRegion != bInRegion {
			return aInRegion
		}

		// 3. Score (Lower is better)
		return a.Score < b.Score
	})

	return &scored[0].Candidate, nil
}
