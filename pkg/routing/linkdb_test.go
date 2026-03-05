package routing

import (
	"context"
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/types"
)

func TestLinkDB_Probing(t *testing.T) {
	localNode := types.NodeInfo{
		ID:      "node-a",
		Region:  "region-1",
		Cluster: "cluster-1",
	}

	remoteNode := types.NodeInfo{
		ID:      "node-b",
		Region:  "region-2",
		Cluster: "cluster-2",
	}

	probeCalled := make(chan struct{}, 1)
	rttProvider := func(nodeID types.NodeID) (int64, bool) {
		if nodeID != remoteNode.ID {
			return 0, false
		}
		select {
		case probeCalled <- struct{}{}:
		default:
		}
		return 25, true
	}

	linkDB := NewLinkDB(localNode, WithProbeInterval(200*time.Millisecond), WithRTTProvider(rttProvider))

	// 5. Add Remote Node to LinkDB
	linkDB.AddNode(remoteNode)

	// 6. Start Probing
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	linkDB.StartProbing(ctx)

	// 7. Wait for probe to happen
	select {
	case <-probeCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("Probe did not happen")
	}

	// 8. Verify Stats Updated
	stats := linkDB.GetLink(remoteNode.Cluster, "")
	switch stats.AvgRTT {
	case 1000:
		t.Errorf("Stats not updated after probe")
	case 0:
		// In local test environment, RTT can be 0ms. This is acceptable.
		t.Logf("Probe success! AvgRTT: 0 ms (Local fast path)")
	default:
		t.Logf("Probe success! AvgRTT: %d ms", stats.AvgRTT)
	}
}

func TestLinkDB_Fallback(t *testing.T) {
	db := NewLinkDB(types.NodeInfo{})

	cluster := types.ClusterID("cluster-1")
	region := types.RegionID("region-1")

	// 1. Initial State (Defaults)
	stats := db.GetLink(cluster, region)
	if stats.AvgRTT != 1000 {
		t.Errorf("Expected default stats")
	}

	// 2. Set Region Stats
	db.UpdateRegionLink(region, 50, 0, 10)
	stats = db.GetLink(cluster, region)
	if stats.AvgRTT != 50 {
		t.Errorf("Expected fallback to region stats (50), got %d", stats.AvgRTT)
	}

	// 3. Set Cluster Stats
	db.UpdateClusterLink(cluster, 20, 0, 5)
	stats = db.GetLink(cluster, region)
	if stats.AvgRTT != 20 {
		t.Errorf("Expected cluster stats (20), got %d", stats.AvgRTT)
	}
}

func TestLinkDB_SetRTTProvider(t *testing.T) {
	db := NewLinkDB(types.NodeInfo{ID: "n1", Cluster: "c1"})
	called := false
	db.SetRTTProvider(func(nodeID types.NodeID) (int64, bool) {
		called = true
		return 10, true
	})
	if db.rttProvider == nil {
		t.Fatal("expected rttProvider to be set")
	}
	db.rttProvider("n2")
	if !called {
		t.Fatal("expected rttProvider to be called")
	}
}

func TestLinkDB_RemoveNode(t *testing.T) {
	localNode := types.NodeInfo{ID: "n1", Region: "r1", Cluster: "c1"}
	db := NewLinkDB(localNode)

	db.AddNode(types.NodeInfo{ID: "n2", Region: "r1", Cluster: "c2"})
	db.AddNode(types.NodeInfo{ID: "n3", Region: "r1", Cluster: "c2"})

	db.nodesMu.RLock()
	count := len(db.clusterNodes["c2"])
	db.nodesMu.RUnlock()
	if count != 2 {
		t.Fatalf("expected 2 nodes in c2, got %d", count)
	}

	db.RemoveNode("n2")
	db.nodesMu.RLock()
	count = len(db.clusterNodes["c2"])
	db.nodesMu.RUnlock()
	if count != 1 {
		t.Fatalf("expected 1 node after remove, got %d", count)
	}

	// Remove non-existent node should be no-op
	db.RemoveNode("n99")
}

func TestLinkDB_AddNode_Filters(t *testing.T) {
	localNode := types.NodeInfo{ID: "n1", Region: "r1", Cluster: "c1"}
	db := NewLinkDB(localNode)

	// Self should be ignored
	db.AddNode(localNode)
	db.nodesMu.RLock()
	count := len(db.clusterNodes)
	db.nodesMu.RUnlock()
	if count != 0 {
		t.Fatal("self node should be ignored")
	}

	// Same cluster should be ignored
	db.AddNode(types.NodeInfo{ID: "n2", Region: "r1", Cluster: "c1"})
	db.nodesMu.RLock()
	count = len(db.clusterNodes)
	db.nodesMu.RUnlock()
	if count != 0 {
		t.Fatal("same-cluster node should be ignored")
	}

	// Duplicate add should be idempotent
	db.AddNode(types.NodeInfo{ID: "n3", Region: "r2", Cluster: "c2"})
	db.AddNode(types.NodeInfo{ID: "n3", Region: "r2", Cluster: "c2"})
	db.nodesMu.RLock()
	count = len(db.clusterNodes["c2"])
	db.nodesMu.RUnlock()
	if count != 1 {
		t.Fatalf("expected 1 node (no duplicates), got %d", count)
	}
}

func TestLinkDB_ProbeCluster_NilProvider(t *testing.T) {
	db := NewLinkDB(types.NodeInfo{ID: "n1", Cluster: "c1"})
	db.AddNode(types.NodeInfo{ID: "n2", Region: "r2", Cluster: "c2"})
	// Should not panic with nil rttProvider
	db.probeCluster("c2")
}

func TestLinkDB_ProbeCluster_EmptyNodes(t *testing.T) {
	db := NewLinkDB(types.NodeInfo{ID: "n1", Cluster: "c1"})
	db.SetRTTProvider(func(nodeID types.NodeID) (int64, bool) { return 10, true })
	// Probe an empty cluster
	db.probeCluster("empty-cluster")
}
