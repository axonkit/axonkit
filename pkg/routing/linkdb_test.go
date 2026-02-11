package routing

import (
	"context"
	"github.com/jiyeyuran/omnistream/pkg/types"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func setupNATS(t *testing.T) *server.Server {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	srv := natsserver.RunServer(&opts)
	return srv
}

func TestLinkDB_Probing(t *testing.T) {
	// 1. Setup NATS Server
	srv := setupNATS(t)
	defer srv.Shutdown()

	// 2. Setup Client A (Local Node)
	ncA, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer ncA.Close()

	localNode := types.NodeInfo{
		ID:      "node-a",
		Region:  "region-1",
		Cluster: "cluster-1",
	}

	linkDB := NewLinkDB(ncA, localNode, WithProbeInterval(500*time.Millisecond))

	// 3. Setup Client B (Remote Node in different cluster)
	ncB, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer ncB.Close()

	remoteNode := types.NodeInfo{
		ID:      "node-b",
		Region:  "region-2",
		Cluster: "cluster-2",
	}

	// Create LinkDB for Remote Node (to auto-start Ping Handler)
	linkDB_Remote := NewLinkDB(ncB, remoteNode)
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	linkDB_Remote.StartProbing(ctxB)

	// 5. Add Remote Node to LinkDB
	linkDB.AddNode(remoteNode)

	// 6. Start Probing
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	linkDB.StartProbing(ctx)

	// 7. Wait for probe to happen (ticker 500ms)
	time.Sleep(1 * time.Second)

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
	db := NewLinkDB(nil, types.NodeInfo{})

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
