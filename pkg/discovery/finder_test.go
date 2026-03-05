package discovery

import (
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/routing"
	"github.com/axonkit/axonkit/pkg/store"
	"github.com/axonkit/axonkit/pkg/types"
)

func TestFinder_FindPath_NoCandidates(t *testing.T) {
	ms := store.NewMemoryStore(0)
	nodeInfo := types.NodeInfo{ID: "node-1", Region: "us-east", Cluster: "c1"}
	rib := routing.NewRIB(ms, nodeInfo)
	defer rib.Close()
	linkDB := routing.NewLinkDB(nodeInfo)

	f := NewFinder(rib, linkDB, nodeInfo)
	route, err := f.FindPath("nonexistent-stream")
	if err != nil {
		t.Fatalf("FindPath error: %v", err)
	}
	if route != nil {
		t.Fatal("expected nil route for nonexistent stream")
	}
}

func TestFinder_FindPath_BestRoute(t *testing.T) {
	ms := store.NewMemoryStore(0)
	nodeA := types.NodeInfo{ID: "nodeA", Region: "eu-west", Cluster: "c2"}
	nodeB := types.NodeInfo{ID: "nodeB", Region: "us-east", Cluster: "c1"}
	localNode := types.NodeInfo{ID: "local", Region: "us-east", Cluster: "c1"}

	ribA := routing.NewRIB(ms, nodeA)
	defer ribA.Close()
	ribB := routing.NewRIB(ms, nodeB)
	defer ribB.Close()
	ribLocal := routing.NewRIB(ms, localNode)
	defer ribLocal.Close()

	linkDB := routing.NewLinkDB(localNode)
	streamID := types.StreamID("test-stream")

	// nodeA advertises with high cost, different cluster
	ribA.StartAdvertise(routing.AdvertiseOptions{
		StreamID: streamID, BaseCost: 100, IsOrigin: true,
	})
	// nodeB advertises with low cost, same cluster
	ribB.StartAdvertise(routing.AdvertiseOptions{
		StreamID: streamID, BaseCost: 10, IsOrigin: true,
	})
	time.Sleep(200 * time.Millisecond)

	f := NewFinder(ribLocal, linkDB, localNode)
	route, err := f.FindPath(streamID)
	if err != nil {
		t.Fatalf("FindPath error: %v", err)
	}
	if route == nil {
		t.Fatal("expected a route")
	}
	// nodeB should be preferred (same cluster, lower cost)
	if route.Node != "nodeB" {
		t.Fatalf("expected nodeB, got %s", route.Node)
	}
}

func TestFinder_FindPath_LoopDetection(t *testing.T) {
	ms := store.NewMemoryStore(0)
	origin := types.NodeInfo{ID: "origin", Region: "us-east", Cluster: "c1"}
	localNode := types.NodeInfo{ID: "local", Region: "us-east", Cluster: "c1"}

	ribOrigin := routing.NewRIB(ms, origin)
	defer ribOrigin.Close()
	ribLocal := routing.NewRIB(ms, localNode)
	defer ribLocal.Close()

	linkDB := routing.NewLinkDB(localNode)
	streamID := types.StreamID("test-stream")

	ribOrigin.StartAdvertise(routing.AdvertiseOptions{
		StreamID:   streamID,
		BaseCost:   0,
		IsOrigin:   true,
		PathVector: []types.NodeID{"origin", "local"}, // local in path = loop
	})
	time.Sleep(200 * time.Millisecond)

	f := NewFinder(ribLocal, linkDB, localNode)
	route, err := f.FindPath(streamID)
	if err != nil {
		t.Fatalf("FindPath error: %v", err)
	}
	if route != nil {
		t.Fatalf("expected nil route due to loop, got %+v", route)
	}
}
