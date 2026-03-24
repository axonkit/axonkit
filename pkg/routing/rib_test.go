package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/store"
	"github.com/axonkit/axonkit/pkg/types"
)

func TestWatchDownstream_SelfExclusion(t *testing.T) {
	ms := store.NewMemoryStore(0)
	localNode := types.NodeInfo{ID: "NodeA", Region: "R1", Cluster: "C1"}
	r := NewRIB(ms, localNode)

	streamID := types.StreamID("stream1")
	downstreamCh := make(chan types.NodeID, 10)

	// Register Watcher
	r.WatchDownstream(streamID, func(nodeID types.NodeID, isAdd bool) {
		if isAdd {
			downstreamCh <- nodeID
		}
	})

	// Wait for watcher to start (goroutine scheduling)
	time.Sleep(10 * time.Millisecond)

	// Helper to put route
	putRoute := func(node types.NodeID, path []types.NodeID) {
		val := types.RouteKVValue{
			StreamID:   streamID,
			Node:       node,
			PathVector: path,
		}
		bytes, _ := json.Marshal(val)
		key := fmt.Sprintf("route.%s.R1.C1.%s", streamID, node)
		_ = ms.Put(context.Background(), key, bytes)
	}

	// 1. Self Advertise (Loopback) -> Should be IGNORED
	// This was the bug: NodeA advertised [NodeA], and NodeA counted itself as downstream.
	putRoute("NodeA", []types.NodeID{"NodeA"})
	select {
	case node := <-downstreamCh:
		t.Errorf("BUG: Received downstream notification for self: %s", node)
	case <-time.After(50 * time.Millisecond):
		// Passed
	}

	// 2. Direct Downstream (NodeB) -> Should be NOTIFIED
	// NodeB connects to NodeA. Path: [NodeA, NodeB]
	putRoute("NodeB", []types.NodeID{"NodeA", "NodeB"})
	select {
	case node := <-downstreamCh:
		if node != "NodeB" {
			t.Errorf("Expected NodeB, got %s", node)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for NodeB notification")
	}

	// 3. Indirect Downstream (NodeC) -> Should be IGNORED (by NodeA)
	// NodeC connects to NodeB. Path: [NodeA, NodeB, NodeC]
	// NodeA should only care about NodeB. NodeC is downstream of NodeB.
	putRoute("NodeC", []types.NodeID{"NodeA", "NodeB", "NodeC"})
	select {
	case node := <-downstreamCh:
		t.Errorf("Unexpected downstream notification for indirect node: %s", node)
	case <-time.After(50 * time.Millisecond):
		// Passed
	}

	// 4. Other Upstream / Independent (NodeX) -> Should be IGNORED
	putRoute("NodeX", []types.NodeID{"NodeX"})
	select {
	case node := <-downstreamCh:
		t.Errorf("Unexpected downstream notification for NodeX: %s", node)
	case <-time.After(50 * time.Millisecond):
		// Passed
	}
}

func TestRIB_LocalInfo(t *testing.T) {
	ms := store.NewMemoryStore(0)
	nodeInfo := types.NodeInfo{ID: "nodeX", Region: "us-east", Cluster: "c1"}
	r := NewRIB(ms, nodeInfo)
	defer r.Close()

	if r.LocalInfo().ID != "nodeX" {
		t.Fatalf("expected nodeX, got %s", r.LocalInfo().ID)
	}
}

func TestRIB_GetCandidates(t *testing.T) {
	ms := store.NewMemoryStore(0)
	nodeInfo := types.NodeInfo{ID: "nodeA", Region: "us-east", Cluster: "c1"}
	r := NewRIB(ms, nodeInfo)
	defer r.Close()

	streamID := types.StreamID("test-stream")

	candidates, err := r.GetCandidates(streamID)
	if err != nil {
		t.Fatalf("GetCandidates error: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatal("expected 0 candidates")
	}

	r.StartAdvertise(AdvertiseOptions{
		StreamID: streamID, BaseCost: 5, IsOrigin: true,
	})
	time.Sleep(200 * time.Millisecond)

	candidates, err = r.GetCandidates(streamID)
	if err != nil {
		t.Fatalf("GetCandidates error: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	if candidates[0].Node != "nodeA" {
		t.Fatalf("expected nodeA, got %s", candidates[0].Node)
	}
}

func TestRIB_StartStopAdvertise(t *testing.T) {
	ms := store.NewMemoryStore(0)
	nodeInfo := types.NodeInfo{ID: "nodeA", Region: "us-east", Cluster: "c1"}
	r := NewRIB(ms, nodeInfo)
	defer r.Close()

	streamID := types.StreamID("test-adv")

	if err := r.StartAdvertise(AdvertiseOptions{
		StreamID: streamID, BaseCost: 0, IsOrigin: true,
	}); err != nil {
		t.Fatalf("StartAdvertise: %v", err)
	}

	// Duplicate start should be no-op
	if err := r.StartAdvertise(AdvertiseOptions{
		StreamID: streamID, BaseCost: 0, IsOrigin: true,
	}); err != nil {
		t.Fatalf("Duplicate StartAdvertise: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if err := r.StopAdvertise(streamID); err != nil {
		t.Fatalf("StopAdvertise: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	candidates, _ := r.GetCandidates(streamID)
	if len(candidates) != 0 {
		t.Fatalf("expected 0 candidates after stop, got %d", len(candidates))
	}
}

func TestRIB_SubscribeUpdates(t *testing.T) {
	ms := store.NewMemoryStore(0)
	nodeInfo := types.NodeInfo{ID: "nodeA", Region: "us-east", Cluster: "c1"}
	r := NewRIB(ms, nodeInfo)
	defer r.Close()

	streamID := types.StreamID("test-updates")
	updates := make(chan bool, 10)

	r.SubscribeUpdates(streamID, func(val types.RouteKVValue, isDelete bool) {
		updates <- isDelete
	})
	time.Sleep(100 * time.Millisecond)

	// Directly put a route entry (simulating another node advertising)
	val := types.RouteKVValue{
		StreamID:   streamID,
		Node:       "nodeB",
		PathVector: []types.NodeID{"nodeB"},
		BaseCost:   0,
		IsOrigin:   true,
	}
	data, _ := json.Marshal(val)
	key := fmt.Sprintf("route.%s.us-east.c1.nodeB", streamID)
	_ = ms.Put(context.Background(), key, data)

	select {
	case isDelete := <-updates:
		if isDelete {
			t.Fatal("expected put notification, got delete")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for update notification")
	}
}
