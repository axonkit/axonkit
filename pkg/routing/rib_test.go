package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jiyeyuran/omnistream/pkg/store"
	"github.com/jiyeyuran/omnistream/pkg/types"
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
		ms.Put(context.Background(), key, bytes)
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
