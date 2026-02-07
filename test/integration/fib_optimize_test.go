package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"rib-control/pkg/routing"
	"rib-control/pkg/types"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestFIBOptimization(t *testing.T) {
	// Setup NATS
	srv := setupNATS(t)
	defer srv.Shutdown()

	// Setup Local Node (Cluster C1)
	localInfo := types.NodeInfo{ID: "LocalNode", Region: "R1", Cluster: "C1"}

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to init JetStream: %v", err)
	}

	rib, err := routing.NewRIB(js, localInfo)
	if err != nil {
		t.Fatalf("NewRIB failed: %v", err)
	}
	defer rib.Close()

	fib := routing.NewFIB(rib, localInfo.ID)
	// Enable Optimization with fast interval
	fib.EnableRouteOptimization = true
	fib.OptimizationInterval = 300 * time.Millisecond
	fib.OptimizationThreshold = 10

	// Mock Pipe Logic to track switches
	pipeHistory := make(chan string, 10)
	fib.ConnectPipe = func(streamID types.StreamID, upstream types.NodeID) (string, error) {
		msg := fmt.Sprintf("Connect->%s", upstream)
		pipeHistory <- msg
		return fmt.Sprintf("pipe-%s", upstream), nil
	}
	fib.DisconnectPipe = func(pipeID string) error {
		return nil
	}

	// Helper to inject routes
	kv, _ := js.KeyValue(context.Background(), "RIB")
	putRoute := func(streamID types.StreamID, nodeID types.NodeID, cluster types.ClusterID, cost int) {
		key := fmt.Sprintf("route.%s.R1.%s.%s", streamID, cluster, nodeID)
		val := types.RouteKVValue{
			StreamID:      streamID,
			OriginNode:    nodeID,
			OriginRegion:  "R1",
			OriginCluster: cluster,
			BaseCost:      cost,
			PathVector:    []types.NodeID{nodeID},
			ExpireAt:      time.Now().Add(10 * time.Second),
		}
		data, _ := json.Marshal(val)
		kv.Put(context.Background(), key, data)
	}

	// Dynamic FindPath (always picks best from RIB)
	findPath := func(sid types.StreamID) (*types.RouteKVValue, error) {
		candidates, _ := rib.GetCandidates(sid)
		var best *types.RouteKVValue
		for _, c := range candidates {
			// Copy variable
			candidate := c
			if best == nil || candidate.BaseCost < best.BaseCost {
				best = &candidate
			}
		}
		return best, nil
	}

	// Case 1: Sticky Same Cluster
	t.Run("StickySameCluster", func(t *testing.T) {
		sID := types.StreamID("s1")
		// Initial: NodeSame (C1, 100)
		putRoute(sID, "NodeSame", "C1", 100)
		time.Sleep(50 * time.Millisecond) // Propagate

		// Subscribe
		fib.Subscribe(sID, findPath)

		// Expect Connect->NodeSame
		expectMsg(t, pipeHistory, "Connect->NodeSame")

		// Inject Better: NodeBetter (C2, 50)
		putRoute(sID, "NodeBetter", "C2", 50)

		// Wait for Optimization
		time.Sleep(600 * time.Millisecond)

		// Expect NO Switch (because NodeSame is in C1, same as Local)
		expectNoMsg(t, pipeHistory)

		fib.Unsubscribe(sID)
	})

	// Case 2: Switch to Better (Diff Cluster)
	t.Run("SwitchToBetterDiffCluster", func(t *testing.T) {
		sID := types.StreamID("s2")
		// Initial: NodeRemote1 (C2, 100)
		putRoute(sID, "NodeRemote1", "C2", 100)
		time.Sleep(50 * time.Millisecond)

		fib.Subscribe(sID, findPath)
		expectMsg(t, pipeHistory, "Connect->NodeRemote1")

		// Inject Better: NodeRemote2 (C2, 50) -> Cost Diff = 50 > Threshold(10)
		putRoute(sID, "NodeRemote2", "C2", 50)

		// Wait for Optimization
		time.Sleep(600 * time.Millisecond)

		// Expect Switch
		expectMsg(t, pipeHistory, "Connect->NodeRemote2")

		fib.Unsubscribe(sID)
	})

	// Case 3: Threshold Hysteresis
	t.Run("ThresholdHysteresis", func(t *testing.T) {
		sID := types.StreamID("s3")
		// Initial: NodeRemoteA (C2, 100)
		putRoute(sID, "NodeRemoteA", "C2", 100)
		time.Sleep(50 * time.Millisecond)

		fib.Subscribe(sID, findPath)
		expectMsg(t, pipeHistory, "Connect->NodeRemoteA")

		// Inject Slightly Better: NodeRemoteB (C2, 95) -> Diff = 5 < Threshold(10)
		putRoute(sID, "NodeRemoteB", "C2", 95)

		// Wait for Optimization
		time.Sleep(600 * time.Millisecond)

		// Expect NO Switch
		expectNoMsg(t, pipeHistory)

		fib.Unsubscribe(sID)
	})
}

func expectMsg(t *testing.T, ch chan string, expected string) {
	select {
	case msg := <-ch:
		if msg != expected {
			t.Fatalf("Expected %s, got %s", expected, msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timeout waiting for %s", expected)
	}
}

func expectNoMsg(t *testing.T, ch chan string) {
	select {
	case msg := <-ch:
		t.Fatalf("Unexpected message: %s", msg)
	case <-time.After(100 * time.Millisecond):
		// OK
	}
}
