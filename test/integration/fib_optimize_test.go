package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/jiyeyuran/omnistream/pkg/routing"
	"github.com/jiyeyuran/omnistream/pkg/store"
	"github.com/jiyeyuran/omnistream/pkg/types"

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

	kvStore, err := store.NewNatsStore(context.Background(), js, routing.RibBucketName, routing.RIBTTL)
	if err != nil {
		t.Fatalf("Failed to create NATS store: %v", err)
	}

	rib := routing.NewRIB(kvStore, localInfo)
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
	fib.DisconnectPipe = func(streamID types.StreamID, pipeID string, upstreamNode types.NodeID) error {
		return nil
	}

	// Helper to inject routes
	kv, _ := js.KeyValue(context.Background(), "RIB")
	putRoute := func(streamID types.StreamID, nodeID types.NodeID, cluster types.ClusterID, cost int) {
		key := fmt.Sprintf("route.%s.R1.%s.%s", streamID, cluster, nodeID)
		val := types.RouteKVValue{
			StreamID:   streamID,
			Node:       nodeID,
			Region:     "R1",
			Cluster:    cluster,
			BaseCost:   cost,
			PathVector: []types.NodeID{nodeID},
			ExpireAt:   time.Now().Add(10 * time.Second),
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

func TestConcurrentOptimization(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	srv := setupNATS(t)
	defer srv.Shutdown()

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

	kvStore, err := store.NewNatsStore(context.Background(), js, routing.RibBucketName, routing.RIBTTL)
	if err != nil {
		t.Fatalf("Failed to create NATS store: %v", err)
	}

	rib := routing.NewRIB(kvStore, localInfo)
	defer rib.Close()

	fib := routing.NewFIB(rib, localInfo.ID)
	fib.EnableRouteOptimization = true
	// Fast optimization interval to trigger race conditions
	fib.OptimizationInterval = 100 * time.Millisecond
	fib.OptimizationThreshold = 5

	// Track current upstreams for verification
	currentUpstreams := sync.Map{} // streamID -> upstreamNodeID

	fib.ConnectPipe = func(streamID types.StreamID, upstream types.NodeID) (string, error) {
		// Simulate some work
		// time.Sleep(1 * time.Millisecond) // Reduce sleep to handle 500 streams faster
		currentUpstreams.Store(streamID, upstream)
		return fmt.Sprintf("pipe-%s", upstream), nil
	}
	fib.DisconnectPipe = func(streamID types.StreamID, pipeID string, upstreamNode types.NodeID) error {
		return nil
	}

	kv, _ := js.KeyValue(context.Background(), "RIB")

	putRoute := func(streamID types.StreamID, nodeID types.NodeID, cost int) {
		// Always use C2 to allow optimization switching
		key := fmt.Sprintf("route.%s.R1.C2.%s", streamID, nodeID)
		val := types.RouteKVValue{
			StreamID:   streamID,
			Node:       nodeID,
			Region:     "R1",
			Cluster:    "C2", // Diff cluster
			BaseCost:   cost,
			PathVector: []types.NodeID{nodeID},
			ExpireAt:   time.Now().Add(10 * time.Second),
		}
		data, _ := json.Marshal(val)
		kv.Put(context.Background(), key, data)
	}

	findPath := func(sid types.StreamID) (*types.RouteKVValue, error) {
		candidates, _ := rib.GetCandidates(sid)
		var best *types.RouteKVValue
		for _, c := range candidates {
			if c.Node == "LocalNode" {
				continue
			}
			candidate := c
			if best == nil || candidate.BaseCost < best.BaseCost {
				best = &candidate
			}
		}
		return best, nil
	}

	const NumStreams = 50
	var wg sync.WaitGroup
	doneCh := make(chan struct{})

	// Background: Randomly update route costs to trigger optimization
	stopChaosCh := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stopChaosCh:
				return
			case <-ticker.C:
				// Pick a random stream and update costs
				sIdx := rand.Intn(NumStreams)
				sID := types.StreamID(fmt.Sprintf("conc-s-%d", sIdx))

				// Toggle between NodeA and NodeB with random costs
				costA := rand.Intn(100)
				costB := rand.Intn(100)
				putRoute(sID, "NodeA", costA)
				putRoute(sID, "NodeB", costB)
			}
		}
	}()

	// Workers: Subscribe and wait
	for i := 0; i < NumStreams; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sID := types.StreamID(fmt.Sprintf("conc-s-%d", idx))

			// Initial routes (NodeA=100, NodeB=100)
			putRoute(sID, "NodeA", 100)
			putRoute(sID, "NodeB", 100)

			// Wait for propagation (batch wait?)
			// No, individual wait is safer but slower.
			// Actually, just let them race. findPath will retry or fail if not ready?
			// findPath might return nil if propagation hasn't happened.
			// Let's loop until subscribed.

			for {
				_, _, err := fib.Subscribe(sID, findPath)
				if err == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}

			// Wait until test is done
			<-doneCh

			// Unsubscribe
			fib.Unsubscribe(sID)
		}(i)
	}

	// 1. Run Chaos Phase
	fmt.Println("Running Chaos Phase...")
	time.Sleep(3 * time.Second)

	// 2. Stop Chaos
	fmt.Println("Stopping Chaos...")
	close(stopChaosCh)

	// 3. Stabilization Phase: Make NodeA the BEST for EVERYONE
	fmt.Println("Stabilizing: Setting NodeA as best path (Cost 10 vs 100)...")
	for i := 0; i < NumStreams; i++ {
		sID := types.StreamID(fmt.Sprintf("conc-s-%d", i))
		putRoute(sID, "NodeA", 10)
		putRoute(sID, "NodeB", 100)
	}

	// 4. Wait for Convergence
	// Optimization Interval is 100ms. Wait 2s to be safe.
	fmt.Println("Waiting for convergence...")
	time.Sleep(2 * time.Second)

	// 5. Verify
	fmt.Println("Verifying convergence...")
	verificationFailed := false
	for i := 0; i < NumStreams; i++ {
		sID := types.StreamID(fmt.Sprintf("conc-s-%d", i))
		val, ok := currentUpstreams.Load(sID)
		if !ok {
			t.Errorf("Stream %s never connected!", sID)
			verificationFailed = true
			continue
		}
		upstream := val.(types.NodeID)
		if upstream != "NodeA" {
			t.Errorf("Stream %s failed to converge to NodeA. Current: %s", sID, upstream)
			verificationFailed = true
		}
	}

	if !verificationFailed {
		fmt.Println("SUCCESS: All streams converged to NodeA.")
	}

	// 6. Cleanup
	close(doneCh)
	wg.Wait()
}
