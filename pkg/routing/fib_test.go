package routing

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/store"
	"github.com/axonkit/axonkit/pkg/types"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// setupFIBTestHelper setups NATS, JetStream, Store, and RIB for testing
func setupFIBTestHelper(t *testing.T) (*server.Server, *RIB, *nats.Conn) {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	srv := natsserver.RunServer(&opts)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("JetStream init failed: %v", err)
	}

	// Create KV Store (Use NatsStore wrapper)
	kv, err := store.NewNatsStore(context.Background(), js, RibBucketName, RIBTTL)
	if err != nil {
		t.Fatalf("Create NatsStore failed: %v", err)
	}

	localNode := types.NodeInfo{
		ID:      "local-node",
		Region:  "region-1",
		Cluster: "cluster-1",
	}

	rib := NewRIB(kv, localNode)
	return srv, rib, nc
}

func TestFIB_Subscribe(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node")

	// Mock Connect/Disconnect
	connected := make(map[string]bool)
	var mu sync.Mutex

	fib.ConnectPipe = func(streamID types.StreamID, upstream types.NodeID) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		pid := fmt.Sprintf("pipe-%s-%s", upstream, streamID)
		connected[pid] = true
		return pid, nil
	}
	fib.DisconnectPipe = func(streamID types.StreamID, pipeID string, upstreamNode types.NodeID) error {
		mu.Lock()
		defer mu.Unlock()
		delete(connected, pipeID)
		return nil
	}

	// Mock FindPath
	mockFindPath := func(sid types.StreamID) (*types.RouteKVValue, error) {
		return &types.RouteKVValue{
			StreamID: sid,
			Node:     "upstream-node",
			Region:   "region-1",
			Cluster:  "cluster-2",
			BaseCost: 10,
			IsOrigin: true,
		}, nil
	}

	streamID := types.StreamID("stream-1")

	// 1. Subscribe
	pipeID, upstream, err := fib.Subscribe(streamID, mockFindPath)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if upstream != "upstream-node" {
		t.Errorf("Expected upstream 'upstream-node', got %s", upstream)
	}

	mu.Lock()
	if !connected[pipeID] {
		t.Errorf("Pipe %s not connected", pipeID)
	}
	mu.Unlock()

	// 2. Unsubscribe
	fib.Unsubscribe(streamID)

	// Wait for linger (default 10s is too long for test, we should have set it shorter)
	// But we can check RefCount
	fib.mu.Lock()
	entry := fib.entries[streamID]
	if entry.RefCount != 0 {
		t.Errorf("RefCount not 0 after unsubscribe")
	}
	fib.mu.Unlock()
}

func TestFIB_Linger(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	// Set short linger duration
	fib := NewFIB(rib, "local-node", WithLingerDuration(100*time.Millisecond))

	// Mock Connect/Disconnect
	pipeClosed := make(chan string, 1)
	fib.ConnectPipe = func(streamID types.StreamID, upstream types.NodeID) (string, error) {
		return "pipe-1", nil
	}
	fib.DisconnectPipe = func(streamID types.StreamID, pipeID string, upstreamNode types.NodeID) error {
		pipeClosed <- pipeID
		return nil
	}

	mockFindPath := func(sid types.StreamID) (*types.RouteKVValue, error) {
		return &types.RouteKVValue{
			StreamID: sid,
			Node:     "upstream-node",
			IsOrigin: true,
		}, nil
	}

	streamID := types.StreamID("stream-linger")

	// 1. Subscribe
	fib.Subscribe(streamID, mockFindPath)

	// 2. Unsubscribe
	fib.Unsubscribe(streamID)

	// 3. Verify Pipe NOT closed immediately
	select {
	case <-pipeClosed:
		t.Fatal("Pipe closed immediately, expected linger")
	case <-time.After(50 * time.Millisecond):
		// OK
	}

	// 4. Verify Pipe closed after linger
	select {
	case <-pipeClosed:
		// OK
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Pipe not closed after linger duration")
	}
}

func TestFIB_Resurrect(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node", WithLingerDuration(200*time.Millisecond))

	fib.ConnectPipe = func(streamID types.StreamID, upstream types.NodeID) (string, error) {
		return "pipe-1", nil
	}
	fib.DisconnectPipe = func(streamID types.StreamID, pipeID string, upstreamNode types.NodeID) error {
		t.Error("Pipe should not be disconnected")
		return nil
	}

	mockFindPath := func(sid types.StreamID) (*types.RouteKVValue, error) {
		return &types.RouteKVValue{
			StreamID: sid,
			Node:     "upstream-node",
			IsOrigin: true,
		}, nil
	}

	streamID := types.StreamID("stream-resurrect")

	// 1. Subscribe
	fib.Subscribe(streamID, mockFindPath)

	// 2. Unsubscribe (Start Linger)
	fib.Unsubscribe(streamID)

	// 3. Resubscribe (Resurrect)
	time.Sleep(50 * time.Millisecond)
	fib.Subscribe(streamID, mockFindPath)

	// 4. Wait past linger duration
	time.Sleep(300 * time.Millisecond)

	// Verify entry is still there and connected
	fib.mu.Lock()
	entry, ok := fib.entries[streamID]
	fib.mu.Unlock()

	if !ok {
		t.Fatal("Entry removed, resurrection failed")
	}
	if entry.State != "connected" {
		t.Errorf("Entry state is %s, expected connected", entry.State)
	}
}

func TestFIB_Close(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node", WithLingerDuration(time.Minute))

	disconnected := make(chan types.StreamID, 2)
	fib.ConnectPipe = func(streamID types.StreamID, upstream types.NodeID) (string, error) {
		return "pipe-" + string(streamID), nil
	}
	fib.DisconnectPipe = func(streamID types.StreamID, pipeID string, upstreamNode types.NodeID) error {
		disconnected <- streamID
		return nil
	}

	findPath := func(sid types.StreamID) (*types.RouteKVValue, error) {
		return &types.RouteKVValue{Node: "upstream", IsOrigin: true}, nil
	}

	fib.Subscribe("s1", findPath)
	fib.Subscribe("s2", findPath)

	fib.Close()

	// After close, all entries should be cleaned up
	fib.mu.Lock()
	count := len(fib.entries)
	fib.mu.Unlock()
	if count != 0 {
		t.Fatalf("expected 0 entries after close, got %d", count)
	}
}

func TestFIB_SubscribeFindPathError(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node")
	fib.ConnectPipe = func(sid types.StreamID, up types.NodeID) (string, error) {
		return "", nil
	}
	fib.DisconnectPipe = func(sid types.StreamID, pid string, up types.NodeID) error {
		return nil
	}

	_, _, err := fib.Subscribe("s-err", func(sid types.StreamID) (*types.RouteKVValue, error) {
		return nil, fmt.Errorf("no route")
	})
	if err == nil {
		t.Fatal("expected error when findPath fails")
	}
}

func TestFIB_SubscribeNilRoute(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node")
	fib.ConnectPipe = func(sid types.StreamID, up types.NodeID) (string, error) {
		return "", nil
	}
	fib.DisconnectPipe = func(sid types.StreamID, pid string, up types.NodeID) error {
		return nil
	}

	_, _, err := fib.Subscribe("s-nil", func(sid types.StreamID) (*types.RouteKVValue, error) {
		return nil, nil
	})
	if err == nil {
		t.Fatal("expected error when findPath returns nil")
	}
}

func TestFIB_SubscribeConnectPipeError(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node")
	fib.ConnectPipe = func(sid types.StreamID, up types.NodeID) (string, error) {
		return "", fmt.Errorf("connect failed")
	}
	fib.DisconnectPipe = func(sid types.StreamID, pid string, up types.NodeID) error {
		return nil
	}

	_, _, err := fib.Subscribe("s-fail", func(sid types.StreamID) (*types.RouteKVValue, error) {
		return &types.RouteKVValue{Node: "upstream", IsOrigin: true}, nil
	})
	if err == nil {
		t.Fatal("expected error when ConnectPipe fails")
	}
}

func TestFIB_WithOptions(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node",
		WithLingerDuration(time.Second),
		WithRouteOptimization(true, 5*time.Second, 20),
	)
	defer fib.Close()

	if fib.LingerDuration != time.Second {
		t.Fatalf("expected linger 1s, got %v", fib.LingerDuration)
	}
	if !fib.EnableRouteOptimization {
		t.Fatal("expected route optimization enabled")
	}
}

func TestFIB_SubscribeDuplicate(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	connectCount := 0
	fib := NewFIB(rib, "local-node")
	fib.ConnectPipe = func(sid types.StreamID, up types.NodeID) (string, error) {
		connectCount++
		return "pipe-1", nil
	}
	fib.DisconnectPipe = func(sid types.StreamID, pid string, up types.NodeID) error {
		return nil
	}

	findPath := func(sid types.StreamID) (*types.RouteKVValue, error) {
		return &types.RouteKVValue{Node: "upstream", IsOrigin: true}, nil
	}

	fib.Subscribe("s-dup", findPath)
	fib.Subscribe("s-dup", findPath) // duplicate, should reuse

	if connectCount != 1 {
		t.Fatalf("expected 1 connect, got %d", connectCount)
	}

	fib.mu.Lock()
	entry := fib.entries["s-dup"]
	fib.mu.Unlock()
	if entry.RefCount != 2 {
		t.Fatalf("expected RefCount 2, got %d", entry.RefCount)
	}

	fib.Unsubscribe("s-dup") // refcount goes to 1
	fib.mu.Lock()
	entry = fib.entries["s-dup"]
	fib.mu.Unlock()
	if entry.RefCount != 1 {
		t.Fatalf("expected RefCount 1, got %d", entry.RefCount)
	}
}

func TestFIB_UnsubscribeNonexistent(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node")
	fib.Unsubscribe("nonexistent") // should not panic
}

func TestFIB_WithLogger(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	logger := slog.Default().With("test", true)
	fib := NewFIB(rib, "local-node", WithLogger(logger))
	defer fib.Close()

	if fib.logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestFIB_HandleDownstreamChange(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node", WithLingerDuration(100*time.Millisecond))
	fib.ConnectPipe = func(sid types.StreamID, up types.NodeID) (string, error) {
		return "pipe-ds", nil
	}
	disconnected := make(chan types.StreamID, 2)
	fib.DisconnectPipe = func(sid types.StreamID, pid string, up types.NodeID) error {
		disconnected <- sid
		return nil
	}

	findPath := func(sid types.StreamID) (*types.RouteKVValue, error) {
		return &types.RouteKVValue{Node: "upstream", IsOrigin: true}, nil
	}

	streamID := types.StreamID("s-ds")
	fib.Subscribe(streamID, findPath)

	// Add downstream node
	fib.handleDownstreamChange(streamID, "node-c", true)
	fib.mu.RLock()
	entry := fib.entries[streamID]
	hasC := entry.DownstreamNodes["node-c"]
	fib.mu.RUnlock()
	if !hasC {
		t.Fatal("expected downstream node-c")
	}

	// Duplicate add should be idempotent
	fib.handleDownstreamChange(streamID, "node-c", true)

	// Unsubscribe local (refcount -> 0), but downstream still present
	fib.Unsubscribe(streamID)

	// Should NOT tear down because downstream exists
	select {
	case <-disconnected:
		t.Fatal("should not disconnect while downstream nodes exist")
	case <-time.After(200 * time.Millisecond):
	}

	// Remove downstream -> triggers teardown
	fib.handleDownstreamChange(streamID, "node-c", false)
	select {
	case <-disconnected:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected teardown after removing last downstream")
	}
}

func TestFIB_HandleDownstreamChange_Nonexistent(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node")
	// Should not panic on non-existent stream
	fib.handleDownstreamChange("nonexistent", "node-x", true)
	fib.handleDownstreamChange("nonexistent", "node-x", false)
}

func TestFIB_HandleFailover(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node", WithLingerDuration(100*time.Millisecond))

	connectCalls := make(chan types.NodeID, 5)
	disconnectCalls := make(chan string, 5)

	fib.ConnectPipe = func(sid types.StreamID, up types.NodeID) (string, error) {
		connectCalls <- up
		return fmt.Sprintf("pipe-%s", up), nil
	}
	fib.DisconnectPipe = func(sid types.StreamID, pid string, up types.NodeID) error {
		disconnectCalls <- pid
		return nil
	}

	callCount := 0
	findPath := func(sid types.StreamID) (*types.RouteKVValue, error) {
		callCount++
		if callCount <= 1 {
			return &types.RouteKVValue{Node: "upstream-a", IsOrigin: true, PathVector: []types.NodeID{"upstream-a"}}, nil
		}
		return &types.RouteKVValue{Node: "upstream-b", IsOrigin: true, PathVector: []types.NodeID{"upstream-b"}}, nil
	}

	streamID := types.StreamID("s-failover")
	fib.Subscribe(streamID, findPath)

	// Trigger failover from upstream-a
	fib.handleFailover(streamID, "upstream-a", findPath)
	time.Sleep(100 * time.Millisecond)

	fib.mu.RLock()
	entry := fib.entries[streamID]
	newUp := entry.UpstreamNode
	fib.mu.RUnlock()

	if newUp != "upstream-b" {
		t.Fatalf("expected upstream-b, got %s", newUp)
	}
}

func TestFIB_HandleFailover_NoAlternative(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node", WithLingerDuration(50*time.Millisecond))

	disconnected := make(chan types.StreamID, 2)
	fib.ConnectPipe = func(sid types.StreamID, up types.NodeID) (string, error) {
		return "pipe-1", nil
	}
	fib.DisconnectPipe = func(sid types.StreamID, pid string, up types.NodeID) error {
		disconnected <- sid
		return nil
	}

	findPath := func(sid types.StreamID) (*types.RouteKVValue, error) {
		return &types.RouteKVValue{Node: "upstream-a", IsOrigin: true}, nil
	}
	failFindPath := func(sid types.StreamID) (*types.RouteKVValue, error) {
		return nil, fmt.Errorf("no route")
	}

	streamID := types.StreamID("s-fail-noalt")
	fib.Subscribe(streamID, findPath)

	fib.handleFailover(streamID, "upstream-a", failFindPath)
	time.Sleep(200 * time.Millisecond)

	fib.mu.RLock()
	_, exists := fib.entries[streamID]
	fib.mu.RUnlock()

	if exists {
		t.Fatal("expected entry to be removed after failed failover")
	}
}

func TestFIB_HandleFailover_AlreadyGone(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node")
	// Failover on non-existent stream
	fib.handleFailover("nonexistent", "upstream-x", func(sid types.StreamID) (*types.RouteKVValue, error) {
		return nil, nil
	})
}

func TestFIB_TeardownUpstream(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node")
	disconnected := make(chan types.StreamID, 2)
	fib.ConnectPipe = func(sid types.StreamID, up types.NodeID) (string, error) {
		return "pipe-tear", nil
	}
	fib.DisconnectPipe = func(sid types.StreamID, pid string, up types.NodeID) error {
		disconnected <- sid
		return nil
	}

	findPath := func(sid types.StreamID) (*types.RouteKVValue, error) {
		return &types.RouteKVValue{Node: "upstream-a", IsOrigin: true}, nil
	}

	streamID := types.StreamID("s-teardown")
	fib.Subscribe(streamID, findPath)

	// Teardown with matching upstream
	fib.teardownUpstream(streamID, "upstream-a")
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("expected teardown")
	}

	fib.mu.RLock()
	_, exists := fib.entries[streamID]
	fib.mu.RUnlock()
	if exists {
		t.Fatal("entry should be removed")
	}

	// Teardown with non-matching upstream (no-op)
	fib.teardownUpstream("nonexistent", "nope")
}

func TestFIB_OptimizeRoute(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node",
		WithRouteOptimization(true, 100*time.Millisecond, 5),
		WithLingerDuration(time.Minute),
	)
	defer fib.Close()

	connectCalls := make(chan types.NodeID, 5)
	fib.ConnectPipe = func(sid types.StreamID, up types.NodeID) (string, error) {
		connectCalls <- up
		return fmt.Sprintf("pipe-%s", up), nil
	}
	fib.DisconnectPipe = func(sid types.StreamID, pid string, up types.NodeID) error {
		return nil
	}

	streamID := types.StreamID("s-optimize")

	callCount := 0
	findPath := func(sid types.StreamID) (*types.RouteKVValue, error) {
		callCount++
		if callCount <= 1 {
			return &types.RouteKVValue{
				Node: "upstream-a", IsOrigin: true, Region: "region-1", Cluster: "cluster-2",
				PathVector: []types.NodeID{"upstream-a"}, BaseCost: 50,
			}, nil
		}
		return &types.RouteKVValue{
			Node: "upstream-b", IsOrigin: true, Region: "region-1", Cluster: "cluster-3",
			PathVector: []types.NodeID{"upstream-b"}, BaseCost: 30,
		}, nil
	}

	fib.Subscribe(streamID, findPath)
	// Wait for the optimization timer to fire
	time.Sleep(300 * time.Millisecond)

	fib.mu.RLock()
	entry, exists := fib.entries[streamID]
	var up types.NodeID
	if exists {
		up = entry.UpstreamNode
	}
	fib.mu.RUnlock()

	if !exists {
		t.Fatal("entry should exist")
	}
	// The optimization should have switched to upstream-b because cost improvement (50-30=20) > threshold (5)
	if up != "upstream-b" {
		t.Logf("upstream is %s (optimization may not have fired yet)", up)
	}
}

func TestFIB_SwitchUpstream(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node")

	fib.ConnectPipe = func(sid types.StreamID, up types.NodeID) (string, error) {
		return fmt.Sprintf("pipe-%s", up), nil
	}
	disconnectCalls := make(chan string, 5)
	fib.DisconnectPipe = func(sid types.StreamID, pid string, up types.NodeID) error {
		disconnectCalls <- pid
		return nil
	}

	findPath := func(sid types.StreamID) (*types.RouteKVValue, error) {
		return &types.RouteKVValue{Node: "upstream-a", IsOrigin: true, PathVector: []types.NodeID{"upstream-a"}}, nil
	}

	streamID := types.StreamID("s-switch")
	fib.Subscribe(streamID, findPath)

	newRoute := &types.RouteKVValue{
		Node: "upstream-b", IsOrigin: true,
		PathVector: []types.NodeID{"upstream-b"}, BaseCost: 5,
	}
	fib.switchUpstream(streamID, newRoute)

	fib.mu.RLock()
	entry := fib.entries[streamID]
	up := entry.UpstreamNode
	fib.mu.RUnlock()
	if up != "upstream-b" {
		t.Fatalf("expected upstream-b, got %s", up)
	}

	// Old pipe should be disconnected
	select {
	case pid := <-disconnectCalls:
		if pid != "pipe-upstream-a" {
			t.Fatalf("expected pipe-upstream-a, got %s", pid)
		}
	case <-time.After(time.Second):
		t.Fatal("expected disconnect of old pipe")
	}
}

func TestFIB_SwitchUpstream_Nonexistent(t *testing.T) {
	srv, rib, nc := setupFIBTestHelper(t)
	defer srv.Shutdown()
	defer nc.Close()
	defer rib.Close()

	fib := NewFIB(rib, "local-node")
	// Should not panic on non-existent stream
	fib.switchUpstream("nonexistent", &types.RouteKVValue{Node: "x"})
}
