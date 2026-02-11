package routing

import (
	"context"
	"fmt"
	"rib-control/pkg/store"
	"rib-control/pkg/types"
	"sync"
	"testing"
	"time"

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
