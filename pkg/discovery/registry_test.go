package discovery_test

import (
	"context"
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/discovery"
	"github.com/axonkit/axonkit/pkg/store"
	"github.com/axonkit/axonkit/pkg/types"

	server "github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type noDeleteStore struct {
	store.Store
}

func (s *noDeleteStore) Delete(ctx context.Context, key string) error {
	return nil
}

func setupNATS(t *testing.T) *server.Server {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	srv := natsserver.RunServer(&opts)
	return srv
}

func TestRegistryTTL(t *testing.T) {
	srv := setupNATS(t)
	defer srv.Shutdown()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to init JetStream: %v", err)
	}

	ttl := 1 * time.Second
	nodeTTL := ttl

	// Create real store
	realStore, err := store.NewNatsStore(context.Background(), js, discovery.NodeBucketName, nodeTTL)
	if err != nil {
		t.Fatalf("Failed to create NATS store: %v", err)
	}

	// Create wrapper store that ignores deletes
	testStore := &noDeleteStore{Store: realStore}

	nodeInfo := types.NodeInfo{
		ID:      "test-node",
		Region:  "test-region",
		Cluster: "test-cluster",
	}

	// Use WithTTL to set a short TTL
	r := discovery.NewRegistry(testStore, nodeInfo, discovery.WithTTL(nodeTTL))

	// Start Registry
	if err := r.Start(); err != nil {
		t.Fatalf("Failed to start registry: %v", err)
	}

	// 1. Verify Node Exists
	ctx := context.Background()
	_, err = realStore.Get(ctx, string(nodeInfo.ID))
	if err != nil {
		t.Fatalf("Node not found immediately after start: %v", err)
	}

	// 2. Wait > TTL to verify Keepalive is working
	// KeepAliveInterval should be ttl/2 = 0.5s.
	// We wait 2s, so ~4 keepalives should happen.
	time.Sleep(2 * time.Second)

	_, err = realStore.Get(ctx, string(nodeInfo.ID))
	if err != nil {
		t.Fatalf("Node expired despite keepalive: %v", err)
	}

	// 3. Stop Registry (simulating crash by ignoring delete)
	// r.Close() calls testStore.Delete which does nothing, but it stops the keepalive loop.
	r.Close()

	// 4. Wait > TTL for node to expire (via Delayed Message mechanism in Store)
	// Delayed Message should trigger ~1s after last keepalive.
	// Wait buffer.
	time.Sleep(2 * time.Second)

	// 5. Verify Node is Gone
	_, err = realStore.Get(ctx, string(nodeInfo.ID))
	if err == nil {
		t.Fatal("Node did not expire after TTL")
	}
}

func TestRegistryWatch(t *testing.T) {
	srv := setupNATS(t)
	defer srv.Shutdown()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to init JetStream: %v", err)
	}

	ttl := 1 * time.Second
	nodeTTL := ttl

	// Create real store
	realStore, err := store.NewNatsStore(context.Background(), js, discovery.NodeBucketName, nodeTTL)
	if err != nil {
		t.Fatalf("Failed to create NATS store: %v", err)
	}

	// Local Node (Watcher)
	localNode := types.NodeInfo{
		ID:      "watcher-node",
		Region:  "test-region",
		Cluster: "test-cluster",
	}

	watcher := discovery.NewRegistry(realStore, localNode, discovery.WithTTL(nodeTTL))
	if err := watcher.Start(); err != nil {
		t.Fatalf("Failed to start watcher registry: %v", err)
	}
	defer watcher.Close()

	// Remote Node (To be watched)
	remoteNode := types.NodeInfo{
		ID:      "remote-node",
		Region:  "test-region",
		Cluster: "test-cluster",
	}

	// Use noDeleteStore for remote node so we can simulate crash/TTL expiry
	remoteStore := &noDeleteStore{Store: realStore}
	remoteRegistry := discovery.NewRegistry(remoteStore, remoteNode, discovery.WithTTL(nodeTTL))
	if err := remoteRegistry.Start(); err != nil {
		t.Fatalf("Failed to start remote registry: %v", err)
	}

	// Channel to receive delete notifications
	deleteCh := make(chan types.NodeID, 1)

	// Start Watching
	err = watcher.WatchNodes(func(node types.NodeInfo, isDelete bool) {
		if isDelete {
			deleteCh <- node.ID
		}
	})
	if err != nil {
		t.Fatalf("Failed to start watching: %v", err)
	}

	// 1. Verify remote node is alive (implicitly, no delete yet)
	select {
	case id := <-deleteCh:
		t.Fatalf("Received premature delete notification for %s", id)
	case <-time.After(500 * time.Millisecond):
		// OK
	}

	// 2. Simulate Remote Node Crash (Stop keepalive, don't delete explicitly)
	// We call Close() on the registry using the noDeleteStore, so the key remains
	// but the keepalive stops. NATS TTL should clean it up.
	remoteRegistry.Close()

	// Wait for TTL expiry (1s) + some buffer
	// The Delayed Message should arrive and trigger delete
	time.Sleep(2 * time.Second)

	// 3. Wait for Watch notification
	// Now we expect the Delayed Message worker to have deleted the node, triggering the Watcher.
	select {
	case id := <-deleteCh:
		if id != remoteNode.ID {
			t.Errorf("Received delete for wrong node: got %s, want %s", id, remoteNode.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for delete notification")
	}
}
