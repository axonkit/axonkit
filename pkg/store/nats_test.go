package store

import (
	"context"
	"os"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func runServer(t *testing.T) *natsserver.Server {
	dir, err := os.MkdirTemp("", "nats-js-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	opts := &natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  dir,
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("Server not ready")
	}

	t.Cleanup(func() {
		s.Shutdown()
		os.RemoveAll(dir)
	})

	return s
}

func TestNatsStore_Watch_Put_Delete(t *testing.T) {
	s := runServer(t)

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to init JS: %v", err)
	}

	ctx := context.Background()
	store, err := NewNatsStore(ctx, js, "TEST_BUCKET", 1*time.Minute)
	if err != nil {
		t.Fatalf("NewNatsStore failed: %v", err)
	}

	key := "foo"
	value := []byte("bar")

	// 1. Put
	if err := store.Put(ctx, key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. Watch
	w, err := store.Watch(ctx, key)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer w.Stop()

	// Wait for initial value (Put)
	select {
	case e := <-w.Updates():
		if e == nil {
			t.Fatal("Received nil entry")
		}
		if e.Operation() != OpPut {
			t.Errorf("Expected OpPut, got %v", e.Operation())
		}
		if string(e.Value()) != string(value) {
			t.Errorf("Expected %s, got %s", value, e.Value())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for Put event")
	}

	// 3. Delete
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 4. Verify Watch receives Delete
	select {
	case e := <-w.Updates():
		if e == nil {
			t.Fatal("Received nil entry")
		}
		if e.Operation() != OpDelete {
			t.Errorf("Expected OpDelete, got %v", e.Operation())
		}
		// e.Value() is likely nil/empty for delete
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for Delete event")
	}
}

func TestNatsStore_Keys(t *testing.T) {
	s := runServer(t)

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to init JS: %v", err)
	}

	ctx := context.Background()
	store, err := NewNatsStore(ctx, js, "TEST_BUCKET_KEYS", 1*time.Minute)
	if err != nil {
		t.Fatalf("NewNatsStore failed: %v", err)
	}

	// Prepare Data
	keysToPut := []string{
		"app.us.node1",
		"app.us.node2",
		"app.eu.node1",
		"db.us.primary",
	}

	for _, k := range keysToPut {
		if err := store.Put(ctx, k, []byte("val")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	tests := []struct {
		name    string
		pattern string
		want    []string
	}{
		{
			name:    "Match Prefix",
			pattern: "app.>",
			want:    []string{"app.us.node1", "app.us.node2", "app.eu.node1"},
		},
		{
			name:    "Match Middle",
			pattern: "app.us.*",
			want:    []string{"app.us.node1", "app.us.node2"},
		},
		{
			name:    "Match Exact",
			pattern: "db.us.primary",
			want:    []string{"db.us.primary"},
		},
		{
			name:    "No Match",
			pattern: "other.>",
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.Keys(ctx, tt.pattern)
			if err != nil {
				t.Fatalf("Keys failed: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Errorf("Keys count mismatch: got %d, want %d (got: %v)", len(got), len(tt.want), got)
			}
			m := make(map[string]bool)
			for _, k := range got {
				m[k] = true
			}
			for _, k := range tt.want {
				if !m[k] {
					t.Errorf("Missing key: %s", k)
				}
			}
		})
	}
}

func TestNatsStore_Put_Get(t *testing.T) {
	s := runServer(t)

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to init JS: %v", err)
	}

	ctx := context.Background()
	store, err := NewNatsStore(ctx, js, "TEST_BUCKET_PUT_GET", 1*time.Minute)
	if err != nil {
		t.Fatalf("NewNatsStore failed: %v", err)
	}

	key := "my-key"
	val1 := []byte("value-1")
	val2 := []byte("value-2")

	// 1. Get non-existent
	_, err = store.Get(ctx, key)
	if err == nil {
		t.Fatal("Expected error for non-existent key, got nil")
	}

	// 2. Put
	if err := store.Put(ctx, key, val1); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 3. Get
	entry, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(entry.Value()) != string(val1) {
		t.Errorf("Value mismatch: got %s, want %s", entry.Value(), val1)
	}
	if entry.Key() != key {
		t.Errorf("Key mismatch: got %s, want %s", entry.Key(), key)
	}

	// 4. Update (Overwrite)
	if err := store.Put(ctx, key, val2); err != nil {
		t.Fatalf("Put update failed: %v", err)
	}

	// 5. Get Updated
	entry, err = store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get updated failed: %v", err)
	}
	if string(entry.Value()) != string(val2) {
		t.Errorf("Value mismatch after update: got %s, want %s", entry.Value(), val2)
	}

	// 6. Delete
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 7. Get after Delete (should fail)
	_, err = store.Get(ctx, key)
	if err == nil {
		t.Fatal("Expected error for deleted key, got nil")
	}
}
