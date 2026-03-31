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

func readNonNilEntry(t *testing.T, ch <-chan Entry, timeout time.Duration, msg string) Entry {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case e := <-ch:
			if e == nil {
				continue
			}
			return e
		case <-deadline:
			t.Fatal(msg)
			return nil
		}
	}
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
	e := readNonNilEntry(t, w.Updates(), 2*time.Second, "Timeout waiting for Put event")
	if e.Operation() != OpPut {
		t.Errorf("Expected OpPut, got %v", e.Operation())
	}
	if string(e.Value()) != string(value) {
		t.Errorf("Expected %s, got %s", value, e.Value())
	}

	// 3. Delete
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 4. Verify Watch receives Delete
	e = readNonNilEntry(t, w.Updates(), 2*time.Second, "Timeout waiting for Delete event")
	if e.Operation() != OpDelete {
		t.Errorf("Expected OpDelete, got %v", e.Operation())
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
	store, err := NewNatsStore(ctx, js, "KEYS_BUCKET", 1*time.Minute)
	if err != nil {
		t.Fatalf("NewNatsStore failed: %v", err)
	}

	// Put keys
	_ = store.Put(ctx, "a.1", []byte("v1"))
	_ = store.Put(ctx, "a.2", []byte("v2"))
	_ = store.Put(ctx, "b.1", []byte("v3"))

	// List keys
	keys, err := store.Keys(ctx, "a.>")
	if err != nil {
		t.Fatalf("Keys failed: %v", err)
	}

	if len(keys) != 2 {
		t.Errorf("Expected 2 keys, got %d: %v", len(keys), keys)
	}
}

func TestNatsStore_TTL(t *testing.T) {
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
	// TTL = 1 Second
	ttl := 1 * time.Second
	store, err := NewNatsStore(ctx, js, "TTL_BUCKET", ttl)
	if err != nil {
		t.Fatalf("NewNatsStore failed: %v", err)
	}

	key := "ttl-key"
	value := []byte("val")

	// 1. Put
	if err := store.Put(ctx, key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. Watch
	w, err := store.Watch(ctx, ">")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer w.Stop()

	// Wait for initial value (Put)
	e := readNonNilEntry(t, w.Updates(), 2*time.Second, "Timeout waiting for Put event")
	if e.Operation() != OpPut {
		t.Errorf("Expected OpPut, got %v", e.Operation())
	}

	// 3. Verify exists
	entry, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(entry.Value()) != string(value) {
		t.Errorf("Expected %s, got %s", value, entry.Value())
	}

	// 4. Wait > TTL
	time.Sleep(ttl + 500*time.Millisecond)

	// 5. Verify gone
	_, err = store.Get(ctx, key)
	if err == nil {
		t.Error("Expected error (key expired), got nil")
	}

	// 6. Verify Watch receives Delete (due to TTL)
	e = readNonNilEntry(t, w.Updates(), 3*time.Second, "Timeout waiting for Delete event (TTL)")
	t.Logf("Received event: Op=%v Key=%s", e.Operation(), e.Key())
	if e.Operation() != OpDelete {
		t.Errorf("Expected OpDelete, got %v", e.Operation())
	}
}
