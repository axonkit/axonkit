package store

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestMemoryStore_Basic(t *testing.T) {
	ms := NewMemoryStore(0)
	defer ms.Close()

	ctx := context.Background()
	key := "test.key"
	val := []byte("hello")

	// Put
	if err := ms.Put(ctx, key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get
	entry, err := ms.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(entry.Value()) != string(val) {
		t.Errorf("Get value mismatch: got %s, want %s", entry.Value(), val)
	}

	// Keys
	keys, err := ms.Keys(ctx, ">")
	if err != nil {
		t.Fatalf("Keys failed: %v", err)
	}
	if len(keys) != 1 || keys[0] != key {
		t.Errorf("Keys mismatch: got %v, want [%s]", keys, key)
	}

	// Delete
	if err := ms.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Get after Delete
	if _, err := ms.Get(ctx, key); err == nil {
		t.Error("Get should fail after Delete")
	}
}

func TestMemoryStore_TTL(t *testing.T) {
	ttl := 100 * time.Millisecond
	ms := NewMemoryStore(ttl)
	defer ms.Close()

	ctx := context.Background()
	key := "ttl.key"
	val := []byte("data")

	if err := ms.Put(ctx, key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify exists immediately
	if _, err := ms.Get(ctx, key); err != nil {
		t.Fatalf("Get failed immediately: %v", err)
	}

	// Wait for expiration
	time.Sleep(2 * ttl)

	// Verify gone
	if _, err := ms.Get(ctx, key); err == nil {
		t.Error("Key should have expired")
	}
}

func TestMemoryStore_Watch(t *testing.T) {
	ms := NewMemoryStore(0)
	defer ms.Close()

	ctx := context.Background()

	// Pre-populate data to test initial state
	preKey := "watch.pre"
	if err := ms.Put(ctx, preKey, []byte("pre")); err != nil {
		t.Fatalf("Put pre failed: %v", err)
	}

	w, err := ms.Watch(ctx, "watch.>")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer w.Stop()

	updates := w.Updates()

	// Verify initial state
	select {
	case e := <-updates:
		if e.Key() != preKey {
			t.Errorf("Initial state key mismatch: got %s, want %s", e.Key(), preKey)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for initial state")
	}

	// Test Update
	newKey := "watch.new"
	if err := ms.Put(ctx, newKey, []byte("new")); err != nil {
		t.Fatalf("Put new failed: %v", err)
	}

	select {
	case e := <-updates:
		if e.Key() != newKey {
			t.Errorf("Update key mismatch: got %s, want %s", e.Key(), newKey)
		}
		if e.Operation() != OpPut {
			t.Errorf("Operation mismatch: got %v, want %v", e.Operation(), OpPut)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for update")
	}

	// Test Delete
	if err := ms.Delete(ctx, newKey); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	select {
	case e := <-updates:
		if e.Key() != newKey {
			t.Errorf("Delete key mismatch: got %s, want %s", e.Key(), newKey)
		}
		if e.Operation() != OpDelete {
			t.Errorf("Operation mismatch: got %v, want %v", e.Operation(), OpDelete)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for delete")
	}
}

func TestMemoryStore_Keys(t *testing.T) {
	ms := NewMemoryStore(0)
	defer ms.Close()

	ctx := context.Background()
	data := map[string]string{
		"a.b.c": "1",
		"a.b.d": "2",
		"a.x.y": "3",
		"z.1.2": "4",
	}

	for k, v := range data {
		ms.Put(ctx, k, []byte(v))
	}

	tests := []struct {
		pattern string
		want    []string
	}{
		{">", []string{"a.b.c", "a.b.d", "a.x.y", "z.1.2"}},
		{"a.>", []string{"a.b.c", "a.b.d", "a.x.y"}},
		{"a.b.*", []string{"a.b.c", "a.b.d"}},
		{"*.b.*", []string{"a.b.c", "a.b.d"}}, // Assuming matchSubject supports this
		{"nomatch.>", []string{}},
	}

	for _, tt := range tests {
		got, err := ms.Keys(ctx, tt.pattern)
		if err != nil {
			t.Errorf("Keys(%q) failed: %v", tt.pattern, err)
			continue
		}
		sort.Strings(got)
		sort.Strings(tt.want)

		if len(got) == 0 && len(tt.want) == 0 {
			continue
		}

		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("Keys(%q) = %v, want %v", tt.pattern, got, tt.want)
		}
	}
}

func TestMatchSubject(t *testing.T) {
	tests := []struct {
		pattern string
		subject string
		want    bool
	}{
		{">", "foo.bar", true},
		{"foo.>", "foo.bar", true},
		{"foo.>", "foo.bar.baz", true},
		{"foo.*", "foo.bar", true},
		{"foo.*", "foo.bar.baz", false},
		{"*", "foo", true},
		{"*", "foo.bar", false},
		{"foo.bar", "foo.bar", true},
		{"foo.bar", "foo.baz", false},
	}

	for _, tt := range tests {
		if got := matchSubject(tt.pattern, tt.subject); got != tt.want {
			t.Errorf("matchSubject(%q, %q) = %v, want %v", tt.pattern, tt.subject, got, tt.want)
		}
	}
}
