package whip

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/axonkit/axonkit/pkg/store"
)

// ResourceBucketName is the JetStream KV bucket used to publish WHIP
// standalone resource records across the cluster. The Server wires this
// in via store.NewNatsStore when it constructs the HTTP layer.
const ResourceBucketName = "WHIP_RESOURCES"

// ResourceBucketTTL is the MaxAge of the resource bucket. It must be
// larger than the Session supervisor's keepalive interval so that a
// publisher whose supervisor ticks at DefaultKeepaliveInterval does not
// lose its record between ticks. It must also be short enough that a
// crashed publisher is reclaimed within a reasonable window.
const ResourceBucketTTL = 30 * time.Second

// ResourceRecord is the JSON payload persisted in the WHIP_RESOURCES
// bucket. It contains just enough information for a remote node to pipe
// the streams on demand (FIB.Subscribe per StreamID) and to report which
// node currently owns the publisher.
type ResourceRecord struct {
	ResourceID string   `json:"resource_id"`
	NodeID     string   `json:"node_id"`
	StreamIDs  []string `json:"stream_ids"` // "<routerId>/<producerId>" each
	UpdatedAt  int64    `json:"updated_at"` // unix nanos
}

// ResourceStore is a thin, typed facade over a store.Store scoped to the
// WHIP_RESOURCES bucket. The WHIP service does not import it directly —
// the HTTP layer constructs one, captures it in Session.Keepalive closures,
// and uses it when resolving /whep/{resourceId} on remote nodes.
type ResourceStore struct {
	kv store.Store
}

// NewResourceStore wraps the given KV store.
func NewResourceStore(kv store.Store) *ResourceStore {
	return &ResourceStore{kv: kv}
}

// Put writes (or refreshes) a resource record. The underlying bucket has a
// server-side TTL so a crashed publisher whose record is never refreshed
// gets evicted automatically — the record acts as its own lease.
func (s *ResourceStore) Put(ctx context.Context, rec ResourceRecord) error {
	if s == nil || s.kv == nil {
		return fmt.Errorf("resource store not configured")
	}
	rec.UpdatedAt = time.Now().UnixNano()
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal resource record: %w", err)
	}
	return s.kv.Put(ctx, rec.ResourceID, payload)
}

// Get returns the resource record for resourceID, or nil if no record
// exists (either the publisher is on this same node's in-memory registry
// only, or it has been evicted).
func (s *ResourceStore) Get(ctx context.Context, resourceID string) (*ResourceRecord, error) {
	if s == nil || s.kv == nil {
		return nil, nil
	}
	entry, err := s.kv.Get(ctx, resourceID)
	if err != nil || entry == nil {
		return nil, err
	}
	var rec ResourceRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		return nil, fmt.Errorf("unmarshal resource record: %w", err)
	}
	return &rec, nil
}

// Delete removes the record for resourceID. Called from Session.OnClose
// so that a clean DELETE propagates immediately instead of waiting for
// the bucket TTL to elapse.
func (s *ResourceStore) Delete(ctx context.Context, resourceID string) error {
	if s == nil || s.kv == nil {
		return nil
	}
	return s.kv.Delete(ctx, resourceID)
}
