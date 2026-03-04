package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jiyeyuran/omnistream/pkg/store"
	"github.com/jiyeyuran/omnistream/pkg/types"
)

const (
	SessionBucketName = "SESSIONS"
	SessionTTL        = 30 * time.Second
)

// SessionInfo records which node owns a participant session.
type SessionInfo struct {
	NodeID   types.NodeID `json:"nodeId"`
	RoomName string       `json:"roomName"`
	Identity string       `json:"identity"`
}

// SessionRegistry tracks participant sessions across the cluster using a
// shared KV store (NATS JetStream). This enables cross-node reconnection
// by allowing any node to look up where a given participantSid lives.
type SessionRegistry struct {
	store  store.Store
	nodeID types.NodeID
}

func NewSessionRegistry(s store.Store, nodeID types.NodeID) *SessionRegistry {
	return &SessionRegistry{store: s, nodeID: nodeID}
}

func (sr *SessionRegistry) Register(sid string, info SessionInfo) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return sr.store.Put(context.Background(), sid, data)
}

func (sr *SessionRegistry) Unregister(sid string) error {
	return sr.store.Delete(context.Background(), sid)
}

func (sr *SessionRegistry) Lookup(sid string) (*SessionInfo, error) {
	entry, err := sr.store.Get(context.Background(), sid)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, fmt.Errorf("session not found: %s", sid)
	}
	var info SessionInfo
	if err := json.Unmarshal(entry.Value(), &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// KeepAlive refreshes the TTL on an existing session entry.
func (sr *SessionRegistry) KeepAlive(sid string, info SessionInfo) error {
	return sr.Register(sid, info)
}

func (sr *SessionRegistry) Close() error {
	return sr.store.Close()
}
