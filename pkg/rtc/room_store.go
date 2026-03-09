package rtc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/store"
)

// RoomMeta is the authoritative room state stored in the shared KV so that
// every node creates the local Room instance with identical configuration.
//
// Key: room.<roomName>._meta
type RoomMeta struct {
	Sid          string            `json:"sid"`
	Name         string            `json:"name"`
	Metadata     string            `json:"metadata,omitempty"`
	CreationTime int64             `json:"creationTime"`
	Config       config.RoomConfig `json:"config"`
}

// GetRoomMeta reads the room meta from the shared KV (nil if not found).
func GetRoomMeta(kv store.Store, roomName string) (*RoomMeta, error) {
	key := fmt.Sprintf("room.%s._meta", roomName)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	entry, err := kv.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	var meta RoomMeta
	if err := json.Unmarshal(entry.Value(), &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// PutRoomMeta writes the room meta to the shared KV.
func PutRoomMeta(kv store.Store, meta *RoomMeta) {
	key := fmt.Sprintf("room.%s._meta", meta.Name)
	data, err := json.Marshal(meta)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = kv.Put(ctx, key, data)
}

// DeleteRoomMeta removes the room meta from the shared KV.
func DeleteRoomMeta(kv store.Store, roomName string) {
	key := fmt.Sprintf("room.%s._meta", roomName)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = kv.Delete(ctx, key)
}

// RoomStore provides cross-node synchronisation of room-level metadata
// (participants, groups, chat messages) via a shared KV store (NATS JetStream).
//
// Key layout:
//
//	room.<roomName>._meta              → JSON RoomMeta
//	room.<roomName>.participant.<sid>  → JSON ParticipantInfo
//	room.<roomName>.group.<groupId>   → JSON GroupInfo
//	room.<roomName>.msg.<id>          → JSON ChatMessageNotification
type RoomStore struct {
	kv       store.Store
	roomName string
	nodeID   string
	logger   *slog.Logger
}

func NewRoomStore(kv store.Store, roomName, nodeID string) *RoomStore {
	return &RoomStore{
		kv:       kv,
		roomName: roomName,
		nodeID:   nodeID,
		logger:   slog.Default().With("component", "RoomStore", "room", roomName),
	}
}

func (s *RoomStore) prefix() string {
	return "room." + s.roomName
}

// --- Participant ---

func (s *RoomStore) PutParticipant(info *proto.ParticipantInfo) {
	key := fmt.Sprintf("%s.participant.%s", s.prefix(), info.Sid)
	data, err := json.Marshal(info)
	if err != nil {
		s.logger.Warn("Failed to marshal participant", "error", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.kv.Put(ctx, key, data); err != nil {
		s.logger.Warn("Failed to put participant", "sid", info.Sid, "error", err)
	}
}

func (s *RoomStore) DeleteParticipant(sid string) {
	key := fmt.Sprintf("%s.participant.%s", s.prefix(), sid)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.kv.Delete(ctx, key); err != nil {
		s.logger.Warn("Failed to delete participant", "sid", sid, "error", err)
	}
}

func (s *RoomStore) GetAllParticipants() []*proto.ParticipantInfo {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pattern := fmt.Sprintf("%s.participant.*", s.prefix())
	keys, err := s.kv.Keys(ctx, pattern)
	if err != nil {
		s.logger.Warn("Failed to list participant keys", "error", err)
		return nil
	}
	result := make([]*proto.ParticipantInfo, 0, len(keys))
	for _, k := range keys {
		entry, err := s.kv.Get(ctx, k)
		if err != nil {
			continue
		}
		var info proto.ParticipantInfo
		if err := json.Unmarshal(entry.Value(), &info); err == nil {
			result = append(result, &info)
		}
	}
	return result
}

// --- Group ---

func (s *RoomStore) PutGroup(info *proto.GroupInfo) {
	key := fmt.Sprintf("%s.group.%s", s.prefix(), info.GroupId)
	data, err := json.Marshal(info)
	if err != nil {
		s.logger.Warn("Failed to marshal group", "error", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.kv.Put(ctx, key, data); err != nil {
		s.logger.Warn("Failed to put group", "groupId", info.GroupId, "error", err)
	}
}

func (s *RoomStore) DeleteGroup(groupId string) {
	key := fmt.Sprintf("%s.group.%s", s.prefix(), groupId)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.kv.Delete(ctx, key); err != nil {
		s.logger.Warn("Failed to delete group", "groupId", groupId, "error", err)
	}
}

func (s *RoomStore) GetAllGroups() []*proto.GroupInfo {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pattern := fmt.Sprintf("%s.group.*", s.prefix())
	keys, err := s.kv.Keys(ctx, pattern)
	if err != nil {
		s.logger.Warn("Failed to list group keys", "error", err)
		return nil
	}
	result := make([]*proto.GroupInfo, 0, len(keys))
	for _, k := range keys {
		entry, err := s.kv.Get(ctx, k)
		if err != nil {
			continue
		}
		var info proto.GroupInfo
		if err := json.Unmarshal(entry.Value(), &info); err == nil {
			result = append(result, &info)
		}
	}
	return result
}

// --- Chat Messages ---

func (s *RoomStore) PutMessage(msg *proto.ChatMessageNotification) {
	key := fmt.Sprintf("%s.msg.%s", s.prefix(), msg.ID)
	data, err := json.Marshal(msg)
	if err != nil {
		s.logger.Warn("Failed to marshal message", "error", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.kv.Put(ctx, key, data); err != nil {
		s.logger.Warn("Failed to put message", "id", msg.ID, "error", err)
	}
}

func (s *RoomStore) GetMessagesSince(since int64) []*proto.ChatMessageNotification {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pattern := fmt.Sprintf("%s.msg.*", s.prefix())
	keys, err := s.kv.Keys(ctx, pattern)
	if err != nil {
		s.logger.Warn("Failed to list message keys", "error", err)
		return nil
	}
	var result []*proto.ChatMessageNotification
	for _, k := range keys {
		entry, err := s.kv.Get(ctx, k)
		if err != nil {
			continue
		}
		var msg proto.ChatMessageNotification
		if err := json.Unmarshal(entry.Value(), &msg); err != nil {
			continue
		}
		ts := msg.Timestamp
		if msg.EditTimestamp != nil && *msg.EditTimestamp > ts {
			ts = *msg.EditTimestamp
		}
		if ts > since {
			result = append(result, &msg)
		}
	}
	return result
}

// --- Track Subscription (dynacast cross-node) ---

// TrackSubInfo is the per-layer subscription state for one track on one node.
// Key: room.<roomName>.tracksub.<trackSid>.<nodeId>
type TrackSubInfo struct {
	TrackSid string  `json:"trackSid"`
	NodeId   string  `json:"nodeId"`
	Layers   [3]bool `json:"layers"` // [LOW, MEDIUM, HIGH] — which layers have active subscribers
}

func (s *RoomStore) PutTrackSub(info *TrackSubInfo) {
	key := fmt.Sprintf("%s.tracksub.%s.%s", s.prefix(), info.TrackSid, info.NodeId)
	data, err := json.Marshal(info)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.kv.Put(ctx, key, data)
}

func (s *RoomStore) DeleteTrackSub(trackSid string) {
	key := fmt.Sprintf("%s.tracksub.%s.%s", s.prefix(), trackSid, s.nodeID)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.kv.Delete(ctx, key)
}

// --- Watch ---

// RoomStoreEvent represents a change observed from the shared store.
type RoomStoreEvent struct {
	Type      string // "participant", "group", "msg", "tracksub"
	Operation store.Operation
	Key       string // the sub-key (sid, groupId, msgId, trackSid.nodeId)

	Participant *proto.ParticipantInfo
	Group       *proto.GroupInfo
	Message     *proto.ChatMessageNotification
	TrackSub    *TrackSubInfo
}

// Watch starts watching all keys under this room prefix. Events from the
// local node are included — the caller is expected to filter by comparing
// ParticipantInfo.NodeId or similar if needed.
func (s *RoomStore) Watch(ctx context.Context) (<-chan RoomStoreEvent, error) {
	pattern := fmt.Sprintf("%s.>", s.prefix())
	watcher, err := s.kv.Watch(ctx, pattern)
	if err != nil {
		return nil, err
	}

	ch := make(chan RoomStoreEvent, 64)
	go func() {
		defer close(ch)
		for entry := range watcher.Updates() {
			evt, ok := s.parseEvent(entry)
			if !ok {
				continue
			}
			select {
			case ch <- evt:
			case <-ctx.Done():
				watcher.Stop()
				return
			}
		}
	}()
	return ch, nil
}

func (s *RoomStore) parseEvent(entry store.Entry) (RoomStoreEvent, bool) {
	// Key format: room.<roomName>.<type>.<id>
	key := entry.Key()
	prefix := s.prefix() + "."
	if !strings.HasPrefix(key, prefix) {
		return RoomStoreEvent{}, false
	}
	rest := key[len(prefix):]
	parts := strings.SplitN(rest, ".", 2)
	if len(parts) != 2 {
		return RoomStoreEvent{}, false
	}

	evt := RoomStoreEvent{
		Type:      parts[0],
		Operation: entry.Operation(),
		Key:       parts[1],
	}

	if entry.Operation() == store.OpPut {
		switch evt.Type {
		case "participant":
			var info proto.ParticipantInfo
			if json.Unmarshal(entry.Value(), &info) == nil {
				evt.Participant = &info
			}
		case "group":
			var info proto.GroupInfo
			if json.Unmarshal(entry.Value(), &info) == nil {
				evt.Group = &info
			}
		case "msg":
			var msg proto.ChatMessageNotification
			if json.Unmarshal(entry.Value(), &msg) == nil {
				evt.Message = &msg
			}
		case "tracksub":
			var info TrackSubInfo
			if json.Unmarshal(entry.Value(), &info) == nil {
				evt.TrackSub = &info
			}
		}
	}

	return evt, true
}

// CleanupLocal removes participants belonging to this node from the store.
// After removing local participants, if no participants remain at all it also
// removes the room meta key so the room sid can be recycled.
func (s *RoomStore) CleanupLocal() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Remove local participants.
	pattern := fmt.Sprintf("%s.participant.>", s.prefix())
	keys, err := s.kv.Keys(ctx, pattern)
	if err != nil {
		return
	}
	for _, k := range keys {
		entry, err := s.kv.Get(ctx, k)
		if err != nil {
			continue
		}
		var info proto.ParticipantInfo
		if json.Unmarshal(entry.Value(), &info) == nil && info.NodeId == s.nodeID {
			_ = s.kv.Delete(ctx, k)
		}
	}

	// Remove local track subscriptions.
	subPattern := fmt.Sprintf("%s.tracksub.>", s.prefix())
	subKeys, _ := s.kv.Keys(ctx, subPattern)
	for _, k := range subKeys {
		// tracksub.<trackSid>.<nodeId> — last segment is nodeId
		if strings.HasSuffix(k, "."+s.nodeID) {
			_ = s.kv.Delete(ctx, k)
		}
	}

	// If no participants remain across all nodes, clean up everything.
	remaining, _ := s.kv.Keys(ctx, pattern)
	if len(remaining) == 0 {
		s.cleanupAll(ctx)
	}
}

// cleanupAll removes every key under this room (including _meta).
func (s *RoomStore) cleanupAll(ctx context.Context) {
	pattern := fmt.Sprintf("%s.>", s.prefix())
	keys, err := s.kv.Keys(ctx, pattern)
	if err != nil {
		return
	}
	for _, k := range keys {
		_ = s.kv.Delete(ctx, k)
	}
}
