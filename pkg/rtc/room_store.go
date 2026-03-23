package rtc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/store"
)

// RoomStateBucketTTL is the JetStream KV max age for the ROOM_STATE bucket.
// Each key's timer resets on Put; use a long horizon so idle participants are
// not dropped while still connected. This TTL mainly retires orphans after
// crashes (alongside explicit deletes and node-offline cleanup).
const RoomStateBucketTTL = 7 * 24 * time.Hour

// OrphanRoomMetaGrace is how long room meta may exist without a lease before
// the sweeper purges the room prefix. This avoids racing GetOrCreateRoom,
// which writes meta before the heartbeat goroutine creates the lease key.
const OrphanRoomMetaGrace = 15 * time.Second

// RoomStateSweeperInterval is how often nodes scan for meta-without-lease orphans.
const RoomStateSweeperInterval = time.Minute

// MaxRoomNameLen is the maximum length of a room name accepted on join.
const MaxRoomNameLen = 128

// ValidateRoomName returns an error if name is not allowed for /rtc/join.
// Names must be at most MaxRoomNameLen Unicode code points. Allowed runes are
// letters (any script), decimal digits, hyphen, and underscore; punctuation,
// symbols, spaces, and other non-alphanumeric characters are rejected. This
// keeps one KV subject segment (matches room.*.meta sweeper pattern).
func ValidateRoomName(name string) error {
	if utf8.RuneCountInString(name) > MaxRoomNameLen {
		return fmt.Errorf("room name exceeds %d characters", MaxRoomNameLen)
	}
	if name == "" {
		return fmt.Errorf("room name may only contain letters, digits, hyphen and underscore")
	}
	for _, r := range name {
		if r == '-' || r == '_' {
			continue
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			continue
		}
		return fmt.Errorf("room name may only contain letters, digits, hyphen and underscore")
	}
	return nil
}

// RoomMeta is the authoritative room state stored in the shared KV so that
// every node creates the local Room instance with identical configuration.
//
// Key: room.<roomName>.meta
type RoomMeta struct {
	Sid          string            `json:"sid"`
	Name         string            `json:"name"`
	Metadata     string            `json:"metadata,omitempty"`
	CreationTime int64             `json:"creationTime"`
	Config       config.RoomConfig `json:"config"`
}

// GetRoomMeta reads the room meta from the shared KV (nil if not found).
func GetRoomMeta(kv store.Store, roomName string) (*RoomMeta, error) {
	key := fmt.Sprintf("room.%s.meta", roomName)
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
	key := fmt.Sprintf("room.%s.meta", meta.Name)
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
	key := fmt.Sprintf("room.%s.meta", roomName)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = kv.Delete(ctx, key)
}

// RoomLeaseKey returns the JetStream KV key for a room's liveness lease.
func RoomLeaseKey(roomName string) string {
	return fmt.Sprintf("room.%s.lease", roomName)
}

// PurgeRoomKV removes room.<roomName>.meta (best-effort) and every key under
// room.<roomName>.>. Used after lease loss or when the sweeper finds meta
// without a lease (orphan).
func PurgeRoomKV(ctx context.Context, kv store.Store, roomName string) {
	DeleteRoomMeta(kv, roomName)
	pattern := fmt.Sprintf("room.%s.>", roomName)
	keys, err := kv.Keys(ctx, pattern)
	if err != nil {
		return
	}
	for _, k := range keys {
		_ = kv.Delete(ctx, k)
	}
}

// MetaKeyRoomName parses room.<roomName>.meta and returns roomName.
// Join enforces a single segment without dots; this still accepts dotted
// segments for legacy keys.
func MetaKeyRoomName(metaKey string) (roomName string, ok bool) {
	const suffix = ".meta"
	if !strings.HasPrefix(metaKey, "room.") || !strings.HasSuffix(metaKey, suffix) {
		return "", false
	}
	name := strings.TrimSuffix(metaKey[len("room."):], suffix)
	if name == "" {
		return "", false
	}
	return name, true
}

// RoomStore provides cross-node synchronization of room-level metadata
// (participants, groups, messages, shared state) via a shared KV store (NATS JetStream).
//
// Key layout:
//
//	room.<roomName>.meta              → JSON RoomMeta
//	room.<roomName>.lease             → (empty, with TTL)
//	room.<roomName>.participant.<sid>  → JSON ParticipantInfo
//	room.<roomName>.track.<participantSid>.<trackSid>       → JSON TrackInfo
//	room.<roomName>.dataTrack.<participantSid>.<dataTrackSid> → JSON DataTrackInfo
//	room.<roomName>.group.<groupId>   → JSON GroupInfo
//	room.<roomName>.msg.<id>          → JSON MessageNotification
//	room.<roomName>.state.<key>       → plain string value
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

// StartHeartbeat maintains a lease for the room and cleans up if the lease expires.
// It should be called by the room creator/manager.
func (s *RoomStore) StartHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	leaseKey := fmt.Sprintf("%s.lease", s.prefix())

	// Create the lease key once with a TTL.
	ctxCreate, cancelCreate := context.WithTimeout(ctx, 2*time.Second)
	_ = s.kv.Create(ctxCreate, leaseKey, []byte(s.nodeID), store.WithCreateTTL(5*time.Second))
	cancelCreate()

	updateLease := func() {
		ctxPut, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_ = s.kv.Put(ctxPut, leaseKey, []byte(s.nodeID))
	}

	updateLease()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updateLease()
		}
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

func (s *RoomStore) GetParticipant(sid string) *proto.ParticipantInfo {
	key := fmt.Sprintf("%s.participant.%s", s.prefix(), sid)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	entry, err := s.kv.Get(ctx, key)
	if err != nil {
		return nil
	}
	var info proto.ParticipantInfo
	if err := json.Unmarshal(entry.Value(), &info); err == nil {
		return &info
	}
	return nil
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

// --- Track & DataTrack ---

func (s *RoomStore) PutTrack(participantSid string, info *proto.TrackInfo) {
	key := fmt.Sprintf("%s.track.%s.%s", s.prefix(), participantSid, info.Sid)
	data, err := json.Marshal(info)
	if err != nil {
		s.logger.Warn("Failed to marshal track", "error", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.kv.Put(ctx, key, data); err != nil {
		s.logger.Warn("Failed to put track", "sid", info.Sid, "error", err)
	}
}

func (s *RoomStore) DeleteTrack(participantSid, trackSid string) {
	key := fmt.Sprintf("%s.track.%s.%s", s.prefix(), participantSid, trackSid)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.kv.Delete(ctx, key); err != nil {
		s.logger.Warn("Failed to delete track", "sid", trackSid, "error", err)
	}
}

func (s *RoomStore) GetAllTracks() map[string][]*proto.TrackInfo {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pattern := fmt.Sprintf("%s.track.>", s.prefix())
	keys, err := s.kv.Keys(ctx, pattern)
	if err != nil {
		return nil
	}
	result := make(map[string][]*proto.TrackInfo)
	for _, k := range keys {
		entry, err := s.kv.Get(ctx, k)
		if err != nil {
			continue
		}
		var info proto.TrackInfo
		if err := json.Unmarshal(entry.Value(), &info); err == nil {
			parts := strings.Split(k, ".")
			if len(parts) >= 4 {
				participantSid := parts[3]
				result[participantSid] = append(result[participantSid], &info)
			}
		}
	}
	return result
}

func (s *RoomStore) PutDataTrack(participantSid string, info *proto.DataTrackInfo) {
	key := fmt.Sprintf("%s.dataTrack.%s.%s", s.prefix(), participantSid, info.Sid)
	data, err := json.Marshal(info)
	if err != nil {
		s.logger.Warn("Failed to marshal dataTrack", "error", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.kv.Put(ctx, key, data); err != nil {
		s.logger.Warn("Failed to put dataTrack", "sid", info.Sid, "error", err)
	}
}

func (s *RoomStore) DeleteDataTrack(participantSid, trackSid string) {
	key := fmt.Sprintf("%s.dataTrack.%s.%s", s.prefix(), participantSid, trackSid)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.kv.Delete(ctx, key); err != nil {
		s.logger.Warn("Failed to delete dataTrack", "sid", trackSid, "error", err)
	}
}

func (s *RoomStore) GetAllDataTracks() map[string][]*proto.DataTrackInfo {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pattern := fmt.Sprintf("%s.dataTrack.>", s.prefix())
	keys, err := s.kv.Keys(ctx, pattern)
	if err != nil {
		return nil
	}
	result := make(map[string][]*proto.DataTrackInfo)
	for _, k := range keys {
		entry, err := s.kv.Get(ctx, k)
		if err != nil {
			continue
		}
		var info proto.DataTrackInfo
		if err := json.Unmarshal(entry.Value(), &info); err == nil {
			parts := strings.Split(k, ".")
			if len(parts) >= 4 {
				participantSid := parts[3]
				result[participantSid] = append(result[participantSid], &info)
			}
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

// --- Messages ---

func (s *RoomStore) PutMessage(msg *proto.MessageNotification) {
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

func (s *RoomStore) DeleteMessage(id string) {
	key := fmt.Sprintf("%s.msg.%s", s.prefix(), id)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.kv.Delete(ctx, key); err != nil {
		s.logger.Warn("Failed to delete message", "id", id, "error", err)
	}
}

func (s *RoomStore) DeleteMessagesByIDs(ids []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, id := range ids {
		key := fmt.Sprintf("%s.msg.%s", s.prefix(), id)
		_ = s.kv.Delete(ctx, key)
	}
}

func (s *RoomStore) GetMessagesSince(since int64) []*proto.MessageNotification {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pattern := fmt.Sprintf("%s.msg.*", s.prefix())
	keys, err := s.kv.Keys(ctx, pattern)
	if err != nil {
		s.logger.Warn("Failed to list message keys", "error", err)
		return nil
	}
	var result []*proto.MessageNotification
	for _, k := range keys {
		entry, err := s.kv.Get(ctx, k)
		if err != nil {
			continue
		}
		var msg proto.MessageNotification
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

// --- Room Shared State ---

func (s *RoomStore) PutState(key, value string) {
	storeKey := fmt.Sprintf("%s.state.%s", s.prefix(), key)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.kv.Put(ctx, storeKey, []byte(value)); err != nil {
		s.logger.Warn("Failed to put state", "key", key, "error", err)
	}
}

func (s *RoomStore) DeleteState(key string) {
	storeKey := fmt.Sprintf("%s.state.%s", s.prefix(), key)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.kv.Delete(ctx, storeKey); err != nil {
		s.logger.Warn("Failed to delete state", "key", key, "error", err)
	}
}

func (s *RoomStore) GetAllState() map[string]string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pattern := fmt.Sprintf("%s.state.*", s.prefix())
	keys, err := s.kv.Keys(ctx, pattern)
	if err != nil {
		return nil
	}
	result := make(map[string]string, len(keys))
	prefix := fmt.Sprintf("%s.state.", s.prefix())
	for _, k := range keys {
		entry, err := s.kv.Get(ctx, k)
		if err != nil {
			continue
		}
		stateKey := strings.TrimPrefix(k, prefix)
		result[stateKey] = string(entry.Value())
	}
	return result
}

// --- Track Subscription (dynacast cross-node) ---

// TrackSubInfo is the per-layer subscription state for one track on one node.
// Key: room.<roomName>.tracksub.<trackSid>.<nodeId>
type TrackSubInfo struct {
	TrackSid     string  `json:"trackSid"`
	NodeId       string  `json:"nodeId"`
	Layers       [3]bool `json:"layers"`       // [LOW, MEDIUM, HIGH] — which layers have active subscribers
	HasBackupSub bool    `json:"hasBackupSub"` // true if any consumer on this node uses the backup codec
	BackupLayers [3]bool `json:"backupLayers"` // per-layer state for backup codec consumers
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
	Type      string // "participant", "group", "msg", "tracksub", "state", "lease", "track", "dataTrack"
	Operation store.Operation
	Key       string // the sub-key (sid, groupId, msgId, participantSid, stateKey)
	SubKey    string // the second sub-key (e.g. trackSid for track/dataTrack)

	Participant *proto.ParticipantInfo
	Group       *proto.GroupInfo
	Message     *proto.MessageNotification
	TrackSub    *TrackSubInfo
	StateValue  string // populated for "state" Put events
	Track       *proto.TrackInfo
	DataTrack   *proto.DataTrackInfo
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
	// Key format: room.<roomName>.<type>.<id> or room.<roomName>.<type>.<participantSid>.<trackSid>
	key := entry.Key()
	prefix := s.prefix() + "."
	if !strings.HasPrefix(key, prefix) {
		return RoomStoreEvent{}, false
	}
	rest := key[len(prefix):]
	parts := strings.SplitN(rest, ".", 3)

	evt := RoomStoreEvent{
		Type:      parts[0],
		Operation: entry.Operation(),
	}
	if len(parts) > 1 {
		evt.Key = parts[1]
	}
	if len(parts) > 2 {
		evt.SubKey = parts[2]
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
			var msg proto.MessageNotification
			if json.Unmarshal(entry.Value(), &msg) == nil {
				evt.Message = &msg
			}
		case "tracksub":
			var info TrackSubInfo
			if json.Unmarshal(entry.Value(), &info) == nil {
				evt.TrackSub = &info
			}
		case "state":
			evt.StateValue = string(entry.Value())
		case "track":
			var info proto.TrackInfo
			if json.Unmarshal(entry.Value(), &info) == nil {
				evt.Track = &info
			}
		case "dataTrack":
			var info proto.DataTrackInfo
			if json.Unmarshal(entry.Value(), &info) == nil {
				evt.DataTrack = &info
			}
		}
	}

	return evt, true
}
