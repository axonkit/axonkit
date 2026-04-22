package rtc

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/media"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/store"
	"github.com/axonkit/axonkit/pkg/types"
	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/rs/xid"
)

// RoomManager manages the lifecycle of rooms on this node.
// Each node maintains its own set of rooms; participants in the same
// logical room can be on different nodes (cascading via FIB).
type RoomManager struct {
	rooms      map[string]*Room // name -> room
	roomsBySid map[string]*Room // sid -> room
	mu         sync.RWMutex
	config     config.RoomConfig

	mediaMgr *media.Manager

	kv           store.Store // ROOM_STATE: meta, participants, tracks, …
	leaseKV      store.Store // ROOM_LEASE: one key per room name (liveness)
	nodeID       string
	sharedRouter bool

	backupCodecFwdMu sync.RWMutex
	backupCodecFwd   func(publisherSid, trackSid, publisherNodeId string) error

	forwardersMu sync.Mutex
	forwarders   map[string]*CrossRoomForwarder

	logger *slog.Logger
}

type RoomManagerParams struct {
	Config       config.RoomConfig
	MediaManager *media.Manager
	KV           store.Store
	LeaseKV      store.Store
	NodeID       string
	SharedRouter bool
}

func NewRoomManager(params RoomManagerParams) *RoomManager {
	return &RoomManager{
		rooms:        make(map[string]*Room),
		roomsBySid:   make(map[string]*Room),
		config:       params.Config,
		mediaMgr:     params.MediaManager,
		kv:           params.KV,
		leaseKV:      params.LeaseKV,
		nodeID:       params.NodeID,
		sharedRouter: params.SharedRouter,
		forwarders:   make(map[string]*CrossRoomForwarder),
		logger:       slog.Default().With("component", "RoomManager"),
	}
}

// SetRequestBackupCodec sets the NATS (or in-process) forwarder used when a
// remote subscriber needs the publisher's node to send SubscribedQualityChanged.
// Call before any room is created; safe for use from Server wiring only.
func (rm *RoomManager) SetRequestBackupCodec(fn func(publisherSid, trackSid, publisherNodeId string) error) {
	rm.backupCodecFwdMu.Lock()
	rm.backupCodecFwd = fn
	rm.backupCodecFwdMu.Unlock()
}

// RequestBackupCodecOnPublisher runs backup codec signaling for a track on this node.
func (rm *RoomManager) RequestBackupCodecOnPublisher(publisherSid, trackSid string) error {
	p := rm.GetParticipantBySid(publisherSid)
	if p == nil {
		return fmt.Errorf("publisher not found: %s", publisherSid)
	}
	return p.RequestBackupCodecForTrack(trackSid)
}

// Start starts the RoomManager background tasks.
func (rm *RoomManager) Start(ctx context.Context) {
	go rm.runLeaseRefreshLoop(ctx)
	go rm.watchLeases(ctx)
	go rm.runRoomStateSweeper(ctx)
}

func (rm *RoomManager) watchLeases(ctx context.Context) {
	watcher, err := rm.leaseKV.Watch(ctx, ">")
	if err != nil {
		rm.logger.Error("Failed to watch room leases", "error", err)
		return
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-watcher.Updates():
			if !ok {
				return
			}
			if entry == nil {
				continue
			}
			if entry.Operation() == store.OpDelete {
				roomName := entry.Key()
				if roomName == "" {
					continue
				}
				time.AfterFunc(time.Duration(rand.Intn(1000))*time.Millisecond, func() {
					if _, err := GetRoomMeta(rm.kv, roomName); err == nil {
						purgeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						PurgeRoomKV(purgeCtx, rm.kv, roomName)
						cancel()
						rm.logger.Info("Purged room KV after lease delete", "room", roomName)
					}
				})
			}
		}
	}
}

func (rm *RoomManager) runRoomStateSweeper(ctx context.Context) {
	ticker := time.NewTicker(RoomStateSweeperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rm.sweepOrphanRoomMeta(ctx)
		}
	}
}

// sweepOrphanRoomMeta removes rooms where meta exists but lease does not.
// Any node may perform the delete; jitter + re-check reduces duplicate work.
// A grace period after meta CreationTime avoids racing initial lease Create.
func (rm *RoomManager) sweepOrphanRoomMeta(parentCtx context.Context) {
	sweepCtx, cancel := context.WithTimeout(parentCtx, 45*time.Second)
	defer cancel()

	// Join restricts room names to one segment (ValidateRoomName), so
	// room.*.meta is a valid JetStream filter and lists only meta keys.
	keys, err := rm.kv.Keys(sweepCtx, "room.*.meta")
	if err != nil {
		rm.logger.Warn("Room state sweeper: list keys", "error", err)
		return
	}
	for _, k := range keys {
		roomName, ok := MetaKeyRoomName(k)
		if !ok {
			continue
		}
		rm.tryPurgeMetaWithoutLease(sweepCtx, roomName)
	}
}

func (rm *RoomManager) tryPurgeMetaWithoutLease(ctx context.Context, roomName string) {
	for attempt := 0; attempt < 2; attempt++ {
		if attempt == 1 {
			time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
		}
		meta, err := GetRoomMeta(rm.kv, roomName)
		if err != nil || meta == nil {
			return
		}
		if _, leaseErr := rm.leaseKV.Get(ctx, RoomLeaseStoreKey(roomName)); leaseErr == nil {
			return
		}
		if time.Since(time.UnixMilli(meta.CreationTime)) < OrphanRoomMetaGrace {
			return
		}
	}
	PurgeRoomKV(ctx, rm.kv, roomName)
	rm.logger.Info("Purged orphan room KV", "room", roomName)
}

// GetOrCreateRoom returns an existing room by name, or creates one using
// the provided creation request settings (with server defaults as fallback).
// The room's identity and configuration are stored in the shared KV so that
// every node in the cluster creates an identical local Room instance.
func (rm *RoomManager) GetOrCreateRoom(name string, maxParticipants int, metadata string) *Room {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if room, ok := rm.rooms[name]; ok {
		return room
	}

	// Try to read room state from the shared store (another node may have
	// created this room already).
	existing, _ := GetRoomMeta(rm.kv, name)

	var meta *RoomMeta
	if existing != nil {
		meta = existing
	} else {
		cfg := rm.config
		if maxParticipants > 0 {
			cfg.MaxParticipants = maxParticipants
		}
		meta = &RoomMeta{
			Sid:          "rm_" + xid.New().String(),
			Name:         name,
			Metadata:     metadata,
			CreationTime: time.Now().UnixMilli(),
			Config:       cfg,
		}
		PutRoomMeta(rm.kv, meta)
	}

	routerIndex := rm.routerIndexForRoom(name)
	room := NewRoom(RoomParams{
		Sid:          meta.Sid,
		Name:         meta.Name,
		Config:       meta.Config,
		CreationTime: time.UnixMilli(meta.CreationTime),
		Metadata:     meta.Metadata,
		NodeID:       rm.nodeID,
		RoomStore:    NewRoomStore(rm.kv, name, rm.nodeID),
		SharedRouter: rm.sharedRouter,
		RouterIndex:  routerIndex,
	})
	room.SetOnClose(func(r *Room) {
		rm.removeRoom(r)
	})
	rm.setupCascadeCallbacks(room)

	rm.rooms[name] = room
	rm.roomsBySid[meta.Sid] = room

	rm.logger.Info("Room created", "name", name, "sid", meta.Sid)

	go rm.refreshRoomLease(context.Background(), name)

	return room
}

func (rm *RoomManager) GetRoom(name string) *Room {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.rooms[name]
}

func (rm *RoomManager) GetRoomBySid(sid string) *Room {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.roomsBySid[sid]
}

func (rm *RoomManager) removeRoom(room *Room) {
	rm.mu.Lock()
	delete(rm.rooms, room.name)
	delete(rm.roomsBySid, room.sid)
	rm.mu.Unlock()

	rm.logger.Info("Room removed", "name", room.name, "sid", room.sid)
}

func (rm *RoomManager) runLeaseRefreshLoop(ctx context.Context) {
	ticker := time.NewTicker(RoomLeaseHeartbeatInterval)
	defer ticker.Stop()
	rm.refreshAllRoomLeases(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rm.refreshAllRoomLeases(ctx)
		}
	}
}

func (rm *RoomManager) refreshRoomLease(ctx context.Context, roomName string) {
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rm.leaseKV.Put(c, RoomLeaseStoreKey(roomName), []byte(rm.nodeID)); err != nil {
		rm.logger.Error("Room lease refresh failed", "room", roomName, "error", err)
	}
}

func (rm *RoomManager) refreshAllRoomLeases(ctx context.Context) {
	rm.mu.RLock()
	names := make([]string, 0, len(rm.rooms))
	for n := range rm.rooms {
		names = append(names, n)
	}
	rm.mu.RUnlock()
	for _, name := range names {
		rm.refreshRoomLease(ctx, name)
	}
}

// GetParticipantBySid searches across all rooms for a participant by SID.
func (rm *RoomManager) GetParticipantBySid(sid string) *Participant {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	for _, room := range rm.rooms {
		if p := room.GetParticipant(sid); p != nil {
			return p
		}
	}
	return nil
}

func (rm *RoomManager) setupCascadeCallbacks(room *Room) {
	room.cascade = CascadeCallbacks{
		OnTrackPublished: func(sid string, producer *mediasoup.Producer) {
			if rm.mediaMgr != nil {
				rm.mediaMgr.RegisterProducer(types.StreamID(sid), producer)
			}
		},
		OnTrackUnpublished: func(sid string) {
			if rm.mediaMgr != nil {
				rm.mediaMgr.UnregisterProducer(types.StreamID(sid))
			}
		},
		OnDataTrackPublished: func(sid string, dataProducer *mediasoup.DataProducer) {
			if rm.mediaMgr != nil {
				rm.mediaMgr.RegisterDataProducer(types.StreamID(sid), dataProducer)
			}
		},
		OnDataTrackUnpublished: func(sid string) {
			if rm.mediaMgr != nil {
				rm.mediaMgr.UnregisterProducer(types.StreamID(sid))
			}
		},
		EnsureStream: func(sid string) (*media.LocalProducer, error) {
			if rm.mediaMgr == nil {
				return nil, nil
			}
			lp, err := rm.mediaMgr.EnsureProducer(types.StreamID(sid))
			if err != nil {
				return nil, fmt.Errorf("ensure producer for %s: %w", sid, err)
			}
			return lp, nil
		},
		ReleaseStream: func(sid string) {
			if rm.mediaMgr != nil {
				rm.mediaMgr.ReleaseProducer(types.StreamID(sid))
			}
		},
		GetProducer: func(sid string) *media.LocalProducer {
			if rm.mediaMgr == nil {
				return nil
			}
			return rm.mediaMgr.GetProducer(types.StreamID(sid))
		},
		GetRouter: func(routerId string) *mediasoup.Router {
			if rm.mediaMgr == nil {
				return nil
			}
			return rm.mediaMgr.RouterForId(routerId)
		},
		RequestBackupCodec: func(publisherSid, trackSid, publisherNodeId string) error {
			rm.backupCodecFwdMu.RLock()
			fn := rm.backupCodecFwd
			rm.backupCodecFwdMu.RUnlock()
			if fn == nil {
				return fmt.Errorf("request backup codec forwarder not configured")
			}
			return fn(publisherSid, trackSid, publisherNodeId)
		},
	}
}

// routerIndexForRoom returns a deterministic router index for a room name
// so that all participants in the same room land on the same worker when
// SharedRouter is enabled.
func (rm *RoomManager) routerIndexForRoom(roomName string) int {
	if rm.mediaMgr == nil {
		return 0
	}
	n := len(rm.mediaMgr.Routers())
	if n == 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(roomName))
	return int(h.Sum32() % uint32(n))
}

func (rm *RoomManager) Close() {
	rm.forwardersMu.Lock()
	fwds := make([]*CrossRoomForwarder, 0, len(rm.forwarders))
	for _, f := range rm.forwarders {
		fwds = append(fwds, f)
	}
	rm.forwarders = make(map[string]*CrossRoomForwarder)
	rm.forwardersMu.Unlock()
	for _, f := range fwds {
		f.Close()
	}

	rm.mu.Lock()
	rooms := make([]*Room, 0, len(rm.rooms))
	for _, r := range rm.rooms {
		rooms = append(rooms, r)
	}
	rm.mu.Unlock()

	for _, r := range rooms {
		r.Close(proto.DisconnectReason_SERVER_SHUTDOWN)
	}
}

// ============================================================
// Cross-room participant forwarding (co-host support)
// ============================================================

// ForwardParticipant mirrors a participant from srcRoom into dstRoom using
// the shared RoomStore KV and returns the ParticipantInfo written into the
// destination room. The returned Participant keeps the source Identity (and
// Name / Metadata / Attributes), so subscribers in dstRoom see the guest as
// an ordinary participant. Use RemoveParticipantFromRoom(dstRoom, identity)
// to tear the forward down.
//
// The synthetic participant uses a deterministic SID derived from
// (srcParticipantSid, dstRoom) so repeated invocations converge on the
// same entry (idempotent).
//
// Media is not re-encoded: subscribers in dstRoom pick up the forwarded
// tracks via the normal auto-subscribe path and pipe the original producers
// through FIB + media.Manager. Neither srcRoom nor dstRoom needs to be
// locally hosted on this node; the shared KV is the source of truth.
func (rm *RoomManager) ForwardParticipant(srcRoom, srcIdentity, dstRoom string) (*proto.ParticipantInfo, error) {
	if err := ValidateRoomName(srcRoom); err != nil {
		return nil, fmt.Errorf("invalid srcRoom: %w", err)
	}
	if err := ValidateRoomName(dstRoom); err != nil {
		return nil, fmt.Errorf("invalid dstRoom: %w", err)
	}
	if srcRoom == dstRoom {
		return nil, fmt.Errorf("srcRoom and dstRoom must differ")
	}
	if srcIdentity == "" {
		return nil, fmt.Errorf("srcIdentity is required")
	}

	srcStore := NewRoomStore(rm.kv, srcRoom, rm.nodeID)
	dstStore := NewRoomStore(rm.kv, dstRoom, rm.nodeID)

	var srcInfo *proto.ParticipantInfo
	for _, p := range srcStore.GetAllParticipants() {
		if p.Identity != srcIdentity || p.State == proto.ParticipantInfo_DISCONNECTED {
			continue
		}
		srcInfo = p
		break
	}
	if srcInfo == nil {
		return nil, fmt.Errorf("participant %q not found or disconnected in %q", srcIdentity, srcRoom)
	}

	key := forwarderKey(dstRoom, srcInfo.Sid)

	rm.forwardersMu.Lock()
	if _, exists := rm.forwarders[key]; exists {
		rm.forwardersMu.Unlock()
		existing := dstStore.GetParticipant(srcInfo.Sid)
		if existing == nil {
			return nil, fmt.Errorf("forwarder already registered but participant missing in dstStore")
		}
		return existing, nil
	}
	rm.forwardersMu.Unlock()

	fwd := NewCrossRoomForwarder(CrossRoomForwarderParams{
		SrcStore:       srcStore,
		DstStore:       dstStore,
		SrcRoom:        srcRoom,
		DstRoom:        dstRoom,
		SrcParticipant: srcInfo,
		OnClose: func(k string) {
			rm.forwardersMu.Lock()
			delete(rm.forwarders, k)
			rm.forwardersMu.Unlock()
		},
	})
	if err := fwd.Start(context.Background()); err != nil {
		return nil, err
	}

	rm.forwardersMu.Lock()
	rm.forwarders[key] = fwd
	rm.forwardersMu.Unlock()

	info := dstStore.GetParticipant(srcInfo.Sid)
	if info == nil {
		info = &proto.ParticipantInfo{
			Sid:      srcInfo.Sid,
			Identity: srcInfo.Identity,
			Name:     srcInfo.Name,
		}
	}
	return info, nil
}

// RemoveParticipantFromRoom removes a participant from a room by identity.
// A single KV write uniformly handles both real and forwarded participants:
//
//   - Real (locally joined) participant: the hosting node's RoomStore watch
//     reacts to DISCONNECTED and closes the local session (see
//     Room.handleStoreParticipantEvent).
//   - Forwarded participant (mirror created by ForwardParticipant): the
//     forwarder's destination-room watcher reacts and tears itself down
//     (see CrossRoomForwarder.runDstWatcher). The source participant in
//     the origin room is untouched.
//
// This avoids any explicit NodeRPC: the shared KV is the source of truth
// and every owning node reacts to the change. The call is idempotent —
// unknown / already-disconnected identities return nil.
func (rm *RoomManager) RemoveParticipantFromRoom(roomName, identity string) error {
	if err := ValidateRoomName(roomName); err != nil {
		return fmt.Errorf("invalid room: %w", err)
	}
	if identity == "" {
		return fmt.Errorf("identity is required")
	}

	roomStore := NewRoomStore(rm.kv, roomName, rm.nodeID)

	var target *proto.ParticipantInfo
	for _, p := range roomStore.GetAllParticipants() {
		if p.Identity != identity || p.State == proto.ParticipantInfo_DISCONNECTED {
			continue
		}
		target = p
		break
	}
	if target == nil {
		return nil
	}

	kicked := cloneParticipantInfo(target)
	kicked.State = proto.ParticipantInfo_DISCONNECTED
	kicked.DisconnectReason = proto.DisconnectReason_PARTICIPANT_REMOVED
	roomStore.PutParticipant(kicked)
	rm.logger.Info("Participant kick requested via store",
		"room", roomName, "identity", identity, "sid", target.Sid, "nodeId", target.NodeId,
		"forwarded", isForwardedParticipant(target))
	return nil
}

// isForwardedParticipant reports whether the given ParticipantInfo was
// produced by ForwardParticipant (as opposed to an ordinary local join).
func isForwardedParticipant(p *proto.ParticipantInfo) bool {
	if p == nil || p.Attributes == nil {
		return false
	}
	_, ok := p.Attributes[ForwardedFromRoomAttr]
	return ok
}

// ActiveForwarders returns the process-local forwarder keys of every
// forwarder hosted by this node. The values are opaque bookkeeping keys
// (see forwarderKey); primarily used for diagnostics and tests.
func (rm *RoomManager) ActiveForwarders() []string {
	rm.forwardersMu.Lock()
	defer rm.forwardersMu.Unlock()
	out := make([]string, 0, len(rm.forwarders))
	for k := range rm.forwarders {
		out = append(out, k)
	}
	return out
}
