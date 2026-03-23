package rtc

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/discovery"
	"github.com/axonkit/axonkit/pkg/media"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/routing"
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

	rib      *routing.RIB
	fib      *routing.FIB
	finder   *discovery.Finder
	mediaMgr *media.Manager

	kv     store.Store // shared KV for cross-node room state sync
	nodeID string

	logger *slog.Logger
}

type RoomManagerParams struct {
	Config       config.RoomConfig
	RIB          *routing.RIB
	FIB          *routing.FIB
	Finder       *discovery.Finder
	MediaManager *media.Manager
	KV           store.Store
	NodeID       string
}

func NewRoomManager(params RoomManagerParams) *RoomManager {
	return &RoomManager{
		rooms:      make(map[string]*Room),
		roomsBySid: make(map[string]*Room),
		config:     params.Config,
		rib:        params.RIB,
		fib:        params.FIB,
		finder:     params.Finder,
		mediaMgr:   params.MediaManager,
		kv:         params.KV,
		nodeID:     params.NodeID,
		logger:     slog.Default().With("component", "RoomManager"),
	}
}

// Start starts the RoomManager background tasks.
func (rm *RoomManager) Start(ctx context.Context) {
	go rm.watchLeases(ctx)
	go rm.runRoomStateSweeper(ctx)
}

func (rm *RoomManager) watchLeases(ctx context.Context) {
	watcher, err := rm.kv.Watch(ctx, "room.*.lease")
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
				// Key format: room.<roomName>.lease
				keyParts := strings.Split(entry.Key(), ".")
				if len(keyParts) != 3 {
					continue
				}
				roomName := keyParts[1]

				time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
				purgeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				if _, err := GetRoomMeta(rm.kv, roomName); err == nil {
					PurgeRoomKV(purgeCtx, rm.kv, roomName)
					rm.logger.Info("Purged room KV after lease delete", "room", roomName)
				}
				cancel()
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
		if _, leaseErr := rm.kv.Get(ctx, RoomLeaseKey(roomName)); leaseErr == nil {
			return
		}
		if time.Since(time.UnixMilli(meta.CreationTime)) < OrphanRoomMetaGrace {
			return
		}
	}
	PurgeRoomKV(ctx, rm.kv, roomName)
	rm.logger.Info("Purged orphan room KV (meta without lease)", "room", roomName)
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

	room := NewRoom(RoomParams{
		Sid:          meta.Sid,
		Name:         meta.Name,
		Config:       meta.Config,
		CreationTime: time.UnixMilli(meta.CreationTime),
		Metadata:     meta.Metadata,
	})
	room.SetOnClose(func(r *Room) {
		rm.removeRoom(r)
	})
	rm.setupCascadeCallbacks(room)
	room.SetupStore(rm.kv, rm.nodeID)

	rm.rooms[name] = room
	rm.roomsBySid[meta.Sid] = room
	rm.logger.Info("Room created", "name", name, "sid", meta.Sid)

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
	defer rm.mu.Unlock()

	delete(rm.rooms, room.name)
	delete(rm.roomsBySid, room.sid)
	rm.logger.Info("Room removed", "name", room.name, "sid", room.sid)
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
	unadvertise := func(sid string) {
		streamID := types.StreamID(sid)
		if rm.rib != nil {
			if err := rm.rib.StopAdvertise(streamID); err != nil {
				rm.logger.Error("Failed to stop advertise", "stream_id", streamID, "error", err)
			}
		}
		if rm.mediaMgr != nil {
			rm.mediaMgr.UnregisterProducer(streamID)
		}
	}

	room.cascade = CascadeCallbacks{
		OnTrackPublished: func(sid string, producer *mediasoup.Producer) {
			streamID := types.StreamID(sid)
			if rm.mediaMgr != nil {
				rm.mediaMgr.RegisterProducer(streamID, &media.LocalProducer{
					StreamID: streamID,
					Router:   rm.mediaMgr.GetRouter(streamID.RouterID()),
					Producer: producer,
				})
			}
			if rm.rib != nil {
				if err := rm.rib.StartAdvertise(routing.AdvertiseOptions{
					StreamID: streamID,
					IsOrigin: true,
				}); err != nil {
					rm.logger.Error("Failed to advertise stream", "stream_id", streamID, "error", err)
				}
			}
		},
		OnTrackUnpublished: unadvertise,
		OnDataTrackPublished: func(sid string, dataProducer *mediasoup.DataProducer) {
			streamID := types.StreamID(sid)
			if rm.mediaMgr != nil {
				rm.mediaMgr.RegisterProducer(streamID, &media.LocalProducer{
					StreamID:     streamID,
					Router:       rm.mediaMgr.GetRouter(streamID.RouterID()),
					DataProducer: dataProducer,
				})
			}
			if rm.rib != nil {
				if err := rm.rib.StartAdvertise(routing.AdvertiseOptions{
					StreamID: streamID,
					IsOrigin: true,
				}); err != nil {
					rm.logger.Error("Failed to advertise stream", "stream_id", streamID, "error", err)
				}
			}
		},
		OnDataTrackUnpublished: unadvertise,
		EnsureStream: func(sid string) error {
			streamID := types.StreamID(sid)
			// Local producer — no FIB piping needed
			if rm.mediaMgr != nil && rm.mediaMgr.GetLocalProducer(streamID) != nil {
				return nil
			}
			if rm.fib == nil || rm.finder == nil {
				return nil
			}
			if _, _, err := rm.fib.Subscribe(streamID, rm.finder.FindPath); err != nil {
				return fmt.Errorf("fib subscribe failed for %s: %w", streamID, err)
			}
			return nil
		},
		ReleaseStream: func(sid string) {
			streamID := types.StreamID(sid)
			// Only release remote streams managed by FIB
			if rm.fib == nil {
				return
			}
			if rm.mediaMgr != nil && rm.mediaMgr.GetLocalProducer(streamID) != nil {
				return
			}
			rm.fib.Unsubscribe(streamID)
		},
		GetLocalProducer: func(sid string) *media.LocalProducer {
			if rm.mediaMgr == nil {
				return nil
			}
			return rm.mediaMgr.GetLocalProducer(types.StreamID(sid))
		},
		GetRouter: func(routerId string) *mediasoup.Router {
			if rm.mediaMgr == nil {
				return nil
			}
			for _, r := range rm.mediaMgr.Routers() {
				if r.Id() == routerId {
					return r
				}
			}
			return nil
		},
	}
}

func (rm *RoomManager) Close() {
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
