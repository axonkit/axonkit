package rtc

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/discovery"
	"github.com/axonkit/axonkit/pkg/media"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/routing"
	"github.com/axonkit/axonkit/pkg/store"
	"github.com/axonkit/axonkit/pkg/types"
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

	room := NewRoom(meta.Sid, meta.Name, meta.Config, time.UnixMilli(meta.CreationTime))
	room.metadata = meta.Metadata
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
	advertise := func(sid string) {
		streamID := types.StreamID(sid)
		if rm.mediaMgr != nil {
			rm.mediaMgr.RegisterProducer(streamID, &media.LocalProducer{
				StreamID: streamID,
				RouterId: streamID.RouterID(),
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
	}
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
		OnTrackPublished:       advertise,
		OnTrackUnpublished:     unadvertise,
		OnDataTrackPublished:   advertise,
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
