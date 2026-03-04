package rtc

import (
	"log/slog"
	"sync"

	"github.com/jiyeyuran/omnistream/pkg/config"
	"github.com/jiyeyuran/omnistream/pkg/discovery"
	"github.com/jiyeyuran/omnistream/pkg/routing"
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

	fib    *routing.FIB
	finder *discovery.Finder

	logger *slog.Logger
}

type RoomManagerParams struct {
	Config config.RoomConfig
	FIB    *routing.FIB
	Finder *discovery.Finder
}

func NewRoomManager(params RoomManagerParams) *RoomManager {
	return &RoomManager{
		rooms:      make(map[string]*Room),
		roomsBySid: make(map[string]*Room),
		config:     params.Config,
		fib:        params.FIB,
		finder:     params.Finder,
		logger:     slog.Default().With("component", "RoomManager"),
	}
}

// GetOrCreateRoom returns an existing room by name, or creates one using
// the provided creation request settings (with server defaults as fallback).
func (rm *RoomManager) GetOrCreateRoom(name string, maxParticipants int, metadata string) *Room {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if room, ok := rm.rooms[name]; ok {
		return room
	}

	cfg := rm.config
	if maxParticipants > 0 {
		cfg.MaxParticipants = maxParticipants
	}

	sid := xid.New().String()
	room := NewRoom(sid, name, cfg)
	room.metadata = metadata
	room.SetOnClose(func(r *Room) {
		rm.removeRoom(r)
	})

	rm.rooms[name] = room
	rm.roomsBySid[sid] = room
	rm.logger.Info("Room created", "name", name, "sid", sid)

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

func (rm *RoomManager) Close() {
	rm.mu.Lock()
	rooms := make([]*Room, 0, len(rm.rooms))
	for _, r := range rm.rooms {
		rooms = append(rooms, r)
	}
	rm.mu.Unlock()

	for _, r := range rooms {
		r.Close()
	}
}
