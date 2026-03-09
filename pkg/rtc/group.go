package rtc

import (
	"sync"

	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/rs/xid"
)

// Group represents a breakout room within a Room.
// Participants in the same group subscribe to each other's tracks
// and use the group's permissions instead of the room-level defaults.
type Group struct {
	sid         string
	name        string
	permissions *proto.ParticipantPermission

	participants map[string]bool // participantSid -> present
	mu           sync.RWMutex
}

func NewGroup(name string, permissions *proto.ParticipantPermission) *Group {
	return &Group{
		sid:          "gr_" + xid.New().String(),
		name:         name,
		permissions:  permissions,
		participants: make(map[string]bool),
	}
}

func (g *Group) SID() string { return g.sid }

func (g *Group) ToProto() *proto.GroupInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return &proto.GroupInfo{
		GroupId:          g.sid,
		Name:             g.name,
		Permissions:      g.permissions,
		ParticipantCount: len(g.participants),
	}
}

func (g *Group) AddParticipant(sid string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.participants[sid] = true
}

func (g *Group) RemoveParticipant(sid string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.participants, sid)
}

func (g *Group) HasParticipant(sid string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.participants[sid]
}

func (g *Group) ParticipantSids() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	sids := make([]string, 0, len(g.participants))
	for sid := range g.participants {
		sids = append(sids, sid)
	}
	return sids
}

func (g *Group) ParticipantCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.participants)
}
