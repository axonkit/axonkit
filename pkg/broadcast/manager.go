package broadcast

import (
	"log/slog"
	"sync"

	"github.com/axonkit/axonkit/pkg/rtc/whip"
	"github.com/rs/xid"
)

// ManagerParams groups the ambient dependencies injected once at
// HTTPServer startup. Broadcasts are created cheaply against these.
type ManagerParams struct {
	// Rooms is typically *rtc.RoomManager.
	Rooms RoomLookup
	// WhipService is the shared whip.Service used by HTTPServer for
	// per-room WHIP ingest; broadcast reuses it to materialise the
	// mix ingest transport + producers without registering them as a
	// room participant.
	WhipService *whip.Service
	// Logger is the base logger for the manager itself; per-broadcast
	// loggers are derived from it with id tagged on.
	Logger *slog.Logger
}

// Manager tracks live Broadcasts keyed by their public id. A single
// global Manager lives on HTTPServer. Concurrency: the registry uses a
// plain RWMutex — the hot path is not in here (each Broadcast owns its
// own RTP loop), so finer-grained locking is not worth the complexity.
type Manager struct {
	params ManagerParams

	mu    sync.RWMutex
	items map[string]*Broadcast
}

// NewManager constructs an empty manager. Logger defaults to slog.Default.
func NewManager(params ManagerParams) *Manager {
	if params.Logger == nil {
		params.Logger = slog.Default().With("component", "broadcast.Manager")
	}
	return &Manager{params: params, items: make(map[string]*Broadcast)}
}

// CreateRequest is the HTTP-facing payload decoded by
// POST /rtc/broadcast. Kept here (instead of on HTTPServer) so the
// types package layering matches the handler layer: handlers in
// pkg/server, business rules in pkg/broadcast.
type CreateRequest struct {
	Room                string        `json:"room"`
	ParticipantIdentity string        `json:"participantIdentity"`
	Destinations        []Destination `json:"destinations"`
}

// Validate runs the cheap static checks a POST body must pass.
func (r *CreateRequest) Validate() error {
	if r.Room == "" {
		return ErrRoomNotFound
	}
	if r.ParticipantIdentity == "" {
		return ErrIdentityNotPublishing
	}
	return Validate(r.Destinations)
}

// Create assembles a Broadcast, validates the live identity is
// publishing, registers it in the manager, then calls Start. Returns
// the public Broadcast handle on success.
//
// Errors mirror Start (ErrRoomNotFound / ErrIdentityNotPublishing)
// plus validation errors from the CreateRequest.
func (m *Manager) Create(req *CreateRequest) (*Broadcast, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	id := "bc_" + xid.New().String()
	b := New(Params{
		ID:           id,
		Room:         req.Room,
		LiveIdentity: req.ParticipantIdentity,
		Destinations: req.Destinations,
		Rooms:        m.params.Rooms,
		WhipService:  m.params.WhipService,
		Logger:       m.params.Logger.With("id", id),
	})
	b.setOnTerminate(func(TerminationReason) {
		m.mu.Lock()
		delete(m.items, id)
		m.mu.Unlock()
	})

	if err := b.Start(); err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.items[id] = b
	m.mu.Unlock()
	return b, nil
}

// Get returns the broadcast with the given id, or nil.
func (m *Manager) Get(id string) *Broadcast {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.items[id]
}

// Delete tears down the broadcast identified by id. Returns ErrNotFound
// if no such broadcast exists. Idempotent: subsequent calls to
// Broadcast.Terminate are no-ops.
func (m *Manager) Delete(id string) error {
	b := m.Get(id)
	if b == nil {
		return ErrNotFound
	}
	b.Terminate()
	return nil
}

// Close tears down every active broadcast. Called during HTTPServer
// shutdown so external sinks see a clean disconnect instead of a
// half-open connection.
func (m *Manager) Close() {
	m.mu.Lock()
	items := make([]*Broadcast, 0, len(m.items))
	for _, b := range m.items {
		items = append(items, b)
	}
	m.mu.Unlock()
	for _, b := range items {
		b.Terminate()
	}
}
