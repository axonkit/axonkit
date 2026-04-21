package whip

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/rs/xid"
)

// Mode distinguishes a publish (WHIP) from a subscribe (WHEP) session.
type Mode string

const (
	ModeWHIP Mode = "whip"
	ModeWHEP Mode = "whep"
)

// DefaultICEDisconnectedTimeout is how long we keep a Session alive after the
// mediasoup Transport reports IceStateDisconnected without recovering.
// Chosen to tolerate common short-lived network glitches (roaming, Wi-Fi
// handoff) while still reclaiming resources.
const DefaultICEDisconnectedTimeout = 30 * time.Second

// DefaultKeepaliveInterval is the cadence of the session supervisor: on every
// tick we (a) check the ICE state for watchdog expiry and (b) give any
// attached KeepaliveFunc a chance to refresh external TTL records (KV etc).
// Must be shorter than the external TTL for the refresh to be effective.
const DefaultKeepaliveInterval = 10 * time.Second

// Session bundles the mediasoup resources backing one WHIP/WHEP session.
// Lifetime: Session is created by Service.CreateWHIP / CreateWHEP on POST,
// mutated by HandlePatch on PATCH, and closed by HandleDelete on DELETE —
// or autonomously by the supervisor goroutine when the Transport has been
// disconnected for longer than ICEDisconnectedTimeout.
type Session struct {
	ID        string
	Mode      Mode
	Transport *mediasoup.Transport
	Router    *mediasoup.Router
	Producers []*mediasoup.Producer
	Consumers []*mediasoup.Consumer

	// RoomName is set when the session is also attached to a room participant.
	// The higher-level server code owns the participant lifecycle — see
	// OnClose below to hook into session teardown.
	RoomName string

	// Published/subscribed track sids, stable across PATCH calls.
	TrackSids []string

	// ETag = IceUfrag of the current ICE session; rotated on ICE restart.
	ETag string

	CreatedAt time.Time

	// OnClose is invoked (from Close) after Transport is torn down. Used by
	// the server glue to clean up a linked Participant / Room and any KV
	// entries without introducing a cycle between packages.
	OnClose func()

	// ICEDisconnectedTimeout can be tuned per-session; zero means "use the
	// package default" (DefaultICEDisconnectedTimeout).
	ICEDisconnectedTimeout time.Duration

	// KeepaliveInterval overrides the default supervisor tick rate. Zero
	// means "use DefaultKeepaliveInterval".
	KeepaliveInterval time.Duration

	// Keepalive, if non-nil, is invoked on every supervisor tick while the
	// session is alive — typically refreshing a KV TTL so other nodes
	// continue to see the resource. It must be quick and non-blocking; the
	// supervisor calls it synchronously on its own goroutine.
	Keepalive func()

	closeOnce sync.Once
	mu        sync.Mutex

	// disconnectedAt records the last time the transport transitioned into
	// IceStateDisconnected. A zero value means "currently connected (or
	// never disconnected)"; the supervisor compares against it each tick.
	disconnectedAt time.Time

	// done signals the supervisor goroutine to exit. Closed at most once
	// (by Close, guarded via closeOnce).
	done chan struct{}

	// onExpire is the callback the supervisor invokes when the ICE
	// disconnection deadline elapses. If nil, the supervisor calls
	// Close directly.
	onExpire func(id string)

	logger *slog.Logger
}

// Start kicks off the session's supervisor goroutine: it subscribes to the
// mediasoup Transport's ICE state change events, arms/disarms the internal
// disconnection deadline, and ticks the external Keepalive hook. Safe to
// call exactly once per Session. onExpire, if non-nil, is invoked after the
// ICE disconnection window elapses (used by Service to remove the session
// from its registry); if nil, the supervisor calls Close directly.
//
// The single goroutine model is intentional: it is the only writer of
// disconnectedAt and the only driver of TTL refresh + watchdog, so no
// additional synchronization is needed between those concerns.
func (s *Session) Start(onExpire func(id string)) {
	if s.logger == nil {
		s.logger = slog.Default().With("component", "whip.Session")
	}
	s.done = make(chan struct{})
	s.onExpire = onExpire

	// ICE state change handler only flips disconnectedAt — the supervisor
	// goroutine does the actual tearing down on the next tick so that we
	// never hold two goroutines racing for the Close path.
	if s.Transport == nil {
		go s.supervise()
		return
	}
	s.Transport.OnIceStateChange(func(state mediasoup.IceState) {
		s.mu.Lock()
		defer s.mu.Unlock()
		switch state {
		case mediasoup.IceStateDisconnected:
			if s.disconnectedAt.IsZero() {
				s.disconnectedAt = time.Now()
				if s.logger != nil {
					s.logger.Warn("WHIP/WHEP ICE disconnected, supervisor armed",
						"sessionId", s.ID, "mode", s.Mode)
				}
			}
		case mediasoup.IceStateConnected, mediasoup.IceStateCompleted:
			if !s.disconnectedAt.IsZero() {
				s.disconnectedAt = time.Time{}
				if s.logger != nil {
					s.logger.Info("WHIP/WHEP ICE recovered",
						"sessionId", s.ID, "mode", s.Mode)
				}
			}
		}
	})

	go s.supervise()
}

// supervise is the single goroutine that drives (a) the ICE disconnect
// watchdog and (b) external keepalive (e.g. KV TTL refresh). It exits when
// done is closed (from Close).
func (s *Session) supervise() {
	interval := s.KeepaliveInterval
	if interval <= 0 {
		interval = DefaultKeepaliveInterval
	}
	timeout := s.ICEDisconnectedTimeout
	if timeout <= 0 {
		timeout = DefaultICEDisconnectedTimeout
	}
	// First keepalive immediately — if a publisher takes >interval seconds
	// to start transmitting and another node looks up the KV in the
	// meantime we still want the record to exist.
	if s.Keepalive != nil {
		s.Keepalive()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case now := <-ticker.C:
			// Close could have been invoked since the last tick — Go's
			// select is pseudo-random when multiple cases are ready,
			// so we re-check done before committing to expiry or
			// keepalive work.
			select {
			case <-s.done:
				return
			default:
			}

			s.mu.Lock()
			disconnected := s.disconnectedAt
			s.mu.Unlock()

			if !disconnected.IsZero() && now.Sub(disconnected) >= timeout {
				if s.logger != nil {
					s.logger.Warn("WHIP/WHEP ICE watchdog expired; tearing down session",
						"sessionId", s.ID, "mode", s.Mode, "timeout", timeout)
				}
				if s.onExpire != nil {
					s.onExpire(s.ID)
				} else {
					s.Close()
				}
				return
			}

			if s.Keepalive != nil {
				s.Keepalive()
			}
		}
	}
}

// markDisconnected is called by tests (and, in a roundabout way, by the
// ICE state listener above) to record that ICE went away at time t. The
// supervisor will Close the session after ICEDisconnectedTimeout unless
// ICERecovered is called first.
func (s *Session) markDisconnected(t time.Time) {
	s.mu.Lock()
	if s.disconnectedAt.IsZero() {
		s.disconnectedAt = t
	}
	s.mu.Unlock()
}

// ICERecovered is called by Service.Patch after a successful ICE restart so
// the supervisor knows to disarm the disconnect watchdog. We don't rely on
// the eventual IceStateConnected notification because some mediasoup
// implementations coalesce transitions during a restart window.
func (s *Session) ICERecovered() {
	s.mu.Lock()
	s.disconnectedAt = time.Time{}
	s.mu.Unlock()
}

// Close tears down all mediasoup resources held by the session and stops
// the supervisor goroutine. Safe to call multiple times.
//
// Note: we intentionally do NOT nil out s.done, because the supervisor
// goroutine evaluates `case <-s.done:` on each select iteration — nilling
// the channel would make that case never fire, and the ticker case would
// run forever. closeOnce guarantees we only close the channel once.
func (s *Session) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		transport := s.Transport
		s.Transport = nil
		onClose := s.OnClose
		s.OnClose = nil
		done := s.done
		s.mu.Unlock()

		if done != nil {
			close(done)
		}
		if transport != nil {
			transport.Close()
		}
		if onClose != nil {
			onClose()
		}
	})
}

// Registry is a thread-safe map of resourceID -> Session. It is intentionally
// in-memory only: a node restart drops all WHIP sessions (the same as
// disconnecting a WebRTC client).
//
// Separate registries exist for WHIP and WHEP (they have different URL
// spaces and different lifecycle rules; mixing them in one map would let a
// WHEP DELETE collide with a WHIP resourceId that reuses the same identity).
type Registry struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	mode     Mode
	logger   *slog.Logger
}

// NewRegistry constructs a Registry for the given mode.
func NewRegistry(mode Mode) *Registry {
	return &Registry{
		sessions: make(map[string]*Session),
		mode:     mode,
		logger:   slog.Default().With("component", "whip.Registry", "mode", string(mode)),
	}
}

// Add registers a new session and returns its id. If the session has no ID
// one is generated; if the ID is already taken ErrResourceTaken is returned.
// Callers that want "replace on conflict" semantics should first call
// Remove and then Add (the Service layer does this for WHIP identity).
func (r *Registry) Add(s *Session) (string, error) {
	if s.Mode != "" && r.mode != "" && s.Mode != r.mode {
		return "", fmt.Errorf("session mode %q does not match registry mode %q", s.Mode, r.mode)
	}
	if s.ID == "" {
		s.ID = xid.New().String()
	}
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now()
	}
	r.mu.Lock()
	if _, exists := r.sessions[s.ID]; exists {
		r.mu.Unlock()
		return "", ErrResourceTaken
	}
	r.sessions[s.ID] = s
	r.mu.Unlock()
	return s.ID, nil
}

// Get returns a session by id, nil if missing.
func (r *Registry) Get(id string) *Session {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.sessions[id]
}

// Remove deletes a session from the registry and closes it. Returns
// ErrSessionNotFound if the session does not exist.
func (r *Registry) Remove(id string) error {
	r.mu.Lock()
	s, ok := r.sessions[id]
	if !ok {
		r.mu.Unlock()
		return ErrSessionNotFound
	}
	delete(r.sessions, id)
	r.mu.Unlock()
	s.Close()
	return nil
}

// CloseAll closes every tracked session.
func (r *Registry) CloseAll() {
	r.mu.Lock()
	all := r.sessions
	r.sessions = make(map[string]*Session)
	r.mu.Unlock()
	for _, s := range all {
		s.Close()
	}
}

// Len returns the number of active sessions in the registry. Primarily
// useful for tests and telemetry.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sessions)
}

// ErrSessionNotFound is returned when a resource id has no session attached.
var ErrSessionNotFound = errors.New("whip session not found")

// ErrResourceTaken is returned when the requested resource id is already
// active in the registry. The WHIP service normally converts this into a
// replace-the-old-session flow so end users never see it surface.
var ErrResourceTaken = errors.New("whip resource id already active")
