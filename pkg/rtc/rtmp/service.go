package rtmp

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	gortmp "github.com/yutopp/go-rtmp"

	"github.com/axonkit/axonkit/pkg/auth"
)

// DefaultTCPKeepAlivePeriod is the keep-alive probe interval used when
// ServiceParams.TCPKeepAlivePeriod is zero. 30s is short enough to
// detect half-open sockets (publisher killed with SIGKILL, laptop lid
// closed, NAT binding expired) within a minute or two, but long enough
// that healthy idle keep-alive traffic stays well under 1 KiB/min.
const DefaultTCPKeepAlivePeriod = 30 * time.Second

// DefaultIdleTimeout bounds the silence between two consecutive media
// tags before a session self-closes. 30s comfortably covers B-frame
// spacing + paused streams while still catching stuck publishers
// faster than TCP keep-alive can alone.
const DefaultIdleTimeout = 30 * time.Second

// Service is the lifecycle orchestrator for the RTMP ingest: it owns the
// TCP listener and the embedded go-rtmp Server. Per-connection state lives
// in Handler; this layer just wires up the factory callback that every new
// TCP accept() goes through.
//
// Enhanced RTMP (Opus over ExAudio, H.264 over ExVideo) is supported
// transparently — the parsers in this package accept both classic FLV
// and ExRTMP tags.
//
// The service is intentionally small — it holds no publisher state and no
// router policy. The caller supplies a PublishHooks and the service pipes
// each connection through it.
type Service struct {
	params ServiceParams
	logger *slog.Logger

	mu       sync.Mutex
	listener net.Listener
	server   *gortmp.Server
	wg       sync.WaitGroup
}

// ServiceParams carries the static wiring the RTMP listener needs.
type ServiceParams struct {
	// Listen is the TCP bind address (e.g. ":1935"). Empty disables the
	// service; Start returns an error in that case so the caller catches
	// misconfiguration early.
	Listen string

	// Verifier validates JWT stream-key tokens. Required; publishing
	// without a token is rejected.
	Verifier *auth.TokenVerifier

	// Hooks wires per-connection callbacks. Logger on the hooks is
	// replaced per-connection with a remote-addr annotated child logger.
	Hooks PublishHooks

	// Logger is the service-level logger; defaults to slog.Default with
	// a "component=rtmp.Service" tag.
	Logger *slog.Logger

	// TCPKeepAlivePeriod controls SO_KEEPALIVE probe spacing on every
	// accepted RTMP connection. Zero falls back to
	// DefaultTCPKeepAlivePeriod. Negative values disable kernel
	// keep-alive entirely — not recommended for production, but useful
	// in environments that prefer to rely solely on the session-level
	// idle watchdog.
	TCPKeepAlivePeriod time.Duration

	// IdleTimeout flows into every Session: when a publisher goes
	// silent for longer than this window the session forces the TCP
	// connection closed, which unblocks go-rtmp's OnClose and
	// participant.Close. Zero falls back to DefaultIdleTimeout; a
	// negative value disables the watchdog.
	IdleTimeout time.Duration
}

// NewService constructs the service without starting the listener.
func NewService(p ServiceParams) *Service {
	logger := p.Logger
	if logger == nil {
		logger = slog.Default().With("component", "rtmp.Service")
	}
	return &Service{params: p, logger: logger}
}

// Start binds the TCP listener and kicks off the accept loop in a
// background goroutine. Safe to call at most once; subsequent calls return
// an error until Stop runs.
func (s *Service) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server != nil {
		return errors.New("rtmp service already started")
	}
	if s.params.Listen == "" {
		return errors.New("rtmp listen address is empty")
	}
	if s.params.Hooks.ResolveRouter == nil {
		return errors.New("rtmp hooks.ResolveRouter is required")
	}
	if s.params.Hooks.OnSessionReady == nil {
		return errors.New("rtmp hooks.OnSessionReady is required")
	}

	l, err := net.Listen("tcp", s.params.Listen)
	if err != nil {
		return fmt.Errorf("rtmp listen %q: %w", s.params.Listen, err)
	}
	s.listener = l

	// go-rtmp wants one Handler per connection. The factory closes over
	// the service's auth/hook config and personalises the per-connection
	// logger with the remote address so multi-publisher logs stay
	// correlated.
	keepAlivePeriod := s.params.TCPKeepAlivePeriod
	if keepAlivePeriod == 0 {
		keepAlivePeriod = DefaultTCPKeepAlivePeriod
	}
	idleTimeout := s.params.IdleTimeout
	if idleTimeout == 0 {
		idleTimeout = DefaultIdleTimeout
	}

	srv := gortmp.NewServer(&gortmp.ServerConfig{
		OnConnect: func(conn net.Conn) (io.ReadWriteCloser, *gortmp.ConnConfig) {
			connLogger := s.logger.With("remote", conn.RemoteAddr().String())
			// Enable kernel-level keep-alive so half-open TCP
			// sockets (publisher process crashed, NAT binding
			// expired, router restarted) are torn down promptly
			// instead of stalling the go-rtmp read loop — which
			// is the path that ultimately fires OnClose and the
			// participant-leaves-room bookkeeping. A negative
			// keepAlivePeriod opts out and defers detection to
			// the session-level idle watchdog alone.
			if keepAlivePeriod > 0 {
				if tcp, ok := conn.(*net.TCPConn); ok {
					if err := tcp.SetKeepAlive(true); err != nil {
						connLogger.Warn("rtmp set keepalive", "err", err)
					}
					if err := tcp.SetKeepAlivePeriod(keepAlivePeriod); err != nil {
						connLogger.Warn("rtmp set keepalive period", "err", err)
					}
				}
			}
			hooks := s.params.Hooks
			hooks.Logger = connLogger
			hooks.IdleTimeout = idleTimeout
			return conn, &gortmp.ConnConfig{
				Handler: NewHandler(hooks, s.params.Verifier),
				// Logger defaults to ioutil.Discard inside go-rtmp
				// when left nil — matching our preference to keep the
				// noisy library-internal logs out of stderr.
				Logger: nil,
			}
		},
	})
	s.server = srv

	s.wg.Go(func() {
		s.logger.Info("RTMP service listening", "addr", s.params.Listen)
		if err := srv.Serve(l); err != nil && !errors.Is(err, gortmp.ErrClosed) {
			s.logger.Error("rtmp serve returned", "err", err)
		}
	})

	return nil
}

// Stop closes the listener and waits for the accept loop to exit. Active
// publisher connections are torn down by go-rtmp when their underlying
// net.Conn is closed (Server.Close closes the listener, not the accepted
// conns, but established handlers notice EOF on the next Read).
func (s *Service) Stop() error {
	s.mu.Lock()
	srv := s.server
	s.server = nil
	s.mu.Unlock()

	if srv == nil {
		return nil
	}
	if err := srv.Close(); err != nil {
		return err
	}
	s.wg.Wait()
	return nil
}
