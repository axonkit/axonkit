package rtmp

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	gortmp "github.com/yutopp/go-rtmp"

	"github.com/axonkit/axonkit/pkg/auth"
)

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
	done     chan struct{}
}

// ServiceParams carries the static wiring the RTMP listener needs.
type ServiceParams struct {
	// Listen is the TCP bind address (e.g. ":1935"). Empty disables the
	// service; Start returns an error in that case so the caller catches
	// misconfiguration early.
	Listen string

	// Verifier validates JWT stream-key tokens. May be nil when
	// AllowAnonymous is true.
	Verifier *auth.TokenVerifier

	// AllowAnonymous relaxes token requirements. Matches the HTTP
	// server's AllowAnonymous so operators can run a single switch for
	// all ingest paths.
	AllowAnonymous bool

	// Hooks wires per-connection callbacks. Logger on the hooks is
	// replaced per-connection with a remote-addr annotated child logger.
	Hooks PublishHooks

	// Logger is the service-level logger; defaults to slog.Default with
	// a "component=rtmp.Service" tag.
	Logger *slog.Logger
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
	s.done = make(chan struct{})

	// go-rtmp wants one Handler per connection. The factory closes over
	// the service's auth/hook config and personalises the per-connection
	// logger with the remote address so multi-publisher logs stay
	// correlated.
	srv := gortmp.NewServer(&gortmp.ServerConfig{
		OnConnect: func(conn net.Conn) (io.ReadWriteCloser, *gortmp.ConnConfig) {
			connLogger := s.logger.With("remote", conn.RemoteAddr().String())
			hooks := s.params.Hooks
			hooks.Logger = connLogger
			return conn, &gortmp.ConnConfig{
				Handler: NewHandler(hooks, s.params.Verifier, s.params.AllowAnonymous),
				// Logger defaults to ioutil.Discard inside go-rtmp
				// when left nil — matching our preference to keep the
				// noisy library-internal logs out of stderr.
				Logger: nil,
			}
		},
	})
	s.server = srv

	go func() {
		s.logger.Info("RTMP service listening", "addr", s.params.Listen)
		if err := srv.Serve(l); err != nil && !errors.Is(err, gortmp.ErrClosed) {
			s.logger.Error("rtmp serve returned", "err", err)
		}
		close(s.done)
	}()
	return nil
}

// Stop closes the listener and waits for the accept loop to exit. Active
// publisher connections are torn down by go-rtmp when their underlying
// net.Conn is closed (Server.Close closes the listener, not the accepted
// conns, but established handlers notice EOF on the next Read).
func (s *Service) Stop() error {
	s.mu.Lock()
	srv := s.server
	done := s.done
	s.server = nil
	s.done = nil
	s.mu.Unlock()

	if srv == nil {
		return nil
	}
	if err := srv.Close(); err != nil {
		return err
	}
	if done != nil {
		<-done
	}
	return nil
}
