package rtmp

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"
	"time"

	gortmp "github.com/yutopp/go-rtmp"
	gortmpmsg "github.com/yutopp/go-rtmp/message"

	"github.com/jiyeyuran/mediasoup-go/v2"

	"github.com/axonkit/axonkit/pkg/auth"
)

// PublishHooks are wired by the HTTP server so the Handler can attach a
// new RTMP publish to the same Participant plumbing WHIP uses.
//
// The package supports both classic RTMP and Enhanced RTMP (ExAudio /
// ExVideo) tags; callers do not need to distinguish between the two.
type PublishHooks struct {
	// ResolveRouter picks a router to host the publisher's producers.
	// Returning an error aborts the publish with a NetStream.Publish.Bad.
	ResolveRouter func(roomName string) (*mediasoup.Router, error)

	// OnSessionReady is invoked after OnPublish succeeds and the Session
	// has been created. The server uses it to attach a Participant to the
	// room and register producers as they come online via the session's
	// OnFirst/OnAdditionalProducer callbacks wired by the server.
	//
	// The participant lifetime is owned by the server; when the RTMP
	// connection closes, the Handler calls OnSessionClosed so the server
	// can drop the participant from the room. `attached` is an opaque
	// handle the server echoes back on OnSessionClosed.
	OnSessionReady func(ctx context.Context, info PublishInfo, session *Session) (attached any, err error)

	// OnSessionClosed is invoked when the RTMP connection goes away.
	OnSessionClosed func(attached any)

	// Logger receives connection-level logs. When nil, slog.Default is used.
	Logger *slog.Logger

	// IdleTimeout bounds the silence Session tolerates between two
	// consecutive audio/video tags. Zero disables the application-
	// level watchdog; kernel TCP keep-alive then becomes the sole
	// detector of half-open publisher sockets. The Service injects a
	// non-zero default so callers only override when they explicitly
	// want the watchdog off.
	IdleTimeout time.Duration
}

// PublishInfo carries authenticated publisher metadata surfaced to the
// server via OnSessionReady. The stream key is split by ParseStreamKey.
type PublishInfo struct {
	RoomName   string
	Claims     *auth.Claims
	StreamName string // the raw "publishingName" string from the RTMP NetStream.publish.
}

// ParseStreamKey splits the RTMP publishing name into (room, token).
//
// The canonical form is "<room>?token=<jwt>" (e.g. publishers point at
// rtmp://host/<app>/<room>?token=<jwt>). A bare "<room>" is accepted as
// an anonymous publish and only succeeds when the operator enabled
// AllowAnonymous.
//
// Trailing path components after the token are tolerated: some mobile
// clients stuff extra diagnostics into the stream key. Anything that
// isn't the "token" query parameter is silently ignored.
func ParseStreamKey(name string) (room, token string, hasToken bool) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", "", false
	}
	idx := strings.IndexByte(trimmed, '?')
	if idx < 0 {
		return trimmed, "", false
	}
	room = strings.TrimSpace(trimmed[:idx])
	q, err := url.ParseQuery(trimmed[idx+1:])
	if err != nil {
		return room, "", false
	}
	token = strings.TrimSpace(q.Get("token"))
	return room, token, token != ""
}

// handler is the go-rtmp Handler bound to one TCP connection.
type handler struct {
	gortmp.DefaultHandler

	cfg PublishHooks

	verifier       *auth.TokenVerifier
	allowAnonymous bool

	connLogger *slog.Logger
	streamID   uint32

	session  *Session
	publish  *PublishInfo
	attached any
	router   *mediasoup.Router
	conn     *gortmp.Conn

	connCtx    context.Context
	cancelConn context.CancelFunc
}

// NewHandler returns a go-rtmp.Handler bound to hooks + auth. The returned
// value is per-connection; go-rtmp allocates one for each inbound TCP
// connection and drives its callbacks serially.
func NewHandler(hooks PublishHooks, verifier *auth.TokenVerifier, allowAnonymous bool) gortmp.Handler {
	logger := hooks.Logger
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &handler{
		cfg:            hooks,
		verifier:       verifier,
		allowAnonymous: allowAnonymous,
		connLogger:     logger,
		connCtx:        ctx,
		cancelConn:     cancel,
	}
}

func (h *handler) OnServe(conn *gortmp.Conn) {
	// Save the connection so the server can tear down the TCP socket if
	// the participant is evicted from the room. go-rtmp's Conn.Close() is
	// idempotent and calling it triggers our OnClose hook in turn.
	h.conn = conn
}

func (h *handler) OnConnect(_ uint32, cmd *gortmpmsg.NetConnectionConnect) error {
	h.connLogger = h.connLogger.With("app", cmd.Command.App)
	return nil
}

func (h *handler) OnCreateStream(_ uint32, _ *gortmpmsg.NetConnectionCreateStream) error {
	return nil
}

// OnPublish authenticates the publisher and creates the Session. It
// intentionally does not create producers yet — those are lazy and driven
// by the first OnAudio/OnVideo carrying the codec config.
func (h *handler) OnPublish(ctx *gortmp.StreamContext, _ uint32, cmd *gortmpmsg.NetStreamPublish) error {
	h.streamID = ctx.StreamID
	streamName := cmd.PublishingName

	room, token, hasToken := ParseStreamKey(streamName)
	if room == "" {
		return fmt.Errorf("invalid stream key: expected \"<room>?token=<jwt>\" or \"<room>\", got %q", streamName)
	}

	claims, err := h.authorize(room, token, hasToken)
	if err != nil {
		return fmt.Errorf("authorize: %w", err)
	}

	router, err := h.cfg.ResolveRouter(room)
	if err != nil {
		return fmt.Errorf("resolve router: %w", err)
	}
	h.router = router

	h.connLogger = h.connLogger.With("room", room, "identity", claims.Identity())
	info := PublishInfo{RoomName: room, Claims: claims, StreamName: streamName}

	session := NewSession(SessionParams{
		Router:      router,
		Logger:      h.connLogger,
		IdleTimeout: h.cfg.IdleTimeout,
	})
	if h.conn != nil {
		conn := h.conn
		session.SetConnCloser(func() { _ = conn.Close() })
	}
	h.session = session
	h.publish = &info

	attached, err := h.cfg.OnSessionReady(h.connCtx, info, session)
	if err != nil {
		session.Close()
		h.session = nil
		return fmt.Errorf("attach: %w", err)
	}
	h.attached = attached

	h.connLogger.Info("RTMP publish accepted", "streamId", ctx.StreamID)
	return nil
}

func (h *handler) OnAudio(timestamp uint32, payload io.Reader) error {
	if h.session == nil {
		return nil
	}
	data, err := io.ReadAll(payload)
	if err != nil {
		return err
	}
	tag, err := ParseAudioTag(data)
	if err != nil {
		if IsUnsupportedAudio(err) {
			h.connLogger.Debug("rtmp audio dropped", "reason", err.Error())
			return nil
		}
		h.connLogger.Warn("rtmp audio parse error", "err", err)
		return nil
	}
	if err := h.session.IngestAudio(h.connCtx, timestamp, tag); err != nil {
		h.connLogger.Warn("rtmp audio ingest error", "err", err)
	}
	return nil
}

func (h *handler) OnVideo(timestamp uint32, payload io.Reader) error {
	if h.session == nil {
		return nil
	}
	data, err := io.ReadAll(payload)
	if err != nil {
		return err
	}
	tag, err := ParseVideoTag(data)
	if err != nil {
		if IsUnsupportedVideo(err) {
			h.connLogger.Debug("rtmp video dropped", "reason", err.Error())
			return nil
		}
		h.connLogger.Warn("rtmp video parse error", "err", err)
		return nil
	}
	if err := h.session.IngestVideo(h.connCtx, timestamp, tag); err != nil {
		h.connLogger.Warn("rtmp video ingest error", "err", err)
	}
	return nil
}

func (h *handler) OnClose() {
	h.cancelConn()
	if h.session != nil {
		h.session.Close()
	}
	if h.attached != nil && h.cfg.OnSessionClosed != nil {
		h.cfg.OnSessionClosed(h.attached)
	}
	h.connLogger.Info("RTMP session closed")
}

// authorize turns the stream-key token into a Claims payload. When the
// verifier is nil and anonymous is allowed we mint a permissive claim
// scoped to the room — this matches the HTTP AllowAnonymous behavior.
//
// Anonymous RTMP identities are prefixed "rtmp_" (matching the WHIP
// "whip_" / admin-API "anon_" convention) so room logs make clear which
// ingest surface the publisher came in through.
func (h *handler) authorize(room, token string, hasToken bool) (*auth.Claims, error) {
	if hasToken && token != "" && h.verifier != nil {
		claims, err := h.verifier.Verify(token)
		if err != nil {
			return nil, fmt.Errorf("verify token: %w", err)
		}
		if claims.Grants == nil || claims.Grants.Room == nil {
			return nil, fmt.Errorf("token has no room grant")
		}
		rg := claims.Grants.Room
		if rg.Name != "" && rg.Name != room {
			return nil, fmt.Errorf("token scoped to room %q, publish targets %q", rg.Name, room)
		}
		if claims.Grants.Media == nil || claims.Grants.Media.CanPublish == nil || !*claims.Grants.Media.CanPublish {
			return nil, fmt.Errorf("token does not grant publish permission")
		}
		return claims, nil
	}
	if !h.allowAnonymous {
		return nil, fmt.Errorf("authentication required: add ?token=<jwt> to the stream key")
	}
	return auth.AnonymousClaims("rtmp_"+randomCName(), room), nil
}
