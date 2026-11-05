package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/axonkit/axonkit/pkg/auth"
	"github.com/axonkit/axonkit/pkg/broadcast"
	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/rtc"
	"github.com/axonkit/axonkit/pkg/rtc/whip"
	"github.com/axonkit/axonkit/pkg/store"
	"github.com/axonkit/axonkit/pkg/types"
	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/nats-io/nats.go"
	"github.com/rs/xid"
)

type logIdKey struct{}

// errorBodyCaptureLimit caps the amount of body bytes kept for 5xx logging.
// Large SDPs/JWTs can still be truncated; anything past the limit is elided.
const errorBodyCaptureLimit = 8 * 1024

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int
	body   bytes.Buffer
}

func (w *loggingResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *loggingResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if remaining := errorBodyCaptureLimit - w.body.Len(); remaining > 0 {
		chunk := data
		if len(chunk) > remaining {
			chunk = chunk[:remaining]
		}
		w.body.Write(chunk)
	}
	n, err := w.ResponseWriter.Write(data)
	w.bytes += n
	return n, err
}

func WithLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logId := r.Header.Get("X-Log-Id")
		if logId == "" {
			logId = xid.New().String()
		}
		start := time.Now()

		// Mirror request body bytes into reqBodyBuf as the handler reads them,
		// capped at errorBodyCaptureLimit so large payloads do not blow up
		// memory. Only emitted when the handler returns 5xx.
		var reqBodyBuf bytes.Buffer
		if r.Body != nil && r.Body != http.NoBody {
			r.Body = &teeReadCloser{
				src: r.Body,
				dst: &limitedBuffer{buf: &reqBodyBuf, limit: errorBodyCaptureLimit},
			}
		}

		lrw := &loggingResponseWriter{ResponseWriter: w}
		ctx := context.WithValue(r.Context(), logIdKey{}, logId)
		next.ServeHTTP(lrw, r.WithContext(ctx))

		attrs := []any{
			"method", r.Method, "path", r.URL.Path,
			"status", lrw.status, "bytes", lrw.bytes,
			"logId", logId, "duration_ms", time.Since(start).Milliseconds(),
			"reqBody", reqBodyBuf.String(),
			"respBody", lrw.body.String(),
		}
		if lrw.status >= 500 {
			slog.Error("HTTP request", attrs...)
			return
		}
		slog.Info("HTTP request", attrs...)
	})
}

// limitedBuffer appends to buf only until limit bytes have been stored.
// Writes past the limit are silently dropped (but still report as consumed to
// the caller, so the underlying stream is not blocked).
type limitedBuffer struct {
	buf   *bytes.Buffer
	limit int
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	remaining := l.limit - l.buf.Len()
	if remaining <= 0 {
		return len(p), nil
	}
	if len(p) > remaining {
		l.buf.Write(p[:remaining])
		return len(p), nil
	}
	l.buf.Write(p)
	return len(p), nil
}

// teeReadCloser mirrors bytes read from src into dst, while forwarding the
// full stream to the caller. Errors from the underlying src propagate as-is.
type teeReadCloser struct {
	src io.ReadCloser
	dst io.Writer
}

func (t *teeReadCloser) Read(p []byte) (int, error) {
	n, err := t.src.Read(p)
	if n > 0 {
		_, _ = t.dst.Write(p[:n])
	}
	return n, err
}

func (t *teeReadCloser) Close() error { return t.src.Close() }

func WithCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, PATCH, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		// Expose headers the WHIP/WHEP RFC 9725 resource-oriented flow
		// depends on: browsers would otherwise strip them from
		// fetch().headers, which causes clients to bail out with
		// "Response missing location header" (for POST→201) and lose
		// ETag/Link on subsequent PATCH/DELETE round-trips. X-Node-Id is
		// our own telemetry header and harmless to include.
		w.Header().Set("Access-Control-Expose-Headers", "Location, ETag, Link, Content-Type, X-Node-Id")
		w.Header().Set("Access-Control-Max-Age", "86400")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

type HTTPServer struct {
	cfg           *config.Config
	roomManager   *rtc.RoomManager
	verifier      *auth.TokenVerifier
	node          types.NodeInfo
	nodeRPC       *NodeRPCService
	routers       []*mediasoup.Router
	webRtcServers []*mediasoup.WebRtcServer

	whipSvc *whip.Service
	// whepLocKV is the optional KV store used when WhepRouting == "kv" to
	// map resourceID → owning NodeID. nil when running the "prefix" scheme
	// or when KV provisioning failed at startup (we still serve WHIP+WHEP,
	// just without the discovery table).
	whepLocKV store.Store

	// broadcastMgr owns all live "push room user to RTMP/WHIP" tasks
	// (see pkg/broadcast). Populated in NewHTTPServer; closed on
	// HTTPServer shutdown so external sinks see a clean disconnect.
	broadcastMgr *broadcast.Manager
}

type HTTPServerParams struct {
	Config        *config.Config
	RoomManager   *rtc.RoomManager
	Verifier      *auth.TokenVerifier
	Node          types.NodeInfo
	NodeRPC       *NodeRPCService
	Routers       []*mediasoup.Router
	WebRtcServers []*mediasoup.WebRtcServer
	WhepLocKV     store.Store
}

func NewHTTPServer(params HTTPServerParams) *HTTPServer {
	s := &HTTPServer{
		cfg:           params.Config,
		roomManager:   params.RoomManager,
		verifier:      params.Verifier,
		node:          params.Node,
		nodeRPC:       params.NodeRPC,
		routers:       params.Routers,
		webRtcServers: params.WebRtcServers,
		whepLocKV:     params.WhepLocKV,
	}
	s.whipSvc = whip.NewService(
		&whipTransportFactory{routers: s.routers, webRtcServers: s.webRtcServers},
		&whipProducerSource{server: s},
		slog.Default().With("component", "whip.Service"),
	)
	s.broadcastMgr = broadcast.NewManager(broadcast.ManagerParams{
		Rooms:       s.roomManager,
		WhipService: s.whipSvc,
		Logger:      slog.Default().With("component", "broadcast.Manager"),
	})
	return s
}

// Close releases the runtime resources the HTTPServer owns on top of
// plain HTTP (currently: every live broadcast). Safe to call multiple
// times; broadcast.Manager.Close is itself idempotent.
func (s *HTTPServer) Close() {
	if s.broadcastMgr != nil {
		s.broadcastMgr.Close()
	}
}

func (s *HTTPServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /rtc/join", s.handleJoin)
	mux.HandleFunc("POST /rtc/connect", s.handleConnect)
	mux.HandleFunc("POST /rtc/forward-participant", s.handleForwardParticipant)
	mux.HandleFunc("POST /rtc/remove-participant", s.handleRemoveParticipant)
	mux.HandleFunc("OPTIONS /rtc/join", func(w http.ResponseWriter, r *http.Request) {})
	mux.HandleFunc("OPTIONS /rtc/connect", func(w http.ResponseWriter, r *http.Request) {})
	mux.HandleFunc("OPTIONS /rtc/forward-participant", func(w http.ResponseWriter, r *http.Request) {})
	mux.HandleFunc("OPTIONS /rtc/remove-participant", func(w http.ResponseWriter, r *http.Request) {})
	s.registerWHIPRoutes(mux)
	s.registerBroadcastRoutes(mux)
}

// authorizeAdmin verifies the Bearer token and checks that the caller
// holds a RoomGrant.Admin. When AllowAnonymous is set and no token is
// presented, the request is accepted (useful for local dev and tests).
// Otherwise:
//
//   - the token must verify;
//   - RoomGrant.Admin must be true; and
//   - when roomName != "", the grant must be unscoped (Name == "") OR
//     match roomName so a tenant cannot act on other tenants' rooms.
//
// roomName may be empty when the operation does not target a specific
// room (for example, "find-by-identity" style APIs that accept the room
// name in the body and want to authorize strictly on Admin).
//
// anonPrefix is the identity prefix used when minting anonymous claims
// (AllowAnonymous path). Callers supply "whip_" for WHIP/WHEP surfaces
// and "anon_" for plain HTTP admin APIs; this lets room logs show which
// entry point the caller came in through.
func (s *HTTPServer) authorizeAdmin(r *http.Request, roomName, anonPrefix string) (*auth.Claims, *whipErr) {
	token := extractBearerToken(r)
	if token == "" && s.cfg.Auth.AllowAnonymous {
		return auth.AnonymousClaims(anonPrefix+xid.New().String(), roomName), nil
	}
	if token == "" {
		return nil, &whipErr{status: http.StatusUnauthorized, body: "missing Authorization: Bearer <token>"}
	}
	claims, err := s.verifier.Verify(token)
	if err != nil {
		return nil, &whipErr{status: http.StatusUnauthorized, body: err.Error()}
	}
	if claims.Grants == nil || claims.Grants.Room == nil || !claims.Grants.Room.Admin {
		return nil, &whipErr{status: http.StatusForbidden, body: "room admin grant required"}
	}
	rg := claims.Grants.Room
	if roomName != "" && rg.Name != "" && rg.Name != roomName {
		return nil, &whipErr{status: http.StatusForbidden, body: "token is scoped to a different room"}
	}
	return claims, nil
}

func readRequest(r *http.Request, v any) error {
	contentType := r.Header.Get("Content-Type")
	if contentType == "application/x-www-form-urlencoded" {
		payload := r.PostFormValue("payload")
		if payload == "" {
			return errors.New("missing payload parameter")
		}
		return json.Unmarshal([]byte(payload), v)
	}
	return json.NewDecoder(r.Body).Decode(v)
}

func (s *HTTPServer) handleJoin(w http.ResponseWriter, r *http.Request) {
	logId, _ := r.Context().Value(logIdKey{}).(string)

	var req proto.JoinRequest
	if err := readRequest(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: err.Error()})
		return
	}

	// ---- Node forwarding: if the client specifies a target node, proxy to it ----
	if req.NodeId != "" && types.NodeID(req.NodeId) != s.node.ID && s.nodeRPC != nil {
		payload, _ := json.Marshal(req)
		action := NodeRPCActionJoin
		if req.Reconnect {
			action = NodeRPCActionReconnect
		}
		if err := s.forwardViaNATS(w, types.NodeID(req.NodeId), action, payload); err != nil {
			if req.Reconnect && errors.Is(err, nats.ErrNoResponders) {
				slog.Info("Target node unreachable during reconnect, falling back to full reconnect",
					"oldNode", req.NodeId, "logId", logId)
				s.handleFullReconnect(w, &req, logId)
				return
			}
			writeJSON(w, http.StatusBadGateway, proto.ErrorResponse{
				Code: proto.ErrCodeInternal, Error: "failed to reach target node"})
		}
		return
	}

	// ---- Reconnect branch ----
	if req.Reconnect && req.ParticipantSid != "" {
		s.handleReconnect(w, &req, logId)
		return
	}

	s.handleJoinLocal(w, &req, logId)
}

func (s *HTTPServer) handleJoinLocal(w http.ResponseWriter, req *proto.JoinRequest, logId string) {
	var claims *auth.Claims
	participantSid := "pa_" + xid.New().String()

	switch {
	case req.Token != "":
		var err error
		claims, err = s.verifier.Verify(req.Token)
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, proto.ErrorResponse{
				Code: proto.ErrCodeInvalidToken, Error: err.Error()})
			return
		}
	case s.cfg.Auth.AllowAnonymous:
		roomName := ""
		if req.Room != nil {
			roomName = req.Room.Name
		}
		boolTrue := true
		anonIdentity := participantSid
		claims = &auth.Claims{
			Sub:  anonIdentity,
			Name: anonIdentity,
			Grants: &proto.ClaimGrants{
				Identity: anonIdentity,
				Room: &proto.RoomGrant{
					Name: roomName,
					Join: true,
				},
				Media: &proto.MediaGrant{
					CanPublish:   &boolTrue,
					CanSubscribe: &boolTrue,
				},
			},
		}
	default:
		writeJSON(w, http.StatusUnauthorized, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidToken, Error: "token required"})
		return
	}

	// The room name in the request is authoritative. When omitted, fall
	// back to the room grant in the verified token so tokens stay
	// self-contained. Anonymous joins must still pass an explicit name.
	if req.Room == nil {
		req.Room = &proto.CreateRoomRequest{}
	}
	if req.Room.Name == "" && claims.Grants != nil && claims.Grants.Room != nil {
		req.Room.Name = claims.Grants.Room.Name
	}
	if req.Room.Name == "" {
		writeJSON(w, http.StatusBadRequest, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: "room name required"})
		return
	}

	if err := rtc.ValidateRoomName(req.Room.Name); err != nil {
		writeJSON(w, http.StatusBadRequest, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: err.Error()})
		return
	}

	if claims.Grants != nil && claims.Grants.Room != nil {
		rg := claims.Grants.Room
		if !rg.Join {
			writeJSON(w, http.StatusForbidden, proto.ErrorResponse{
				Code: proto.ErrCodePermissionDenied, Error: "token does not grant room join permission"})
			return
		}
		if rg.Name != "" && rg.Name != req.Room.Name {
			writeJSON(w, http.StatusForbidden, proto.ErrorResponse{
				Code: proto.ErrCodePermissionDenied, Error: "token is scoped to a different room"})
			return
		}
	}

	room := s.roomManager.GetOrCreateRoom(
		req.Room.Name,
		int(req.Room.MaxParticipants),
		req.Room.Metadata,
	)

	var routerIdx int
	if room.SharedRouter() {
		routerIdx = room.RouterIndex()
	} else {
		routerIdx = int(time.Now().UnixNano() % int64(len(s.routers)))
		if routerIdx < 0 {
			routerIdx = -routerIdx
		}
	}
	router := s.routers[routerIdx]
	webRtcServer := s.webRtcServers[routerIdx]

	identity := claims.Identity()
	if identity == "" {
		identity = claims.Name
	}

	autoSubscribe := s.cfg.Room.AutoSubscribe
	var lastN int
	if req.ConnectionSettings != nil {
		autoSubscribe = req.ConnectionSettings.AutoSubscribe
		lastN = int(req.ConnectionSettings.LastN)
	}

	participant := rtc.NewParticipant(rtc.ParticipantParams{
		Sid:             participantSid,
		Identity:        identity,
		Name:            claims.Name,
		Metadata:        req.Metadata,
		Attributes:      req.ParticipantAttributes,
		Grants:          claims.Grants,
		Router:          router,
		AutoSubscribe:   autoSubscribe,
		LastN:           lastN,
		PingInterval:    s.cfg.Room.PingInterval,
		PingTimeout:     s.cfg.Room.PingTimeout,
		StatsInterval:   s.cfg.Room.StatsInterval,
		TokenVerifier:   s.verifier,
		RefreshTokenTTL: s.cfg.Auth.RefreshTokenTTL,
		NodeId:          string(s.node.ID),
	})

	sendTransport, recvTransport, directTransport, err := s.createTransports(router, webRtcServer)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, proto.ErrorResponse{
			Code: proto.ErrCodeInternal, Error: "transport creation failed: " + err.Error()})
		return
	}
	participant.SetTransports(sendTransport, recvTransport, directTransport)

	if req.ConnectionSettings != nil && len(req.ConnectionSettings.DtlsFingerprints) > 0 {
		dtlsParams := &mediasoup.DtlsParameters{
			Role:         mediasoup.DtlsRoleClient,
			Fingerprints: req.ConnectionSettings.DtlsFingerprints,
		}
		if err := connectTransports(dtlsParams, sendTransport, recvTransport); err != nil {
			sendTransport.Close()
			recvTransport.Close()
			directTransport.Close()
			writeJSON(w, http.StatusInternalServerError, proto.ErrorResponse{
				Code: proto.ErrCodeInternal, Error: "transport connect failed: " + err.Error()})
			return
		}
	}

	sig, err := s.createSignalChannels(sendTransport, recvTransport, directTransport)
	if err != nil {
		sendTransport.Close()
		recvTransport.Close()
		directTransport.Close()
		writeJSON(w, http.StatusInternalServerError, proto.ErrorResponse{
			Code: proto.ErrCodeInternal, Error: "signal channel failed: " + err.Error()})
		return
	}
	participant.SetSignal(sig.outProducer, sig.outConsumer, sig.inProducer, sig.inConsumer)

	if err := room.AddParticipant(participant); err != nil {
		sendTransport.Close()
		recvTransport.Close()
		directTransport.Close()
		code := proto.ErrCodeInternal
		switch err {
		case rtc.ErrRoomFull:
			code = proto.ErrCodeRoomFull
		case rtc.ErrDuplicateIdentity:
			code = proto.ErrCodeDuplicateIdentity
		}
		writeJSON(w, http.StatusConflict, proto.ErrorResponse{
			Code: code, Error: err.Error()})
		return
	}

	fullReconnect := req.ParticipantSid != ""

	// Generate a long-lived refresh token for reconnection.
	var refreshTokenStr string
	ttl := s.cfg.Auth.RefreshTokenTTL
	if ttl == 0 {
		ttl = 7 * 24 * time.Hour
	}
	if s.verifier != nil && req.Token != "" {
		rt, err := s.verifier.RefreshToken(req.Token, ttl)
		if err == nil {
			refreshTokenStr = rt
			participant.SetRefreshToken(rt)
		} else {
			slog.Warn("Failed to generate refresh token", "error", err)
		}
	} else if s.verifier != nil && claims != nil {
		// Anonymous or debug mode: generate a fresh token from constructed claims.
		at := auth.NewAccessToken(s.cfg.Auth.APIKey, s.cfg.Auth.APISecret)
		at.SetIdentity(claims.Sub).SetName(claims.Name).SetGrants(claims.Grants).SetTTL(ttl)
		rt, err := at.Sign()
		if err == nil {
			refreshTokenStr = rt
			participant.SetRefreshToken(rt)
		} else {
			slog.Warn("Failed to generate refresh token for anonymous user", "error", err)
		}
	}

	var icePolicy string
	if req.ConnectionSettings != nil && req.ConnectionSettings.ForceTcp {
		icePolicy = "relay"
	}

	resp := proto.JoinResponse{
		Room:          room.ToProto(),
		Participant:   participant.ToProto(),
		PingTimeout:   s.cfg.PingTimeout(),
		PingInterval:  s.cfg.PingInterval(),
		StatsInterval: s.cfg.StatsInterval(),
		ServerInfo: &proto.ServerInfo{
			NodeId:   string(s.node.ID),
			Region:   string(s.node.Region),
			Protocol: 1,
		},
		SendTransport:      s.transportToProto(sendTransport),
		ReceiveTransport:   s.transportToProto(recvTransport),
		Signal:             sig.info,
		RtpCapabilities:    router.RtpCapabilities(),
		FullReconnect:      fullReconnect,
		RefreshToken:       refreshTokenStr,
		IceTransportPolicy: icePolicy,
	}

	w.Header().Set("X-Node-Id", string(s.node.ID))
	w.Header().Set("X-Log-Id", logId)

	writeJSON(w, http.StatusOK, resp)

	slog.Info("Participant joined", "identity", identity, "sid", participantSid,
		"room", req.Room.Name, "fullReconnect", fullReconnect, "logId", logId)
}

// ============================================================
// Transport Helpers
// ============================================================

func (s *HTTPServer) createTransports(router *mediasoup.Router, webRtcServer *mediasoup.WebRtcServer) (send, recv, direct *mediasoup.Transport, err error) {
	enableUdp := true
	webRtcOpts := &mediasoup.WebRtcTransportOptions{
		WebRtcServer: webRtcServer,
		EnableUdp:    &enableUdp,
		EnableTcp:    true,
		PreferUdp:    true,
		EnableSctp:   true,
		NumSctpStreams: &mediasoup.NumSctpStreams{
			OS:  1024,
			MIS: 1024,
		},
	}

	send, err = router.CreateWebRtcTransport(webRtcOpts)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("send transport: %w", err)
	}

	recv, err = router.CreateWebRtcTransport(webRtcOpts)
	if err != nil {
		send.Close()
		return nil, nil, nil, fmt.Errorf("recv transport: %w", err)
	}
	recv.SetMaxIncomingBitrate(8000000)

	direct, err = router.CreateDirectTransport(&mediasoup.DirectTransportOptions{})
	if err != nil {
		send.Close()
		recv.Close()
		return nil, nil, nil, fmt.Errorf("direct transport: %w", err)
	}

	return send, recv, direct, nil
}

// signalChannels holds all four signal-related resources.
type signalChannels struct {
	outProducer *mediasoup.DataProducer // server→client: DataProducer on DirectTransport
	outConsumer *mediasoup.DataConsumer // server→client: DataConsumer on RecvTransport
	inProducer  *mediasoup.DataProducer // client→server: DataProducer on SendTransport (pre-created)
	inConsumer  *mediasoup.DataConsumer // client→server: DataConsumer on DirectTransport
	info        *proto.SignalInfo
}

func (s *HTTPServer) createSignalChannels(send, recv, direct *mediasoup.Transport) (*signalChannels, error) {
	// Server→Client: DataProducer on DirectTransport, DataConsumer on RecvTransport
	outProducer, err := direct.ProduceData(&mediasoup.DataProducerOptions{
		Label:    "signal",
		Protocol: "json",
	})
	if err != nil {
		return nil, fmt.Errorf("signal outProducer: %w", err)
	}

	outConsumer, err := recv.ConsumeData(&mediasoup.DataConsumerOptions{
		DataProducerId: outProducer.Id(),
	})
	if err != nil {
		outProducer.Close()
		return nil, fmt.Errorf("signal outConsumer: %w", err)
	}

	// Client→Server: DataProducer on SendTransport (pre-created), DataConsumer on DirectTransport
	ordered := true
	inProducer, err := send.ProduceData(&mediasoup.DataProducerOptions{
		SctpStreamParameters: &mediasoup.SctpStreamParameters{
			StreamId: 0,
			Ordered:  &ordered,
		},
		Label:    "signal",
		Protocol: "json",
	})
	if err != nil {
		outConsumer.Close()
		outProducer.Close()
		return nil, fmt.Errorf("signal inProducer: %w", err)
	}

	inConsumer, err := direct.ConsumeData(&mediasoup.DataConsumerOptions{
		DataProducerId: inProducer.Id(),
	})
	if err != nil {
		inProducer.Close()
		outConsumer.Close()
		outProducer.Close()
		return nil, fmt.Errorf("signal inConsumer: %w", err)
	}

	info := &proto.SignalInfo{
		Receive: &proto.SignalChannelInfo{
			ChannelSid:           outConsumer.Id(),
			DataSourceSid:        outProducer.Id(),
			SctpStreamParameters: outConsumer.SctpStreamParameters(),
			Label:                outConsumer.Label(),
			Protocol:             outConsumer.Protocol(),
		},
		Send: &proto.SignalChannelInfo{
			ChannelSid:           inProducer.Id(),
			SctpStreamParameters: inProducer.SctpStreamParameters(),
			Label:                "signal",
			Protocol:             "json",
		},
	}

	return &signalChannels{
		outProducer: outProducer,
		outConsumer: outConsumer,
		inProducer:  inProducer,
		inConsumer:  inConsumer,
		info:        info,
	}, nil
}

func connectTransports(dtlsParams *mediasoup.DtlsParameters, transports ...*mediasoup.Transport) error {
	for _, t := range transports {
		if err := t.Connect(&mediasoup.TransportConnectOptions{DtlsParameters: dtlsParams}); err != nil {
			return err
		}
	}
	return nil
}

func (s *HTTPServer) transportToProto(t *mediasoup.Transport) *proto.TransportInfo {
	data := t.Data()
	info := &proto.TransportInfo{
		Sid: t.Id(),
	}
	if d := data.WebRtcTransportData; d != nil {
		info.IceParameters = &d.IceParameters
		info.IceCandidates = d.IceCandidates
		info.DtlsParameters = &d.DtlsParameters
		info.SctpParameters = d.SctpParameters
	}
	return info
}

// handleConnect is the fallback for clients that do not support
// RTCPeerConnection.generateCertificate() and cannot provide
// DtlsFingerprints in JoinRequest.
func (s *HTTPServer) handleConnect(w http.ResponseWriter, r *http.Request) {
	logId, _ := r.Context().Value(logIdKey{}).(string)

	var req proto.ConnectRequest
	if err := readRequest(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: err.Error()})
		return
	}

	if req.NodeId != "" && types.NodeID(req.NodeId) != s.node.ID && s.nodeRPC != nil {
		payload, _ := json.Marshal(req)
		if err := s.forwardViaNATS(w, types.NodeID(req.NodeId), NodeRPCActionConnect, payload); err != nil {
			writeJSON(w, http.StatusBadGateway, proto.ErrorResponse{
				Code: proto.ErrCodeInternal, Error: "failed to reach target node"})
		}
		return
	}

	participant := s.roomManager.GetParticipantBySid(req.ParticipantSid)
	if participant == nil {
		writeJSON(w, http.StatusNotFound, proto.ErrorResponse{
			Code: proto.ErrCodeNotFound, Error: "participant not found"})
		return
	}

	s.handleConnectLocal(w, &req, participant, logId)
}

func (s *HTTPServer) handleConnectLocal(w http.ResponseWriter, req *proto.ConnectRequest, participant *rtc.Participant, logId string) {
	for _, tc := range req.Transports {
		transport := participant.GetTransportBySid(tc.TransportSid)
		if transport == nil {
			writeJSON(w, http.StatusNotFound, proto.ErrorResponse{
				Code: proto.ErrCodeTransportNotFound, Error: "transport not found: " + tc.TransportSid})
			return
		}
		if err := transport.Connect(&mediasoup.TransportConnectOptions{
			DtlsParameters: tc.DtlsParameters,
		}); err != nil {
			writeJSON(w, http.StatusInternalServerError, proto.ErrorResponse{
				Code: proto.ErrCodeInternal, Error: "connect failed: " + err.Error()})
			return
		}
	}

	w.Header().Set("X-Node-Id", string(s.node.ID))
	w.Header().Set("X-Log-Id", logId)

	writeJSON(w, http.StatusOK, proto.ConnectResponse{})
}

// ============================================================
// Reconnect
// ============================================================

func (s *HTTPServer) handleReconnect(w http.ResponseWriter, req *proto.JoinRequest, logId string) {
	s.handleReconnectLocal(w, req, logId)
}

func (s *HTTPServer) handleReconnectLocal(w http.ResponseWriter, req *proto.JoinRequest, logId string) {
	// Validate reconnect credentials: prefer refreshToken, fall back to original token.
	// Anonymous users always have a refreshToken from their initial join.
	tokenToVerify := req.RefreshToken
	if tokenToVerify == "" {
		tokenToVerify = req.Token
	}
	if tokenToVerify == "" && !s.cfg.Auth.AllowAnonymous {
		writeJSON(w, http.StatusUnauthorized, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidToken, Error: "refresh token or token required for reconnect"})
		return
	}
	if tokenToVerify != "" {
		if _, err := s.verifier.Verify(tokenToVerify); err != nil {
			writeJSON(w, http.StatusUnauthorized, proto.ErrorResponse{
				Code: proto.ErrCodeInvalidToken, Error: err.Error()})
			return
		}
	}

	participant := s.roomManager.GetParticipantBySid(req.ParticipantSid)
	if participant != nil {
		// Fast path: server still has session — ICE restart only.
		s.handleIceRestart(w, participant, logId)
		return
	}

	// Slow path: session lost (e.g. server restarted) — full rejoin.
	s.handleFullReconnect(w, req, logId)
}

// handleIceRestart performs ICE restart on existing transports without
// tearing down the participant session.
func (s *HTTPServer) handleIceRestart(w http.ResponseWriter, participant *rtc.Participant, logId string) {
	iceParams, err := participant.RestartIce()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, proto.ErrorResponse{
			Code: proto.ErrCodeInternal, Error: "ICE restart failed: " + err.Error()})
		return
	}

	w.Header().Set("X-Node-Id", string(s.node.ID))
	w.Header().Set("X-Log-Id", logId)

	writeJSON(w, http.StatusOK, proto.ReconnectResponse{IceParameters: iceParams})

	slog.Info("Participant ICE restarted", "sid", participant.Sid(), "logId", logId)
}

// handleFullReconnect executes a full join flow and sets FullReconnect=true
// so the client knows to clean up old resources and re-publish tracks.
// If the original token is missing/expired but a valid refreshToken is present,
// the refreshToken is promoted to the primary token for the join flow.
func (s *HTTPServer) handleFullReconnect(w http.ResponseWriter, req *proto.JoinRequest, logId string) {
	if req.Token == "" && req.RefreshToken != "" {
		req.Token = req.RefreshToken
	}
	req.Reconnect = false
	s.handleJoinLocal(w, req, logId)
}

// ============================================================
// NATS RPC Forwarding
// ============================================================

func (s *HTTPServer) forwardViaNATS(w http.ResponseWriter, targetNode types.NodeID, action string, payload json.RawMessage) error {
	if s.nodeRPC == nil {
		return fmt.Errorf("node rpc not configured")
	}
	resp, err := s.nodeRPC.Request(string(targetNode), action, payload)
	if err != nil {
		slog.Error("Node RPC forward failed", "target", targetNode, "action", action, "error", err)
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Node-Id", string(targetNode))
	w.WriteHeader(resp.Status)
	_, _ = w.Write(resp.Payload)
	return nil
}
