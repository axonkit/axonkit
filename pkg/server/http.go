package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/axonkit/axonkit/pkg/auth"
	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/rtc"
	"github.com/axonkit/axonkit/pkg/types"
	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/nats-io/nats.go"
	"github.com/rs/xid"
)

type logIdKey struct{}

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *loggingResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *loggingResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
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
		lrw := &loggingResponseWriter{ResponseWriter: w}
		ctx := context.WithValue(r.Context(), logIdKey{}, logId)
		next.ServeHTTP(lrw, r.WithContext(ctx))
		slog.Info("HTTP request", "method", r.Method, "path", r.URL.Path,
			"status", lrw.status, "bytes", lrw.bytes,
			"logId", logId, "duration_ms", time.Since(start).Milliseconds())
	})
}

func WithCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		// w.Header().Set("Access-Control-Max-Age", "86400")

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

type Server struct {
	cfg           *config.Config
	roomManager   *rtc.RoomManager
	verifier      *auth.TokenVerifier
	node          types.NodeInfo
	nc            *nats.Conn
	routers       []*mediasoup.Router
	webRtcServers []*mediasoup.WebRtcServer
}

type ServerParams struct {
	Config        *config.Config
	RoomManager   *rtc.RoomManager
	Verifier      *auth.TokenVerifier
	Node          types.NodeInfo
	NC            *nats.Conn
	Routers       []*mediasoup.Router
	WebRtcServers []*mediasoup.WebRtcServer
}

func NewServer(params ServerParams) *Server {
	return &Server{
		cfg:           params.Config,
		roomManager:   params.RoomManager,
		verifier:      params.Verifier,
		node:          params.Node,
		nc:            params.NC,
		routers:       params.Routers,
		webRtcServers: params.WebRtcServers,
	}
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /rtc/join", s.handleJoin)
	mux.HandleFunc("POST /rtc/connect", s.handleConnect)
	mux.HandleFunc("OPTIONS /rtc/join", func(w http.ResponseWriter, r *http.Request) {})
	mux.HandleFunc("OPTIONS /rtc/connect", func(w http.ResponseWriter, r *http.Request) {})
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	logId, _ := r.Context().Value(logIdKey{}).(string)

	var req proto.JoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: err.Error()})
		return
	}

	// ---- Node forwarding: if the client specifies a target node, proxy to it ----
	if req.NodeId != "" && types.NodeID(req.NodeId) != s.node.ID && s.nc != nil {
		payload, _ := json.Marshal(req)
		action := "join"
		if req.Reconnect {
			action = "reconnect"
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

func (s *Server) handleJoinLocal(w http.ResponseWriter, req *proto.JoinRequest, logId string) {
	var claims *auth.Claims
	if !s.cfg.Debug {
		if req.Token == "" {
			writeJSON(w, http.StatusUnauthorized, proto.ErrorResponse{
				Code: proto.ErrCodeInvalidToken, Error: "token required"})
			return
		}
		var err error
		claims, err = s.verifier.Verify(req.Token)
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, proto.ErrorResponse{
				Code: proto.ErrCodeInvalidToken, Error: err.Error()})
			return
		}
	} else {
		roomName := ""
		if req.Room != nil {
			roomName = req.Room.Name
		}
		claims = &auth.Claims{
			Sub:  req.DisplayName,
			Name: req.DisplayName,
			Grants: &proto.ClaimGrants{
				Identity: req.DisplayName,
				Video: &proto.VideoGrant{
					RoomJoin: true,
					Room:     roomName,
				},
			},
		}
	}

	if req.Room == nil || req.Room.Name == "" {
		writeJSON(w, http.StatusBadRequest, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: "room name required"})
		return
	}

	room := s.roomManager.GetOrCreateRoom(
		req.Room.Name,
		int(req.Room.MaxParticipants),
		req.Room.Metadata,
	)

	routerIdx := int(time.Now().UnixNano() % int64(len(s.routers)))
	if routerIdx < 0 {
		routerIdx = -routerIdx
	}
	router := s.routers[routerIdx]
	webRtcServer := s.webRtcServers[routerIdx]
	participantSid := xid.New().String()

	identity := claims.Identity()
	if identity == "" {
		identity = req.DisplayName
	}

	autoSubscribe := s.cfg.Room.AutoSubscribe
	if req.ConnectionSettings != nil {
		autoSubscribe = req.ConnectionSettings.AutoSubscribe
	}

	participant := rtc.NewParticipant(rtc.ParticipantParams{
		Sid:           participantSid,
		Identity:      identity,
		Name:          claims.Name,
		Metadata:      req.Metadata,
		Attributes:    req.ParticipantAttributes,
		Grants:        claims.Grants,
		Router:        router,
		AutoSubscribe: autoSubscribe,
		PingInterval:  s.cfg.Room.PingInterval,
		PingTimeout:   s.cfg.Room.PingTimeout,
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
		writeJSON(w, http.StatusConflict, proto.ErrorResponse{
			Code: proto.ErrCodeRoomFull, Error: err.Error()})
		return
	}

	fullReconnect := req.ParticipantSid != ""

	resp := proto.JoinResponse{
		Room:         room.ToProto(),
		Participant:  participant.ToProto(),
		PingTimeout:  s.cfg.PingTimeout(),
		PingInterval: s.cfg.PingInterval(),
		ServerInfo: &proto.ServerInfo{
			NodeId:   string(s.node.ID),
			Region:   string(s.node.Region),
			Protocol: 1,
		},
		SendTransport:    s.transportToProto(sendTransport),
		ReceiveTransport: s.transportToProto(recvTransport),
		Signal:           sig.info,
		RtpCapabilities:  router.RtpCapabilities(),
		FullReconnect:    fullReconnect,
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

func (s *Server) createTransports(router *mediasoup.Router, webRtcServer *mediasoup.WebRtcServer) (send, recv, direct *mediasoup.Transport, err error) {
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

func (s *Server) createSignalChannels(send, recv, direct *mediasoup.Transport) (*signalChannels, error) {
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

func (s *Server) transportToProto(t *mediasoup.Transport) *proto.TransportInfo {
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
func (s *Server) handleConnect(w http.ResponseWriter, r *http.Request) {
	logId, _ := r.Context().Value(logIdKey{}).(string)

	var req proto.ConnectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: err.Error()})
		return
	}

	if req.NodeId != "" && types.NodeID(req.NodeId) != s.node.ID && s.nc != nil {
		payload, _ := json.Marshal(req)
		if err := s.forwardViaNATS(w, types.NodeID(req.NodeId), "connect", payload); err != nil {
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

func (s *Server) handleConnectLocal(w http.ResponseWriter, req *proto.ConnectRequest, participant *rtc.Participant, logId string) {
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

func (s *Server) handleReconnect(w http.ResponseWriter, req *proto.JoinRequest, logId string) {
	s.handleReconnectLocal(w, req, logId)
}

func (s *Server) handleReconnectLocal(w http.ResponseWriter, req *proto.JoinRequest, logId string) {
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
func (s *Server) handleIceRestart(w http.ResponseWriter, participant *rtc.Participant, logId string) {
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
func (s *Server) handleFullReconnect(w http.ResponseWriter, req *proto.JoinRequest, logId string) {
	req.Reconnect = false
	s.handleJoinLocal(w, req, logId)
}

// ============================================================
// NATS RPC Forwarding
// ============================================================

func (s *Server) forwardViaNATS(w http.ResponseWriter, targetNode types.NodeID, action string, payload json.RawMessage) error {
	rpcReq := proto.NodeRPCRequest{Action: action, Payload: payload}
	data, _ := json.Marshal(rpcReq)

	subject := fmt.Sprintf("rtc.rpc.%s", targetNode)
	msg, err := s.nc.Request(subject, data, 5*time.Second)
	if err != nil {
		slog.Error("NATS forward failed", "target", targetNode, "action", action, "error", err)
		return err
	}

	var rpcResp proto.NodeRPCResponse
	if err := json.Unmarshal(msg.Data, &rpcResp); err != nil {
		writeJSON(w, http.StatusBadGateway, proto.ErrorResponse{
			Code: proto.ErrCodeInternal, Error: "invalid response from target node"})
		return nil
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Node-Id", string(targetNode))
	w.WriteHeader(rpcResp.Status)
	_, _ = w.Write(rpcResp.Payload)
	return nil
}

// HandleNodeRPC processes forwarded requests from other nodes via NATS.
// Each node subscribes to "rtc.rpc.<nodeID>".
func (s *Server) HandleNodeRPC(msg *nats.Msg) {
	var req proto.NodeRPCRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		resp := proto.NodeRPCResponse{Status: http.StatusBadRequest}
		resp.Payload, _ = json.Marshal(proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: err.Error()})
		data, _ := json.Marshal(resp)
		_ = msg.Respond(data)
		return
	}

	rec := &responseRecorder{}

	switch req.Action {
	case "join":
		var joinReq proto.JoinRequest
		if err := json.Unmarshal(req.Payload, &joinReq); err != nil {
			rec.status = http.StatusBadRequest
			rec.body, _ = json.Marshal(proto.ErrorResponse{
				Code: proto.ErrCodeInvalidRequest, Error: err.Error()})
		} else {
			s.handleJoinLocal(rec, &joinReq, "")
		}

	case "reconnect":
		var joinReq proto.JoinRequest
		if err := json.Unmarshal(req.Payload, &joinReq); err != nil {
			rec.status = http.StatusBadRequest
			rec.body, _ = json.Marshal(proto.ErrorResponse{
				Code: proto.ErrCodeInvalidRequest, Error: err.Error()})
		} else {
			s.handleReconnectLocal(rec, &joinReq, "")
		}

	case "connect":
		var connReq proto.ConnectRequest
		if err := json.Unmarshal(req.Payload, &connReq); err != nil {
			rec.status = http.StatusBadRequest
			rec.body, _ = json.Marshal(proto.ErrorResponse{
				Code: proto.ErrCodeInvalidRequest, Error: err.Error()})
		} else {
			participant := s.roomManager.GetParticipantBySid(connReq.ParticipantSid)
			if participant == nil {
				rec.status = http.StatusNotFound
				rec.body, _ = json.Marshal(proto.ErrorResponse{
					Code: proto.ErrCodeNotFound, Error: "participant not found"})
			} else {
				s.handleConnectLocal(rec, &connReq, participant, "")
			}
		}

	default:
		rec.status = http.StatusBadRequest
		rec.body, _ = json.Marshal(proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: "unknown action: " + req.Action})
	}

	if rec.status == 0 {
		rec.status = http.StatusOK
	}
	rpcResp := proto.NodeRPCResponse{Status: rec.status, Payload: rec.body}
	data, _ := json.Marshal(rpcResp)
	_ = msg.Respond(data)
}

// responseRecorder captures the status and body from handler methods so they
// can be relayed over NATS instead of HTTP.
type responseRecorder struct {
	headers http.Header
	status  int
	body    json.RawMessage
}

func (r *responseRecorder) Header() http.Header {
	if r.headers == nil {
		r.headers = make(http.Header)
	}
	return r.headers
}

func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
}

func (r *responseRecorder) Write(data []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	r.body = data
	return len(data), nil
}
