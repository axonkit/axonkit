package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/jiyeyuran/omnistream/pkg/auth"
	"github.com/jiyeyuran/omnistream/pkg/config"
	"github.com/jiyeyuran/omnistream/pkg/proto"
	"github.com/jiyeyuran/omnistream/pkg/rtc"
	"github.com/jiyeyuran/omnistream/pkg/types"
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

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

type Server struct {
	cfg         *config.Config
	roomManager *rtc.RoomManager
	verifier    *auth.TokenVerifier
	node        types.NodeInfo
	routers     []*mediasoup.Router
}

type ServerParams struct {
	Config      *config.Config
	RoomManager *rtc.RoomManager
	Verifier    *auth.TokenVerifier
	Node        types.NodeInfo
	Routers     []*mediasoup.Router
}

func NewServer(params ServerParams) *Server {
	return &Server{
		cfg:         params.Config,
		roomManager: params.RoomManager,
		verifier:    params.Verifier,
		node:        params.Node,
		routers:     params.Routers,
	}
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /rtc/join", s.handleJoin)
	mux.HandleFunc("POST /rtc/connect", s.handleConnect)
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	logId, _ := r.Context().Value(logIdKey{}).(string)

	var req proto.JoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, proto.ErrorResponse{
			Code: proto.ErrCodeInvalidRequest, Error: err.Error()})
		return
	}

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
		PingInterval:  time.Duration(s.cfg.PingInterval()) * time.Second,
		PingTimeout:   time.Duration(s.cfg.PingTimeout()) * time.Second,
	})

	sendTransport, recvTransport, directTransport, err := s.createTransports(router)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, proto.ErrorResponse{
			Code: proto.ErrCodeInternal, Error: "transport creation failed: " + err.Error()})
		return
	}
	participant.SetTransports(sendTransport, recvTransport, directTransport)

	// Connect transports immediately using pre-generated DTLS fingerprints.
	// mediasoup is ICE-Lite so the remote (client) DTLS role is always "client".
	if req.ConnectionSettings != nil && len(req.ConnectionSettings.DtlsFingerprints) > 0 {
		dtlsParams := &mediasoup.DtlsParameters{
			Role:         mediasoup.DtlsRoleClient,
			Fingerprints: req.ConnectionSettings.DtlsFingerprints,
		}
		for _, t := range []*mediasoup.Transport{sendTransport, recvTransport} {
			err := t.Connect(&mediasoup.TransportConnectOptions{
				DtlsParameters: dtlsParams,
			})
			if err != nil {
				sendTransport.Close()
				recvTransport.Close()
				directTransport.Close()
				writeJSON(w, http.StatusInternalServerError, proto.ErrorResponse{
					Code: proto.ErrCodeInternal, Error: "transport connect failed: " + err.Error()})
				return
			}
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
	}

	w.Header().Set("X-Node-Id", string(s.node.ID))
	w.Header().Set("X-Log-Id", logId)

	writeJSON(w, http.StatusOK, resp)

	slog.Info("Participant joined", "identity", identity, "sid", participantSid,
		"room", req.Room.Name, "logId", logId)
}

// ============================================================
// Transport Helpers
// ============================================================

func (s *Server) createTransports(router *mediasoup.Router) (send, recv, direct *mediasoup.Transport, err error) {
	enableUdp := true
	webRtcOpts := &mediasoup.WebRtcTransportOptions{
		ListenInfos: []mediasoup.TransportListenInfo{
			{
				Ip:               s.cfg.Media.WebRtc.ListenIP,
				AnnouncedAddress: s.cfg.Media.WebRtc.AnnouncedIP,
				Protocol:         mediasoup.TransportProtocolUDP,
			},
		},
		EnableUdp:  &enableUdp,
		EnableTcp:  true,
		PreferUdp:  true,
		EnableSctp: true,
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

	participant := s.roomManager.GetParticipantBySid(req.ParticipantSid)
	if participant == nil {
		writeJSON(w, http.StatusNotFound, proto.ErrorResponse{
			Code: proto.ErrCodeNotFound, Error: "participant not found"})
		return
	}

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
