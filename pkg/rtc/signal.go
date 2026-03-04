package rtc

import (
	"encoding/json"
	"time"

	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/jiyeyuran/omnistream/pkg/proto"
)

// dispatchRPC routes incoming datachannel RPC messages to the appropriate handler.
// This is the central dispatch for all client->server RPC methods.
func (p *Participant) dispatchRPC(msg *proto.RPCMessage) {
	var (
		body   any
		status int
		errMsg string
	)

	switch msg.Header.Method {
	case proto.MethodReady:
		body, status, errMsg = p.handleReady(msg)

	case proto.MethodPing:
		body, status, errMsg = p.handlePingRPC(msg)

	case proto.MethodLeave:
		p.handleLeave(msg)
		return

	case proto.MethodTokenRefresh:
		body, status, errMsg = p.handleTokenRefresh(msg)

	case proto.MethodTrackPublish:
		body, status, errMsg = p.handleTrackPublish(msg)

	case proto.MethodTrackUnpublish:
		body, status, errMsg = p.handleTrackUnpublish(msg)

	case proto.MethodTrackMute:
		body, status, errMsg = p.handleTrackMute(msg)

	case proto.MethodTrackUpdate:
		body, status, errMsg = p.handleTrackUpdate(msg)

	case proto.MethodDataTrackPublish:
		body, status, errMsg = p.handleDataTrackPublish(msg)

	case proto.MethodDataTrackUnpublish:
		body, status, errMsg = p.handleDataTrackUnpublish(msg)

	case proto.MethodSubscriptionPause:
		body, status, errMsg = p.handleSubscriptionPause(msg)

	case proto.MethodSubscriptionResume:
		body, status, errMsg = p.handleSubscriptionResume(msg)

	case proto.MethodSubscriptionSetPreferredLayers:
		body, status, errMsg = p.handleSubscriptionSetPreferredLayers(msg)

	case proto.MethodSubscriptionSetPriority:
		body, status, errMsg = p.handleSubscriptionSetPriority(msg)

	case proto.MethodSubscriptionRequestKeyFrame:
		body, status, errMsg = p.handleSubscriptionRequestKeyFrame(msg)

	case proto.MethodSubscriptionClose:
		body, status, errMsg = p.handleSubscriptionClose(msg)

	case proto.MethodDataSubscriptionClose:
		body, status, errMsg = p.handleDataSubscriptionClose(msg)

	case proto.MethodTransportGetStats:
		body, status, errMsg = p.handleTransportGetStats(msg)

	case proto.MethodParticipantUpdate:
		body, status, errMsg = p.handleParticipantUpdate(msg)

	default:
		status = proto.StatusNotFound
		errMsg = "unknown method: " + msg.Header.Method
	}

	if msg.Header.LogId != "" {
		resp, _ := proto.NewResponse(msg.Header.Method, msg.Header.LogId, status, errMsg, body)
		p.sendMessage(resp)
	}
}

func (p *Participant) handleReady(msg *proto.RPCMessage) (any, int, string) {
	var req proto.ReadyRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request body"
	}

	p.SetRtpCapabilities(req.RtpCapabilities)
	p.SetState(proto.ParticipantInfo_JOINED)
	p.StartHeartbeat()

	others := p.room.GetOtherParticipants(p.sid)
	otherInfos := make([]*proto.ParticipantInfo, len(others))
	for i, o := range others {
		otherInfos[i] = o.ToProto()
	}

	// Notify others that this participant joined
	p.room.BroadcastNotification(proto.MethodParticipantJoined,
		&proto.ParticipantJoinedNotification{Participant: p.ToProto()}, p.sid)

	// Subscribe to existing tracks in the room
	go p.room.SubscribeExistingTracks(p)

	return &proto.ReadyResponse{OtherParticipants: otherInfos}, proto.StatusOK, ""
}

func (p *Participant) handlePingRPC(msg *proto.RPCMessage) (any, int, string) {
	var req proto.PingRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid ping"
	}

	p.HandlePing(&req)

	return &proto.PongResponse{
		LastTimestamp: req.Timestamp,
		ServerTime:    time.Now().UnixMilli(),
	}, proto.StatusOK, ""
}

func (p *Participant) handleLeave(msg *proto.RPCMessage) {
	p.logger.Info("Client requested leave")
	p.Close(proto.DisconnectReason_CLIENT_INITIATED)
}

func (p *Participant) handleTokenRefresh(msg *proto.RPCMessage) (any, int, string) {
	// Token refresh is handled at a higher level; the participant just
	// stores the new grants. For now, acknowledge the request.
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleTrackPublish(msg *proto.RPCMessage) (any, int, string) {
	var req proto.PublishTrackRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}

	track, err := p.PublishTrack(&req)
	if err != nil {
		return nil, proto.StatusInternalError, err.Error()
	}

	return &proto.PublishTrackResponse{TrackSid: track.sid}, proto.StatusOK, ""
}

func (p *Participant) handleTrackUnpublish(msg *proto.RPCMessage) (any, int, string) {
	var req proto.UnpublishTrackRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}
	if err := p.UnpublishTrack(req.TrackSid); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleTrackMute(msg *proto.RPCMessage) (any, int, string) {
	var req proto.MuteTrackRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}
	if err := p.MuteTrack(req.TrackSid, req.Muted); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleTrackUpdate(msg *proto.RPCMessage) (any, int, string) {
	var req proto.UpdateTrackRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}
	if err := p.UpdateTrack(&req); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleDataTrackPublish(msg *proto.RPCMessage) (any, int, string) {
	var req proto.PublishDataTrackRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}
	track, err := p.PublishDataTrack(&req)
	if err != nil {
		return nil, proto.StatusInternalError, err.Error()
	}
	return &proto.PublishDataTrackResponse{DataTrackSid: track.sid}, proto.StatusOK, ""
}

func (p *Participant) handleDataTrackUnpublish(msg *proto.RPCMessage) (any, int, string) {
	var req proto.UnpublishDataTrackRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}
	if err := p.UnpublishDataTrack(req.DataTrackSid); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleSubscriptionPause(msg *proto.RPCMessage) (any, int, string) {
	var req proto.PauseSubscriptionRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}
	if err := p.PauseSubscription(req.SubscriptionSid); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleSubscriptionResume(msg *proto.RPCMessage) (any, int, string) {
	var req proto.ResumeSubscriptionRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}
	if err := p.ResumeSubscription(req.SubscriptionSid); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleSubscriptionSetPreferredLayers(msg *proto.RPCMessage) (any, int, string) {
	var req proto.SetPreferredLayersRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}
	if err := p.SetPreferredLayers(&req); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleSubscriptionSetPriority(msg *proto.RPCMessage) (any, int, string) {
	var req proto.SetPriorityRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}
	if err := p.SetPriority(req.SubscriptionSid, req.Priority); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleSubscriptionRequestKeyFrame(msg *proto.RPCMessage) (any, int, string) {
	var req proto.RequestKeyFrameRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}
	if err := p.RequestKeyFrame(req.SubscriptionSid); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleSubscriptionClose(msg *proto.RPCMessage) (any, int, string) {
	var req proto.CloseSubscriptionRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}
	if err := p.CloseSubscription(req.SubscriptionSid); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleDataSubscriptionClose(msg *proto.RPCMessage) (any, int, string) {
	var req proto.CloseDataSubscriptionRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}
	if err := p.CloseDataSubscription(req.DataSubscriptionSid); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleTransportGetStats(msg *proto.RPCMessage) (any, int, string) {
	var req proto.GetTransportStatsRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}

	var transport *mediasoup.Transport
	switch req.TransportSid {
	case p.SendTransportSid():
		transport = p.sendTransport
	case p.RecvTransportSid():
		transport = p.recvTransport
	default:
		return nil, proto.StatusNotFound, "transport not found"
	}

	if transport == nil {
		return nil, proto.StatusNotFound, "transport not available"
	}

	stat, err := transport.GetStats()
	if err != nil {
		return nil, proto.StatusInternalError, err.Error()
	}

	statsJSON, _ := json.Marshal(stat)
	return &proto.TransportStatsResponse{Stats: statsJSON}, proto.StatusOK, ""
}

func (p *Participant) handleParticipantUpdate(msg *proto.RPCMessage) (any, int, string) {
	var req proto.UpdateParticipantRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}

	if (req.Metadata != nil || len(req.Attributes) > 0) && p.permission != nil && !p.permission.CanUpdateMetadata {
		return nil, proto.StatusForbidden, "no permission to update metadata"
	}

	p.UpdateMetadata(req.Metadata, req.Name, req.Attributes)
	return nil, proto.StatusOK, ""
}
