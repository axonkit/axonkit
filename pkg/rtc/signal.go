package rtc

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/jiyeyuran/mediasoup-go/v2"
)

// dispatchRPC routes incoming datachannel RPC messages to the appropriate handler.
func (p *Participant) dispatchRPC(msg *proto.Message) {
	var (
		body   any
		status int
		errMsg string
	)

	if msg.Method != proto.MethodPing {
		p.logger.Info("dispatchRPC", "method", msg.Method, "log_id", msg.LogId, "data", string(msg.Data))
	}

	switch msg.Method {
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
		errMsg = "unknown method: " + msg.Method
	}

	if msg.IsRequest() {
		var resp *proto.Message
		if errMsg != "" {
			p.logger.Error("dispatchRPC", "method", msg.Method, "log_id", msg.LogId, "status", status, "err_msg", errMsg)
			resp = proto.NewErrorResponse(msg.LogId, status, errMsg)
		} else {
			var err error
			resp, err = proto.NewJSONResponse(msg.LogId, status, body)
			if err != nil {
				resp = proto.NewErrorResponse(msg.LogId, proto.StatusInternalError, "marshal response failed")
			}
		}
		_ = p.sendMessage(resp)
	}
}

func (p *Participant) handleReady(msg *proto.Message) (any, int, string) {
	var req proto.ReadyRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request body"
	}

	p.SetRtpCapabilities(req.RtpCapabilities)
	p.SetState(proto.ParticipantInfo_JOINED)
	p.StartHeartbeat()

	p.mu.Lock()
	syncState := p.pendingSyncState
	p.pendingSyncState = nil
	p.mu.Unlock()

	isReconnect := syncState != nil

	others := p.room.GetOtherParticipants(p.sid)
	otherInfos := make([]*proto.ParticipantInfo, len(others))
	for i, o := range others {
		otherInfos[i] = o.ToProto()
	}

	if isReconnect {
		go p.processReconnectSync(syncState)
	} else {
		p.room.BroadcastNotification(proto.MethodParticipantJoined,
			&proto.ParticipantJoinedNotification{Participant: p.ToProto()}, p.sid)
		go p.room.SubscribeExistingTracks(p)
	}

	return &proto.ReadyResponse{OtherParticipants: otherInfos}, proto.StatusOK, ""
}

// processReconnectSync compares the client's last-known state with the current
// room state, sends notifications about changes, and re-subscribes to all
// current tracks.
func (p *Participant) processReconnectSync(syncState *proto.SyncState) {
	if p.room == nil {
		return
	}

	// Build set of trackSids the client previously knew about.
	knownTracks := make(map[string]bool, len(syncState.SubscribedTrackSids))
	for _, sid := range syncState.SubscribedTrackSids {
		knownTracks[sid] = true
	}
	knownDataTracks := make(map[string]bool, len(syncState.SubscribedDataTrackSids))
	for _, sid := range syncState.SubscribedDataTrackSids {
		knownDataTracks[sid] = true
	}

	// Collect current tracks in the room from all other participants.
	currentTracks := make(map[string]struct{})
	currentDataTracks := make(map[string]struct{})

	others := p.room.GetOtherParticipants(p.sid)
	for _, pub := range others {
		for _, t := range pub.GetPublishedTracks() {
			currentTracks[t.sid] = struct{}{}
			if !knownTracks[t.sid] {
				// New track the client didn't know about.
				_ = p.SendNotification(proto.MethodTrackPublished, &proto.TrackPublishedNotification{
					ParticipantSid: pub.sid,
					Track:          t.ToProto(),
				})
			}
		}
		for _, dt := range pub.GetDataPublishedTracks() {
			currentDataTracks[dt.sid] = struct{}{}
			if !knownDataTracks[dt.sid] {
				// TODO: send data track published notification when protocol is added
				p.logger.Debug("New data track since disconnect", "dataTrackSid", dt.sid)
			}
		}
	}

	// Notify about tracks that were removed while disconnected.
	for _, sid := range syncState.SubscribedTrackSids {
		if _, exists := currentTracks[sid]; !exists {
			_ = p.SendNotification(proto.MethodTrackUnpublished, &proto.TrackUnpublishedNotification{
				TrackSid: sid,
			})
		}
	}
	for _, sid := range syncState.SubscribedDataTrackSids {
		if _, exists := currentDataTracks[sid]; !exists {
			_ = p.SendNotification(proto.MethodDataSubscriptionClosed, &proto.DataSubscriptionClosedNotification{
				DataSubscriptionSid: sid,
			})
		}
	}

	// Re-subscribe to all current tracks.
	p.room.SubscribeExistingTracks(p)

	p.logger.Info("Reconnect sync completed",
		"knownTracks", len(syncState.SubscribedTrackSids),
		"currentTracks", len(currentTracks))
}

func (p *Participant) handlePingRPC(msg *proto.Message) (any, int, string) {
	var req proto.PingRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid ping"
	}

	p.HandlePing(&req)

	return &proto.PongResponse{
		LastTimestamp: req.Timestamp,
		ServerTime:    time.Now().UnixMilli(),
	}, proto.StatusOK, ""
}

func (p *Participant) handleLeave(msg *proto.Message) {
	p.logger.Info("Client requested leave")
	p.Close(proto.DisconnectReason_CLIENT_INITIATED)
}

func (p *Participant) handleTokenRefresh(msg *proto.Message) (any, int, string) {
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleTrackPublish(msg *proto.Message) (any, int, string) {
	var req proto.PublishTrackRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}

	track, err := p.PublishTrack(&req)
	if err != nil {
		return nil, proto.StatusInternalError, err.Error()
	}

	return &proto.PublishTrackResponse{TrackSid: track.sid}, proto.StatusOK, ""
}

func (p *Participant) handleTrackUnpublish(msg *proto.Message) (any, int, string) {
	var req proto.UnpublishTrackRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}
	if err := p.UnpublishTrack(req.TrackSid); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleTrackMute(msg *proto.Message) (any, int, string) {
	var req proto.MuteTrackRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}
	if err := p.MuteTrack(req.TrackSid, req.Muted); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleTrackUpdate(msg *proto.Message) (any, int, string) {
	var req proto.UpdateTrackRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}
	if err := p.UpdateTrack(&req); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleDataTrackPublish(msg *proto.Message) (any, int, string) {
	var req proto.PublishDataTrackRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}
	track, err := p.PublishDataTrack(&req)
	if err != nil {
		return nil, proto.StatusInternalError, err.Error()
	}
	return &proto.PublishDataTrackResponse{DataTrackSid: track.sid}, proto.StatusOK, ""
}

func (p *Participant) handleDataTrackUnpublish(msg *proto.Message) (any, int, string) {
	var req proto.UnpublishDataTrackRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}
	if err := p.UnpublishDataTrack(req.DataTrackSid); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleSubscriptionPause(msg *proto.Message) (any, int, string) {
	var req proto.PauseSubscriptionRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}
	if err := p.PauseSubscription(req.SubscriptionSid); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleSubscriptionResume(msg *proto.Message) (any, int, string) {
	var req proto.ResumeSubscriptionRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}
	if err := p.ResumeSubscription(req.SubscriptionSid); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleSubscriptionSetPreferredLayers(msg *proto.Message) (any, int, string) {
	var req proto.SetPreferredLayersRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}
	if err := p.SetPreferredLayers(&req); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleSubscriptionSetPriority(msg *proto.Message) (any, int, string) {
	var req proto.SetPriorityRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}
	if err := p.SetPriority(req.SubscriptionSid, req.Priority); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleSubscriptionRequestKeyFrame(msg *proto.Message) (any, int, string) {
	var req proto.RequestKeyFrameRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}
	if err := p.RequestKeyFrame(req.SubscriptionSid); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleSubscriptionClose(msg *proto.Message) (any, int, string) {
	var req proto.CloseSubscriptionRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}
	if err := p.CloseSubscription(req.SubscriptionSid); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleDataSubscriptionClose(msg *proto.Message) (any, int, string) {
	var req proto.CloseDataSubscriptionRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}
	if err := p.CloseDataSubscription(req.DataSubscriptionSid); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleTransportGetStats(msg *proto.Message) (any, int, string) {
	var req proto.GetTransportStatsRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
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

func (p *Participant) handleParticipantUpdate(msg *proto.Message) (any, int, string) {
	var req proto.UpdateParticipantRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}

	if (req.Metadata != nil || len(req.Attributes) > 0) && p.permission != nil && !p.permission.CanUpdateMetadata {
		return nil, proto.StatusForbidden, "no permission to update metadata"
	}

	p.UpdateMetadata(req.Metadata, req.Name, req.Attributes)
	return nil, proto.StatusOK, ""
}
