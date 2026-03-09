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

	switch msg.Method {
	case proto.MethodPing, proto.MethodStatsReport,
		proto.MethodChatMessage, proto.MethodRpcRequest, proto.MethodRpcResponse:
	default:
		p.logger.Info("dispatchRPC", "method", msg.Method, "log_id", msg.LogId, "data", string(msg.Data))
	}

	switch msg.Method {
	case proto.MethodReady:
		body, status, errMsg = p.handleReady(msg)

	case proto.MethodPing:
		body, status, errMsg = p.handlePingRPC(msg)

	case proto.MethodStatsReport:
		p.handleStatsReport(msg)

	case proto.MethodLeave:
		body, status, errMsg = p.handleLeave(msg)

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

	case proto.MethodSubscriptionSubscribe:
		body, status, errMsg = p.handleSubscriptionSubscribe(msg)

	case proto.MethodSubscriptionUnsubscribe:
		body, status, errMsg = p.handleSubscriptionUnsubscribe(msg)

	case proto.MethodSubscriptionUpdatePermissions:
		body, status, errMsg = p.handleSubscriptionUpdatePermissions(msg)

	case proto.MethodGroupCreate:
		body, status, errMsg = p.handleGroupCreate(msg)

	case proto.MethodGroupDelete:
		body, status, errMsg = p.handleGroupDelete(msg)

	case proto.MethodGroupList:
		body, status, errMsg = p.handleGroupList(msg)

	case proto.MethodGroupJoin:
		body, status, errMsg = p.handleGroupJoin(msg)

	case proto.MethodGroupLeave:
		body, status, errMsg = p.handleGroupLeave(msg)

	case proto.MethodChatMessage:
		p.handleChatMessageRelay(msg)

	case proto.MethodRpcRequest:
		p.handleRpcRequest(msg)

	case proto.MethodRpcResponse:
		p.handleRpcResponse(msg)

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

	others := p.room.GetOtherParticipants(p.sid)
	otherInfos := make([]*proto.ParticipantInfo, len(others))
	for i, o := range others {
		otherInfos[i] = o.ToProto()
	}

	// Collect groups for the response.
	groupList := p.room.ListGroups()
	groupInfos := make([]*proto.GroupInfo, len(groupList))
	for i, g := range groupList {
		groupInfos[i] = g.ToProto()
	}

	// For reconnect, deliver chat messages the client missed.
	var missedMessages []*proto.ChatMessageNotification
	if req.SyncState != nil {
		missedMessages = p.room.GetMessagesSince(req.SyncState.LastMessageTimestamp)
		go p.processReconnectSync(req.SyncState)
	} else {
		p.room.BroadcastNotification(proto.MethodParticipantJoined,
			&proto.ParticipantJoinedNotification{Participant: p.ToProto()}, p.sid)
		go p.room.SubscribeExistingTracks(p)
	}

	return &proto.ReadyResponse{
		OtherParticipants: otherInfos,
		Room:              p.room.ToProto(),
		Groups:            groupInfos,
		Messages:          missedMessages,
	}, proto.StatusOK, ""
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

func (p *Participant) handleStatsReport(msg *proto.Message) {
	// Reset heartbeat timer -- stats reports act as keepalive
	p.mu.Lock()
	p.lastPing = time.Now()
	p.mu.Unlock()
	p.resetPingTimer()

	// Write stats to the dedicated file logger
	if p.statsLogger != nil {
		p.statsLogger.Info("stats", "data", string(msg.Data))
	}
}

func (p *Participant) handleLeave(msg *proto.Message) (any, int, string) {
	p.logger.Info("Client left")
	// avoid close in sync
	time.AfterFunc(time.Millisecond*100, func() {
		p.Close(proto.DisconnectReason_CLIENT_INITIATED)
	})
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

// ============================================================
// Manual Subscription
// ============================================================

func (p *Participant) handleSubscriptionSubscribe(msg *proto.Message) (any, int, string) {
	var req proto.SubscribeRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}

	if !p.CanSubscribe() {
		return nil, proto.StatusForbidden, "no subscribe permission"
	}

	if p.room == nil {
		return nil, proto.StatusInternalError, "not in a room"
	}

	track, publisher := p.room.FindPublishedTrackAndPublisher(req.TrackSid)
	if track == nil || publisher == nil {
		return nil, proto.StatusNotFound, fmt.Sprintf("track not found: %s", req.TrackSid)
	}

	if publisher.sid == p.sid {
		return nil, proto.StatusBadRequest, "cannot subscribe to own track"
	}

	// Check if already subscribed to this track.
	p.mu.RLock()
	for _, sub := range p.subscriptions {
		if sub.trackSid == req.TrackSid {
			p.mu.RUnlock()
			return nil, proto.StatusOK, ""
		}
	}
	p.mu.RUnlock()

	go func() {
		if err := p.Subscribe(track, publisher); err != nil {
			p.logger.Warn("Manual subscribe failed", "trackSid", req.TrackSid, "error", err)
		}
	}()

	return nil, proto.StatusOK, ""
}

func (p *Participant) handleSubscriptionUnsubscribe(msg *proto.Message) (any, int, string) {
	var req proto.UnsubscribeRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}

	p.mu.RLock()
	var targetSubSid string
	for sid, sub := range p.subscriptions {
		if sub.trackSid == req.TrackSid {
			targetSubSid = sid
			break
		}
	}
	p.mu.RUnlock()

	if targetSubSid == "" {
		return nil, proto.StatusNotFound, fmt.Sprintf("no subscription for track: %s", req.TrackSid)
	}

	if err := p.CloseSubscription(targetSubSid); err != nil {
		return nil, proto.StatusInternalError, err.Error()
	}

	_ = p.SendNotification(proto.MethodSubscriptionClosed, &proto.SubscriptionClosedNotification{
		SubscriptionSid: targetSubSid,
	})

	return nil, proto.StatusOK, ""
}

func (p *Participant) handleSubscriptionUpdatePermissions(msg *proto.Message) (any, int, string) {
	var req proto.UpdateSubscriptionPermissionsRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, fmt.Sprintf("invalid request: %s", err)
	}

	p.mu.Lock()
	p.allParticipantsAllowedToSubscribe = req.AllParticipantsAllowed
	p.trackPermissions = req.TrackPermissions
	p.mu.Unlock()

	p.logger.Info("Updated subscription permissions",
		"allAllowed", req.AllParticipantsAllowed,
		"permissionCount", len(req.TrackPermissions))

	return nil, proto.StatusOK, ""
}

// ============================================================
// Chat Message Relay
// ============================================================

func (p *Participant) handleChatMessageRelay(msg *proto.Message) {
	if p.room == nil {
		return
	}

	var chatMsg proto.ChatMessageNotification
	if err := json.Unmarshal(msg.Data, &chatMsg); err != nil {
		p.logger.Warn("Invalid chat message", "error", err)
		return
	}

	chatMsg.ParticipantSid = p.sid
	chatMsg.ParticipantIdentity = p.identity

	p.room.StoreChatMessage(&chatMsg)
	p.room.BroadcastNotification(proto.MethodChatMessage, &chatMsg, p.sid)
}

// ============================================================
// P2P RPC Relay
// ============================================================

func (p *Participant) handleRpcRequest(msg *proto.Message) {
	if p.room == nil {
		return
	}

	var req proto.RpcRequestNotification
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		p.logger.Warn("Invalid RPC request", "error", err)
		return
	}

	req.CallerIdentity = p.identity

	target := p.room.GetParticipantByIdentity(req.DestinationIdentity)
	if target == nil {
		_ = p.SendNotification(proto.MethodRpcResponse, &proto.RpcResponseNotification{
			RequestID:      req.RequestID,
			CallerIdentity: p.identity,
			Error: &proto.RpcError{
				Code:    1500,
				Message: "recipient not found",
			},
		})
		return
	}

	_ = target.SendNotification(proto.MethodRpcInvoke, &req)
}

func (p *Participant) handleRpcResponse(msg *proto.Message) {
	if p.room == nil {
		return
	}

	var resp proto.RpcResponseNotification
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		p.logger.Warn("Invalid RPC response", "error", err)
		return
	}

	target := p.room.GetParticipantByIdentity(resp.CallerIdentity)
	if target == nil {
		p.logger.Warn("RPC response target not found", "callerIdentity", resp.CallerIdentity)
		return
	}

	_ = target.SendNotification(proto.MethodRpcResponse, &resp)
}

// ============================================================
// Group (Breakout Room) Handlers
// ============================================================

func (p *Participant) handleGroupCreate(msg *proto.Message) (any, int, string) {
	if p.room == nil {
		return nil, proto.StatusBadRequest, "not in a room"
	}
	if p.permission == nil || !p.permission.CanManageGroups {
		return nil, proto.StatusForbidden, "no permission to manage groups"
	}

	var req proto.CreateGroupRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}

	g := p.room.CreateGroup(req.Name, req.Permissions)
	return &proto.CreateGroupResponse{Group: g.ToProto()}, proto.StatusOK, ""
}

func (p *Participant) handleGroupDelete(msg *proto.Message) (any, int, string) {
	if p.room == nil {
		return nil, proto.StatusBadRequest, "not in a room"
	}
	if p.permission == nil || !p.permission.CanManageGroups {
		return nil, proto.StatusForbidden, "no permission to manage groups"
	}

	var req proto.DeleteGroupRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}

	if err := p.room.DeleteGroup(req.GroupId); err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}
	return nil, proto.StatusOK, ""
}

func (p *Participant) handleGroupList(msg *proto.Message) (any, int, string) {
	if p.room == nil {
		return nil, proto.StatusBadRequest, "not in a room"
	}

	groups := p.room.ListGroups()
	infos := make([]*proto.GroupInfo, 0, len(groups))
	for _, g := range groups {
		infos = append(infos, g.ToProto())
	}
	return &proto.ListGroupsResponse{Groups: infos}, proto.StatusOK, ""
}

func (p *Participant) handleGroupJoin(msg *proto.Message) (any, int, string) {
	if p.room == nil {
		return nil, proto.StatusBadRequest, "not in a room"
	}

	var req proto.JoinGroupRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return nil, proto.StatusBadRequest, "invalid request"
	}

	others, err := p.room.JoinGroup(p, req.GroupId)
	if err != nil {
		return nil, proto.StatusNotFound, err.Error()
	}

	infos := make([]*proto.ParticipantInfo, 0, len(others))
	for _, o := range others {
		infos = append(infos, o.ToProto())
	}
	return &proto.JoinGroupResponse{OtherParticipants: infos}, proto.StatusOK, ""
}

func (p *Participant) handleGroupLeave(msg *proto.Message) (any, int, string) {
	if p.room == nil {
		return nil, proto.StatusBadRequest, "not in a room"
	}

	if err := p.room.LeaveGroup(p); err != nil {
		return nil, proto.StatusBadRequest, err.Error()
	}
	return nil, proto.StatusOK, ""
}
