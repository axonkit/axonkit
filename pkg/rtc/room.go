package rtc

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/store"
)

var (
	ErrRoomFull          = errors.New("room is full")
	ErrRoomClosed        = errors.New("room is closed")
	ErrDuplicateIdentity = errors.New("duplicate identity")
)

// CascadeCallbacks are invoked by Room to integrate with the distributed
// routing layer (RIB/FIB). They are set by RoomManager at creation time.
// All stream identifiers use the format "routerID.producerID" (= types.StreamID).
type CascadeCallbacks struct {
	// OnTrackPublished advertises the stream via RIB and registers
	// the producer in media.Manager so remote nodes can pipe it.
	OnTrackPublished func(streamID string)
	// OnTrackUnpublished stops the RIB advertisement and unregisters the producer.
	OnTrackUnpublished func(streamID string)
	// OnDataTrackPublished advertises a data stream.
	OnDataTrackPublished func(streamID string)
	// OnDataTrackUnpublished stops the data stream advertisement.
	OnDataTrackUnpublished func(streamID string)
	// EnsureStream is called before subscribing. For remote streams it
	// triggers fib.Subscribe to pipe the media locally; for local streams
	// it is a no-op.
	EnsureStream func(streamID string) error
	// ReleaseStream is called when a consumer closes. It decrements the
	// FIB ref-count and tears down the pipe when no consumers remain.
	ReleaseStream func(streamID string)
}

type Room struct {
	sid          string
	name         string
	config       config.RoomConfig
	metadata     string
	creationTime time.Time

	participants       map[string]*Participant
	remoteParticipants map[string]bool   // SIDs of participants on other nodes
	groups             map[string]*Group // groupSid -> Group
	messages           []*proto.MessageNotification
	remoteMsgIds       map[string]bool   // dedup remote messages
	sharedState        map[string]string // room-level KV for collaboration features
	mu                 sync.RWMutex

	cascade    CascadeCallbacks
	roomStore  *RoomStore
	watchCtx   context.Context
	watchClose context.CancelFunc

	onClose func(*Room)
	logger  *slog.Logger

	emptyTimer *time.Timer
	closed     bool
}

type RoomParams struct {
	Sid          string
	Name         string
	Config       config.RoomConfig
	CreationTime time.Time
	Metadata     string
}

func NewRoom(params RoomParams) *Room {
	return &Room{
		sid:                params.Sid,
		name:               params.Name,
		config:             params.Config,
		metadata:           params.Metadata,
		participants:       make(map[string]*Participant),
		remoteParticipants: make(map[string]bool),
		groups:             make(map[string]*Group),
		remoteMsgIds:       make(map[string]bool),
		sharedState:        make(map[string]string),
		creationTime:       params.CreationTime,
		logger:             slog.Default().With("component", "Room", "room", params.Name, "roomSid", params.Sid),
	}
}

func (r *Room) maxMessages() int {
	if r.config.MaxMessages > 0 {
		return r.config.MaxMessages
	}
	return 10000
}

func (r *Room) Sid() string          { return r.sid }
func (r *Room) Name() string         { return r.name }
func (r *Room) Metadata() string     { return r.metadata }
func (r *Room) SetMetadata(m string) { r.metadata = m }
func (r *Room) Dynacast() bool       { return r.config.Dynacast }
func (r *Room) Store() *RoomStore    { return r.roomStore }

// ShouldRegressCodec returns true when the PREFER_REGRESSION policy should
// behave like REGRESSION. This is the case when codecRegressionThreshold
// is 0 (always regress) or when the room has fewer participants than the
// configured threshold.
func (r *Room) ShouldRegressCodec() bool {
	threshold := r.config.CodecRegressionThreshold
	if threshold == 0 {
		return true
	}
	r.mu.RLock()
	count := len(r.participants)
	r.mu.RUnlock()
	return count < threshold
}

func (r *Room) SetOnClose(fn func(*Room)) { r.onClose = fn }

// SetupStore initializes the cross-node RoomStore and starts watching for
// remote changes. Must be called after NewRoom and before participants join.
func (r *Room) SetupStore(kv store.Store, nodeID string) {
	r.roomStore = NewRoomStore(kv, r.name, nodeID)
	ctx, cancel := context.WithCancel(context.Background())
	r.watchCtx = ctx
	r.watchClose = cancel

	ch, err := r.roomStore.Watch(ctx)
	if err != nil {
		r.logger.Warn("Failed to start room store watch", "error", err)
		return
	}
	go r.processStoreEvents(ch, nodeID)
}

// processStoreEvents reacts to changes made by other nodes (participant
// join/leave, group changes, new chat messages). Events originating from the
// local node are ignored because the local Room already processes them.
func (r *Room) processStoreEvents(ch <-chan RoomStoreEvent, localNodeID string) {
	for evt := range ch {
		switch evt.Type {
		case "participant":
			if evt.Participant != nil && evt.Participant.NodeId == localNodeID {
				continue
			}
			r.handleRemoteParticipantEvent(evt)
		case "group":
			r.handleRemoteGroupEvent(evt)
		case "msg":
			r.handleRemoteMessageEvent(evt)
		case "state":
			r.handleRemoteStateEvent(evt)
		case "tracksub":
			if evt.TrackSub != nil && evt.TrackSub.NodeId != localNodeID {
				r.handleRemoteTrackSubEvent(evt)
			} else if evt.Operation == store.OpDelete {
				r.handleRemoteTrackSubEvent(evt)
			}
		}
	}
}

func (r *Room) handleRemoteParticipantEvent(evt RoomStoreEvent) {
	switch evt.Operation {
	case store.OpPut:
		if evt.Participant == nil {
			return
		}
		info := evt.Participant

		r.mu.Lock()
		_, known := r.remoteParticipants[info.Sid]
		r.remoteParticipants[info.Sid] = true
		r.mu.Unlock()

		if known {
			r.BroadcastNotification(proto.MethodParticipantUpdated,
				&proto.ParticipantUpdatedNotification{Participant: info}, "")
		} else {
			r.BroadcastNotification(proto.MethodParticipantJoined,
				&proto.ParticipantJoinedNotification{Participant: info}, "")
		}
	case store.OpDelete:
		r.mu.Lock()
		delete(r.remoteParticipants, evt.Key)
		r.mu.Unlock()

		r.BroadcastNotification(proto.MethodParticipantLeft,
			&proto.ParticipantLeftNotification{
				ParticipantSid: evt.Key,
				Reason:         proto.DisconnectReason_UNKNOWN_REASON,
			}, "")
	}
}

func (r *Room) handleRemoteGroupEvent(evt RoomStoreEvent) {
	r.mu.Lock()
	switch evt.Operation {
	case store.OpPut:
		if evt.Group != nil {
			if existing, ok := r.groups[evt.Group.GroupId]; ok {
				existing.name = evt.Group.Name
			} else {
				r.groups[evt.Group.GroupId] = &Group{
					sid:  evt.Group.GroupId,
					name: evt.Group.Name,
				}
			}
		}
	case store.OpDelete:
		delete(r.groups, evt.Key)
	}
	r.mu.Unlock()
}

func (r *Room) handleRemoteMessageEvent(evt RoomStoreEvent) {
	switch evt.Operation {
	case store.OpPut:
		if evt.Message == nil {
			return
		}
		r.mu.Lock()
		if r.remoteMsgIds[evt.Message.ID] {
			r.mu.Unlock()
			return
		}
		r.remoteMsgIds[evt.Message.ID] = true
		r.messages = append(r.messages, evt.Message)
		r.mu.Unlock()

		r.BroadcastNotification(proto.MethodMessageReceived, evt.Message, "")

	case store.OpDelete:
		r.mu.Lock()
		msgID := evt.Key
		for i, m := range r.messages {
			if m.ID == msgID {
				r.messages = append(r.messages[:i], r.messages[i+1:]...)
				break
			}
		}
		delete(r.remoteMsgIds, msgID)
		r.mu.Unlock()
	}
}

func (r *Room) handleRemoteStateEvent(evt RoomStoreEvent) {
	r.mu.Lock()
	switch evt.Operation {
	case store.OpPut:
		r.sharedState[evt.Key] = evt.StateValue
		r.mu.Unlock()
		r.BroadcastNotification(proto.MethodStateUpdated, &proto.StateUpdatedNotification{
			Key:   evt.Key,
			Value: evt.StateValue,
		}, "")
	case store.OpDelete:
		delete(r.sharedState, evt.Key)
		r.mu.Unlock()
		r.BroadcastNotification(proto.MethodStateDeleted, &proto.StateDeletedNotification{
			Key: evt.Key,
		}, "")
	default:
		r.mu.Unlock()
	}
}

func (r *Room) handleRemoteTrackSubEvent(evt RoomStoreEvent) {
	// evt.Key format: <trackSid>.<nodeId>
	parts := strings.SplitN(evt.Key, ".", 2)
	if len(parts) != 2 {
		return
	}
	trackSid := parts[0]
	nodeID := parts[1]

	track := r.FindPublishedTrack(trackSid)
	if track == nil {
		return
	}

	switch evt.Operation {
	case store.OpPut:
		if evt.TrackSub != nil {
			track.updateRemoteSubscription(nodeID, evt.TrackSub)
		}
	case store.OpDelete:
		track.removeRemoteSubscription(nodeID)
	}
}

func (r *Room) AddParticipant(p *Participant) error {
	// First pass: detect duplicate identity and check capacity.
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRoomClosed
	}

	var existing *Participant
	for _, ep := range r.participants {
		if ep.identity == p.identity && ep.sid != p.sid {
			existing = ep
			break
		}
	}

	effectiveCount := len(r.participants)
	if existing != nil {
		effectiveCount--
	}
	if r.config.MaxParticipants > 0 && effectiveCount >= r.config.MaxParticipants {
		r.mu.Unlock()
		return ErrRoomFull
	}
	r.mu.Unlock()

	// Close old participant synchronously (outside the lock) so its
	// producers/consumers are fully torn down before the new one is added.
	// Close() internally calls RemoveParticipant which handles broadcast
	// and store cleanup.
	if existing != nil {
		r.logger.Info("Kicking duplicate identity", "identity", p.identity, "oldSid", existing.sid)
		existing.Close(proto.DisconnectReason_DUPLICATE_IDENTITY)
	}

	// Second pass: add the new participant.
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRoomClosed
	}

	r.participants[p.sid] = p
	r.stopEmptyTimer()
	p.room = r
	p.setupStatsLogger(r.name, r.sid)

	r.logger.Info("Participant added", "participant", p.identity, "sid", p.sid,
		"count", len(r.participants))

	if r.roomStore != nil {
		go r.roomStore.PutParticipant(p.ToProto())
	}

	return nil
}

func (r *Room) RemoveParticipant(sid string, reason proto.DisconnectReason) {
	r.mu.Lock()
	p, ok := r.participants[sid]
	if !ok {
		r.mu.Unlock()
		return
	}
	delete(r.participants, sid)
	count := len(r.participants)
	r.mu.Unlock()

	r.logger.Info("Participant removed", "participant", p.identity, "sid", sid, "remaining", count)

	if r.roomStore != nil {
		go r.roomStore.DeleteParticipant(sid)
	}

	r.BroadcastNotification(proto.MethodParticipantLeft, &proto.ParticipantLeftNotification{
		ParticipantSid: sid,
		Reason:         reason,
	}, sid)

	if count == 0 {
		r.mu.Lock()
		// Groups still contain refs to participant SIDs but those
		// participants are now gone. Only start the empty timer when
		// there are truly no participants left in the room at all.
		r.startEmptyTimer()
		r.mu.Unlock()
	}
}

func (r *Room) GetParticipant(sid string) *Participant {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.participants[sid]
}

// FindPublishedTrack looks up a published track by its SID across all participants.
func (r *Room) FindPublishedTrack(trackSid string) *PublishedTrack {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.participants {
		p.mu.RLock()
		t := p.publishedTracks[trackSid]
		p.mu.RUnlock()
		if t != nil {
			return t
		}
	}
	return nil
}

// FindPublishedTrackAndPublisher looks up a published track and its publisher.
func (r *Room) FindPublishedTrackAndPublisher(trackSid string) (*PublishedTrack, *Participant) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.participants {
		p.mu.RLock()
		t := p.publishedTracks[trackSid]
		p.mu.RUnlock()
		if t != nil {
			return t, p
		}
	}
	return nil, nil
}

func (r *Room) GetParticipantByIdentity(identity string) *Participant {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.participants {
		if p.identity == identity {
			return p
		}
	}
	return nil
}

func (r *Room) GetOtherParticipants(excludeSid string) []*Participant {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*Participant, 0, len(r.participants))
	for _, p := range r.participants {
		if p.sid != excludeSid {
			result = append(result, p)
		}
	}
	return result
}

func (r *Room) ParticipantCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.participants)
}

func (r *Room) ToProto() *proto.Room {
	r.mu.RLock()
	defer r.mu.RUnlock()

	numPublishers := uint32(0)
	for _, p := range r.participants {
		if p.IsPublisher() {
			numPublishers++
		}
	}

	var ss map[string]string
	if len(r.sharedState) > 0 {
		ss = make(map[string]string, len(r.sharedState))
		for k, v := range r.sharedState {
			ss[k] = v
		}
	}

	return &proto.Room{
		Sid:              r.sid,
		Name:             r.name,
		EmptyTimeout:     uint32(r.config.EmptyTimeout.Seconds()),
		DepartureTimeout: uint32(r.config.DepartureTimeout.Seconds()),
		MaxParticipants:  uint32(r.config.MaxParticipants),
		CreationTime:     r.creationTime.UnixMilli(),
		Metadata:         r.metadata,
		NumParticipants:  uint32(len(r.participants)),
		NumPublishers:    numPublishers,
		SharedState:      ss,
	}
}

// GetAllParticipantInfos returns ParticipantInfo for all participants across
// all nodes. Local participants are serialized from in-memory state; remote
// participants are fetched from the shared store.
func (r *Room) GetAllParticipantInfos(excludeSid string) []*proto.ParticipantInfo {
	r.mu.RLock()
	localSids := make(map[string]bool, len(r.participants))
	var result []*proto.ParticipantInfo
	for _, p := range r.participants {
		localSids[p.sid] = true
		if p.sid != excludeSid {
			result = append(result, p.ToProto())
		}
	}
	r.mu.RUnlock()

	if r.roomStore != nil {
		remote := r.roomStore.GetAllParticipants()
		for _, info := range remote {
			if info.Sid == excludeSid || localSids[info.Sid] {
				continue
			}
			result = append(result, info)
		}
	}
	return result
}

// GetAllGroupInfos returns GroupInfo for all groups across all nodes.
func (r *Room) GetAllGroupInfos() []*proto.GroupInfo {
	r.mu.RLock()
	localIds := make(map[string]bool, len(r.groups))
	var result []*proto.GroupInfo
	for _, g := range r.groups {
		localIds[g.SID()] = true
		result = append(result, g.ToProto())
	}
	r.mu.RUnlock()

	if r.roomStore != nil {
		remote := r.roomStore.GetAllGroups()
		for _, info := range remote {
			if localIds[info.GroupId] {
				continue
			}
			result = append(result, info)
		}
	}
	return result
}

// GetAllMessagesSince returns messages across all nodes since the given
// timestamp. Local messages come from in-memory cache; remote from the store.
func (r *Room) GetAllMessagesSince(since int64) []*proto.MessageNotification {
	r.mu.RLock()
	localIds := make(map[string]bool, len(r.messages))
	var result []*proto.MessageNotification
	for _, m := range r.messages {
		ts := m.Timestamp
		if m.EditTimestamp != nil && *m.EditTimestamp > ts {
			ts = *m.EditTimestamp
		}
		if since == 0 || ts > since {
			localIds[m.ID] = true
			result = append(result, m)
		}
	}
	r.mu.RUnlock()

	if r.roomStore != nil {
		remote := r.roomStore.GetMessagesSince(since)
		for _, m := range remote {
			if localIds[m.ID] {
				continue
			}
			result = append(result, m)
		}
	}
	return result
}

// GetAllSharedState returns the merged shared state from local and store.
func (r *Room) GetAllSharedState() map[string]string {
	r.mu.RLock()
	result := make(map[string]string, len(r.sharedState))
	for k, v := range r.sharedState {
		result[k] = v
	}
	r.mu.RUnlock()

	if r.roomStore != nil {
		remote := r.roomStore.GetAllState()
		for k, v := range remote {
			if _, exists := result[k]; !exists {
				result[k] = v
			}
		}
	}
	return result
}

// BroadcastNotification sends a notification to all participants except the excluded one.
func (r *Room) BroadcastNotification(method string, body any, excludeSid string) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, p := range r.participants {
		if p.sid == excludeSid {
			continue
		}
		if err := p.SendNotification(method, body); err != nil {
			r.logger.Warn("Failed to send notification", "method", method,
				"participant", p.identity, "error", err)
		}
	}
}

// StoreMessage appends a message to the room's history.
// Messages with an EditTimestamp update the existing message in-place.
// Transient messages are skipped (not stored).
func (r *Room) StoreMessage(msg *proto.MessageNotification) {
	if msg.Transient {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if msg.EditTimestamp != nil {
		for i, m := range r.messages {
			if m.ID == msg.ID {
				r.messages[i] = msg
				if r.roomStore != nil {
					go r.roomStore.PutMessage(msg)
				}
				return
			}
		}
	}

	r.messages = append(r.messages, msg)
	if r.roomStore != nil {
		go r.roomStore.PutMessage(msg)
	}

	r.evictOldMessages()
}

// evictOldMessages removes the oldest messages when the limit is exceeded.
// Must be called with r.mu held.
func (r *Room) evictOldMessages() {
	maxMsg := r.maxMessages()
	if len(r.messages) <= maxMsg {
		return
	}
	overflow := len(r.messages) - maxMsg
	evicted := r.messages[:overflow]
	r.messages = r.messages[overflow:]

	if r.roomStore != nil {
		ids := make([]string, len(evicted))
		for i, m := range evicted {
			ids[i] = m.ID
		}
		go r.roomStore.DeleteMessagesByIDs(ids)
	}
}

// DeleteMessage removes a single message by ID. Returns true if found.
func (r *Room) DeleteMessage(id string) bool {
	r.mu.Lock()
	found := false
	for i, m := range r.messages {
		if m.ID == id {
			r.messages = append(r.messages[:i], r.messages[i+1:]...)
			found = true
			break
		}
	}
	r.mu.Unlock()

	if found && r.roomStore != nil {
		go r.roomStore.DeleteMessage(id)
	}
	return found
}

// DeleteMessagesByType removes all messages of a given type and returns their IDs.
func (r *Room) DeleteMessagesByType(msgType string) []string {
	r.mu.Lock()
	var ids []string
	filtered := r.messages[:0]
	for _, m := range r.messages {
		if m.Type == msgType {
			ids = append(ids, m.ID)
		} else {
			filtered = append(filtered, m)
		}
	}
	r.messages = filtered
	r.mu.Unlock()

	if len(ids) > 0 && r.roomStore != nil {
		go r.roomStore.DeleteMessagesByIDs(ids)
	}
	return ids
}

// UpdateState sets a key in the room's shared state and broadcasts the change.
func (r *Room) UpdateState(key, value, participantSid, participantIdentity string) {
	r.mu.Lock()
	r.sharedState[key] = value
	r.mu.Unlock()

	if r.roomStore != nil {
		go r.roomStore.PutState(key, value)
	}

	r.BroadcastNotification(proto.MethodStateUpdated, &proto.StateUpdatedNotification{
		Key:                 key,
		Value:               value,
		ParticipantSid:      participantSid,
		ParticipantIdentity: participantIdentity,
	}, "")
}

// DeleteState removes a key from the room's shared state and broadcasts the change.
func (r *Room) DeleteState(key, participantSid, participantIdentity string) {
	r.mu.Lock()
	delete(r.sharedState, key)
	r.mu.Unlock()

	if r.roomStore != nil {
		go r.roomStore.DeleteState(key)
	}

	r.BroadcastNotification(proto.MethodStateDeleted, &proto.StateDeletedNotification{
		Key:                 key,
		ParticipantSid:      participantSid,
		ParticipantIdentity: participantIdentity,
	}, "")
}

// GetMessagesSince returns all messages with a timestamp strictly
// greater than the provided value. If since is 0 all messages are returned.
func (r *Room) GetMessagesSince(since int64) []*proto.MessageNotification {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if since == 0 {
		out := make([]*proto.MessageNotification, len(r.messages))
		copy(out, r.messages)
		return out
	}

	var out []*proto.MessageNotification
	for _, m := range r.messages {
		ts := m.Timestamp
		if m.EditTimestamp != nil && *m.EditTimestamp > ts {
			ts = *m.EditTimestamp
		}
		if ts > since {
			out = append(out, m)
		}
	}
	return out
}

// OnTrackPublished handles a new track being published.
// For auto-subscribe rooms, it creates subscriptions for all other participants.
func (r *Room) OnTrackPublished(publisher *Participant, track *PublishedTrack) {
	if r.cascade.OnTrackPublished != nil {
		r.cascade.OnTrackPublished(track.sid)
	}

	r.BroadcastNotification(proto.MethodTrackPublished, &proto.TrackPublishedNotification{
		ParticipantSid: publisher.sid,
		Track:          track.ToProto(),
	}, publisher.sid)

	if !r.config.AutoSubscribe {
		return
	}

	publisherGroupId := publisher.GetGroupId()
	others := r.GetOtherParticipants(publisher.sid)
	for _, sub := range others {
		if !sub.IsReady() || !sub.CanSubscribe() || !sub.AutoSubscribe() {
			continue
		}
		if !r.shouldPeerSubscribe(publisherGroupId, sub.GetGroupId()) {
			continue
		}
		go func(subscriber *Participant) {
			if err := subscriber.Subscribe(track, publisher); err != nil {
				r.logger.Warn("Auto-subscribe failed", "subscriber", subscriber.identity,
					"track", track.sid, "error", err)
			}
		}(sub)
	}
}

// OnTrackUnpublished handles a track being unpublished.
func (r *Room) OnTrackUnpublished(publisher *Participant, trackSid string) {
	if r.cascade.OnTrackUnpublished != nil {
		r.cascade.OnTrackUnpublished(trackSid)
	}

	r.BroadcastNotification(proto.MethodTrackUnpublished, &proto.TrackUnpublishedNotification{
		ParticipantSid: publisher.sid,
		TrackSid:       trackSid,
	}, publisher.sid)
}

// OnTrackMuted handles a track being muted/unmuted.
func (r *Room) OnTrackMuted(publisher *Participant, trackSid string, muted bool) {
	r.BroadcastNotification(proto.MethodTrackMuted, &proto.TrackMutedNotification{
		ParticipantSid: publisher.sid,
		TrackSid:       trackSid,
		Muted:          muted,
	}, publisher.sid)
}

// OnDataTrackPublished handles auto-subscribe for data tracks.
func (r *Room) OnDataTrackPublished(publisher *Participant, track *DataPublishedTrack) {
	if r.cascade.OnDataTrackPublished != nil {
		r.cascade.OnDataTrackPublished(track.sid)
	}

	if !r.config.AutoSubscribe {
		return
	}

	others := r.GetOtherParticipants(publisher.sid)
	for _, sub := range others {
		if !sub.IsReady() || !sub.CanSubscribe() || !sub.AutoSubscribe() {
			continue
		}
		go func(subscriber *Participant) {
			if err := subscriber.SubscribeData(track, publisher); err != nil {
				r.logger.Warn("Auto data subscribe failed", "subscriber", subscriber.identity,
					"dataTrack", track.sid, "error", err)
			}
		}(sub)
	}
}

// OnDataTrackUnpublished handles a data track being unpublished.
func (r *Room) OnDataTrackUnpublished(publisher *Participant, dataTrackSid string) {
	if r.cascade.OnDataTrackUnpublished != nil {
		r.cascade.OnDataTrackUnpublished(dataTrackSid)
	}
}

// SubscribeExistingTracks subscribes a new participant to all existing tracks
// from peers in the same group (or all in the main room when groupId == "").
func (r *Room) SubscribeExistingTracks(subscriber *Participant) {
	if !r.config.AutoSubscribe || !subscriber.CanSubscribe() || !subscriber.AutoSubscribe() {
		return
	}

	subscriberGroupId := subscriber.GetGroupId()
	others := r.GetOtherParticipants(subscriber.sid)
	for _, publisher := range others {
		if !r.shouldPeerSubscribe(subscriberGroupId, publisher.GetGroupId()) {
			continue
		}

		tracks := publisher.GetPublishedTracks()
		for _, track := range tracks {
			go func(t *PublishedTrack, pub *Participant) {
				if err := subscriber.Subscribe(t, pub); err != nil {
					r.logger.Warn("Subscribe existing track failed",
						"subscriber", subscriber.identity, "track", t.sid, "error", err)
				}
			}(track, publisher)
		}

		dataTracks := publisher.GetDataPublishedTracks()
		for _, dt := range dataTracks {
			go func(t *DataPublishedTrack, pub *Participant) {
				if err := subscriber.SubscribeData(t, pub); err != nil {
					r.logger.Warn("Subscribe existing data track failed",
						"subscriber", subscriber.identity, "dataTrack", t.sid, "error", err)
				}
			}(dt, publisher)
		}
	}
}

// ============================================================
// Group (Breakout Room) Management
// ============================================================

func (r *Room) CreateGroup(name string, permissions *proto.ParticipantPermission) *Group {
	g := NewGroup(name, permissions)
	r.mu.Lock()
	r.groups[g.SID()] = g
	r.mu.Unlock()

	if r.roomStore != nil {
		go r.roomStore.PutGroup(g.ToProto())
	}

	r.BroadcastNotification(proto.MethodGroupCreated, &proto.GroupCreatedNotification{
		Group: g.ToProto(),
	}, "")

	r.logger.Info("Group created", "groupId", g.SID(), "name", name)
	return g
}

func (r *Room) DeleteGroup(groupId string) error {
	r.mu.Lock()
	g, ok := r.groups[groupId]
	if !ok {
		r.mu.Unlock()
		return errors.New("group not found")
	}
	delete(r.groups, groupId)
	r.mu.Unlock()

	participantSids := g.ParticipantSids()
	for _, pSid := range participantSids {
		p := r.GetParticipant(pSid)
		if p == nil {
			continue
		}
		r.participantLeaveGroup(p, g)
	}

	if r.roomStore != nil {
		go r.roomStore.DeleteGroup(groupId)
	}

	r.BroadcastNotification(proto.MethodGroupDeleted, &proto.GroupDeletedNotification{
		GroupId: groupId,
	}, "")

	r.logger.Info("Group deleted", "groupId", groupId, "participantsReturned", len(participantSids))
	return nil
}

func (r *Room) GetGroup(groupId string) *Group {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.groups[groupId]
}

func (r *Room) ListGroups() []*Group {
	r.mu.RLock()
	defer r.mu.RUnlock()
	groups := make([]*Group, 0, len(r.groups))
	for _, g := range r.groups {
		groups = append(groups, g)
	}
	return groups
}

// JoinGroup moves a participant from their current context (main room or
// another group) into the specified group.  Subscriptions are rewired:
// old peers lose this participant's tracks, new peers gain them.
// Returns the list of other participants already in the group.
func (r *Room) JoinGroup(p *Participant, groupId string) ([]*Participant, error) {
	r.mu.RLock()
	g, ok := r.groups[groupId]
	r.mu.RUnlock()
	if !ok {
		return nil, errors.New("group not found")
	}

	oldGroupId := p.GetGroupId()

	if oldGroupId != "" {
		if oldGroup := r.GetGroup(oldGroupId); oldGroup != nil {
			r.participantLeaveGroup(p, oldGroup)
		}
	}

	// Snapshot the existing members before adding the new participant.
	existingSids := g.ParticipantSids()

	g.AddParticipant(p.sid)
	p.SetGroupId(groupId)

	if g.permissions != nil {
		p.SetEffectivePermission(g.permissions)
	}

	r.rewireSubscriptions(p)

	if r.roomStore != nil {
		go r.roomStore.PutParticipant(p.ToProto())
		go r.roomStore.PutGroup(g.ToProto())
	}

	r.BroadcastNotification(proto.MethodGroupParticipantJoined, &proto.GroupParticipantJoinedNotification{
		GroupId:        groupId,
		ParticipantSid: p.sid,
	}, "")

	// Collect ParticipantInfo for existing group members.
	r.mu.RLock()
	others := make([]*Participant, 0, len(existingSids))
	for _, sid := range existingSids {
		if peer, ok := r.participants[sid]; ok {
			others = append(others, peer)
		}
	}
	r.mu.RUnlock()

	r.logger.Info("Participant joined group", "participant", p.identity,
		"groupId", groupId, "from", oldGroupId)
	return others, nil
}

// LeaveGroup returns a participant to the main room.
func (r *Room) LeaveGroup(p *Participant) error {
	groupId := p.GetGroupId()
	if groupId == "" {
		return errors.New("participant not in a group")
	}

	g := r.GetGroup(groupId)
	if g != nil {
		r.participantLeaveGroup(p, g)
	}

	r.BroadcastNotification(proto.MethodGroupParticipantLeft, &proto.GroupParticipantLeftNotification{
		GroupId:        groupId,
		ParticipantSid: p.sid,
	}, "")

	return nil
}

// participantLeaveGroup is the internal helper that removes a participant
// from a group, restores room-level permissions, and rewires subscriptions.
func (r *Room) participantLeaveGroup(p *Participant, g *Group) {
	g.RemoveParticipant(p.sid)
	if r.roomStore != nil {
		go r.roomStore.PutGroup(g.ToProto())
	}
	p.SetGroupId("")
	p.RestoreRoomPermission()

	r.rewireSubscriptions(p)

	if r.roomStore != nil {
		go r.roomStore.PutParticipant(p.ToProto())
	}

	r.logger.Info("Participant left group", "participant", p.identity, "groupId", g.SID())
}

// rewireSubscriptions adjusts the subscriptions for a participant and all
// other participants so that only peers in the same group (or all in the
// main room when groupId == "") are cross-subscribed.
func (r *Room) rewireSubscriptions(target *Participant) {
	if !r.config.AutoSubscribe {
		return
	}

	others := r.GetOtherParticipants(target.sid)
	targetGroupId := target.GetGroupId()

	for _, other := range others {
		otherGroupId := other.GetGroupId()
		shouldSubscribe := r.shouldPeerSubscribe(targetGroupId, otherGroupId)

		if shouldSubscribe {
			r.ensureCrossSubscription(target, other)
		} else {
			r.removeCrossSubscription(target, other)
		}
	}
}

// shouldPeerSubscribe determines if two participants should be subscribed
// to each other based on their group membership.
// Rules:
//   - Same group (non-empty) -> yes
//   - Both in main room ("") -> yes
//   - One in group, one in main room -> no
//   - Different groups -> no
func (r *Room) shouldPeerSubscribe(groupA, groupB string) bool {
	return groupA == groupB
}

func (r *Room) ensureCrossSubscription(a, b *Participant) {
	if a.IsReady() && a.CanSubscribe() && a.AutoSubscribe() {
		for _, track := range b.GetPublishedTracks() {
			if !a.HasSubscription(track.sid) {
				go func(t *PublishedTrack, pub *Participant) {
					if err := a.Subscribe(t, pub); err != nil {
						r.logger.Debug("Group re-subscribe failed", "subscriber", a.identity,
							"track", t.sid, "error", err)
					}
				}(track, b)
			}
		}
	}
	if b.IsReady() && b.CanSubscribe() && b.AutoSubscribe() {
		for _, track := range a.GetPublishedTracks() {
			if !b.HasSubscription(track.sid) {
				go func(t *PublishedTrack, pub *Participant) {
					if err := b.Subscribe(t, pub); err != nil {
						r.logger.Debug("Group re-subscribe failed", "subscriber", b.identity,
							"track", t.sid, "error", err)
					}
				}(track, a)
			}
		}
	}
}

func (r *Room) removeCrossSubscription(a, b *Participant) {
	for _, track := range b.GetPublishedTracks() {
		if a.HasSubscription(track.sid) {
			go a.CloseSubscription(track.sid)
		}
	}
	for _, track := range a.GetPublishedTracks() {
		if b.HasSubscription(track.sid) {
			go b.CloseSubscription(track.sid)
		}
	}
}

func (r *Room) startEmptyTimer() {
	if r.config.DepartureTimeout <= 0 {
		return
	}
	r.emptyTimer = time.AfterFunc(r.config.DepartureTimeout, func() {
		r.logger.Info("Room empty timeout reached, closing")
		r.Close(proto.DisconnectReason_ROOM_CLOSED)
	})
}

func (r *Room) stopEmptyTimer() {
	if r.emptyTimer != nil {
		r.emptyTimer.Stop()
		r.emptyTimer = nil
	}
}

// deleteLocalParticipantKV removes this participant's dynacast tracksub rows and
// participant row for the local node only (no KV key listing).
func (r *Room) deleteLocalParticipantKV(p *Participant) {
	if r.roomStore == nil || p == nil {
		return
	}
	for _, t := range p.GetPublishedTracks() {
		if t == nil {
			continue
		}
		if sid := t.ToProto().Sid; sid != "" {
			r.roomStore.DeleteTrackSub(sid)
		}
	}
	r.roomStore.DeleteParticipant(p.Sid())
}

func (r *Room) Close(reason proto.DisconnectReason) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.stopEmptyTimer()
	participants := make([]*Participant, 0, len(r.participants))
	for _, p := range r.participants {
		participants = append(participants, p)
	}
	r.participants = make(map[string]*Participant)
	r.mu.Unlock()

	for _, p := range participants {
		r.deleteLocalParticipantKV(p)
		p.Close(reason)
	}

	if r.watchClose != nil {
		r.watchClose()
	}

	if r.onClose != nil {
		r.onClose(r)
	}

	r.logger.Info("Room closed", "reason", reason)
}

// TestEnsureStream exposes the cascade EnsureStream callback for integration testing.
func (r *Room) TestEnsureStream(streamID string) error {
	if r.cascade.EnsureStream != nil {
		return r.cascade.EnsureStream(streamID)
	}
	return nil
}

// TestReleaseStream exposes the cascade ReleaseStream callback for integration testing.
func (r *Room) TestReleaseStream(streamID string) {
	if r.cascade.ReleaseStream != nil {
		r.cascade.ReleaseStream(streamID)
	}
}
