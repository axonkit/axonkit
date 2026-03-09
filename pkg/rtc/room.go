package rtc

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/proto"
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

	participants map[string]*Participant
	groups       map[string]*Group // groupSid -> Group
	chatMessages []*proto.ChatMessageNotification
	mu           sync.RWMutex

	cascade CascadeCallbacks
	onClose func(*Room)
	logger  *slog.Logger

	emptyTimer *time.Timer
	closed     bool
}

func NewRoom(sid, name string, cfg config.RoomConfig) *Room {
	return &Room{
		sid:          sid,
		name:         name,
		config:       cfg,
		participants: make(map[string]*Participant),
		groups:       make(map[string]*Group),
		creationTime: time.Now(),
		logger:       slog.Default().With("component", "Room", "room", name, "roomSid", sid),
	}
}

func (r *Room) Sid() string          { return r.sid }
func (r *Room) Name() string         { return r.name }
func (r *Room) Metadata() string     { return r.metadata }
func (r *Room) SetMetadata(m string) { r.metadata = m }
func (r *Room) Dynacast() bool       { return r.config.Dynacast }

func (r *Room) SetOnClose(fn func(*Room)) { r.onClose = fn }

func (r *Room) AddParticipant(p *Participant) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRoomClosed
	}

	if r.config.MaxParticipants > 0 && len(r.participants) >= r.config.MaxParticipants {
		return ErrRoomFull
	}

	// Kick existing participant with same identity
	for _, existing := range r.participants {
		if existing.identity == p.identity && existing.sid != p.sid {
			r.logger.Info("Kicking duplicate identity", "identity", p.identity, "oldSid", existing.sid)
			go existing.Close(proto.DisconnectReason_DUPLICATE_IDENTITY)
			delete(r.participants, existing.sid)
			break
		}
	}

	r.participants[p.sid] = p
	r.stopEmptyTimer()
	p.room = r
	p.setupStatsLogger(r.name, r.sid)

	r.logger.Info("Participant added", "participant", p.identity, "sid", p.sid,
		"count", len(r.participants))

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
	}
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

// StoreChatMessage appends a chat message to the room's history.
// Messages with an EditTimestamp update the existing message in-place.
func (r *Room) StoreChatMessage(msg *proto.ChatMessageNotification) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if msg.EditTimestamp != nil {
		for i, m := range r.chatMessages {
			if m.ID == msg.ID {
				r.chatMessages[i] = msg
				return
			}
		}
	}
	r.chatMessages = append(r.chatMessages, msg)
}

// GetMessagesSince returns all chat messages with a timestamp strictly
// greater than the provided value. If since is 0 all messages are returned.
func (r *Room) GetMessagesSince(since int64) []*proto.ChatMessageNotification {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if since == 0 {
		out := make([]*proto.ChatMessageNotification, len(r.chatMessages))
		copy(out, r.chatMessages)
		return out
	}

	var out []*proto.ChatMessageNotification
	for _, m := range r.chatMessages {
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
		if !sub.IsReady() || !sub.CanSubscribe() {
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
		if !sub.IsReady() || !sub.CanSubscribe() {
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
	if !r.config.AutoSubscribe || !subscriber.CanSubscribe() {
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
	p.SetGroupId("")
	p.RestoreRoomPermission()

	r.rewireSubscriptions(p)

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
	if a.IsReady() && a.CanSubscribe() {
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
	if b.IsReady() && b.CanSubscribe() {
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
		p.Close(reason)
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
