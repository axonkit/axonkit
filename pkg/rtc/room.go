package rtc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/media"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/store"
	"github.com/jiyeyuran/mediasoup-go/v2"
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
	OnTrackPublished func(streamID string, producer *mediasoup.Producer)
	// OnTrackUnpublished stops the RIB advertisement and unregisters the producer.
	OnTrackUnpublished func(streamID string)
	// OnDataTrackPublished advertises a data stream.
	OnDataTrackPublished func(streamID string, dataProducer *mediasoup.DataProducer)
	// OnDataTrackUnpublished stops the data stream advertisement.
	OnDataTrackUnpublished func(streamID string)
	// EnsureStream is called before subscribing. For remote streams it
	// triggers fib.Subscribe to pipe the media locally; for local streams
	// it is a no-op.
	EnsureStream func(streamID string) error
	// ReleaseStream is called when a consumer closes. It decrements the
	// FIB ref-count and tears down the pipe when no consumers remain.
	ReleaseStream func(streamID string)
	// GetLocalProducer returns the piped producer for a remote stream.
	GetLocalProducer func(streamID string) *media.LocalProducer
	// GetRouter returns a local router by ID.
	GetRouter func(routerId string) *mediasoup.Router
	// RequestBackupCodec forwards a backup-codec request to the node that
	// hosts the publisher (publisherSid). publisherNodeId comes from room KV.
	RequestBackupCodec func(publisherSid, trackSid, publisherNodeId string) error
}

type Room struct {
	sid          string
	name         string
	config       config.RoomConfig
	metadata     string
	creationTime time.Time

	participants      map[string]*Participant
	storeParticipants map[string]*proto.ParticipantInfo
	groups            map[string]*Group
	storeGroups       map[string]*proto.GroupInfo
	// storeTracks: trackSid -> store TrackInfo plus optional materialized
	// PublishedTrack after this node has piped the stream.
	storeTracks     map[string]*storeTrackEntry
	storeDataTracks map[string]*proto.DataTrackInfo
	messages        []*proto.MessageNotification // store-driven message cache
	sharedState     map[string]string            // room-level KV cache
	mu              sync.RWMutex

	cascade    CascadeCallbacks
	roomStore  *RoomStore
	watchCtx   context.Context
	watchClose context.CancelFunc

	onClose func(*Room)
	logger  *slog.Logger

	emptyTimer *time.Timer
	closed     bool
	asyncWG    sync.WaitGroup
	watchWG    sync.WaitGroup
}

// storeTrackEntry joins the room-store TrackInfo with optional materialized
// PublishedTrack after this node has piped the stream locally.
type storeTrackEntry struct {
	info  *proto.TrackInfo
	track *PublishedTrack // non-nil after local pipe; track.publisher carries the router stub
}

type RoomParams struct {
	Sid          string
	Name         string
	Config       config.RoomConfig
	CreationTime time.Time
	Metadata     string
	NodeID       string
	RoomStore    *RoomStore
}

func NewRoom(params RoomParams) *Room {
	if params.RoomStore == nil {
		// Keep roomStore always available so direct Room construction paths
		// (especially tests) behave the same as RoomManager-created rooms.
		params.RoomStore = NewRoomStore(store.NewMemoryStore(0), params.Name, params.NodeID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	room := &Room{
		sid:               params.Sid,
		name:              params.Name,
		config:            params.Config,
		metadata:          params.Metadata,
		participants:      make(map[string]*Participant),
		storeParticipants: make(map[string]*proto.ParticipantInfo),
		groups:            make(map[string]*Group),
		storeGroups:       make(map[string]*proto.GroupInfo),
		storeTracks:       make(map[string]*storeTrackEntry),
		storeDataTracks:   make(map[string]*proto.DataTrackInfo),
		sharedState:       make(map[string]string),
		creationTime:      params.CreationTime,
		roomStore:         params.RoomStore,
		watchCtx:          ctx,
		watchClose:        cancel,
		logger:            slog.Default().With("component", "Room", "room", params.Name, "roomSid", params.Sid),
	}
	room.startStoreWatch(ctx)
	return room
}

func cloneParticipantPermission(permission *proto.ParticipantPermission) *proto.ParticipantPermission {
	if permission == nil {
		return nil
	}
	cloned := *permission
	cloned.CanPublishSources = slices.Clone(permission.CanPublishSources)
	return &cloned
}

func cloneTrackInfo(info *proto.TrackInfo) *proto.TrackInfo {
	if info == nil {
		return nil
	}
	cloned := *info
	cloned.AudioFeatures = slices.Clone(info.AudioFeatures)
	cloned.AppData = maps.Clone(info.AppData)
	return &cloned
}

func cloneTrackInfos(infos []*proto.TrackInfo) []*proto.TrackInfo {
	if len(infos) == 0 {
		return nil
	}
	cloned := make([]*proto.TrackInfo, 0, len(infos))
	for _, info := range infos {
		cloned = append(cloned, cloneTrackInfo(info))
	}
	return cloned
}

func cloneDataTrackInfo(info *proto.DataTrackInfo) *proto.DataTrackInfo {
	if info == nil {
		return nil
	}
	cloned := *info
	cloned.AppData = maps.Clone(info.AppData)
	return &cloned
}

func cloneDataTrackInfos(infos []*proto.DataTrackInfo) []*proto.DataTrackInfo {
	if len(infos) == 0 {
		return nil
	}
	cloned := make([]*proto.DataTrackInfo, 0, len(infos))
	for _, info := range infos {
		cloned = append(cloned, cloneDataTrackInfo(info))
	}
	return cloned
}

func cloneParticipantInfo(info *proto.ParticipantInfo) *proto.ParticipantInfo {
	if info == nil {
		return nil
	}
	cloned := *info
	cloned.Tracks = cloneTrackInfos(info.Tracks)
	cloned.DataTracks = cloneDataTrackInfos(info.DataTracks)
	cloned.Permission = cloneParticipantPermission(info.Permission)
	cloned.Attributes = maps.Clone(info.Attributes)
	return &cloned
}

func cloneGroupInfo(info *proto.GroupInfo) *proto.GroupInfo {
	if info == nil {
		return nil
	}
	cloned := *info
	cloned.Permissions = cloneParticipantPermission(info.Permissions)
	return &cloned
}

func cloneMessage(msg *proto.MessageNotification) *proto.MessageNotification {
	if msg == nil {
		return nil
	}
	cloned := *msg
	if msg.EditTimestamp != nil {
		editTimestamp := *msg.EditTimestamp
		cloned.EditTimestamp = &editTimestamp
	}
	return &cloned
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
	return r.ParticipantCount() < threshold
}

func (r *Room) SetOnClose(fn func(*Room)) { r.onClose = fn }

func (r *Room) startStoreWatch(ctx context.Context) {
	result, err := r.roomStore.Watch(ctx)
	if err != nil {
		r.logger.Warn("Failed to start room store watch", "error", err)
		return
	}
	ready := make(chan struct{})
	r.watchWG.Go(func() {
		r.processStoreEvents(result.Events, ready)
	})
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		r.logger.Warn("Timeout waiting for initial store snapshot")
	}
}

// processStoreEvents reacts to room-store changes and fans them back out to
// local participants. ready is closed after the nil sentinel (initial snapshot
// complete) is received from the channel.
func (r *Room) processStoreEvents(ch <-chan *RoomStoreEvent, ready chan struct{}) {
	readyClosed := false
	for evt := range ch {
		if evt == nil {
			if !readyClosed {
				close(ready)
				readyClosed = true
			}
			continue
		}
		r.dispatchStoreEvent(evt)
	}
	if !readyClosed {
		close(ready)
	}
}

func (r *Room) dispatchStoreEvent(evt *RoomStoreEvent) {
	switch evt.Type {
	case "participant":
		r.handleStoreParticipantEvent(evt)
	case "group":
		r.handleStoreGroupEvent(evt)
	case "msg":
		r.handleStoreMessageEvent(evt)
	case "state":
		r.handleStoreStateEvent(evt)
	case "tracksub":
		r.handleStoreTrackSubEvent(evt)
	case "track":
		r.handleStoreTrackEvent(evt)
	case "dataTrack":
		r.handleStoreDataTrackEvent(evt)
	}
}

func (r *Room) handleStoreParticipantEvent(evt *RoomStoreEvent) {
	switch evt.Operation {
	case store.OpPut:
		if evt.Participant == nil {
			return
		}
		info := evt.Participant

		r.mu.Lock()
		prev := r.storeParticipants[info.Sid]
		prevGroupID := ""
		if prev != nil {
			prevGroupID = prev.GroupId
		}
		r.storeParticipants[info.Sid] = info
		localP := r.participants[info.Sid]
		r.mu.Unlock()

		// Remote kick: another node wrote DISCONNECTED for a local participant.
		if info.State == proto.ParticipantInfo_DISCONNECTED && localP != nil {
			go localP.Close(info.DisconnectReason)
			return
		}

		r.broadcastGroupMembershipChange(info.Sid, prevGroupID, info.GroupId)

		if prev != nil {
			// ParticipantInfo_DISCONNECTED is used to set DisconnectReason,
			// so we don't need to broadcast an update.
			if info.State != proto.ParticipantInfo_DISCONNECTED {
				r.BroadcastNotification(proto.MethodParticipantUpdated,
					&proto.ParticipantUpdatedNotification{Participant: info}, info.Sid)
			}
			if prev.GroupId != info.GroupId {
				r.rewireRemoteParticipantSubscriptions(info.Sid, info.GroupId)
			}
		} else {
			r.BroadcastNotification(proto.MethodParticipantJoined,
				&proto.ParticipantJoinedNotification{Participant: info}, info.Sid)
		}
	case store.OpDelete:
		r.mu.Lock()
		prev := r.storeParticipants[evt.Key]
		delete(r.storeParticipants, evt.Key)
		r.mu.Unlock()

		if prev == nil {
			return
		}
		if prev.GroupId != "" {
			r.broadcastGroupMembershipChange(evt.Key, prev.GroupId, "")
		}

		r.BroadcastNotification(proto.MethodParticipantLeft,
			&proto.ParticipantLeftNotification{
				ParticipantSid: evt.Key,
				Reason:         prev.DisconnectReason,
			}, prev.Sid)
	}
}

func (r *Room) broadcastGroupMembershipChange(participantSid, previousGroupID, currentGroupID string) {
	if previousGroupID == currentGroupID {
		return
	}
	if previousGroupID != "" {
		r.BroadcastNotification(proto.MethodGroupParticipantLeft, &proto.GroupParticipantLeftNotification{
			GroupId:        previousGroupID,
			ParticipantSid: participantSid,
		}, participantSid)
	}
	if currentGroupID != "" {
		r.BroadcastNotification(proto.MethodGroupParticipantJoined, &proto.GroupParticipantJoinedNotification{
			GroupId:        currentGroupID,
			ParticipantSid: participantSid,
		}, participantSid)
	}
}

func (r *Room) handleStoreGroupEvent(evt *RoomStoreEvent) {
	switch evt.Operation {
	case store.OpPut:
		if evt.Group == nil {
			return
		}
		groupInfo := evt.Group
		r.mu.Lock()
		_, existed := r.storeGroups[groupInfo.GroupId]
		r.storeGroups[groupInfo.GroupId] = groupInfo
		enriched := r.enrichGroupInfoLocked(groupInfo)
		r.mu.Unlock()
		if existed {
			r.BroadcastNotification(proto.MethodGroupUpdated, &proto.GroupUpdatedNotification{
				Group: enriched,
			}, groupInfo.OwnerSid)
		} else {
			r.BroadcastNotification(proto.MethodGroupCreated, &proto.GroupCreatedNotification{
				Group: enriched,
			}, groupInfo.OwnerSid)
		}
	case store.OpDelete:
		r.mu.Lock()
		prev := r.storeGroups[evt.Key]
		delete(r.storeGroups, evt.Key)
		r.mu.Unlock()
		if prev != nil {
			r.BroadcastNotification(proto.MethodGroupDeleted, &proto.GroupDeletedNotification{
				GroupId: evt.Key,
			}, prev.OwnerSid)
		}
	}
}

func (r *Room) handleStoreMessageEvent(evt *RoomStoreEvent) {
	switch evt.Operation {
	case store.OpPut:
		if evt.Message == nil {
			return
		}
		r.mu.Lock()
		replaced := false
		for i, m := range r.messages {
			if m.ID == evt.Message.ID {
				r.messages[i] = evt.Message
				replaced = true
				break
			}
		}
		if !replaced {
			r.messages = append(r.messages, evt.Message)
		}
		r.mu.Unlock()

		r.BroadcastNotification(proto.MethodMessageReceived, evt.Message, evt.Message.ParticipantSid)

	case store.OpDelete:
		r.mu.Lock()
		msgID := evt.Key
		participantSid := ""
		for i, m := range r.messages {
			if m.ID == msgID {
				participantSid = m.ParticipantSid
				r.messages = append(r.messages[:i], r.messages[i+1:]...)
				break
			}
		}
		r.mu.Unlock()

		r.BroadcastNotification(proto.MethodMessageDeleted, &proto.MessageDeletedNotification{
			IDs: []string{msgID},
		}, participantSid)
	}
}

func (r *Room) handleStoreStateEvent(evt *RoomStoreEvent) {
	switch evt.Operation {
	case store.OpPut:
		r.mu.Lock()
		r.sharedState[evt.Key] = evt.StateValue
		r.mu.Unlock()
		r.BroadcastNotification(proto.MethodStateUpdated, &proto.StateUpdatedNotification{
			Key:   evt.Key,
			Value: evt.StateValue,
		}, "")
	case store.OpDelete:
		r.mu.Lock()
		delete(r.sharedState, evt.Key)
		r.mu.Unlock()
		r.BroadcastNotification(proto.MethodStateDeleted, &proto.StateDeletedNotification{
			Key: evt.Key,
		}, "")
	}
}

func (r *Room) handleStoreTrackSubEvent(evt *RoomStoreEvent) {
	if evt.SubKey == "" || evt.SubKey == r.Store().nodeID {
		return
	}

	track := r.FindPublishedTrack(evt.Key)
	if track == nil {
		return
	}

	switch evt.Operation {
	case store.OpPut:
		track.updateRemoteSubscription(evt.SubKey, evt.TrackSub)
	case store.OpDelete:
		track.removeRemoteSubscription(evt.SubKey)
	}
}

func (r *Room) handleStoreTrackEvent(evt *RoomStoreEvent) {
	trackSid := evt.Key

	switch evt.Operation {
	case store.OpPut:
		if evt.Track == nil {
			return
		}
		trackInfo := evt.Track
		participantSid := trackInfo.ParticipantSid
		r.mu.Lock()
		mt := r.storeTracks[trackSid]
		if mt == nil {
			mt = &storeTrackEntry{}
			r.storeTracks[trackSid] = mt
		}
		existing := mt.info
		mt.info = trackInfo
		if mt.track != nil {
			applyRemoteTrackProto(mt.track, trackInfo, nil, r.Dynacast())
		}
		r.mu.Unlock()

		if participantSid != "" {
			if existing == nil {
				r.BroadcastNotification(proto.MethodTrackPublished, &proto.TrackPublishedNotification{
					ParticipantSid: participantSid,
					Track:          trackInfo,
				}, participantSid)

				if r.config.AutoSubscribe {
					r.autoSubscribeTrack(trackSid, trackInfo, participantSid)
				}
			} else if existing.Muted != evt.Track.Muted {
				r.BroadcastNotification(proto.MethodTrackMuted, &proto.TrackMutedNotification{
					ParticipantSid: participantSid,
					TrackSid:       trackSid,
					Muted:          evt.Track.Muted,
				}, participantSid)
			}
		}
	case store.OpDelete:
		r.mu.Lock()
		prev := r.storeTracks[trackSid]
		delete(r.storeTracks, trackSid)
		r.mu.Unlock()

		var participantSid string
		if prev != nil && prev.info != nil {
			participantSid = prev.info.ParticipantSid
		}
		r.BroadcastNotification(proto.MethodTrackUnpublished, &proto.TrackUnpublishedNotification{
			ParticipantSid: participantSid,
			TrackSid:       trackSid,
		}, participantSid)
	}
}

func (r *Room) autoSubscribeTrack(trackSid string, trackInfo *proto.TrackInfo, participantSid string) {
	publisherGroupId := r.storeParticipantGroupID(participantSid)
	localTrack, localPub := r.FindPublishedTrackAndPublisher(trackSid)

	others := r.GetOtherParticipants(participantSid)
	for _, sub := range others {
		if !sub.IsReady() || !sub.CanSubscribe() || !sub.AutoSubscribe() {
			continue
		}
		if !r.shouldPeerSubscribe(publisherGroupId, sub.GetGroupId()) {
			continue
		}
		if localTrack != nil {
			go func(subscriber *Participant) {
				if err := subscriber.Subscribe(localTrack, localPub); err != nil {
					r.logger.Warn("Auto subscribe local track failed",
						"subscriber", subscriber.identity, "track", trackSid, "error", err)
				}
			}(sub)
		} else {
			go func(subscriber *Participant) {
				track, publisher, err := r.resolveRemoteTrack(trackInfo, participantSid)
				if err != nil {
					r.logger.Warn("Resolve remote track failed",
						"subscriber", subscriber.identity, "track", trackSid, "error", err)
					return
				}
				if err := subscriber.Subscribe(track, publisher); err != nil {
					r.logger.Warn("Auto subscribe remote track failed",
						"subscriber", subscriber.identity, "track", trackSid, "error", err)
				}
			}(sub)
		}
	}
}

func (r *Room) autoSubscribeDataTrack(trackSid string, dataTrackInfo *proto.DataTrackInfo, participantSid string) {
	publisherGroupId := r.storeParticipantGroupID(participantSid)
	localPub := r.GetParticipant(participantSid)

	others := r.GetOtherParticipants(participantSid)
	for _, sub := range others {
		if !sub.IsReady() || !sub.CanSubscribe() || !sub.AutoSubscribe() {
			continue
		}
		if !r.shouldPeerSubscribe(publisherGroupId, sub.GetGroupId()) {
			continue
		}
		if localPub != nil {
			localTrack := localPub.GetDataPublishedTrack(trackSid)
			if localTrack != nil {
				go func(subscriber *Participant) {
					if err := subscriber.SubscribeData(localTrack, localPub); err != nil {
						r.logger.Warn("Auto subscribe local data track failed",
							"subscriber", subscriber.identity, "dataTrack", trackSid, "error", err)
					}
				}(sub)
				continue
			}
		}
		go func(subscriber *Participant) {
			track, publisher, err := r.resolveRemoteDataTrack(dataTrackInfo, participantSid)
			if err != nil {
				r.logger.Warn("Resolve remote data track failed",
					"subscriber", subscriber.identity, "dataTrack", trackSid, "error", err)
				return
			}
			if err := subscriber.SubscribeData(track, publisher); err != nil {
				r.logger.Warn("Auto subscribe remote data track failed",
					"subscriber", subscriber.identity, "dataTrack", trackSid, "error", err)
			}
		}(sub)
	}
}

func (r *Room) handleStoreDataTrackEvent(evt *RoomStoreEvent) {
	trackSid := evt.Key

	switch evt.Operation {
	case store.OpPut:
		if evt.DataTrack == nil {
			return
		}
		dataTrackInfo := evt.DataTrack
		participantSid := dataTrackInfo.ParticipantSid
		r.mu.Lock()
		existing := r.storeDataTracks[trackSid]
		r.storeDataTracks[trackSid] = dataTrackInfo
		r.mu.Unlock()

		if participantSid != "" && existing == nil {
			r.BroadcastNotification(proto.MethodDataTrackPublished, &proto.DataTrackPublishedNotification{
				ParticipantSid: participantSid,
				DataTrack:      dataTrackInfo,
			}, participantSid)
		}

		if participantSid != "" && existing == nil && r.config.AutoSubscribe {
			r.autoSubscribeDataTrack(trackSid, dataTrackInfo, participantSid)
		}
	case store.OpDelete:
		r.mu.Lock()
		prev := r.storeDataTracks[trackSid]
		delete(r.storeDataTracks, trackSid)
		r.mu.Unlock()

		var participantSid string
		if prev != nil {
			participantSid = prev.ParticipantSid
		}
		r.BroadcastNotification(proto.MethodDataTrackUnpublished, &proto.DataTrackUnpublishedNotification{
			ParticipantSid: participantSid,
			DataTrackSid:   trackSid,
		}, participantSid)
	}
}

// applyRemoteTrackProto updates PublishedTrack fields from TrackInfo. If producer
// is non-nil it replaces the mediasoup producer (e.g. after re-pipe). Dynacast maps
// are allocated once when missing and conditions require them.
func applyRemoteTrackProto(t *PublishedTrack, info *proto.TrackInfo, producer *mediasoup.Producer, dynacast bool) {
	if info == nil {
		return
	}
	t.sid = info.Sid
	if producer != nil {
		t.producer = producer
	}
	t.name = info.Name
	t.source = info.Source
	t.kind = info.Kind
	t.muted = info.Muted
	t.width = info.Width
	t.height = info.Height
	t.appData = info.AppData
	t.videoCodec = info.VideoCodec
	t.backupCodec = info.BackupCodec
	t.backupSid = info.BackupStreamSid
	t.backupCodecPolicy = info.BackupCodecPolicy
	if info.BackupCodec != "" && t.backupConsumers == nil {
		t.backupConsumers = make(map[string]string)
	}
	if dynacast && info.Kind == mediasoup.MediaKindVideo && t.activeConsumers == nil {
		t.activeConsumers = make(map[string]*consumerLayerState)
		t.lastQualities = [3]bool{true, true, true}
	}
}

// resolveRemoteTrack pipes a remote track to the local node and, if the pipe
// router differs from targetRouter, does a cross-router PipeToRouter so the
// returned producer is directly consumable on targetRouter.
func (r *Room) resolveRemoteTrack(trackInfo *proto.TrackInfo, publisherSid string) (*PublishedTrack, *Participant, error) {
	if r.cascade.EnsureStream == nil || r.cascade.GetLocalProducer == nil {
		return nil, nil, errors.New("cascade not configured")
	}
	if err := r.cascade.EnsureStream(trackInfo.Sid); err != nil {
		return nil, nil, fmt.Errorf("ensure stream: %w", err)
	}
	lp := r.cascade.GetLocalProducer(trackInfo.Sid)
	if lp == nil || lp.Producer == nil {
		return nil, nil, fmt.Errorf("local producer not found for %s", trackInfo.Sid)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	sid := trackInfo.Sid
	mt := r.storeTracks[sid]
	if mt == nil {
		mt = &storeTrackEntry{info: cloneTrackInfo(trackInfo)}
		r.storeTracks[sid] = mt
	} else {
		mt.info = cloneTrackInfo(trackInfo)
	}

	if mt.track != nil {
		return mt.track, mt.track.publisher, nil
	}

	stub := &Participant{
		sid:    publisherSid,
		router: lp.Router,
		room:   r,
		logger: slog.Default().With("component", "RemotePublisherStub", "room", r.name, "participantSid", publisherSid),
	}
	track := &PublishedTrack{publisher: stub}
	applyRemoteTrackProto(track, trackInfo, lp.Producer, r.Dynacast())
	mt.track = track
	return track, stub, nil
}

// resolveRemoteDataTrack is the data-track counterpart of resolveRemoteTrack.
func (r *Room) resolveRemoteDataTrack(trackInfo *proto.DataTrackInfo, publisherSid string) (*DataPublishedTrack, *Participant, error) {
	if r.cascade.EnsureStream == nil || r.cascade.GetLocalProducer == nil {
		return nil, nil, errors.New("cascade not configured")
	}
	if err := r.cascade.EnsureStream(trackInfo.Sid); err != nil {
		return nil, nil, fmt.Errorf("ensure stream: %w", err)
	}
	lp := r.cascade.GetLocalProducer(trackInfo.Sid)
	if lp == nil || lp.DataProducer == nil {
		return nil, nil, fmt.Errorf("local data producer not found for %s", trackInfo.Sid)
	}

	track := &DataPublishedTrack{
		sid:          trackInfo.Sid,
		name:         trackInfo.Name,
		dataProducer: lp.DataProducer,
	}
	publisher := &Participant{
		sid:    publisherSid,
		router: lp.Router,
		room:   r,
		logger: slog.Default().With("component", "RemotePublisherStub", "room", r.name, "participantSid", publisherSid),
	}
	return track, publisher, nil
}

func (r *Room) AddParticipant(p *Participant) error {
	// First pass: detect duplicate identity and check capacity.
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return ErrRoomClosed
	}

	var existing *Participant
	for _, ep := range r.participants {
		if ep.identity == p.identity && ep.sid != p.sid {
			existing = ep
			break
		}
	}

	effectiveCount := r.totalParticipantCountLocked()
	if existing != nil {
		effectiveCount--
	}
	// Kick remote duplicate: write DISCONNECTED to the store so the remote
	// node's store-watch closes it. We then proceed to add the new participant.
	var remoteKicks []*proto.ParticipantInfo
	if existing == nil {
		for _, info := range r.storeParticipants {
			if info.Identity == p.identity && info.Sid != p.sid && r.participants[info.Sid] == nil {
				kicked := cloneParticipantInfo(info)
				kicked.State = proto.ParticipantInfo_DISCONNECTED
				kicked.DisconnectReason = proto.DisconnectReason_DUPLICATE_IDENTITY
				remoteKicks = append(remoteKicks, kicked)
				effectiveCount--
			}
		}
	}
	if r.config.MaxParticipants > 0 && effectiveCount >= r.config.MaxParticipants {
		r.mu.RUnlock()
		return ErrRoomFull
	}
	r.mu.RUnlock()

	// Close old participant synchronously (outside the lock) so its
	// producers/consumers are fully torn down before the new one is added.
	// Close() internally calls RemoveParticipant which handles broadcast
	// and store cleanup.
	if existing != nil {
		r.logger.Info("Kicking duplicate identity", "identity", p.identity, "oldSid", existing.sid)
		existing.Close(proto.DisconnectReason_DUPLICATE_IDENTITY)
	}
	for _, kicked := range remoteKicks {
		r.logger.Info("Kicking remote duplicate identity", "identity", p.identity, "remoteSid", kicked.Sid)
		r.roomStore.PutParticipant(kicked)
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

	r.runAsync(func() { r.roomStore.PutParticipant(p.ToProto()) })

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
	delete(r.storeParticipants, sid)
	count := len(r.participants)
	r.mu.Unlock()

	r.logger.Info("Participant removed", "participant", p.identity, "sid", sid, "remaining", count)

	r.runAsync(func() {
		r.roomStore.PutParticipant(p.ToProto())
		r.roomStore.DeleteParticipant(sid)
	})

	if count == 0 {
		r.mu.Lock()
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

// ParticipantCount returns the best-effort total number of participants across
// all nodes. It combines the locally-connected set with the store-driven cache
// to account for async propagation delay.
func (r *Room) ParticipantCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.totalParticipantCountLocked()
}

// totalParticipantCountLocked returns a de-duplicated count of active
// participants from both the local map and the store cache. DISCONNECTED
// entries in storeParticipants are excluded. Must be called with r.mu held.
func (r *Room) totalParticipantCountLocked() int {
	if len(r.storeParticipants) == 0 {
		return len(r.participants)
	}
	count := 0
	for sid, info := range r.storeParticipants {
		if info.State == proto.ParticipantInfo_DISCONNECTED {
			continue
		}
		_ = sid
		count++
	}
	for sid := range r.participants {
		if _, inStore := r.storeParticipants[sid]; !inStore {
			count++
		}
	}
	return count
}

func (r *Room) ToProto() *proto.Room {
	r.mu.RLock()
	defer r.mu.RUnlock()

	numPublishers := uint32(0)
	for _, p := range r.storeParticipants {
		if p.IsPublisher {
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
		NumParticipants:  uint32(r.totalParticipantCountLocked()),
		NumPublishers:    numPublishers,
		SharedState:      maps.Clone(r.sharedState),
	}
}

func (r *Room) runAsync(f func()) {
	r.asyncWG.Go(f)
}

func (r *Room) storeParticipantGroupID(participantSid string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if info := r.storeParticipants[participantSid]; info != nil {
		return info.GroupId
	}
	if p := r.participants[participantSid]; p != nil {
		return p.GetGroupId()
	}
	return ""
}

// GetAllParticipantInfos returns ParticipantInfo for all participants using the
// local runtime objects plus the store-driven participant/track caches.
func (r *Room) GetAllParticipantInfos(excludeSid string) []*proto.ParticipantInfo {
	r.mu.RLock()
	trackInfosByParticipant := make(map[string][]*proto.TrackInfo)
	for _, mt := range r.storeTracks {
		if mt == nil || mt.info == nil || mt.info.ParticipantSid == "" {
			continue
		}
		trackInfosByParticipant[mt.info.ParticipantSid] = append(trackInfosByParticipant[mt.info.ParticipantSid], cloneTrackInfo(mt.info))
	}
	dataTrackInfosByParticipant := make(map[string][]*proto.DataTrackInfo)
	for _, dt := range r.storeDataTracks {
		if dt == nil || dt.ParticipantSid == "" {
			continue
		}
		dataTrackInfosByParticipant[dt.ParticipantSid] = append(dataTrackInfosByParticipant[dt.ParticipantSid], cloneDataTrackInfo(dt))
	}
	result := make([]*proto.ParticipantInfo, 0, len(r.storeParticipants))
	for _, p := range r.participants {
		if p.sid != excludeSid {
			result = append(result, cloneParticipantInfo(p.ToProto()))
		}
	}
	for sid, info := range r.storeParticipants {
		if sid == excludeSid {
			continue
		}
		if _, ok := r.participants[sid]; ok {
			continue
		}
		cloned := cloneParticipantInfo(info)
		cloned.Tracks = trackInfosByParticipant[sid]
		cloned.DataTracks = dataTrackInfosByParticipant[sid]
		result = append(result, cloned)
	}
	r.mu.RUnlock()
	return result
}

// GetAllGroupInfos returns GroupInfo for all groups, with ParticipantCount
// derived from storeParticipants for cross-node accuracy.
func (r *Room) GetAllGroupInfos() []*proto.GroupInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*proto.GroupInfo, 0, len(r.storeGroups))
	for _, info := range r.storeGroups {
		result = append(result, r.enrichGroupInfoLocked(info))
	}
	return result
}

// enrichGroupInfoLocked clones a GroupInfo and populates ParticipantCount
// from storeParticipants. Must be called with r.mu held.
func (r *Room) enrichGroupInfoLocked(info *proto.GroupInfo) *proto.GroupInfo {
	cloned := cloneGroupInfo(info)
	count := 0
	for _, p := range r.storeParticipants {
		if p.GroupId == info.GroupId && p.State != proto.ParticipantInfo_DISCONNECTED {
			count++
		}
	}
	cloned.ParticipantCount = count
	return cloned
}

// GetAllMessagesSince returns messages from the local message cache.
func (r *Room) GetAllMessagesSince(since int64) []*proto.MessageNotification {
	r.mu.RLock()
	var result []*proto.MessageNotification
	for _, m := range r.messages {
		ts := m.Timestamp
		if m.EditTimestamp != nil && *m.EditTimestamp > ts {
			ts = *m.EditTimestamp
		}
		if since == 0 || ts > since {
			result = append(result, cloneMessage(m))
		}
	}
	r.mu.RUnlock()
	return result
}

// GetAllSharedState returns the current shared-state cache.
func (r *Room) GetAllSharedState() map[string]string {
	r.mu.RLock()
	result := make(map[string]string, len(r.sharedState))
	for k, v := range r.sharedState {
		result[k] = v
	}
	r.mu.RUnlock()
	return result
}

// BroadcastNotification sends a notification to all participants except the excluded one.
func (r *Room) BroadcastNotification(method string, body any, excludeSid string) {
	r.mu.RLock()
	participants := make([]*Participant, 0, len(r.participants))
	for _, p := range r.participants {
		if p.sid == excludeSid {
			continue
		}
		participants = append(participants, p)
	}
	r.mu.RUnlock()

	for _, p := range participants {
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
				r.runAsync(func() { r.roomStore.PutMessage(msg) })
				return
			}
		}
	}

	r.messages = append(r.messages, msg)
	r.runAsync(func() { r.roomStore.PutMessage(msg) })

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

	ids := make([]string, len(evicted))
	for i, m := range evicted {
		ids[i] = m.ID
	}
	r.runAsync(func() { r.roomStore.DeleteMessagesByIDs(ids) })
}

// DeleteMessage removes a single message by ID. Returns true if found.
func (r *Room) DeleteMessage(id, participantSid string) bool {
	r.mu.Lock()
	found := false
	for _, m := range r.messages {
		if m.ID == id {
			m.Deleted = true
			found = true
			break
		}
	}
	r.mu.Unlock()

	if found {
		r.runAsync(func() { r.roomStore.DeleteMessage(id) })
	}
	return found
}

// DeleteMessagesByType removes all messages of a given type and returns their IDs.
func (r *Room) DeleteMessagesByType(msgType string) []string {
	r.mu.Lock()
	var ids []string
	for _, m := range r.messages {
		if m.Type == msgType {
			ids = append(ids, m.ID)
			m.Deleted = true
		}
	}
	r.mu.Unlock()

	if len(ids) > 0 {
		r.runAsync(func() { r.roomStore.DeleteMessagesByIDs(ids) })
	}
	return ids
}

// UpdateState sets a key in the room's shared state.
func (r *Room) UpdateState(key, value, participantSid, participantIdentity string) {
	r.mu.Lock()
	r.sharedState[key] = value
	r.mu.Unlock()

	r.runAsync(func() { r.roomStore.PutState(key, value) })
}

// DeleteState removes a key from the room's shared state.
func (r *Room) DeleteState(key, participantSid, participantIdentity string) {
	r.mu.Lock()
	delete(r.sharedState, key)
	r.mu.Unlock()

	r.runAsync(func() { r.roomStore.DeleteState(key) })
}

// GetMessagesSince returns all messages with a timestamp strictly
// greater than the provided value. If since is 0 all messages are returned.
func (r *Room) GetMessagesSince(since int64) []*proto.MessageNotification {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var out []*proto.MessageNotification
	for _, m := range r.messages {
		ts := m.Timestamp
		if m.EditTimestamp != nil && *m.EditTimestamp > ts {
			ts = *m.EditTimestamp
		}
		if ts > since && !m.Deleted {
			out = append(out, m)
		}
	}
	return out
}

// OnParticipantUpdated syncs the participant snapshot to the shared store.
// The store watch handles broadcasting ParticipantUpdated to other participants.
func (r *Room) OnParticipantUpdated(p *Participant) {
	r.runAsync(func() { r.roomStore.PutParticipant(p.ToProto()) })
}

// OnTrackPublished handles a new track being published.
// For auto-subscribe rooms, it creates subscriptions for all other participants.
func (r *Room) OnTrackPublished(publisher *Participant, track *PublishedTrack) {
	if r.cascade.OnTrackPublished != nil {
		r.cascade.OnTrackPublished(track.sid, track.producer)
	}

	r.runAsync(func() { r.roomStore.PutTrack(track.ToProto()) })
}

// OnTrackUnpublished handles a track being unpublished.
func (r *Room) OnTrackUnpublished(publisher *Participant, trackSid string) {
	if r.cascade.OnTrackUnpublished != nil {
		r.cascade.OnTrackUnpublished(trackSid)
	}

	r.runAsync(func() { r.roomStore.DeleteTrack(trackSid) })
}

// OnTrackMuted handles a track being muted/unmuted.
func (r *Room) OnTrackMuted(publisher *Participant, trackSid string, muted bool) {
	if track := r.FindPublishedTrack(trackSid); track != nil {
		r.runAsync(func() { r.roomStore.PutTrack(track.ToProto()) })
	}
}

// OnDataTrackPublished handles auto-subscribe for data tracks.
func (r *Room) OnDataTrackPublished(publisher *Participant, track *DataPublishedTrack) {
	if r.cascade.OnDataTrackPublished != nil {
		r.cascade.OnDataTrackPublished(track.sid, track.dataProducer)
	}
	r.runAsync(func() { r.roomStore.PutDataTrack(track.ToProto()) })
}

// OnDataTrackUnpublished handles a data track being unpublished.
func (r *Room) OnDataTrackUnpublished(publisher *Participant, dataTrackSid string) {
	if r.cascade.OnDataTrackUnpublished != nil {
		r.cascade.OnDataTrackUnpublished(dataTrackSid)
	}
	r.runAsync(func() { r.roomStore.DeleteDataTrack(dataTrackSid) })
}

// SubscribeExistingTracks subscribes a new participant to all tracks carried
// in otherInfos (the same slice returned in the ready response), so the
// server-side subscriptions are consistent with what the client sees.
func (r *Room) SubscribeExistingTracks(subscriber *Participant, otherInfos []*proto.ParticipantInfo) {
	if !r.config.AutoSubscribe || !subscriber.CanSubscribe() || !subscriber.AutoSubscribe() {
		return
	}

	subscriberGroupId := subscriber.GetGroupId()
	for _, info := range otherInfos {
		if !r.shouldPeerSubscribe(subscriberGroupId, info.GroupId) {
			continue
		}

		// Local participant — use real track objects (supports backup codec)
		if localPub := r.GetParticipant(info.Sid); localPub != nil {
			for _, t := range localPub.GetPublishedTracks() {
				go func(track *PublishedTrack, pub *Participant) {
					if err := subscriber.Subscribe(track, pub); err != nil {
						r.logger.Warn("Subscribe existing track failed",
							"subscriber", subscriber.identity, "track", track.sid, "error", err)
					}
				}(t, localPub)
			}
			for _, dt := range localPub.GetDataPublishedTracks() {
				go func(track *DataPublishedTrack, pub *Participant) {
					if err := subscriber.SubscribeData(track, pub); err != nil {
						r.logger.Warn("Subscribe existing data track failed",
							"subscriber", subscriber.identity, "dataTrack", track.sid, "error", err)
					}
				}(dt, localPub)
			}
			continue
		}

		// Remote participant — resolve via FIB + PipeToRouter
		for _, ti := range info.Tracks {
			go func(trackInfo *proto.TrackInfo, pSid string) {
				track, publisher, err := r.resolveRemoteTrack(trackInfo, pSid)
				if err != nil {
					r.logger.Warn("Resolve remote track failed",
						"subscriber", subscriber.identity, "track", trackInfo.Sid, "error", err)
					return
				}
				if err := subscriber.Subscribe(track, publisher); err != nil {
					r.logger.Warn("Subscribe existing remote track failed",
						"subscriber", subscriber.identity, "track", trackInfo.Sid, "error", err)
				}
			}(ti, info.Sid)
		}
		for _, dti := range info.DataTracks {
			go func(dataTrackInfo *proto.DataTrackInfo, pSid string) {
				track, publisher, err := r.resolveRemoteDataTrack(dataTrackInfo, pSid)
				if err != nil {
					r.logger.Warn("Resolve remote data track failed",
						"subscriber", subscriber.identity, "dataTrack", dataTrackInfo.Sid, "error", err)
					return
				}
				if err := subscriber.SubscribeData(track, publisher); err != nil {
					r.logger.Warn("Subscribe existing remote data track failed",
						"subscriber", subscriber.identity, "dataTrack", dataTrackInfo.Sid, "error", err)
				}
			}(dti, info.Sid)
		}
	}
}

// ============================================================
// Group (Breakout Room) Management
// ============================================================

func (r *Room) CreateGroup(name, ownerSid string, permissions *proto.ParticipantPermission) *Group {
	g := NewGroup(name, permissions)
	g.ownerSid = ownerSid
	groupInfo := g.ToProto()
	r.mu.Lock()
	r.groups[g.SID()] = g
	r.mu.Unlock()

	r.runAsync(func() { r.roomStore.PutGroup(groupInfo) })

	r.logger.Info("Group created", "groupId", g.SID(), "name", name, "owner", ownerSid)
	return g
}

func (r *Room) DeleteGroup(groupId, ownerSid string) error {
	r.mu.Lock()
	g, ok := r.groups[groupId]
	if !ok {
		r.mu.Unlock()
		return errors.New("group not found")
	}
	delete(r.groups, groupId)
	// Stamp ownerSid so the store-watch broadcast can exclude the deleter.
	if info := r.storeGroups[groupId]; info != nil {
		info.OwnerSid = ownerSid
	}
	r.mu.Unlock()

	participantSids := g.ParticipantSids()
	for _, pSid := range participantSids {
		p := r.GetParticipant(pSid)
		if p == nil {
			continue
		}
		r.participantLeaveGroup(p, g)
	}

	r.runAsync(func() { r.roomStore.DeleteGroup(groupId) })

	r.logger.Info("Group deleted", "groupId", groupId, "owner", ownerSid, "participantsReturned", len(participantSids))
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
// Returns ParticipantInfo for all other group members (local + remote).
func (r *Room) JoinGroup(p *Participant, groupId string) ([]*proto.ParticipantInfo, error) {
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

	g.AddParticipant(p.sid)
	p.SetGroupId(groupId)

	if g.permissions != nil {
		p.SetEffectivePermission(g.permissions)
	}

	r.rewireSubscriptions(p)

	r.mu.Lock()
	r.storeGroups[groupId] = cloneGroupInfo(g.ToProto())
	r.mu.Unlock()

	r.runAsync(func() { r.roomStore.PutParticipant(p.ToProto()) })
	r.runAsync(func() { r.roomStore.PutGroup(g.ToProto()) })

	others := r.groupMemberInfos(groupId, p.sid)

	r.logger.Info("Participant joined group", "participant", p.identity,
		"groupId", groupId, "from", oldGroupId)
	return others, nil
}

// groupMemberInfos returns ParticipantInfo for all group members (local +
// remote) excluding excludeSid.
func (r *Room) groupMemberInfos(groupId, excludeSid string) []*proto.ParticipantInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]bool)
	var result []*proto.ParticipantInfo
	for _, lp := range r.participants {
		if lp.sid == excludeSid || lp.GetGroupId() != groupId {
			continue
		}
		seen[lp.sid] = true
		result = append(result, cloneParticipantInfo(lp.ToProto()))
	}
	for sid, info := range r.storeParticipants {
		if sid == excludeSid || info.GroupId != groupId || seen[sid] {
			continue
		}
		if info.State == proto.ParticipantInfo_DISCONNECTED {
			continue
		}
		result = append(result, cloneParticipantInfo(info))
	}
	return result
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

	return nil
}

// participantLeaveGroup is the internal helper that removes a participant
// from a group, restores room-level permissions, and rewires subscriptions.
func (r *Room) participantLeaveGroup(p *Participant, g *Group) {
	g.RemoveParticipant(p.sid)
	shouldPersistGroup := false
	r.mu.Lock()
	if _, ok := r.groups[g.SID()]; ok {
		shouldPersistGroup = true
		r.storeGroups[g.SID()] = cloneGroupInfo(g.ToProto())
	}
	r.mu.Unlock()
	if shouldPersistGroup {
		groupInfo := g.ToProto()
		r.runAsync(func() { r.roomStore.PutGroup(groupInfo) })
	}
	p.SetGroupId("")
	p.RestoreRoomPermission()

	r.rewireSubscriptions(p)

	r.runAsync(func() { r.roomStore.PutParticipant(p.ToProto()) })

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

	// Handle remote tracks: subscribe/unsubscribe target to tracks whose
	// publisher lives on another node.
	if !target.IsReady() || !target.CanSubscribe() || !target.AutoSubscribe() {
		return
	}
	r.mu.RLock()
	var toSubscribe []*proto.TrackInfo
	var toUnsubscribe []string
	for trackSid, entry := range r.storeTracks {
		if entry == nil || entry.info == nil || entry.info.ParticipantSid == target.sid {
			continue
		}
		pubSid := entry.info.ParticipantSid
		if r.participants[pubSid] != nil {
			continue // local publisher, already handled above
		}
		pubGroupId := ""
		if pi := r.storeParticipants[pubSid]; pi != nil {
			pubGroupId = pi.GroupId
		}
		shouldSub := r.shouldPeerSubscribe(targetGroupId, pubGroupId)
		if shouldSub && !target.HasSubscription(trackSid) {
			toSubscribe = append(toSubscribe, entry.info)
		} else if !shouldSub && target.HasSubscription(trackSid) {
			toUnsubscribe = append(toUnsubscribe, trackSid)
		}
	}
	r.mu.RUnlock()

	for _, info := range toSubscribe {
		go func(info *proto.TrackInfo) {
			track, pub, err := r.resolveRemoteTrack(info, info.ParticipantSid)
			if err != nil {
				return
			}
			_ = target.Subscribe(track, pub)
		}(info)
	}
	for _, trackSid := range toUnsubscribe {
		go target.CloseSubscription(trackSid)
	}
}

// rewireRemoteParticipantSubscriptions is called when a remote participant's
// groupId changes. It adjusts local subscribers' subscriptions to that
// remote participant's tracks.
func (r *Room) rewireRemoteParticipantSubscriptions(remoteSid, remoteGroupId string) {
	if !r.config.AutoSubscribe {
		return
	}
	r.mu.RLock()
	if r.participants[remoteSid] != nil {
		r.mu.RUnlock()
		return // local participant, handled by rewireSubscriptions
	}
	type trackRef struct {
		sid  string
		info *proto.TrackInfo
	}
	var tracks []trackRef
	for _, entry := range r.storeTracks {
		if entry != nil && entry.info != nil && entry.info.ParticipantSid == remoteSid {
			tracks = append(tracks, trackRef{sid: entry.info.Sid, info: entry.info})
		}
	}
	locals := make([]*Participant, 0, len(r.participants))
	for _, p := range r.participants {
		locals = append(locals, p)
	}
	r.mu.RUnlock()

	for _, sub := range locals {
		if !sub.IsReady() || !sub.CanSubscribe() || !sub.AutoSubscribe() {
			continue
		}
		shouldSub := r.shouldPeerSubscribe(sub.GetGroupId(), remoteGroupId)
		for _, t := range tracks {
			if shouldSub && !sub.HasSubscription(t.sid) {
				go func(subscriber *Participant, info *proto.TrackInfo) {
					track, pub, err := r.resolveRemoteTrack(info, remoteSid)
					if err != nil {
						return
					}
					_ = subscriber.Subscribe(track, pub)
				}(sub, t.info)
			} else if !shouldSub && sub.HasSubscription(t.sid) {
				go sub.CloseSubscription(t.sid)
			}
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
	for _, t := range p.GetPublishedTracks() {
		r.roomStore.DeleteTrack(t.sid)
		r.roomStore.DeleteTrackSub(t.sid)
	}
	for _, t := range p.GetDataPublishedTracks() {
		r.roomStore.DeleteDataTrack(t.sid)
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
	r.mu.Unlock()

	if r.watchClose != nil {
		r.watchClose()
	}
	r.watchWG.Wait()

	r.mu.Lock()
	participants := make([]*Participant, 0, len(r.participants))
	for _, p := range r.participants {
		participants = append(participants, p)
	}
	r.participants = make(map[string]*Participant)
	r.storeParticipants = make(map[string]*proto.ParticipantInfo)
	r.storeTracks = make(map[string]*storeTrackEntry)
	r.storeDataTracks = make(map[string]*proto.DataTrackInfo)
	r.mu.Unlock()

	for _, p := range participants {
		r.deleteLocalParticipantKV(p)
		p.Close(reason)
	}

	r.asyncWG.Wait()

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
