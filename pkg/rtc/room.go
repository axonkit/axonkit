package rtc

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jiyeyuran/omnistream/pkg/config"
	"github.com/jiyeyuran/omnistream/pkg/proto"
)

var (
	ErrRoomFull          = errors.New("room is full")
	ErrRoomClosed        = errors.New("room is closed")
	ErrDuplicateIdentity = errors.New("duplicate identity")
)

type Room struct {
	sid          string
	name         string
	config       config.RoomConfig
	metadata     string
	creationTime time.Time

	participants map[string]*Participant
	mu           sync.RWMutex

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

	r.logger.Info("Participant added", "participant", p.identity, "sid", p.sid,
		"count", len(r.participants))

	return nil
}

func (r *Room) RemoveParticipant(sid string) {
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

	// Notify others
	r.BroadcastNotification(proto.MethodParticipantLeft, &proto.ParticipantLeftNotification{
		ParticipantSid: sid,
		Reason:         proto.DisconnectReason_CLIENT_INITIATED,
	}, sid)

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

// OnTrackPublished handles a new track being published.
// For auto-subscribe rooms, it creates subscriptions for all other participants.
func (r *Room) OnTrackPublished(publisher *Participant, track *PublishedTrack) {
	r.BroadcastNotification(proto.MethodTrackPublished, &proto.TrackPublishedNotification{
		ParticipantSid: publisher.sid,
		Track:          track.ToProto(),
	}, publisher.sid)

	if !r.config.AutoSubscribe {
		return
	}

	others := r.GetOtherParticipants(publisher.sid)
	for _, sub := range others {
		if !sub.IsReady() || !sub.CanSubscribe() {
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

// SubscribeExistingTracks subscribes a new participant to all existing tracks in the room.
func (r *Room) SubscribeExistingTracks(subscriber *Participant) {
	if !r.config.AutoSubscribe || !subscriber.CanSubscribe() {
		return
	}

	others := r.GetOtherParticipants(subscriber.sid)
	for _, publisher := range others {
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

func (r *Room) startEmptyTimer() {
	if r.config.DepartureTimeout <= 0 {
		return
	}
	r.emptyTimer = time.AfterFunc(r.config.DepartureTimeout, func() {
		r.logger.Info("Room empty timeout reached, closing")
		r.Close()
	})
}

func (r *Room) stopEmptyTimer() {
	if r.emptyTimer != nil {
		r.emptyTimer.Stop()
		r.emptyTimer = nil
	}
}

func (r *Room) Close() {
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
		p.Close(proto.DisconnectReason_ROOM_CLOSED)
	}

	if r.onClose != nil {
		r.onClose(r)
	}

	r.logger.Info("Room closed")
}
