package rtc

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"

	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/store"
)

// ForwardedFromRoomAttr is stamped on a forwarded participant so operators
// (and rehydration logic) can recognize it as a mirrored copy and find the
// source room. The participant's Sid in the destination store is identical
// to the Sid in the source room, so no extra attribute is needed to recover
// the source participant identity.
const ForwardedFromRoomAttr = "axonkit.forwarded_from_room"

// forwarderKey returns a deterministic, process-local key used by
// RoomManager to track active CrossRoomForwarder instances in its
// `forwarders` map.
func forwarderKey(dstRoom, srcSid string) string {
	return fmt.Sprintf("%s/%s", srcSid, dstRoom)
}

// CrossRoomForwarder mirrors a participant from srcRoom into dstRoom using
// the shared RoomStore KV. It writes a ParticipantInfo and the source
// participant's TrackInfos into the destination room under the SAME Sid as
// the source — so subscribers in dstRoom see an ordinary participant — and
// keeps those entries in sync with the source participant's lifecycle
// (track publish / unpublish / mute, disconnect) until Close is called.
//
// Media does not re-encode: subscribers in dstRoom see the forwarded
// participant via the existing auto-subscribe path and pipe the original
// producer through FIB + media.Manager.
//
// RoomManager uses a deterministic, process-local forwarderKey(srcSid,
// dstRoom) in its `forwarders` map for idempotency; that key is never
// exposed in the KV or to API callers.
type CrossRoomForwarder struct {
	srcRoom           string
	dstRoom           string
	srcParticipantSid string
	srcIdentity       string
	key               string

	srcStore *RoomStore
	dstStore *RoomStore

	ctx    context.Context
	cancel context.CancelFunc

	mu             sync.Mutex
	mirroredTracks map[string]struct{}
	mirroredData   map[string]struct{}
	closed         bool

	onClose func(key string)

	logger *slog.Logger
}

// CrossRoomForwarderParams wires a CrossRoomForwarder to the shared stores.
type CrossRoomForwarderParams struct {
	SrcStore       *RoomStore
	DstStore       *RoomStore
	SrcRoom        string
	DstRoom        string
	SrcParticipant *proto.ParticipantInfo
	// OnClose is invoked when the forwarder finishes teardown (including
	// an implicit teardown triggered by the source participant going away).
	// The argument is the process-local forwarder key (see forwarderKey).
	OnClose func(key string)
}

// NewCrossRoomForwarder constructs a forwarder without starting it. Callers
// must invoke Start to publish the mirror state and begin watching.
func NewCrossRoomForwarder(params CrossRoomForwarderParams) *CrossRoomForwarder {
	key := forwarderKey(params.DstRoom, params.SrcParticipant.Sid)
	return &CrossRoomForwarder{
		srcRoom:           params.SrcRoom,
		dstRoom:           params.DstRoom,
		srcParticipantSid: params.SrcParticipant.Sid,
		srcIdentity:       params.SrcParticipant.Identity,
		key:               key,
		srcStore:          params.SrcStore,
		dstStore:          params.DstStore,
		mirroredTracks:    make(map[string]struct{}),
		mirroredData:      make(map[string]struct{}),
		onClose:           params.OnClose,
		logger: slog.Default().With(
			"component", "CrossRoomForwarder",
			"srcRoom", params.SrcRoom, "dstRoom", params.DstRoom,
			"srcSid", params.SrcParticipant.Sid, "forwarderKey", key,
		),
	}
}

// Key returns the process-local forwarder key used by RoomManager's
// `forwarders` map. It is not a ParticipantSid.
func (f *CrossRoomForwarder) Key() string { return f.key }

// SrcParticipantSid returns the source participant SID being mirrored.
// Because dstStore uses this SID verbatim, callers may use it to find the
// forwarded participant row in the destination room.
func (f *CrossRoomForwarder) SrcParticipantSid() string { return f.srcParticipantSid }

// Start publishes the initial synthetic participant and its tracks, then
// begins watching the source store for changes. It returns after the
// initial snapshot has been written so callers can rely on subscribers in
// the destination room observing the forwarded participant before Start
// returns.
func (f *CrossRoomForwarder) Start(ctx context.Context) error {
	f.ctx, f.cancel = context.WithCancel(ctx)

	src := f.srcStore.GetParticipant(f.srcParticipantSid)
	if src == nil {
		f.cancel()
		return fmt.Errorf("source participant %q not found in %q", f.srcParticipantSid, f.srcRoom)
	}
	if src.State == proto.ParticipantInfo_DISCONNECTED {
		f.cancel()
		return fmt.Errorf("source participant %q already disconnected", f.srcParticipantSid)
	}

	f.publishForwardedParticipant(src)

	allTracks := f.srcStore.GetAllTracks()
	for _, t := range allTracks[f.srcParticipantSid] {
		f.mirrorTrack(t)
	}
	allData := f.srcStore.GetAllDataTracks()
	for _, dt := range allData[f.srcParticipantSid] {
		f.mirrorDataTrack(dt)
	}

	srcWatch, err := f.srcStore.Watch(f.ctx)
	if err != nil {
		f.teardownStoreEntries()
		f.cancel()
		return fmt.Errorf("watch src store: %w", err)
	}

	dstWatch, err := f.dstStore.Watch(f.ctx)
	if err != nil {
		f.teardownStoreEntries()
		f.cancel()
		return fmt.Errorf("watch dst store: %w", err)
	}

	go f.runSrc(srcWatch.Events)
	go f.runDst(dstWatch.Events)
	f.logger.Info("CrossRoomForwarder started",
		"tracks", len(allTracks[f.srcParticipantSid]),
		"dataTracks", len(allData[f.srcParticipantSid]))
	return nil
}

// publishForwardedParticipant writes the forwarded ParticipantInfo into
// the destination store. The row keeps the source participant's Sid so
// subscribers see it as an ordinary participant. The Tracks and DataTracks
// slices are cleared because individual TrackInfo rows are stored under
// their own keys and discovered via store watch; carrying them in the
// ParticipantInfo row would confuse consumers that expect authoritative
// data under the track.* keys.
//
// When Close() has already run the write is silently dropped; this
// suppresses late events that race with teardown and would otherwise
// touch a shutting-down KV.
func (f *CrossRoomForwarder) publishForwardedParticipant(src *proto.ParticipantInfo) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.mu.Unlock()

	mirror := cloneParticipantInfo(src)
	mirror.Tracks = nil
	mirror.DataTracks = nil
	// Cross-room group semantics don't survive: a forwarded participant
	// starts in the main room of the destination.
	mirror.GroupId = ""
	if mirror.Attributes == nil {
		mirror.Attributes = make(map[string]string, 1)
	} else {
		mirror.Attributes = maps.Clone(mirror.Attributes)
	}
	mirror.Attributes[ForwardedFromRoomAttr] = f.srcRoom
	f.dstStore.PutParticipant(mirror)
}

// mirrorTrack writes the source TrackInfo verbatim into the destination
// store. Because the Sid / ParticipantSid already point at the source
// participant (which we mirror under the same Sid in dstStore), no
// rewriting is needed.
func (f *CrossRoomForwarder) mirrorTrack(info *proto.TrackInfo) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.mirroredTracks[info.Sid] = struct{}{}
	f.mu.Unlock()
	f.dstStore.PutTrack(cloneTrackInfo(info))
}

func (f *CrossRoomForwarder) mirrorDataTrack(info *proto.DataTrackInfo) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.mirroredData[info.Sid] = struct{}{}
	f.mu.Unlock()
	f.dstStore.PutDataTrack(cloneDataTrackInfo(info))
}

// runSrc mirrors state from the source room into the destination room.
func (f *CrossRoomForwarder) runSrc(events <-chan *RoomStoreEvent) {
	for evt := range events {
		if evt == nil {
			// Initial snapshot sentinel: we already seeded above, nothing to do.
			continue
		}
		switch evt.Type {
		case "participant":
			f.handleParticipantEvent(evt)
		case "track":
			f.handleTrackEvent(evt)
		case "dataTrack":
			f.handleDataTrackEvent(evt)
		}
	}
	// Watch closed (ctx done or store closed): make sure teardown runs.
	f.Close()
}

// runDst watches the destination room for external teardown signals on
// our forwarded participant row. When any node (including ours) writes a
// DISCONNECTED state or deletes the participant entry — typically because
// RemoveParticipantFromRoom(dstRoom, identity) was invoked — this
// goroutine closes the forwarder so it stops re-creating the mirror.
func (f *CrossRoomForwarder) runDst(events <-chan *RoomStoreEvent) {
	for evt := range events {
		if evt == nil || evt.Type != "participant" || evt.Key != f.srcParticipantSid {
			continue
		}
		switch evt.Operation {
		case store.OpPut:
			if evt.Participant != nil && evt.Participant.State == proto.ParticipantInfo_DISCONNECTED {
				f.logger.Info("Forwarded participant marked DISCONNECTED externally, tearing down",
					"reason", evt.Participant.DisconnectReason)
				go f.Close()
				return
			}
		case store.OpDelete:
			f.logger.Info("Forwarded participant entry deleted externally, tearing down")
			go f.Close()
			return
		}
	}
}

func (f *CrossRoomForwarder) handleParticipantEvent(evt *RoomStoreEvent) {
	if evt.Key != f.srcParticipantSid {
		return
	}
	switch evt.Operation {
	case store.OpPut:
		if evt.Participant == nil {
			return
		}
		if evt.Participant.State == proto.ParticipantInfo_DISCONNECTED {
			f.logger.Info("Source participant disconnected, tearing down forwarder")
			go f.Close()
			return
		}
		f.publishForwardedParticipant(evt.Participant)
	case store.OpDelete:
		f.logger.Info("Source participant removed, tearing down forwarder")
		go f.Close()
	}
}

func (f *CrossRoomForwarder) handleTrackEvent(evt *RoomStoreEvent) {
	trackSid := trackSidFromEvent(evt)
	switch evt.Operation {
	case store.OpPut:
		if evt.Track == nil || evt.Track.ParticipantSid != f.srcParticipantSid {
			return
		}
		f.mirrorTrack(evt.Track)
	case store.OpDelete:
		f.mu.Lock()
		_, ok := f.mirroredTracks[trackSid]
		if ok {
			delete(f.mirroredTracks, trackSid)
		}
		f.mu.Unlock()
		if !ok {
			return
		}
		f.dstStore.DeleteTrack(trackSid)
	}
}

func (f *CrossRoomForwarder) handleDataTrackEvent(evt *RoomStoreEvent) {
	trackSid := trackSidFromEvent(evt)
	switch evt.Operation {
	case store.OpPut:
		if evt.DataTrack == nil || evt.DataTrack.ParticipantSid != f.srcParticipantSid {
			return
		}
		f.mirrorDataTrack(evt.DataTrack)
	case store.OpDelete:
		f.mu.Lock()
		_, ok := f.mirroredData[trackSid]
		if ok {
			delete(f.mirroredData, trackSid)
		}
		f.mu.Unlock()
		if !ok {
			return
		}
		f.dstStore.DeleteDataTrack(trackSid)
	}
}

// trackSidFromEvent reconstructs the full track SID, since RoomStore's
// event parser splits the dotted streamID (routerId.producerId) into
// Key ("routerId") and SubKey ("producerId").
func trackSidFromEvent(evt *RoomStoreEvent) string {
	if evt.SubKey == "" {
		return evt.Key
	}
	return evt.Key + "." + evt.SubKey
}

// Close stops the watcher and removes the forwarded participant and its
// tracks from the destination store. Safe to call multiple times.
func (f *CrossRoomForwarder) Close() {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.closed = true
	tracks := slices.Collect(maps.Keys(f.mirroredTracks))
	dataTracks := slices.Collect(maps.Keys(f.mirroredData))
	f.mirroredTracks = nil
	f.mirroredData = nil
	f.mu.Unlock()

	if f.cancel != nil {
		f.cancel()
	}

	for _, sid := range tracks {
		f.dstStore.DeleteTrack(sid)
	}
	for _, sid := range dataTracks {
		f.dstStore.DeleteDataTrack(sid)
	}
	f.dstStore.DeleteParticipant(f.srcParticipantSid)

	if f.onClose != nil {
		f.onClose(f.key)
	}
	f.logger.Info("CrossRoomForwarder closed")
}

// teardownStoreEntries is the partial-cleanup used when Start fails after
// writing the forwarded participant. Close() handles the steady-state path.
func (f *CrossRoomForwarder) teardownStoreEntries() {
	f.mu.Lock()
	tracks := slices.Collect(maps.Keys(f.mirroredTracks))
	dataTracks := slices.Collect(maps.Keys(f.mirroredData))
	clear(f.mirroredTracks)
	clear(f.mirroredData)
	f.mu.Unlock()
	for _, sid := range tracks {
		f.dstStore.DeleteTrack(sid)
	}
	for _, sid := range dataTracks {
		f.dstStore.DeleteDataTrack(sid)
	}
	f.dstStore.DeleteParticipant(f.srcParticipantSid)
}
