package rtc

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/axonkit/axonkit/pkg/auth"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/types"
	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/rs/xid"
)

const dynacastDowngradeDelay = 5 * time.Second

// PublishedTrack holds a published media track and its signaling metadata.
type PublishedTrack struct {
	sid      string
	name     string
	source   proto.TrackSource
	kind     mediasoup.MediaKind
	producer *mediasoup.Producer
	muted    bool
	width    uint32
	height   uint32
	appData  map[string]any

	// Backup codec support
	videoCodec        string
	backupCodec       string
	backupCodecPolicy proto.BackupCodecPolicy
	backupProducer    *mediasoup.Producer
	backupSid         string
	regressed         bool                   // true after REGRESSION has fired
	pendingBackupSubs []*pendingBackupSub    // subscribers waiting for backup producer
	backupConsumers   map[string]string      // consumerID → codec used ("primary"/"backup")
	backupRequested   bool                   // true once we've asked the publisher for backup

	// Dynacast: track which consumers are active and their max spatial layer.
	// Protected by mu.
	mu                  sync.Mutex
	activeConsumers     map[string]*consumerLayerState // consumerID → state (local node)
	remoteSubscriptions map[string]*TrackSubInfo       // nodeID → aggregated state (other nodes)
	lastQualities       [3]bool                        // [LOW, MEDIUM, HIGH] last notified
	pendingQualities    [3]bool                        // qualities to send after debounce
	debounceTimer       *time.Timer
	publisher           *Participant
}

// pendingBackupSub holds a subscriber that failed to consume the primary
// codec and is waiting for the backup producer to be published.
type pendingBackupSub struct {
	subscriber *Participant
	publisher  *Participant
}

type consumerLayerState struct {
	paused       bool
	spatialLayer int32 // -1 means unset (use all)
}

func (t *PublishedTrack) ToProto() *proto.TrackInfo {
	return &proto.TrackInfo{
		Sid:               t.sid,
		Kind:              t.kind,
		Name:              t.name,
		Muted:             t.muted,
		Width:             t.width,
		Height:            t.height,
		Source:            t.source,
		BackupCodecPolicy: t.backupCodecPolicy,
	}
}

// stopDebounce cancels any pending debounce timer. Safe to call from outside
// the mu lock since it acquires mu internally.
func (t *PublishedTrack) stopDebounce() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.debounceTimer != nil {
		t.debounceTimer.Stop()
		t.debounceTimer = nil
	}
}

// addConsumer registers a consumer for dynacast tracking.
func (t *PublishedTrack) addConsumer(consumerID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.activeConsumers == nil {
		return
	}
	t.activeConsumers[consumerID] = &consumerLayerState{spatialLayer: -1}
	t.recalcLocked()
}

// removeConsumer unregisters a consumer and triggers recalculation.
func (t *PublishedTrack) removeConsumer(consumerID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.activeConsumers == nil {
		return
	}
	delete(t.activeConsumers, consumerID)
	t.recalcLocked()
}

// updateConsumerLayers updates a consumer's preferred spatial layer.
func (t *PublishedTrack) updateConsumerLayers(consumerID string, spatialLayer int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.activeConsumers == nil {
		return
	}
	if st, ok := t.activeConsumers[consumerID]; ok {
		st.spatialLayer = spatialLayer
	}
	t.recalcLocked()
}

// updateConsumerPaused updates whether a consumer is paused.
func (t *PublishedTrack) updateConsumerPaused(consumerID string, paused bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.activeConsumers == nil {
		return
	}
	if st, ok := t.activeConsumers[consumerID]; ok {
		st.paused = paused
	}
	t.recalcLocked()
}

// updateRemoteSubscription updates the subscription state from a remote node.
func (t *PublishedTrack) updateRemoteSubscription(nodeID string, info *TrackSubInfo) {
	t.mu.Lock()
	if t.remoteSubscriptions == nil {
		t.remoteSubscriptions = make(map[string]*TrackSubInfo)
	}
	if info == nil || (info.Layers == [3]bool{} && !info.HasBackupSub) {
		delete(t.remoteSubscriptions, nodeID)
	} else {
		t.remoteSubscriptions[nodeID] = info
	}

	// If a remote node needs the backup codec and we haven't requested it yet,
	// trigger the backup codec request.
	needBackupRequest := false
	if info != nil && info.HasBackupSub && t.backupCodec != "" && !t.backupRequested && t.backupProducer == nil {
		needBackupRequest = true
		t.backupRequested = true
	}

	t.recalcLocked()
	t.mu.Unlock()

	if needBackupRequest {
		t.requestBackupCodec()
	}
}

// removeRemoteSubscription removes a remote node's subscription state.
func (t *PublishedTrack) removeRemoteSubscription(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.remoteSubscriptions == nil {
		return
	}
	delete(t.remoteSubscriptions, nodeID)
	t.recalcLocked()
}

// recalcLocked computes the per-layer subscription state for video tracks.
// Must be called with mu held. Dynacast only applies to video.
//
// Debounce strategy:
//   - Upgrade (any layer false→true): notify publisher immediately so it can
//     start sending the layer with minimal latency.
//   - Downgrade only (some layers true→false, none false→true): delay
//     notification by dynacastDowngradeDelay so that brief fluctuations
//     (e.g. window resize, scroll) don't cause unnecessary layer toggling.
func (t *PublishedTrack) recalcLocked() {
	if t.publisher == nil || t.kind != mediasoup.MediaKindVideo {
		return
	}

	var q [3]bool

	// Local consumers — each consumer marks the specific layer it needs.
	for _, st := range t.activeConsumers {
		if st.paused {
			continue
		}
		if st.spatialLayer < 0 {
			q[0], q[1], q[2] = true, true, true
		} else if st.spatialLayer >= 0 && st.spatialLayer <= 2 {
			q[st.spatialLayer] = true
		}
	}

	// Remote node subscriptions — already per-layer.
	for _, rs := range t.remoteSubscriptions {
		for i, needed := range rs.Layers {
			if needed {
				q[i] = true
			}
		}
	}

	if q == t.lastQualities {
		// No change — cancel any pending downgrade timer.
		if t.debounceTimer != nil {
			t.debounceTimer.Stop()
			t.debounceTimer = nil
		}
		return
	}

	// Detect whether any layer is being upgraded (false → true).
	hasUpgrade := false
	for i := 0; i < 3; i++ {
		if q[i] && !t.lastQualities[i] {
			hasUpgrade = true
			break
		}
	}

	if hasUpgrade {
		// Upgrade: cancel pending debounce and send immediately.
		if t.debounceTimer != nil {
			t.debounceTimer.Stop()
			t.debounceTimer = nil
		}
		t.lastQualities = q
		t.sendQualityNotification(q)
	} else {
		// Downgrade only: debounce — schedule a delayed send.
		t.pendingQualities = q
		if t.debounceTimer != nil {
			t.debounceTimer.Stop()
		}
		t.debounceTimer = time.AfterFunc(dynacastDowngradeDelay, func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			pending := t.pendingQualities
			t.debounceTimer = nil
			if pending == t.lastQualities {
				return
			}
			t.lastQualities = pending
			t.sendQualityNotification(pending)
		})
	}
}

func (t *PublishedTrack) sendQualityNotification(q [3]bool) {
	primaryQualities := []proto.SubscribedQuality{
		{Quality: proto.VideoQuality_LOW, Enabled: q[0]},
		{Quality: proto.VideoQuality_MEDIUM, Enabled: q[1]},
		{Quality: proto.VideoQuality_HIGH, Enabled: q[2]},
	}

	notification := &proto.SubscribedQualityChangedNotification{
		TrackSid:            t.sid,
		SubscribedQualities: primaryQualities,
	}

	// When backup codec is active, include per-codec quality information
	if t.backupCodec != "" && t.backupProducer != nil {
		hasBackupConsumer := false
		if t.backupConsumers != nil {
			for _, codec := range t.backupConsumers {
				if codec == "backup" {
					hasBackupConsumer = true
					break
				}
			}
		}

		codecs := []proto.SubscribedCodec{}
		if t.videoCodec != "" {
			codecs = append(codecs, proto.SubscribedCodec{
				Codec:     t.videoCodec,
				Qualities: primaryQualities,
			})
		}
		if t.backupCodec != "" {
			backupEnabled := hasBackupConsumer || len(t.pendingBackupSubs) > 0
			codecs = append(codecs, proto.SubscribedCodec{
				Codec: t.backupCodec,
				Qualities: []proto.SubscribedQuality{
					{Quality: proto.VideoQuality_LOW, Enabled: backupEnabled},
					{Quality: proto.VideoQuality_MEDIUM, Enabled: backupEnabled},
					{Quality: proto.VideoQuality_HIGH, Enabled: backupEnabled},
				},
			})
		}
		notification.SubscribedCodecs = codecs
	}

	_ = t.publisher.SendNotification(proto.MethodTrackSubscribedQualityChanged, notification)

	// Sync local consumer state to the shared store so that the publisher's
	// node (if different) can include it in its dynacast calculation.
	t.syncLocalSubToStore()
}

// syncLocalSubToStore writes the per-layer local consumer state for this
// track to the shared store. Must be called with mu held.
func (t *PublishedTrack) syncLocalSubToStore() {
	if t.publisher == nil || t.publisher.room == nil || t.publisher.room.Store() == nil {
		return
	}

	var layers [3]bool
	for _, st := range t.activeConsumers {
		if st.paused {
			continue
		}
		if st.spatialLayer < 0 {
			layers = [3]bool{true, true, true}
			break
		}
		if st.spatialLayer >= 0 && st.spatialLayer <= 2 {
			layers[st.spatialLayer] = true
		}
	}

	// Track whether any consumer uses the backup codec
	hasBackupSub := false
	if t.backupConsumers != nil {
		for _, codec := range t.backupConsumers {
			if codec == "backup" {
				hasBackupSub = true
				break
			}
		}
	}

	rs := t.publisher.room.Store()
	if layers == ([3]bool{}) && !hasBackupSub {
		go rs.DeleteTrackSub(t.sid)
	} else {
		go rs.PutTrackSub(&TrackSubInfo{
			TrackSid:     t.sid,
			NodeId:       rs.nodeID,
			Layers:       layers,
			HasBackupSub: hasBackupSub,
			BackupLayers: [3]bool{hasBackupSub, hasBackupSub, hasBackupSub},
		})
	}
}

// requestBackupCodec sends a SubscribedQualityChangedNotification with
// subscribedCodecs to ask the publisher to start sending the backup codec.
func (t *PublishedTrack) requestBackupCodec() {
	if t.publisher == nil || t.backupCodec == "" {
		return
	}

	notification := &proto.SubscribedQualityChangedNotification{
		TrackSid:            t.sid,
		SubscribedQualities: t.currentSubscribedQualities(),
		SubscribedCodecs: []proto.SubscribedCodec{
			{
				Codec: t.videoCodec,
				Qualities: []proto.SubscribedQuality{
					{Quality: proto.VideoQuality_LOW, Enabled: true},
					{Quality: proto.VideoQuality_MEDIUM, Enabled: true},
					{Quality: proto.VideoQuality_HIGH, Enabled: true},
				},
			},
			{
				Codec: t.backupCodec,
				Qualities: []proto.SubscribedQuality{
					{Quality: proto.VideoQuality_LOW, Enabled: true},
					{Quality: proto.VideoQuality_MEDIUM, Enabled: true},
					{Quality: proto.VideoQuality_HIGH, Enabled: true},
				},
			},
		},
	}

	t.publisher.logger.Info("Requesting backup codec from publisher",
		"trackSid", t.sid, "backupCodec", t.backupCodec)
	_ = t.publisher.SendNotification(proto.MethodTrackSubscribedQualityChanged, notification)
}

// currentSubscribedQualities returns the current quality state for backward compat.
func (t *PublishedTrack) currentSubscribedQualities() []proto.SubscribedQuality {
	t.mu.Lock()
	q := t.lastQualities
	t.mu.Unlock()
	return []proto.SubscribedQuality{
		{Quality: proto.VideoQuality_LOW, Enabled: q[0]},
		{Quality: proto.VideoQuality_MEDIUM, Enabled: q[1]},
		{Quality: proto.VideoQuality_HIGH, Enabled: q[2]},
	}
}

// enableRegression returns true if the track's backup codec policy indicates
// that codec regression should be performed.
func (t *PublishedTrack) enableRegression() bool {
	if t.publisher == nil || t.publisher.room == nil {
		return false
	}
	switch t.backupCodecPolicy {
	case proto.BackupCodecPolicy_REGRESSION:
		return true
	case proto.BackupCodecPolicy_PREFER_REGRESSION:
		return t.publisher.room.ShouldRegressCodec()
	default:
		return false
	}
}

// onBackupProducerReady is called when the backup producer has been created.
// It processes pending subscribers and applies the regression policy.
func (t *PublishedTrack) onBackupProducerReady() {
	t.mu.Lock()
	pending := t.pendingBackupSubs
	t.pendingBackupSubs = nil
	shouldRegress := t.enableRegression()
	t.mu.Unlock()

	// Subscribe all pending participants from the backup producer
	for _, ps := range pending {
		go func(ps *pendingBackupSub) {
			ps.subscriber.mu.RLock()
			transport := ps.subscriber.recvTransport
			caps := ps.subscriber.rtpCapabilities
			ps.subscriber.mu.RUnlock()

			if transport == nil || caps == nil {
				return
			}
			if err := ps.subscriber.consumeFromBackup(t, ps.publisher, transport, caps); err != nil {
				t.publisher.logger.Warn("Failed to subscribe pending backup subscriber",
					"error", err, "subscriber", ps.subscriber.identity)
			}
		}(ps)
	}

	if shouldRegress {
		t.performRegression()
	} else {
		t.sendCodecQualityNotification(true, true)
	}
}

// performRegression switches ALL existing primary-codec consumers to the
// backup producer. After regression, the publisher is told to stop primary.
func (t *PublishedTrack) performRegression() {
	t.mu.Lock()
	if t.regressed {
		t.mu.Unlock()
		return
	}
	t.regressed = true

	// Collect all subscriptions using the primary codec
	var primarySubs []*Participant
	for consumerID, codec := range t.backupConsumers {
		if codec == "primary" {
			// Find the participant that owns this consumer
			if sub := t.findSubscriberByConsumer(consumerID); sub != nil {
				primarySubs = append(primarySubs, sub)
			}
		}
	}
	t.mu.Unlock()

	t.publisher.logger.Info("Performing codec regression",
		"trackSid", t.sid, "from", t.videoCodec, "to", t.backupCodec,
		"affectedSubscribers", len(primarySubs))

	// Re-subscribe each primary consumer from backup
	for _, sub := range primarySubs {
		go func(sub *Participant) {
			// Close the existing primary subscription
			sub.mu.RLock()
			for _, s := range sub.subscriptions {
				if s.trackSid == t.sid {
					s.consumer.Close()
					break
				}
			}
			transport := sub.recvTransport
			caps := sub.rtpCapabilities
			sub.mu.RUnlock()

			if transport == nil || caps == nil {
				return
			}
			if err := sub.consumeFromBackup(t, t.publisher, transport, caps); err != nil {
				t.publisher.logger.Warn("Regression re-subscribe failed",
					"error", err, "subscriber", sub.identity)
			}
		}(sub)
	}

	// Notify publisher: stop primary, keep backup
	t.sendCodecQualityNotification(false, true)
}

// findSubscriberByConsumer finds the participant that owns a given consumer.
// Must be called with relevant locks held or in a context where the data is stable.
func (t *PublishedTrack) findSubscriberByConsumer(consumerID string) *Participant {
	if t.publisher == nil || t.publisher.room == nil {
		return nil
	}
	t.publisher.room.mu.RLock()
	defer t.publisher.room.mu.RUnlock()
	for _, p := range t.publisher.room.participants {
		p.mu.RLock()
		for _, s := range p.subscriptions {
			if s.consumer.Id() == consumerID {
				p.mu.RUnlock()
				return p
			}
		}
		p.mu.RUnlock()
	}
	return nil
}

// sendCodecQualityNotification tells the publisher which codecs should be active.
func (t *PublishedTrack) sendCodecQualityNotification(primaryEnabled, backupEnabled bool) {
	if t.publisher == nil {
		return
	}

	buildQualities := func(enabled bool) []proto.SubscribedQuality {
		return []proto.SubscribedQuality{
			{Quality: proto.VideoQuality_LOW, Enabled: enabled},
			{Quality: proto.VideoQuality_MEDIUM, Enabled: enabled},
			{Quality: proto.VideoQuality_HIGH, Enabled: enabled},
		}
	}

	codecs := []proto.SubscribedCodec{}
	if t.videoCodec != "" {
		codecs = append(codecs, proto.SubscribedCodec{
			Codec:     t.videoCodec,
			Qualities: buildQualities(primaryEnabled),
		})
	}
	if t.backupCodec != "" {
		codecs = append(codecs, proto.SubscribedCodec{
			Codec:     t.backupCodec,
			Qualities: buildQualities(backupEnabled),
		})
	}

	_ = t.publisher.SendNotification(proto.MethodTrackSubscribedQualityChanged,
		&proto.SubscribedQualityChangedNotification{
			TrackSid:            t.sid,
			SubscribedQualities: t.currentSubscribedQualities(),
			SubscribedCodecs:    codecs,
		})
}

// checkCodecRegressionReversal checks if all remaining consumers can use
// the primary codec, and if so, reverts the regression.
func (t *PublishedTrack) checkCodecRegressionReversal() {
	if t.backupCodec == "" || t.backupConsumers == nil {
		return
	}

	t.mu.Lock()
	hasBackupConsumer := false
	for _, codec := range t.backupConsumers {
		if codec == "backup" {
			hasBackupConsumer = true
			break
		}
	}
	// Also consider remote nodes that need backup codec
	if !hasBackupConsumer {
		for _, rs := range t.remoteSubscriptions {
			if rs.HasBackupSub {
				hasBackupConsumer = true
				break
			}
		}
	}
	wasRegressed := t.regressed
	t.mu.Unlock()

	if !hasBackupConsumer && wasRegressed {
		t.mu.Lock()
		t.regressed = false
		t.mu.Unlock()
		t.publisher.logger.Info("No backup consumers remain, reverting regression",
			"trackSid", t.sid)
		t.sendCodecQualityNotification(true, false)
	} else if !hasBackupConsumer && t.backupProducer != nil {
		t.sendCodecQualityNotification(true, false)
	}
}

// DataPublishedTrack holds a published data track.
type DataPublishedTrack struct {
	sid          string
	name         string
	dataProducer *mediasoup.DataProducer
	appData      map[string]any
}

func (t *DataPublishedTrack) ToProto() *proto.DataTrackInfo {
	return &proto.DataTrackInfo{
		Sid:     t.sid,
		Name:    t.name,
		AppData: t.appData,
	}
}

// Subscription holds a media subscription and its signaling metadata.
type Subscription struct {
	sid          string
	trackSid     string
	publisherSid string
	consumer     *mediasoup.Consumer
}

// DataSubscription holds a data subscription for data tracks.
type DataSubscription struct {
	sid          string
	dataTrackSid string
	publisherSid string
	dataConsumer *mediasoup.DataConsumer
}

// Participant represents a client connected to a room.
type Participant struct {
	sid      string
	identity string
	name     string
	state    proto.ParticipantInfo_State
	room     *Room

	metadata   string
	attributes map[string]string
	joinedAt   time.Time
	version    uint32

	grants     *proto.ClaimGrants
	permission *proto.ParticipantPermission

	router          *mediasoup.Router
	sendTransport   *mediasoup.Transport
	recvTransport   *mediasoup.Transport
	directTransport *mediasoup.Transport

	// Signal datachannel
	signalOutProducer *mediasoup.DataProducer
	signalOutConsumer *mediasoup.DataConsumer
	signalInProducer  *mediasoup.DataProducer
	signalInConsumer  *mediasoup.DataConsumer

	rtpCapabilities *mediasoup.RtpCapabilities

	publishedTracks     map[string]*PublishedTrack
	dataPublishedTracks map[string]*DataPublishedTrack
	subscriptions       map[string]*Subscription
	dataSubscriptions   map[string]*DataSubscription

	// RPC request/response correlation
	pendingRPC map[string]chan *proto.Message
	msgCh      chan *proto.Message // bounded inbound message queue
	done       chan struct{}       // closed on Close() to stop messageLoop

	// Heartbeat
	lastPing     time.Time
	pingTimer    *time.Timer
	pingTimeout  time.Duration
	pingInterval time.Duration

	// Stats
	statsInterval time.Duration
	statsLogger   *slog.Logger
	statsFile     *os.File

	autoSubscribe                     bool
	reconnecting                      bool
	allParticipantsAllowedToSubscribe bool
	trackPermissions                  []proto.TrackPermission

	// Group (breakout room) membership
	groupId        string                       // empty = main room
	roomPermission *proto.ParticipantPermission // saved room-level permission for restore

	// Node identification for cross-node sync
	nodeId string

	// Token refresh
	refreshToken    string
	tokenVerifier   *auth.TokenVerifier
	refreshTokenTTL time.Duration
	refreshTimer    *time.Timer

	mu     sync.RWMutex
	logger *slog.Logger
	closed bool
}

type ParticipantParams struct {
	Sid        string
	Identity   string
	Name       string
	Metadata   string
	Attributes map[string]string
	Grants     *proto.ClaimGrants
	Router     *mediasoup.Router

	AutoSubscribe   bool
	PingInterval    time.Duration
	PingTimeout     time.Duration
	StatsInterval   time.Duration
	TokenVerifier   *auth.TokenVerifier
	RefreshTokenTTL time.Duration
	NodeId          string
}

const msgQueueSize = 256

func NewParticipant(params ParticipantParams) *Participant {
	p := &Participant{
		sid:                               params.Sid,
		identity:                          params.Identity,
		name:                              params.Name,
		metadata:                          params.Metadata,
		attributes:                        params.Attributes,
		grants:                            params.Grants,
		router:                            params.Router,
		state:                             proto.ParticipantInfo_JOINING,
		joinedAt:                          time.Now(),
		publishedTracks:                   make(map[string]*PublishedTrack),
		dataPublishedTracks:               make(map[string]*DataPublishedTrack),
		subscriptions:                     make(map[string]*Subscription),
		dataSubscriptions:                 make(map[string]*DataSubscription),
		pendingRPC:                        make(map[string]chan *proto.Message),
		msgCh:                             make(chan *proto.Message, msgQueueSize),
		done:                              make(chan struct{}),
		autoSubscribe:                     params.AutoSubscribe,
		allParticipantsAllowedToSubscribe: true,
		pingInterval:                      params.PingInterval,
		pingTimeout:                       params.PingTimeout,
		statsInterval:                     params.StatsInterval,
		nodeId:                            params.NodeId,
		tokenVerifier:                     params.TokenVerifier,
		refreshTokenTTL:                   params.RefreshTokenTTL,
		logger: slog.Default().With("component", "Participant",
			"identity", params.Identity, "sid", params.Sid),
	}
	p.permission = p.permissionFromGrants()
	go p.messageLoop()
	return p
}

func (p *Participant) Sid() string      { return p.sid }
func (p *Participant) Identity() string { return p.identity }
func (p *Participant) Name() string     { return p.name }
func (p *Participant) Room() *Room      { return p.room }

func (p *Participant) IsPublisher() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.publishedTracks) > 0 || len(p.dataPublishedTracks) > 0
}

func (p *Participant) IsReady() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state == proto.ParticipantInfo_JOINED || p.state == proto.ParticipantInfo_CONNECTED
}

func (p *Participant) CanSubscribe() bool {
	return p.permission != nil && p.permission.CanSubscribe
}

func (p *Participant) CanPublish() bool {
	return p.permission != nil && p.permission.CanPublish
}

// SetRefreshToken stores the refresh token and starts the background refresh scheduler.
func (p *Participant) SetRefreshToken(token string) {
	p.mu.Lock()
	p.refreshToken = token
	p.mu.Unlock()
	p.scheduleTokenRefresh()
}

func (p *Participant) RefreshToken() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.refreshToken
}

// scheduleTokenRefresh sets a timer to push a new refresh token to the client
// before the current one expires. Fires at 90% of the TTL.
func (p *Participant) scheduleTokenRefresh() {
	if p.tokenVerifier == nil || p.refreshTokenTTL == 0 {
		return
	}
	p.mu.Lock()
	if p.refreshTimer != nil {
		p.refreshTimer.Stop()
	}
	delay := time.Duration(float64(p.refreshTokenTTL) * 0.9)
	p.refreshTimer = time.AfterFunc(delay, func() {
		p.mu.RLock()
		oldToken := p.refreshToken
		closed := p.closed
		p.mu.RUnlock()
		if closed || oldToken == "" {
			return
		}
		newToken, err := p.tokenVerifier.RefreshToken(oldToken, p.refreshTokenTTL)
		if err != nil {
			p.logger.Warn("Failed to refresh token", "error", err)
			return
		}
		p.mu.Lock()
		p.refreshToken = newToken
		p.mu.Unlock()
		_ = p.SendNotification(proto.MethodTokenRefreshed, &proto.TokenRefreshedNotification{
			Token: newToken,
		})
		p.logger.Info("Refresh token pushed to client")
		p.scheduleTokenRefresh()
	})
	p.mu.Unlock()
}

func (p *Participant) GetGroupId() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.groupId
}

func (p *Participant) SetGroupId(groupId string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.groupId = groupId
}

// SetEffectivePermission saves the current room-level permission (if not
// already saved) and replaces it with the given group-level permission.
func (p *Participant) SetEffectivePermission(perm *proto.ParticipantPermission) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.roomPermission == nil {
		p.roomPermission = p.permission
	}
	p.permission = perm
}

// RestoreRoomPermission reverts the effective permission to the room-level
// permission that was saved when the participant joined a group.
func (p *Participant) RestoreRoomPermission() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.roomPermission != nil {
		p.permission = p.roomPermission
		p.roomPermission = nil
	}
}

func (p *Participant) HasSubscription(trackSid string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.subscriptions[trackSid]
	return ok
}

func (p *Participant) State() proto.ParticipantInfo_State {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state
}

func (p *Participant) SetState(state proto.ParticipantInfo_State) {
	p.mu.Lock()
	p.state = state
	p.mu.Unlock()
}

func (p *Participant) SendTransportSid() string {
	if p.sendTransport != nil {
		return p.sendTransport.Id()
	}
	return ""
}

func (p *Participant) RecvTransportSid() string {
	if p.recvTransport != nil {
		return p.recvTransport.Id()
	}
	return ""
}

func (p *Participant) GetTransportBySid(sid string) *mediasoup.Transport {
	p.mu.RLock()
	defer p.mu.RUnlock()
	switch sid {
	case "":
		return nil
	default:
		if p.sendTransport != nil && p.sendTransport.Id() == sid {
			return p.sendTransport
		}
		if p.recvTransport != nil && p.recvTransport.Id() == sid {
			return p.recvTransport
		}
		if p.directTransport != nil && p.directTransport.Id() == sid {
			return p.directTransport
		}
		return nil
	}
}

func (p *Participant) GetDirectTransport() *mediasoup.Transport {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.directTransport
}

func (p *Participant) SetTransports(send, recv, direct *mediasoup.Transport) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sendTransport = send
	p.recvTransport = recv
	p.directTransport = direct

	iceHandler := func(state mediasoup.IceState) {
		p.handleIceStateChange(state)
	}
	if send != nil {
		send.OnIceStateChange(iceHandler)
	}
	if recv != nil {
		recv.OnIceStateChange(iceHandler)
	}
}

// SetSignal sets all four signal datachannel resources at once.
// Both directions are pre-created during Join; the incoming message handler
// is registered on inConsumer so it fires once the client transport is connected.
func (p *Participant) SetSignal(
	outProducer *mediasoup.DataProducer,
	outConsumer *mediasoup.DataConsumer,
	inProducer *mediasoup.DataProducer,
	inConsumer *mediasoup.DataConsumer,
) {
	p.mu.Lock()
	p.signalOutProducer = outProducer
	p.signalOutConsumer = outConsumer
	p.signalInProducer = inProducer
	p.signalInConsumer = inConsumer
	p.mu.Unlock()

	if inConsumer != nil {
		inConsumer.OnMessage(func(payload []byte, ppid mediasoup.SctpPayloadType) {
			p.handleIncomingMessage(payload)
		})
	}
}

func (p *Participant) handleIceStateChange(state mediasoup.IceState) {
	p.mu.RLock()
	reconnecting := p.reconnecting
	p.mu.RUnlock()

	if reconnecting {
		return
	}

	p.logger.Info("ICE state changed", "state", state)

	switch state {
	case mediasoup.IceStateConnected, mediasoup.IceStateCompleted:
		p.mu.Lock()
		wasSuspended := p.state == proto.ParticipantInfo_SUSPENDED
		if p.state == proto.ParticipantInfo_JOINING || p.state == proto.ParticipantInfo_SUSPENDED {
			p.state = proto.ParticipantInfo_CONNECTED
		}
		p.mu.Unlock()

		if wasSuspended && p.room != nil {
			p.room.BroadcastNotification(proto.MethodParticipantUpdated,
				&proto.ParticipantUpdatedNotification{Participant: p.ToProto()}, "")
		}

	case mediasoup.IceStateDisconnected:
		p.mu.Lock()
		p.state = proto.ParticipantInfo_SUSPENDED
		p.mu.Unlock()

		p.logger.Warn("ICE disconnected, suspending participant")
		if p.room != nil {
			p.room.BroadcastNotification(proto.MethodParticipantUpdated,
				&proto.ParticipantUpdatedNotification{Participant: p.ToProto()}, p.sid)
		}

	case mediasoup.IceStateClosed:
		p.logger.Warn("ICE closed, closing participant")
		p.Close(proto.DisconnectReason_SIGNAL_CLOSE)
	}
}

func (p *Participant) SetRtpCapabilities(caps *mediasoup.RtpCapabilities) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rtpCapabilities = caps
}

// ============================================================
// Track Operations
// ============================================================

func (p *Participant) PublishTrack(req *proto.PublishTrackRequest) (*PublishedTrack, error) {
	if !p.CanPublish() {
		return nil, fmt.Errorf("no publish permission")
	}

	p.mu.RLock()
	transport := p.sendTransport
	p.mu.RUnlock()

	if transport == nil {
		return nil, fmt.Errorf("send transport not available")
	}

	producer, err := transport.Produce(&mediasoup.ProducerOptions{
		Kind:          req.Kind,
		RtpParameters: req.RtpParameters,
		Paused:        req.Muted,
		AppData:       req.AppData,
	})
	if err != nil {
		return nil, fmt.Errorf("produce failed: %w", err)
	}

	track := &PublishedTrack{
		sid:               string(types.NewStreamID(p.router.Id(), producer.Id())),
		name:              req.Name,
		source:            req.Source,
		kind:              req.Kind,
		producer:          producer,
		muted:             req.Muted,
		width:             req.Width,
		height:            req.Height,
		appData:           req.AppData,
		videoCodec:        req.VideoCodec,
		backupCodec:       req.BackupCodec,
		backupCodecPolicy: req.BackupCodecPolicy,
		publisher:         p,
	}

	if req.BackupCodec != "" {
		track.backupConsumers = make(map[string]string)
	}

	if p.room != nil && p.room.Dynacast() && req.Kind == mediasoup.MediaKindVideo {
		track.activeConsumers = make(map[string]*consumerLayerState)
		track.lastQualities = [3]bool{true, true, true}
	}

	p.mu.Lock()
	p.publishedTracks[track.sid] = track
	p.mu.Unlock()

	producer.OnClose(func(ctx context.Context) {
		track.stopDebounce()
		p.mu.Lock()
		delete(p.publishedTracks, track.sid)
		p.mu.Unlock()
	})

	p.logger.Info("Track published", "trackSid", track.sid, "kind", req.Kind, "name", req.Name)

	if p.room != nil {
		p.room.OnTrackPublished(p, track)
	}

	return track, nil
}

// PublishBackupTrack creates a secondary producer for the backup codec
// on an already-published video track and triggers pending subscriptions.
func (p *Participant) PublishBackupTrack(req *proto.PublishBackupTrackRequest) (string, error) {
	if !p.CanPublish() {
		return "", fmt.Errorf("no publish permission")
	}

	p.mu.RLock()
	track, ok := p.publishedTracks[req.TrackSid]
	transport := p.sendTransport
	p.mu.RUnlock()

	if !ok || track == nil {
		return "", fmt.Errorf("track not found: %s", req.TrackSid)
	}
	if transport == nil {
		return "", fmt.Errorf("send transport not available")
	}
	if track.backupCodec == "" {
		return "", fmt.Errorf("track has no backup codec configured")
	}

	track.mu.Lock()
	if track.backupProducer != nil {
		sid := track.backupSid
		track.mu.Unlock()
		return sid, nil
	}
	track.mu.Unlock()

	producer, err := transport.Produce(&mediasoup.ProducerOptions{
		Kind:          mediasoup.MediaKindVideo,
		RtpParameters: req.RtpParameters,
		Paused:        false,
	})
	if err != nil {
		return "", fmt.Errorf("backup produce failed: %w", err)
	}

	backupSid := string(types.NewStreamID(p.router.Id(), producer.Id()))

	track.mu.Lock()
	track.backupProducer = producer
	track.backupSid = backupSid
	track.mu.Unlock()

	producer.OnClose(func(ctx context.Context) {
		track.mu.Lock()
		track.backupProducer = nil
		track.backupSid = ""
		track.mu.Unlock()
	})

	p.logger.Info("Backup track published",
		"trackSid", req.TrackSid, "backupSid", backupSid, "codec", track.backupCodec)

	// Advertise backup stream for cross-node piping
	if p.room != nil && p.room.cascade.OnTrackPublished != nil {
		p.room.cascade.OnTrackPublished(backupSid)
	}

	// Process pending subscribers and apply regression policy
	track.onBackupProducerReady()

	return backupSid, nil
}

func (p *Participant) UnpublishTrack(trackSid string) error {
	p.mu.Lock()
	track, ok := p.publishedTracks[trackSid]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("track not found: %s", trackSid)
	}
	delete(p.publishedTracks, trackSid)
	p.mu.Unlock()

	track.producer.Close()

	if p.room != nil {
		p.room.OnTrackUnpublished(p, trackSid)
	}

	p.logger.Info("Track unpublished", "trackSid", trackSid)
	return nil
}

func (p *Participant) MuteTrack(trackSid string, muted bool) error {
	p.mu.RLock()
	track, ok := p.publishedTracks[trackSid]
	p.mu.RUnlock()

	if !ok {
		return fmt.Errorf("track not found: %s", trackSid)
	}

	var err error
	if muted {
		err = track.producer.Pause()
	} else {
		err = track.producer.Resume()
	}
	if err != nil {
		return err
	}

	track.muted = muted

	if p.room != nil {
		p.room.OnTrackMuted(p, trackSid, muted)
	}

	p.logger.Info("Track mute changed", "trackSid", trackSid, "muted", muted)
	return nil
}

func (p *Participant) UpdateTrack(req *proto.UpdateTrackRequest) error {
	p.mu.RLock()
	track, ok := p.publishedTracks[req.TrackSid]
	p.mu.RUnlock()

	if !ok {
		return fmt.Errorf("track not found: %s", req.TrackSid)
	}

	if req.Width > 0 {
		track.width = req.Width
	}
	if req.Height > 0 {
		track.height = req.Height
	}

	return nil
}

func (p *Participant) GetPublishedTracks() []*PublishedTrack {
	p.mu.RLock()
	defer p.mu.RUnlock()
	tracks := make([]*PublishedTrack, 0, len(p.publishedTracks))
	for _, t := range p.publishedTracks {
		tracks = append(tracks, t)
	}
	return tracks
}

// ============================================================
// Data Track Operations
// ============================================================

func (p *Participant) PublishDataTrack(req *proto.PublishDataTrackRequest) (*DataPublishedTrack, error) {
	if !p.CanPublish() {
		return nil, fmt.Errorf("no publish permission")
	}

	p.mu.RLock()
	transport := p.sendTransport
	p.mu.RUnlock()

	if transport == nil {
		return nil, fmt.Errorf("send transport not available")
	}

	dataProducer, err := transport.ProduceData(&mediasoup.DataProducerOptions{
		SctpStreamParameters: req.SctpStreamParameters,
		Label:                req.Label,
		Protocol:             req.Protocol,
		AppData:              req.AppData,
	})
	if err != nil {
		return nil, fmt.Errorf("produce data failed: %w", err)
	}

	track := &DataPublishedTrack{
		sid:          string(types.NewStreamID(p.router.Id(), dataProducer.Id())),
		name:         req.Name,
		dataProducer: dataProducer,
		appData:      req.AppData,
	}

	p.mu.Lock()
	p.dataPublishedTracks[track.sid] = track
	p.mu.Unlock()

	dataProducer.OnClose(func(ctx context.Context) {
		p.mu.Lock()
		delete(p.dataPublishedTracks, track.sid)
		p.mu.Unlock()
	})

	p.logger.Info("Data track published", "dataTrackSid", track.sid, "name", req.Name)

	if p.room != nil {
		p.room.OnDataTrackPublished(p, track)
	}

	return track, nil
}

func (p *Participant) UnpublishDataTrack(dataTrackSid string) error {
	p.mu.Lock()
	track, ok := p.dataPublishedTracks[dataTrackSid]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("data track not found: %s", dataTrackSid)
	}
	delete(p.dataPublishedTracks, dataTrackSid)
	p.mu.Unlock()

	track.dataProducer.Close()

	if p.room != nil {
		p.room.OnDataTrackUnpublished(p, dataTrackSid)
	}

	p.logger.Info("Data track unpublished", "dataTrackSid", dataTrackSid)
	return nil
}

func (p *Participant) GetDataPublishedTracks() []*DataPublishedTrack {
	p.mu.RLock()
	defer p.mu.RUnlock()
	tracks := make([]*DataPublishedTrack, 0, len(p.dataPublishedTracks))
	for _, t := range p.dataPublishedTracks {
		tracks = append(tracks, t)
	}
	return tracks
}

// ============================================================
// Subscription Operations
// ============================================================

// Subscribe creates a subscription for the given track on this participant's
// receive transport. The subscription is created paused, the client is
// notified via RPC, and only after acknowledgment is media resumed.
//
// When the primary codec is unsupported by the subscriber and a backup
// codec is configured, the method falls back to the backup producer or
// queues the subscriber until the backup producer becomes available.
func (p *Participant) Subscribe(track *PublishedTrack, publisher *Participant) error {
	p.mu.RLock()
	transport := p.recvTransport
	caps := p.rtpCapabilities
	p.mu.RUnlock()

	if transport == nil || caps == nil {
		return fmt.Errorf("participant not ready for subscription")
	}

	if p.room != nil && p.room.cascade.EnsureStream != nil {
		if err := p.room.cascade.EnsureStream(track.sid); err != nil {
			return fmt.Errorf("ensure stream failed: %w", err)
		}
	}

	producerId := track.producer.Id()

	// Cross-router pipe within the same node
	if publisher.router.Id() != p.router.Id() {
		res, err := publisher.router.PipeToRouter(&mediasoup.PipeToRouterOptions{
			ProducerId: producerId,
			Router:     p.router,
			ListenInfo: mediasoup.TransportListenInfo{
				Ip:       "127.0.0.1",
				Protocol: mediasoup.TransportProtocolUDP,
			},
		})
		if err != nil {
			return fmt.Errorf("cross-router pipe failed: %w", err)
		}
		producerId = res.PipeProducer.Id()
	}

	consumer, err := transport.Consume(&mediasoup.ConsumerOptions{
		ProducerId:      producerId,
		RtpCapabilities: caps,
		Paused:          true,
	})
	if err != nil {
		// Primary consume failed — try backup codec fallback
		if track.backupCodec != "" {
			return p.subscribeWithBackupFallback(track, publisher, transport, caps)
		}
		return fmt.Errorf("consume failed: %w", err)
	}

	if track.backupConsumers != nil {
		track.mu.Lock()
		track.backupConsumers[consumer.Id()] = "primary"
		track.mu.Unlock()
	}

	return p.finishSubscribe(track, publisher, consumer)
}

// subscribeWithBackupFallback handles the case where the primary codec is
// unsupported by the subscriber.
func (p *Participant) subscribeWithBackupFallback(
	track *PublishedTrack,
	publisher *Participant,
	transport *mediasoup.Transport,
	caps *mediasoup.RtpCapabilities,
) error {
	track.mu.Lock()

	// If backup producer already exists, consume from it directly
	if track.backupProducer != nil {
		track.mu.Unlock()
		return p.consumeFromBackup(track, publisher, transport, caps)
	}

	// Queue subscriber and request backup from publisher if not yet requested
	track.pendingBackupSubs = append(track.pendingBackupSubs, &pendingBackupSub{
		subscriber: p,
		publisher:  publisher,
	})

	needRequest := !track.backupRequested
	track.backupRequested = true
	track.mu.Unlock()

	if needRequest {
		track.requestBackupCodec()
	}

	p.logger.Info("Subscriber queued for backup codec",
		"trackSid", track.sid, "backupCodec", track.backupCodec)
	return nil
}

// consumeFromBackup creates a consumer from the backup producer.
func (p *Participant) consumeFromBackup(
	track *PublishedTrack,
	publisher *Participant,
	transport *mediasoup.Transport,
	caps *mediasoup.RtpCapabilities,
) error {
	backupProducerId := track.backupProducer.Id()

	if publisher.router.Id() != p.router.Id() {
		res, err := publisher.router.PipeToRouter(&mediasoup.PipeToRouterOptions{
			ProducerId: backupProducerId,
			Router:     p.router,
			ListenInfo: mediasoup.TransportListenInfo{
				Ip:       "127.0.0.1",
				Protocol: mediasoup.TransportProtocolUDP,
			},
		})
		if err != nil {
			return fmt.Errorf("cross-router pipe (backup) failed: %w", err)
		}
		backupProducerId = res.PipeProducer.Id()
	}

	consumer, err := transport.Consume(&mediasoup.ConsumerOptions{
		ProducerId:      backupProducerId,
		RtpCapabilities: caps,
		Paused:          true,
	})
	if err != nil {
		return fmt.Errorf("backup consume failed: %w", err)
	}

	if track.backupConsumers != nil {
		track.mu.Lock()
		track.backupConsumers[consumer.Id()] = "backup"
		track.mu.Unlock()
	}

	return p.finishSubscribe(track, publisher, consumer)
}

// finishSubscribe completes subscription setup: RPC to client, resume, callbacks.
func (p *Participant) finishSubscribe(
	track *PublishedTrack,
	publisher *Participant,
	consumer *mediasoup.Consumer,
) error {
	sub := &Subscription{
		sid:          consumer.Id(),
		trackSid:     track.sid,
		publisherSid: publisher.sid,
		consumer:     consumer,
	}

	req := &proto.CreateSubscriptionRequest{
		SubscriptionSid:  sub.sid,
		TrackSid:         track.sid,
		PublisherSid:     publisher.sid,
		Kind:             consumer.Kind(),
		RtpParameters:    consumer.RtpParameters(),
		SubscriptionType: string(consumer.Type()),
		TrackMuted:       consumer.ProducerPaused(),
	}

	resp, err := p.Request(proto.MethodSubscriptionCreate, req)
	if err != nil {
		consumer.Close()
		return fmt.Errorf("subscription RPC failed: %w", err)
	}

	if resp.Status != proto.StatusOK {
		consumer.Close()
		return fmt.Errorf("client rejected subscription: %s", resp.Error())
	}

	if err := consumer.Resume(); err != nil {
		consumer.Close()
		return fmt.Errorf("consumer resume failed: %w", err)
	}

	p.mu.Lock()
	p.subscriptions[sub.sid] = sub
	p.mu.Unlock()

	track.addConsumer(consumer.Id())

	consumer.OnClose(func(ctx context.Context) {
		p.mu.Lock()
		delete(p.subscriptions, sub.sid)
		closed := p.closed
		p.mu.Unlock()

		track.removeConsumer(consumer.Id())

		if track.backupConsumers != nil {
			track.mu.Lock()
			delete(track.backupConsumers, consumer.Id())
			track.mu.Unlock()
		}

		if p.room != nil && p.room.cascade.ReleaseStream != nil {
			p.room.cascade.ReleaseStream(track.sid)
		}

		if !closed {
			_ = p.SendNotification(proto.MethodSubscriptionClosed, &proto.SubscriptionClosedNotification{
				SubscriptionSid: sub.sid,
			})
		}

		// Check if we can stop the backup codec after a consumer leaves
		track.checkCodecRegressionReversal()
	})

	consumer.OnLayersChange(func(layers *mediasoup.ConsumerLayers) {
		if layers != nil {
			track.updateConsumerLayers(consumer.Id(), int32(layers.SpatialLayer))
		}
	})

	p.logger.Info("Subscription created", "subscriptionSid", sub.sid,
		"trackSid", track.sid, "publisher", publisher.identity)
	return nil
}

// SubscribeData creates a data consumer subscription.
func (p *Participant) SubscribeData(track *DataPublishedTrack, publisher *Participant) error {
	p.mu.RLock()
	transport := p.recvTransport
	p.mu.RUnlock()

	if transport == nil {
		return fmt.Errorf("participant not ready for data subscription")
	}

	if p.room != nil && p.room.cascade.EnsureStream != nil {
		if err := p.room.cascade.EnsureStream(track.sid); err != nil {
			return fmt.Errorf("ensure data stream failed: %w", err)
		}
	}

	dataProducerId := track.dataProducer.Id()

	if publisher.router.Id() != p.router.Id() {
		// For data tracks, pipe to same router
		res, err := publisher.router.PipeToRouter(&mediasoup.PipeToRouterOptions{
			DataProducerId: dataProducerId,
			Router:         p.router,
			ListenInfo: mediasoup.TransportListenInfo{
				Ip:       "127.0.0.1",
				Protocol: mediasoup.TransportProtocolUDP,
			},
		})
		if err != nil {
			return fmt.Errorf("cross-router data pipe failed: %w", err)
		}
		dataProducerId = res.PipeDataProducer.Id()
	}

	dataConsumer, err := transport.ConsumeData(&mediasoup.DataConsumerOptions{
		DataProducerId: dataProducerId,
	})
	if err != nil {
		return fmt.Errorf("consume data failed: %w", err)
	}

	sub := &DataSubscription{
		sid:          dataConsumer.Id(),
		dataTrackSid: track.sid,
		publisherSid: publisher.sid,
		dataConsumer: dataConsumer,
	}

	req := &proto.CreateDataSubscriptionRequest{
		DataSubscriptionSid:  sub.sid,
		DataTrackSid:         track.sid,
		PublisherSid:         publisher.sid,
		SctpStreamParameters: dataConsumer.SctpStreamParameters(),
		Label:                dataConsumer.Label(),
		Protocol:             dataConsumer.Protocol(),
	}

	resp, err := p.Request(proto.MethodDataSubscriptionCreate, req)
	if err != nil {
		dataConsumer.Close()
		return fmt.Errorf("data subscription RPC failed: %w", err)
	}

	if resp.Status != proto.StatusOK {
		dataConsumer.Close()
		return fmt.Errorf("client rejected data subscription: %s", resp.Error())
	}

	p.mu.Lock()
	p.dataSubscriptions[sub.sid] = sub
	p.mu.Unlock()

	dataConsumer.OnClose(func(ctx context.Context) {
		p.mu.Lock()
		delete(p.dataSubscriptions, sub.sid)
		closed := p.closed
		p.mu.Unlock()

		if p.room != nil && p.room.cascade.ReleaseStream != nil {
			p.room.cascade.ReleaseStream(track.sid)
		}

		if !closed {
			_ = p.SendNotification(proto.MethodDataSubscriptionClosed, &proto.DataSubscriptionClosedNotification{
				DataSubscriptionSid: sub.sid,
			})
		}
	})

	p.logger.Info("Data subscription created", "dataSub", sub.sid,
		"dataTrack", track.sid, "publisher", publisher.identity)
	return nil
}

func (p *Participant) PauseSubscription(subSid string) error {
	p.mu.RLock()
	sub, ok := p.subscriptions[subSid]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("subscription not found: %s", subSid)
	}
	if err := sub.consumer.Pause(); err != nil {
		return err
	}
	if p.room != nil {
		if track := p.room.FindPublishedTrack(sub.trackSid); track != nil {
			track.updateConsumerPaused(sub.consumer.Id(), true)
		}
	}
	return nil
}

func (p *Participant) ResumeSubscription(subSid string) error {
	p.mu.RLock()
	sub, ok := p.subscriptions[subSid]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("subscription not found: %s", subSid)
	}
	if err := sub.consumer.Resume(); err != nil {
		return err
	}
	if p.room != nil {
		if track := p.room.FindPublishedTrack(sub.trackSid); track != nil {
			track.updateConsumerPaused(sub.consumer.Id(), false)
		}
	}
	return nil
}

func (p *Participant) SetPreferredLayers(req *proto.SetPreferredLayersRequest) error {
	p.mu.RLock()
	sub, ok := p.subscriptions[req.SubscriptionSid]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("subscription not found: %s", req.SubscriptionSid)
	}
	opts := mediasoup.ConsumerLayers{
		SpatialLayer: uint8(req.SpatialLayer),
	}
	if req.TemporalLayer != nil {
		tl := uint8(*req.TemporalLayer)
		opts.TemporalLayer = &tl
	}
	if err := sub.consumer.SetPreferredLayers(opts); err != nil {
		return err
	}
	if p.room != nil {
		if track := p.room.FindPublishedTrack(sub.trackSid); track != nil {
			track.updateConsumerLayers(sub.consumer.Id(), req.SpatialLayer)
		}
	}
	return nil
}

func (p *Participant) SetPriority(subSid string, priority int32) error {
	p.mu.RLock()
	sub, ok := p.subscriptions[subSid]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("subscription not found: %s", subSid)
	}
	return sub.consumer.SetPriority(byte(priority))
}

func (p *Participant) RequestKeyFrame(subSid string) error {
	p.mu.RLock()
	sub, ok := p.subscriptions[subSid]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("subscription not found: %s", subSid)
	}
	return sub.consumer.RequestKeyFrame()
}

func (p *Participant) CloseSubscription(subSid string) error {
	p.mu.Lock()
	sub, ok := p.subscriptions[subSid]
	if ok {
		delete(p.subscriptions, subSid)
	}
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("subscription not found: %s", subSid)
	}
	sub.consumer.Close()
	p.logger.Info("Subscription closed", "subscriptionSid", subSid)
	return nil
}

func (p *Participant) CloseDataSubscription(dataSub string) error {
	p.mu.Lock()
	sub, ok := p.dataSubscriptions[dataSub]
	if ok {
		delete(p.dataSubscriptions, dataSub)
	}
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("data subscription not found: %s", dataSub)
	}
	sub.dataConsumer.Close()
	p.logger.Info("Data subscription closed", "dataSubscriptionSid", dataSub)
	return nil
}

// ============================================================
// Signal: Send / Receive
// ============================================================

// SendNotification sends a one-way message to the client (no response expected).
func (p *Participant) SendNotification(method string, body any) error {
	msg, err := proto.NewJSONNotification(method, body)
	if err != nil {
		return err
	}
	msg.LogId = xid.New().String()
	return p.sendMessage(msg)
}

// Request sends an RPC request to the client and waits for the response.
// This is used for server-initiated RPCs like subscription.create where
// the server needs the client to acknowledge before proceeding.
func (p *Participant) Request(method string, body any) (*proto.Message, error) {
	logId := xid.New().String()
	msg, err := proto.NewJSONRequest(method, logId, body)
	if err != nil {
		return nil, err
	}

	ch := make(chan *proto.Message, 1)
	p.mu.Lock()
	p.pendingRPC[logId] = ch
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.pendingRPC, logId)
		p.mu.Unlock()
	}()

	if err := p.sendMessage(msg); err != nil {
		return nil, err
	}

	select {
	case resp := <-ch:
		return resp, nil
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("RPC timeout: %s (logId=%s)", method, logId)
	}
}

func (p *Participant) sendMessage(msg *proto.Message) error {
	p.mu.RLock()
	producer := p.signalOutProducer
	suspended := p.state == proto.ParticipantInfo_SUSPENDED
	p.mu.RUnlock()

	if suspended {
		return nil
	}

	if producer == nil {
		return fmt.Errorf("signal channel not established")
	}

	data, err := msg.Encode()
	if err != nil {
		return err
	}

	return producer.Send(data)
}

func (p *Participant) handleIncomingMessage(payload []byte) {
	msg, err := proto.DecodeMessage(payload)
	if err != nil {
		p.logger.Warn("Invalid RPC message", "error", err)
		return
	}

	// Responses bypass the queue — deliver directly to the waiting caller.
	if msg.IsResponse() {
		p.mu.RLock()
		ch, ok := p.pendingRPC[msg.LogId]
		p.mu.RUnlock()
		if ok {
			ch <- msg
		}
		return
	}

	// Requests/notifications are queued for ordered, single-goroutine processing.
	select {
	case p.msgCh <- msg:
	case <-p.done:
	default:
		p.logger.Warn("Message queue full, dropping message", "method", msg.Method)
		if msg.IsRequest() {
			_ = p.sendMessage(proto.NewErrorResponse(msg.LogId, proto.StatusInternalError, "server busy"))
		}
	}
}

// messageLoop drains the inbound message queue in a single goroutine,
// guaranteeing processing order and bounding concurrency.
func (p *Participant) messageLoop() {
	for {
		select {
		case msg := <-p.msgCh:
			p.dispatchRPC(msg)
		case <-p.done:
			return
		}
	}
}

// ============================================================
// Heartbeat
// ============================================================

func (p *Participant) HandlePing(req *proto.PingRequest) {
	p.mu.Lock()
	p.lastPing = time.Now()
	p.mu.Unlock()
	p.resetPingTimer()
}

func (p *Participant) StartHeartbeat() {
	p.mu.Lock()
	p.lastPing = time.Now()
	p.mu.Unlock()
	p.resetPingTimer()
}

func (p *Participant) resetPingTimer() {
	p.mu.Lock()
	if p.pingTimer != nil {
		p.pingTimer.Stop()
	}
	if p.pingTimeout > 0 {
		p.pingTimer = time.AfterFunc(p.pingTimeout, func() {
			p.logger.Warn("Ping timeout, disconnecting participant")
			p.Close(proto.DisconnectReason_CONNECTION_TIMEOUT)
		})
	}
	p.mu.Unlock()
}

// ============================================================
// Stats Logger
// ============================================================

func (p *Participant) setupStatsLogger(roomName, roomSid string) {
	dir := filepath.Join("logs", "stats")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		p.logger.Warn("Failed to create stats log directory", "error", err)
		return
	}

	filename := filepath.Join(dir, fmt.Sprintf("%s_%s.log", roomSid, p.sid))
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		p.logger.Warn("Failed to open stats log file", "error", err)
		return
	}

	p.mu.Lock()
	p.statsFile = f
	p.statsLogger = slog.New(slog.NewJSONHandler(f, nil)).With(
		"room", roomName,
		"roomSid", roomSid,
		"participant", p.identity,
		"participantSid", p.sid,
	)
	p.mu.Unlock()
}

func (p *Participant) closeStatsLogger() {
	p.mu.Lock()
	f := p.statsFile
	p.statsFile = nil
	p.statsLogger = nil
	p.mu.Unlock()

	if f != nil {
		f.Close()
	}
}

// ============================================================
// Participant Info
// ============================================================

func (p *Participant) ToProto() *proto.ParticipantInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()

	tracks := make([]*proto.TrackInfo, 0, len(p.publishedTracks))
	for _, t := range p.publishedTracks {
		tracks = append(tracks, t.ToProto())
	}
	dataTracks := make([]*proto.DataTrackInfo, 0, len(p.dataPublishedTracks))
	for _, t := range p.dataPublishedTracks {
		dataTracks = append(dataTracks, t.ToProto())
	}

	return &proto.ParticipantInfo{
		Sid:         p.sid,
		Identity:    p.identity,
		State:       p.state,
		Tracks:      tracks,
		DataTracks:  dataTracks,
		Metadata:    p.metadata,
		JoinedAt:    p.joinedAt.UnixMilli(),
		Name:        p.name,
		Version:     p.version,
		Permission:  p.permission,
		IsPublisher: len(p.publishedTracks) > 0 || len(p.dataPublishedTracks) > 0,
		Attributes:  p.attributes,
		GroupId:     p.groupId,
		NodeId:      p.nodeId,
	}
}

func (p *Participant) UpdateMetadata(metadata, name *string, attributes map[string]string) {
	p.mu.Lock()
	if metadata != nil {
		p.metadata = *metadata
	}
	if name != nil {
		p.name = *name
	}
	for k, v := range attributes {
		if p.attributes == nil {
			p.attributes = make(map[string]string)
		}
		p.attributes[k] = v
	}
	p.version++
	p.mu.Unlock()

	if p.room != nil {
		p.room.BroadcastNotification(proto.MethodParticipantUpdated,
			&proto.ParticipantUpdatedNotification{Participant: p.ToProto()}, p.sid)
		if p.room.Store() != nil {
			go p.room.Store().PutParticipant(p.ToProto())
		}
	}
}

// ============================================================
// Reconnect
// ============================================================

// RestartIce performs ICE restart on the send and recv transports, returning
// new IceParameters per transport. This is the fast reconnect path used when
// the server still holds the participant session.
func (p *Participant) RestartIce() (map[string]*mediasoup.IceParameters, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("participant is closed")
	}
	send := p.sendTransport
	recv := p.recvTransport
	p.mu.RUnlock()

	result := make(map[string]*mediasoup.IceParameters)

	if send != nil {
		params, err := send.RestartIce()
		if err != nil {
			return nil, fmt.Errorf("send transport ICE restart: %w", err)
		}
		result[send.Id()] = params
	}
	if recv != nil {
		params, err := recv.RestartIce()
		if err != nil {
			return nil, fmt.Errorf("recv transport ICE restart: %w", err)
		}
		result[recv.Id()] = params
	}

	p.logger.Info("ICE restart completed")
	return result, nil
}

// ============================================================
// Close
// ============================================================

func (p *Participant) Close(reason proto.DisconnectReason) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.state = proto.ParticipantInfo_DISCONNECTED

	if p.pingTimer != nil {
		p.pingTimer.Stop()
	}
	if p.refreshTimer != nil {
		p.refreshTimer.Stop()
	}

	for _, ch := range p.pendingRPC {
		close(ch)
	}
	p.pendingRPC = make(map[string]chan *proto.Message)

	// Snapshot published tracks to clean up outside the lock.
	trackSids := make([]string, 0, len(p.publishedTracks))
	for sid := range p.publishedTracks {
		trackSids = append(trackSids, sid)
	}
	dataTrackSids := make([]string, 0, len(p.dataPublishedTracks))
	for sid := range p.dataPublishedTracks {
		dataTrackSids = append(dataTrackSids, sid)
	}
	p.mu.Unlock()

	close(p.done)

	if reason != proto.DisconnectReason_CLIENT_INITIATED {
		_ = p.SendNotification(proto.MethodDisconnect, &proto.DisconnectNotification{
			Reason: reason,
		})
	}

	// Clean up RIB/FIB/MediaManager advertised streams before closing transports.
	if p.room != nil {
		for _, sid := range trackSids {
			p.room.OnTrackUnpublished(p, sid)
		}
		for _, sid := range dataTrackSids {
			p.room.OnDataTrackUnpublished(p, sid)
		}
	}

	if p.signalInConsumer != nil {
		p.signalInConsumer.Close()
	}
	if p.signalOutConsumer != nil {
		p.signalOutConsumer.Close()
	}
	if p.signalInProducer != nil {
		p.signalInProducer.Close()
	}
	if p.signalOutProducer != nil {
		p.signalOutProducer.Close()
	}
	if p.sendTransport != nil {
		p.sendTransport.Close()
	}
	if p.recvTransport != nil {
		p.recvTransport.Close()
	}
	if p.directTransport != nil {
		p.directTransport.Close()
	}

	if p.room != nil {
		p.room.RemoveParticipant(p.sid, reason)
	}

	p.closeStatsLogger()
	p.logger.Info("Participant closed", "reason", reason)
}

// ============================================================
// Helpers
// ============================================================

func (p *Participant) permissionFromGrants() *proto.ParticipantPermission {
	perm := &proto.ParticipantPermission{
		CanSubscribe:      true,
		CanPublish:        true,
		CanPublishData:    true,
		CanUpdateMetadata: false,
	}
	if p.grants == nil {
		return perm
	}
	if m := p.grants.Media; m != nil {
		perm.CanPublish = m.GetCanPublish()
		perm.CanSubscribe = m.GetCanSubscribe()
		perm.CanPublishData = m.GetCanPublishData()
		perm.CanUpdateMetadata = m.GetCanUpdateOwnMetadata()
		perm.CanSubscribeMetrics = m.GetCanSubscribeMetrics()
	}
	if r := p.grants.Room; r != nil {
		perm.Hidden = r.Hidden
	}
	return perm
}
