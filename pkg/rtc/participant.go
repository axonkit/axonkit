package rtc

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/jiyeyuran/omnistream/pkg/proto"
	"github.com/rs/xid"
)

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
}

func (t *PublishedTrack) ToProto() *proto.TrackInfo {
	tt := proto.TrackType_AUDIO
	if t.kind == mediasoup.MediaKindVideo {
		tt = proto.TrackType_VIDEO
	}
	return &proto.TrackInfo{
		Sid:    t.sid,
		Type:   tt,
		Name:   t.name,
		Muted:  t.muted,
		Width:  t.width,
		Height: t.height,
		Source: t.source,
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

	autoSubscribe    bool
	reconnecting     bool
	pendingSyncState *proto.SyncState

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

	AutoSubscribe bool
	PingInterval  time.Duration
	PingTimeout   time.Duration
}

const msgQueueSize = 256

func NewParticipant(params ParticipantParams) *Participant {
	p := &Participant{
		sid:                 params.Sid,
		identity:            params.Identity,
		name:                params.Name,
		metadata:            params.Metadata,
		attributes:          params.Attributes,
		grants:              params.Grants,
		router:              params.Router,
		state:               proto.ParticipantInfo_JOINING,
		joinedAt:            time.Now(),
		publishedTracks:     make(map[string]*PublishedTrack),
		dataPublishedTracks: make(map[string]*DataPublishedTrack),
		subscriptions:       make(map[string]*Subscription),
		dataSubscriptions:   make(map[string]*DataSubscription),
		pendingRPC:          make(map[string]chan *proto.Message),
		msgCh:               make(chan *proto.Message, msgQueueSize),
		done:                make(chan struct{}),
		autoSubscribe:       params.AutoSubscribe,
		pingInterval:        params.PingInterval,
		pingTimeout:         params.PingTimeout,
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
	return p.state == proto.ParticipantInfo_JOINED || p.state == proto.ParticipantInfo_ACTIVE
}

func (p *Participant) CanSubscribe() bool {
	return p.permission != nil && p.permission.CanSubscribe
}

func (p *Participant) CanPublish() bool {
	return p.permission != nil && p.permission.CanPublish
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
		if p.state == proto.ParticipantInfo_JOINING {
			p.state = proto.ParticipantInfo_ACTIVE
		}
		p.mu.Unlock()

	case mediasoup.IceStateDisconnected:
		p.logger.Warn("ICE disconnected")

	case mediasoup.IceStateClosed:
		p.logger.Warn("ICE closed, disconnecting participant")
		p.Close(proto.DisconnectReason_CONNECTION_TIMEOUT)
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
		sid:      producer.Id(),
		name:     req.Name,
		source:   req.Source,
		kind:     req.Kind,
		producer: producer,
		muted:    req.Muted,
		width:    req.Width,
		height:   req.Height,
		appData:  req.AppData,
	}

	p.mu.Lock()
	p.publishedTracks[track.sid] = track
	p.mu.Unlock()

	producer.OnClose(func(ctx context.Context) {
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
		sid:          dataProducer.Id(),
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
func (p *Participant) Subscribe(track *PublishedTrack, publisher *Participant) error {
	p.mu.RLock()
	transport := p.recvTransport
	caps := p.rtpCapabilities
	p.mu.RUnlock()

	if transport == nil || caps == nil {
		return fmt.Errorf("participant not ready for subscription")
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
		return fmt.Errorf("consume failed: %w", err)
	}

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

	consumer.OnClose(func(ctx context.Context) {
		p.mu.Lock()
		delete(p.subscriptions, sub.sid)
		p.mu.Unlock()
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
		p.mu.Unlock()
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
	return sub.consumer.Pause()
}

func (p *Participant) ResumeSubscription(subSid string) error {
	p.mu.RLock()
	sub, ok := p.subscriptions[subSid]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("subscription not found: %s", subSid)
	}
	return sub.consumer.Resume()
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
	return sub.consumer.SetPreferredLayers(opts)
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
	p.mu.RUnlock()

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
			&proto.ParticipantUpdatedNotification{Participant: p.ToProto()}, "")
	}
}

// ============================================================
// Reconnect
// ============================================================

// Reconnect replaces the participant's transports and signal channels without
// destroying the session (published tracks, subscriptions, room membership).
// This is used when a client's ICE/DTLS connection breaks and it needs to
// re-establish connectivity to the same mediasoup worker.
func (p *Participant) Reconnect(send, recv, direct *mediasoup.Transport,
	outProducer *mediasoup.DataProducer,
	outConsumer *mediasoup.DataConsumer,
	inProducer *mediasoup.DataProducer,
	inConsumer *mediasoup.DataConsumer,
	syncState *proto.SyncState,
) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("participant is closed")
	}
	p.reconnecting = true
	p.pendingSyncState = syncState

	// Snapshot old resources to close after releasing the lock.
	oldSend := p.sendTransport
	oldRecv := p.recvTransport
	oldDirect := p.directTransport
	oldSigInConsumer := p.signalInConsumer
	oldSigOutConsumer := p.signalOutConsumer
	oldSigInProducer := p.signalInProducer
	oldSigOutProducer := p.signalOutProducer

	// Abort pending RPCs — callers will get a nil response.
	for _, ch := range p.pendingRPC {
		close(ch)
	}
	p.pendingRPC = make(map[string]chan *proto.Message)

	// Clear dead subscriptions — old consumers die with the old recvTransport.
	p.subscriptions = make(map[string]*Subscription)
	p.dataSubscriptions = make(map[string]*DataSubscription)

	p.state = proto.ParticipantInfo_JOINING
	p.mu.Unlock()

	// Close old resources outside the lock. Order: signal → transports.
	if oldSigInConsumer != nil {
		oldSigInConsumer.Close()
	}
	if oldSigOutConsumer != nil {
		oldSigOutConsumer.Close()
	}
	if oldSigInProducer != nil {
		oldSigInProducer.Close()
	}
	if oldSigOutProducer != nil {
		oldSigOutProducer.Close()
	}
	if oldSend != nil {
		oldSend.Close()
	}
	if oldRecv != nil {
		oldRecv.Close()
	}
	if oldDirect != nil {
		oldDirect.Close()
	}

	// Install new resources.
	p.mu.Lock()
	p.reconnecting = false
	p.mu.Unlock()

	p.SetTransports(send, recv, direct)
	p.SetSignal(outProducer, outConsumer, inProducer, inConsumer)

	p.logger.Info("Participant reconnected")
	return nil
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

	for _, ch := range p.pendingRPC {
		close(ch)
	}
	p.pendingRPC = make(map[string]chan *proto.Message)
	p.mu.Unlock()

	close(p.done)

	_ = p.SendNotification(proto.MethodDisconnect, &proto.DisconnectNotification{
		Reason: reason,
	})

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
		p.room.RemoveParticipant(p.sid)
	}

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
	if p.grants == nil || p.grants.Video == nil {
		return perm
	}
	v := p.grants.Video
	perm.CanPublish = v.GetCanPublish()
	perm.CanSubscribe = v.GetCanSubscribe()
	perm.CanPublishData = v.GetCanPublishData()
	perm.CanUpdateMetadata = v.GetCanUpdateOwnMetadata()
	perm.CanSubscribeMetrics = v.GetCanSubscribeMetrics()
	perm.Hidden = v.Hidden
	return perm
}
