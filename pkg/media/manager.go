package media

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/discovery"
	"github.com/axonkit/axonkit/pkg/routing"
	"github.com/axonkit/axonkit/pkg/types"

	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/singleflight"
)

type pipeTransport struct {
	*mediasoup.Transport
	connectMu sync.Mutex
	connected bool
}

type Manager struct {
	nodeID    types.NodeID
	nc        *nats.Conn
	webRtcCfg config.WebRtcConfig
	logger    *slog.Logger

	workerBin   string
	workerCount int

	workers       []*mediasoup.Worker
	routers       []*mediasoup.Router
	webRtcServers []*mediasoup.WebRtcServer
	workerMu      sync.RWMutex

	// Pipe Reuse: map[LocalRouterID]map[RemoteRouterID]*pipeTransport
	pipeTransports map[string]map[string]*pipeTransport
	pipeMu         sync.RWMutex
	// Direct Reuse: map[RouterID]*mediasoup.Transport
	directTransports map[string]*mediasoup.Transport
	directMu         sync.Mutex

	// Local Producers: StreamID -> {RouterIndex, Producer}
	producers   map[types.StreamID]*LocalProducer
	producersMu sync.RWMutex

	// Singleflight
	sf singleflight.Group

	// Serving Remote Consumers (Pipe): ConsumerID -> Info
	servingConsumers     map[string]*ServingConsumerInfo
	servingMu            sync.Mutex
	servingDataConsumers map[string]*ServingDataConsumerInfo
	servingDataMu        sync.Mutex

	pingChannels map[types.NodeID]*PingChannel
	pingMu       sync.Mutex

	// Routing (optional; only wired in production). When nil, Register/
	// Unregister skip advertise and EnsureProducer falls back to single-node
	// behavior. FIB is constructed internally when rib is provided; the
	// finder supplied via WithRIB drives FIB.Subscribe for remote streams.
	rib    *routing.RIB
	fib    *routing.FIB
	finder *discovery.Finder

	closed atomic.Bool

	OnStreamClosed func(streamID types.StreamID)
}

type ManagerOption func(*Manager)

func WithWorkerCount(count int) ManagerOption {
	return func(m *Manager) {
		m.workerCount = count
	}
}

// WithRIB wires the routing layer into the Manager. Both rib and finder must
// be supplied together: rib powers producer advertise/unadvertise and backs
// the internal FIB, while finder resolves the best upstream node for remote
// streams during EnsureProducer. Without this option the Manager operates in
// single-node mode and EnsureProducer errors for any non-local stream.
func WithRIB(rib *routing.RIB, finder *discovery.Finder) ManagerOption {
	return func(m *Manager) {
		m.rib = rib
		m.finder = finder
	}
}

type ServingConsumerInfo struct {
	Consumer   *mediasoup.Consumer
	RemoteNode types.NodeID
	StreamID   types.StreamID
}

type ServingDataConsumerInfo struct {
	Consumer   *mediasoup.DataConsumer
	RemoteNode types.NodeID
	StreamID   types.StreamID
}

type LocalProducer struct {
	StreamID           types.StreamID
	Producer           *mediasoup.Producer
	DataProducer       *mediasoup.DataProducer
	Router             *mediasoup.Router
	UpstreamNode       types.NodeID
	UpstreamConsumerId string
}

func (lp *LocalProducer) Close() {
	if lp.Producer != nil {
		lp.Producer.Close()
	}
	if lp.DataProducer != nil {
		lp.DataProducer.Close()
	}
}

type PingChannel struct {
	remoteNode   types.NodeID
	routerId     string
	producer     *mediasoup.DataProducer
	consumer     *mediasoup.DataConsumer
	pipeProducer *mediasoup.DataProducer
	pipeConsumer *mediasoup.DataConsumer
	pending      map[string]chan int64
	mu           sync.Mutex
}

type pingMessage struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	TS   int64  `json:"ts"`
}

func (lp *LocalProducer) Id() string {
	if lp.Producer != nil {
		return lp.Producer.Id()
	}
	if lp.DataProducer != nil {
		return lp.DataProducer.Id()
	}
	return ""
}

// NewManager constructs a Manager for the given local node. Pass WithRIB to
// enable cross-node streaming; without it the Manager operates in single-node
// mode.
func NewManager(nodeID types.NodeID, nc *nats.Conn, workerBin string, webRtcCfg config.WebRtcConfig, opts ...ManagerOption) *Manager {
	m := &Manager{
		nodeID:               nodeID,
		nc:                   nc,
		workerBin:            workerBin,
		webRtcCfg:            webRtcCfg,
		logger:               slog.Default().With("component", "MediaManager"),
		pipeTransports:       make(map[string]map[string]*pipeTransport),
		producers:            make(map[types.StreamID]*LocalProducer),
		servingConsumers:     make(map[string]*ServingConsumerInfo),
		servingDataConsumers: make(map[string]*ServingDataConsumerInfo),
		pingChannels:         make(map[types.NodeID]*PingChannel),
		directTransports:     make(map[string]*mediasoup.Transport),
	}
	for _, opt := range opts {
		opt(m)
	}
	if m.rib != nil {
		m.fib = routing.NewFIB(m.rib, m.nodeID)
		m.fib.ConnectPipe = m.connectPipe
		m.fib.DisconnectPipe = m.disconnectPipe
	}
	return m
}

// FIB returns the internal FIB, or nil if the Manager was constructed without
// a RIB. Callers must not mutate ConnectPipe/DisconnectPipe on the returned
// FIB — those are owned by the Manager.
func (m *Manager) FIB() *routing.FIB { return m.fib }

// Routers returns the mediasoup routers managed by this Manager.
func (m *Manager) Routers() []*mediasoup.Router {
	m.workerMu.RLock()
	defer m.workerMu.RUnlock()
	return m.routers
}

func (m *Manager) RouterForId(routerId string) *mediasoup.Router {
	for _, r := range m.routers {
		if r.Id() == routerId {
			return r
		}
	}
	return nil
}

// WebRtcServers returns the WebRtcServers managed by this Manager (one per worker).
func (m *Manager) WebRtcServers() []*mediasoup.WebRtcServer {
	m.workerMu.RLock()
	defer m.workerMu.RUnlock()
	return m.webRtcServers
}

// RegisterProducer registers a local producer (from a participant) so the
// pipe system can find it when a remote node requests the stream. When the
// Manager was constructed with a RIB the stream is also advertised as an
// origin route so remote nodes can discover it.
func (m *Manager) RegisterProducer(streamID types.StreamID, producer *mediasoup.Producer) {
	m.producersMu.Lock()
	m.producers[streamID] = &LocalProducer{
		StreamID: streamID,
		Producer: producer,
		Router:   m.RouterForId(streamID.RouterID()),
	}
	m.producersMu.Unlock()
	m.advertiseOrigin(streamID)
	m.logger.Info("Producer registered", "streamId", streamID)
}

// RegisterDataProducer registers a local data producer (from a participant) so
// the pipe system can find it when a remote node requests the stream. Behaves
// like RegisterProducer with respect to origin advertisement.
func (m *Manager) RegisterDataProducer(streamID types.StreamID, dataProducer *mediasoup.DataProducer) {
	m.producersMu.Lock()
	m.producers[streamID] = &LocalProducer{
		StreamID:     streamID,
		DataProducer: dataProducer,
		Router:       m.RouterForId(streamID.RouterID()),
	}
	m.producersMu.Unlock()
	m.advertiseOrigin(streamID)
	m.logger.Info("Data producer registered", "streamId", streamID)
}

// UnregisterProducer removes a producer and cleans up any downstream pipe
// consumers that were serving the stream to remote nodes. Also stops the
// origin advertisement when the Manager owns a RIB.
func (m *Manager) UnregisterProducer(streamID types.StreamID) {
	m.producersMu.Lock()
	delete(m.producers, streamID)
	m.producersMu.Unlock()

	m.unadvertise(streamID)

	m.servingMu.Lock()
	for id, info := range m.servingConsumers {
		if info.StreamID == streamID {
			delete(m.servingConsumers, id)
			info.Consumer.Close()
		}
	}
	m.servingMu.Unlock()

	m.servingDataMu.Lock()
	for id, info := range m.servingDataConsumers {
		if info.StreamID == streamID {
			delete(m.servingDataConsumers, id)
			info.Consumer.Close()
		}
	}
	m.servingDataMu.Unlock()

	m.logger.Info("Producer unregistered", "streamId", streamID)
}

// GetProducer returns the local or already-piped producer for streamID, or nil
// if the stream has not been materialized on this node. It is a non-blocking
// lookup — use EnsureProducer when a remote pipe may need to be established.
func (m *Manager) GetProducer(streamID types.StreamID) *LocalProducer {
	m.producersMu.RLock()
	defer m.producersMu.RUnlock()
	return m.producers[streamID]
}

// EnsureProducer returns the local or piped producer for streamID. If the
// stream is remote it triggers a FIB subscribe (increments the pipe refcount
// and connects the pipe transport on demand) before returning. Every
// successful call must be balanced by ReleaseProducer when the consumer no
// longer needs the stream.
//
// Requires WithRIB+WithLinkDB at construction time; otherwise returns an
// error for remote streams.
func (m *Manager) EnsureProducer(streamID types.StreamID) (*LocalProducer, error) {
	if lp := m.GetProducer(streamID); lp != nil {
		return lp, nil
	}
	if m.fib == nil {
		return nil, fmt.Errorf("media manager has no FIB configured; cannot pipe %s", streamID)
	}
	if m.finder == nil {
		return nil, fmt.Errorf("media manager has no Finder configured (need WithLinkDB); cannot pipe %s", streamID)
	}
	if _, _, err := m.fib.Subscribe(streamID, m.finder.FindPath); err != nil {
		return nil, err
	}
	return m.GetProducer(streamID), nil
}

// ReleaseProducer decrements the FIB refcount for a piped stream. It is a
// no-op for locally originated producers (they are released via
// UnregisterProducer) and when no FIB is configured.
func (m *Manager) ReleaseProducer(streamID types.StreamID) {
	if m.fib == nil {
		return
	}
	if lp := m.GetProducer(streamID); lp != nil && lp.UpstreamNode == "" {
		// Local origin — Unsubscribe would be a no-op but for clarity we
		// skip rather than pass it through.
		return
	}
	m.fib.Unsubscribe(streamID)
}

func (m *Manager) advertiseOrigin(streamID types.StreamID) {
	if m.rib == nil {
		return
	}
	if err := m.rib.StartAdvertise(routing.AdvertiseOptions{
		StreamID: streamID,
		IsOrigin: true,
	}); err != nil {
		m.logger.Error("Failed to advertise stream", "stream_id", streamID, "error", err)
	}
}

func (m *Manager) unadvertise(streamID types.StreamID) {
	if m.rib == nil {
		return
	}
	if err := m.rib.StopAdvertise(streamID); err != nil {
		m.logger.Error("Failed to stop advertise", "stream_id", streamID, "error", err)
	}
}

func (m *Manager) Start() error {
	numWorkers := m.workerCount
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	m.logger.Info("Starting mediasoup workers", "count", numWorkers, "bin", m.workerBin)

	m.workers = make([]*mediasoup.Worker, numWorkers)
	m.routers = make([]*mediasoup.Router, numWorkers)
	m.webRtcServers = make([]*mediasoup.WebRtcServer, numWorkers)

	for i := 0; i < numWorkers; i++ {
		if err := m.launchWorker(i); err != nil {
			return err
		}
	}

	// Start NATS Subscription for Signaling
	_, err := m.nc.Subscribe(fmt.Sprintf("signaling.pipe.%s", m.nodeID), m.handleSignaling)
	if err != nil {
		return err
	}

	return nil
}

func (m *Manager) Close() {
	if m.closed.Swap(true) {
		return
	}
	m.logger.Info("Closing Media Manager...")

	// Tear down FIB first so that outstanding pipe/refcount state releases
	// before we close the underlying workers.
	if m.fib != nil {
		m.fib.Close()
	}

	m.producersMu.Lock()
	streamIDs := make([]types.StreamID, 0, len(m.producers))
	for streamID := range m.producers {
		streamIDs = append(streamIDs, streamID)
	}
	m.producersMu.Unlock()

	for _, streamID := range streamIDs {
		m.unadvertise(streamID)
		if m.OnStreamClosed != nil {
			m.OnStreamClosed(streamID)
		}
	}

	m.workerMu.Lock()
	defer m.workerMu.Unlock()

	for _, worker := range m.workers {
		if worker != nil {
			worker.Close()
		}
	}
}

func (m *Manager) buildWebRtcServerListenInfos() []*mediasoup.TransportListenInfo {
	cfg := m.webRtcCfg
	ip := cfg.ListenIP
	if ip == "" {
		ip = "0.0.0.0"
	}

	base := mediasoup.TransportListenInfo{
		Ip:               ip,
		AnnouncedAddress: cfg.AnnouncedIP,
		ExposeInternalIp: true,
	}
	if cfg.MinPort > 0 && cfg.MaxPort > 0 {
		base.PortRange = mediasoup.TransportPortRange{Min: cfg.MinPort, Max: cfg.MaxPort}
	}

	udp := base
	udp.Protocol = mediasoup.TransportProtocolUDP
	tcp := base
	tcp.Protocol = mediasoup.TransportProtocolTCP
	return []*mediasoup.TransportListenInfo{&udp, &tcp}
}

func (m *Manager) launchWorker(index int) error {
	worker, err := mediasoup.NewWorker(m.workerBin, func(o *mediasoup.WorkerSettings) {
		o.LogLevel = mediasoup.WorkerLogLevelWarn
	})
	if err != nil {
		return err
	}

	router, err := worker.CreateRouter(&mediasoup.RouterOptions{
		MediaCodecs: rtpCodecCapabilities,
	})
	if err != nil {
		worker.Close()
		return err
	}

	listenInfos := m.buildWebRtcServerListenInfos()
	webRtcServer, err := worker.CreateWebRtcServer(&mediasoup.WebRtcServerOptions{
		ListenInfos: listenInfos,
	})
	if err != nil {
		router.Close()
		worker.Close()
		return fmt.Errorf("webrtc server: %w", err)
	}

	m.workerMu.Lock()
	m.workers[index] = worker
	m.routers[index] = router
	m.webRtcServers[index] = webRtcServer
	m.workerMu.Unlock()

	// Wait for death
	worker.OnClose(func(context.Context) {
		if m.closed.Load() {
			return
		}
		m.logger.Error("Worker died, cleaning up and restarting...", "index", index, "pid", worker.Pid(), "error", worker.Err())
		m.cleanupWorkerResources(router.Id())
		go m.restartWorker(index)
	})

	return nil
}

func (m *Manager) restartWorker(index int) {
	for {
		if m.closed.Load() {
			return
		}
		time.Sleep(1 * time.Second)
		m.logger.Info("Restarting worker", "index", index)
		if err := m.launchWorker(index); err == nil {
			m.logger.Info("Worker restarted", "index", index)
			return
		}
		m.logger.Error("Restart failed, retrying...", "index", index)
	}
}

// connectPipe is the FIB ConnectPipe callback. It either returns an existing
// local producer ID (when the stream is already hosted on this node) or
// establishes a pipe transport with the upstream node and produces a local
// "pipe" producer forwarding the media.
func (m *Manager) connectPipe(streamID types.StreamID, upstreamNode types.NodeID) (string, error) {
	// Optimization: If upstream is local
	if upstreamNode == m.nodeID {
		m.producersMu.RLock()
		lp, ok := m.producers[streamID]
		m.producersMu.RUnlock()

		if !ok {
			return "", fmt.Errorf("stream not found locally")
		}

		// Return original producer ID. Consume will handle routing.
		m.logger.Info("Stream is local", "stream_id", streamID, "router_id", lp.Router.Id())
		return lp.Id(), nil
	}

	// Singleflight
	val, err, _ := m.sf.Do(string(streamID), func() (interface{}, error) {
		// 1. Re-Check Existing (Inside Lock)
		m.producersMu.RLock()
		lp, ok := m.producers[streamID]
		m.producersMu.RUnlock()
		if ok {
			return lp.Id(), nil
		}

		remoteRouterId := streamID.RouterID()
		if remoteRouterId == "" {
			return nil, fmt.Errorf("invalid streamID: %s", streamID)
		}
		remoteProducerId := streamID.ProducerID()

		// 2. Load Balance Local Router
		idx := int(crc32.ChecksumIEEE([]byte(streamID))) % len(m.routers)
		localRouter := m.routers[idx]

		// 3. Get/Create Pipe (Reuse if possible)
		pipe, err := m.getOrCreateLocalPipe(localRouter, remoteRouterId, upstreamNode)
		if err != nil {
			return nil, err
		}

		transportData := pipe.Data().PipeTransportData
		tuple := transportData.Tuple

		// 4. Prepare Request
		myInfo := PipeInfo{
			Ip:             tuple.LocalAddress,
			Port:           tuple.LocalPort,
			SrtpParameters: transportData.SrtpParameters,
		}
		req := PipeRequest{
			Command:           "EstablishPipe",
			StreamID:          streamID,
			RequesterNode:     m.nodeID,
			RequesterRouterId: localRouter.Id(),
			PipeInfo:          myInfo,
		}

		resp, err := m.request(upstreamNode, req)
		if err != nil {
			return nil, err
		}

		err = m.ensurePipeConnected(pipe, resp.PipeInfo)
		if err != nil {
			return nil, fmt.Errorf("pipe connect failed: %v", err)
		}
		m.logger.Info("Pipe Connected", "upstream", upstreamNode)

		if streamID.IsDataProducer() {
			if resp.DataProducerInfo == nil {
				return nil, fmt.Errorf("missing data producer info")
			}
			resp.DataProducerInfo.Id = remoteProducerId
			dataProducer, err := pipe.ProduceData(resp.DataProducerInfo)
			if err != nil {
				m.logger.Error("Pipe ProduceData failed", "error", err)
				return nil, err
			}

			m.producersMu.Lock()
			m.producers[streamID] = &LocalProducer{
				StreamID:           streamID,
				DataProducer:       dataProducer,
				Router:             localRouter,
				UpstreamNode:       upstreamNode,
				UpstreamConsumerId: resp.PipeConsumerId,
			}
			m.producersMu.Unlock()

			m.logger.Info("Pipe Data Stream Ready", "stream_id", streamID, "pipe_producer_id", dataProducer.Id())
			return resp.PipeConsumerId, nil
		}

		// 7. Produce (Receive from remote)
		pipeProducer, err := pipe.Produce(&mediasoup.ProducerOptions{
			Id:            resp.ProducerInfo.Id,
			Kind:          resp.ProducerInfo.Kind,
			RtpParameters: resp.ProducerInfo.RtpParameters,
			Paused:        resp.ProducerInfo.Paused,
			AppData:       resp.ProducerInfo.AppData,
		})
		if err != nil {
			m.logger.Error("Pipe Produce failed", "error", err)
			return nil, err
		}

		m.producersMu.Lock()
		m.producers[streamID] = &LocalProducer{
			StreamID:           streamID,
			Producer:           pipeProducer,
			Router:             localRouter,
			UpstreamNode:       upstreamNode,
			UpstreamConsumerId: resp.PipeConsumerId,
		}
		m.producersMu.Unlock()

		m.logger.Info("Pipe Stream Ready", "stream_id", streamID, "pipe_producer_id", pipeProducer.Id())
		return resp.PipeConsumerId, nil
	})

	if err != nil {
		return "", err
	}
	return val.(string), nil
}

func (m *Manager) getPingChannel(remoteNode types.NodeID) *PingChannel {
	m.pingMu.Lock()
	defer m.pingMu.Unlock()

	if ch, ok := m.pingChannels[remoteNode]; ok {
		return ch
	}
	ch := &PingChannel{
		remoteNode: remoteNode,
		pending:    make(map[string]chan int64),
	}
	m.pingChannels[remoteNode] = ch
	return ch
}

func (m *Manager) ensurePingChannel(remoteNode types.NodeID) (*PingChannel, error) {
	if remoteNode == m.nodeID {
		return nil, fmt.Errorf("cannot ping self")
	}

	ch := m.getPingChannel(remoteNode)

	ch.mu.Lock()
	if ch.producer != nil && ch.consumer != nil {
		ch.mu.Unlock()
		return ch, nil
	}
	ch.mu.Unlock()

	_, err, _ := m.sf.Do("ping:"+string(remoteNode), func() (interface{}, error) {
		idx := int(crc32.ChecksumIEEE([]byte(remoteNode))) % len(m.routers)
		localRouter := m.routers[idx]

		pipe, err := m.getOrCreateLocalPipe(localRouter, "ping:"+string(remoteNode), remoteNode)
		if err != nil {
			return nil, err
		}
		myInfo := NewPipeInfoFromTransportData(pipe.Data().PipeTransportData)

		ch, err := m.ensurePingChannelOnPipe(remoteNode, localRouter, pipe, nil)
		if err != nil {
			return nil, err
		}
		if ch.pipeConsumer == nil {
			return nil, fmt.Errorf("missing ping pipe consumer")
		}

		pingStreamID := types.NewStreamID(localRouter.Id(), ch.producer.Id())
		resp, err := m.request(remoteNode, PipeRequest{
			Command:       "EnsurePingChannel",
			RequesterNode: m.nodeID,
			StreamID:      pingStreamID,
			PipeInfo:      myInfo,
			DataProducerInfo: &mediasoup.DataProducerOptions{
				Id:                   ch.pipeConsumer.Id(),
				SctpStreamParameters: ch.pipeConsumer.SctpStreamParameters(),
				Label:                ch.pipeConsumer.Label(),
				Protocol:             ch.pipeConsumer.Protocol(),
				AppData:              ch.pipeConsumer.AppData(),
			},
		})
		if err != nil {
			return nil, err
		}

		err = m.ensurePipeConnected(pipe, resp.PipeInfo)
		if err != nil {
			return nil, fmt.Errorf("pipe connect failed: %v", err)
		}

		if resp.DataProducerInfo == nil {
			return nil, fmt.Errorf("missing ping producer")
		}
		_, err = m.ensurePingChannelOnPipe(remoteNode, localRouter, pipe, resp.DataProducerInfo)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	if err != nil {
		return nil, err
	}

	ch = m.getPingChannel(remoteNode)
	return ch, nil
}

func (m *Manager) ensurePingChannelOnPipe(remoteNode types.NodeID, router *mediasoup.Router, pipe *pipeTransport, remoteProducerInfo *mediasoup.DataProducerOptions) (*PingChannel, error) {
	ch := m.getPingChannel(remoteNode)
	ch.mu.Lock()
	defer ch.mu.Unlock()

	direct, err := m.getOrCreateDirectTransport(router)
	if err != nil {
		return nil, err
	}

	if ch.producer == nil {
		producer, err := direct.ProduceData(&mediasoup.DataProducerOptions{
			Label:    "rtt",
			Protocol: "rtt",
		})
		if err != nil {
			return nil, err
		}
		ch.producer = producer
		ch.routerId = router.Id()
	}
	if ch.pipeConsumer == nil {
		pipeConsumer, err := pipe.ConsumeData(&mediasoup.DataConsumerOptions{
			DataProducerId: ch.producer.Id(),
		})
		if err != nil {
			return nil, err
		}
		ch.pipeConsumer = pipeConsumer
	}
	if remoteProducerInfo != nil && ch.pipeProducer == nil {
		pipeProducer, err := pipe.ProduceData(remoteProducerInfo)
		if err != nil {
			return nil, err
		}
		ch.pipeProducer = pipeProducer
	}
	if ch.pipeProducer != nil && ch.consumer == nil {
		consumer, err := direct.ConsumeData(&mediasoup.DataConsumerOptions{
			DataProducerId: ch.pipeProducer.Id(),
		})
		if err != nil {
			return nil, err
		}
		ch.consumer = consumer
		consumer.OnMessage(func(payload []byte, ppid mediasoup.SctpPayloadType) {
			m.handlePingMessage(remoteNode, payload)
		})
	}
	return ch, nil
}

func (m *Manager) ensurePipeConnected(pipe *pipeTransport, info PipeInfo) error {
	pipe.connectMu.Lock()
	defer pipe.connectMu.Unlock()

	if pipe.connected {
		return nil
	}
	if err := pipe.Connect(&mediasoup.TransportConnectOptions{
		Ip:             info.Ip,
		Port:           &info.Port,
		SrtpParameters: info.SrtpParameters,
	}); err != nil {
		return err
	}
	pipe.connected = true
	return nil
}

func (m *Manager) handlePingMessage(remoteNode types.NodeID, payload []byte) {
	var msg pingMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return
	}

	ch := m.getPingChannel(remoteNode)

	switch msg.Type {
	case "ping":
		ch.mu.Lock()
		producer := ch.producer
		ch.mu.Unlock()
		if producer == nil {
			return
		}
		resp := pingMessage{
			Type: "pong",
			ID:   msg.ID,
			TS:   msg.TS,
		}
		data, err := json.Marshal(resp)
		if err != nil {
			return
		}
		_ = producer.Send(data)
	case "pong":
		ch.mu.Lock()
		waitCh, ok := ch.pending[msg.ID]
		if ok {
			delete(ch.pending, msg.ID)
		}
		ch.mu.Unlock()
		if ok {
			rtt := time.Since(time.Unix(0, msg.TS)).Milliseconds()
			waitCh <- rtt
		}
	}
}

func (m *Manager) ProbeRTT(remoteNode types.NodeID) (int64, bool) {
	ch, err := m.ensurePingChannel(remoteNode)
	if err != nil {
		return 0, false
	}
	ch.mu.Lock()
	producer := ch.producer
	consumer := ch.consumer
	ch.mu.Unlock()

	if producer == nil || consumer == nil {
		return 0, false
	}

	id := fmt.Sprintf("%d", time.Now().UnixNano())
	ts := time.Now().UnixNano()
	msg := pingMessage{
		Type: "ping",
		ID:   id,
		TS:   ts,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return 0, false
	}

	waitCh := make(chan int64, 1)
	ch.mu.Lock()
	ch.pending[id] = waitCh
	ch.mu.Unlock()

	if err := producer.Send(data); err != nil {
		ch.mu.Lock()
		delete(ch.pending, id)
		ch.mu.Unlock()
		return 0, false
	}

	select {
	case rtt := <-waitCh:
		return rtt, true
	case <-time.After(1 * time.Second):
		ch.mu.Lock()
		delete(ch.pending, id)
		ch.mu.Unlock()
		return 0, false
	}
}

func (m *Manager) HandlePingPayload(remoteNode types.NodeID, payload []byte) {
	m.handlePingMessage(remoteNode, payload)
}

func (m *Manager) getOrCreateLocalPipe(localRouter *mediasoup.Router, remoteRouterId string, remoteNode types.NodeID) (*pipeTransport, error) {
	if remoteRouterId == "" {
		return nil, fmt.Errorf("remoteRouterId is required")
	}

	m.pipeMu.Lock()
	defer m.pipeMu.Unlock()

	if _, ok := m.pipeTransports[localRouter.Id()]; !ok {
		m.pipeTransports[localRouter.Id()] = make(map[string]*pipeTransport)
	}

	if pipe, ok := m.pipeTransports[localRouter.Id()][remoteRouterId]; ok {
		return pipe, nil
	}

	// Create New
	pipe, err := localRouter.CreatePipeTransport(&mediasoup.PipeTransportOptions{
		ListenInfo: mediasoup.TransportListenInfo{
			Ip:       "127.0.0.1",
			Protocol: mediasoup.TransportProtocolUDP,
		},
		EnableSctp: true,
		EnableSrtp: true,
		EnableRtx:  true,
		AppData: map[string]interface{}{
			"remoteNode": remoteNode,
		},
	})
	if err != nil {
		return nil, err
	}

	wrapped := &pipeTransport{Transport: pipe}
	m.pipeTransports[localRouter.Id()][remoteRouterId] = wrapped

	return wrapped, nil
}

func (m *Manager) getOrCreateDirectTransport(router *mediasoup.Router) (*mediasoup.Transport, error) {
	m.directMu.Lock()
	defer m.directMu.Unlock()

	if t, ok := m.directTransports[router.Id()]; ok {
		return t, nil
	}
	t, err := router.CreateDirectTransport(&mediasoup.DirectTransportOptions{})
	if err != nil {
		return nil, err
	}
	m.directTransports[router.Id()] = t
	return t, nil
}

// CleanUpNode closes all pipe transports to a specific remote node
func (m *Manager) CleanUpNode(remoteNode types.NodeID) {
	// 2. Clean Pipe Transports
	m.pipeMu.Lock()
	for _, pipes := range m.pipeTransports {
		for remoteRouterId, pipe := range pipes {
			// Check AppData for remoteNode
			if node, ok := pipe.AppData()["remoteNode"].(types.NodeID); ok && node == remoteNode {
				delete(pipes, remoteRouterId)
				pipe.Close()
				m.logger.Info("Force closed pipe to remote node", "remote_node", remoteNode, "pipe_id", pipe.Id())
			}
		}
	}
	m.pipeMu.Unlock()

	m.pingMu.Lock()
	if ch, ok := m.pingChannels[remoteNode]; ok {
		if ch.producer != nil {
			ch.producer.Close()
		}
		if ch.consumer != nil {
			ch.consumer.Close()
		}
		if ch.pipeProducer != nil {
			ch.pipeProducer.Close()
		}
		if ch.pipeConsumer != nil {
			ch.pipeConsumer.Close()
		}
		delete(m.pingChannels, remoteNode)
	}
	m.pingMu.Unlock()

	// 3. Clean Serving Consumers for this node
	m.servingMu.Lock()
	for id, info := range m.servingConsumers {
		if info.RemoteNode == remoteNode {
			delete(m.servingConsumers, id)
			info.Consumer.Close()
			m.logger.Info("Cleaned up serving consumer for dead node", "consumer_id", id, "node", remoteNode)
		}
	}
	m.servingMu.Unlock()

	m.servingDataMu.Lock()
	for id, info := range m.servingDataConsumers {
		if info.RemoteNode == remoteNode {
			delete(m.servingDataConsumers, id)
			info.Consumer.Close()
			m.logger.Info("Cleaned up serving data consumer for dead node", "consumer_id", id, "node", remoteNode)
		}
	}
	m.servingDataMu.Unlock()
}

func (m *Manager) request(node types.NodeID, req PipeRequest) (*PipeResponse, error) {
	data, _ := json.Marshal(req)
	msg, err := m.nc.Request(fmt.Sprintf("signaling.pipe.%s", node), data, 2*time.Second)
	if err != nil {
		return nil, err
	}
	var resp PipeResponse
	json.Unmarshal(msg.Data, &resp)
	if resp.Error != "" {
		return nil, fmt.Errorf("%s", resp.Error)
	}
	return &resp, nil
}

func (m *Manager) notifyPipeCleanup(remoteNode types.NodeID, deadRouterID string) {
	_, err := m.request(remoteNode, PipeRequest{
		Command:           "CleanupPipeTransports",
		RequesterRouterId: deadRouterID,
	})
	if err != nil {
		m.logger.Error("Failed to send pipe cleanup signaling", "remote_node", remoteNode, "router_id", deadRouterID, "error", err)
	}
}

func (m *Manager) notifyPipeProducerState(remoteNode types.NodeID, streamID types.StreamID, pipeConsumerId string, paused bool) {
	command := "ResumePipeProducer"
	if paused {
		command = "PausePipeProducer"
	}
	_, err := m.request(remoteNode, PipeRequest{
		Command:        command,
		StreamID:       streamID,
		PipeConsumerId: pipeConsumerId,
	})
	if err != nil {
		m.logger.Error("Notify pipe producer state failed", "error", err, "remote_node", remoteNode, "stream_id", streamID, "paused", paused)
	}
}

func (m *Manager) notifyPipeClose(remoteNode types.NodeID, streamID types.StreamID, pipeConsumerId string) {
	req := PipeRequest{
		Command:        "ClosePipe",
		StreamID:       streamID,
		RequesterNode:  m.nodeID,
		PipeConsumerId: pipeConsumerId,
	}
	if _, err := m.request(remoteNode, req); err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			m.logger.Info("ClosePipe signaling skipped (node offline)", "upstream", remoteNode)
		} else {
			m.logger.Warn("Failed to send ClosePipe signaling", "upstream", remoteNode, "error", err)
		}
	}
}

func (m *Manager) handlePipeCleanup(req PipeRequest) PipeResponse {
	resp := PipeResponse{}
	if req.RequesterRouterId == "" {
		resp.Error = "deadRouterId is required"
		return resp
	}
	m.pipeMu.Lock()
	for _, pipes := range m.pipeTransports {
		if pipe, ok := pipes[req.RequesterRouterId]; ok {
			pipe.Close()
			delete(pipes, req.RequesterRouterId)
		}
	}
	m.pipeMu.Unlock()
	return resp
}

func (m *Manager) handleEstablishPipe(req PipeRequest) PipeResponse {
	resp := PipeResponse{}

	m.producersMu.RLock()
	lp, ok := m.producers[req.StreamID]
	m.producersMu.RUnlock()

	if !ok {
		resp.Error = "Stream not found"
		return resp
	}

	pipe, err := m.getOrCreateLocalPipe(lp.Router, req.RequesterRouterId, req.RequesterNode)
	if err != nil {
		resp.Error = err.Error()
		return resp
	}

	if err := m.ensurePipeConnected(pipe, req.PipeInfo); err != nil {
		resp.Error = "Connect failed: " + err.Error()
		return resp
	}

	var consumer *mediasoup.Consumer
	var dataConsumer *mediasoup.DataConsumer
	if req.StreamID.IsDataProducer() {
		if lp.DataProducer == nil {
			resp.Error = "DataProducer not found"
			return resp
		}
		dataConsumer, err = pipe.ConsumeData(&mediasoup.DataConsumerOptions{
			DataProducerId: lp.DataProducer.Id(),
		})
		if err != nil {
			m.logger.Error("Pipe ConsumeData failed", "error", err)
			resp.Error = "ConsumeData failed: " + err.Error()
			return resp
		}

		m.servingDataMu.Lock()
		m.servingDataConsumers[dataConsumer.Id()] = &ServingDataConsumerInfo{
			Consumer:   dataConsumer,
			RemoteNode: req.RequesterNode,
			StreamID:   req.StreamID,
		}
		m.servingDataMu.Unlock()
		dataConsumer.OnClose(func(ctx context.Context) {
			m.logger.Info("Pipe data consumer closed", "remote_node", req.RequesterNode, "stream_id", req.StreamID, "consumer_id", dataConsumer.Id())
			go m.notifyPipeClose(req.RequesterNode, req.StreamID, dataConsumer.Id())
		})
		dataConsumer.OnPause(func(ctx context.Context) {
			m.logger.Info("Pipe data consumer paused", "remote_node", req.RequesterNode, "stream_id", req.StreamID, "consumer_id", dataConsumer.Id())
			go m.notifyPipeProducerState(req.RequesterNode, req.StreamID, dataConsumer.Id(), true)
		})
		dataConsumer.OnResume(func(ctx context.Context) {
			m.logger.Info("Pipe data consumer resumed", "remote_node", req.RequesterNode, "stream_id", req.StreamID, "consumer_id", dataConsumer.Id())
			go m.notifyPipeProducerState(req.RequesterNode, req.StreamID, dataConsumer.Id(), false)
		})
	} else {
		if lp.Producer == nil {
			resp.Error = "Producer not found"
			return resp
		}
		consumer, err = pipe.Consume(&mediasoup.ConsumerOptions{
			ProducerId: lp.Producer.Id(),
		})
		if err != nil {
			m.logger.Error("Pipe Consume failed", "error", err)
			resp.Error = "Consume failed: " + err.Error()
			return resp
		}

		m.servingMu.Lock()
		m.servingConsumers[consumer.Id()] = &ServingConsumerInfo{
			Consumer:   consumer,
			RemoteNode: req.RequesterNode,
			StreamID:   req.StreamID,
		}
		m.servingMu.Unlock()
		consumer.OnClose(func(ctx context.Context) {
			m.logger.Info("Pipe consumer closed", "remote_node", req.RequesterNode, "stream_id", req.StreamID, "consumer_id", consumer.Id())
			go m.notifyPipeClose(req.RequesterNode, req.StreamID, consumer.Id())
		})
		consumer.OnPause(func(ctx context.Context) {
			m.logger.Info("Pipe consumer paused", "remote_node", req.RequesterNode, "stream_id", req.StreamID, "consumer_id", consumer.Id())
			go m.notifyPipeProducerState(req.RequesterNode, req.StreamID, consumer.Id(), true)
		})
		consumer.OnResume(func(ctx context.Context) {
			m.logger.Info("Pipe consumer resumed", "remote_node", req.RequesterNode, "stream_id", req.StreamID, "consumer_id", consumer.Id())
			go m.notifyPipeProducerState(req.RequesterNode, req.StreamID, consumer.Id(), false)
		})
	}

	if consumer != nil {
		resp.PipeConsumerId = consumer.Id()
		resp.ProducerInfo = &mediasoup.ProducerOptions{
			Id:            lp.Producer.Id(),
			Kind:          consumer.Kind(),
			RtpParameters: consumer.RtpParameters(),
			Paused:        consumer.ProducerPaused(),
			AppData:       lp.Producer.AppData(),
		}
	}
	if dataConsumer != nil {
		resp.PipeConsumerId = dataConsumer.Id()
		resp.DataProducerInfo = &mediasoup.DataProducerOptions{
			Id:                   lp.DataProducer.Id(),
			SctpStreamParameters: dataConsumer.SctpStreamParameters(),
			Label:                dataConsumer.Label(),
			Protocol:             dataConsumer.Protocol(),
			AppData:              lp.DataProducer.AppData(),
		}
	}

	resp.PipeInfo = NewPipeInfoFromTransportData(pipe.Data().PipeTransportData)

	return resp
}

func (m *Manager) handleEnsurePingChannel(req PipeRequest) PipeResponse {
	resp := PipeResponse{}

	requesterRouterId := req.StreamID.RouterID()
	if requesterRouterId == "" {
		resp.Error = "invalid streamID"
		return resp
	}

	idx := int(crc32.ChecksumIEEE([]byte(req.RequesterNode))) % len(m.routers)
	router := m.routers[idx]

	pipe, err := m.getOrCreateLocalPipe(router, requesterRouterId, req.RequesterNode)
	if err != nil {
		resp.Error = err.Error()
		return resp
	}

	if err := m.ensurePipeConnected(pipe, req.PipeInfo); err != nil {
		resp.Error = "Connect failed: " + err.Error()
		return resp
	}

	ch, err := m.ensurePingChannelOnPipe(req.RequesterNode, router, pipe, req.DataProducerInfo)
	if err != nil {
		resp.Error = err.Error()
		return resp
	}
	if ch.pipeConsumer == nil {
		resp.Error = "missing ping pipe consumer"
		return resp
	}

	resp.PipeInfo = NewPipeInfoFromTransportData(pipe.Data().PipeTransportData)
	resp.DataProducerInfo = &mediasoup.DataProducerOptions{
		Id:                   ch.producer.Id(),
		SctpStreamParameters: ch.pipeConsumer.SctpStreamParameters(),
		Label:                ch.pipeConsumer.Label(),
		Protocol:             ch.pipeConsumer.Protocol(),
		AppData:              ch.producer.AppData(),
	}

	return resp
}

func (m *Manager) handleClosePipe(req PipeRequest) {
	m.servingMu.Lock()
	info, ok := m.servingConsumers[req.PipeConsumerId]
	if ok {
		delete(m.servingConsumers, req.PipeConsumerId)
		info.Consumer.Close()
		m.logger.Info("Closed serving pipe consumer", "consumer_id", req.PipeConsumerId)
	} else {
		m.logger.Warn("ClosePipe: consumer not found (already closed?)", "consumer_id", req.PipeConsumerId)
	}
	m.servingMu.Unlock()

	m.servingDataMu.Lock()
	dataInfo, ok := m.servingDataConsumers[req.PipeConsumerId]
	if ok {
		delete(m.servingDataConsumers, req.PipeConsumerId)
		dataInfo.Consumer.Close()
		m.logger.Info("Closed serving data pipe consumer", "consumer_id", req.PipeConsumerId)
	}
	m.servingDataMu.Unlock()

	m.producersMu.Lock()
	if lp, ok := m.producers[req.StreamID]; ok {
		if lp.UpstreamNode == req.RequesterNode {
			delete(m.producers, req.StreamID)
			lp.Close()
			m.logger.Info("Closed pipe producer for stream", "stream_id", req.StreamID, "upstream", req.RequesterNode)
		}
	}
	m.producersMu.Unlock()
}

func (m *Manager) handlePipeProducerState(req PipeRequest, paused bool) PipeResponse {
	resp := PipeResponse{}
	m.producersMu.RLock()
	lp, ok := m.producers[req.StreamID]
	m.producersMu.RUnlock()
	if !ok {
		resp.Error = "Stream not found"
		return resp
	}
	if req.PipeConsumerId != "" && lp.UpstreamConsumerId != "" && lp.UpstreamConsumerId != req.PipeConsumerId {
		resp.Error = "Pipe consumer mismatch"
		return resp
	}
	var err error
	if req.StreamID.IsDataProducer() {
		if lp.DataProducer == nil {
			resp.Error = "DataProducer not found"
			return resp
		}
		if paused {
			err = lp.DataProducer.Pause()
		} else {
			err = lp.DataProducer.Resume()
		}
	} else {
		if lp.Producer == nil {
			resp.Error = "Producer not found"
			return resp
		}
		if paused {
			err = lp.Producer.Pause()
		} else {
			err = lp.Producer.Resume()
		}
	}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp
}

func (m *Manager) handleSignaling(msg *nats.Msg) {
	var req PipeRequest
	json.Unmarshal(msg.Data, &req)
	resp := PipeResponse{}

	switch req.Command {
	case "EstablishPipe":
		resp = m.handleEstablishPipe(req)

	case "EnsurePingChannel":
		resp = m.handleEnsurePingChannel(req)

	case "ClosePipe":
		m.handleClosePipe(req)

	case "PausePipeProducer":
		resp = m.handlePipeProducerState(req, true)

	case "ResumePipeProducer":
		resp = m.handlePipeProducerState(req, false)
	case "CleanupPipeTransports":
		resp = m.handlePipeCleanup(req)
	}

	data, _ := json.Marshal(resp)
	msg.Respond(data)
}

// disconnectPipe is the FIB DisconnectPipe callback. It closes the local
// pipe producer for streamID (so the pipe transport is freed) when the FIB
// entry is being torn down.
func (m *Manager) disconnectPipe(streamID types.StreamID, pipeID string, upstreamNode types.NodeID) error {
	m.producersMu.Lock()
	defer m.producersMu.Unlock()

	lp, ok := m.producers[streamID]
	if ok {
		if lp.UpstreamConsumerId != pipeID {
			m.logger.Warn("disconnectPipe mismatch, skipping", "stream_id", streamID, "expected", pipeID, "found", lp.UpstreamConsumerId)
			return nil
		}

		delete(m.producers, streamID)

		if lp.Producer != nil {
			lp.Producer.Close()
		}
		if lp.DataProducer != nil {
			lp.DataProducer.Close()
		}
		m.logger.Info("Disconnected pipe", "stream_id", streamID, "pipe_id", pipeID)
	}

	return nil
}

func (m *Manager) cleanupWorkerResources(deadRouterID string) {
	m.logger.Info("Cleaning up worker resources", "router_id", deadRouterID)

	m.producersMu.Lock()
	affectedStreams := make([]types.StreamID, 0)
	for streamID, lp := range m.producers {
		if lp.Router.Id() == deadRouterID {
			delete(m.producers, streamID)
			affectedStreams = append(affectedStreams, streamID)
			m.logger.Info("Cleaned up producer", "stream_id", streamID)
		}
	}
	m.producersMu.Unlock()

	for _, streamID := range affectedStreams {
		m.unadvertise(streamID)
		if m.OnStreamClosed != nil {
			go m.OnStreamClosed(streamID)
		}
	}

	// 2. Clean Pipe Transports
	m.pipeMu.Lock()
	remoteNodes := make(map[types.NodeID]struct{})
	if pipes, ok := m.pipeTransports[deadRouterID]; ok {
		for _, pipe := range pipes {
			if node, ok := pipe.AppData()["remoteNode"].(types.NodeID); ok {
				remoteNodes[node] = struct{}{}
			}
		}
	}
	delete(m.pipeTransports, deadRouterID)
	m.pipeMu.Unlock()
	for remoteNode := range remoteNodes {
		go m.notifyPipeCleanup(remoteNode, deadRouterID)
	}

	// 3. Clean Direct Transports and Ping Channels
	m.directMu.Lock()
	delete(m.directTransports, deadRouterID)
	m.directMu.Unlock()

	m.pingMu.Lock()
	for node, ch := range m.pingChannels {
		ch.mu.Lock()
		routerId := ch.routerId
		ch.mu.Unlock()
		if routerId == deadRouterID {
			if ch.producer != nil {
				ch.producer.Close()
			}
			if ch.consumer != nil {
				ch.consumer.Close()
			}
			if ch.pipeProducer != nil {
				ch.pipeProducer.Close()
			}
			if ch.pipeConsumer != nil {
				ch.pipeConsumer.Close()
			}
			delete(m.pingChannels, node)
		}
	}
	m.pingMu.Unlock()

	// 4. Clean serving Consumers
	m.servingMu.Lock()
	for id, info := range m.servingConsumers {
		if info.StreamID.RouterID() == deadRouterID {
			delete(m.servingConsumers, id)
			info.Consumer.Close()
			m.logger.Info("Cleaned up serving consumer for dead router", "consumer_id", id, "router_id", deadRouterID)
		}
	}
	m.servingMu.Unlock()

	// 5. Clean serving Data Consumers
	m.servingDataMu.Lock()
	for id, info := range m.servingDataConsumers {
		if info.StreamID.RouterID() == deadRouterID {
			delete(m.servingDataConsumers, id)
			info.Consumer.Close()
			m.logger.Info("Cleaned up serving data consumer for dead router", "consumer_id", id, "router_id", deadRouterID)
		}
	}
	m.servingDataMu.Unlock()
}
