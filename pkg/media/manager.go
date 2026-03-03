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

	"github.com/jiyeyuran/omnistream/pkg/types"

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
	nodeID types.NodeID
	nc     *nats.Conn
	logger *slog.Logger

	workerBin   string
	workerCount int

	workers  []*mediasoup.Worker
	routers  []*mediasoup.Router
	workerMu sync.RWMutex

	// Pipe Reuse: map[LocalRouterID]map[RemoteRouterID]*pipeTransport
	pipeTransports map[string]map[string]*pipeTransport
	pipeMu         sync.RWMutex
	// Direct Reuse: map[RouterID]*mediasoup.Transport
	directTransports map[string]*mediasoup.Transport
	directMu         sync.Mutex

	// Local Producers: StreamID -> {RouterIndex, Producer}
	producers   map[types.StreamID]*LocalProducer
	producersMu sync.RWMutex

	// Clients: ClientID -> Context
	clients   map[string]*ClientContext
	clientsMu sync.RWMutex

	// Singleflight
	sf singleflight.Group

	// Serving Remote Consumers (Pipe): ConsumerID -> Info
	servingConsumers     map[string]*ServingConsumerInfo
	servingMu            sync.Mutex
	servingDataConsumers map[string]*ServingDataConsumerInfo
	servingDataMu        sync.Mutex

	pingChannels map[types.NodeID]*PingChannel
	pingMu       sync.Mutex

	closed atomic.Bool

	OnStreamClosed func(streamID types.StreamID)
}

type ManagerOption func(*Manager)

func WithWorkerCount(count int) ManagerOption {
	return func(m *Manager) {
		m.workerCount = count
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

type ClientContext struct {
	ClientID  string
	Transport *mediasoup.Transport
	Router    *mediasoup.Router
	Consumers map[types.StreamID]*mediasoup.Consumer
	Mu        sync.Mutex
}

type LocalProducer struct {
	StreamID           types.StreamID
	RouterIdx          int
	Producer           *mediasoup.Producer
	DataProducer       *mediasoup.DataProducer
	RouterId           string
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

func NewManager(nodeID types.NodeID, nc *nats.Conn, workerBin string, opts ...ManagerOption) *Manager {
	m := &Manager{
		nodeID:               nodeID,
		nc:                   nc,
		workerBin:            workerBin,
		logger:               slog.Default().With("component", "MediaManager"),
		pipeTransports:       make(map[string]map[string]*pipeTransport),
		producers:            make(map[types.StreamID]*LocalProducer),
		clients:              make(map[string]*ClientContext),
		servingConsumers:     make(map[string]*ServingConsumerInfo),
		servingDataConsumers: make(map[string]*ServingDataConsumerInfo),
		pingChannels:         make(map[types.NodeID]*PingChannel),
		directTransports:     make(map[string]*mediasoup.Transport),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *Manager) Start() error {
	numWorkers := m.workerCount
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	m.logger.Info("Starting mediasoup workers", "count", numWorkers, "bin", m.workerBin)

	m.workers = make([]*mediasoup.Worker, numWorkers)
	m.routers = make([]*mediasoup.Router, numWorkers)

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

	// 1. Cleanup all streams (Unadvertise)
	m.producersMu.Lock()
	for streamID := range m.producers {
		if m.OnStreamClosed != nil {
			m.OnStreamClosed(streamID)
		}
	}
	m.producersMu.Unlock()

	m.workerMu.Lock()
	defer m.workerMu.Unlock()

	for _, worker := range m.workers {
		if worker != nil {
			worker.Close()
		}
	}
}

func (m *Manager) launchWorker(index int) error {
	m.logger.Info("Launching worker", "index", index)
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

	m.workerMu.Lock()
	m.workers[index] = worker
	m.routers[index] = router
	m.workerMu.Unlock()

	m.logger.Info("Worker started", "index", index, "pid", worker.Pid())

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
		if err := m.launchWorker(index); err == nil {
			m.logger.Info("Worker restarted", "index", index)
			return
		}
		m.logger.Error("Restart failed, retrying...", "index", index)
	}
}

// Publish simulates a client publishing a stream
func (m *Manager) Publish() (types.StreamID, error) {
	idx := int(time.Now().UnixNano() % int64(len(m.routers)))
	if idx < 0 {
		idx = -idx
	}
	router := m.routers[idx]

	transport, err := router.CreatePlainTransport(&mediasoup.PlainTransportOptions{
		ListenInfo: mediasoup.TransportListenInfo{
			Ip:       "127.0.0.1",
			Protocol: mediasoup.TransportProtocolUDP,
		},
	})
	if err != nil {
		return "", err
	}

	producer, err := transport.Produce(&mediasoup.ProducerOptions{
		RtpParameters: &mediasoup.RtpParameters{
			Mid: "VIDEO",
			Codecs: []*mediasoup.RtpCodecParameters{
				{
					MimeType:    "video/h264",
					PayloadType: 112,
					ClockRate:   90000,
					Parameters: mediasoup.RtpCodecSpecificParameters{
						PacketizationMode: 1,
						ProfileLevelId:    "4d0032",
					},
					RtcpFeedback: []*mediasoup.RtcpFeedback{
						{Type: "nack", Parameter: ""},
						{Type: "nack", Parameter: "pli"},
						{Type: "goog-remb", Parameter: ""},
					},
				},
				{
					MimeType:    "video/rtx",
					PayloadType: 113,
					ClockRate:   90000,
					Parameters:  mediasoup.RtpCodecSpecificParameters{Apt: 112},
				},
			},
			HeaderExtensions: []*mediasoup.RtpHeaderExtensionParameters{
				{
					Uri: "urn:ietf:params:rtp-hdrext:sdes:mid",
					Id:  10,
				},
				{
					Uri: "urn:3gpp:video-orientation",
					Id:  13,
				},
			},
			Encodings: []*mediasoup.RtpEncodingParameters{
				{Ssrc: 22222222, Rtx: &mediasoup.RtpEncodingRtx{Ssrc: 22222223}, ScalabilityMode: "L1T5"},
				{Ssrc: 22222224, Rtx: &mediasoup.RtpEncodingRtx{Ssrc: 22222225}, ScalabilityMode: "L1T5"},
				{Ssrc: 22222226, Rtx: &mediasoup.RtpEncodingRtx{Ssrc: 22222227}, ScalabilityMode: "L1T5"},
				{Ssrc: 22222228, Rtx: &mediasoup.RtpEncodingRtx{Ssrc: 22222229}, ScalabilityMode: "L1T5"},
			},
			Rtcp: &mediasoup.RtcpParameters{
				Cname: "video-1",
			},
		},
	})
	if err != nil {
		return "", err
	}

	streamID := types.NewStreamID(router.Id(), producer.Id())

	m.producersMu.Lock()
	m.producers[streamID] = &LocalProducer{
		StreamID:     streamID,
		RouterIdx:    idx,
		Producer:     producer,
		RouterId:     router.Id(),
		UpstreamNode: m.nodeID,
	}
	m.producersMu.Unlock()

	m.logger.Info("Published stream", "stream_id", streamID, "router_idx", idx)
	return streamID, nil
}

func (m *Manager) PublishData() (types.StreamID, error) {
	idx := int(time.Now().UnixNano() % int64(len(m.routers)))
	if idx < 0 {
		idx = -idx
	}
	router := m.routers[idx]

	direct, err := m.getOrCreateDirectTransport(router)
	if err != nil {
		return "", err
	}
	streamId := uint16(time.Now().UnixNano() % 65535)
	dataProducer, err := direct.ProduceData(&mediasoup.DataProducerOptions{
		Label: "data",
		SctpStreamParameters: &mediasoup.SctpStreamParameters{
			StreamId: streamId,
		},
	})
	if err != nil {
		return "", err
	}

	streamID := types.NewStreamID(router.Id(), dataProducer.Id())
	m.producersMu.Lock()
	m.producers[streamID] = &LocalProducer{
		StreamID:     streamID,
		RouterIdx:    idx,
		DataProducer: dataProducer,
		RouterId:     router.Id(),
		UpstreamNode: m.nodeID,
	}
	m.producersMu.Unlock()

	m.logger.Info("Published data stream", "stream_id", streamID, "router_idx", idx)
	return streamID, nil
}

// ConnectPipe (FIB Callback)
func (m *Manager) ConnectPipe(streamID types.StreamID, upstreamNode types.NodeID) (string, error) {
	// Optimization: If upstream is local
	if upstreamNode == m.nodeID {
		m.producersMu.RLock()
		lp, ok := m.producers[streamID]
		m.producersMu.RUnlock()

		if !ok {
			return "", fmt.Errorf("stream not found locally")
		}

		// Return original producer ID. Consume will handle routing.
		m.logger.Info("Stream is local", "stream_id", streamID, "router_id", lp.RouterId)
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
				RouterIdx:          idx,
				DataProducer:       dataProducer,
				RouterId:           localRouter.Id(),
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
			RouterIdx:          idx,
			Producer:           pipeProducer,
			RouterId:           localRouter.Id(),
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

	router := m.routers[int(crc32.ChecksumIEEE([]byte(req.RequesterNode)))%len(m.routers)]
	if lp != nil {
		router = m.routers[lp.RouterIdx]
	}

	pipe, err := m.getOrCreateLocalPipe(router, req.RequesterRouterId, req.RequesterNode)
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
	if len(requesterRouterId) == 0 {
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

// Consume simulates a client consuming the stream
func (m *Manager) Consume(clientID string, streamID types.StreamID) (*mediasoup.Consumer, error) {
	// Lookup Router from local state
	m.producersMu.RLock()
	lp, ok := m.producers[streamID]
	m.producersMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("stream %s not found locally", streamID)
	}

	// Get/Create Client Context
	m.clientsMu.Lock()
	clientCtx, ok := m.clients[clientID]
	if !ok {
		clientCtx = &ClientContext{
			ClientID:  clientID,
			Consumers: make(map[types.StreamID]*mediasoup.Consumer),
		}
		m.clients[clientID] = clientCtx
	}
	m.clientsMu.Unlock()

	clientCtx.Mu.Lock()
	defer clientCtx.Mu.Unlock()

	var clientRouter *mediasoup.Router

	// Reuse or Create Transport
	if clientCtx.Transport == nil {
		// Select Router based on ClientID for Affinity
		idx := int(crc32.ChecksumIEEE([]byte(clientID))) % len(m.routers)
		clientRouter = m.routers[idx]
		clientCtx.Router = clientRouter

		t, err := clientRouter.CreatePlainTransport(&mediasoup.PlainTransportOptions{
			ListenInfo: mediasoup.TransportListenInfo{
				Ip:       "127.0.0.1",
				Protocol: mediasoup.TransportProtocolUDP,
			},
		})
		if err != nil {
			return nil, err
		}
		clientCtx.Transport = t
		m.logger.Info("Created new transport for client", "client_id", clientID, "router_idx", idx)
	} else {
		clientRouter = clientCtx.Router
	}

	// Check if already consumed (Idempotency)
	if consumer, exists := clientCtx.Consumers[streamID]; exists {
		m.logger.Info("Client already consuming stream, returning existing consumer", "client_id", clientID, "stream_id", streamID)
		return consumer, nil
	}

	// Check Cross-Router Pipe
	producerIDToConsume := lp.Id()
	if lp.RouterId != clientRouter.Id() {
		sourceRouter := m.routers[lp.RouterIdx]
		m.logger.Info("Piping cross-router for client", "from", sourceRouter.Id(), "to", clientRouter.Id())

		res, err := sourceRouter.PipeToRouter(&mediasoup.PipeToRouterOptions{
			ProducerId: lp.Id(),
			Router:     clientRouter,
			ListenInfo: mediasoup.TransportListenInfo{Ip: "127.0.0.1", Protocol: mediasoup.TransportProtocolUDP},
		})
		if err != nil {
			return nil, err
		}
		producerIDToConsume = res.PipeProducer.Id()
	}

	// Consume
	consumer, err := clientCtx.Transport.Consume(&mediasoup.ConsumerOptions{
		ProducerId:      producerIDToConsume,
		RtpCapabilities: consumerDeviceCapabilities,
	})

	if err != nil {
		return nil, err
	}

	clientCtx.Consumers[streamID] = consumer
	m.logger.Info("Client Consumed Stream", "client_id", clientID, "stream_id", streamID, "consumer_id", consumer.Id())
	return consumer, nil
}

func (m *Manager) Unpublish(streamID types.StreamID) error {
	m.producersMu.Lock()
	if lp, ok := m.producers[streamID]; ok {
		delete(m.producers, streamID)
		if lp.Producer != nil {
			lp.Producer.Close()
		}
		if lp.DataProducer != nil {
			lp.DataProducer.Close()
		}
		m.logger.Info("Unpublished stream", "stream_id", streamID)
	}
	m.producersMu.Unlock()

	// Clean up serving consumers (Downstreams)
	m.servingMu.Lock()
	for id, info := range m.servingConsumers {
		if info.StreamID == streamID {
			delete(m.servingConsumers, id)
			info.Consumer.Close()
			m.logger.Info("Closing serving consumer for unpublished stream", "consumer_id", id, "remote_node", info.RemoteNode)
		}
	}
	m.servingMu.Unlock()

	m.servingDataMu.Lock()
	for id, info := range m.servingDataConsumers {
		if info.StreamID == streamID {
			delete(m.servingDataConsumers, id)
			info.Consumer.Close()
			m.logger.Info("Closing serving data consumer for unpublished stream", "consumer_id", id, "remote_node", info.RemoteNode)
		}
	}
	m.servingDataMu.Unlock()

	return nil
}

func (m *Manager) DisconnectPipe(streamID types.StreamID, pipeID string, upstreamNode types.NodeID) error {
	m.producersMu.Lock()
	defer m.producersMu.Unlock()

	lp, ok := m.producers[streamID]
	if ok {
		// Verify ID matches to avoid deleting wrong producer
		if lp.UpstreamConsumerId != pipeID {
			m.logger.Warn("DisconnectPipe mismatch, skipping", "stream_id", streamID, "expected", pipeID, "found", lp.UpstreamConsumerId)
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

func (m *Manager) Unsubscribe(clientID string, streamID types.StreamID) error {
	m.clientsMu.RLock()
	clientCtx, ok := m.clients[clientID]
	m.clientsMu.RUnlock()

	if !ok {
		return nil
	}

	clientCtx.Mu.Lock()
	defer clientCtx.Mu.Unlock()

	if consumer, ok := clientCtx.Consumers[streamID]; ok {
		consumer.Close()
		delete(clientCtx.Consumers, streamID)
		m.logger.Info("Unsubscribed stream", "client_id", clientID, "stream_id", streamID)
	}
	return nil
}

func (m *Manager) cleanupWorkerResources(deadRouterID string) {
	m.logger.Info("Cleaning up worker resources", "router_id", deadRouterID)

	// 1. Clean Producers
	m.producersMu.Lock()
	for streamID, lp := range m.producers {
		if lp.RouterId == deadRouterID {
			delete(m.producers, streamID)
			if m.OnStreamClosed != nil {
				go m.OnStreamClosed(streamID)
			}
			m.logger.Info("Cleaned up producer", "stream_id", streamID)
		}
	}
	m.producersMu.Unlock()

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

	// 4. Clean Clients
	m.clientsMu.Lock()
	for clientID, ctx := range m.clients {
		if ctx.Router.Id() == deadRouterID {
			delete(m.clients, clientID)
			m.logger.Info("Cleaned up client", "client_id", clientID)
		}
	}
	m.clientsMu.Unlock()

	// 5. Clean serving Consumers
	m.servingMu.Lock()
	for id, info := range m.servingConsumers {
		if info.StreamID.RouterID() == deadRouterID {
			delete(m.servingConsumers, id)
			info.Consumer.Close()
			m.logger.Info("Cleaned up serving consumer for dead router", "consumer_id", id, "router_id", deadRouterID)
		}
	}
	m.servingMu.Unlock()

	// 6. Clean serving Data Consumers
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
