package media

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"rib-control/pkg/types"

	"github.com/jiyeyuran/mediasoup-go/v2"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/singleflight"
)

type Manager struct {
	nodeID    types.NodeID
	nc        *nats.Conn
	workerBin string
	logger    *slog.Logger

	workers  []*mediasoup.Worker
	routers  []*mediasoup.Router
	workerMu sync.RWMutex

	// Pipe Reuse: map[LocalRouterID]map[RemoteRouterID]*mediasoup.Transport
	pipeTransports map[string]map[string]*mediasoup.Transport
	pipeMu         sync.RWMutex

	// Local Producers: StreamID -> {RouterIndex, Producer}
	producers   map[types.StreamID]*LocalProducer
	producersMu sync.RWMutex

	// Clients: ClientID -> Context
	clients   map[string]*ClientContext
	clientsMu sync.RWMutex

	// Singleflight
	sf singleflight.Group

	// Serving Remote Consumers (Pipe): ConsumerID -> Consumer
	servingConsumers map[string]*mediasoup.Consumer
	servingMu        sync.Mutex

	closed    atomic.Bool
	cleanupWg sync.WaitGroup
}

type ClientContext struct {
	ClientID  string
	Transport *mediasoup.Transport
	Router    *mediasoup.Router
	Consumers map[types.StreamID]*mediasoup.Consumer
	Mu        sync.Mutex
}

type LocalProducer struct {
	StreamID     types.StreamID
	RouterIdx    int
	Producer     *mediasoup.Producer
	PipeConsumer *mediasoup.Consumer
	RouterId     string
	UpstreamNode types.NodeID
}

func (lp *LocalProducer) Id() string {
	if lp.Producer != nil {
		return lp.Producer.Id()
	}
	if lp.PipeConsumer != nil {
		return lp.PipeConsumer.Id()
	}
	return ""
}

func NewManager(nodeID types.NodeID, nc *nats.Conn, workerBin string) *Manager {
	return &Manager{
		nodeID:           nodeID,
		nc:               nc,
		workerBin:        workerBin,
		logger:           slog.Default().With("component", "MediaManager"),
		pipeTransports:   make(map[string]map[string]*mediasoup.Transport),
		producers:        make(map[types.StreamID]*LocalProducer),
		clients:          make(map[string]*ClientContext),
		servingConsumers: make(map[string]*mediasoup.Consumer),
	}
}

func (m *Manager) Start() error {
	numWorkers := runtime.NumCPU()
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

	m.workerMu.Lock()
	defer m.workerMu.Unlock()

	for _, worker := range m.workers {
		if worker != nil {
			worker.Close()
		}
	}
	m.logger.Info("Waiting for cleanup requests...")
	m.cleanupWg.Wait()
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
		MediaCodecs: []*mediasoup.RtpCodecCapability{
			{
				Kind:      mediasoup.MediaKindAudio,
				MimeType:  "audio/opus",
				ClockRate: 48000,
				Channels:  2,
			},
			{
				Kind:      mediasoup.MediaKindVideo,
				MimeType:  "video/VP8",
				ClockRate: 90000,
			},
			{
				Kind:      mediasoup.MediaKindVideo,
				MimeType:  "video/H264",
				ClockRate: 90000,
				Parameters: mediasoup.RtpCodecSpecificParameters{
					LevelAsymmetryAllowed: 1,
					PacketizationMode:     1,
					ProfileLevelId:        "4d0032",
				},
			},
		},
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
		m.logger.Error("Worker died, cleaning up and restarting...", "index", index, "pid", worker.Pid())
		m.cleanupWorkerResources(router)
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
func (m *Manager) Publish(streamID types.StreamID) error {
	idx := int(len(streamID) % len(m.routers))
	router := m.routers[idx]

	transport, err := router.CreatePlainTransport(&mediasoup.PlainTransportOptions{
		ListenInfo: mediasoup.TransportListenInfo{
			Ip:       "127.0.0.1",
			Protocol: mediasoup.TransportProtocolUDP,
		},
	})
	if err != nil {
		return err
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
		return err
	}

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
	return nil
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

		// 1. Locate Stream on Remote
		locateResp, err := m.request(upstreamNode, PipeRequest{
			Command:  "Locate",
			StreamID: streamID,
		})
		if err != nil {
			return nil, fmt.Errorf("locate stream failed: %v", err)
		}
		remoteRouterId := locateResp.OriginRouterId

		// 2. Load Balance Local Router
		idx := int(crc32.ChecksumIEEE([]byte(streamID))) % len(m.routers)
		localRouter := m.routers[idx]

		// 3. Get/Create Pipe (Reuse if possible)
		pipe, isNew, err := m.getOrCreateLocalPipe(localRouter, remoteRouterId, upstreamNode)
		if err != nil {
			return nil, err
		}

		// Extract Info from Pipe
		dump, err := pipe.Dump()
		if err != nil {
			if isNew {
				pipe.Close()
			}
			return nil, err
		}
		// Assuming it's a PipeTransport, PipeTransportDump should be present
		if dump.PipeTransportDump == nil {
			if isNew {
				pipe.Close()
			}
			return nil, fmt.Errorf("transport dump missing PipeTransport data")
		}
		tuple := dump.PipeTransportDump.Tuple

		// 4. Prepare Request
		myInfo := PipeInfo{
			Ip:             tuple.LocalAddress,
			Port:           tuple.LocalPort,
			SrtpParameters: dump.PipeTransportDump.SrtpParameters,
		}
		req := PipeRequest{
			Command:           "EstablishPipe",
			StreamID:          streamID,
			RequesterNode:     m.nodeID,
			RequesterRouterId: localRouter.Id(),
			PipeInfo:          myInfo,
		}

		// 5. Signaling
		resp, err := m.request(upstreamNode, req)
		if err != nil {
			if isNew {
				pipe.Close()
			}
			return nil, err
		}

		// 6. Connect (Only if New)
		if isNew {
			err = pipe.Connect(&mediasoup.TransportConnectOptions{
				Ip:             resp.PipeInfo.Ip,
				Port:           &resp.PipeInfo.Port,
				SrtpParameters: resp.PipeInfo.SrtpParameters,
			})
			if err != nil {
				pipe.Close()
				return nil, fmt.Errorf("pipe connect failed: %v", err)
			}
			m.logger.Info("Pipe Connected", "upstream", upstreamNode)
		}

		// 7. Produce (Receive from remote)
		pipeProducer, err := pipe.Produce(&mediasoup.ProducerOptions{
			Id:            resp.PipeProducerId,
			Kind:          mediasoup.MediaKindVideo,
			RtpParameters: &resp.RtpParameters, // resp.RtpParameters is struct, need pointer
		})
		if err != nil {
			m.logger.Error("Pipe Produce failed", "error", err)
			return nil, err
		}

		m.producersMu.Lock()
		m.producers[streamID] = &LocalProducer{
			StreamID:     streamID,
			RouterIdx:    idx,
			Producer:     pipeProducer,
			RouterId:     localRouter.Id(),
			UpstreamNode: upstreamNode,
		}
		m.producersMu.Unlock()

		m.logger.Info("Pipe Stream Ready", "stream_id", streamID, "pipe_producer_id", pipeProducer.Id())
		return pipeProducer.Id(), nil
	})

	if err != nil {
		return "", err
	}
	return val.(string), nil
}

func (m *Manager) getOrCreateLocalPipe(localRouter *mediasoup.Router, remoteRouterId string, remoteNode types.NodeID) (*mediasoup.Transport, bool, error) {
	if remoteRouterId == "" {
		return nil, false, fmt.Errorf("remoteRouterId is required")
	}

	m.pipeMu.Lock()
	defer m.pipeMu.Unlock()

	if _, ok := m.pipeTransports[localRouter.Id()]; !ok {
		m.pipeTransports[localRouter.Id()] = make(map[string]*mediasoup.Transport)
	}

	if pipe, ok := m.pipeTransports[localRouter.Id()][remoteRouterId]; ok {
		return pipe, false, nil
	}

	// Create New
	pipe, err := localRouter.CreatePipeTransport(&mediasoup.PipeTransportOptions{
		ListenInfo: mediasoup.TransportListenInfo{
			Ip:       "127.0.0.1",
			Protocol: mediasoup.TransportProtocolUDP,
		},
		EnableSrtp: true,
		AppData: map[string]interface{}{
			"remoteNode": remoteNode,
		},
	})
	if err != nil {
		return nil, false, err
	}

	m.pipeTransports[localRouter.Id()][remoteRouterId] = pipe

	// Cleanup on close
	pipe.OnClose(func(ctx context.Context) {
		m.pipeMu.Lock()
		defer m.pipeMu.Unlock()
		if pipes, ok := m.pipeTransports[localRouter.Id()]; ok {
			delete(pipes, remoteRouterId)
			m.logger.Info("Pipe transport closed and removed", "router_id", localRouter.Id(), "remote_router", remoteRouterId)
		}
	})

	return pipe, true, nil
}

// CleanUpNode closes all pipe transports to a specific remote node
func (m *Manager) CleanUpNode(remoteNode types.NodeID) {
	m.pipeMu.Lock()
	var pipesToClose []*mediasoup.Transport

	for _, pipes := range m.pipeTransports {
		for remoteRouterId, pipe := range pipes {
			// Check AppData for remoteNode
			if node, ok := pipe.AppData()["remoteNode"].(types.NodeID); ok && node == remoteNode {
				pipesToClose = append(pipesToClose, pipe)
				delete(pipes, remoteRouterId)
			}
		}
	}
	m.pipeMu.Unlock()

	// Close outside lock to avoid deadlock with OnClose handler
	for _, pipe := range pipesToClose {
		pipe.Close()
		m.logger.Info("Force closed pipe to remote node", "remote_node", remoteNode, "pipe_id", pipe.Id())
	}
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

func (m *Manager) handleSignaling(msg *nats.Msg) {
	var req PipeRequest
	json.Unmarshal(msg.Data, &req)
	resp := PipeResponse{}

	switch req.Command {
	case "Locate":
		m.producersMu.RLock()
		lp, ok := m.producers[req.StreamID]
		m.producersMu.RUnlock()

		if !ok {
			resp.Error = "Stream not found"
			break
		}
		resp.OriginRouterId = m.routers[lp.RouterIdx].Id()

	case "EstablishPipe":
		// 1. Find Stream
		m.producersMu.RLock()
		lp, ok := m.producers[req.StreamID]
		m.producersMu.RUnlock()

		if !ok {
			resp.Error = "Stream not found"
			break
		}

		router := m.routers[lp.RouterIdx]

		// 2. Get/Create Pipe to Requester
		pipe, isNew, err := m.getOrCreateLocalPipe(router, req.RequesterRouterId, req.RequesterNode)
		if err != nil {
			resp.Error = err.Error()
			break
		}

		// 3. Connect if new
		if isNew {
			err = pipe.Connect(&mediasoup.TransportConnectOptions{
				Ip:             req.PipeInfo.Ip,
				Port:           &req.PipeInfo.Port,
				SrtpParameters: req.PipeInfo.SrtpParameters,
			})
			if err != nil {
				pipe.Close()
				resp.Error = "Connect failed: " + err.Error()
				break
			}
		}

		// 4. Consume (Send to Remote)
		consumer, err := pipe.Consume(&mediasoup.ConsumerOptions{
			ProducerId: lp.Producer.Id(),
		})
		if err != nil {
			m.logger.Error("Pipe Consume failed", "error", err)
			resp.Error = "Consume failed: " + err.Error()
			break
		}

		// Track Serving Consumer
		m.servingMu.Lock()
		m.servingConsumers[consumer.Id()] = consumer
		m.servingMu.Unlock()

		consumer.OnClose(func(context.Context) {
			m.servingMu.Lock()
			delete(m.servingConsumers, consumer.Id())
			m.servingMu.Unlock()
		})

		// Get Pipe Info for Response
		dump, err := pipe.Dump()
		if err != nil || dump.PipeTransportDump == nil {
			resp.Error = "Failed to dump pipe info"
			break
		}
		tuple := dump.PipeTransportDump.Tuple

		resp.PipeProducerId = consumer.Id()
		resp.RtpParameters = *consumer.RtpParameters()

		// Fill Response
		resp.PipeInfo = PipeInfo{
			Ip:             tuple.LocalAddress,
			Port:           tuple.LocalPort,
			SrtpParameters: dump.PipeTransportDump.SrtpParameters,
		}
		resp.OriginRouterId = router.Id()

	case "ClosePipe":
		m.servingMu.Lock()
		consumer, ok := m.servingConsumers[req.PipeConsumerId]
		m.servingMu.Unlock()
		if ok {
			consumer.Close()
			m.logger.Info("Closed serving pipe consumer", "consumer_id", req.PipeConsumerId)
		} else {
			m.logger.Warn("ClosePipe: consumer not found", "consumer_id", req.PipeConsumerId)
		}
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
		ProducerId: producerIDToConsume,
		RtpCapabilities: &mediasoup.RtpCapabilities{
			Codecs: []*mediasoup.RtpCodecCapability{
				{
					Kind:      mediasoup.MediaKindVideo,
					MimeType:  "video/VP8",
					ClockRate: 90000,
				},
				{
					Kind:      mediasoup.MediaKindVideo,
					MimeType:  "video/H264",
					ClockRate: 90000,
				},
			},
		},
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
	defer m.producersMu.Unlock()

	if lp, ok := m.producers[streamID]; ok {
		if lp.Producer != nil {
			lp.Producer.Close()
		}
		if lp.PipeConsumer != nil {
			lp.PipeConsumer.Close()
		}
		delete(m.producers, streamID)
		m.logger.Info("Unpublished stream", "stream_id", streamID)
	}
	return nil
}

func (m *Manager) DisconnectPipe(streamID types.StreamID, pipeID string, upstreamNode types.NodeID) error {
	m.producersMu.Lock()
	defer m.producersMu.Unlock()

	lp, ok := m.producers[streamID]
	if ok {
		// Verify ID matches to avoid deleting wrong producer
		if lp.Id() != pipeID {
			m.logger.Warn("DisconnectPipe mismatch, skipping", "stream_id", streamID, "expected", pipeID, "found", lp.Id())
			return nil
		}

		if lp.Producer != nil {
			lp.Producer.Close()
		}
		if lp.PipeConsumer != nil {
			lp.PipeConsumer.Close()
		}
		delete(m.producers, streamID)
		m.logger.Info("Disconnected pipe", "stream_id", streamID, "pipe_id", pipeID)
	}

	// Signaling Remote
	if upstreamNode != m.nodeID {
		req := PipeRequest{
			Command:        "ClosePipe",
			StreamID:       streamID,
			RequesterNode:  m.nodeID,
			PipeConsumerId: pipeID,
		}
		// Send async to avoid blocking lock/caller
		go func() {
			if _, err := m.request(upstreamNode, req); err != nil {
				m.logger.Warn("Failed to send ClosePipe signaling", "upstream", upstreamNode, "error", err)
			}
		}()
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

func (m *Manager) cleanupWorkerResources(deadRouter *mediasoup.Router) {
	deadRouterID := deadRouter.Id()
	m.logger.Info("Cleaning up worker resources", "router_id", deadRouterID)

	// 1. Clean Producers
	m.producersMu.Lock()
	for streamID, lp := range m.producers {
		if lp.RouterId == deadRouterID {
			// Notify Remote if it's a Pipe
			if lp.UpstreamNode != "" && lp.UpstreamNode != m.nodeID {
				req := PipeRequest{
					Command:        "ClosePipe",
					StreamID:       streamID,
					RequesterNode:  m.nodeID,
					PipeConsumerId: lp.Id(),
				}
				m.cleanupWg.Add(1)
				go func(node types.NodeID, r PipeRequest) {
					defer m.cleanupWg.Done()
					m.request(node, r)
				}(lp.UpstreamNode, req)
			}

			delete(m.producers, streamID)
			m.logger.Info("Cleaned up producer", "stream_id", streamID)
		}
	}
	m.producersMu.Unlock()

	// 2. Clean Pipe Transports
	m.pipeMu.Lock()
	delete(m.pipeTransports, deadRouterID)
	m.pipeMu.Unlock()

	// 3. Clean Clients
	m.clientsMu.Lock()
	for clientID, ctx := range m.clients {
		if ctx.Router.Id() == deadRouterID {
			delete(m.clients, clientID)
			m.logger.Info("Cleaned up client", "client_id", clientID)
		}
	}
	m.clientsMu.Unlock()

	// 4. Serving Consumers: Cleaned up by their own OnClose handlers
}
