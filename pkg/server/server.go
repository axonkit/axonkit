package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/axonkit/axonkit/pkg/auth"
	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/discovery"
	"github.com/axonkit/axonkit/pkg/media"
	"github.com/axonkit/axonkit/pkg/routing"
	"github.com/axonkit/axonkit/pkg/rtc"
	"github.com/axonkit/axonkit/pkg/store"
	"github.com/axonkit/axonkit/pkg/types"

	nserver "github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Server runs one axonkit process: NATS/JetStream, routing, media workers, room
// manager, and the HTTP + NATS RPC API.
type Server struct {
	cfg    *config.Config
	myNode types.NodeInfo

	embeddedNats *nserver.Server
	nc           *nats.Conn
	rpcSub       *nats.Subscription

	rib         *routing.RIB
	linkDB      *routing.LinkDB
	fib         *routing.FIB
	mediaMgr    *media.Manager
	registry    *discovery.Registry
	roomManager *rtc.RoomManager
	nodeRPC     *NodeRPCService
	httpAPI     *HTTPServer
	httpServer  *http.Server

	probeCancel context.CancelFunc
}

// NewServer wires NATS, stores, routing, media, discovery, and RTC. It does not
// start background loops or HTTP; call Start for that.
func NewServer(cfg *config.Config) (*Server, error) {
	if cfg == nil {
		return nil, errors.New("config is nil")
	}

	myNode := types.NodeInfo{
		ID:      types.NodeID(cfg.Node.ID),
		Region:  types.RegionID(cfg.Node.Region),
		Cluster: types.ClusterID(cfg.Node.Cluster),
	}

	n := &Server{cfg: cfg, myNode: myNode}

	var natsOpts []nats.Option
	if cfg.Nats.URL == "" {
		slog.Info("NATS URL is empty, using in-process NATS server")
		opts := natsserver.DefaultTestOptions
		opts.Port = -1
		opts.JetStream = true
		opts.StoreDir = "./.data"
		n.embeddedNats = natsserver.RunServer(&opts)
		natsOpts = append(natsOpts, nats.InProcessServer(n.embeddedNats))
	}

	nc, err := nats.Connect(cfg.Nats.URL, natsOpts...)
	if err != nil {
		if n.embeddedNats != nil {
			n.embeddedNats.Shutdown()
		}
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	n.nc = nc

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		if n.embeddedNats != nil {
			n.embeddedNats.Shutdown()
		}
		return nil, fmt.Errorf("jetstream: %w", err)
	}

	kv, err := store.NewNatsStore(context.Background(), js, routing.RibBucketName, routing.RIBTTL)
	if err != nil {
		n.closeDeps()
		return nil, fmt.Errorf("rib store: %w", err)
	}
	rib := routing.NewRIB(kv, myNode)
	linkDB := routing.NewLinkDB(myNode)
	finder := discovery.NewFinder(rib, linkDB, myNode)
	fib := routing.NewFIB(rib, myNode.ID)
	n.rib, n.linkDB, n.fib = rib, linkDB, fib

	mediaMgr := media.NewManager(myNode.ID, nc, cfg.Media.WorkerBin, cfg.Media.WebRtc)
	mediaMgr.OnStreamClosed = func(streamID types.StreamID) {
		if err := rib.StopAdvertise(streamID); err != nil {
			slog.Error("Failed to stop advertise", "stream_id", streamID, "error", err)
		}
	}
	if err := mediaMgr.Start(); err != nil {
		n.closeDeps()
		return nil, fmt.Errorf("media manager: %w", err)
	}
	n.mediaMgr = mediaMgr
	linkDB.SetRTTProvider(mediaMgr.ProbeRTT)

	fib.ConnectPipe = mediaMgr.ConnectPipe
	fib.DisconnectPipe = mediaMgr.DisconnectPipe

	nodeStore, err := store.NewNatsStore(context.Background(), js, discovery.NodeBucketName, discovery.NodeTTL)
	if err != nil {
		n.closeDeps()
		return nil, fmt.Errorf("node store: %w", err)
	}

	registry := discovery.NewRegistry(nodeStore, myNode, discovery.WithTTL(discovery.NodeTTL))
	if err := registry.Start(); err != nil {
		n.closeDeps()
		return nil, fmt.Errorf("registry: %w", err)
	}
	n.registry = registry

	if err := registry.WatchNodes(func(node types.NodeInfo, isDelete bool) {
		if isDelete {
			slog.Info("Node offline, cleaning up", "node_id", node.ID)
			mediaMgr.CleanUpNode(node.ID)
			linkDB.RemoveNode(node.ID)
		} else {
			slog.Debug("Node online", "node_id", node.ID, "cluster", node.Cluster)
			linkDB.AddNode(node)
		}
	}); err != nil {
		n.closeDeps()
		return nil, fmt.Errorf("node watcher: %w", err)
	}

	roomKV, err := store.NewNatsStore(context.Background(), js, "ROOM_STATE", rtc.RoomStateBucketTTL)
	if err != nil {
		n.closeDeps()
		return nil, fmt.Errorf("room state store: %w", err)
	}

	roomLeaseKV, err := store.NewNatsStore(context.Background(), js, rtc.RoomLeaseBucketName, rtc.RoomLeaseBucketTTL)
	if err != nil {
		n.closeDeps()
		return nil, fmt.Errorf("room lease store: %w", err)
	}

	roomManager := rtc.NewRoomManager(rtc.RoomManagerParams{
		Config:       cfg.Room,
		RIB:          rib,
		FIB:          fib,
		Finder:       finder,
		MediaManager: mediaMgr,
		KV:           roomKV,
		LeaseKV:      roomLeaseKV,
		NodeID:       string(myNode.ID),
	})
	n.roomManager = roomManager

	nodeRPC := NewNodeRPCService(nc, myNode.ID)
	n.nodeRPC = nodeRPC

	verifier := auth.NewTokenVerifier(cfg.Auth.APIKey, cfg.Auth.APISecret)
	httpAPI := NewHTTPServer(HTTPServerParams{
		Config:        cfg,
		RoomManager:   roomManager,
		Verifier:      verifier,
		Node:          myNode,
		NodeRPC:       nodeRPC,
		Routers:       mediaMgr.Routers(),
		WebRtcServers: mediaMgr.WebRtcServers(),
	})
	n.httpAPI = httpAPI
	registerDefaultNodeRPCHandlers(nodeRPC, httpAPI, roomManager)
	roomManager.SetRequestBackupCodec(n.ForwardRequestBackupCodec)

	rpcSub, err := nc.Subscribe(nodeRPCSubjectPrefix+string(myNode.ID), nodeRPC.Handle)
	if err != nil {
		n.closeDeps()
		return nil, fmt.Errorf("nats rpc subscribe: %w", err)
	}
	n.rpcSub = rpcSub

	mux := http.NewServeMux()
	httpAPI.RegisterRoutes(mux)
	n.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Node.Port),
		Handler: WithLogging(WithCORS(mux)),
	}

	return n, nil
}

// ForwardRequestBackupCodec asks the publisher's node to send backup-codec
// signaling to the real client. Used by rtc when the subscriber is remote.
func (n *Server) ForwardRequestBackupCodec(publisherSid, trackSid, publisherNodeId string) error {
	return ForwardRequestBackupCodecRPC(n.nodeRPC, publisherSid, trackSid, publisherNodeId)
}

func (n *Server) closeDeps() {
	if n.rpcSub != nil {
		_ = n.rpcSub.Unsubscribe()
		n.rpcSub = nil
	}
	if n.fib != nil {
		n.fib.Close()
		n.fib = nil
	}
	if n.rib != nil {
		n.rib.Close()
		n.rib = nil
	}
	if n.roomManager != nil {
		n.roomManager.Close()
		n.roomManager = nil
	}
	if n.mediaMgr != nil {
		n.mediaMgr.Close()
		n.mediaMgr = nil
	}
	if n.registry != nil {
		n.registry.Close()
		n.registry = nil
	}
	if n.nc != nil {
		n.nc.Close()
		n.nc = nil
	}
	if n.embeddedNats != nil {
		n.embeddedNats.Shutdown()
		n.embeddedNats = nil
	}
}

// Start begins LinkDB probing and the HTTP server in the background. Call Stop
// for graceful shutdown.
func (n *Server) Start(ctx context.Context) error {
	if n == nil {
		return errors.New("nil node")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	probeCtx, cancel := context.WithCancel(ctx)
	n.probeCancel = cancel
	n.linkDB.StartProbing(probeCtx)
	n.roomManager.Start(probeCtx)

	go func() {
		slog.Info("HTTP server listening", "addr", n.httpServer.Addr)
		if err := n.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server failed", "error", err)
		}
	}()
	return nil
}

// Stop shuts down HTTP, subscriptions, rooms, media, routing, and NATS.
func (n *Server) Stop(ctx context.Context) error {
	n.closeDeps()
	if n.probeCancel != nil {
		n.probeCancel()
		n.probeCancel = nil
	}
	if n.httpServer != nil {
		if err := n.httpServer.Shutdown(ctx); err != nil {
			return err
		}
	}
	return nil
}
