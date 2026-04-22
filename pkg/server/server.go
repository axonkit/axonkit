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
		opts.StoreDir = cfg.Nats.StoreDir
		if opts.StoreDir == "" {
			opts.StoreDir = "./.data"
		}
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
	n.rib, n.linkDB = rib, linkDB

	mediaMgr := media.NewManager(myNode.ID, nc, cfg.Media.WorkerBin, cfg.Media.WebRtc,
		media.WithRIB(rib, finder),
	)
	if err := mediaMgr.Start(); err != nil {
		n.closeDeps()
		return nil, fmt.Errorf("media manager: %w", err)
	}
	n.mediaMgr = mediaMgr
	linkDB.SetRTTProvider(mediaMgr.ProbeRTT)

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
		MediaManager: mediaMgr,
		KV:           roomKV,
		LeaseKV:      roomLeaseKV,
		NodeID:       string(myNode.ID),
		SharedRouter: cfg.Media.SharedRouter,
	})
	n.roomManager = roomManager

	nodeRPC := NewNodeRPCService(nc, myNode.ID)
	n.nodeRPC = nodeRPC

	// Provision the WHEP routing KV only if the operator opted into the
	// "kv" scheme. The "prefix" scheme is KV-free and costs nothing at
	// startup.
	var whepLocKV store.Store
	if cfg.Whip.WhepRouting == config.WhepRoutingKV {
		whepLocKV, err = store.NewNatsStore(context.Background(), js, whepLocBucket, whepLocTTL)
		if err != nil {
			n.closeDeps()
			return nil, fmt.Errorf("whep location store: %w", err)
		}
	}

	verifier := auth.NewTokenVerifier(cfg.Auth.APIKey, cfg.Auth.APISecret)
	httpAPI := NewHTTPServer(HTTPServerParams{
		Config:        cfg,
		RoomManager:   roomManager,
		Verifier:      verifier,
		Node:          myNode,
		NodeRPC:       nodeRPC,
		Routers:       mediaMgr.Routers(),
		WebRtcServers: mediaMgr.WebRtcServers(),
		WhepLocKV:     whepLocKV,
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
	// Shut down in reverse dependency order: consumers of the routing/media
	// layer first, then the data layers, then registry, then NATS. This in
	// particular guarantees mediaMgr.Close() can still reach the RIB to
	// un-advertise residual producers.
	if n.rpcSub != nil {
		_ = n.rpcSub.Unsubscribe()
		n.rpcSub = nil
	}
	if n.roomManager != nil {
		n.roomManager.Close()
		n.roomManager = nil
	}
	if n.mediaMgr != nil {
		n.mediaMgr.Close()
		n.mediaMgr = nil
	}
	if n.rib != nil {
		n.rib.Close()
		n.rib = nil
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

// Stop shuts down HTTP first (so no new joins arrive), then subscriptions,
// rooms (sending SERVER_SHUTDOWN disconnect to each participant and
// unpublishing their tracks), media workers, routing, and NATS.
func (n *Server) Stop(ctx context.Context) error {
	if n.probeCancel != nil {
		n.probeCancel()
		n.probeCancel = nil
	}
	var httpErr error
	if n.httpServer != nil {
		httpErr = n.httpServer.Shutdown(ctx)
	}
	n.closeDeps()
	return httpErr
}
