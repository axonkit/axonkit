package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/jiyeyuran/omnistream/pkg/auth"
	"github.com/jiyeyuran/omnistream/pkg/config"
	"github.com/jiyeyuran/omnistream/pkg/discovery"
	"github.com/jiyeyuran/omnistream/pkg/media"
	"github.com/jiyeyuran/omnistream/pkg/routing"
	"github.com/jiyeyuran/omnistream/pkg/rtc"
	"github.com/jiyeyuran/omnistream/pkg/server"
	"github.com/jiyeyuran/omnistream/pkg/store"
	"github.com/jiyeyuran/omnistream/pkg/types"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	nodeID    = flag.String("node", "node-1", "Node ID")
	regionID  = flag.String("region", "default-region", "Region ID")
	clusterID = flag.String("cluster", "default-cluster", "Cluster ID")
	natsURL   = flag.String("nats", nats.DefaultURL, "NATS URL")
	httpPort  = flag.Int("port", 8080, "HTTP Port")
	workerBin = flag.String("worker", "mediasoup-worker", "Path to mediasoup-worker binary")
	apiKey    = flag.String("api-key", "", "API key for token verification")
	apiSecret = flag.String("api-secret", "", "API secret for token signing")
	debug     = flag.Bool("debug", false, "Debug mode (skip token verification)")
)

func main() {
	flag.Parse()

	opts := &slog.HandlerOptions{Level: slog.LevelDebug}
	logger := slog.New(slog.NewTextHandler(os.Stderr, opts))
	slog.SetDefault(logger)

	cfg := config.DefaultConfig()
	cfg.Node.ID = *nodeID
	cfg.Node.Region = *regionID
	cfg.Node.Cluster = *clusterID
	cfg.Node.Port = *httpPort
	cfg.Media.WorkerBin = *workerBin
	cfg.Nats.URL = *natsURL
	cfg.Auth.APIKey = *apiKey
	cfg.Auth.APISecret = *apiSecret
	cfg.Debug = *debug

	myNode := types.NodeInfo{
		ID:      types.NodeID(cfg.Node.ID),
		Region:  types.RegionID(cfg.Node.Region),
		Cluster: types.ClusterID(cfg.Node.Cluster),
	}

	slog.Info("Starting OmniStream Node",
		"id", myNode.ID, "region", myNode.Region, "cluster", myNode.Cluster,
		"debug", cfg.Debug)

	// 1. Connect to NATS
	nc, err := nats.Connect(cfg.Nats.URL)
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("Failed to init JetStream", "error", err)
		os.Exit(1)
	}

	// 2. Init RIB & FIB
	kv, err := store.NewNatsStore(context.Background(), js, routing.RibBucketName, routing.RIBTTL)
	if err != nil {
		slog.Error("Failed to init NATS Store", "error", err)
		os.Exit(1)
	}
	rib := routing.NewRIB(kv, myNode)

	linkDB := routing.NewLinkDB(myNode)
	switch *clusterID {
	case "SanFrancisco":
		linkDB.UpdateClusterLink("Beijing", 200, 0, 100)
		linkDB.UpdateClusterLink("NewYork", 30, 0, 10)
		linkDB.UpdateRegionLink("CN", 180, 0, 90)
	case "NewYork":
		linkDB.UpdateClusterLink("Beijing", 200, 0, 100)
	}

	finder := discovery.NewFinder(rib, linkDB, myNode)
	fib := routing.NewFIB(rib, myNode.ID)

	// 3. Init Media Manager
	mediaMgr := media.NewManager(myNode.ID, nc, cfg.Media.WorkerBin, cfg.Media.WebRtc)
	mediaMgr.OnStreamClosed = func(streamID types.StreamID) {
		if err := rib.StopAdvertise(streamID); err != nil {
			slog.Error("Failed to stop advertise", "stream_id", streamID, "error", err)
		}
	}
	if err := mediaMgr.Start(); err != nil {
		slog.Error("Failed to start MediaManager", "error", err)
		os.Exit(1)
	}
	linkDB.SetRTTProvider(mediaMgr.ProbeRTT)

	fib.ConnectPipe = mediaMgr.ConnectPipe
	fib.DisconnectPipe = mediaMgr.DisconnectPipe

	// 4. Init Node Registry
	nodeStore, err := store.NewNatsStore(context.Background(), js, discovery.NodeBucketName, discovery.NodeTTL)
	if err != nil {
		slog.Error("Failed to init Node Store", "error", err)
		os.Exit(1)
	}

	registry := discovery.NewRegistry(nodeStore, myNode, discovery.WithTTL(discovery.NodeTTL))
	if err := registry.Start(); err != nil {
		slog.Error("Failed to start Node Registry", "error", err)
		os.Exit(1)
	}

	err = registry.WatchNodes(func(node types.NodeInfo, isDelete bool) {
		if isDelete {
			slog.Info("Node offline, cleaning up", "node_id", node.ID)
			mediaMgr.CleanUpNode(node.ID)
			linkDB.RemoveNode(node.ID)
		} else {
			slog.Debug("Node online", "node_id", node.ID, "cluster", node.Cluster)
			linkDB.AddNode(node)
		}
	})
	if err != nil {
		slog.Error("Failed to start node watcher", "error", err)
	}

	linkDB.StartProbing(context.Background())

	// 5. Init Session Registry
	sessionStore, err := store.NewNatsStore(context.Background(), js, discovery.SessionBucketName, discovery.SessionTTL)
	if err != nil {
		slog.Error("Failed to init Session Store", "error", err)
		os.Exit(1)
	}
	sessionRegistry := discovery.NewSessionRegistry(sessionStore, myNode.ID)

	// 6. Init Room Manager
	roomManager := rtc.NewRoomManager(rtc.RoomManagerParams{
		Config: cfg.Room,
		FIB:    fib,
		Finder: finder,
	})

	// 7. Init Token Verifier
	verifier := auth.NewTokenVerifier(cfg.Auth.APIKey, cfg.Auth.APISecret)

	// 8. HTTP API + NATS RPC
	srv := server.NewServer(server.ServerParams{
		Config:        cfg,
		RoomManager:   roomManager,
		Verifier:      verifier,
		Node:          myNode,
		NC:            nc,
		Sessions:      sessionRegistry,
		Routers:       mediaMgr.Routers(),
		WebRtcServers: mediaMgr.WebRtcServers(),
	})

	_, err = nc.Subscribe(fmt.Sprintf("rtc.rpc.%s", myNode.ID), srv.HandleNodeRPC)
	if err != nil {
		slog.Error("Failed to subscribe NATS RPC", "error", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	go func() {
		addr := fmt.Sprintf(":%d", cfg.Node.Port)
		slog.Info("HTTP server listening", "addr", addr)
		if err := http.ListenAndServe(addr, server.WithLogging(mux)); err != nil {
			slog.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	slog.Info("Shutting down...")

	fib.Close()
	rib.Close()
	roomManager.Close()
	mediaMgr.Close()
	sessionRegistry.Close()
	registry.Close()

	slog.Info("Bye!")
}
