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

	"github.com/jiyeyuran/omnistream/pkg/discovery"
	"github.com/jiyeyuran/omnistream/pkg/media"
	"github.com/jiyeyuran/omnistream/pkg/routing"
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
)

func main() {
	flag.Parse()

	// Initialize structured logger
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, opts))
	slog.SetDefault(logger)

	myNode := types.NodeInfo{
		ID:      types.NodeID(*nodeID),
		Region:  types.RegionID(*regionID),
		Cluster: types.ClusterID(*clusterID),
	}

	slog.Info("Starting Mediasoup Demo Node", "id", myNode.ID, "region", myNode.Region, "cluster", myNode.Cluster)

	// 1. Connect to NATS
	nc, err := nats.Connect(*natsURL)
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
	// Demo Setup: Pre-populate LinkDB
	// Note: We use ClusterID for LinkDB now
	switch *clusterID {
	case "SanFrancisco": // US-B
		linkDB.UpdateClusterLink("Beijing", 200, 0, 100)
		linkDB.UpdateClusterLink("NewYork", 30, 0, 10)
		// Fallback region link
		linkDB.UpdateRegionLink("CN", 180, 0, 90)
		slog.Info("Configured LinkDB: SanFrancisco -> Beijing (200ms), Region CN (180ms)")
	case "NewYork": // US-A
		linkDB.UpdateClusterLink("Beijing", 200, 0, 100)
		slog.Info("Configured LinkDB: NewYork -> Beijing (200ms)")
	}

	finder := discovery.NewFinder(rib, linkDB, myNode)
	fib := routing.NewFIB(rib, myNode.ID)

	// 3. Init Media Manager
	mediaMgr := media.NewManager(myNode.ID, nc, *workerBin)
	mediaMgr.OnStreamClosed = func(streamID types.StreamID) {
		if err := rib.StopAdvertise(streamID); err != nil {
			slog.Error("Failed to stop advertise closed stream", "stream_id", streamID, "error", err)
		} else {
			slog.Info("Stop advertise closed stream", "stream_id", streamID)
		}
	}
	if err := mediaMgr.Start(); err != nil {
		slog.Error("Failed to start MediaManager", "error", err)
		os.Exit(1)
	}
	linkDB.SetRTTProvider(mediaMgr.ProbeRTT)

	// Wire FIB to Media Manager
	fib.ConnectPipe = mediaMgr.ConnectPipe
	fib.DisconnectPipe = mediaMgr.DisconnectPipe

	// 4. Init Node Registry (Discovery & Heartbeat)
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
	// Note: We don't defer registry.Close() here because it might delete the node record immediately
	// while we want it to expire naturally if we crash.
	// But for graceful shutdown we can call it at the end.

	// Watch for node offline events to cleanup resources
	err = registry.WatchNodes(func(node types.NodeInfo, isDelete bool) {
		if isDelete {
			slog.Info("Detected node offline, cleaning up resources", "node_id", node.ID)
			mediaMgr.CleanUpNode(node.ID)
			linkDB.RemoveNode(node.ID)
		} else {
			// Node Added/Updated
			slog.Debug("Detected node online", "node_id", node.ID, "cluster", node.Cluster)
			linkDB.AddNode(node)
		}
	})
	if err != nil {
		slog.Error("Failed to start node watcher", "error", err)
	}

	// Start Probing & Ping Handler
	linkDB.StartProbing(context.Background())

	// 5. HTTP API
	mux := http.NewServeMux()
	server.RegisterHTTPRoutes(mux, mediaMgr, rib, fib, finder, myNode)

	go func() {
		addr := fmt.Sprintf(":%d", *httpPort)
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
	mediaMgr.Close()
	registry.Close()

	slog.Info("Bye!")
}
