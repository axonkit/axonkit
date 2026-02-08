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
	"time"

	"rib-control/pkg/discovery"
	"rib-control/pkg/routing"
	"rib-control/pkg/store"
	"rib-control/pkg/types"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	nodeID       = flag.String("node", "node-1", "Node ID")
	regionID     = flag.String("region", "default-region", "Region ID")
	clusterID    = flag.String("cluster", "default-cluster", "Cluster ID")
	natsURL      = flag.String("nats", nats.DefaultURL, "NATS URL")
	httpPort     = flag.Int("port", 8080, "HTTP Port")
	demoScenario = flag.Bool("demo", false, "Run demo scenario logic")
)

func main() {
	flag.Parse()

	// Initialize structured logger
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	myNode := types.NodeInfo{
		ID:      types.NodeID(*nodeID),
		Region:  types.RegionID(*regionID),
		Cluster: types.ClusterID(*clusterID),
	}

	slog.Info("Starting Node", "id", myNode.ID, "region", myNode.Region, "cluster", myNode.Cluster)

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

	// 2. Init Components
	kv, err := store.NewNatsStore(context.Background(), js, routing.RibBucketName, routing.RIBTTL)
	if err != nil {
		slog.Error("Failed to init NATS Store", "error", err)
		os.Exit(1)
	}

	rib := routing.NewRIB(kv, myNode)

	linkDB := routing.NewLinkDB()
	finder := discovery.NewFinder(rib, linkDB, myNode.ID)
	fib := routing.NewFIB(rib, myNode.ID)

	// Mock Pipe Callbacks
	fib.ConnectPipe = func(streamID types.StreamID, upstream types.NodeID) (string, error) {
		slog.Info("Pipe Connect", "upstream", upstream, "stream_id", streamID)
		time.Sleep(100 * time.Millisecond) // Simulate delay
		return fmt.Sprintf("pipe-%s-%s", upstream, streamID), nil
	}
	fib.DisconnectPipe = func(pipeID string) error {
		slog.Info("Pipe Disconnect", "pipe_id", pipeID)
		return nil
	}

	// Demo Setup: Pre-populate LinkDB
	if *demoScenario {
		switch *regionID {
		case "US-B":
			linkDB.UpdateLink("CN", 200, 0, 100)
			linkDB.UpdateLink("US-A", 30, 0, 10)
			slog.Info("Configured LinkDB: US-B -> CN (200ms), US-B -> US-A (30ms)")
		case "US-A":
			linkDB.UpdateLink("CN", 200, 0, 100)
			slog.Info("Configured LinkDB: US-A -> CN (200ms)")
		}
	}

	// 3. HTTP API
	http.HandleFunc("/advertise", func(w http.ResponseWriter, r *http.Request) {
		streamID := r.URL.Query().Get("stream")
		if streamID == "" {
			http.Error(w, "missing stream", 400)
			return
		}

		// Origin advertising
		err := rib.StartAdvertise(types.StreamID(streamID), 0, []types.NodeID{myNode.ID}, 0)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		fmt.Fprintf(w, "Advertised %s (Keepalive Started)\n", streamID)
		slog.Info("Origin Advertised", "stream_id", streamID)
	})

	http.HandleFunc("/unadvertise", func(w http.ResponseWriter, r *http.Request) {
		streamID := r.URL.Query().Get("stream")
		if streamID == "" {
			http.Error(w, "missing stream", 400)
			return
		}

		// Origin unadvertising
		err := rib.StopAdvertise(types.StreamID(streamID))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		fmt.Fprintf(w, "Unadvertised %s (Keepalive Stopped)\n", streamID)
		slog.Info("Origin Unadvertised", "stream_id", streamID)
	})

	http.HandleFunc("/subscribe", func(w http.ResponseWriter, r *http.Request) {
		streamID := r.URL.Query().Get("stream")
		if streamID == "" {
			http.Error(w, "missing stream", 400)
			return
		}

		pipeID, upstream, err := fib.Subscribe(types.StreamID(streamID), finder.FindPath)

		if err != nil {
			http.Error(w, fmt.Sprintf("Subscribe failed: %v", err), 500)
			return
		}

		fmt.Fprintf(w, "Subscribed %s via %s (Pipe: %s)\n", streamID, upstream, pipeID)
	})

	http.HandleFunc("/unsubscribe", func(w http.ResponseWriter, r *http.Request) {
		streamID := r.URL.Query().Get("stream")
		if streamID == "" {
			http.Error(w, "missing stream", 400)
			return
		}

		fib.Unsubscribe(types.StreamID(streamID))
		fmt.Fprintf(w, "Unsubscribed %s\n", streamID)
		slog.Info("Unsubscribed", "stream_id", streamID)
	})

	go func() {
		addr := fmt.Sprintf(":%d", *httpPort)
		slog.Info("HTTP server listening", "addr", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			slog.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	slog.Info("Shutting down...")

	// Graceful Shutdown
	fib.Close()
	rib.Close()

	slog.Info("Bye!")
}
