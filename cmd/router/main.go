package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rib-control/pkg/discovery"
	"rib-control/pkg/routing"
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

	myNode := types.NodeInfo{
		ID:      types.NodeID(*nodeID),
		Region:  types.RegionID(*regionID),
		Cluster: types.ClusterID(*clusterID),
	}

	log.Printf("Starting Node %s in Region %s, Cluster %s", myNode.ID, myNode.Region, myNode.Cluster)

	// 1. Connect to NATS
	nc, err := nats.Connect(*natsURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Failed to init JetStream: %v", err)
	}

	// 2. Init Components
	rib, err := routing.NewRIB(js, myNode)
	if err != nil {
		log.Fatalf("Failed to init RIB: %v", err)
	}

	linkDB := routing.NewLinkDB()
	finder := discovery.NewFinder(rib, linkDB, myNode.ID)
	fib := routing.NewFIB(rib, myNode.ID)

	// Mock Pipe Callbacks
	fib.ConnectPipe = func(streamID types.StreamID, upstream types.NodeID) (string, error) {
		log.Printf(">>> [PIPE] Connecting to %s for stream %s...", upstream, streamID)
		time.Sleep(100 * time.Millisecond) // Simulate delay
		return fmt.Sprintf("pipe-%s-%s", upstream, streamID), nil
	}
	fib.DisconnectPipe = func(pipeID string) error {
		log.Printf("<<< [PIPE] Disconnecting pipe %s", pipeID)
		return nil
	}

	// Demo Setup: Pre-populate LinkDB
	if *demoScenario {
		switch *regionID {
		case "US-B":
			linkDB.UpdateLink("CN", 200, 0, 100)
			linkDB.UpdateLink("US-A", 30, 0, 10)
			log.Println("Configured LinkDB: US-B -> CN (200ms), US-B -> US-A (30ms)")
		case "US-A":
			linkDB.UpdateLink("CN", 200, 0, 100)
			log.Println("Configured LinkDB: US-A -> CN (200ms)")
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
		log.Printf("Origin Advertised %s", streamID)
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
		log.Printf("Origin Unadvertised %s", streamID)
	})

	http.HandleFunc("/subscribe", func(w http.ResponseWriter, r *http.Request) {
		streamID := r.URL.Query().Get("stream")
		if streamID == "" {
			http.Error(w, "missing stream", 400)
			return
		}

		pipeID, upstream, err := fib.Subscribe(types.StreamID(streamID), func() (*types.RouteKVValue, error) {
			return finder.FindPath(types.StreamID(streamID))
		})

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
		log.Printf("Unsubscribed %s", streamID)
	})

	go func() {
		addr := fmt.Sprintf(":%d", *httpPort)
		log.Printf("HTTP server listening on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Wait for shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("Shutting down...")

	// Graceful Shutdown
	fib.Close()
	rib.Close()

	log.Println("Bye!")
}
