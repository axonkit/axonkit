package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/server"
)

var (
	cfg            = config.DefaultConfig()
	nodeID         = flag.String("node", cfg.Node.ID, "Node ID")
	regionID       = flag.String("region", cfg.Node.Region, "Region ID")
	clusterID      = flag.String("cluster", cfg.Node.Cluster, "Cluster ID")
	natsURL        = flag.String("nats", cfg.Nats.URL, "NATS URL")
	httpPort       = flag.Int("port", cfg.Node.Port, "HTTP Port")
	workerBin      = flag.String("worker", cfg.Media.WorkerBin, "Path to mediasoup-worker binary")
	apiKey         = flag.String("api-key", cfg.Auth.APIKey, "API key for token verification")
	apiSecret      = flag.String("api-secret", cfg.Auth.APISecret, "API secret for token signing")
	allowAnonymous = flag.Bool("allow-anonymous", cfg.Auth.AllowAnonymous, "Allow joining without a token (auto-generate identity)")
	rtmpListen     = flag.String("rtmp-listen", cfg.Rtmp.Listen, "RTMP TCP bind address (e.g. ':1935'); empty disables RTMP ingest")
)

func main() {
	flag.Parse()

	opts := &slog.HandlerOptions{Level: slog.LevelDebug}
	logger := slog.New(slog.NewTextHandler(os.Stderr, opts))
	slog.SetDefault(logger)

	cfg.Node.ID = *nodeID
	cfg.Node.Region = *regionID
	cfg.Node.Cluster = *clusterID
	cfg.Node.Port = *httpPort
	cfg.Media.WorkerBin = *workerBin
	cfg.Nats.URL = *natsURL
	cfg.Auth.APIKey = *apiKey
	cfg.Auth.APISecret = *apiSecret
	cfg.Auth.AllowAnonymous = *allowAnonymous
	cfg.Rtmp.Listen = *rtmpListen

	slog.Info("Starting axonkit Node",
		"id", cfg.Node.ID, "region", cfg.Node.Region, "cluster", cfg.Node.Cluster,
		"allowAnonymous", cfg.Auth.AllowAnonymous, "rtmpListen", cfg.Rtmp.Listen)

	n, err := server.NewServer(cfg)
	if err != nil {
		slog.Error("Failed to start node", "error", err)
		os.Exit(1)
	}

	if err := n.Start(context.Background()); err != nil {
		slog.Error("Start failed", "error", err)
		os.Exit(1)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	slog.Info("Shutting down...")
	if err := n.Stop(context.Background()); err != nil {
		slog.Error("Stop error", "error", err)
	}
	slog.Info("Bye!")
}
