package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/server"
)

var (
	cfg               = config.DefaultConfig()
	nodeID            = flag.String("node", cfg.Node.ID, "Node ID")
	regionID          = flag.String("region", cfg.Node.Region, "Region ID")
	clusterID         = flag.String("cluster", cfg.Node.Cluster, "Cluster ID")
	natsURL           = flag.String("nats", cfg.Nats.URL, "NATS URL (empty enables the embedded in-process NATS server)")
	natsStoreDir      = flag.String("nats-storedir", cfg.Nats.StoreDir, "JetStream store directory for the embedded NATS server (ignored when -nats is set)")
	httpPort          = flag.Int("port", cfg.Node.Port, "HTTP Port")
	workerBin         = flag.String("worker", cfg.Media.WorkerBin, "Path to mediasoup-worker binary")
	apiKey            = flag.String("api-key", cfg.Auth.APIKey, "API key for token verification")
	apiSecret         = flag.String("api-secret", cfg.Auth.APISecret, "API secret for token signing")
	allowAnonymous    = flag.Bool("allow-anonymous", cfg.Auth.AllowAnonymous, "Allow joining without a token (auto-generate identity)")
	dev               = flag.Bool("dev", false, "Development mode: auto-generate a temporary API key/secret when none are set")
	rtmpListen        = flag.String("rtmp-listen", cfg.Rtmp.Listen, "RTMP TCP bind address (e.g. ':1935'); empty disables RTMP ingest")
	webrtcListenIP    = flag.String("webrtc-listen-ip", cfg.Media.WebRtc.ListenIP, "WebRTC listen IP for ICE candidates")
	webrtcAnnouncedIP = flag.String("webrtc-announced-ip", cfg.Media.WebRtc.AnnouncedIP, "WebRTC announced IP advertised to peers (override for local testing)")
	webrtcMinPort     = flag.Int("webrtc-min-port", int(cfg.Media.WebRtc.MinPort), "WebRTC ICE UDP port range minimum")
	webrtcMaxPort     = flag.Int("webrtc-max-port", int(cfg.Media.WebRtc.MaxPort), "WebRTC ICE UDP port range maximum")
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
	cfg.Nats.StoreDir = *natsStoreDir
	cfg.Auth.AllowAnonymous = *allowAnonymous
	cfg.Rtmp.Listen = *rtmpListen
	cfg.Media.WebRtc.ListenIP = *webrtcListenIP
	cfg.Media.WebRtc.AnnouncedIP = *webrtcAnnouncedIP
	cfg.Media.WebRtc.MinPort = uint16(*webrtcMinPort)
	cfg.Media.WebRtc.MaxPort = uint16(*webrtcMaxPort)

	key, secret, err := resolveAuth(*dev, *allowAnonymous, *apiKey, *apiSecret)
	if err != nil {
		slog.Error("Invalid auth configuration", "error", err)
		os.Exit(1)
	}
	cfg.Auth.APIKey = key
	cfg.Auth.APISecret = secret

	slog.Info("Starting axonkit Node",
		"id", cfg.Node.ID, "region", cfg.Node.Region, "cluster", cfg.Node.Cluster,
		"allowAnonymous", cfg.Auth.AllowAnonymous, "dev", *dev, "rtmpListen", cfg.Rtmp.Listen)

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

// resolveAuth decides the API key/secret the node runs with.
//
//   - When both are provided, they are used as-is.
//   - When either is missing and -dev is set, a temporary random pair is
//     generated and printed so local demos can mint tokens without a secret
//     ever living in the repo or a config file.
//   - When either is missing and -allow-anonymous is set, the node starts in
//     anonymous mode (tokens are optional).
//   - Otherwise it fails fast: an empty secret would let anyone forge an
//     admin JWT, silently disabling authentication.
func resolveAuth(dev, allowAnonymous bool, apiKey, apiSecret string) (string, string, error) {
	if apiKey != "" && apiSecret != "" {
		return apiKey, apiSecret, nil
	}

	if dev {
		key, err := randomHex(16)
		if err != nil {
			return "", "", fmt.Errorf("generate dev api key: %w", err)
		}
		secret, err := randomHex(32)
		if err != nil {
			return "", "", fmt.Errorf("generate dev api secret: %w", err)
		}
		slog.Warn("DEV MODE: generated temporary API credentials (valid for this process only)",
			"apiKey", key, "apiSecret", secret)
		return key, secret, nil
	}

	if allowAnonymous {
		slog.Warn("Starting with empty API key/secret in anonymous mode; token verification is disabled")
		return apiKey, apiSecret, nil
	}

	return "", "", fmt.Errorf(
		"api-key and api-secret must both be set, or start with -dev to generate temporary credentials, or -allow-anonymous to allow tokenless joins")
}

func randomHex(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
