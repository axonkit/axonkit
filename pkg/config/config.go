package config

import (
	"log"
	"net"
	"time"

	"github.com/rs/xid"
)

type Config struct {
	Node  NodeConfig  `json:"node"`
	Room  RoomConfig  `json:"room"`
	Media MediaConfig `json:"media"`
	Auth  AuthConfig  `json:"auth"`
	Nats  NatsConfig  `json:"nats"`
	Debug bool        `json:"debug"`
}

type NodeConfig struct {
	ID      string `json:"id"`
	Region  string `json:"region"`
	Cluster string `json:"cluster"`
	Port    int    `json:"port"`
}

type RoomConfig struct {
	EmptyTimeout     time.Duration `json:"emptyTimeout"`
	DepartureTimeout time.Duration `json:"departureTimeout"`
	MaxParticipants  int           `json:"maxParticipants"`
	AutoSubscribe    bool          `json:"autoSubscribe"`
	// Dynacast: only forward layers that have active subscribers.
	// When enabled, the server tracks which layers each subscriber needs
	// and pauses/resumes producer layers accordingly.
	Dynacast bool `json:"dynacast"`
}

type MediaConfig struct {
	WorkerBin   string       `json:"workerBin"`
	WorkerCount int          `json:"workerCount"`
	WebRtc      WebRtcConfig `json:"webRtc"`
}

type WebRtcConfig struct {
	ListenIP    string `json:"listenIp"`
	AnnouncedIP string `json:"announcedIp"`
	MinPort     uint16 `json:"minPort"`
	MaxPort     uint16 `json:"maxPort"`
}

type AuthConfig struct {
	APIKey    string        `json:"apiKey"`
	APISecret string        `json:"apiSecret"`
	TokenTTL  time.Duration `json:"tokenTtl"`
}

type NatsConfig struct {
	URL string `json:"url"`
}

func DefaultConfig() *Config {
	return &Config{
		Node: NodeConfig{
			ID:      xid.New().String(),
			Region:  "default-region",
			Cluster: "default-cluster",
			Port:    7990,
		},
		Room: RoomConfig{
			EmptyTimeout:     5 * time.Minute,
			DepartureTimeout: 20 * time.Second,
			MaxParticipants:  100,
			AutoSubscribe:    true,
			Dynacast:         true,
		},
		Media: MediaConfig{
			WorkerBin: "./mediasoup-worker",
			WebRtc: WebRtcConfig{
				ListenIP:    "0.0.0.0",
				AnnouncedIP: GetOutboundIP().String(),
				MinPort:     40000,
				MaxPort:     49999,
			},
		},
		Auth: AuthConfig{
			TokenTTL: 6 * time.Hour,
		},
		Debug: false,
	}
}

// PingInterval returns the ping interval in seconds for client heartbeat.
func (c *Config) PingInterval() int32 { return 5 }

// PingTimeout returns the ping timeout in seconds. If no ping is received
// within this duration, the participant is considered disconnected.
func (c *Config) PingTimeout() int32 { return 15 }

// Get preferred outbound ip of this machine
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}
