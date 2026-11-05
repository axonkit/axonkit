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
	Whip  WhipConfig  `json:"whip"`
	Rtmp  RtmpConfig  `json:"rtmp"`
}

// WHEP resource-id routing schemes. The scheme decides how a node that
// receives a PATCH/DELETE locates the node currently owning the session.
const (
	// WhepRoutingPrefix encodes the owner NodeID directly in the resource
	// id as "{nodeID}.{xid}". Any node can parse and forward without
	// touching KV. A node restart invalidates its WHEP sessions anyway,
	// so the "nodeID might be renamed" caveat is moot in practice.
	WhepRoutingPrefix = "prefix"
	// WhepRoutingKV maintains a NATS KV bucket (resourceID → nodeID)
	// refreshed by the session's keepalive. Survives node renames; costs
	// one KV lookup per cross-node request.
	WhepRoutingKV = "kv"
)

// WhipConfig tunes WHIP/WHEP specific behavior.
type WhipConfig struct {
	// WhepRouting selects the distributed-lookup scheme for WHEP
	// PATCH/DELETE. See the WhepRouting* constants. Defaults to
	// WhepRoutingPrefix.
	WhepRouting string `json:"whepRouting"`
}

// RtmpConfig tunes the RTMP push ingest service. The implementation
// speaks both classic RTMP and Enhanced RTMP (ExAudio / ExVideo) so
// publishers can push H.264 + Opus without transcoding; any other codec
// is dropped without failing the connection.
//
// The ingest is disabled when Listen is empty; only axonkit operators who
// explicitly opt-in by setting a bind address (e.g. ":1935") pay the
// go-rtmp goroutine / port cost.
//
// A single axonkit node serves both WHIP (WebRTC) and RTMP pushes into
// the same rooms. Authentication is JWT-based and carried in the RTMP
// stream key as a query parameter: publishers point at
// "rtmp://host/<app>/<room>?token=<jwt>".
type RtmpConfig struct {
	// Listen is the TCP bind address for the RTMP server (e.g. ":1935").
	// Empty disables the service so `go-rtmp` is never wired in.
	Listen string `json:"listen"`

	// MaxMessageSize caps individual RTMP messages accepted from
	// publishers. 0 means use go-rtmp default.
	MaxMessageSize uint32 `json:"maxMessageSize,omitempty"`
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
	Dynacast      bool          `json:"dynacast"`
	PingInterval  time.Duration `json:"pingInterval"`
	PingTimeout   time.Duration `json:"pingTimeout"`
	StatsInterval time.Duration `json:"statsInterval"`
	// CodecRegressionThreshold controls when PREFER_REGRESSION policy
	// triggers codec regression. When 0 (default) or when the room's
	// participant count is below this threshold, regression is used.
	// Otherwise the system falls back to simulcast behavior.
	CodecRegressionThreshold int `json:"codecRegressionThreshold"`
	// MaxMessages is the maximum number of messages kept in room history.
	// Oldest messages are evicted when the limit is exceeded. 0 means use default (10000).
	MaxMessages int `json:"maxMessages"`
}

type MediaConfig struct {
	WorkerBin   string       `json:"workerBin"`
	WorkerCount int          `json:"workerCount"`
	WebRtc      WebRtcConfig `json:"webRtc"`
	// SharedRouter forces all participants in the same room to use the same
	// mediasoup Router (and its associated WebRtcServer). This eliminates
	// cross-router PipeToRouter for intra-room traffic and ensures a single
	// AudioLevelObserver for consistent active-speaker detection.
	SharedRouter bool `json:"sharedRouter"`
}

type WebRtcConfig struct {
	ListenIP    string `json:"listenIp"`
	AnnouncedIP string `json:"announcedIp"`
	MinPort     uint16 `json:"minPort"`
	MaxPort     uint16 `json:"maxPort"`
}

type AuthConfig struct {
	APIKey          string        `json:"apiKey"`
	APISecret       string        `json:"apiSecret"`
	TokenTTL        time.Duration `json:"tokenTtl"`
	RefreshTokenTTL time.Duration `json:"refreshTokenTtl"`
	// DevMode enables development-only conveniences such as the
	// GET /dev/token endpoint. Set by the -dev CLI flag; never in production.
	DevMode bool `json:"-"`
}

type NatsConfig struct {
	URL      string `json:"url"`
	StoreDir string `json:"storeDir,omitempty"`
}

func DefaultConfig() *Config {
	return &Config{
		Node: NodeConfig{
			ID:      "nd_" + xid.New().String(),
			Region:  "default-region",
			Cluster: "default-cluster",
			Port:    7990,
		},
		Room: RoomConfig{
			EmptyTimeout:     30 * time.Second,
			DepartureTimeout: 30 * time.Second,
			MaxParticipants:  100,
			AutoSubscribe:    true,
			Dynacast:         true,
			PingInterval:     15 * time.Second,
			PingTimeout:      30 * time.Second,
			StatsInterval:    10 * time.Second,
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
			TokenTTL:        6 * time.Hour,
			RefreshTokenTTL: 7 * 24 * time.Hour,
		},
		Whip: WhipConfig{
			WhepRouting: WhepRoutingPrefix,
		},
		Rtmp: RtmpConfig{
			Listen: ":1935",
		},
	}
}

// PingInterval returns the ping interval in seconds for client heartbeat.
func (c *Config) PingInterval() int32 {
	if c.Room.PingInterval > 0 {
		return int32(c.Room.PingInterval / time.Second)
	}
	return 5
}

// PingTimeout returns the ping timeout in seconds. If no ping is received
// within this duration, the participant is considered disconnected.
func (c *Config) PingTimeout() int32 {
	if c.Room.PingTimeout > 0 {
		return int32(c.Room.PingTimeout / time.Second)
	}
	return 15
}

// StatsInterval returns the stats reporting interval in seconds.
func (c *Config) StatsInterval() int32 {
	if c.Room.StatsInterval > 0 {
		return int32(c.Room.StatsInterval / time.Second)
	}
	return 10
}

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
