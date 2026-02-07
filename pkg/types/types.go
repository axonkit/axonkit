package types

import "time"

type StreamID string
type NodeID string
type RegionID string
type ClusterID string

// NodeInfo identifies a node's location
type NodeInfo struct {
	ID      NodeID    `json:"id"`
	Region  RegionID  `json:"region"`
	Cluster ClusterID `json:"cluster"`
}

// RouteKVValue is the value stored in NATS KV
// Key format: route.<StreamID>.<Region>.<Cluster>.<NodeID>
type RouteKVValue struct {
	StreamID      StreamID  `json:"stream_id"`
	OriginNode    NodeID    `json:"origin_node"` // The node providing this hop
	OriginRegion  RegionID  `json:"origin_region"`
	OriginCluster ClusterID `json:"origin_cluster"`

	BaseCost    int       `json:"base_cost"`     // Cost to reach this node (from stream source)
	PathVector  []NodeID  `json:"path_vector"`   // Loop detection
	RttToOrigin int64     `json:"rtt_to_origin"` // Cumulative RTT from source to here
	ExpireAt    time.Time `json:"expire_at"`
}

// LinkStats stores quality metrics for a link (e.g. to a Region)
type LinkStats struct {
	AvgRTT     int64   `json:"avg_rtt"`     // ms
	PacketLoss float64 `json:"packet_loss"` // percentage 0-100
	CostFactor int     `json:"cost_factor"` // Artificial administrative cost
}

// FIBEntry represents a local active subscription
type FIBEntry struct {
	StreamID        StreamID
	UpstreamNode    NodeID
	PipeID          string          // Mock Pipe ID
	State           string          // "connected", "connecting", "disconnected", "lingering"
	RefCount        int             // Local user references
	DownstreamNodes map[NodeID]bool // Set of downstream nodes depending on us

	TeardownTimer     *time.Timer // Timer for idle timeout (linger)
	OptimizationTimer *time.Timer // Timer for periodic path optimization
	FindPathFunc      func() (*RouteKVValue, error)
}
