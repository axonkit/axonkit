package proto

import "github.com/jiyeyuran/mediasoup-go/v2"

type ClientInfo struct {
	Sdk            ClientInfo_SDK `json:"sdk,omitempty"`
	Version        string         `json:"version,omitempty"`
	Protocol       int32          `json:"protocol,omitempty"`
	Os             string         `json:"os,omitempty"`
	OsVersion      string         `json:"osVersion,omitempty"`
	DeviceModel    string         `json:"deviceModel,omitempty"`
	Browser        string         `json:"browser,omitempty"`
	BrowserVersion string         `json:"browserVersion,omitempty"`
	Address        string         `json:"address,omitempty"`
	Network        string         `json:"network,omitempty"`
	OtherSdks      string         `json:"otherSdks,omitempty"`
	ClientProtocol int32          `json:"clientProtocol,omitempty"`
}

type ClientInfo_SDK int32

const (
	ClientInfo_UNKNOWN      ClientInfo_SDK = 0
	ClientInfo_JS           ClientInfo_SDK = 1
	ClientInfo_SWIFT        ClientInfo_SDK = 2
	ClientInfo_ANDROID      ClientInfo_SDK = 3
	ClientInfo_FLUTTER      ClientInfo_SDK = 4
	ClientInfo_GO           ClientInfo_SDK = 5
	ClientInfo_UNITY        ClientInfo_SDK = 6
	ClientInfo_REACT_NATIVE ClientInfo_SDK = 7
	ClientInfo_RUST         ClientInfo_SDK = 8
	ClientInfo_PYTHON       ClientInfo_SDK = 9
	ClientInfo_CPP          ClientInfo_SDK = 10
	ClientInfo_UNITY_WEB    ClientInfo_SDK = 11
	ClientInfo_NODE         ClientInfo_SDK = 12
	ClientInfo_UNREAL       ClientInfo_SDK = 13
	ClientInfo_ESP32        ClientInfo_SDK = 14
)

type Room struct {
	Sid              string        `json:"sid,omitempty"`
	Name             string        `json:"name,omitempty"`
	EmptyTimeout     uint32        `json:"emptyTimeout,omitempty"`
	DepartureTimeout uint32        `json:"departureTimeout,omitempty"`
	MaxParticipants  uint32        `json:"maxParticipants,omitempty"`
	CreationTime     int64         `json:"creationTime,omitempty"`
	Metadata         string        `json:"metadata,omitempty"`
	NumParticipants  uint32        `json:"numParticipants,omitempty"`
	NumPublishers    uint32        `json:"numPublishers,omitempty"`
	ActiveRecording  bool          `json:"activeRecording,omitempty"`
	ActiveStreaming  bool          `json:"activeStreaming,omitempty"`
	Version          *TimedVersion `json:"version,omitempty"`
}

type ParticipantInfo struct {
	Sid              string                       `json:"sid,omitempty"`
	Identity         string                       `json:"identity,omitempty"`
	State            ParticipantInfo_State        `json:"state,omitempty"`
	Tracks           []*TrackInfo                 `json:"tracks,omitempty"`
	Metadata         string                       `json:"metadata,omitempty"`
	JoinedAt         int64                        `json:"joinedAt,omitempty"`
	Name             string                       `json:"name,omitempty"`
	Version          uint32                       `json:"version,omitempty"`
	Permission       *ParticipantPermission       `json:"permission,omitempty"`
	Region           string                       `json:"region,omitempty"`
	IsPublisher      bool                         `json:"isPublisher,omitempty"`
	Kind             ParticipantInfo_Kind         `json:"kind,omitempty"`
	Attributes       map[string]string            `json:"attributes,omitempty"`
	DisconnectReason DisconnectReason             `json:"disconnectReason,omitempty"`
	KindDetails      []ParticipantInfo_KindDetail `json:"kindDetails,omitempty"`
	DataTracks       []*DataTrackInfo             `json:"dataTracks,omitempty"`
	ClientProtocol   int32                        `json:"clientProtocol,omitempty"`
	GroupId          string                       `json:"groupId,omitempty"`
}

type ParticipantInfo_State int32

const (
	// ParticipantInfo_JOINING indicates the participant has connected but not yet sent 'ready'.
	ParticipantInfo_JOINING ParticipantInfo_State = 0
	// ParticipantInfo_CONNECTED indicates transport ice has connected, but hasn't received client's ready request yet.
	ParticipantInfo_CONNECTED ParticipantInfo_State = 1
	// ParticipantInfo_JOINED indicates the participant has sent 'ready' and is fully joined.
	ParticipantInfo_JOINED ParticipantInfo_State = 2
	// ParticipantInfo_SUSPENDED indicates ICE is temporarily disconnected.
	// Messages to this participant are discarded until ICE reconnects.
	ParticipantInfo_SUSPENDED ParticipantInfo_State = 3
	// ParticipantInfo_DISCONNECTED indicates the participant has left the room or been kicked.
	ParticipantInfo_DISCONNECTED ParticipantInfo_State = 4
)

// TrackInfo represents a published media track.
type TrackInfo struct {
	Sid               string              `json:"sid,omitempty"`
	Kind              mediasoup.MediaKind `json:"kind,omitempty"`
	Name              string              `json:"name,omitempty"`
	Muted             bool                `json:"muted,omitempty"`
	Width             uint32              `json:"width,omitempty"`
	Height            uint32              `json:"height,omitempty"`
	Source            TrackSource         `json:"source,omitempty"`
	Version           *TimedVersion       `json:"version,omitempty"`
	AudioFeatures     []AudioTrackFeature `json:"audioFeatures,omitempty"`
	BackupCodecPolicy BackupCodecPolicy   `json:"backupCodecPolicy,omitempty"`
	AppData           map[string]any      `json:"appData,omitempty"`
}

type TrackSource string

const (
	TrackSource_UNKNOWN            TrackSource = "unknown"
	TrackSource_CAMERA             TrackSource = "camera"
	TrackSource_MICROPHONE         TrackSource = "microphone"
	TrackSource_SCREEN_SHARE       TrackSource = "screenShare"
	TrackSource_SCREEN_SHARE_AUDIO TrackSource = "screenShareAudio"
)

type AudioTrackFeature int32

const (
	AudioTrackFeature_TF_STEREO                      AudioTrackFeature = 0
	AudioTrackFeature_TF_NO_DTX                      AudioTrackFeature = 1
	AudioTrackFeature_TF_AUTO_GAIN_CONTROL           AudioTrackFeature = 2
	AudioTrackFeature_TF_ECHO_CANCELLATION           AudioTrackFeature = 3
	AudioTrackFeature_TF_NOISE_SUPPRESSION           AudioTrackFeature = 4
	AudioTrackFeature_TF_ENHANCED_NOISE_CANCELLATION AudioTrackFeature = 5
	AudioTrackFeature_TF_PRECONNECT_BUFFER           AudioTrackFeature = 6
)

type BackupCodecPolicy int32

const (
	BackupCodecPolicy_PREFER_REGRESSION BackupCodecPolicy = 0
	BackupCodecPolicy_SIMULCAST         BackupCodecPolicy = 1
	BackupCodecPolicy_REGRESSION        BackupCodecPolicy = 2
)

type ParticipantPermission struct {
	CanSubscribe          bool          `json:"canSubscribe,omitempty"`
	CanPublish            bool          `json:"canPublish,omitempty"`
	CanPublishData        bool          `json:"canPublishData,omitempty"`
	CanPublishSources     []TrackSource `json:"canPublishSources,omitempty"`
	Hidden                bool          `json:"hidden,omitempty"`
	CanUpdateMetadata     bool          `json:"canUpdateMetadata,omitempty"`
	CanSubscribeMetrics   bool          `json:"canSubscribeMetrics,omitempty"`
	CanManageAgentSession bool          `json:"canManageAgentSession,omitempty"`
	CanManageGroups       bool          `json:"canManageGroups,omitempty"`
}

type ParticipantInfo_Kind int32

const (
	ParticipantInfo_STANDARD  ParticipantInfo_Kind = 0
	ParticipantInfo_INGRESS   ParticipantInfo_Kind = 1
	ParticipantInfo_EGRESS    ParticipantInfo_Kind = 2
	ParticipantInfo_SIP       ParticipantInfo_Kind = 3
	ParticipantInfo_AGENT     ParticipantInfo_Kind = 4
	ParticipantInfo_CONNECTOR ParticipantInfo_Kind = 7
	ParticipantInfo_BRIDGE    ParticipantInfo_Kind = 8
)

type DisconnectReason int32

const (
	DisconnectReason_UNKNOWN_REASON      DisconnectReason = 0
	DisconnectReason_CLIENT_INITIATED    DisconnectReason = 1
	DisconnectReason_DUPLICATE_IDENTITY  DisconnectReason = 2
	DisconnectReason_SERVER_SHUTDOWN     DisconnectReason = 3
	DisconnectReason_PARTICIPANT_REMOVED DisconnectReason = 4
	DisconnectReason_ROOM_DELETED        DisconnectReason = 5
	DisconnectReason_STATE_MISMATCH      DisconnectReason = 6
	DisconnectReason_JOIN_FAILURE        DisconnectReason = 7
	DisconnectReason_MIGRATION           DisconnectReason = 8
	DisconnectReason_SIGNAL_CLOSE        DisconnectReason = 9
	DisconnectReason_ROOM_CLOSED         DisconnectReason = 10
	DisconnectReason_USER_UNAVAILABLE    DisconnectReason = 11
	DisconnectReason_USER_REJECTED       DisconnectReason = 12
	DisconnectReason_SIP_TRUNK_FAILURE   DisconnectReason = 13
	DisconnectReason_CONNECTION_TIMEOUT  DisconnectReason = 14
	DisconnectReason_MEDIA_FAILURE       DisconnectReason = 15
)

type ParticipantInfo_KindDetail int32

const (
	ParticipantInfo_CLOUD_AGENT        ParticipantInfo_KindDetail = 0
	ParticipantInfo_FORWARDED          ParticipantInfo_KindDetail = 1
	ParticipantInfo_CONNECTOR_WHATSAPP ParticipantInfo_KindDetail = 2
	ParticipantInfo_CONNECTOR_TWILIO   ParticipantInfo_KindDetail = 3
	ParticipantInfo_BRIDGE_RTSP        ParticipantInfo_KindDetail = 4
)

// DataTrackInfo represents a published data track.
type DataTrackInfo struct {
	Sid     string         `json:"sid,omitempty"`
	Name    string         `json:"name,omitempty"`
	AppData map[string]any `json:"appData,omitempty"`
}

// SubscriptionInfo represents a media subscription delivered to a client.
type SubscriptionInfo struct {
	Sid              string                   `json:"sid,omitempty"`
	TrackSid         string                   `json:"trackSid,omitempty"`
	Kind             mediasoup.MediaKind      `json:"kind,omitempty"`
	RtpParameters    *mediasoup.RtpParameters `json:"rtpParameters,omitempty"`
	SubscriptionType string                   `json:"subscriptionType,omitempty"`
	TrackMuted       bool                     `json:"trackMuted,omitempty"`
	AppData          map[string]any           `json:"appData,omitempty"`
}

// DataSubscriptionInfo represents a data subscription delivered to a client.
type DataSubscriptionInfo struct {
	Sid                  string                          `json:"sid,omitempty"`
	DataTrackSid         string                          `json:"dataTrackSid,omitempty"`
	SctpStreamParameters *mediasoup.SctpStreamParameters `json:"sctpStreamParameters,omitempty"`
	Label                string                          `json:"label,omitempty"`
	Protocol             string                          `json:"protocol,omitempty"`
	AppData              map[string]any                  `json:"appData,omitempty"`
}

// SignalChannelInfo describes one direction of the signal datachannel.
type SignalChannelInfo struct {
	// ChannelSid is the ID for this channel.
	//  - Receive: client passes as `id` to consumeData()
	//  - Send: client returns as `id` from produceData() callback
	ChannelSid string `json:"channelSid"`
	// DataSourceSid is the server-side data source feeding this channel;
	// client passes as `dataProducerId` to consumeData(). Only for Receive.
	DataSourceSid        string                          `json:"dataSourceSid,omitempty"`
	SctpStreamParameters *mediasoup.SctpStreamParameters `json:"sctpStreamParameters,omitempty"`
	Label                string                          `json:"label,omitempty"`
	Protocol             string                          `json:"protocol,omitempty"`
}

// SignalInfo describes the bidirectional signal datachannel for server<->client RPC.
// Both directions are pre-created during Join so the client does not need
// a server round-trip when calling produceData() for the signal channel.
type SignalInfo struct {
	Receive *SignalChannelInfo `json:"receive"`
	Send    *SignalChannelInfo `json:"send"`
}

type ClientConfiguration struct {
	Video            *VideoConfiguration `json:"video,omitempty"`
	Screen           *VideoConfiguration `json:"screen,omitempty"`
	ResumeConnection ClientConfigSetting `json:"resumeConnection,omitempty"`
	DisabledCodecs   *DisabledCodecs     `json:"disabledCodecs,omitempty"`
	ForceRelay       ClientConfigSetting `json:"forceRelay,omitempty"`
}

type VideoConfiguration struct {
	HardwareEncoder ClientConfigSetting `json:"hardwareEncoder,omitempty"`
}

type DisabledCodecs struct {
	Codecs  []*Codec `json:"codecs,omitempty"`
	Publish []*Codec `json:"publish,omitempty"`
}

type ClientConfigSetting int32

const (
	ClientConfigSetting_UNSET    ClientConfigSetting = 0
	ClientConfigSetting_DISABLED ClientConfigSetting = 1
	ClientConfigSetting_ENABLED  ClientConfigSetting = 2
)

type Codec struct {
	Mime     string `json:"mime,omitempty"`
	FmtpLine string `json:"fmtpLine,omitempty"`
}

type ServerInfo struct {
	Edition       ServerInfo_Edition `json:"edition,omitempty"`
	Version       string             `json:"version,omitempty"`
	Protocol      int32              `json:"protocol,omitempty"`
	Region        string             `json:"region,omitempty"`
	NodeId        string             `json:"nodeId,omitempty"`
	DebugInfo     string             `json:"debugInfo,omitempty"`
	AgentProtocol int32              `json:"agentProtocol,omitempty"`
}

type ServerInfo_Edition int32

const (
	ServerInfo_Standard ServerInfo_Edition = 0
	ServerInfo_Cloud    ServerInfo_Edition = 1
)

type TimedVersion struct {
	UnixMicro int64 `json:"unixMicro,omitempty"`
	Ticks     int32 `json:"ticks,omitempty"`
}

type ReconnectReason int32

const (
	ReconnectReason_RR_UNKNOWN             ReconnectReason = 0
	ReconnectReason_RR_SIGNAL_DISCONNECTED ReconnectReason = 1
	ReconnectReason_RR_SWITCH_CANDIDATE    ReconnectReason = 2
)

type TransportInfo struct {
	Sid string `json:"sid,omitempty"`

	IceParameters  *mediasoup.IceParameters  `json:"iceParameters,omitempty"`
	IceCandidates  []mediasoup.IceCandidate  `json:"iceCandidates,omitempty"`
	DtlsParameters *mediasoup.DtlsParameters `json:"dtlsParameters,omitempty"`
	SctpParameters *mediasoup.SctpParameters `json:"sctpParameters,omitempty"`

	Ip             string                    `json:"ip,omitempty"`
	Port           uint16                    `json:"port,omitempty"`
	RtcpPort       *uint16                   `json:"rtcpPort,omitempty"`
	SrtpParameters *mediasoup.SrtpParameters `json:"srtpParameters,omitempty"`
}

type ICEServer struct {
	Urls       []string `json:"urls,omitempty"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

// SpeakerInfo provides audio level information for active speaker detection.
type SpeakerInfo struct {
	ParticipantSid string  `json:"participantSid"`
	Level          float32 `json:"level"`
	Active         bool    `json:"active"`
}

// VideoLayer describes a simulcast or SVC video layer.
type VideoLayer struct {
	Quality       VideoQuality `json:"quality,omitempty"`
	Width         uint32       `json:"width,omitempty"`
	Height        uint32       `json:"height,omitempty"`
	Bitrate       uint32       `json:"bitrate,omitempty"`
	Ssrc          uint32       `json:"ssrc,omitempty"`
	SpatialLayer  int32        `json:"spatialLayer,omitempty"`
	TemporalLayer int32        `json:"temporalLayer,omitempty"`
}

type VideoQuality int32

const (
	VideoQuality_LOW    VideoQuality = 0
	VideoQuality_MEDIUM VideoQuality = 1
	VideoQuality_HIGH   VideoQuality = 2
	VideoQuality_OFF    VideoQuality = 3
)
