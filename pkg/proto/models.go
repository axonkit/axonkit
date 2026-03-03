package proto

import "github.com/jiyeyuran/mediasoup-go/v2"

// details about the client
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
	// wifi, wired, cellular, vpn, empty if not known
	Network string `json:"network,omitempty"`
	// comma separated list of additional LiveKit SDKs in use of this client, with versions
	// e.g. "components-js:1.2.3,track-processors-js:1.2.3"
	OtherSdks string `json:"otherSdks,omitempty"`
	// client protocol version
	ClientProtocol int32 `json:"clientProtocol,omitempty"`
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
	Sid      string                `json:"sid,omitempty"`
	Identity string                `json:"identity,omitempty"`
	State    ParticipantInfo_State `json:"state,omitempty"`
	Tracks   []*TrackInfo          `json:"tracks,omitempty"`
	Metadata string                `json:"metadata,omitempty"`
	// timestamp when participant joined room, in milliseconds
	JoinedAt   int64                  `json:"joinedAt,omitempty"`
	Name       string                 `json:"name,omitempty"`
	Version    uint32                 `json:"version,omitempty"`
	Permission *ParticipantPermission `json:"permission,omitempty"`
	Region     string                 `json:"region,omitempty"`
	// indicates the participant has an active publisher connection
	// and can publish to the server
	IsPublisher      bool                         `json:"isPublisher,omitempty"`
	Kind             ParticipantInfo_Kind         `json:"kind,omitempty"`
	Attributes       map[string]string            `json:"attributes,omitempty"`
	DisconnectReason DisconnectReason             `json:"disconnectReason,omitempty"`
	KindDetails      []ParticipantInfo_KindDetail `json:"kindDetails,omitempty"`
	DataTracks       []*DataTrackInfo             `json:"dataTracks,omitempty"`
	// protocol version used for client feature compatibility
	ClientProtocol int32 `json:"clientProtocol,omitempty"`
}

type ParticipantInfo_State int32

const (
	// received client join request, but not offered yet
	ParticipantInfo_JOINING ParticipantInfo_State = 0
	// ICE connectivity established
	ParticipantInfo_ACTIVE ParticipantInfo_State = 1
	// server received client ready request
	ParticipantInfo_JOINED ParticipantInfo_State = 2
	// transport disconnected
	ParticipantInfo_DISCONNECTED ParticipantInfo_State = 3
)

type TrackInfo struct {
	Sid   string    `json:"sid,omitempty"`
	Type  TrackType `json:"type,omitempty"`
	Name  string    `json:"name,omitempty"`
	Muted bool      `json:"muted,omitempty"`
	// original width of video (unset for audio)
	// clients may receive a lower resolution version with simulcast
	Width uint32 `json:"width,omitempty"`
	// original height of video (unset for audio)
	Height uint32 `json:"height,omitempty"`
	// source of media
	Source            TrackSource         `json:"source,omitempty"`
	Version           *TimedVersion       `json:"version,omitempty"`
	AudioFeatures     []AudioTrackFeature `json:"audioFeatures,omitempty"`
	BackupCodecPolicy BackupCodecPolicy   `json:"backupCodecPolicy,omitempty"`
	AppData           map[string]any      `json:"appData,omitempty"`
}

type TrackType int32

const (
	TrackType_AUDIO TrackType = 0
	TrackType_VIDEO TrackType = 1
	TrackType_DATA  TrackType = 2
)

type TrackSource int32

const (
	TrackSource_UNKNOWN            TrackSource = 0
	TrackSource_CAMERA             TrackSource = 1
	TrackSource_MICROPHONE         TrackSource = 2
	TrackSource_SCREEN_SHARE       TrackSource = 3
	TrackSource_SCREEN_SHARE_AUDIO TrackSource = 4
)

type AudioTrackFeature int32

const (
	AudioTrackFeature_TF_STEREO                      AudioTrackFeature = 0
	AudioTrackFeature_TF_NO_DTX                      AudioTrackFeature = 1
	AudioTrackFeature_TF_AUTO_GAIN_CONTROL           AudioTrackFeature = 2
	AudioTrackFeature_TF_ECHO_CANCELLATION           AudioTrackFeature = 3
	AudioTrackFeature_TF_NOISE_SUPPRESSION           AudioTrackFeature = 4
	AudioTrackFeature_TF_ENHANCED_NOISE_CANCELLATION AudioTrackFeature = 5
	AudioTrackFeature_TF_PRECONNECT_BUFFER           AudioTrackFeature = 6 // client will buffer audio once available and send it to the server via bytes stream once connected
)

// Policy for publisher to handle subscribers that are unable to support the primary codec of a track
type BackupCodecPolicy int32

const (
	// default behavior, the track prefer to regress to backup codec and all subscribers will receive the backup codec,
	// the sfu will try to regress codec if possible but not assured.
	BackupCodecPolicy_PREFER_REGRESSION BackupCodecPolicy = 0
	// encoding/send the primary and backup codec simultaneously
	BackupCodecPolicy_SIMULCAST BackupCodecPolicy = 1
	// force the track to regress to backup codec, this option can be used in video conference or the publisher has limited bandwidth/encoding power
	BackupCodecPolicy_REGRESSION BackupCodecPolicy = 2
)

type ParticipantPermission struct {
	// allow participant to subscribe to other tracks in the room
	CanSubscribe bool `json:"canSubscribe,omitempty"`
	// allow participant to publish new tracks to room
	CanPublish bool `json:"canPublish,omitempty"`
	// allow participant to publish data
	CanPublishData bool `json:"canPublishData,omitempty"`
	// sources that are allowed to be published
	CanPublishSources []TrackSource `json:"canPublishSources,omitempty"`
	// indicates that it's hidden to others
	Hidden bool `json:"hidden,omitempty"`
	// indicates that participant can update own metadata and attributes
	CanUpdateMetadata bool `json:"canUpdateMetadata,omitempty"`
	// if a participant can subscribe to metrics
	CanSubscribeMetrics bool `json:"canSubscribeMetrics,omitempty"`
	// if a participant can manage an agent session via RemoteSession (control and access state)
	CanManageAgentSession bool `json:"canManageAgentSession,omitempty"`
}

type ParticipantInfo_Kind int32

const (
	// standard participants, e.g. web clients
	ParticipantInfo_STANDARD ParticipantInfo_Kind = 0
	// only ingests streams
	ParticipantInfo_INGRESS ParticipantInfo_Kind = 1
	// only consumes streams
	ParticipantInfo_EGRESS ParticipantInfo_Kind = 2
	// SIP participants
	ParticipantInfo_SIP ParticipantInfo_Kind = 3
	// LiveKit agents
	ParticipantInfo_AGENT ParticipantInfo_Kind = 4
	// Connectors participants
	ParticipantInfo_CONNECTOR ParticipantInfo_Kind = 7
	// Bridge participants
	ParticipantInfo_BRIDGE ParticipantInfo_Kind = 8 // NEXT_ID: 9
)

type DisconnectReason int32

const (
	DisconnectReason_UNKNOWN_REASON DisconnectReason = 0
	// the client initiated the disconnect
	DisconnectReason_CLIENT_INITIATED DisconnectReason = 1
	// another participant with the same identity has joined the room
	DisconnectReason_DUPLICATE_IDENTITY DisconnectReason = 2
	// the server instance is shutting down
	DisconnectReason_SERVER_SHUTDOWN DisconnectReason = 3
	// RoomService.RemoveParticipant was called
	DisconnectReason_PARTICIPANT_REMOVED DisconnectReason = 4
	// RoomService.DeleteRoom was called
	DisconnectReason_ROOM_DELETED DisconnectReason = 5
	// the client is attempting to resume a session, but server is not aware of it
	DisconnectReason_STATE_MISMATCH DisconnectReason = 6
	// client was unable to connect fully
	DisconnectReason_JOIN_FAILURE DisconnectReason = 7
	// Cloud-only, the server requested Participant to migrate the connection elsewhere
	DisconnectReason_MIGRATION DisconnectReason = 8
	// the signal ice was closed unexpectedly
	DisconnectReason_SIGNAL_CLOSE DisconnectReason = 9
	// the room was closed, due to all Standard and Ingress participants having left
	DisconnectReason_ROOM_CLOSED DisconnectReason = 10
	// SIP callee did not respond in time
	DisconnectReason_USER_UNAVAILABLE DisconnectReason = 11
	// SIP callee rejected the call (busy)
	DisconnectReason_USER_REJECTED DisconnectReason = 12
	// SIP protocol failure or unexpected response
	DisconnectReason_SIP_TRUNK_FAILURE DisconnectReason = 13
	// server timed out a participant session
	DisconnectReason_CONNECTION_TIMEOUT DisconnectReason = 14
	// media stream failure or media timeout
	DisconnectReason_MEDIA_FAILURE DisconnectReason = 15
)

type ParticipantInfo_KindDetail int32

const (
	ParticipantInfo_CLOUD_AGENT        ParticipantInfo_KindDetail = 0
	ParticipantInfo_FORWARDED          ParticipantInfo_KindDetail = 1
	ParticipantInfo_CONNECTOR_WHATSAPP ParticipantInfo_KindDetail = 2
	ParticipantInfo_CONNECTOR_TWILIO   ParticipantInfo_KindDetail = 3
	ParticipantInfo_BRIDGE_RTSP        ParticipantInfo_KindDetail = 4 // NEXT_ID: 5
)

type DataTrackInfo struct {
	// Server-assigned track identifier.
	Sid string `json:"sid,omitempty"`
	// Human-readable identifier (e.g., `geoLocation`, `servoPosition.x`, etc.), unique per publisher.
	Name    string         `json:"name,omitempty"`
	AppData map[string]any `json:"appData,omitempty"`
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
	// disabled for both publish and subscribe
	Codecs []*Codec `json:"codecs,omitempty"`
	// only disable for publish
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
	Edition  ServerInfo_Edition `json:"edition,omitempty"`
	Version  string             `json:"version,omitempty"`
	Protocol int32              `json:"protocol,omitempty"`
	Region   string             `json:"region,omitempty"`
	NodeId   string             `json:"nodeId,omitempty"`
	// additional debugging information. sent only if server is in development mode
	DebugInfo     string `json:"debugInfo,omitempty"`
	AgentProtocol int32  `json:"agentProtocol,omitempty"`
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

	// webrtc transport
	IceParameters  *mediasoup.IceParameters  `json:"iceParameters,omitempty"`
	IceCandidates  []mediasoup.IceCandidate  `json:"iceCandidates,omitempty"`
	DtlsParameters *mediasoup.DtlsParameters `json:"dtlsParameters,omitempty"`
	SctpParameters *mediasoup.SctpParameters `json:"sctpParameters,omitempty"`

	// plain transport
	Ip             string                    `json:"ip,omitempty"`
	Port           uint16                    `json:"port,omitempty"`
	RtcpPort       *uint16                   `json:"rtcpPort,omitempty"`
	SrtpParameters *mediasoup.SrtpParameters `json:"srtpParameters,omitempty"`
}

type ConsumerInfo struct {
	Sid            string                   `json:"sid,omitempty"`
	TrackId        string                   `json:"trackId,omitempty"`
	Kind           mediasoup.MediaKind      `json:"kind,omitempty"`
	RtpParameters  *mediasoup.RtpParameters `json:"rtpParameters,omitempty"`
	Type           mediasoup.ConsumerType   `json:"type,omitempty"`
	AppData        map[string]any           `json:"appData,omitempty"`
	ProducerPaused bool                     `json:"producerPaused,omitempty"`
}

type DataConsumerInfo struct {
	Sid                  string                          `json:"sid,omitempty"`
	DataTrackId          string                          `json:"dataTrackId,omitempty"`
	SctpStreamParameters *mediasoup.SctpStreamParameters `json:"sctpStreamParameters,omitempty"`
	Label                string                          `json:"label,omitempty"`
	Protocol             string                          `json:"protocol,omitempty"`
	AppData              map[string]any                  `json:"appData,omitempty"`
}
