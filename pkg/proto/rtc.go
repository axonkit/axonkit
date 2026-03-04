package proto

import (
	"encoding/json"

	"github.com/jiyeyuran/mediasoup-go/v2"
)

// ============================================================
// HTTP: Join Flow
// ============================================================

type JoinRequest struct {
	Token                 string              `json:"token,omitempty"`
	DisplayName           string              `json:"displayName,omitempty"`
	Room                  *CreateRoomRequest  `json:"room,omitempty"`
	ClientInfo            *ClientInfo         `json:"clientInfo,omitempty"`
	ConnectionSettings    *ConnectionSettings `json:"connectionSettings,omitempty"`
	Metadata              string              `json:"metadata,omitempty"`
	ParticipantAttributes map[string]string   `json:"participantAttributes,omitempty"`

	Reconnect       bool            `json:"reconnect,omitempty"`
	ReconnectReason ReconnectReason `json:"reconnectReason,omitempty"`
	ParticipantSid  string          `json:"participantSid,omitempty"`
	SyncState       *SyncState      `json:"syncState,omitempty"`
}

type ConnectionSettings struct {
	AllowPublishing      bool                        `json:"allowPublishing,omitempty"`
	AllowSubscribing     bool                        `json:"allowSubscribing,omitempty"`
	AutoSubscribe        bool                        `json:"autoSubscribe,omitempty"`
	AutoSubscribeData    *bool                       `json:"autoSubscribeData,omitempty"`
	SubscriberAllowPause *bool                       `json:"subscriberAllowPause,omitempty"`
	AdaptiveStream       bool                        `json:"adaptiveStream,omitempty"`
	ForceTcp             bool                        `json:"forceTcp,omitempty"`
	UsePlainTransport    bool                        `json:"usePlainTransport,omitempty"`
	DtlsFingerprints     []mediasoup.DtlsFingerprint `json:"dtlsFingerprints,omitempty"`
}

type CreateRoomRequest struct {
	Name             string `json:"name,omitempty"`
	RoomPreset       string `json:"roomPreset,omitempty"`
	EmptyTimeout     uint32 `json:"emptyTimeout,omitempty"`
	DepartureTimeout uint32 `json:"departureTimeout,omitempty"`
	MaxParticipants  uint32 `json:"maxParticipants,omitempty"`
	NodeId           string `json:"nodeId,omitempty"`
	Metadata         string `json:"metadata,omitempty"`
}

type SyncState struct {
	RemoteTrackSids     []string           `json:"remoteTrackSids,omitempty"`
	RemoteDataTrackSids []string           `json:"remoteDataTrackSids,omitempty"`
	DataReceiveStates   []DataReceiveState `json:"dataReceiveStates,omitempty"`
}

type DataReceiveState struct {
	DataTrackSid string `json:"dataTrackSid,omitempty"`
	LastSeq      uint32 `json:"lastSeq,omitempty"`
}

type JoinResponse struct {
	Room                *Room                      `json:"room,omitempty"`
	Participant         *ParticipantInfo           `json:"participant,omitempty"`
	IceServers          []*ICEServer               `json:"iceServers,omitempty"`
	AlternativeUrl      string                     `json:"alternativeUrl,omitempty"`
	ClientConfiguration *ClientConfiguration       `json:"clientConfiguration,omitempty"`
	PingTimeout         int32                      `json:"pingTimeout,omitempty"`
	PingInterval        int32                      `json:"pingInterval,omitempty"`
	ServerInfo          *ServerInfo                `json:"serverInfo,omitempty"`
	SendTransport       *TransportInfo             `json:"sendTransport,omitempty"`
	ReceiveTransport    *TransportInfo             `json:"receiveTransport,omitempty"`
	Signal              *SignalInfo                `json:"signal,omitempty"`
	RtpCapabilities     *mediasoup.RtpCapabilities `json:"rtpCapabilities,omitempty"`
}

// ============================================================
// HTTP: Connect Flow (fallback for clients without pre-generated certificates)
// ============================================================

type ConnectRequest struct {
	ParticipantSid string             `json:"participantSid"`
	Transports     []TransportConnect `json:"transports"`
}

type TransportConnect struct {
	TransportSid   string                    `json:"transportSid"`
	DtlsParameters *mediasoup.DtlsParameters `json:"dtlsParameters"`
}

type ConnectResponse struct{}

// ============================================================
// Datachannel RPC: Lifecycle
// ============================================================

type ReadyRequest struct {
	RtpCapabilities *mediasoup.RtpCapabilities `json:"rtpCapabilities,omitempty"`
}

type ReadyResponse struct {
	OtherParticipants []*ParticipantInfo `json:"otherParticipants,omitempty"`
}

type LeaveRequest struct {
	Reason string `json:"reason,omitempty"`
}

type PingRequest struct {
	Timestamp int64 `json:"timestamp"`
}

type PongResponse struct {
	LastTimestamp int64 `json:"lastTimestamp"`
	ServerTime    int64 `json:"serverTime"`
}

// ============================================================
// Datachannel RPC: Track Operations
// ============================================================

// PublishTrackRequest is sent by client to publish a new media track.
// ClientTrackId is a client-generated correlation ID used to match the
// server response back to the local track object on the client side.
type PublishTrackRequest struct {
	ClientTrackId string                   `json:"clientTrackId"`
	Name          string                   `json:"name,omitempty"`
	Kind          mediasoup.MediaKind      `json:"kind"`
	RtpParameters *mediasoup.RtpParameters `json:"rtpParameters"`
	Source        TrackSource              `json:"source,omitempty"`
	Width         uint32                   `json:"width,omitempty"`
	Height        uint32                   `json:"height,omitempty"`
	Muted         bool                     `json:"muted,omitempty"`
	Layers        []*VideoLayer            `json:"layers,omitempty"`
	AppData       map[string]any           `json:"appData,omitempty"`
}

type PublishTrackResponse struct {
	TrackSid string `json:"trackSid"`
}

type UnpublishTrackRequest struct {
	TrackSid string `json:"trackSid"`
}

type MuteTrackRequest struct {
	TrackSid string `json:"trackSid"`
	Muted    bool   `json:"muted"`
}

type UpdateTrackRequest struct {
	TrackSid string        `json:"trackSid"`
	Width    uint32        `json:"width,omitempty"`
	Height   uint32        `json:"height,omitempty"`
	Layers   []*VideoLayer `json:"layers,omitempty"`
}

// ============================================================
// Datachannel RPC: Data Track Operations
// ============================================================

// PublishDataTrackRequest is sent by client to publish a new data track.
type PublishDataTrackRequest struct {
	ClientTrackId        string                          `json:"clientTrackId"`
	TransportSid         string                          `json:"transportSid"`
	SctpStreamParameters *mediasoup.SctpStreamParameters `json:"sctpStreamParameters,omitempty"`
	Name                 string                          `json:"name,omitempty"`
	Label                string                          `json:"label,omitempty"`
	Protocol             string                          `json:"protocol,omitempty"`
	AppData              map[string]any                  `json:"appData,omitempty"`
}

type PublishDataTrackResponse struct {
	DataTrackSid string `json:"dataTrackSid"`
}

type UnpublishDataTrackRequest struct {
	DataTrackSid string `json:"dataTrackSid"`
}

// ============================================================
// Datachannel RPC: Subscription Operations
// ============================================================

// CreateSubscriptionRequest is sent by server to client for auto-subscribe.
// The server creates the subscription paused, sends this RPC, waits for the
// client to acknowledge, then resumes the subscription to avoid sending media
// before the client is ready.
type CreateSubscriptionRequest struct {
	SubscriptionSid  string                   `json:"subscriptionSid"`
	TrackSid         string                   `json:"trackSid"`
	PublisherSid     string                   `json:"publisherSid"`
	Kind             mediasoup.MediaKind      `json:"kind"`
	RtpParameters    *mediasoup.RtpParameters `json:"rtpParameters"`
	SubscriptionType string                   `json:"subscriptionType"`
	TrackMuted       bool                     `json:"trackMuted"`
	AppData          map[string]any           `json:"appData,omitempty"`
}

type CreateSubscriptionResponse struct {
	Ok bool `json:"ok"`
}

type PauseSubscriptionRequest struct {
	SubscriptionSid string `json:"subscriptionSid"`
}

type ResumeSubscriptionRequest struct {
	SubscriptionSid string `json:"subscriptionSid"`
}

type SetPreferredLayersRequest struct {
	SubscriptionSid string `json:"subscriptionSid"`
	SpatialLayer    int32  `json:"spatialLayer"`
	TemporalLayer   *int32 `json:"temporalLayer,omitempty"`
}

type SetPriorityRequest struct {
	SubscriptionSid string `json:"subscriptionSid"`
	Priority        int32  `json:"priority"`
}

type RequestKeyFrameRequest struct {
	SubscriptionSid string `json:"subscriptionSid"`
}

type CloseSubscriptionRequest struct {
	SubscriptionSid string `json:"subscriptionSid"`
}

// ============================================================
// Datachannel RPC: Data Subscription Operations
// ============================================================

type CreateDataSubscriptionRequest struct {
	DataSubscriptionSid  string                          `json:"dataSubscriptionSid"`
	DataTrackSid         string                          `json:"dataTrackSid"`
	PublisherSid         string                          `json:"publisherSid"`
	SctpStreamParameters *mediasoup.SctpStreamParameters `json:"sctpStreamParameters"`
	Label                string                          `json:"label,omitempty"`
	Protocol             string                          `json:"protocol,omitempty"`
	AppData              map[string]any                  `json:"appData,omitempty"`
}

type CreateDataSubscriptionResponse struct {
	Ok bool `json:"ok"`
}

type CloseDataSubscriptionRequest struct {
	DataSubscriptionSid string `json:"dataSubscriptionSid"`
}

// ============================================================
// Datachannel RPC: Transport Operations
// ============================================================

type GetTransportStatsRequest struct {
	TransportSid string `json:"transportSid"`
}

type TransportStatsResponse struct {
	Stats json.RawMessage `json:"stats"`
}

// ============================================================
// Datachannel RPC: Participant Operations
// ============================================================

type UpdateParticipantRequest struct {
	Metadata   *string           `json:"metadata,omitempty"`
	Name       *string           `json:"name,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// ============================================================
// Datachannel RPC: Token Refresh
// ============================================================

type RefreshTokenRequest struct {
	Token string `json:"token"`
}

type RefreshTokenResponse struct {
	Token string `json:"token"`
}

// ============================================================
// Server -> Client Notifications
// ============================================================

type ParticipantJoinedNotification struct {
	Participant *ParticipantInfo `json:"participant"`
}

type ParticipantLeftNotification struct {
	ParticipantSid string           `json:"participantSid"`
	Reason         DisconnectReason `json:"reason"`
}

type ParticipantUpdatedNotification struct {
	Participant *ParticipantInfo `json:"participant"`
}

type TrackPublishedNotification struct {
	ParticipantSid string     `json:"participantSid"`
	Track          *TrackInfo `json:"track"`
}

type TrackUnpublishedNotification struct {
	ParticipantSid string `json:"participantSid"`
	TrackSid       string `json:"trackSid"`
}

type TrackMutedNotification struct {
	ParticipantSid string `json:"participantSid"`
	TrackSid       string `json:"trackSid"`
	Muted          bool   `json:"muted"`
}

type ActiveSpeakersNotification struct {
	Speakers []*SpeakerInfo `json:"speakers"`
}

type RoomUpdatedNotification struct {
	Room *Room `json:"room"`
}

type SubscriptionClosedNotification struct {
	SubscriptionSid string `json:"subscriptionSid"`
}

type DataSubscriptionClosedNotification struct {
	DataSubscriptionSid string `json:"dataSubscriptionSid"`
}

type DisconnectNotification struct {
	Reason DisconnectReason `json:"reason"`
}
