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
	RefreshToken          string              `json:"refreshToken,omitempty"`
	Room                  *CreateRoomRequest  `json:"room,omitempty"`
	ClientInfo            *ClientInfo         `json:"clientInfo,omitempty"`
	ConnectionSettings    *ConnectionSettings `json:"connectionSettings,omitempty"`
	Metadata              string              `json:"metadata,omitempty"`
	ParticipantAttributes map[string]string   `json:"participantAttributes,omitempty"`

	Reconnect       bool            `json:"reconnect,omitempty"`
	ReconnectReason ReconnectReason `json:"reconnectReason,omitempty"`
	ParticipantSid  string          `json:"participantSid,omitempty"`
	NodeId          string          `json:"nodeId,omitempty"`
}

type ConnectionSettings struct {
	AllowPublishing      bool                        `json:"allowPublishing,omitempty"`
	AllowSubscribing     bool                        `json:"allowSubscribing,omitempty"`
	AutoSubscribe        bool                        `json:"autoSubscribe,omitempty"`
	AutoSubscribeData    *bool                       `json:"autoSubscribeData,omitempty"`
	SubscriberAllowPause *bool                       `json:"subscriberAllowPause,omitempty"`
	AdaptiveStream       bool                        `json:"adaptiveStream,omitempty"`
	ForceTcp             bool                        `json:"forceTcp,omitempty"`
	DtlsFingerprints     []mediasoup.DtlsFingerprint `json:"dtlsFingerprints,omitempty"`
}

type CreateRoomRequest struct {
	Name             string `json:"name,omitempty"`
	RoomPreset       string `json:"roomPreset,omitempty"`
	EmptyTimeout     uint32 `json:"emptyTimeout,omitempty"`
	DepartureTimeout uint32 `json:"departureTimeout,omitempty"`
	MaxParticipants  uint32 `json:"maxParticipants,omitempty"`
	Metadata         string `json:"metadata,omitempty"`
}

// SyncState is sent by the client during reconnection to describe its
// last-known view of the room. The server uses this to compute a diff and
// notify the client of any tracks that appeared or disappeared while
// the connection was broken.
type SyncState struct {
	// TrackSids the client was subscribed to (by published track sid).
	SubscribedTrackSids []string `json:"subscribedTrackSids,omitempty"`
	// DataTrackSids the client was subscribed to.
	SubscribedDataTrackSids []string `json:"subscribedDataTrackSids,omitempty"`
	// PublishedTrackSids the client had published itself (for server to verify).
	PublishedTrackSids []string `json:"publishedTrackSids,omitempty"`
	// LastMessageTimestamp is the timestamp of the last chat message the client
	// received before disconnecting. The server returns messages newer than this.
	LastMessageTimestamp int64 `json:"lastMessageTimestamp,omitempty"`
}

type JoinResponse struct {
	Room                *Room                      `json:"room,omitempty"`
	Participant         *ParticipantInfo           `json:"participant,omitempty"`
	IceServers          []*ICEServer               `json:"iceServers,omitempty"`
	AlternativeUrl      string                     `json:"alternativeUrl,omitempty"`
	ClientConfiguration *ClientConfiguration       `json:"clientConfiguration,omitempty"`
	PingTimeout         int32                      `json:"pingTimeout,omitempty"`
	PingInterval        int32                      `json:"pingInterval,omitempty"`
	StatsInterval       int32                      `json:"statsInterval,omitempty"`
	ServerInfo          *ServerInfo                `json:"serverInfo,omitempty"`
	SendTransport       *TransportInfo             `json:"sendTransport,omitempty"`
	ReceiveTransport    *TransportInfo             `json:"receiveTransport,omitempty"`
	Signal              *SignalInfo                `json:"signal,omitempty"`
	RtpCapabilities     *mediasoup.RtpCapabilities `json:"rtpCapabilities,omitempty"`
	FullReconnect       bool                       `json:"fullReconnect,omitempty"`
	RefreshToken        string                     `json:"refreshToken,omitempty"`
	IceTransportPolicy  string                     `json:"iceTransportPolicy,omitempty"`
}

// ReconnectResponse is returned when a participant reconnects via ICE restart
// (server still has the session). Contains new IceParameters per transport.
type ReconnectResponse struct {
	IceParameters map[string]*mediasoup.IceParameters `json:"iceParameters"`
}

// ============================================================
// HTTP: Connect Flow (fallback for clients without pre-generated certificates)
// ============================================================

type ConnectRequest struct {
	NodeId         string             `json:"nodeId,omitempty"`
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
	SyncState       *SyncState                 `json:"syncState,omitempty"`
}

type ReadyResponse struct {
	OtherParticipants []*ParticipantInfo         `json:"otherParticipants,omitempty"`
	Room              *Room                      `json:"room,omitempty"`
	Groups            []*GroupInfo               `json:"groups,omitempty"`
	Messages          []*ChatMessageNotification `json:"messages,omitempty"`
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
	ClientTrackId     string                   `json:"clientTrackId"`
	Name              string                   `json:"name,omitempty"`
	Kind              mediasoup.MediaKind      `json:"kind"`
	RtpParameters     *mediasoup.RtpParameters `json:"rtpParameters"`
	Source            TrackSource              `json:"source,omitempty"`
	Width             uint32                   `json:"width,omitempty"`
	Height            uint32                   `json:"height,omitempty"`
	Muted             bool                     `json:"muted,omitempty"`
	VideoCodec        string                   `json:"videoCodec,omitempty"`
	BackupCodec       string                   `json:"backupCodec,omitempty"`
	BackupCodecPolicy BackupCodecPolicy        `json:"backupCodecPolicy,omitempty"`
	AppData           map[string]any           `json:"appData,omitempty"`
}

type PublishTrackResponse struct {
	TrackSid string `json:"trackSid"`
}

// PublishBackupTrackRequest is sent by the client to publish a backup codec
// stream for an already-published video track.
type PublishBackupTrackRequest struct {
	TrackSid      string                   `json:"trackSid"`
	ClientTrackId string                   `json:"clientTrackId"`
	RtpParameters *mediasoup.RtpParameters `json:"rtpParameters"`
}

type PublishBackupTrackResponse struct {
	BackupTrackSid string `json:"backupTrackSid"`
}

type UnpublishTrackRequest struct {
	TrackSid string `json:"trackSid"`
}

type MuteTrackRequest struct {
	TrackSid string `json:"trackSid"`
	Muted    bool   `json:"muted"`
}

type UpdateTrackRequest struct {
	TrackSid string `json:"trackSid"`
	Width    uint32 `json:"width,omitempty"`
	Height   uint32 `json:"height,omitempty"`
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
// Datachannel RPC: Subscription Permissions
// ============================================================

type TrackPermission struct {
	ParticipantIdentity string   `json:"participantIdentity,omitempty"`
	ParticipantSid      string   `json:"participantSid,omitempty"`
	AllTracks           bool     `json:"allowAll,omitempty"`
	TrackSids           []string `json:"allowedTrackSids,omitempty"`
}

type UpdateSubscriptionPermissionsRequest struct {
	AllParticipantsAllowed bool              `json:"allParticipantsAllowed"`
	TrackPermissions       []TrackPermission `json:"trackPermissions"`
}

// ============================================================
// Server -> Client: Token Refresh Notification
// ============================================================

type TokenRefreshedNotification struct {
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

// SubscribedQualityChangedNotification is sent by the server to the publisher
// when dynacast is enabled. It tells the publisher which simulcast layers
// currently have at least one subscriber, so the publisher can pause/unpause
// encodings to save bandwidth and CPU.
type SubscribedQualityChangedNotification struct {
	TrackSid            string              `json:"trackSid"`
	SubscribedQualities []SubscribedQuality `json:"subscribedQualities"`
	SubscribedCodecs    []SubscribedCodec   `json:"subscribedCodecs,omitempty"`
}

// SubscribedQuality reports whether a specific quality level has active subscribers.
type SubscribedQuality struct {
	Quality VideoQuality `json:"quality"`
	Enabled bool         `json:"enabled"`
}

// SubscribedCodec reports subscribed qualities per codec (for backup codec support).
type SubscribedCodec struct {
	Codec     string              `json:"codec"`
	Qualities []SubscribedQuality `json:"qualities"`
}

// SubscriptionStatusChangedNotification is sent to the publisher of an
// audio track when the aggregate subscription status changes. Video tracks
// use SubscribedQualityChangedNotification instead (the publisher can derive
// hasSubscribers from whether all qualities are disabled).
type SubscriptionStatusChangedNotification struct {
	TrackSid       string `json:"trackSid"`
	HasSubscribers bool   `json:"hasSubscribers"`
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

// ============================================================
// Client -> Server: Manual Subscription
// ============================================================

type SubscribeRequest struct {
	TrackSid string `json:"trackSid"`
}

type UnsubscribeRequest struct {
	TrackSid string `json:"trackSid"`
}

// ============================================================
// Client -> Server: Chat, Data Stream, P2P RPC (relay payloads)
// ============================================================

type ChatMessageNotification struct {
	ID                  string `json:"id"`
	Message             string `json:"message"`
	Timestamp           int64  `json:"timestamp"`
	EditTimestamp       *int64 `json:"editTimestamp,omitempty"`
	ParticipantSid      string `json:"participantSid,omitempty"`
	ParticipantIdentity string `json:"participantIdentity,omitempty"`
}

type RpcRequestNotification struct {
	RequestID           string `json:"requestId"`
	DestinationIdentity string `json:"destinationIdentity"`
	Method              string `json:"method"`
	Payload             string `json:"payload"`
	ResponseTimeout     int    `json:"responseTimeout,omitempty"`
	CallerIdentity      string `json:"callerIdentity"`
}

type RpcResponseNotification struct {
	RequestID      string    `json:"requestId"`
	CallerIdentity string    `json:"callerIdentity"`
	Payload        string    `json:"payload,omitempty"`
	Error          *RpcError `json:"error,omitempty"`
}

type RpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data,omitempty"`
}

// ============================================================
// Group (Breakout Room) Management
// ============================================================

type GroupInfo struct {
	GroupId          string                 `json:"groupId"`
	Name             string                 `json:"name"`
	Permissions      *ParticipantPermission `json:"permissions,omitempty"`
	ParticipantCount int                    `json:"participantCount"`
}

type CreateGroupRequest struct {
	Name        string                 `json:"name"`
	Permissions *ParticipantPermission `json:"permissions,omitempty"`
}

type CreateGroupResponse struct {
	Group *GroupInfo `json:"group"`
}

type DeleteGroupRequest struct {
	GroupId string `json:"groupId"`
}

type ListGroupsResponse struct {
	Groups []*GroupInfo `json:"groups"`
}

type JoinGroupRequest struct {
	GroupId string `json:"groupId"`
}

type JoinGroupResponse struct {
	OtherParticipants []*ParticipantInfo `json:"otherParticipants,omitempty"`
}

type LeaveGroupRequest struct{}

type GroupCreatedNotification struct {
	Group *GroupInfo `json:"group"`
}

type GroupDeletedNotification struct {
	GroupId string `json:"groupId"`
}

type GroupParticipantJoinedNotification struct {
	GroupId        string `json:"groupId"`
	ParticipantSid string `json:"participantSid"`
}

type GroupParticipantLeftNotification struct {
	GroupId        string `json:"groupId"`
	ParticipantSid string `json:"participantSid"`
}

// ============================================================
// Inter-node RPC (via NATS)
// ============================================================

type NodeRPCRequest struct {
	Action  string          `json:"action"`
	Payload json.RawMessage `json:"payload"`
}

type NodeRPCResponse struct {
	Status  int             `json:"status"`
	Payload json.RawMessage `json:"payload"`
}
