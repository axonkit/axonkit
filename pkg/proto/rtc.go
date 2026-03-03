package proto

import (
	"github.com/jiyeyuran/mediasoup-go/v2"
)

type JoinRequest struct {
	DisplayName        string              `json:"displayName,omitempty"`
	Room               *CreateRoomRequest  `json:"room,omitempty"`
	ClientInfo         *ClientInfo         `json:"clientInfo,omitempty"`
	ConnectionSettings *ConnectionSettings `json:"connectionSettings,omitempty"`
	Metadata           string              `json:"metadata,omitempty"`
	// will set keys provided via this
	// will overwrite if the same key is in the token
	// will not delete keys from token if there is a key collision and this sets that key to empty value
	ParticipantAttributes map[string]string `json:"participantAttributes,omitempty"`

	// information for reconnection
	Reconnect       bool            `json:"reconnect,omitempty"`
	ReconnectReason ReconnectReason `json:"reconnectReason,omitempty"`
	ParticipantSid  string          `json:"participantSid,omitempty"`
	SyncState       *SyncState      `json:"syncState,omitempty"`
}

type ConnectionSettings struct {
	AllowPublishing      bool  `json:"allowPublishing,omitempty"`
	AllowSubscribing     bool  `json:"allowSubscribing,omitempty"`
	AutoSubscribe        bool  `json:"autoSubscribe,omitempty"`
	AutoSubscribeData    *bool `json:"autoSubscribeData,omitempty"`
	SubscriberAllowPause *bool `json:"subscriberAllowPause,omitempty"`
	AdaptiveStream       bool  `json:"adaptiveStream,omitempty"`
	ForceTcp             bool  `json:"forceTcp,omitempty"`
	UsePlainTransport    bool  `json:"usePlainTransport,omitempty"`
	// DtlsFingerprints is used to connect webrtc transport in advance
	DtlsFingerprints []mediasoup.DtlsFingerprint `json:"dtlsFingerprints,omitempty"`
}

type CreateRoomRequest struct {
	// name of the room
	Name string `json:"name,omitempty"`
	// configuration to use for this room parameters. Setting parameters below override the config defaults.
	RoomPreset string `json:"roomPreset,omitempty"`
	// number of seconds to keep the room open if no one joins
	EmptyTimeout uint32 `json:"emptyTimeout,omitempty"`
	// number of seconds to keep the room open after everyone leaves
	DepartureTimeout uint32 `json:"departureTimeout,omitempty"`
	// limit number of participants that can be in a room
	MaxParticipants uint32 `json:"maxParticipants,omitempty"`
	// override the node room is allocated to, for debugging
	NodeId string `json:"nodeId,omitempty"`
	// metadata of room
	Metadata string `json:"metadata,omitempty"`
}

type SyncState struct {
	RemoteTrackIds     []string           `json:"remoteTrackIds,omitempty"`
	RemoteDataTrackIds []string           `json:"remoteDataTrackIds,omitempty"`
	DataReceiveStates  []DataReceiveState `json:"dataReceiveStates,omitempty"`
}

type DataReceiveState struct {
	DataTrackId string `json:"dataTrackId,omitempty"`
	LastSeq     uint32 `json:"lastSeq,omitempty"`
}

type JoinResponse struct {
	Room        *Room            `json:"room,omitempty"`
	Participant *ParticipantInfo `json:"participant,omitempty"`
	IceServers  []*ICEServer     `json:"iceServers,omitempty"`
	// when the current server isn't available, return alternate url to retry connection
	// when this is set, the other fields will be largely empty
	AlternativeUrl      string                     `json:"alternativeUrl,omitempty"`
	ClientConfiguration *ClientConfiguration       `json:"clientConfiguration,omitempty"`
	PingTimeout         int32                      `json:"pingTimeout,omitempty"`
	PingInterval        int32                      `json:"pingInterval,omitempty"`
	ServerInfo          *ServerInfo                `json:"serverInfo,omitempty"`
	SendTransport       *TransportInfo             `json:"sendTransport,omitempty"`
	ReceiveTransport    *TransportInfo             `json:"receiveTransport,omitempty"`
	Signal              *DataConsumerInfo          `json:"signal,omitempty"`
	RtpCapabilities     *mediasoup.RtpCapabilities `json:"rtpCapabilities,omitempty"`
}

type ICEServer struct {
	Urls       []string `json:"urls,omitempty"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

// ReadyRequest is used to signal that the participant is ready to receive media or data.
// ReadyRequest should be sent in datachannel.
type ReadyRequest struct {
	RtpCapabilities *mediasoup.RtpCapabilities `json:"rtpCapabilities,omitempty"`
}

type ReadyResponse struct {
	OtherParticipants []*ParticipantInfo `json:"otherParticipants,omitempty"`
}
