package proto

import (
	"github.com/axonkit/axonkit/pkg/signal"
)

// RPC method constants for datachannel communication.
const (
	// Client -> Server: Lifecycle
	MethodReady = "ready"
	MethodLeave = "leave"
	MethodPing  = "ping"
	MethodPong  = "pong"

	// Client -> Server: Track operations
	MethodTrackPublish       = "track.publish"
	MethodTrackPublishBackup = "track.publishBackup"
	MethodTrackUnpublish     = "track.unpublish"
	MethodTrackMute          = "track.mute"
	MethodTrackUpdate        = "track.update"

	// Client -> Server: Data track operations
	MethodDataTrackPublish   = "dataTrack.publish"
	MethodDataTrackUnpublish = "dataTrack.unpublish"

	// Client -> Server: Subscription operations
	MethodSubscriptionPause              = "subscription.pause"
	MethodSubscriptionResume             = "subscription.resume"
	MethodSubscriptionSetPreferredLayers = "subscription.setPreferredLayers"
	MethodSubscriptionSetPriority        = "subscription.setPriority"
	MethodSubscriptionRequestKeyFrame    = "subscription.requestKeyFrame"
	MethodSubscriptionClose              = "subscription.close"

	// Client -> Server: Data subscription operations
	MethodDataSubscriptionClose = "dataSubscription.close"

	// Client -> Server: Transport
	MethodTransportGetStats = "transport.getStats"

	// Client -> Server: Participant
	MethodParticipantUpdate = "participant.update"

	// Client -> Server: Stats (notification, no response)
	MethodStatsReport = "stats.report"

	// Client -> Server: Manual subscription control
	MethodSubscriptionSubscribe         = "subscription.subscribe"
	MethodSubscriptionUnsubscribe       = "subscription.unsubscribe"
	MethodSubscriptionUpdatePermissions = "subscription.updatePermissions"

	// Client -> Server: Pin/Unpin participant (lastN)
	MethodPinParticipant   = "participant.pin"
	MethodUnpinParticipant = "participant.unpin"

	// Client -> Server: Group (breakout room) operations
	MethodGroupCreate = "group.create"
	MethodGroupDelete = "group.delete"
	MethodGroupList   = "group.list"
	MethodGroupJoin   = "group.join"
	MethodGroupLeave  = "group.leave"

	// Client -> Server: Message (chat, whiteboard, document, etc.)
	MethodMessage             = "message.send"
	MethodMessageDelete       = "message.delete"
	MethodMessageDeleteByType = "message.deleteByType"

	// Client -> Server: Room shared state
	MethodStateUpdate = "state.update"
	MethodStateDelete = "state.delete"

	// Client -> Server: P2P RPC (notifications, server relays)
	MethodRpcRequest  = "rpc.request"
	MethodRpcResponse = "rpc.response"

	// Server -> Client: P2P RPC relay
	MethodRpcInvoke = "rpc.invoke"

	// Server -> Client: Subscription lifecycle (RPC, requires response)
	MethodSubscriptionCreate     = "subscription.create"
	MethodDataSubscriptionCreate = "dataSubscription.create"

	// Server -> Client: Notifications (one-way)
	MethodSubscriptionClosed             = "subscription.closed"
	MethodDataSubscriptionClosed         = "dataSubscription.closed"
	MethodParticipantJoined              = "participant.joined"
	MethodParticipantLeft                = "participant.left"
	MethodParticipantUpdated             = "participant.updated"
	MethodTrackPublished                 = "track.published"
	MethodTrackUnpublished               = "track.unpublished"
	MethodDataTrackPublished             = "dataTrack.published"
	MethodDataTrackUnpublished           = "dataTrack.unpublished"
	MethodTrackMuted                     = "track.muted"
	MethodTrackSubscribedQualityChanged  = "track.subscribedQualityChanged"
	MethodTrackSubscriptionStatusChanged = "track.subscriptionStatusChanged"
	MethodActiveSpeakersChanged          = "activeSpeakers.changed"
	MethodRoomUpdated                    = "room.updated"
	MethodDisconnect                     = "disconnect"

	// Server -> Client: Token refresh
	MethodTokenRefreshed = "token.refreshed"

	// Server -> Client: Message notifications
	MethodMessageReceived = "message.received"
	MethodMessageDeleted  = "message.deleted"

	// Server -> Client: Room shared state notifications
	MethodStateUpdated = "state.updated"
	MethodStateDeleted = "state.deleted"

	// Server -> Client: Group notifications
	MethodGroupCreated           = "group.created"
	MethodGroupUpdated           = "group.updated"
	MethodGroupDeleted           = "group.deleted"
	MethodGroupParticipantJoined = "group.participantJoined"
	MethodGroupParticipantLeft   = "group.participantLeft"
)

// Status codes, HTTP-compatible.
const (
	StatusOK            = 200
	StatusBadRequest    = 400
	StatusUnauthorized  = 401
	StatusForbidden     = 403
	StatusNotFound      = 404
	StatusConflict      = 409
	StatusInternalError = 500
)

// Re-export signal wire types and constants for backward compatibility.
type (
	Header      = signal.Header
	Message     = signal.Message
	MessageType = signal.MessageType
)

const (
	HdrError    = signal.HdrError
	HdrEncoding = signal.HdrEncoding

	TypeRequest      = signal.TypeRequest
	TypeResponse     = signal.TypeResponse
	TypeNotification = signal.TypeNotification
)

var (
	ErrInvalidMessage = signal.ErrInvalidMessage
	ErrBadHeader      = signal.ErrBadHeader
)

// Wire format functions: delegate to protocol/signal.
func DecodeMessage(data []byte) (*Message, error) {
	return signal.DecodeMessage(data)
}

func NewRequest(method, logId string, data []byte) *Message {
	return signal.NewRequest(method, logId, data)
}

func NewResponse(logId string, status int, data []byte) *Message {
	return signal.NewResponse(logId, status, data)
}

func NewErrorResponse(logId string, status int, errMsg string) *Message {
	return signal.NewErrorResponse(logId, status, errMsg)
}

func NewNotification(method string, data []byte) *Message {
	return signal.NewNotification(method, data)
}

func NewJSONRequest(method, logId string, body any) (*Message, error) {
	return signal.NewJSONRequest(method, logId, body)
}

func NewJSONResponse(logId string, status int, body any) (*Message, error) {
	return signal.NewJSONResponse(logId, status, body)
}

func NewJSONNotification(method string, body any) (*Message, error) {
	return signal.NewJSONNotification(method, body)
}
