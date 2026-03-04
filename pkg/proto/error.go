package proto

type ErrorResponse struct {
	Code  ErrorCode `json:"code"`
	Error string    `json:"error"`
}

type ErrorCode string

const (
	ErrCodeInvalidToken         ErrorCode = "INVALID_TOKEN"
	ErrCodeTokenExpired         ErrorCode = "TOKEN_EXPIRED"
	ErrCodeRoomFull             ErrorCode = "ROOM_FULL"
	ErrCodeRoomNotFound         ErrorCode = "ROOM_NOT_FOUND"
	ErrCodeDuplicateIdentity    ErrorCode = "DUPLICATE_IDENTITY"
	ErrCodePermissionDenied     ErrorCode = "PERMISSION_DENIED"
	ErrCodeTrackNotFound        ErrorCode = "TRACK_NOT_FOUND"
	ErrCodeSubscriptionNotFound ErrorCode = "SUBSCRIPTION_NOT_FOUND"
	ErrCodeTransportNotFound    ErrorCode = "TRANSPORT_NOT_FOUND"
	ErrCodeInvalidRequest       ErrorCode = "INVALID_REQUEST"
	ErrCodeInternal             ErrorCode = "INTERNAL_ERROR"
	ErrCodeNotReady             ErrorCode = "NOT_READY"
	ErrCodeAlreadyJoined        ErrorCode = "ALREADY_JOINED"
	ErrCodeNotFound             ErrorCode = "NOT_FOUND"
	ErrCodeSessionNotFound      ErrorCode = "SESSION_NOT_FOUND"
)
