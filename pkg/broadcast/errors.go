package broadcast

import "errors"

var (
	// ErrNoDestinations is returned for an empty destinations list.
	ErrNoDestinations = errors.New("broadcast: at least one destination required")
	// ErrTooManyDestinations is returned when more than MaxDestinations
	// destinations are supplied in a single POST.
	ErrTooManyDestinations = errors.New("broadcast: at most 3 destinations allowed")
	// ErrIdentityNotPublishing is returned when the requested room
	// participant does not currently have any live producers. Per spec,
	// creation fails hard and the caller must retry after the user
	// starts publishing.
	ErrIdentityNotPublishing = errors.New("broadcast: participant identity is not publishing")
	// ErrRoomNotFound is returned when the requested room does not exist
	// on this node (or in the cross-node store).
	ErrRoomNotFound = errors.New("broadcast: room not found")
	// ErrNotFound is returned when a broadcast id is looked up via GET /
	// PATCH / DELETE after the task has already been dropped from the
	// Manager.
	ErrNotFound = errors.New("broadcast: not found")
	// ErrTerminated is returned when a PATCH or mix-ingest op is invoked
	// on a broadcast that has already self-terminated (e.g. the live
	// identity disappeared).
	ErrTerminated = errors.New("broadcast: already terminated")
	// ErrMixAlreadyConnected is returned when POST /rtc/broadcast/{id}/whip
	// is called a second time while a previous mix ingest is still live.
	// Clients should DELETE the prior resource before opening a new one.
	ErrMixAlreadyConnected = errors.New("broadcast: mix ingest already connected")
)
