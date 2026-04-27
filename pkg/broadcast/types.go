// Package broadcast implements "live-broadcast / 连麦切换":
// a single broadcast pushes one room participant's encoded tracks
// (passthrough, no transcode) to up to three external RTMP/WHIP
// destinations, while exposing a broadcast-scoped WHIP ingest endpoint
// that avflow (or any WHIP client) can use to push a mixed stream in.
// ICE state on the mix ingest decides which source (live user or mix)
// is currently forwarded to every destination; on the mix dropping we
// fall back to the live user. If the live user ever stops publishing
// the whole broadcast terminates with reason "live_identity_gone".
package broadcast

import "fmt"

// DestinationType is the wire protocol of a single push target.
type DestinationType string

const (
	// DestinationRTMP is classic RTMP(S), FLV over the wire. The URL
	// typically looks like rtmp://host/app/stream.
	DestinationRTMP DestinationType = "rtmp"
	// DestinationWHIP is WebRTC-HTTP Ingest (RFC 9725). The URL is the
	// POST endpoint where we submit our SDP offer and receive an
	// answer + Location for PATCH/DELETE.
	DestinationWHIP DestinationType = "whip"
)

// Destination is one external sink. URLs are validated at creation
// time; duplicates within a single Broadcast are rejected.
type Destination struct {
	Type DestinationType `json:"type"`
	URL  string          `json:"url"`

	// DTSReorder controls the (DTS, CTS) reconstruction buffer on
	// the egress side of this destination, applied to H.264 and
	// H.265 video samples. Pointer-typed so the JSON form
	// distinguishes the three usable settings:
	//
	//   nil    → auto (default).
	//   true   → always reorder.
	//   false  → always direct (no buffer; CTS=0 + PTS→DTS).
	//
	// In auto mode the egress starts in reorder-buffer mode and
	// monitors the input PTS sequence: once it has emitted enough
	// consecutive frames whose CTS came out as zero (i.e. the head
	// of the lookahead window was already the smallest PTS in the
	// window — the canonical "no reorder happening" signal), it
	// flips to direct passthrough so latency-sensitive pipelines do
	// not pay the buffering tax. If a later PTS regression is
	// observed the egress flips back into buffered mode at one
	// frame of jitter cost.
	//
	// Set to true to pin reorder on regardless of observed input
	// (useful for recording pipelines where ~133 ms of buffering
	// is fine and you want to avoid the rare flip-back jitter).
	// Set to false to pin it off when the upstream is guaranteed
	// B-frame-free (typical for WebRTC SFU producers and avflow
	// mixes) and you want zero buffer latency.
	//
	// Has no effect on VP9 / AV1 destinations: those codecs travel
	// through the Enhanced-RTMP CodedFramesX tag, which has no
	// CompositionTime field, so the egress always passes them
	// through unchanged regardless of this setting. H.265 uses the
	// CodedFrames (PacketType=1) tag with an SI24 CTS field, the
	// HEVC analog of the classic AVC tag — same reorder policy
	// applies.
	DTSReorder *bool `json:"dts_reorder,omitempty"`
}

// DTSReorderMode encodes the resolved (post-default) reorder policy
// for an outbound destination. Used by the pusher to wire the right
// shape of dtsReorder buffer.
type DTSReorderMode uint8

const (
	// DTSReorderAuto starts in buffered mode and adapts based on
	// the observed PTS sequence: switches to direct passthrough
	// after a stable streak of zero-CTS emits, and switches back
	// when a PTS regression is detected.
	DTSReorderAuto DTSReorderMode = iota
	// DTSReorderAlways pins the buffer on for the lifetime of the
	// pusher.
	DTSReorderAlways
	// DTSReorderNever skips the buffer entirely; H.264 / H.265
	// samples are muxed straight through with CTS=0.
	DTSReorderNever
)

// DTSReorderMode returns the effective reorder policy for d, applying
// the documented default ("absent → auto"). Centralized so call sites
// in the pusher / manager / tests cannot accidentally disagree about
// what nil means.
func (d Destination) DTSReorderMode() DTSReorderMode {
	if d.DTSReorder == nil {
		return DTSReorderAuto
	}
	if *d.DTSReorder {
		return DTSReorderAlways
	}
	return DTSReorderNever
}

// SourceKind names the active forwarding source inside the Switcher.
type SourceKind string

const (
	// SourceLive is the room participant's producer on this broadcast's
	// router. Used whenever no mix is connected (or mix has dropped).
	SourceLive SourceKind = "live"
	// SourceMix is the producer attached via the broadcast-scoped WHIP
	// ingest (typically avflow). Takes over from SourceLive on the next
	// key frame after the ingest reaches IceStateConnected/Completed.
	SourceMix SourceKind = "mix"
)

// TerminationReason is the cause recorded on Broadcast.Terminate for
// status responses and log lines. Kept as a small closed set so
// scripts and dashboards can switch on it.
type TerminationReason string

const (
	// TerminationDeleted is the clean DELETE /rtc/broadcast/{id} path.
	TerminationDeleted TerminationReason = "deleted"
	// TerminationLiveIdentityGone fires the moment the bound live
	// participant's producer closes (participant leaves, unpublishes,
	// or crashes). The task does not wait: it tears down every pusher
	// and the mix ingest immediately. Extends to "mix→live switchover
	// asks for a source that does not exist", which is the same root
	// cause.
	TerminationLiveIdentityGone TerminationReason = "live_identity_gone"
	// TerminationMixIceFailed reserved for future use when the mix
	// ingest has entered a state we cannot recover from while no live
	// source is available either. Currently unused — ICE disconnect on
	// the mix leg alone does not terminate, it just flips back to live.
	TerminationMixIceFailed TerminationReason = "mix_ice_failed"
)

// MaxDestinations caps the number of outbound pushes a single
// broadcast may fan out to. Requirement from the spec: 最多同时推 3 个地址。
const MaxDestinations = 3

// Validate performs the static, request-time checks a POST /rtc/broadcast
// body must pass before any mediasoup/Pion plumbing is touched.
func Validate(dsts []Destination) error {
	if len(dsts) == 0 {
		return ErrNoDestinations
	}
	if len(dsts) > MaxDestinations {
		return ErrTooManyDestinations
	}
	seen := make(map[string]struct{}, len(dsts))
	for i, d := range dsts {
		if d.URL == "" {
			return fmt.Errorf("destinations[%d]: empty url", i)
		}
		switch d.Type {
		case DestinationRTMP, DestinationWHIP:
		default:
			return fmt.Errorf("destinations[%d]: unknown type %q", i, d.Type)
		}
		if _, dup := seen[d.URL]; dup {
			return fmt.Errorf("destinations[%d]: duplicate url %q", i, d.URL)
		}
		seen[d.URL] = struct{}{}
	}
	return nil
}
