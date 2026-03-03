package proto

import (
	"errors"
	"maps"
)

type RoomConfiguration struct{}

var ErrSensitiveCredentials = errors.New("room configuration should not contain sensitive credentials")

func (c *RoomConfiguration) Clone() *RoomConfiguration {
	if c == nil {
		return nil
	}
	return &RoomConfiguration{}
}

type ClaimGrants struct {
	Identity      string              `json:"identity,omitempty"`
	Name          string              `json:"name,omitempty"`
	Kind          string              `json:"kind,omitempty"`
	KindDetails   []string            `json:"kindDetails,omitempty"`
	Video         *VideoGrant         `json:"video,omitempty"`
	SIP           *SIPGrant           `json:"sip,omitempty"`
	Agent         *AgentGrant         `json:"agent,omitempty"`
	Inference     *InferenceGrant     `json:"inference,omitempty"`
	Observability *ObservabilityGrant `json:"observability,omitempty"`
	// Room configuration to use if this participant initiates the room
	RoomConfig *RoomConfiguration `json:"roomConfig,omitempty"`
	// Cloud-only, config preset to use
	// when both room and roomPreset are set, parameters in room overrides the preset
	RoomPreset string `json:"roomPreset,omitempty"`
	// for verifying integrity of the message body
	Sha256   string `json:"sha256,omitempty"`
	Metadata string `json:"metadata,omitempty"`
	// Key/value attributes to attach to the participant
	Attributes map[string]string `json:"attributes,omitempty"`
}

func (c *ClaimGrants) Clone() *ClaimGrants {
	if c == nil {
		return nil
	}

	clone := *c
	clone.Video = c.Video.Clone()
	clone.SIP = c.SIP.Clone()
	clone.Agent = c.Agent.Clone()
	clone.Inference = c.Inference.Clone()
	clone.Observability = c.Observability.Clone()
	clone.Attributes = maps.Clone(c.Attributes)
	clone.RoomConfig = c.RoomConfig.Clone()
	if len(c.KindDetails) > 0 {
		clone.KindDetails = append([]string{}, c.KindDetails...)
	}

	return &clone
}

// -------------------------------------------------------------

type VideoGrant struct {
	// actions on rooms
	RoomCreate bool `json:"roomCreate,omitempty"`
	RoomList   bool `json:"roomList,omitempty"`
	RoomRecord bool `json:"roomRecord,omitempty"`

	// actions on a particular room
	RoomAdmin bool   `json:"roomAdmin,omitempty"`
	RoomJoin  bool   `json:"roomJoin,omitempty"`
	Room      string `json:"room,omitempty"`

	// permissions within a room, if none of the permissions are set explicitly
	// it will be granted with all publish and subscribe permissions
	CanPublish     *bool `json:"canPublish,omitempty"`
	CanSubscribe   *bool `json:"canSubscribe,omitempty"`
	CanPublishData *bool `json:"canPublishData,omitempty"`
	// TrackSource types that a participant may publish.
	// When set, it supersedes CanPublish. Only sources explicitly set here can be published
	CanPublishSources []string `json:"canPublishSources,omitempty"` // keys keep track of each source
	// by default, a participant is not allowed to update its own metadata
	CanUpdateOwnMetadata *bool `json:"canUpdateOwnMetadata,omitempty"`

	// actions on ingresses
	IngressAdmin bool `json:"ingressAdmin,omitempty"` // applies to all ingress

	// participant is not visible to other participants
	Hidden bool `json:"hidden,omitempty"`
	// indicates to the room that current participant is a recorder
	Recorder bool `json:"recorder,omitempty"`
	// indicates that the holder can register as an Agent framework worker
	Agent bool `json:"agent,omitempty"`

	// if a participant can subscribe to metrics
	CanSubscribeMetrics *bool `json:"canSubscribeMetrics,omitempty"`

	// destination room which this participant can forward to
	DestinationRoom string `json:"destinationRoom,omitempty"`
}

func (v *VideoGrant) SetCanPublish(val bool) {
	v.CanPublish = &val
}

func (v *VideoGrant) SetCanPublishData(val bool) {
	v.CanPublishData = &val
}

func (v *VideoGrant) SetCanSubscribe(val bool) {
	v.CanSubscribe = &val
}

func (v *VideoGrant) SetCanUpdateOwnMetadata(val bool) {
	v.CanUpdateOwnMetadata = &val
}

func (v *VideoGrant) SetCanSubscribeMetrics(val bool) {
	v.CanSubscribeMetrics = &val
}

func (v *VideoGrant) GetCanPublish() bool {
	if v.CanPublish == nil {
		return true
	}
	return *v.CanPublish
}

func (v *VideoGrant) GetCanPublishData() bool {
	if v.CanPublishData == nil {
		return v.GetCanPublish()
	}
	return *v.CanPublishData
}

func (v *VideoGrant) GetCanSubscribe() bool {
	if v.CanSubscribe == nil {
		return true
	}
	return *v.CanSubscribe
}

func (v *VideoGrant) GetCanUpdateOwnMetadata() bool {
	if v.CanUpdateOwnMetadata == nil {
		return false
	}
	return *v.CanUpdateOwnMetadata
}

func (v *VideoGrant) GetCanSubscribeMetrics() bool {
	if v.CanSubscribeMetrics == nil {
		return false
	}
	return *v.CanSubscribeMetrics
}

func (v *VideoGrant) Clone() *VideoGrant {
	if v == nil {
		return nil
	}

	clone := *v

	if v.CanPublish != nil {
		canPublish := *v.CanPublish
		clone.CanPublish = &canPublish
	}

	if v.CanSubscribe != nil {
		canSubscribe := *v.CanSubscribe
		clone.CanSubscribe = &canSubscribe
	}

	if v.CanPublishData != nil {
		canPublishData := *v.CanPublishData
		clone.CanPublishData = &canPublishData
	}

	if v.CanPublishSources != nil {
		clone.CanPublishSources = make([]string, len(v.CanPublishSources))
		copy(clone.CanPublishSources, v.CanPublishSources)
	}

	if v.CanUpdateOwnMetadata != nil {
		canUpdateOwnMetadata := *v.CanUpdateOwnMetadata
		clone.CanUpdateOwnMetadata = &canUpdateOwnMetadata
	}

	return &clone
}

// ----------------------------------------------------------------

type SIPGrant struct {
	// Admin grants access to all SIP features.
	Admin bool `json:"admin,omitempty"`

	// Call allows making outbound SIP calls.
	Call bool `json:"call,omitempty"`
}

func (s *SIPGrant) Clone() *SIPGrant {
	if s == nil {
		return nil
	}

	clone := *s

	return &clone
}

type AgentGrant struct {
	// Admin grants to create/update/delete Cloud Agents.
	Admin bool `json:"admin,omitempty"`
	// CreateSimulation grants access to create simulations to evaluate an agent.
	CreateSimulation bool `json:"createSimulation,omitempty"`
}

func (s *AgentGrant) Clone() *AgentGrant {
	if s == nil {
		return nil
	}

	clone := *s

	return &clone
}

// ------------------------------------------------------------------

type InferenceGrant struct {
	// Perform grants to all inference features (LLM, STT, TTS)
	Perform bool `json:"perform,omitempty"`
}

func (s *InferenceGrant) Clone() *InferenceGrant {
	if s == nil {
		return nil
	}

	clone := *s

	return &clone
}

// ------------------------------------------------------------------

type ObservabilityGrant struct {
	// Write grants to publish observability data
	Write bool `json:"write,omitempty"`
}

func (s *ObservabilityGrant) Clone() *ObservabilityGrant {
	if s == nil {
		return nil
	}

	clone := *s

	return &clone
}
