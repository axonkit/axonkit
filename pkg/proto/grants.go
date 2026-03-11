package proto

import (
	"errors"
	"maps"
	"slices"
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
	Identity string `json:"identity,omitempty"`
	Name     string `json:"name,omitempty"`
	Kind     string `json:"kind,omitempty"`

	Room          *RoomGrant          `json:"room,omitempty"`
	Media         *MediaGrant         `json:"media,omitempty"`
	SIP           *SIPGrant           `json:"sip,omitempty"`
	Agent         *AgentGrant         `json:"agent,omitempty"`
	Inference     *InferenceGrant     `json:"inference,omitempty"`
	Observability *ObservabilityGrant `json:"observability,omitempty"`

	RoomConfig *RoomConfiguration `json:"roomConfig,omitempty"`
	RoomPreset string             `json:"roomPreset,omitempty"`

	Sha256     string            `json:"sha256,omitempty"`
	Metadata   string            `json:"metadata,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func (c *ClaimGrants) Clone() *ClaimGrants {
	if c == nil {
		return nil
	}

	clone := *c
	clone.Room = c.Room.Clone()
	clone.Media = c.Media.Clone()
	clone.SIP = c.SIP.Clone()
	clone.Agent = c.Agent.Clone()
	clone.Inference = c.Inference.Clone()
	clone.Observability = c.Observability.Clone()
	clone.Attributes = maps.Clone(c.Attributes)
	clone.RoomConfig = c.RoomConfig.Clone()

	return &clone
}

// -------------------------------------------------------------
// RoomGrant holds room-level permissions. Extracted from the
// legacy VideoGrant so that room semantics are not conflated
// with media semantics.
// -------------------------------------------------------------

type RoomGrant struct {
	Name   string `json:"name,omitempty"`
	Join   bool   `json:"join,omitempty"`
	Admin  bool   `json:"admin,omitempty"`
	Create bool   `json:"create,omitempty"`
	List   bool   `json:"list,omitempty"`
	Record bool   `json:"record,omitempty"`
	Hidden bool   `json:"hidden,omitempty"`

	DestinationRoom string `json:"destinationRoom,omitempty"`
}

func (r *RoomGrant) Clone() *RoomGrant {
	if r == nil {
		return nil
	}
	clone := *r
	return &clone
}

// -------------------------------------------------------------
// MediaGrant holds publish/subscribe permissions. Renamed from
// VideoGrant because it governs audio, video, and data — not
// just video.
// -------------------------------------------------------------

type MediaGrant struct {
	CanPublish     *bool `json:"canPublish,omitempty"`
	CanSubscribe   *bool `json:"canSubscribe,omitempty"`
	CanPublishData *bool `json:"canPublishData,omitempty"`
	// TrackSource types that a participant may publish.
	// When set, it supersedes CanPublish. Only sources explicitly set here can be published.
	CanPublishSources    []string `json:"canPublishSources,omitempty"`
	CanUpdateOwnMetadata *bool    `json:"canUpdateOwnMetadata,omitempty"`
	CanSubscribeMetrics  *bool    `json:"canSubscribeMetrics,omitempty"`

	Recorder bool `json:"recorder,omitempty"`
}

func (m *MediaGrant) SetCanPublish(val bool) {
	m.CanPublish = &val
}

func (m *MediaGrant) SetCanPublishData(val bool) {
	m.CanPublishData = &val
}

func (m *MediaGrant) SetCanSubscribe(val bool) {
	m.CanSubscribe = &val
}

func (m *MediaGrant) SetCanUpdateOwnMetadata(val bool) {
	m.CanUpdateOwnMetadata = &val
}

func (m *MediaGrant) SetCanSubscribeMetrics(val bool) {
	m.CanSubscribeMetrics = &val
}

func (m *MediaGrant) GetCanPublish() bool {
	if m.CanPublish == nil {
		return true
	}
	return *m.CanPublish
}

func (m *MediaGrant) GetCanPublishData() bool {
	if m.CanPublishData == nil {
		return m.GetCanPublish()
	}
	return *m.CanPublishData
}

func (m *MediaGrant) GetCanSubscribe() bool {
	if m.CanSubscribe == nil {
		return true
	}
	return *m.CanSubscribe
}

func (m *MediaGrant) GetCanUpdateOwnMetadata() bool {
	if m.CanUpdateOwnMetadata == nil {
		return false
	}
	return *m.CanUpdateOwnMetadata
}

func (m *MediaGrant) GetCanSubscribeMetrics() bool {
	if m.CanSubscribeMetrics == nil {
		return false
	}
	return *m.CanSubscribeMetrics
}

func (m *MediaGrant) Clone() *MediaGrant {
	if m == nil {
		return nil
	}

	clone := *m

	if m.CanPublish != nil {
		v := *m.CanPublish
		clone.CanPublish = &v
	}
	if m.CanSubscribe != nil {
		v := *m.CanSubscribe
		clone.CanSubscribe = &v
	}
	if m.CanPublishData != nil {
		v := *m.CanPublishData
		clone.CanPublishData = &v
	}
	if m.CanPublishSources != nil {
		clone.CanPublishSources = slices.Clone(m.CanPublishSources)
	}
	if m.CanUpdateOwnMetadata != nil {
		v := *m.CanUpdateOwnMetadata
		clone.CanUpdateOwnMetadata = &v
	}
	if m.CanSubscribeMetrics != nil {
		v := *m.CanSubscribeMetrics
		clone.CanSubscribeMetrics = &v
	}

	return &clone
}

// ToPermission builds a ParticipantPermission from the media grant
// and optional room grant (for hidden flag).
func (m *MediaGrant) ToPermission(room *RoomGrant) *ParticipantPermission {
	perm := &ParticipantPermission{
		CanPublish:          m.GetCanPublish(),
		CanSubscribe:        m.GetCanSubscribe(),
		CanPublishData:      m.GetCanPublishData(),
		CanUpdateMetadata:   m.GetCanUpdateOwnMetadata(),
		CanSubscribeMetrics: m.GetCanSubscribeMetrics(),
	}
	if m.CanPublishSources != nil {
		perm.CanPublishSources = make([]TrackSource, len(m.CanPublishSources))
		for i, s := range m.CanPublishSources {
			perm.CanPublishSources[i] = TrackSource(s)
		}
	}
	if room != nil {
		perm.Hidden = room.Hidden
	}
	return perm
}

// MatchesPermission reports whether the grants already match the given permission.
func (m *MediaGrant) MatchesPermission(perm *ParticipantPermission, room *RoomGrant) bool {
	if perm == nil {
		return false
	}
	if m.GetCanPublish() != perm.CanPublish ||
		m.GetCanSubscribe() != perm.CanSubscribe ||
		m.GetCanPublishData() != perm.CanPublishData ||
		m.GetCanUpdateOwnMetadata() != perm.CanUpdateMetadata ||
		m.GetCanSubscribeMetrics() != perm.CanSubscribeMetrics {
		return false
	}
	hidden := false
	if room != nil {
		hidden = room.Hidden
	}
	if hidden != perm.Hidden {
		return false
	}
	return true
}

// UpdateFromPermission overwrites grant fields from the given permission.
func (m *MediaGrant) UpdateFromPermission(perm *ParticipantPermission, room *RoomGrant) {
	if perm == nil {
		return
	}
	m.SetCanPublish(perm.CanPublish)
	m.SetCanSubscribe(perm.CanSubscribe)
	m.SetCanPublishData(perm.CanPublishData)
	m.SetCanUpdateOwnMetadata(perm.CanUpdateMetadata)
	m.SetCanSubscribeMetrics(perm.CanSubscribeMetrics)
	if room != nil {
		room.Hidden = perm.Hidden
	}
	if perm.CanPublishSources != nil {
		m.CanPublishSources = make([]string, len(perm.CanPublishSources))
		for i, s := range perm.CanPublishSources {
			m.CanPublishSources[i] = string(s)
		}
	}
}

// ----------------------------------------------------------------

type SIPGrant struct {
	Admin bool `json:"admin,omitempty"`
	Call  bool `json:"call,omitempty"`
}

func (s *SIPGrant) Clone() *SIPGrant {
	if s == nil {
		return nil
	}
	clone := *s
	return &clone
}

type AgentGrant struct {
	Admin            bool `json:"admin,omitempty"`
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
	Write bool `json:"write,omitempty"`
}

func (s *ObservabilityGrant) Clone() *ObservabilityGrant {
	if s == nil {
		return nil
	}
	clone := *s
	return &clone
}
