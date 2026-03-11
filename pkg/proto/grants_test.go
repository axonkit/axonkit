package proto

import "testing"

func TestClaimGrants_Clone_Nil(t *testing.T) {
	var g *ClaimGrants
	if g.Clone() != nil {
		t.Fatal("expected nil")
	}
}

func TestClaimGrants_Clone(t *testing.T) {
	g := &ClaimGrants{
		Identity: "alice",
		Name:     "Alice",
		Room: &RoomGrant{
			Name: "room1",
			Join: true,
		},
		Media:      &MediaGrant{},
		Attributes: map[string]string{"key": "val"},
	}
	c := g.Clone()
	if c.Identity != "alice" || c.Room == nil || !c.Room.Join {
		t.Fatal("clone mismatch")
	}
	c.Attributes["key"] = "changed"
	if g.Attributes["key"] != "val" {
		t.Fatal("clone should not share Attributes map")
	}
}

func TestRoomGrant_Clone(t *testing.T) {
	r := &RoomGrant{Name: "room1", Join: true, Admin: true, Hidden: true}
	c := r.Clone()
	if c.Name != "room1" || !c.Join || !c.Admin || !c.Hidden {
		t.Fatal("clone mismatch")
	}
}

func TestRoomGrant_Clone_Nil(t *testing.T) {
	var r *RoomGrant
	if r.Clone() != nil {
		t.Fatal("expected nil")
	}
}

func TestMediaGrant_GetDefaults(t *testing.T) {
	m := &MediaGrant{}
	if !m.GetCanPublish() || !m.GetCanSubscribe() || !m.GetCanPublishData() {
		t.Fatal("expected defaults to true")
	}
	if m.GetCanUpdateOwnMetadata() || m.GetCanSubscribeMetrics() {
		t.Fatal("expected defaults to false")
	}
}

func TestMediaGrant_SetAndGet(t *testing.T) {
	m := &MediaGrant{}
	m.SetCanPublish(false)
	if m.GetCanPublish() {
		t.Fatal("expected false")
	}
	m.SetCanSubscribe(false)
	if m.GetCanSubscribe() {
		t.Fatal("expected false")
	}
	m.SetCanPublishData(false)
	if m.GetCanPublishData() {
		t.Fatal("expected false")
	}
	m.SetCanUpdateOwnMetadata(true)
	if !m.GetCanUpdateOwnMetadata() {
		t.Fatal("expected true")
	}
	m.SetCanSubscribeMetrics(true)
	if !m.GetCanSubscribeMetrics() {
		t.Fatal("expected true")
	}
}

func TestMediaGrant_Clone(t *testing.T) {
	m := &MediaGrant{}
	m.SetCanPublish(false)
	m.CanPublishSources = []string{"camera"}

	c := m.Clone()
	if c.GetCanPublish() != false {
		t.Fatal("clone mismatch")
	}
	if len(c.CanPublishSources) != 1 || c.CanPublishSources[0] != "camera" {
		t.Fatal("clone sources mismatch")
	}

	c.CanPublishSources[0] = "screen"
	if m.CanPublishSources[0] != "camera" {
		t.Fatal("clone should not share slice")
	}
}

func TestMediaGrant_Clone_Nil(t *testing.T) {
	var m *MediaGrant
	if m.Clone() != nil {
		t.Fatal("expected nil")
	}
}

func TestMediaGrant_ToPermission(t *testing.T) {
	m := &MediaGrant{}
	m.SetCanPublish(true)
	m.SetCanSubscribe(true)
	m.CanPublishSources = []string{"camera", "microphone"}

	room := &RoomGrant{Hidden: true}
	perm := m.ToPermission(room)

	if !perm.CanPublish || !perm.CanSubscribe || !perm.Hidden {
		t.Fatal("permission mismatch")
	}
	if len(perm.CanPublishSources) != 2 {
		t.Fatal("sources mismatch")
	}
}

func TestMediaGrant_ToPermission_NilRoom(t *testing.T) {
	m := &MediaGrant{}
	perm := m.ToPermission(nil)
	if !perm.CanPublish || !perm.CanSubscribe || perm.Hidden {
		t.Fatal("permission mismatch with nil room")
	}
}

func TestMediaGrant_MatchesPermission(t *testing.T) {
	m := &MediaGrant{}
	room := &RoomGrant{}
	perm := m.ToPermission(room)
	if !m.MatchesPermission(perm, room) {
		t.Fatal("should match")
	}

	perm.CanPublish = false
	if m.MatchesPermission(perm, room) {
		t.Fatal("should not match after change")
	}
}

func TestMediaGrant_UpdateFromPermission(t *testing.T) {
	m := &MediaGrant{}
	room := &RoomGrant{}
	perm := &ParticipantPermission{
		CanPublish:        false,
		CanSubscribe:      true,
		CanPublishData:    false,
		CanUpdateMetadata: true,
		Hidden:            true,
		CanPublishSources: []TrackSource{TrackSource_CAMERA},
	}
	m.UpdateFromPermission(perm, room)

	if m.GetCanPublish() || !m.GetCanSubscribe() || m.GetCanPublishData() || !m.GetCanUpdateOwnMetadata() {
		t.Fatal("update mismatch")
	}
	if !room.Hidden {
		t.Fatal("hidden should be updated on room grant")
	}
	if len(m.CanPublishSources) != 1 || m.CanPublishSources[0] != string(TrackSource_CAMERA) {
		t.Fatal("sources mismatch")
	}
}

func TestSIPGrant_Clone(t *testing.T) {
	s := &SIPGrant{Admin: true, Call: true}
	c := s.Clone()
	if !c.Admin || !c.Call {
		t.Fatal("clone mismatch")
	}
}

func TestSIPGrant_Clone_Nil(t *testing.T) {
	var s *SIPGrant
	if s.Clone() != nil {
		t.Fatal("expected nil")
	}
}

func TestAgentGrant_Clone(t *testing.T) {
	a := &AgentGrant{Admin: true}
	c := a.Clone()
	if !c.Admin {
		t.Fatal("clone mismatch")
	}
}

func TestAgentGrant_Clone_Nil(t *testing.T) {
	var a *AgentGrant
	if a.Clone() != nil {
		t.Fatal("expected nil")
	}
}

func TestInferenceGrant_Clone(t *testing.T) {
	i := &InferenceGrant{Perform: true}
	c := i.Clone()
	if !c.Perform {
		t.Fatal("clone mismatch")
	}
}

func TestInferenceGrant_Clone_Nil(t *testing.T) {
	var i *InferenceGrant
	if i.Clone() != nil {
		t.Fatal("expected nil")
	}
}

func TestObservabilityGrant_Clone(t *testing.T) {
	o := &ObservabilityGrant{Write: true}
	c := o.Clone()
	if !c.Write {
		t.Fatal("clone mismatch")
	}
}

func TestObservabilityGrant_Clone_Nil(t *testing.T) {
	var o *ObservabilityGrant
	if o.Clone() != nil {
		t.Fatal("expected nil")
	}
}

func TestRoomConfiguration_Clone(t *testing.T) {
	rc := &RoomConfiguration{}
	c := rc.Clone()
	if c == nil {
		t.Fatal("expected non-nil clone")
	}
}

func TestRoomConfiguration_Clone_Nil(t *testing.T) {
	var rc *RoomConfiguration
	if rc.Clone() != nil {
		t.Fatal("expected nil")
	}
}
