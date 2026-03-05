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
		Video: &VideoGrant{
			RoomJoin: true,
		},
		Attributes: map[string]string{"key": "val"},
	}
	c := g.Clone()
	if c.Identity != "alice" || c.Video == nil || !c.Video.RoomJoin {
		t.Fatal("clone mismatch")
	}
	c.Attributes["key"] = "changed"
	if g.Attributes["key"] != "val" {
		t.Fatal("clone should not share Attributes map")
	}
}

func TestVideoGrant_GetDefaults(t *testing.T) {
	v := &VideoGrant{RoomJoin: true}
	if !v.GetCanPublish() || !v.GetCanSubscribe() || !v.GetCanPublishData() {
		t.Fatal("expected defaults to true")
	}
	if v.GetCanUpdateOwnMetadata() || v.GetCanSubscribeMetrics() {
		t.Fatal("expected defaults to false")
	}
}

func TestVideoGrant_SetAndGet(t *testing.T) {
	v := &VideoGrant{RoomJoin: true}
	v.SetCanPublish(false)
	if v.GetCanPublish() {
		t.Fatal("expected false")
	}
	v.SetCanSubscribe(false)
	if v.GetCanSubscribe() {
		t.Fatal("expected false")
	}
	v.SetCanPublishData(false)
	if v.GetCanPublishData() {
		t.Fatal("expected false")
	}
	v.SetCanUpdateOwnMetadata(true)
	if !v.GetCanUpdateOwnMetadata() {
		t.Fatal("expected true")
	}
	v.SetCanSubscribeMetrics(true)
	if !v.GetCanSubscribeMetrics() {
		t.Fatal("expected true")
	}
}

func TestVideoGrant_Clone(t *testing.T) {
	v := &VideoGrant{RoomJoin: true}
	v.SetCanPublish(false)
	v.CanPublishSources = []string{"camera"}

	c := v.Clone()
	if c.GetCanPublish() != false || c.RoomJoin != true {
		t.Fatal("clone mismatch")
	}
	if len(c.CanPublishSources) != 1 || c.CanPublishSources[0] != "camera" {
		t.Fatal("clone sources mismatch")
	}

	c.CanPublishSources[0] = "screen"
	if v.CanPublishSources[0] != "camera" {
		t.Fatal("clone should not share slice")
	}
}

func TestVideoGrant_Clone_Nil(t *testing.T) {
	var v *VideoGrant
	if v.Clone() != nil {
		t.Fatal("expected nil")
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
