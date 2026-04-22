package whip

import "testing"

func TestRegistry_AddGetRemove(t *testing.T) {
	reg := NewRegistry()
	s := &Session{}
	id, err := reg.Add(s)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if id == "" || reg.Get(id) != s {
		t.Fatalf("Add/Get broken: id=%q", id)
	}
	if err := reg.Remove(id); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if reg.Get(id) != nil {
		t.Fatalf("session still present after Remove")
	}
	if err := reg.Remove(id); err != ErrSessionNotFound {
		t.Fatalf("expected ErrSessionNotFound, got %v", err)
	}
}

func TestRegistry_AddDuplicate(t *testing.T) {
	reg := NewRegistry()
	s1 := &Session{ID: "alice"}
	if _, err := reg.Add(s1); err != nil {
		t.Fatalf("first Add: %v", err)
	}
	s2 := &Session{ID: "alice"}
	if _, err := reg.Add(s2); err != ErrResourceTaken {
		t.Fatalf("expected ErrResourceTaken, got %v", err)
	}
}

func TestRegistry_IndependentInstances(t *testing.T) {
	a := NewRegistry()
	b := NewRegistry()
	// Same ID is allowed across independent registries; removal from one must
	// not touch the other.
	if _, err := a.Add(&Session{ID: "same"}); err != nil {
		t.Fatalf("a.Add: %v", err)
	}
	if _, err := b.Add(&Session{ID: "same"}); err != nil {
		t.Fatalf("b.Add: %v", err)
	}
	if a.Len() != 1 || b.Len() != 1 {
		t.Fatalf("expected 1 session in each registry")
	}
	if err := a.Remove("same"); err != nil {
		t.Fatalf("a.Remove: %v", err)
	}
	if b.Get("same") == nil {
		t.Fatalf("b session incorrectly removed when a session was deleted")
	}
}

func TestSession_CloseRunsHookOnce(t *testing.T) {
	calls := 0
	s := &Session{OnClose: func() { calls++ }}
	s.Close()
	s.Close()
	if calls != 1 {
		t.Fatalf("expected OnClose to run once, got %d", calls)
	}
}
