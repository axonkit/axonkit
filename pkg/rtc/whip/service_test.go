package whip

import "testing"

func TestRegistry_AddGetRemove(t *testing.T) {
	reg := NewRegistry(ModeWHIP)
	s := &Session{Mode: ModeWHIP}
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
	reg := NewRegistry(ModeWHIP)
	s1 := &Session{ID: "alice", Mode: ModeWHIP}
	if _, err := reg.Add(s1); err != nil {
		t.Fatalf("first Add: %v", err)
	}
	s2 := &Session{ID: "alice", Mode: ModeWHIP}
	if _, err := reg.Add(s2); err != ErrResourceTaken {
		t.Fatalf("expected ErrResourceTaken, got %v", err)
	}
}

func TestRegistry_SeparateWHIPvsWHEP(t *testing.T) {
	whipReg := NewRegistry(ModeWHIP)
	whepReg := NewRegistry(ModeWHEP)
	// Same ID allowed in each registry because WHIP/WHEP are independent.
	if _, err := whipReg.Add(&Session{ID: "same", Mode: ModeWHIP}); err != nil {
		t.Fatalf("whip add: %v", err)
	}
	if _, err := whepReg.Add(&Session{ID: "same", Mode: ModeWHEP}); err != nil {
		t.Fatalf("whep add (different registry): %v", err)
	}
	if whipReg.Len() != 1 || whepReg.Len() != 1 {
		t.Fatalf("expected 1 session in each registry")
	}
	// Removing from WHIP must not affect WHEP.
	if err := whipReg.Remove("same"); err != nil {
		t.Fatalf("whip remove: %v", err)
	}
	if whepReg.Get("same") == nil {
		t.Fatalf("WHEP session incorrectly removed when WHIP session was deleted")
	}
}

func TestRegistry_ModeMismatchRejected(t *testing.T) {
	reg := NewRegistry(ModeWHIP)
	if _, err := reg.Add(&Session{Mode: ModeWHEP}); err == nil {
		t.Fatalf("expected mode mismatch error")
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
