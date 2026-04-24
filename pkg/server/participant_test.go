package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/auth"
	"github.com/axonkit/axonkit/pkg/config"
	"github.com/axonkit/axonkit/pkg/proto"
	"github.com/axonkit/axonkit/pkg/rtc"
	"github.com/axonkit/axonkit/pkg/store"
	"github.com/axonkit/axonkit/pkg/types"
)

const (
	testAPIKey    = "test-api-key"
	testAPISecret = "test-api-secret-must-be-long-enough"
)

// newTestServer builds a minimal HTTPServer that exercises the forward /
// remove participant paths without spinning up mediasoup workers. It
// wires the shared KV used by rtc.RoomManager so tests can pre-populate
// the source room and then observe the destination room after a call.
func newTestServer(t *testing.T, allowAnonymous bool) (*HTTPServer, store.Store) {
	t.Helper()
	kv := store.NewMemoryStore(0)
	t.Cleanup(func() { kv.Close() })

	cfg := &config.Config{
		Auth: config.AuthConfig{
			APIKey:         testAPIKey,
			APISecret:      testAPISecret,
			AllowAnonymous: allowAnonymous,
		},
		Room: config.RoomConfig{
			EmptyTimeout:     5 * time.Minute,
			DepartureTimeout: 200 * time.Millisecond,
			MaxParticipants:  100,
			AutoSubscribe:    true,
			Dynacast:         true,
		},
	}
	rm := rtc.NewRoomManager(rtc.RoomManagerParams{
		Config:  cfg.Room,
		KV:      kv,
		LeaseKV: store.NewMemoryStore(0),
	})
	t.Cleanup(func() { rm.Close() })

	return &HTTPServer{
		cfg:         cfg,
		roomManager: rm,
		verifier:    auth.NewTokenVerifier(testAPIKey, testAPISecret),
		node:        types.NodeInfo{ID: "node-test"},
	}, kv
}

// seedParticipantKV writes a JOINED participant into the shared KV; used
// to make ForwardParticipant / RemoveParticipantFromRoom observable.
func seedParticipantKV(t *testing.T, kv store.Store, room, sid, identity string) {
	t.Helper()
	rs := rtc.NewRoomStore(kv, room, "seed")
	rs.PutParticipant(&proto.ParticipantInfo{
		Sid: sid, Identity: identity, State: proto.ParticipantInfo_JOINED, Name: identity,
	})
}

func adminToken(t *testing.T, roomName string, admin bool) string {
	t.Helper()
	at := auth.NewAccessToken(testAPIKey, testAPISecret).
		SetIdentity("admin").
		SetName("admin").
		SetGrants(&proto.ClaimGrants{
			Identity: "admin",
			Room:     &proto.RoomGrant{Name: roomName, Admin: admin, Join: true},
		}).
		SetTTL(time.Hour)
	tok, err := at.Sign()
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return tok
}

func doJSON(s *HTTPServer, method, path string, body any, token string, handler http.HandlerFunc) *httptest.ResponseRecorder {
	buf, _ := json.Marshal(body)
	req := httptest.NewRequest(method, path, bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	handler(rec, req)
	_ = s
	return rec
}

// -------------------- authorizeAdmin --------------------

func TestAuthorizeAdmin_AnonymousWhenAllowed(t *testing.T) {
	s, _ := newTestServer(t, true)
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	claims, werr := s.authorizeAdmin(req, "room-a", "anon_")
	if werr != nil {
		t.Fatalf("expected nil whipErr, got %+v", werr)
	}
	if claims == nil || claims.Sub == "" {
		t.Fatalf("expected anonymous claims, got %+v", claims)
	}
	if !strings.HasPrefix(claims.Sub, "anon_") {
		t.Fatalf("expected anonymous identity prefixed with 'anon_', got %q", claims.Sub)
	}
}

func TestAuthorizeAdmin_MissingTokenWhenAnonymousDisabled(t *testing.T) {
	s, _ := newTestServer(t, false)
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	_, werr := s.authorizeAdmin(req, "room-a", "anon_")
	if werr == nil || werr.status != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %+v", werr)
	}
}

func TestAuthorizeAdmin_InvalidToken(t *testing.T) {
	s, _ := newTestServer(t, false)
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	req.Header.Set("Authorization", "Bearer not-a-real-jwt")
	_, werr := s.authorizeAdmin(req, "room-a", "anon_")
	if werr == nil || werr.status != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %+v", werr)
	}
}

func TestAuthorizeAdmin_MissingAdminGrant(t *testing.T) {
	s, _ := newTestServer(t, false)
	tok := adminToken(t, "room-a", false) // Admin=false
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+tok)
	_, werr := s.authorizeAdmin(req, "room-a", "anon_")
	if werr == nil || werr.status != http.StatusForbidden {
		t.Fatalf("expected 403 room-admin-required, got %+v", werr)
	}
}

func TestAuthorizeAdmin_RoomScopeMismatch(t *testing.T) {
	s, _ := newTestServer(t, false)
	tok := adminToken(t, "room-a", true)
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+tok)
	_, werr := s.authorizeAdmin(req, "room-b", "anon_")
	if werr == nil || werr.status != http.StatusForbidden {
		t.Fatalf("expected 403 scope mismatch, got %+v", werr)
	}
}

func TestAuthorizeAdmin_RoomScopeMatch(t *testing.T) {
	s, _ := newTestServer(t, false)
	tok := adminToken(t, "room-a", true)
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+tok)
	claims, werr := s.authorizeAdmin(req, "room-a", "anon_")
	if werr != nil {
		t.Fatalf("expected success, got %+v", werr)
	}
	if !claims.Grants.Room.Admin {
		t.Fatal("expected Admin=true on claims")
	}
}

func TestAuthorizeAdmin_UnscopedGrantAllowsAnyRoom(t *testing.T) {
	s, _ := newTestServer(t, false)
	tok := adminToken(t, "", true) // grant.Name == ""
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+tok)
	if _, werr := s.authorizeAdmin(req, "any-room", "anon_"); werr != nil {
		t.Fatalf("expected success for unscoped admin, got %+v", werr)
	}
}

func TestAuthorizeAdmin_EmptyRoomArgIgnoresScope(t *testing.T) {
	s, _ := newTestServer(t, false)
	tok := adminToken(t, "room-a", true)
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+tok)
	// Caller passes "" when the operation isn't room-specific — still valid.
	if _, werr := s.authorizeAdmin(req, "", "anon_"); werr != nil {
		t.Fatalf("expected success when roomName is empty, got %+v", werr)
	}
}

func TestAuthorizeAdmin_WHIPPrefix(t *testing.T) {
	// WHIP/WHEP surfaces pass "whip_" so room logs show the ingest
	// entry point. The prefix must flow through to the minted claim.
	s, _ := newTestServer(t, true)
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	claims, werr := s.authorizeAdmin(req, "room-a", "whip_")
	if werr != nil {
		t.Fatalf("expected success, got %+v", werr)
	}
	if !strings.HasPrefix(claims.Sub, "whip_") {
		t.Fatalf("expected anonymous identity prefixed with 'whip_', got %q", claims.Sub)
	}
}

// -------------------- forwardErrorToStatus --------------------

func TestForwardErrorToStatus(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		status   int
		wantCode proto.ErrorCode
	}{
		{"nil", nil, http.StatusOK, ""},
		{"not found", errString("participant not found"), http.StatusNotFound, proto.ErrCodeNotFound},
		{"invalid", errString("invalid dstRoom: bad"), http.StatusBadRequest, proto.ErrCodeInvalidRequest},
		{"required", errString("srcIdentity is required"), http.StatusBadRequest, proto.ErrCodeInvalidRequest},
		{"must differ", errString("srcRoom and dstRoom must differ"), http.StatusBadRequest, proto.ErrCodeInvalidRequest},
		{"default", errString("boom"), http.StatusInternalServerError, proto.ErrCodeInternal},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st, code := forwardErrorToStatus(c.err)
			if st != c.status || code != c.wantCode {
				t.Fatalf("got (%d,%q) want (%d,%q)", st, code, c.status, c.wantCode)
			}
		})
	}
}

type errString string

func (e errString) Error() string { return string(e) }

// -------------------- handleForwardParticipant --------------------

func TestHandleForwardParticipant_Success(t *testing.T) {
	s, kv := newTestServer(t, true)
	seedParticipantKV(t, kv, "room-src", "pa_alice", "alice")

	body := ForwardParticipantRequest{
		SrcRoom: "room-src", SrcIdentity: "alice", DstRoom: "room-dst",
	}
	rec := doJSON(s, http.MethodPost, "/rtc/forward-participant", body, "", s.handleForwardParticipant)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp ForwardParticipantResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Participant == nil || resp.Participant.Identity != "alice" {
		t.Fatalf("unexpected participant: %+v", resp.Participant)
	}
	if resp.Participant.Sid != "pa_alice" {
		t.Fatalf("expected forwarded participant to keep src sid, got %q", resp.Participant.Sid)
	}
}

func TestHandleForwardParticipant_BadBody(t *testing.T) {
	s, _ := newTestServer(t, true)
	req := httptest.NewRequest(http.MethodPost, "/rtc/forward-participant",
		bytes.NewReader([]byte("not-json")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.handleForwardParticipant(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (%s)", rec.Code, rec.Body.String())
	}
}

func TestHandleForwardParticipant_ValidationError(t *testing.T) {
	s, _ := newTestServer(t, true)
	body := ForwardParticipantRequest{SrcRoom: "same", SrcIdentity: "alice", DstRoom: "same"}
	rec := doJSON(s, http.MethodPost, "/rtc/forward-participant", body, "", s.handleForwardParticipant)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for same src/dst, got %d (%s)", rec.Code, rec.Body.String())
	}
}

func TestHandleForwardParticipant_NotFound(t *testing.T) {
	s, _ := newTestServer(t, true)
	body := ForwardParticipantRequest{SrcRoom: "room-src", SrcIdentity: "ghost", DstRoom: "room-dst"}
	rec := doJSON(s, http.MethodPost, "/rtc/forward-participant", body, "", s.handleForwardParticipant)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing participant, got %d (%s)", rec.Code, rec.Body.String())
	}
}

func TestHandleForwardParticipant_Unauthorized(t *testing.T) {
	s, _ := newTestServer(t, false) // no anonymous
	body := ForwardParticipantRequest{SrcRoom: "room-src", SrcIdentity: "alice", DstRoom: "room-dst"}
	rec := doJSON(s, http.MethodPost, "/rtc/forward-participant", body, "", s.handleForwardParticipant)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandleForwardParticipant_ForbiddenForNonAdmin(t *testing.T) {
	s, kv := newTestServer(t, false)
	seedParticipantKV(t, kv, "room-src", "pa_alice", "alice")
	tok := adminToken(t, "room-src", false) // Admin=false
	body := ForwardParticipantRequest{SrcRoom: "room-src", SrcIdentity: "alice", DstRoom: "room-dst"}
	rec := doJSON(s, http.MethodPost, "/rtc/forward-participant", body, tok, s.handleForwardParticipant)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d (%s)", rec.Code, rec.Body.String())
	}
}

// -------------------- handleRemoveParticipant --------------------

func TestHandleRemoveParticipant_WritesDisconnected(t *testing.T) {
	s, kv := newTestServer(t, true)
	seedParticipantKV(t, kv, "room-a", "pa_alice", "alice")

	body := RemoveParticipantRequest{Room: "room-a", Identity: "alice"}
	rec := doJSON(s, http.MethodPost, "/rtc/remove-participant", body, "", s.handleRemoveParticipant)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", rec.Code, rec.Body.String())
	}
	rs := rtc.NewRoomStore(kv, "room-a", "verify")
	got := rs.GetParticipant("pa_alice")
	if got == nil {
		t.Fatal("expected participant row present (DISCONNECTED), got nil")
	} else {
		if got.State != proto.ParticipantInfo_DISCONNECTED {
			t.Fatalf("expected state=DISCONNECTED, got %v", got.State)
		}
		if got.DisconnectReason != proto.DisconnectReason_PARTICIPANT_REMOVED {
			t.Fatalf("expected reason=PARTICIPANT_REMOVED, got %v", got.DisconnectReason)
		}
	}
}

func TestHandleRemoveParticipant_UnknownIsOK(t *testing.T) {
	s, _ := newTestServer(t, true)
	body := RemoveParticipantRequest{Room: "room-a", Identity: "ghost"}
	rec := doJSON(s, http.MethodPost, "/rtc/remove-participant", body, "", s.handleRemoveParticipant)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for unknown identity, got %d (%s)", rec.Code, rec.Body.String())
	}
}

func TestHandleRemoveParticipant_BadBody(t *testing.T) {
	s, _ := newTestServer(t, true)
	req := httptest.NewRequest(http.MethodPost, "/rtc/remove-participant",
		bytes.NewReader([]byte("not-json")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.handleRemoveParticipant(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleRemoveParticipant_ValidationError(t *testing.T) {
	s, _ := newTestServer(t, true)
	body := RemoveParticipantRequest{Room: "room-a", Identity: ""}
	rec := doJSON(s, http.MethodPost, "/rtc/remove-participant", body, "", s.handleRemoveParticipant)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty identity, got %d (%s)", rec.Code, rec.Body.String())
	}
}

func TestHandleRemoveParticipant_Unauthorized(t *testing.T) {
	s, _ := newTestServer(t, false)
	body := RemoveParticipantRequest{Room: "room-a", Identity: "alice"}
	rec := doJSON(s, http.MethodPost, "/rtc/remove-participant", body, "", s.handleRemoveParticipant)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}
