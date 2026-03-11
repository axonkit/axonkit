package auth

import (
	"errors"
	"testing"
	"time"

	"github.com/axonkit/axonkit/pkg/proto"
)

const (
	testAPIKey    = "test-key"
	testAPISecret = "test-secret-at-least-32-chars-long"
)

func TestAccessToken_SignAndVerify(t *testing.T) {
	at := NewAccessToken(testAPIKey, testAPISecret).
		SetIdentity("alice").
		SetName("Alice").
		SetRoomGrant(&proto.RoomGrant{Name: "test-room", Join: true}).
		SetMediaGrant(&proto.MediaGrant{}).
		SetTTL(time.Hour).
		SetTokenID("jti-1")

	tokenStr, err := at.Sign()
	if err != nil {
		t.Fatalf("Sign failed: %v", err)
	}
	if tokenStr == "" {
		t.Fatal("expected non-empty token")
	}

	v := NewTokenVerifier(testAPIKey, testAPISecret)
	claims, err := v.Verify(tokenStr)
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}
	if claims.Identity() != "alice" {
		t.Fatalf("expected alice, got %s", claims.Identity())
	}
	if claims.Name != "Alice" {
		t.Fatalf("expected Alice, got %s", claims.Name)
	}
	if claims.Grants == nil || claims.Grants.Room == nil || claims.Grants.Room.Name != "test-room" {
		t.Fatal("room grant mismatch")
	}
	if !claims.Grants.Room.Join {
		t.Fatal("expected room join to be true")
	}
	if claims.Jti != "jti-1" {
		t.Fatalf("expected jti-1, got %s", claims.Jti)
	}
}

func TestAccessToken_SetterChain(t *testing.T) {
	at := NewAccessToken(testAPIKey, testAPISecret).
		SetIdentity("bob").
		SetName("Bob").
		SetRoomGrant(&proto.RoomGrant{Name: "room1", Join: true}).
		SetMediaGrant(&proto.MediaGrant{}).
		SetSIPGrant(&proto.SIPGrant{Admin: true}).
		SetMetadata("test-meta").
		SetAttributes(map[string]string{"role": "admin"}).
		SetKind("agent").
		SetRoomConfig(&proto.RoomConfiguration{}).
		SetTTL(time.Hour)

	tokenStr, err := at.Sign()
	if err != nil {
		t.Fatalf("Sign failed: %v", err)
	}

	v := NewTokenVerifier(testAPIKey, testAPISecret)
	claims, err := v.Verify(tokenStr)
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}
	g := claims.Grants
	if g == nil {
		t.Fatal("expected non-nil grants")
	}
	if g.Room == nil || g.Room.Name != "room1" || !g.Room.Join {
		t.Fatal("room grant mismatch")
	}
	if g.Media == nil {
		t.Fatal("expected non-nil media grant")
	}
	if g.SIP == nil || !g.SIP.Admin {
		t.Fatal("SIP grant mismatch")
	}
	if g.Metadata != "test-meta" {
		t.Fatalf("expected test-meta, got %s", g.Metadata)
	}
	if g.Attributes["role"] != "admin" {
		t.Fatal("attributes mismatch")
	}
	if g.Kind != "agent" {
		t.Fatalf("expected agent kind, got %s", g.Kind)
	}
	if g.RoomConfig == nil {
		t.Fatal("expected non-nil room config")
	}
}

func TestAccessToken_DefaultTTL(t *testing.T) {
	at := NewAccessToken(testAPIKey, testAPISecret).SetIdentity("bob")
	tokenStr, err := at.Sign()
	if err != nil {
		t.Fatalf("Sign failed: %v", err)
	}

	v := NewTokenVerifier(testAPIKey, testAPISecret)
	claims, err := v.Verify(tokenStr)
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}
	if claims.Identity() != "bob" {
		t.Fatalf("expected bob, got %s", claims.Identity())
	}
}

func TestTokenVerifier_InvalidToken(t *testing.T) {
	v := NewTokenVerifier(testAPIKey, testAPISecret)
	_, err := v.Verify("not-a-jwt")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTokenVerifier_WrongSecret(t *testing.T) {
	at := NewAccessToken(testAPIKey, testAPISecret).SetIdentity("alice").SetTTL(time.Hour)
	tokenStr, _ := at.Sign()

	v := NewTokenVerifier(testAPIKey, "wrong-secret")
	_, err := v.Verify(tokenStr)
	if err == nil || !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestTokenVerifier_WrongIssuer(t *testing.T) {
	at := NewAccessToken("other-key", testAPISecret).SetIdentity("alice").SetTTL(time.Hour)
	tokenStr, _ := at.Sign()

	v := NewTokenVerifier(testAPIKey, testAPISecret)
	_, err := v.Verify(tokenStr)
	if err == nil || !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestTokenVerifier_ExpiredToken(t *testing.T) {
	at := NewAccessToken(testAPIKey, testAPISecret).SetIdentity("alice")
	at.claims.Exp = time.Now().Add(-time.Hour).Unix()
	tokenStr, _ := at.Sign()

	v := NewTokenVerifier(testAPIKey, testAPISecret)
	_, err := v.Verify(tokenStr)
	if err == nil || !errors.Is(err, ErrExpiredToken) {
		t.Fatalf("expected ErrExpiredToken, got %v", err)
	}
}

func TestTokenVerifier_NotYetValid(t *testing.T) {
	at := NewAccessToken(testAPIKey, testAPISecret).SetIdentity("alice").SetTTL(time.Hour)
	at.claims.Nbf = time.Now().Add(time.Hour).Unix()
	tokenStr, _ := at.Sign()

	v := NewTokenVerifier(testAPIKey, testAPISecret)
	_, err := v.Verify(tokenStr)
	if err == nil || !errors.Is(err, ErrTokenNotYetValid) {
		t.Fatalf("expected ErrTokenNotYetValid, got %v", err)
	}
}

func TestTokenVerifier_NbfZeroSkipped(t *testing.T) {
	at := NewAccessToken(testAPIKey, testAPISecret).SetIdentity("alice").SetTTL(time.Hour)
	at.claims.Nbf = 0
	tokenStr, _ := at.Sign()

	v := NewTokenVerifier(testAPIKey, testAPISecret)
	_, err := v.Verify(tokenStr)
	if err != nil {
		t.Fatalf("expected no error when nbf is 0, got %v", err)
	}
}

func TestTokenVerifier_RefreshToken(t *testing.T) {
	at := NewAccessToken(testAPIKey, testAPISecret).
		SetIdentity("alice").
		SetRoomGrant(&proto.RoomGrant{Join: true}).
		SetTTL(time.Hour)
	tokenStr, _ := at.Sign()

	v := NewTokenVerifier(testAPIKey, testAPISecret)
	newToken, err := v.RefreshToken(tokenStr, 2*time.Hour)
	if err != nil {
		t.Fatalf("RefreshToken failed: %v", err)
	}

	claims, err := v.Verify(newToken)
	if err != nil {
		t.Fatalf("Verify refreshed token failed: %v", err)
	}
	if claims.Identity() != "alice" {
		t.Fatal("identity should be preserved")
	}
}

func TestTokenVerifier_RefreshExpiredToken(t *testing.T) {
	at := NewAccessToken(testAPIKey, testAPISecret).SetIdentity("alice")
	at.claims.Exp = time.Now().Add(-time.Hour).Unix()
	tokenStr, _ := at.Sign()

	v := NewTokenVerifier(testAPIKey, testAPISecret)
	newToken, err := v.RefreshToken(tokenStr, time.Hour)
	if err != nil {
		t.Fatalf("RefreshToken should succeed for expired token: %v", err)
	}

	claims, err := v.Verify(newToken)
	if err != nil {
		t.Fatalf("Verify refreshed token failed: %v", err)
	}
	if claims.Identity() != "alice" {
		t.Fatal("identity should be preserved")
	}
}

func TestTokenVerifier_RefreshInvalidToken(t *testing.T) {
	v := NewTokenVerifier(testAPIKey, testAPISecret)
	_, err := v.RefreshToken("garbage", time.Hour)
	if err == nil {
		t.Fatal("expected error for invalid token")
	}
}
