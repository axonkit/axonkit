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
		SetGrants(&proto.ClaimGrants{
			Video: &proto.VideoGrant{RoomJoin: true, Room: "test-room"},
		}).
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
	if claims.Grants == nil || claims.Grants.Video == nil || claims.Grants.Video.Room != "test-room" {
		t.Fatal("grants mismatch")
	}
	if claims.Jti != "jti-1" {
		t.Fatalf("expected jti-1, got %s", claims.Jti)
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

func TestTokenVerifier_RefreshToken(t *testing.T) {
	at := NewAccessToken(testAPIKey, testAPISecret).
		SetIdentity("alice").
		SetGrants(&proto.ClaimGrants{Video: &proto.VideoGrant{RoomJoin: true}}).
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
