package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/axonkit/axonkit/pkg/proto"
)

var (
	ErrInvalidToken = errors.New("invalid token")
	ErrExpiredToken = errors.New("token expired")
)

type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

type Claims struct {
	Iss    string             `json:"iss"`
	Sub    string             `json:"sub"`
	Exp    int64              `json:"exp"`
	Iat    int64              `json:"iat"`
	Nbf    int64              `json:"nbf"`
	Jti    string             `json:"jti,omitempty"`
	Name   string             `json:"name,omitempty"`
	Grants *proto.ClaimGrants `json:"grants,omitempty"`
}

func (c *Claims) Identity() string { return c.Sub }

// AccessToken creates and signs JWT tokens for room access.
// Design follows the LiveKit token pattern: the token encodes
// identity, display name, and permission grants (room join/publish/subscribe).
type AccessToken struct {
	apiKey    string
	apiSecret string
	claims    Claims
}

func NewAccessToken(apiKey, apiSecret string) *AccessToken {
	now := time.Now()
	return &AccessToken{
		apiKey:    apiKey,
		apiSecret: apiSecret,
		claims: Claims{
			Iss: apiKey,
			Iat: now.Unix(),
			Nbf: now.Unix(),
		},
	}
}

func (t *AccessToken) SetIdentity(identity string) *AccessToken {
	t.claims.Sub = identity
	return t
}

func (t *AccessToken) SetName(name string) *AccessToken {
	t.claims.Name = name
	return t
}

func (t *AccessToken) SetGrants(grants *proto.ClaimGrants) *AccessToken {
	t.claims.Grants = grants
	return t
}

func (t *AccessToken) SetTTL(ttl time.Duration) *AccessToken {
	t.claims.Exp = time.Now().Add(ttl).Unix()
	return t
}

func (t *AccessToken) SetTokenID(jti string) *AccessToken {
	t.claims.Jti = jti
	return t
}

func (t *AccessToken) Sign() (string, error) {
	if t.claims.Exp == 0 {
		t.claims.Exp = time.Now().Add(6 * time.Hour).Unix()
	}
	return signJWT(t.apiSecret, t.claims)
}

// TokenVerifier validates incoming JWT tokens.
type TokenVerifier struct {
	apiKey    string
	apiSecret string
}

func NewTokenVerifier(apiKey, apiSecret string) *TokenVerifier {
	return &TokenVerifier{apiKey: apiKey, apiSecret: apiSecret}
}

func (v *TokenVerifier) Verify(tokenStr string) (*Claims, error) {
	claims, err := verifyJWT(v.apiSecret, tokenStr)
	if err != nil {
		return nil, err
	}
	if claims.Iss != v.apiKey {
		return nil, fmt.Errorf("%w: issuer mismatch", ErrInvalidToken)
	}
	if claims.Exp < time.Now().Unix() {
		return nil, ErrExpiredToken
	}
	return claims, nil
}

// RefreshToken creates a new token with extended expiry, preserving
// the original grants. This allows clients to refresh via datachannel
// without reconnecting.
func (v *TokenVerifier) RefreshToken(tokenStr string, ttl time.Duration) (string, error) {
	claims, err := v.Verify(tokenStr)
	if err != nil && !errors.Is(err, ErrExpiredToken) {
		return "", err
	}
	// Re-verify without expiration check for refresh
	if claims == nil {
		claims, err = verifyJWT(v.apiSecret, tokenStr)
		if err != nil {
			return "", err
		}
	}
	at := NewAccessToken(v.apiKey, v.apiSecret)
	at.claims = *claims
	at.claims.Iat = time.Now().Unix()
	at.claims.Exp = time.Now().Add(ttl).Unix()
	return at.Sign()
}

func b64Encode(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

func b64Decode(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}

func signJWT(secret string, claims Claims) (string, error) {
	header := jwtHeader{Alg: "HS256", Typ: "JWT"}
	headerJSON, _ := json.Marshal(header)
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	signingInput := b64Encode(headerJSON) + "." + b64Encode(claimsJSON)

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signingInput))
	signature := b64Encode(mac.Sum(nil))

	return signingInput + "." + signature, nil
}

func verifyJWT(secret, tokenStr string) (*Claims, error) {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return nil, ErrInvalidToken
	}

	signingInput := parts[0] + "." + parts[1]
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signingInput))
	expected := b64Encode(mac.Sum(nil))

	if !hmac.Equal([]byte(parts[2]), []byte(expected)) {
		return nil, ErrInvalidToken
	}

	claimsJSON, err := b64Decode(parts[1])
	if err != nil {
		return nil, ErrInvalidToken
	}

	var claims Claims
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		return nil, ErrInvalidToken
	}

	return &claims, nil
}
