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
	ErrInvalidToken     = errors.New("invalid token")
	ErrExpiredToken     = errors.New("token expired")
	ErrTokenNotYetValid = errors.New("token not yet valid")
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

func (t *AccessToken) SetRoomGrant(grant *proto.RoomGrant) *AccessToken {
	t.ensureGrants()
	t.claims.Grants.Room = grant
	return t
}

func (t *AccessToken) SetMediaGrant(grant *proto.MediaGrant) *AccessToken {
	t.ensureGrants()
	t.claims.Grants.Media = grant
	return t
}

func (t *AccessToken) SetSIPGrant(grant *proto.SIPGrant) *AccessToken {
	t.ensureGrants()
	t.claims.Grants.SIP = grant
	return t
}

func (t *AccessToken) SetMetadata(metadata string) *AccessToken {
	t.ensureGrants()
	t.claims.Grants.Metadata = metadata
	return t
}

func (t *AccessToken) SetAttributes(attrs map[string]string) *AccessToken {
	t.ensureGrants()
	t.claims.Grants.Attributes = attrs
	return t
}

func (t *AccessToken) SetKind(kind string) *AccessToken {
	t.ensureGrants()
	t.claims.Grants.Kind = kind
	return t
}

func (t *AccessToken) SetRoomConfig(config *proto.RoomConfiguration) *AccessToken {
	t.ensureGrants()
	t.claims.Grants.RoomConfig = config
	return t
}

func (t *AccessToken) ensureGrants() {
	if t.claims.Grants == nil {
		t.claims.Grants = &proto.ClaimGrants{}
	}
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
	now := time.Now().Unix()
	if claims.Nbf > 0 && now < claims.Nbf {
		return nil, ErrTokenNotYetValid
	}
	if claims.Exp < now {
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
	if claims == nil {
		claims, err = verifyJWT(v.apiSecret, tokenStr)
		if err != nil {
			return "", err
		}
	}
	now := time.Now()
	at := NewAccessToken(v.apiKey, v.apiSecret)
	at.claims = *claims
	at.claims.Iat = now.Unix()
	at.claims.Nbf = now.Unix()
	at.claims.Exp = now.Add(ttl).Unix()
	return at.Sign()
}

func b64Encode(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

func b64Decode(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}

// B64Decode exposes base64url decoding for CLI tools that need
// to inspect raw JWT payloads.
func B64Decode(s string) ([]byte, error) {
	return b64Decode(s)
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
