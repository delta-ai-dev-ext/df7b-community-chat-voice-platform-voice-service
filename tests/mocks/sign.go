package mocks

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/livekit/protocol/auth"
)

// KeyPair is a generated RSA key pair used to sign identity access tokens in
// tests.
type KeyPair struct {
	// Private signs the tokens.
	Private *rsa.PrivateKey
	// PublicPEM is the configured verification key entry.
	PublicPEM string
}

// NewKeyPair generates a 2048-bit RSA key pair.
func NewKeyPair() (*KeyPair, error) {
	private, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("mocks: generate key: %w", err)
	}
	der, err := x509.MarshalPKIXPublicKey(&private.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("mocks: marshal public key: %w", err)
	}
	encoded := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	return &KeyPair{Private: private, PublicPEM: string(encoded)}, nil
}

// JWK renders the public key as a JSON Web Key, as the deployment may
// distribute the identity keys in that form.
func (k *KeyPair) JWK() string {
	exponent := bigEndianBytes(k.Private.E)
	key := map[string]string{
		"kty": "RSA",
		"n":   base64.RawURLEncoding.EncodeToString(k.Private.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(exponent),
	}
	encoded, err := json.Marshal(key)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

// bigEndianBytes renders a positive int as minimal big-endian bytes.
func bigEndianBytes(value int) []byte {
	if value == 0 {
		return []byte{0}
	}
	out := make([]byte, 0, 4)
	for value > 0 {
		out = append([]byte{byte(value & 0xff)}, out...)
		value >>= 8
	}
	return out
}

// IdentityTokenOptions describes an identity access token to sign.
type IdentityTokenOptions struct {
	// Subject overrides the sub claim (defaults to the user id).
	Subject string
	// SessionID is the sid claim.
	SessionID string
	// Issuer is the iss claim.
	Issuer string
	// Audience is the aud claim.
	Audience string
	// EmailVerified is the email_verified claim.
	EmailVerified bool
	// ExpiresAt is the exp claim.
	ExpiresAt time.Time
	// IssuedAt is the iat claim.
	IssuedAt time.Time
}

// SignIdentityToken signs an identity access token with the key pair.
func (k *KeyPair) SignIdentityToken(userID uuid.UUID, opts IdentityTokenOptions) (string, error) {
	subject := opts.Subject
	if subject == "" {
		subject = userID.String()
	}
	expiresAt := opts.ExpiresAt
	if expiresAt.IsZero() {
		expiresAt = time.Now().Add(15 * time.Minute)
	}
	issuedAt := opts.IssuedAt
	if issuedAt.IsZero() {
		issuedAt = time.Now()
	}
	claims := jwt.MapClaims{
		"sub":            subject,
		"sid":            opts.SessionID,
		"email_verified": opts.EmailVerified,
		"iat":            issuedAt.Unix(),
		"exp":            expiresAt.Unix(),
	}
	if opts.Issuer != "" {
		claims["iss"] = opts.Issuer
	}
	if opts.Audience != "" {
		claims["aud"] = opts.Audience
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed, err := token.SignedString(k.Private)
	if err != nil {
		return "", fmt.Errorf("mocks: sign identity token: %w", err)
	}
	return signed, nil
}

// SignWebhook produces the LiveKit webhook Authorization header value for body:
// a signed API token carrying the sha256 hash of the raw body.
func SignWebhook(apiKey, secret string, body []byte) (string, error) {
	sum := sha256.Sum256(body)
	accessToken := auth.NewAccessToken(apiKey, secret).
		SetValidFor(time.Minute).
		SetSha256(base64.StdEncoding.EncodeToString(sum[:]))
	value, err := accessToken.ToJWT()
	if err != nil {
		return "", fmt.Errorf("mocks: sign webhook: %w", err)
	}
	return value, nil
}
