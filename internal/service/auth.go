package service

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"voice_service/internal/models"
)

// allowedAlgorithms are the asymmetric signature algorithms accepted for
// identity access tokens.
var allowedAlgorithms = []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512"}

// accessTokenClaims is the claim set of an identity access token
// (docs/architecture.md §2).
type accessTokenClaims struct {
	SessionID     string `json:"sid"`
	EmailVerified bool   `json:"email_verified"`
	jwt.RegisteredClaims
}

// TokenVerifier verifies identity access tokens locally with the configured
// public keys (current + next for rotation). Identity is never called at
// request time.
type TokenVerifier struct {
	keys     []any
	issuer   string
	audience string
	now      func() time.Time
	parser   *jwt.Parser
}

// NewTokenVerifier builds a verifier from PEM/JWK public keys. The issuer and
// audience are enforced when non-empty.
func NewTokenVerifier(rawKeys []string, issuer, audience string) (*TokenVerifier, error) {
	keys, err := ParsePublicKeys(rawKeys)
	if err != nil {
		return nil, err
	}
	options := []jwt.ParserOption{
		jwt.WithValidMethods(allowedAlgorithms),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(30 * time.Second),
	}
	if issuer != "" {
		options = append(options, jwt.WithIssuer(issuer))
	}
	if audience != "" {
		options = append(options, jwt.WithAudience(audience))
	}
	parser := jwt.NewParser(options...)
	return &TokenVerifier{
		keys:     keys,
		issuer:   issuer,
		audience: audience,
		now:      time.Now,
		parser:   parser,
	}, nil
}

// WithClock replaces the verifier clock (used by tests).
func (v *TokenVerifier) WithClock(now func() time.Time) *TokenVerifier {
	if now != nil {
		v.now = now
		v.parser = jwt.NewParser(v.parserOptions()...)
	}
	return v
}

// parserOptions rebuilds the parser options with the current clock.
func (v *TokenVerifier) parserOptions() []jwt.ParserOption {
	options := []jwt.ParserOption{
		jwt.WithValidMethods(allowedAlgorithms),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(30 * time.Second),
		jwt.WithTimeFunc(v.now),
	}
	if v.issuer != "" {
		options = append(options, jwt.WithIssuer(v.issuer))
	}
	if v.audience != "" {
		options = append(options, jwt.WithAudience(v.audience))
	}
	return options
}

// Verify validates a raw access token and returns its claims. Every failure is
// reported as models.ErrUnauthorized (the reason is returned wrapped for logs).
func (v *TokenVerifier) Verify(raw string) (*models.AccessTokenClaims, error) {
	if raw == "" {
		return nil, fmt.Errorf("%w: missing access token", models.ErrUnauthorized)
	}
	var lastErr error
	for _, key := range v.keys {
		claims := &accessTokenClaims{}
		token, err := v.parser.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
			if !keyMatchesAlgorithm(key, t.Method.Alg()) {
				return nil, fmt.Errorf("key does not match algorithm %s", t.Method.Alg())
			}
			return key, nil
		})
		if err != nil || !token.Valid {
			lastErr = err
			continue
		}
		verified, err := v.toClaims(claims)
		if err != nil {
			lastErr = err
			continue
		}
		return verified, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no configured key verified the token")
	}
	return nil, fmt.Errorf("%w: %v", models.ErrUnauthorized, lastErr)
}

// toClaims converts untrusted claims into the domain type.
func (v *TokenVerifier) toClaims(claims *accessTokenClaims) (*models.AccessTokenClaims, error) {
	userID, err := uuid.Parse(claims.Subject)
	if err != nil {
		return nil, fmt.Errorf("sub claim is not a user id: %w", err)
	}
	if !claims.EmailVerified {
		return nil, errors.New("email is not verified")
	}
	var expiresAt, issuedAt time.Time
	if claims.ExpiresAt != nil {
		expiresAt = claims.ExpiresAt.Time
	}
	if claims.IssuedAt != nil {
		issuedAt = claims.IssuedAt.Time
	}
	return &models.AccessTokenClaims{
		UserID:        userID,
		SessionID:     claims.SessionID,
		EmailVerified: claims.EmailVerified,
		Issuer:        claims.Issuer,
		Audience:      claims.Audience,
		ExpiresAt:     expiresAt,
		IssuedAt:      issuedAt,
	}, nil
}

// keyMatchesAlgorithm reports whether key can verify a token signed with alg.
func keyMatchesAlgorithm(key any, alg string) bool {
	switch key.(type) {
	case *rsa.PublicKey:
		switch alg {
		case "RS256", "RS384", "RS512", "PS256", "PS384", "PS512":
			return true
		}
	case *ecdsa.PublicKey:
		switch alg {
		case "ES256", "ES384", "ES512":
			return true
		}
	case ed25519.PublicKey:
		return alg == "EdDSA"
	}
	return false
}

// ParsePublicKeys parses the configured verification keys. Each entry is a PEM
// block, a base64-encoded PEM block, or an RSA/EC JWK JSON object. Rotating the
// identity keys means adding the next key to the list.
func ParsePublicKeys(entries []string) ([]any, error) {
	keys := make([]any, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for i, entry := range entries {
		key, err := parseKeyEntry(entry)
		if err != nil {
			return nil, fmt.Errorf("IDENTITY_JWT_PUBLIC_KEYS[%d]: %w", i, err)
		}
		fingerprint := fmt.Sprintf("%v", key)
		if _, duplicate := seen[fingerprint]; duplicate {
			continue
		}
		seen[fingerprint] = struct{}{}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, errors.New("IDENTITY_JWT_PUBLIC_KEYS is empty")
	}
	return keys, nil
}

// parseKeyEntry parses one configured key entry.
func parseKeyEntry(entry string) (any, error) {
	trimmed := trimSpace(entry)
	if trimmed == "" {
		return nil, errors.New("empty key entry")
	}
	if trimmed[0] == '{' {
		return parseJWK([]byte(trimmed))
	}
	if block, _ := pem.Decode([]byte(trimmed)); block != nil {
		return parsePEMBlock(block)
	}
	decoded, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil {
		return nil, fmt.Errorf("key is neither PEM, base64 PEM, nor JWK: %w", err)
	}
	if block, _ := pem.Decode(decoded); block != nil {
		return parsePEMBlock(block)
	}
	if len(decoded) > 0 && decoded[0] == '{' {
		return parseJWK(decoded)
	}
	return nil, errors.New("decoded key is neither PEM nor JWK")
}

// parsePEMBlock decodes a public key PEM block.
func parsePEMBlock(block *pem.Block) (any, error) {
	switch block.Type {
	case "CERTIFICATE":
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse certificate: %w", err)
		}
		return cert.PublicKey, nil
	case "RSA PUBLIC KEY":
		return x509.ParsePKCS1PublicKey(block.Bytes)
	default:
		key, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse public key: %w", err)
		}
		return key, nil
	}
}

// jwk is the subset of the JSON Web Key format used for verification keys.
type jwk struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	N   string `json:"n"`
	E   string `json:"e"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// parseJWK decodes an RSA or EC JSON Web Key.
func parseJWK(data []byte) (any, error) {
	var key jwk
	if err := json.Unmarshal(data, &key); err != nil {
		return nil, fmt.Errorf("parse jwk: %w", err)
	}
	switch key.Kty {
	case "RSA":
		modulus, err := base64.RawURLEncoding.DecodeString(key.N)
		if err != nil {
			return nil, fmt.Errorf("parse jwk modulus: %w", err)
		}
		exponent, err := base64.RawURLEncoding.DecodeString(key.E)
		if err != nil {
			return nil, fmt.Errorf("parse jwk exponent: %w", err)
		}
		return &rsa.PublicKey{
			N: new(big.Int).SetBytes(modulus),
			E: int(new(big.Int).SetBytes(exponent).Int64()),
		}, nil
	case "EC":
		return parseECJWK(key)
	default:
		return nil, fmt.Errorf("unsupported jwk key type %q", key.Kty)
	}
}

// parseECJWK decodes a P-256/P-384/P-521 JSON Web Key.
func parseECJWK(key jwk) (any, error) {
	var curve elliptic.Curve
	var coordinateSize int
	switch key.Crv {
	case "P-256":
		curve = elliptic.P256()
		coordinateSize = 32
	case "P-384":
		curve = elliptic.P384()
		coordinateSize = 48
	case "P-521":
		curve = elliptic.P521()
		coordinateSize = 66
	default:
		return nil, fmt.Errorf("unsupported jwk curve %q", key.Crv)
	}
	xBytes, err := base64.RawURLEncoding.DecodeString(key.X)
	if err != nil {
		return nil, fmt.Errorf("parse jwk x: %w", err)
	}
	yBytes, err := base64.RawURLEncoding.DecodeString(key.Y)
	if err != nil {
		return nil, fmt.Errorf("parse jwk y: %w", err)
	}
	// A JWK carries the raw coordinates; the SEC1 uncompressed point encoding
	// prefixes them with 0x04 and pads both to the field size.
	point := make([]byte, 1+2*coordinateSize)
	point[0] = 4
	new(big.Int).SetBytes(xBytes).FillBytes(point[1 : 1+coordinateSize])
	new(big.Int).SetBytes(yBytes).FillBytes(point[1+coordinateSize:])
	publicKey, err := ecdsa.ParseUncompressedPublicKey(curve, point)
	if err != nil {
		return nil, fmt.Errorf("parse jwk point: %w", err)
	}
	return publicKey, nil
}

// trimSpace trims the surrounding whitespace of a key entry.
func trimSpace(v string) string { return strings.TrimSpace(v) }
