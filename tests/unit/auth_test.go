package unit

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"voice_service/internal/models"
	"voice_service/internal/service"
	"voice_service/tests/mocks"
)

// newVerifier builds a verifier for the key pair.
func newVerifier(t *testing.T, keyPair *mocks.KeyPair, issuer, audience string) *service.TokenVerifier {
	t.Helper()
	verifier, err := service.NewTokenVerifier([]string{keyPair.PublicPEM}, issuer, audience)
	require.NoError(t, err)
	return verifier
}

// TestTokenVerifierAcceptsValidToken covers the happy path.
func TestTokenVerifierAcceptsValidToken(t *testing.T) {
	t.Parallel()
	keyPair, err := mocks.NewKeyPair()
	require.NoError(t, err)
	userID := uuid.New()
	verifier := newVerifier(t, keyPair, "https://identity.test", "platform")

	raw, err := keyPair.SignIdentityToken(userID, mocks.IdentityTokenOptions{
		SessionID:     "session-1",
		Issuer:        "https://identity.test",
		Audience:      "platform",
		EmailVerified: true,
	})
	require.NoError(t, err)

	claims, err := verifier.Verify(raw)
	require.NoError(t, err)
	assert.Equal(t, userID, claims.UserID)
	assert.Equal(t, "session-1", claims.SessionID)
	assert.True(t, claims.EmailVerified)
	assert.True(t, claims.ExpiresAt.After(time.Now()))
}

// TestTokenVerifierSupportsKeyRotation covers the current + next key rule: a
// token signed with the next key verifies while the previous key is still
// configured.
func TestTokenVerifierSupportsKeyRotation(t *testing.T) {
	t.Parallel()
	previous, err := mocks.NewKeyPair()
	require.NoError(t, err)
	next, err := mocks.NewKeyPair()
	require.NoError(t, err)

	verifier, err := service.NewTokenVerifier([]string{previous.PublicPEM, next.PublicPEM}, "", "")
	require.NoError(t, err)
	userID := uuid.New()

	raw, err := next.SignIdentityToken(userID, mocks.IdentityTokenOptions{EmailVerified: true})
	require.NoError(t, err)
	claims, err := verifier.Verify(raw)
	require.NoError(t, err)
	assert.Equal(t, userID, claims.UserID)

	// A key that is not configured must not verify.
	unknown, err := mocks.NewKeyPair()
	require.NoError(t, err)
	foreign, err := unknown.SignIdentityToken(userID, mocks.IdentityTokenOptions{EmailVerified: true})
	require.NoError(t, err)
	_, err = verifier.Verify(foreign)
	require.ErrorIs(t, err, models.ErrUnauthorized)
}

// TestTokenVerifierRejectsInvalidTokens covers US-004: invalid, expired, and
// unverified tokens yield 401.
func TestTokenVerifierRejectsInvalidTokens(t *testing.T) {
	t.Parallel()
	keyPair, err := mocks.NewKeyPair()
	require.NoError(t, err)
	userID := uuid.New()
	verifier := newVerifier(t, keyPair, "https://identity.test", "platform")

	expired, err := keyPair.SignIdentityToken(userID, mocks.IdentityTokenOptions{
		Issuer:        "https://identity.test",
		Audience:      "platform",
		EmailVerified: true,
		ExpiresAt:     time.Now().Add(-2 * time.Hour),
		IssuedAt:      time.Now().Add(-3 * time.Hour),
	})
	require.NoError(t, err)

	wrongAudience, err := keyPair.SignIdentityToken(userID, mocks.IdentityTokenOptions{
		Issuer:        "https://identity.test",
		Audience:      "somebody-else",
		EmailVerified: true,
	})
	require.NoError(t, err)

	wrongIssuer, err := keyPair.SignIdentityToken(userID, mocks.IdentityTokenOptions{
		Issuer:        "https://evil.test",
		Audience:      "platform",
		EmailVerified: true,
	})
	require.NoError(t, err)

	unverifiedEmail, err := keyPair.SignIdentityToken(userID, mocks.IdentityTokenOptions{
		Issuer:        "https://identity.test",
		Audience:      "platform",
		EmailVerified: false,
	})
	require.NoError(t, err)

	badSubject, err := keyPair.SignIdentityToken(userID, mocks.IdentityTokenOptions{
		Subject:       "not-a-uuid",
		Issuer:        "https://identity.test",
		Audience:      "platform",
		EmailVerified: true,
	})
	require.NoError(t, err)

	hmacForged := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":            userID.String(),
		"email_verified": true,
		"exp":            time.Now().Add(time.Hour).Unix(),
	})
	forged, err := hmacForged.SignedString([]byte("secret"))
	require.NoError(t, err)

	cases := map[string]string{
		"empty":           "",
		"garbage":         "not-a-token",
		"expired":         expired,
		"wrong audience":  wrongAudience,
		"wrong issuer":    wrongIssuer,
		"unverified mail": unverifiedEmail,
		"bad subject":     badSubject,
		"hmac forged":     forged,
	}
	for name, raw := range cases {
		raw := raw
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := verifier.Verify(raw)
			require.ErrorIs(t, err, models.ErrUnauthorized)
		})
	}
}

// TestTokenVerifierHonorsClock covers deterministic time control.
func TestTokenVerifierHonorsClock(t *testing.T) {
	t.Parallel()
	keyPair, err := mocks.NewKeyPair()
	require.NoError(t, err)
	userID := uuid.New()
	verifier := newVerifier(t, keyPair, "", "").WithClock(func() time.Time {
		return time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	})

	raw, err := keyPair.SignIdentityToken(userID, mocks.IdentityTokenOptions{
		EmailVerified: true,
		ExpiresAt:     time.Date(2030, 1, 1, 0, 10, 0, 0, time.UTC),
		IssuedAt:      time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	_, err = verifier.Verify(raw)
	require.NoError(t, err)
}

// TestParsePublicKeysAcceptsDeploymentFormats covers PEM, base64 PEM, and JWK.
func TestParsePublicKeysAcceptsDeploymentFormats(t *testing.T) {
	t.Parallel()
	keyPair, err := mocks.NewKeyPair()
	require.NoError(t, err)

	base64PEM := base64.StdEncoding.EncodeToString([]byte(keyPair.PublicPEM))
	keys, err := service.ParsePublicKeys([]string{keyPair.PublicPEM, base64PEM, keyPair.JWK()})
	require.NoError(t, err)
	assert.NotEmpty(t, keys)
}

// TestParsePublicKeysRejectsBadEntries covers configuration errors.
func TestParsePublicKeysRejectsBadEntries(t *testing.T) {
	t.Parallel()
	_, err := service.ParsePublicKeys(nil)
	require.Error(t, err)
	_, err = service.ParsePublicKeys([]string{"not-a-key"})
	require.Error(t, err)
	_, err = service.ParsePublicKeys([]string{""})
	require.Error(t, err)
	_, err = service.ParsePublicKeys([]string{`{"kty":"oct","k":"abc"}`})
	require.Error(t, err)
	_, err = service.NewTokenVerifier([]string{"nonsense"}, "", "")
	require.Error(t, err)
}
