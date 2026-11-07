package unit

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"voice_service/internal/events"
	"voice_service/internal/models"
	"voice_service/internal/repository"
	"voice_service/internal/service"
	"voice_service/tests/mocks"
)

// TestChannelFullErrorDescribesTheAlternatives covers the domain error text.
func TestChannelFullErrorDescribesTheAlternatives(t *testing.T) {
	t.Parallel()
	full := &models.ChannelFullError{Alternatives: []models.ChannelAlternative{
		{ChannelID: uuid.New(), Name: "Chill", Capacity: 4},
	}}
	message := full.Error()
	assert.NotEmpty(t, message)
	assert.Contains(t, message, "full")

	empty := &models.ChannelFullError{}
	assert.NotEmpty(t, empty.Error())
	assert.Error(t, empty)
}

// TestMemberProjectionValidation covers the EVT-002 payload guards.
func TestMemberProjectionValidation(t *testing.T) {
	t.Parallel()
	communityID, userID := uuid.New(), uuid.New()
	valid := events.MemberChangedPayload{
		CommunityID:   communityID.String(),
		UserID:        userID.String(),
		Change:        events.MemberChangeJoined,
		Status:        models.MemberStatusActive,
		MemberVersion: 1,
	}
	projection, err := valid.MemberProjection(sampleTime)
	require.NoError(t, err)
	assert.Equal(t, communityID, projection.CommunityID)
	assert.Equal(t, userID, projection.UserID)

	cases := map[string]func(*events.MemberChangedPayload){
		"community id": func(p *events.MemberChangedPayload) { p.CommunityID = "nope" },
		"user id":      func(p *events.MemberChangedPayload) { p.UserID = "nope" },
		"version":      func(p *events.MemberChangedPayload) { p.MemberVersion = 0 },
		"status":       func(p *events.MemberChangedPayload) { p.Status = "ghost" },
	}
	for name, mutate := range cases {
		mutate := mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			payload := valid
			mutate(&payload)
			_, err := payload.MemberProjection(sampleTime)
			require.Error(t, err)
		})
	}
}

// TestMemberRemovalClassification covers the removal rule of EVT-002.
func TestMemberRemovalClassification(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		payload events.MemberChangedPayload
		removal bool
	}{
		"removed":        {payload: events.MemberChangedPayload{Change: events.MemberChangeRemoved}, removal: true},
		"banned":         {payload: events.MemberChangedPayload{Change: events.MemberChangeBanned, IsBanned: true}, removal: true},
		"left":           {payload: events.MemberChangedPayload{Change: events.MemberChangeLeft}, removal: true},
		"status removed": {payload: events.MemberChangedPayload{Change: events.MemberChangeRolesChanged, Status: models.MemberStatusRemoved}, removal: true},
		"banned flag":    {payload: events.MemberChangedPayload{Change: events.MemberChangeUnmuted, IsBanned: true}, removal: true},
		"joined":         {payload: events.MemberChangedPayload{Change: events.MemberChangeJoined, Status: models.MemberStatusActive}, removal: false},
		"muted":          {payload: events.MemberChangedPayload{Change: events.MemberChangeMuted, Status: models.MemberStatusActive}, removal: false},
	}
	for name, testCase := range cases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, testCase.removal, testCase.payload.IsRemoval())
		})
	}
}

// TestChannelProjectionValidation covers the EVT-003 payload guards.
func TestChannelProjectionValidation(t *testing.T) {
	t.Parallel()
	communityID, channelID := uuid.New(), uuid.New()
	capacity := 4
	valid := events.ChannelChangedPayload{
		CommunityID:    communityID.String(),
		ChannelID:      channelID.String(),
		Change:         events.ChannelChangeCreated,
		Kind:           models.ChannelKindVoice,
		Name:           "General",
		Capacity:       &capacity,
		ChannelVersion: 1,
	}
	projection, err := valid.ChannelProjection(sampleTime)
	require.NoError(t, err)
	assert.True(t, projection.IsVoice())
	assert.False(t, projection.Deleted)

	deleted := valid
	deleted.Change = events.ChannelChangeDeleted
	projection, err = deleted.ChannelProjection(sampleTime)
	require.NoError(t, err)
	assert.True(t, projection.Deleted)

	tooLarge := 42
	cases := map[string]func(*events.ChannelChangedPayload){
		"community id": func(p *events.ChannelChangedPayload) { p.CommunityID = "nope" },
		"channel id":   func(p *events.ChannelChangedPayload) { p.ChannelID = "nope" },
		"version":      func(p *events.ChannelChangedPayload) { p.ChannelVersion = 0 },
		"kind":         func(p *events.ChannelChangedPayload) { p.Kind = "stage" },
		"capacity":     func(p *events.ChannelChangedPayload) { p.Capacity = &tooLarge },
	}
	for name, mutate := range cases {
		mutate := mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			payload := valid
			mutate(&payload)
			_, err := payload.ChannelProjection(sampleTime)
			require.Error(t, err)
		})
	}
}

// TestReadinessGuards covers the probe defaults and the shutdown flag.
func TestReadinessGuards(t *testing.T) {
	t.Parallel()
	probe := service.NewProbe("noop", nil)
	assert.Equal(t, "noop", probe.Name())
	require.NoError(t, probe.Check(context.Background()))

	readiness := service.NewReadiness([]service.Probe{probe}, 0)
	assert.False(t, readiness.ShuttingDown())
	require.NoError(t, readiness.Check(context.Background()))
	readiness.BeginShutdown()
	assert.True(t, readiness.ShuttingDown())
	require.ErrorIs(t, readiness.Check(context.Background()), service.ErrShuttingDown)
}

// TestTokenVerifierSupportsECDSAKeys covers the ES256 key format.
func TestTokenVerifierSupportsECDSAKeys(t *testing.T) {
	t.Parallel()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	jwk := map[string]string{
		"kty": "EC",
		"crv": "P-256",
		"x":   base64.RawURLEncoding.EncodeToString(privateKey.X.Bytes()),
		"y":   base64.RawURLEncoding.EncodeToString(privateKey.Y.Bytes()),
	}
	encoded, err := json.Marshal(jwk)
	require.NoError(t, err)

	verifier, err := service.NewTokenVerifier([]string{string(encoded)}, "", "")
	require.NoError(t, err)

	userID := uuid.New()
	token := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"sub":            userID.String(),
		"email_verified": true,
		"exp":            time.Now().Add(time.Hour).Unix(),
		"iat":            time.Now().Unix(),
	})
	signed, err := token.SignedString(privateKey)
	require.NoError(t, err)

	claims, err := verifier.Verify(signed)
	require.NoError(t, err)
	assert.Equal(t, userID, claims.UserID)
}

// TestTokenVerifierRejectsUnsupportedCurve covers an unsupported JWK curve.
func TestTokenVerifierRejectsUnsupportedCurve(t *testing.T) {
	t.Parallel()
	_, err := service.NewTokenVerifier([]string{`{"kty":"EC","crv":"P-192","x":"AA","y":"AA"}`}, "", "")
	require.Error(t, err)
}

// TestPoolConstructionAndPing covers the pool helper without a server.
func TestPoolConstructionAndPing(t *testing.T) {
	t.Parallel()
	pool, err := repository.NewPool(context.Background(), "postgres://voice:voice@127.0.0.1:1/voice_service")
	require.NoError(t, err)
	defer pool.Close()

	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.Error(t, repository.Ping(pingCtx, pool))
}

// TestProducerCloseWithoutClient covers the producer close guard.
func TestProducerCloseWithoutClient(t *testing.T) {
	t.Parallel()
	producer := events.NewKafkaProducerWithClient(nil, events.TopicVoiceEvents, nil)
	require.NoError(t, producer.Close())
}

// TestParticipantMinutesHelperIsExportedIntoMetrics covers the media split used
// after a session ends.
func TestParticipantMinutesHelperIsExportedIntoMetrics(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	f.Metrics.RecordParticipantMinutes(90*time.Second, true, false)
	assert.Equal(t, float64(1.5), metricValue(t, f.Registry, "voice_participant_minutes_total", map[string]string{"media": "audio"}))
	assert.Equal(t, float64(1.5), metricValue(t, f.Registry, "voice_participant_minutes_total", map[string]string{"media": "video"}))
	assert.Zero(t, metricValue(t, f.Registry, "voice_participant_minutes_total", map[string]string{"media": "screen"}))
}

// TestUnknownErrorsAreWrapped keeps error wrapping intact.
func TestUnknownErrorsAreWrapped(t *testing.T) {
	t.Parallel()
	base := errors.New("base")
	wrapped := errors.Join(base, errors.New("other"))
	require.ErrorIs(t, wrapped, base)
}
