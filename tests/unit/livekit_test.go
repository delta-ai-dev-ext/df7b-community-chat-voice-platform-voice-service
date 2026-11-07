package unit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/livekit/protocol/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"voice_service/internal/livekit"
	"voice_service/tests/mocks"
)

// TestTokenMinterGrantsPublishingByDefault covers US-001: the token joins the
// room, subscribes, and publishes microphone, camera, and screen share.
func TestTokenMinterGrantsPublishingByDefault(t *testing.T) {
	t.Parallel()
	minter := livekit.NewSDKTokenMinter("api-key", "api-secret")
	issuedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	minter.WithClock(func() time.Time { return issuedAt })
	userID := uuid.New()

	token, err := minter.MintParticipantToken(context.Background(), livekit.TokenRequest{
		RoomName:   "voice-room",
		Identity:   userID.String(),
		CanPublish: true,
		TTL:        90 * time.Second,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, token.Value)
	assert.Equal(t, issuedAt.Add(90*time.Second), token.ExpiresAt)

	grant := parseGrant(t, token.Value, "api-key", "api-secret")
	assert.True(t, grant.RoomJoin)
	assert.Equal(t, "voice-room", grant.Room)
	require.NotNil(t, grant.CanPublish)
	assert.True(t, *grant.CanPublish)
	require.NotNil(t, grant.CanSubscribe)
	assert.True(t, *grant.CanSubscribe)
	assert.Equal(t, []string{"microphone", "camera", "screen_share"}, grant.CanPublishSources)
}

// TestTokenMinterListenOnlyForMutedMembers covers US-003: no publishing grants.
func TestTokenMinterListenOnlyForMutedMembers(t *testing.T) {
	t.Parallel()
	minter := livekit.NewSDKTokenMinter("api-key", "api-secret")
	token, err := minter.MintParticipantToken(context.Background(), livekit.TokenRequest{
		RoomName:   "voice-room",
		Identity:   uuid.NewString(),
		CanPublish: false,
		TTL:        time.Minute,
	})
	require.NoError(t, err)

	grant := parseGrant(t, token.Value, "api-key", "api-secret")
	assert.True(t, grant.RoomJoin)
	require.NotNil(t, grant.CanPublish)
	assert.False(t, *grant.CanPublish, "a muted member must not publish")
	assert.Nil(t, grant.CanPublishSources)
}

// TestTokenMinterCapsTheTokenLifetime covers the pinned <= 5 minute rule.
func TestTokenMinterCapsTheTokenLifetime(t *testing.T) {
	t.Parallel()
	minter := livekit.NewSDKTokenMinter("api-key", "api-secret")
	issuedAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	minter.WithClock(func() time.Time { return issuedAt })

	token, err := minter.MintParticipantToken(context.Background(), livekit.TokenRequest{
		RoomName: "voice-room",
		Identity: uuid.NewString(),
		TTL:      time.Hour,
	})
	require.NoError(t, err)
	assert.Equal(t, issuedAt.Add(livekit.MaxParticipantTokenTTL), token.ExpiresAt)
	assert.LessOrEqual(t, token.ExpiresAt.Sub(issuedAt), 5*time.Minute)
}

// TestTokenMinterRequiresRoomAndIdentity covers the guards.
func TestTokenMinterRequiresRoomAndIdentity(t *testing.T) {
	t.Parallel()
	minter := livekit.NewSDKTokenMinter("api-key", "api-secret")
	_, err := minter.MintParticipantToken(context.Background(), livekit.TokenRequest{Identity: "user"})
	require.Error(t, err)
	_, err = minter.MintParticipantToken(context.Background(), livekit.TokenRequest{RoomName: "room"})
	require.Error(t, err)
}

// parseGrant decodes the video grant of a minted token.
func parseGrant(t *testing.T, raw, apiKey, apiSecret string) *auth.VideoGrant {
	t.Helper()
	verified, err := auth.ParseAPIToken(raw)
	require.NoError(t, err)
	assert.Equal(t, apiKey, verified.APIKey())
	_, grants, err := verified.Verify(apiSecret)
	require.NoError(t, err)
	require.NotNil(t, grants.Video)
	return grants.Video
}

// TestWebhookVerifierAcceptsSignedBody covers US-011: a correctly signed
// webhook is decoded into the event.
func TestWebhookVerifierAcceptsSignedBody(t *testing.T) {
	t.Parallel()
	body := []byte(`{"id":"evt-1","event":"participant_joined","room":{"name":"voice-room"},"participant":{"identity":"user"}}`)
	header, err := mocks.SignWebhook("api-key", "webhook-secret", body)
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodPost, "/voice/livekit/webhooks", strings.NewReader(string(body)))
	request.Header.Set("Authorization", header)
	event, err := livekit.NewWebhookVerifier("api-key", "webhook-secret").Verify(request)
	require.NoError(t, err)
	assert.Equal(t, "participant_joined", event.GetEvent())
	assert.Equal(t, "voice-room", event.GetRoom().GetName())
}

// TestWebhookVerifierRejectsUnsignedAndTamperedBodies covers the 401 path.
func TestWebhookVerifierRejectsUnsignedAndTamperedBodies(t *testing.T) {
	t.Parallel()
	body := []byte(`{"event":"participant_joined","room":{"name":"voice-room"}}`)
	validHeader, err := mocks.SignWebhook("api-key", "webhook-secret", body)
	require.NoError(t, err)
	wrongSecretHeader, err := mocks.SignWebhook("api-key", "other-secret", body)
	require.NoError(t, err)

	cases := map[string]struct {
		header string
		body   string
	}{
		"missing header": {header: "", body: string(body)},
		"invalid token":  {header: "not-a-token", body: string(body)},
		"wrong secret":   {header: wrongSecretHeader, body: string(body)},
		"tampered body":  {header: validHeader, body: `{"event":"room_finished"}`},
	}
	for name, testCase := range cases {
		testCase := testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			request := httptest.NewRequest(http.MethodPost, "/voice/livekit/webhooks", strings.NewReader(testCase.body))
			if testCase.header != "" {
				request.Header.Set("Authorization", testCase.header)
			}
			_, err := livekit.NewWebhookVerifier("api-key", "webhook-secret").Verify(request)
			require.Error(t, err)
			assert.True(t, livekit.IsInvalidSignature(err))
		})
	}
}

// TestWebhookVerifierRejectsMalformedPayloads covers signed but invalid bodies.
func TestWebhookVerifierRejectsMalformedPayloads(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"not json":           `not-json`,
		"missing event":      `{"room":{"name":"voice-room"}}`,
		"missing webhook id": `{"event":"participant_joined","room":{"name":"voice-room"}}`,
	}
	for name, payload := range cases {
		payload := payload
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			body := []byte(payload)
			header, err := mocks.SignWebhook("api-key", "webhook-secret", body)
			require.NoError(t, err)
			request := httptest.NewRequest(http.MethodPost, "/voice/livekit/webhooks", strings.NewReader(payload))
			request.Header.Set("Authorization", header)

			_, err = livekit.NewWebhookVerifier("api-key", "webhook-secret").Verify(request)
			require.Error(t, err)
			assert.True(t, livekit.IsMalformedWebhook(err))
		})
	}
}

// TestSDKClientRequiresURLCredentials covers the adapter constructor guard.
func TestSDKClientRequiresURLCredentials(t *testing.T) {
	t.Parallel()
	_, err := livekit.NewSDKClient("", "key", "secret")
	require.Error(t, err)
	_, err = livekit.NewSDKClient("wss://livekit.test", "key", "")
	require.Error(t, err)
}

// TestSDKClientBuildsFromCredentials covers the adapter constructor.
func TestSDKClientBuildsFromCredentials(t *testing.T) {
	t.Parallel()
	client, err := livekit.NewSDKClient("wss://livekit.test", "key", "secret")
	require.NoError(t, err)
	require.NotNil(t, client)
}
