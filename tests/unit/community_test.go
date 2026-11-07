package unit

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"voice_service/internal/community"
	"voice_service/internal/events"
	"voice_service/internal/models"
)

// accessCheckURL is the API-002 endpoint of the tests.
func accessCheckURL(communityID, channelID uuid.UUID) string {
	return "http://community.test/internal/communities/" + communityID.String() +
		"/channels/" + channelID.String() + "/voice-access-checks"
}

// accessCheckBody builds a 200 API-002 answer.
func accessCheckBody(communityID, channelID, userID uuid.UUID, allowed bool, reason *string, capacity int) map[string]any {
	body := map[string]any{
		"community_id":  communityID.String(),
		"user_id":       userID.String(),
		"allowed":       allowed,
		"denial_reason": reason,
		"muted_until":   nil,
		"voice_channels": []map[string]any{
			{
				"channel_id": channelID.String(),
				"name":       "General",
				"capacity":   capacity,
			},
		},
	}
	if allowed {
		body["channel"] = map[string]any{
			"channel_id": channelID.String(),
			"name":       "General",
			"capacity":   capacity,
		}
	}
	return body
}

// TestAccessCheckSendsServiceTokenAndTrace covers the outbound contract.
func TestAccessCheckSendsServiceTokenAndTrace(t *testing.T) {
	httpmock.Activate(t)
	defer httpmock.DeactivateAndReset()

	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	url := accessCheckURL(communityID, channelID)
	httpmock.RegisterResponder(http.MethodPost, url, func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, "Bearer service-token", request.Header.Get("Authorization"))
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
		trace := request.Header.Get(events.TraceIDHeader)
		assert.True(t, events.ValidTraceID(trace))
		assert.Contains(t, request.Header.Get(events.TraceparentHeader), trace)

		var body map[string]string
		require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
		assert.Equal(t, userID.String(), body["user_id"])
		return httpmock.NewJsonResponse(http.StatusOK, accessCheckBody(communityID, channelID, userID, true, nil, 5))
	})

	client := community.NewClient("http://community.test", "service-token", 2*time.Second).WithHTTPClient(http.DefaultClient)
	result, err := client.CheckVoiceAccess(context.Background(), community.AccessCheckRequest{
		CommunityID: communityID,
		ChannelID:   channelID,
		UserID:      userID,
		TraceID:     events.NewTraceID(),
	})
	require.NoError(t, err)
	assert.True(t, result.Allowed)
	require.NotNil(t, result.Channel)
	assert.Equal(t, 5, result.Channel.Capacity)
	assert.Len(t, result.VoiceChannels, 1)
}

// TestAccessCheckRetriesOnceOnServerError covers the single-retry rule.
func TestAccessCheckRetriesOnceOnServerError(t *testing.T) {
	httpmock.Activate(t)
	defer httpmock.DeactivateAndReset()

	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	calls := 0
	httpmock.RegisterResponder(http.MethodPost, accessCheckURL(communityID, channelID),
		func(request *http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				return httpmock.NewStringResponse(http.StatusInternalServerError, "boom"), nil
			}
			return httpmock.NewJsonResponse(http.StatusOK, accessCheckBody(communityID, channelID, userID, true, nil, 4))
		})

	client := community.NewClient("http://community.test", "service-token", 2*time.Second)
	result, err := client.CheckVoiceAccess(context.Background(), community.AccessCheckRequest{
		CommunityID: communityID, ChannelID: channelID, UserID: userID, TraceID: events.NewTraceID(),
	})
	require.NoError(t, err)
	assert.True(t, result.Allowed)
	assert.Equal(t, 2, calls, "a 5xx is retried exactly once")
}

// TestAccessCheckGivesUpAfterOneRetry covers the bounded budget.
func TestAccessCheckGivesUpAfterOneRetry(t *testing.T) {
	httpmock.Activate(t)
	defer httpmock.DeactivateAndReset()

	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	calls := 0
	httpmock.RegisterResponder(http.MethodPost, accessCheckURL(communityID, channelID),
		func(*http.Request) (*http.Response, error) {
			calls++
			return httpmock.NewStringResponse(http.StatusServiceUnavailable, "unavailable"), nil
		})

	client := community.NewClient("http://community.test", "service-token", 2*time.Second)
	_, err := client.CheckVoiceAccess(context.Background(), community.AccessCheckRequest{
		CommunityID: communityID, ChannelID: channelID, UserID: userID,
	})
	require.ErrorIs(t, err, community.ErrUnavailable)
	assert.Equal(t, 2, calls)
}

// TestAccessCheckDoesNotRetryClientErrors covers 4xx behaviour.
func TestAccessCheckDoesNotRetryClientErrors(t *testing.T) {
	httpmock.Activate(t)
	defer httpmock.DeactivateAndReset()

	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	calls := 0
	httpmock.RegisterResponder(http.MethodPost, accessCheckURL(communityID, channelID),
		func(*http.Request) (*http.Response, error) {
			calls++
			return httpmock.NewStringResponse(http.StatusUnauthorized, "invalid service token"), nil
		})

	client := community.NewClient("http://community.test", "service-token", 2*time.Second)
	_, err := client.CheckVoiceAccess(context.Background(), community.AccessCheckRequest{
		CommunityID: communityID, ChannelID: channelID, UserID: userID,
	})
	require.ErrorIs(t, err, community.ErrUnavailable)
	assert.Equal(t, 1, calls, "a 401 from the provider is final")
}

// TestAccessCheckMapsNotFound covers the unknown-community answer.
func TestAccessCheckMapsNotFound(t *testing.T) {
	httpmock.Activate(t)
	defer httpmock.DeactivateAndReset()

	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	httpmock.RegisterResponder(http.MethodPost, accessCheckURL(communityID, channelID),
		httpmock.NewStringResponder(http.StatusNotFound, "community_not_found"))

	client := community.NewClient("http://community.test", "service-token", 2*time.Second)
	_, err := client.CheckVoiceAccess(context.Background(), community.AccessCheckRequest{
		CommunityID: communityID, ChannelID: channelID, UserID: userID,
	})
	require.ErrorIs(t, err, community.ErrCommunityNotFound)
}

// TestAccessCheckRejectsUntrustedBodies covers response validation.
func TestAccessCheckRejectsUntrustedBodies(t *testing.T) {
	cases := map[string]map[string]any{
		"missing channel when allowed": {
			"community_id": uuid.New().String(),
			"user_id":      uuid.New().String(),
			"allowed":      true,
		},
		"capacity out of range": {
			"community_id": uuid.New().String(),
			"user_id":      uuid.New().String(),
			"allowed":      true,
			"channel": map[string]any{
				"channel_id": uuid.New().String(),
				"name":       "General",
				"capacity":   42,
			},
		},
		"invalid community id": {
			"community_id": "not-a-uuid",
			"user_id":      uuid.New().String(),
			"allowed":      false,
		},
	}
	for name, body := range cases {
		body := body
		t.Run(name, func(t *testing.T) {
			httpmock.Activate(t)
			defer httpmock.DeactivateAndReset()

			communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
			httpmock.RegisterResponder(http.MethodPost, accessCheckURL(communityID, channelID),
				httpmock.NewJsonResponderOrPanic(http.StatusOK, body))

			client := community.NewClient("http://community.test", "service-token", 2*time.Second)
			_, err := client.CheckVoiceAccess(context.Background(), community.AccessCheckRequest{
				CommunityID: communityID, ChannelID: channelID, UserID: userID,
			})
			require.ErrorIs(t, err, community.ErrUnavailable)
		})
	}
}

// TestAccessCheckDeniedResponse covers the 200-with-denial contract.
func TestAccessCheckDeniedResponse(t *testing.T) {
	httpmock.Activate(t)
	defer httpmock.DeactivateAndReset()

	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	reason := "not_a_member"
	httpmock.RegisterResponder(http.MethodPost, accessCheckURL(communityID, channelID),
		httpmock.NewJsonResponderOrPanic(http.StatusOK, accessCheckBody(communityID, channelID, userID, false, &reason, 5)))

	client := community.NewClient("http://community.test", "service-token", 2*time.Second)
	result, err := client.CheckVoiceAccess(context.Background(), community.AccessCheckRequest{
		CommunityID: communityID, ChannelID: channelID, UserID: userID,
	})
	require.NoError(t, err)
	assert.False(t, result.Allowed)
	assert.Equal(t, "not_a_member", result.DenialReason)
	assert.Nil(t, result.Channel)
}

// TestAccessCheckRequiresIdentifiers covers boundary validation.
func TestAccessCheckRequiresIdentifiers(t *testing.T) {
	t.Parallel()
	client := community.NewClient("http://community.test", "service-token", 2*time.Second)
	_, err := client.CheckVoiceAccess(context.Background(), community.AccessCheckRequest{})
	require.Error(t, err)
}

// TestAccessCheckTimesOut covers the bounded budget as a fail-closed answer.
func TestAccessCheckTimesOut(t *testing.T) {
	httpmock.Activate(t)
	defer httpmock.DeactivateAndReset()

	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	httpmock.RegisterResponder(http.MethodPost, accessCheckURL(communityID, channelID),
		func(*http.Request) (*http.Response, error) {
			time.Sleep(80 * time.Millisecond)
			return httpmock.NewStringResponse(http.StatusOK, "{}"), nil
		})

	client := community.NewClient("http://community.test", "service-token", 20*time.Millisecond)
	_, err := client.CheckVoiceAccess(context.Background(), community.AccessCheckRequest{
		CommunityID: communityID, ChannelID: channelID, UserID: userID,
	})
	require.ErrorIs(t, err, community.ErrUnavailable)
}

// TestChannelSummaryJSONShape keeps the shared contract field names.
func TestChannelSummaryJSONShape(t *testing.T) {
	t.Parallel()
	encoded, err := json.Marshal(models.ChannelSummary{ChannelID: uuid.New(), Name: "General", Capacity: 4})
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"channel_id"`)
	assert.Contains(t, string(encoded), `"name"`)
	assert.Contains(t, string(encoded), `"capacity"`)
}
