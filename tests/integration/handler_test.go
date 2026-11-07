// Package integration exercises the HTTP surface of voice_service end to end:
// gin routing, JWT authentication, LiveKit webhook verification, the probes, and
// the Prometheus endpoint, all wired to the in-memory fakes.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"voice_service/internal/community"
	"voice_service/internal/config"
	"voice_service/internal/handler"
	"voice_service/internal/livekit"
	"voice_service/internal/models"
	"voice_service/internal/service"
	"voice_service/tests/mocks"
)

// harness wires the real router around the in-memory fakes.
type harness struct {
	fixture   *mocks.Fixture
	router    *gin.Engine
	keys      *mocks.KeyPair
	readiness *service.Readiness
	probeMu   sync.Mutex
	probeErr  error
}

// newHarness builds the HTTP test harness.
func newHarness(t *testing.T) *harness {
	t.Helper()
	fixture := mocks.NewFixture()
	keys, err := mocks.NewKeyPair()
	require.NoError(t, err)
	tokens, err := service.NewTokenVerifier([]string{keys.PublicPEM}, "", "")
	require.NoError(t, err)

	h := &harness{fixture: fixture, keys: keys}
	readiness := service.NewReadiness([]service.Probe{
		service.NewProbe("postgres", func(context.Context) error {
			h.probeMu.Lock()
			defer h.probeMu.Unlock()
			return h.probeErr
		}),
	}, time.Second)
	h.readiness = readiness

	collaborators := fixture.Collaborators()
	collaborators.LiveKit = service.NewInstrumentedRoomClient(fixture.LiveKit, fixture.Metrics)
	h.router = handler.New(handler.Deps{
		Join:      service.NewJoinService(collaborators),
		Leave:     service.NewLeaveService(collaborators),
		Presence:  service.NewPresenceService(collaborators),
		Webhook:   service.NewWebhookProcessor(collaborators),
		Verifier:  livekit.NewWebhookVerifier("api-key", "webhook-secret"),
		Tokens:    tokens,
		Readiness: readiness,
		Metrics:   fixture.Metrics,
		Gatherer:  fixture.Registry,
	}).Router()
	return h
}

// setProbe adjusts the readiness probe outcome.
func (h *harness) setProbe(err error) {
	h.probeMu.Lock()
	defer h.probeMu.Unlock()
	h.probeErr = err
}

// token mints a valid identity access token.
func (h *harness) token(t *testing.T, userID uuid.UUID) string {
	t.Helper()
	raw, err := h.keys.SignIdentityToken(userID, mocks.IdentityTokenOptions{EmailVerified: true})
	require.NoError(t, err)
	return raw
}

// do performs one request against the router.
func (h *harness) do(t *testing.T, method, path, body, token string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	h.router.ServeHTTP(recorder, request)
	return recorder
}

// decodeError reads an {"error","detail"} body.
func decodeError(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	return body
}

// expectError asserts the documented error envelope.
func expectError(t *testing.T, recorder *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	require.Equal(t, status, recorder.Code, recorder.Body.String())
	body := decodeError(t, recorder)
	assert.Equal(t, code, body["error"])
	assert.NotEmpty(t, body["detail"])
}

// joinPath builds the join endpoint path.
func joinPath(channelID uuid.UUID) string {
	return "/voice/channels/" + channelID.String() + "/join"
}

// leavePath builds the leave endpoint path.
func leavePath(channelID uuid.UUID) string {
	return "/voice/channels/" + channelID.String() + "/leave"
}

// presencePath builds the presence endpoint path.
func presencePath(communityID uuid.UUID) string {
	return "/communities/" + communityID.String() + "/voice-presence"
}

// TestJoinEndpoint covers US-001 end to end.
func TestJoinEndpoint(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	h.fixture.Access.Result = &community.AccessCheckResult{
		CommunityID: communityID,
		UserID:      userID,
		Allowed:     true,
		Channel:     &models.ChannelSummary{ChannelID: channelID, Name: "General", Capacity: 5},
		VoiceChannels: []models.ChannelSummary{
			{ChannelID: channelID, Name: "General", Capacity: 5},
		},
	}

	body := `{"community_id":"` + communityID.String() + `"}`
	recorder := h.do(t, http.MethodPost, joinPath(channelID), body, h.token(t, userID), nil)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var response models.JoinResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	assert.Equal(t, "wss://livekit.test", response.LiveKitURL)
	assert.Equal(t, "participant-token", response.Token)
	assert.Equal(t, service.RoomName(channelID), response.RoomName)
	assert.True(t, response.CanPublishAudio)
	assert.NotEmpty(t, recorder.Header().Get("X-Trace-Id"))
}

// TestJoinEndpointAuthentication covers the 401 paths.
func TestJoinEndpointAuthentication(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	body := `{"community_id":"` + communityID.String() + `"}`

	recorder := h.do(t, http.MethodPost, joinPath(channelID), body, "", nil)
	expectError(t, recorder, http.StatusUnauthorized, "unauthorized")

	recorder = h.do(t, http.MethodPost, joinPath(channelID), body, "not-a-token", nil)
	expectError(t, recorder, http.StatusUnauthorized, "unauthorized")

	expired, err := h.keys.SignIdentityToken(userID, mocks.IdentityTokenOptions{
		EmailVerified: true,
		ExpiresAt:     time.Now().Add(-time.Hour),
		IssuedAt:      time.Now().Add(-2 * time.Hour),
	})
	require.NoError(t, err)
	recorder = h.do(t, http.MethodPost, joinPath(channelID), body, expired, nil)
	expectError(t, recorder, http.StatusUnauthorized, "unauthorized")

	assert.Equal(t, 0, h.fixture.Presence.Len())
}

// TestJoinEndpointValidation covers the 400 paths.
func TestJoinEndpointValidation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	userID := uuid.New()
	token := h.token(t, userID)
	communityID, channelID := uuid.New(), uuid.New()

	recorder := h.do(t, http.MethodPost, "/voice/channels/not-a-uuid/join",
		`{"community_id":"`+communityID.String()+`"}`, token, nil)
	expectError(t, recorder, http.StatusBadRequest, "invalid_request")

	recorder = h.do(t, http.MethodPost, joinPath(channelID), `{`, token, nil)
	expectError(t, recorder, http.StatusBadRequest, "invalid_request")

	recorder = h.do(t, http.MethodPost, joinPath(channelID), `{}`, token, nil)
	expectError(t, recorder, http.StatusBadRequest, "invalid_request")
}

// TestJoinEndpointDenials covers the 403/404 mapping (US-004).
func TestJoinEndpointDenials(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		reason string
		status int
		code   string
	}{
		{name: "not a member", reason: "not_a_member", status: http.StatusForbidden, code: "not_a_member"},
		{name: "unknown channel", reason: "channel_not_found", status: http.StatusNotFound, code: "channel_not_found"},
		{name: "text channel", reason: "not_a_voice_channel", status: http.StatusNotFound, code: "not_a_voice_channel"},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
			h.fixture.Access.Result = &community.AccessCheckResult{Allowed: false, DenialReason: testCase.reason}

			recorder := h.do(t, http.MethodPost, joinPath(channelID),
				`{"community_id":"`+communityID.String()+`"}`, h.token(t, userID), nil)
			expectError(t, recorder, testCase.status, testCase.code)
			assert.Equal(t, 0, h.fixture.Presence.Len())
		})
	}
}

// TestJoinEndpointFullChannel covers US-002 including the alternatives list.
func TestJoinEndpointFullChannel(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	alternativeID := uuid.New()
	h.fixture.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 1})
	h.fixture.Rooms.Seed(models.VoiceRoom{ChannelID: alternativeID, CommunityID: communityID, Capacity: 4})
	h.fixture.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: uuid.New()})
	h.fixture.Access.Result = &community.AccessCheckResult{
		Allowed: true,
		Channel: &models.ChannelSummary{ChannelID: channelID, Name: "General", Capacity: 1},
		VoiceChannels: []models.ChannelSummary{
			{ChannelID: channelID, Name: "General", Capacity: 1},
			{ChannelID: alternativeID, Name: "Chill", Capacity: 4},
		},
	}

	recorder := h.do(t, http.MethodPost, joinPath(channelID),
		`{"community_id":"`+communityID.String()+`"}`, h.token(t, userID), nil)
	require.Equal(t, http.StatusConflict, recorder.Code, recorder.Body.String())

	var body models.ChannelFullResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	assert.Equal(t, "voice_channel_full", body.Error)
	require.Len(t, body.Alternatives, 1)
	assert.Equal(t, alternativeID, body.Alternatives[0].ChannelID)
}

// TestJoinEndpointFailsClosed covers the 503 mapping when API-002 is down.
func TestJoinEndpointFailsClosed(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	h.fixture.Access.Err = community.ErrUnavailable

	recorder := h.do(t, http.MethodPost, joinPath(channelID),
		`{"community_id":"`+communityID.String()+`"}`, h.token(t, userID), nil)
	expectError(t, recorder, http.StatusServiceUnavailable, "voice_authorization_unavailable")
	assert.Equal(t, 0, h.fixture.Presence.Len())
}

// TestJoinEndpointMediaUnavailable covers the LiveKit 503 mapping.
func TestJoinEndpointMediaUnavailable(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	h.fixture.LiveKit.FailCreate = errors.New("livekit unavailable")
	h.fixture.Access.Result = &community.AccessCheckResult{
		Allowed: true,
		Channel: &models.ChannelSummary{ChannelID: channelID, Name: "General", Capacity: 4},
	}

	recorder := h.do(t, http.MethodPost, joinPath(channelID),
		`{"community_id":"`+communityID.String()+`"}`, h.token(t, userID), nil)
	expectError(t, recorder, http.StatusServiceUnavailable, "voice_media_unavailable")
	assert.Equal(t, 0, h.fixture.Seats.LiveReservations())
}

// TestJoinEndpointInternalError covers the 500 mapping.
func TestJoinEndpointInternalError(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	h.fixture.Rooms.Err = errors.New("database down")
	h.fixture.Access.Result = &community.AccessCheckResult{
		Allowed: true,
		Channel: &models.ChannelSummary{ChannelID: channelID, Name: "General", Capacity: 4},
	}

	recorder := h.do(t, http.MethodPost, joinPath(channelID),
		`{"community_id":"`+communityID.String()+`"}`, h.token(t, userID), nil)
	expectError(t, recorder, http.StatusInternalServerError, "internal_error")
}

// TestLeaveEndpoint covers US-005.
func TestLeaveEndpoint(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	h.fixture.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	h.fixture.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID})
	token := h.token(t, userID)

	recorder := h.do(t, http.MethodPost, leavePath(channelID), "", token, nil)
	require.Equal(t, http.StatusNoContent, recorder.Code, recorder.Body.String())
	assert.Equal(t, 0, h.fixture.Presence.Len())

	// A second leave is a no-op with the same status.
	recorder = h.do(t, http.MethodPost, leavePath(channelID), "", token, nil)
	require.Equal(t, http.StatusNoContent, recorder.Code)

	recorder = h.do(t, http.MethodPost, leavePath(uuid.New()), "", token, nil)
	expectError(t, recorder, http.StatusNotFound, "channel_not_found")

	recorder = h.do(t, http.MethodPost, leavePath(channelID), "", "", nil)
	expectError(t, recorder, http.StatusUnauthorized, "unauthorized")
}

// TestPresenceEndpoint covers US-006.
func TestPresenceEndpoint(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	capacity := 4
	_, err := h.fixture.Projections.UpsertMember(context.Background(), models.MemberProjection{
		CommunityID: communityID, UserID: userID, Status: models.MemberStatusActive, MemberVersion: 1,
	})
	require.NoError(t, err)
	_, err = h.fixture.Projections.UpsertChannel(context.Background(), models.ChannelProjection{
		CommunityID: communityID, ChannelID: channelID, Kind: models.ChannelKindVoice,
		Name: "General", Capacity: &capacity, ChannelVersion: 1,
	})
	require.NoError(t, err)
	h.fixture.Presence.Seed(models.Presence{
		CommunityID: communityID, ChannelID: channelID, UserID: uuid.New(), PublishingAllowed: true,
	})

	recorder := h.do(t, http.MethodGet, presencePath(communityID), "", h.token(t, userID), nil)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var snapshot models.PresenceResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &snapshot))
	assert.Equal(t, communityID, snapshot.CommunityID)
	require.Len(t, snapshot.Channels, 1)
	assert.Equal(t, 1, snapshot.Channels[0].SeatsUsed)
	assert.Equal(t, 4, snapshot.Channels[0].Capacity)

	// A non-member is refused.
	recorder = h.do(t, http.MethodGet, presencePath(communityID), "", h.token(t, uuid.New()), nil)
	expectError(t, recorder, http.StatusForbidden, "not_a_member")

	recorder = h.do(t, http.MethodGet, "/communities/not-a-uuid/voice-presence", "", h.token(t, userID), nil)
	expectError(t, recorder, http.StatusBadRequest, "invalid_request")
}

// TestWebhookEndpoint covers US-011: signed webhooks are applied, unsigned ones
// are rejected without any state change.
func TestWebhookEndpoint(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	h.fixture.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})

	body := `{"id":"evt-1","event":"participant_joined","room":{"name":"` + service.RoomName(channelID) +
		`"},"participant":{"identity":"` + userID.String() + `"}}`
	signature, err := mocks.SignWebhook("api-key", "webhook-secret", []byte(body))
	require.NoError(t, err)

	recorder := h.do(t, http.MethodPost, "/voice/livekit/webhooks", body, "", map[string]string{"Authorization": signature})
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	_, active := h.fixture.Presence.Active(communityID, channelID, userID)
	assert.True(t, active)

	// The duplicate delivery is accepted but does not double count.
	recorder = h.do(t, http.MethodPost, "/voice/livekit/webhooks", body, "", map[string]string{"Authorization": signature})
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, 1, h.fixture.Producer.Len())

	// An unsigned delivery is rejected and changes nothing.
	recorder = h.do(t, http.MethodPost, "/voice/livekit/webhooks", body, "", nil)
	expectError(t, recorder, http.StatusUnauthorized, "invalid_signature")

	tampered := `{"id":"evt-2","event":"participant_left","room":{"name":"` + service.RoomName(channelID) + `"}}`
	recorder = h.do(t, http.MethodPost, "/voice/livekit/webhooks", tampered, "", map[string]string{"Authorization": signature})
	expectError(t, recorder, http.StatusUnauthorized, "invalid_signature")

	_, stillActive := h.fixture.Presence.Active(communityID, channelID, userID)
	assert.True(t, stillActive, "a rejected webhook must not change state")
}

// TestWebhookEndpointMalformedPayload covers the 400 mapping.
func TestWebhookEndpointMalformedPayload(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	body := `{"event":"participant_joined"}`
	signature, err := mocks.SignWebhook("api-key", "webhook-secret", []byte(body))
	require.NoError(t, err)

	recorder := h.do(t, http.MethodPost, "/voice/livekit/webhooks", body, "", map[string]string{"Authorization": signature})
	expectError(t, recorder, http.StatusBadRequest, "invalid_request")
	assert.Equal(t, 0, h.fixture.Presence.Len())
}

// TestWebhookEndpointInternalFailure covers the 500 mapping.
func TestWebhookEndpointInternalFailure(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	h.fixture.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	h.fixture.Presence.UpsertErr = errors.New("database down")

	body := `{"id":"evt-3","event":"participant_joined","room":{"name":"` + service.RoomName(channelID) +
		`"},"participant":{"identity":"` + userID.String() + `"}}`
	signature, err := mocks.SignWebhook("api-key", "webhook-secret", []byte(body))
	require.NoError(t, err)

	recorder := h.do(t, http.MethodPost, "/voice/livekit/webhooks", body, "", map[string]string{"Authorization": signature})
	expectError(t, recorder, http.StatusInternalServerError, "internal_error")
}

// TestHealthAndReadinessEndpoints covers the probes: /healthz never depends on
// a dependency, /readyz does.
func TestHealthAndReadinessEndpoints(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	h.setProbe(nil)
	recorder := h.do(t, http.MethodGet, "/healthz", "", "", nil)
	require.Equal(t, http.StatusOK, recorder.Code)

	recorder = h.do(t, http.MethodGet, "/readyz", "", "", nil)
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "ready")

	// An unhealthy dependency fails readiness but never liveness.
	h.setProbe(errors.New("postgres down"))
	recorder = h.do(t, http.MethodGet, "/healthz", "", "", nil)
	require.Equal(t, http.StatusOK, recorder.Code, "liveness must not depend on PostgreSQL")
	recorder = h.do(t, http.MethodGet, "/readyz", "", "", nil)
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	body := decodeError(t, recorder)
	assert.Equal(t, "not_ready", body["error"])
	assert.Contains(t, body["detail"], "postgres")

	// Graceful shutdown drains the instance.
	h.setProbe(nil)
	h.readiness.BeginShutdown()
	recorder = h.do(t, http.MethodGet, "/healthz", "", "", nil)
	require.Equal(t, http.StatusOK, recorder.Code)
	recorder = h.do(t, http.MethodGet, "/readyz", "", "", nil)
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "shutting down")
}

// TestMetricsEndpoint covers the Prometheus exposition.
func TestMetricsEndpoint(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	h.fixture.Access.Result = &community.AccessCheckResult{
		Allowed: true,
		Channel: &models.ChannelSummary{ChannelID: channelID, Name: "General", Capacity: 4},
	}
	recorder := h.do(t, http.MethodPost, joinPath(channelID),
		`{"community_id":"`+communityID.String()+`"}`, h.token(t, userID), nil)
	require.Equal(t, http.StatusOK, recorder.Code)

	recorder = h.do(t, http.MethodGet, "/metrics", "", "", nil)
	require.Equal(t, http.StatusOK, recorder.Code)
	body := recorder.Body.String()
	assert.Contains(t, body, "voice_joins_total")
	assert.Contains(t, body, `result="joined"`)
	assert.Contains(t, body, "http_requests_total")
	assert.Contains(t, body, "http_request_duration_seconds")
	assert.Contains(t, body, "external_call_duration_seconds")
	assert.Contains(t, body, `provider="livekit"`)
}

// TestUnknownRouteReturnsErrorEnvelope covers the 404 shape.
func TestUnknownRouteReturnsErrorEnvelope(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	recorder := h.do(t, http.MethodGet, "/nope", "", "", nil)
	expectError(t, recorder, http.StatusNotFound, "not_found")
}

// TestTraceIDPropagation covers the edge rule: a caller-supplied trace id is
// reused, otherwise one is minted and echoed.
func TestTraceIDPropagation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	h.fixture.Access.Result = &community.AccessCheckResult{
		Allowed: true,
		Channel: &models.ChannelSummary{ChannelID: channelID, Name: "General", Capacity: 4},
	}
	upstream := "4bf92f3577b34da6a3ce929d0e0e4736"
	recorder := h.do(t, http.MethodPost, joinPath(channelID),
		`{"community_id":"`+communityID.String()+`"}`, h.token(t, userID),
		map[string]string{"traceparent": "00-" + upstream + "-00f067aa0ba902b7-01"})
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, upstream, recorder.Header().Get("X-Trace-Id"))

	request, ok := h.fixture.Access.LastRequest()
	require.True(t, ok)
	assert.Equal(t, upstream, request.TraceID, "the trace id reaches API-002")
}

// TestRouteTableIsStable keeps the documented paths from the OpenAPI spec.
func TestRouteTableIsStable(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	channelID, communityID := uuid.New(), uuid.New()
	userID := uuid.New()
	token := h.token(t, userID)

	paths := []struct {
		method string
		path   string
	}{
		{method: http.MethodPost, path: joinPath(channelID)},
		{method: http.MethodPost, path: leavePath(channelID)},
		{method: http.MethodGet, path: presencePath(communityID)},
		{method: http.MethodPost, path: "/voice/livekit/webhooks"},
		{method: http.MethodGet, path: "/healthz"},
		{method: http.MethodGet, path: "/readyz"},
		{method: http.MethodGet, path: "/metrics"},
	}
	for _, route := range paths {
		recorder := h.do(t, route.method, route.path, "", token, nil)
		// Every documented path is routed; domain 404s (unknown channel) are
		// fine, an unrouted path answers with the "not_found" envelope.
		assert.NotEqual(t, "not_found", routeErrorCode(t, recorder), "%s %s must be routed", route.method, route.path)
	}
	assert.Equal(t, config.DefaultPort, "8080")
}

// routeErrorCode reports the error code of a response body, if any.
func routeErrorCode(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		return ""
	}
	code, _ := body["error"].(string)
	return code
}
