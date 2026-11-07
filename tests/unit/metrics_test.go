package unit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"voice_service/internal/livekit"
	"voice_service/internal/service"
	"voice_service/tests/mocks"
)

// TestMetricsRegisterTheDocumentedCollectors covers docs/architecture.md §6.
func TestMetricsRegisterTheDocumentedCollectors(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics := service.NewMetrics(registry)
	require.NotNil(t, metrics)

	metrics.RecordJoin(service.JoinResultJoined)
	metrics.RecordJoin(service.JoinResultFull)
	metrics.RecordJoin(service.JoinResultForbidden)
	metrics.RecordParticipantMinutes(2*time.Minute, true, true)
	metrics.SetActivePresence(3, 2)
	metrics.RecordModeration(service.ModerationActionRemoved)
	metrics.RecordModeration(service.ModerationActionPublishRevoked)
	metrics.ObserveHTTP("/voice/channels/:channel_id/join", "POST", 200, 25*time.Millisecond)
	metrics.ObserveHTTP("/voice/channels/:channel_id/join", "POST", 503, 2*time.Second)
	metrics.SetKafkaLag("community_events", "voice_service", 12)
	metrics.ObserveExternal(service.OperationCreateRoom, 40*time.Millisecond, nil)
	metrics.ObserveExternal(service.OperationListParticipants, 40*time.Millisecond, livekit.ErrRoomNotFound)
	metrics.ObserveExternal(service.OperationRemoveParticipant, 40*time.Millisecond, context.DeadlineExceeded)
	metrics.ObserveExternal(service.OperationDeleteRoom, 40*time.Millisecond, errors.New("boom"))

	for _, name := range []string{
		"voice_joins_total",
		"voice_participant_minutes_total",
		"voice_active_participants",
		"voice_active_rooms",
		"voice_moderation_removals_total",
		"http_requests_total",
		"http_request_duration_seconds",
		"kafka_consumer_lag",
		"external_call_duration_seconds",
		"external_call_errors_total",
	} {
		assert.True(t, metricFamily(t, registry, name), "%s must be registered", name)
	}

	assert.Equal(t, float64(1), metricValue(t, registry, "voice_joins_total", map[string]string{"result": "joined"}))
	assert.Equal(t, float64(2), metricValue(t, registry, "voice_participant_minutes_total", map[string]string{"media": "audio"}))
	assert.Equal(t, float64(2), metricValue(t, registry, "voice_participant_minutes_total", map[string]string{"media": "video"}))
	assert.Equal(t, float64(3), metricValue(t, registry, "voice_active_participants", nil))
	assert.Equal(t, float64(2), metricValue(t, registry, "voice_active_rooms", nil))
	assert.Equal(t, float64(1), metricValue(t, registry, "voice_moderation_removals_total", map[string]string{"action": "removed"}))
	assert.Equal(t, float64(1), metricValue(t, registry, "http_requests_total", map[string]string{
		"route": "/voice/channels/:channel_id/join", "method": "POST", "status_class": "2xx",
	}))
	assert.Equal(t, float64(1), metricValue(t, registry, "http_requests_total", map[string]string{
		"route": "/voice/channels/:channel_id/join", "method": "POST", "status_class": "5xx",
	}))
	assert.Equal(t, float64(12), metricValue(t, registry, "kafka_consumer_lag", map[string]string{
		"topic": "community_events", "consumer_group": "voice_service",
	}))
	assert.Equal(t, float64(1), metricValue(t, registry, "external_call_errors_total", map[string]string{
		"provider": "livekit", "operation": service.OperationListParticipants, "error_class": "not_found",
	}))
	assert.Equal(t, float64(1), metricValue(t, registry, "external_call_errors_total", map[string]string{
		"provider": "livekit", "operation": service.OperationRemoveParticipant, "error_class": "timeout",
	}))
	assert.Equal(t, float64(1), metricValue(t, registry, "external_call_errors_total", map[string]string{
		"provider": "livekit", "operation": service.OperationDeleteRoom, "error_class": "unavailable",
	}))
}

// TestMetricsTolerateDuplicateRegistration covers a shared registry.
func TestMetricsTolerateDuplicateRegistration(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	first := service.NewMetrics(registry)
	second := service.NewMetrics(registry)
	require.NotNil(t, first)
	require.NotNil(t, second)
}

// TestMetricsAreNilSafe keeps partially wired processes working.
func TestMetricsAreNilSafe(t *testing.T) {
	t.Parallel()
	var metrics *service.Metrics
	metrics.RecordJoin(service.JoinResultJoined)
	metrics.RecordParticipantMinutes(time.Minute, true, true)
	metrics.SetActivePresence(1, 1)
	metrics.RecordModeration(service.ModerationActionRemoved)
	metrics.ObserveHTTP("/", "GET", 200, time.Millisecond)
	metrics.SetKafkaLag("topic", "group", 1)
	metrics.ObserveExternal(service.OperationCreateRoom, time.Millisecond, nil)
}

// TestMetricsIgnoreZeroLengthSessions keeps the minutes counter honest.
func TestMetricsIgnoreZeroLengthSessions(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics := service.NewMetrics(registry)
	metrics.RecordParticipantMinutes(0, true, true)
	assert.Zero(t, metricValue(t, registry, "voice_participant_minutes_total", map[string]string{"media": "audio"}))
}

// TestInstrumentedRoomClientRecordsEveryOperation covers the external_call_*
// metrics of the LiveKit adapter wrapper.
func TestInstrumentedRoomClientRecordsEveryOperation(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	client := service.NewInstrumentedRoomClient(f.LiveKit, f.Metrics)
	require.NotNil(t, client)

	ctx := context.Background()
	require.NoError(t, client.CreateRoom(ctx, livekit.CreateRoomRequest{RoomName: "voice-1", MaxParticipants: 2}))
	require.NoError(t, client.DeleteRoom(ctx, "voice-1"))
	require.NoError(t, client.RemoveParticipant(ctx, "voice-1", "user"))
	require.NoError(t, client.UpdateParticipantPermissions(ctx, "voice-1", "user", false))
	_, err := client.ListParticipants(ctx, "voice-1")
	require.ErrorIs(t, err, livekit.ErrRoomNotFound)

	for _, operation := range []string{
		service.OperationCreateRoom,
		service.OperationDeleteRoom,
		service.OperationRemoveParticipant,
		service.OperationUpdatePermissions,
		service.OperationListParticipants,
	} {
		assert.True(t, metricFamily(t, f.Registry, "external_call_duration_seconds"))
		assert.GreaterOrEqual(t, metricValue(t, f.Registry, "external_call_duration_seconds", map[string]string{
			"provider": "livekit", "operation": operation,
		}), float64(1), "%s must be observed", operation)
	}
	assert.Equal(t, float64(1), metricValue(t, f.Registry, "external_call_errors_total", map[string]string{
		"provider": "livekit", "operation": service.OperationListParticipants, "error_class": "not_found",
	}))
}

// TestInstrumentedRoomClientWithoutInnerClient covers the nil guard.
func TestInstrumentedRoomClientWithoutInnerClient(t *testing.T) {
	t.Parallel()
	assert.Nil(t, service.NewInstrumentedRoomClient(nil, nil))
}

// TestRetryPolicyStopsOnPermanentFailures covers the back-off helper.
func TestRetryPolicyStopsOnPermanentFailures(t *testing.T) {
	t.Parallel()
	policy := service.RetryPolicy{Attempts: 5, Base: time.Millisecond}
	calls := 0
	err := policy.Do(context.Background(), nil, "remove_participant", func(context.Context) error {
		calls++
		return livekit.ErrParticipantNotFound
	})
	require.ErrorIs(t, err, livekit.ErrParticipantNotFound)
	assert.Equal(t, 1, calls, "a not-found answer is final")

	transient := service.RetryPolicy{Attempts: 3, Base: time.Millisecond}
	calls = 0
	err = transient.Do(context.Background(), nil, "remove_participant", func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, calls)

	exhausted := service.RetryPolicy{Attempts: 2, Base: time.Millisecond}
	calls = 0
	err = exhausted.Do(context.Background(), nil, "remove_participant", func(context.Context) error {
		calls++
		return errors.New("still broken")
	})
	require.Error(t, err)
	assert.Equal(t, 2, calls)
}

// TestRetryPolicyHonorsContextCancellation covers the shutdown path.
func TestRetryPolicyHonorsContextCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	policy := service.RetryPolicy{Attempts: 3, Base: time.Second}
	err := policy.Do(ctx, nil, "remove_participant", func(context.Context) error {
		return errors.New("transient")
	})
	require.ErrorIs(t, err, context.Canceled)
}
