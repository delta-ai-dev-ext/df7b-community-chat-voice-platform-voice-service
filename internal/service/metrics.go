package service

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"voice_service/internal/livekit"
)

// Metric label values.
const (
	// JoinResultJoined marks a successful join.
	JoinResultJoined = "joined"
	// JoinResultFull marks a join refused because the channel is full.
	JoinResultFull = "full"
	// JoinResultForbidden marks a join refused by the access check.
	JoinResultForbidden = "forbidden"

	// MediaAudio labels audio participant minutes.
	MediaAudio = "audio"
	// MediaVideo labels camera participant minutes.
	MediaVideo = "video"
	// MediaScreen labels screen-share participant minutes.
	MediaScreen = "screen"

	// ModerationActionRemoved marks a removal from a live room.
	ModerationActionRemoved = "removed"
	// ModerationActionPublishRevoked marks an in-place publish revocation.
	ModerationActionPublishRevoked = "publish_revoked"

	// ProviderLiveKit is the provider label of LiveKit external calls.
	ProviderLiveKit = "livekit"

	// OperationCreateRoom labels room creation calls.
	OperationCreateRoom = "create_room"
	// OperationDeleteRoom labels room deletion calls.
	OperationDeleteRoom = "delete_room"
	// OperationRemoveParticipant labels participant removal calls.
	OperationRemoveParticipant = "remove_participant"
	// OperationUpdatePermissions labels permission update calls.
	OperationUpdatePermissions = "update_participant_permissions"
	// OperationListParticipants labels participant listing calls.
	OperationListParticipants = "list_participants"
)

// Metrics holds every Prometheus collector of voice_service
// (docs/architecture.md §6). All labels are low-cardinality.
type Metrics struct {
	// Joins counts join attempts by result.
	Joins *prometheus.CounterVec
	// ParticipantMinutes counts media time by media kind.
	ParticipantMinutes *prometheus.CounterVec
	// ActiveParticipants gauges the participants across all rooms.
	ActiveParticipants prometheus.Gauge
	// ActiveRooms gauges the rooms with at least one participant.
	ActiveRooms prometheus.Gauge
	// ModerationRemovals counts moderation reactions by action.
	ModerationRemovals *prometheus.CounterVec
	// HTTPRequests counts HTTP requests (RED).
	HTTPRequests *prometheus.CounterVec
	// HTTPDuration observes HTTP request latency (RED).
	HTTPDuration *prometheus.HistogramVec
	// KafkaLag gauges the community_events consumer lag.
	KafkaLag *prometheus.GaugeVec
	// ExternalDuration observes LiveKit call latency.
	ExternalDuration *prometheus.HistogramVec
	// ExternalErrors counts LiveKit call failures by error class.
	ExternalErrors *prometheus.CounterVec
}

// NewMetrics registers every collector on reg. Registering twice is tolerated
// so multiple instances can share a registry.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	m := &Metrics{
		Joins: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "voice_joins_total",
			Help: "Voice channel join attempts by result.",
		}, []string{"result"}),
		ParticipantMinutes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "voice_participant_minutes_total",
			Help: "Participant minutes in voice channels by media kind.",
		}, []string{"media"}),
		ActiveParticipants: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voice_active_participants",
			Help: "Participants currently present in voice channels.",
		}),
		ActiveRooms: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "voice_active_rooms",
			Help: "Voice channels with at least one participant.",
		}),
		ModerationRemovals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "voice_moderation_removals_total",
			Help: "Moderation reactions applied to live voice rooms by action.",
		}, []string{"action"}),
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests by route, method, and status class.",
		}, []string{"route", "method", "status_class"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency by route and method.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.3, 0.5, 1, 2.5, 5},
		}, []string{"route", "method"}),
		KafkaLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_consumer_lag",
			Help: "Consumer lag of the community_events consumer group.",
		}, []string{"topic", "consumer_group"}),
		ExternalDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "external_call_duration_seconds",
			Help:    "Latency of external provider calls by provider and operation.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"provider", "operation"}),
		ExternalErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "external_call_errors_total",
			Help: "External provider call failures by provider, operation, and error class.",
		}, []string{"provider", "operation", "error_class"}),
	}
	for _, collector := range []prometheus.Collector{
		m.Joins, m.ParticipantMinutes, m.ActiveParticipants, m.ActiveRooms, m.ModerationRemovals,
		m.HTTPRequests, m.HTTPDuration, m.KafkaLag, m.ExternalDuration, m.ExternalErrors,
	} {
		if err := reg.Register(collector); err != nil {
			var already prometheus.AlreadyRegisteredError
			if !errors.As(err, &already) {
				panic(err)
			}
		}
	}
	return m
}

// RecordJoin counts one join attempt.
func (m *Metrics) RecordJoin(result string) {
	if m == nil {
		return
	}
	m.Joins.WithLabelValues(result).Inc()
}

// RecordParticipantMinutes adds the minutes of one finished presence session,
// split into audio, camera, and screen-share minutes.
func (m *Metrics) RecordParticipantMinutes(d time.Duration, camera, screen bool) {
	if m == nil || d <= 0 {
		return
	}
	minutes := d.Minutes()
	m.ParticipantMinutes.WithLabelValues(MediaAudio).Add(minutes)
	if camera {
		m.ParticipantMinutes.WithLabelValues(MediaVideo).Add(minutes)
	}
	if screen {
		m.ParticipantMinutes.WithLabelValues(MediaScreen).Add(minutes)
	}
}

// RecordModeration counts one moderation reaction.
func (m *Metrics) RecordModeration(action string) {
	if m == nil {
		return
	}
	m.ModerationRemovals.WithLabelValues(action).Inc()
}

// SetActivePresence publishes the current presence gauges.
func (m *Metrics) SetActivePresence(participants, rooms int) {
	if m == nil {
		return
	}
	m.ActiveParticipants.Set(float64(participants))
	m.ActiveRooms.Set(float64(rooms))
}

// ObserveHTTP records one HTTP request (RED metrics).
func (m *Metrics) ObserveHTTP(route, method string, status int, d time.Duration) {
	if m == nil {
		return
	}
	class := statusClass(status)
	m.HTTPRequests.WithLabelValues(route, method, class).Inc()
	m.HTTPDuration.WithLabelValues(route, method).Observe(d.Seconds())
}

// SetKafkaLag publishes the observed consumer lag.
func (m *Metrics) SetKafkaLag(topic, consumerGroup string, lag int64) {
	if m == nil {
		return
	}
	m.KafkaLag.WithLabelValues(topic, consumerGroup).Set(float64(lag))
}

// ObserveExternal records one LiveKit call and its outcome.
func (m *Metrics) ObserveExternal(operation string, d time.Duration, err error) {
	if m == nil {
		return
	}
	m.ExternalDuration.WithLabelValues(ProviderLiveKit, operation).Observe(d.Seconds())
	if err != nil {
		m.ExternalErrors.WithLabelValues(ProviderLiveKit, operation, errorClass(err)).Inc()
	}
}

// statusClass maps an HTTP status onto its status class label.
func statusClass(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	case status >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}

// errorClass classifies a LiveKit failure for the external_call_errors_total
// metric.
func errorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, livekit.ErrRoomNotFound), errors.Is(err, livekit.ErrParticipantNotFound):
		return "not_found"
	default:
		return "unavailable"
	}
}
