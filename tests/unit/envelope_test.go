package unit

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"voice_service/internal/events"
)

// TestNewEnvelopeMintsRequiredIdentifiers covers the envelope contract: the
// event id, trace id, and correlation id are always present.
func TestNewEnvelopeMintsRequiredIdentifiers(t *testing.T) {
	t.Parallel()
	occurredAt := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	envelope, err := events.NewEnvelope(events.EnvelopeOptions{
		EventType:  events.EventTypeVoicePresenceChanged,
		OccurredAt: occurredAt,
	})
	require.NoError(t, err)

	assert.NotEqual(t, uuid.Nil.String(), envelope.EventID)
	_, err = uuid.Parse(envelope.EventID)
	require.NoError(t, err)
	assert.Equal(t, events.ProducerName, envelope.Producer)
	assert.Equal(t, events.DefaultEventVersion, envelope.EventVersion)
	assert.Equal(t, "EVENT_CONTRACTS.md#voice_presence_changed-v1", envelope.SchemaRef)
	assert.True(t, events.ValidTraceID(envelope.TraceID), "trace_id must be minted and non-empty")
	_, err = uuid.Parse(envelope.CorrelationID)
	require.NoError(t, err)
	assert.Nil(t, envelope.CausationID)
	assert.Equal(t, occurredAt, envelope.OccurredAt)
	assert.Equal(t, time.UTC, envelope.OccurredAt.Location())
}

// TestNewEnvelopePropagatesTraceAndCausation covers the "caused by" rules.
func TestNewEnvelopePropagatesTraceAndCausation(t *testing.T) {
	t.Parallel()
	cause, err := events.NewEnvelope(events.EnvelopeOptions{EventType: events.EventTypeCommunityMemberChanged})
	require.NoError(t, err)

	causeID := cause.EventID
	envelope, err := events.NewEnvelope(events.EnvelopeOptions{
		EventType:     events.EventTypeVoicePresenceChanged,
		TraceID:       cause.TraceID,
		CorrelationID: cause.CorrelationID,
		CausationID:   &causeID,
	})
	require.NoError(t, err)
	assert.Equal(t, cause.TraceID, envelope.TraceID)
	assert.Equal(t, cause.CorrelationID, envelope.CorrelationID)
	require.NotNil(t, envelope.CausationID)
	assert.Equal(t, cause.EventID, *envelope.CausationID)
	assert.NotEqual(t, cause.EventID, envelope.EventID)
}

// TestPresenceMessageWireFormat covers EVT-010: exactly the two top-level keys,
// keyed by channel id, and a payload without envelope field names.
func TestPresenceMessageWireFormat(t *testing.T) {
	t.Parallel()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	message, err := events.NewPresenceMessage(events.PresenceEvent{
		CommunityID:        communityID,
		ChannelID:          channelID,
		UserID:             userID,
		Change:             events.ChangeJoined,
		CameraEnabled:      true,
		ScreenShareEnabled: false,
		PublishingAllowed:  true,
		ParticipantCount:   3,
		Capacity:           5,
	})
	require.NoError(t, err)
	assert.Equal(t, channelID.String(), message.Key)

	encoded, err := message.Marshal()
	require.NoError(t, err)
	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Len(t, decoded, 2)
	assert.Contains(t, decoded, "envelope")
	assert.Contains(t, decoded, "payload")

	var payload map[string]any
	require.NoError(t, json.Unmarshal(decoded["payload"], &payload))
	for _, envelopeField := range []string{"event_id", "event_type", "trace_id", "correlation_id", "causation_id", "schema_ref"} {
		assert.NotContains(t, payload, envelopeField, "the payload must not repeat envelope field names")
	}
	assert.Equal(t, "joined", payload["change"])
	assert.Equal(t, userID.String(), payload["user_id"])
	assert.Equal(t, float64(3), payload["participant_count"])
	assert.Equal(t, float64(5), payload["capacity"])

	var envelope map[string]any
	require.NoError(t, json.Unmarshal(decoded["envelope"], &envelope))
	assert.Len(t, envelope, 9, "the envelope carries exactly the nine contract fields")
	assert.NotEmpty(t, envelope["trace_id"])
}

// TestPresenceMessageValidation covers the payload guards.
func TestPresenceMessageValidation(t *testing.T) {
	t.Parallel()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()

	_, err := events.NewPresenceMessage(events.PresenceEvent{
		CommunityID: communityID, ChannelID: channelID, UserID: userID,
		Change: "shouted", Capacity: 5,
	})
	require.Error(t, err)

	_, err = events.NewPresenceMessage(events.PresenceEvent{
		CommunityID: communityID, ChannelID: channelID, UserID: userID,
		Change: events.ChangeJoined, Capacity: 42,
	})
	require.Error(t, err, "a capacity outside 1..10 is rejected")

	_, err = events.NewPresenceMessage(events.PresenceEvent{
		Change: events.ChangeJoined, Capacity: 5,
	})
	require.Error(t, err, "the identifiers are required")

	message, err := events.NewPresenceMessage(events.PresenceEvent{
		CommunityID: communityID, ChannelID: channelID, UserID: userID,
		Change: events.ChangeLeft, Capacity: 5, ParticipantCount: 99,
	})
	require.NoError(t, err)
	payload, ok := message.Payload.(events.PresencePayload)
	require.True(t, ok)
	assert.Equal(t, 10, payload.ParticipantCount, "the count is clamped into the contract range")
}

// TestPresenceMessageFromCause copies trace and correlation ids of the causing
// event (moderation removal flows).
func TestPresenceMessageFromCause(t *testing.T) {
	t.Parallel()
	cause, err := events.NewEnvelope(events.EnvelopeOptions{EventType: events.EventTypeCommunityMemberChanged})
	require.NoError(t, err)
	message, err := events.NewPresenceMessage(events.PresenceEvent{
		CommunityID: uuid.New(),
		ChannelID:   uuid.New(),
		UserID:      uuid.New(),
		Change:      events.ChangeLeft,
		Capacity:    4,
		Cause:       &cause,
	})
	require.NoError(t, err)
	assert.Equal(t, cause.TraceID, message.Envelope.TraceID)
	assert.Equal(t, cause.CorrelationID, message.Envelope.CorrelationID)
	require.NotNil(t, message.Envelope.CausationID)
	assert.Equal(t, cause.EventID, *message.Envelope.CausationID)
}

// TestEnvelopeValidationRejectsBadValues covers the contract guards.
func TestEnvelopeValidationRejectsBadValues(t *testing.T) {
	t.Parallel()
	valid, err := events.NewEnvelope(events.EnvelopeOptions{EventType: events.EventTypeVoicePresenceChanged})
	require.NoError(t, err)
	require.NoError(t, valid.Validate())

	cases := map[string]func(*events.Envelope){
		"event id":       func(e *events.Envelope) { e.EventID = "not-a-uuid" },
		"event type":     func(e *events.Envelope) { e.EventType = "VoicePresenceChanged" },
		"event version":  func(e *events.Envelope) { e.EventVersion = 0 },
		"occurred at":    func(e *events.Envelope) { e.OccurredAt = time.Time{} },
		"producer":       func(e *events.Envelope) { e.Producer = "" },
		"trace id":       func(e *events.Envelope) { e.TraceID = "" },
		"correlation id": func(e *events.Envelope) { e.CorrelationID = "" },
		"causation id":   func(e *events.Envelope) { bad := "nope"; e.CausationID = &bad },
		"schema ref":     func(e *events.Envelope) { e.SchemaRef = "voice_presence_changed" },
	}
	for name, mutate := range cases {
		mutate := mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			envelope := valid
			mutate(&envelope)
			require.Error(t, envelope.Validate())
		})
	}
}

// TestParseWireMessage covers decoding of consumed events.
func TestParseWireMessage(t *testing.T) {
	t.Parallel()
	envelope, err := events.NewEnvelope(events.EnvelopeOptions{EventType: events.EventTypeCommunityChannelChanged})
	require.NoError(t, err)
	encoded, err := json.Marshal(map[string]any{
		"envelope": envelope,
		"payload":  map[string]any{"channel_id": uuid.New().String()},
	})
	require.NoError(t, err)

	wire, err := events.ParseWireMessage(encoded)
	require.NoError(t, err)
	assert.Equal(t, envelope.EventID, wire.Envelope.EventID)
	var payload map[string]string
	require.NoError(t, wire.DecodePayload(&payload))
	assert.NotEmpty(t, payload["channel_id"])

	_, err = events.ParseWireMessage(nil)
	require.Error(t, err)
	_, err = events.ParseWireMessage([]byte("{"))
	require.Error(t, err)

	badEnvelope, err := json.Marshal(map[string]any{
		"envelope": map[string]any{"event_id": "nope"},
		"payload":  map[string]any{},
	})
	require.NoError(t, err)
	_, err = events.ParseWireMessage(badEnvelope)
	require.Error(t, err)

	emptyPayload := &events.WireMessage{Envelope: envelope}
	require.Error(t, emptyPayload.DecodePayload(&payload), "a missing payload is an error")
}

// TestParseWireMessageMintsMissingTraceID keeps EVT-010 emissions valid even
// when a producer violated the contract.
func TestParseWireMessageMintsMissingTraceID(t *testing.T) {
	t.Parallel()
	envelope, err := events.NewEnvelope(events.EnvelopeOptions{EventType: events.EventTypeCommunityMemberChanged})
	require.NoError(t, err)
	envelope.TraceID = ""
	encoded, err := json.Marshal(map[string]any{"envelope": envelope, "payload": map[string]any{}})
	require.NoError(t, err)

	wire, err := events.ParseWireMessage(encoded)
	require.NoError(t, err)
	assert.True(t, events.ValidTraceID(wire.Envelope.TraceID))
}

// TestTraceIDHelpers covers trace header propagation and minting.
func TestTraceIDHelpers(t *testing.T) {
	t.Parallel()
	minted := events.NewTraceID()
	assert.Len(t, minted, 32)
	assert.True(t, events.ValidTraceID(minted))
	require.NoError(t, events.ValidateTraceID(minted))
	require.Error(t, events.ValidateTraceID("short"))

	upstream := "4bf92f3577b34da6a3ce929d0e0e4736"
	trace, ok := events.TraceIDFromTraceparent("00-" + upstream + "-00f067aa0ba902b7-01")
	require.True(t, ok)
	assert.Equal(t, upstream, trace)

	_, ok = events.TraceIDFromTraceparent("garbage")
	assert.False(t, ok)
	_, ok = events.TraceIDFromTraceparent("00-" + "00000000000000000000000000000000" + "-00f067aa0ba902b7-01")
	assert.False(t, ok, "the all-zero trace id is invalid")

	header := http.Header{}
	header.Set(events.TraceparentHeader, "00-"+upstream+"-00f067aa0ba902b7-01")
	header.Set(events.TraceIDHeader, minted)
	assert.Equal(t, upstream, events.TraceIDFromHeader(header), "traceparent wins over X-Trace-Id")

	onlyHeader := http.Header{}
	onlyHeader.Set(events.TraceIDHeader, minted)
	assert.Equal(t, minted, events.TraceIDFromHeader(onlyHeader))

	assert.True(t, events.ValidTraceID(events.TraceIDFromHeader(nil)), "a trace id is minted when nothing is propagated")

	parent := events.Traceparent(minted)
	assert.Contains(t, parent, minted)
	assert.Contains(t, parent, "00-")
}
