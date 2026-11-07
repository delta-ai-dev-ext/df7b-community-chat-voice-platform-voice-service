package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// Topic and contract names owned by voice_service.
const (
	// ProducerName is the value of envelope.producer for this service.
	ProducerName = "voice_service"
	// TopicVoiceEvents is the EVT-010 topic.
	TopicVoiceEvents = "voice_events"
	// TopicCommunityEvents is the EVT-002/EVT-003 topic consumed here.
	TopicCommunityEvents = "community_events"
	// EventTypeVoicePresenceChanged is EVT-010.
	EventTypeVoicePresenceChanged = "voice_presence_changed"
	// EventTypeCommunityMemberChanged is EVT-002.
	EventTypeCommunityMemberChanged = "community_member_changed"
	// EventTypeCommunityChannelChanged is EVT-003.
	EventTypeCommunityChannelChanged = "community_channel_changed"
	// SchemaRefPrefix prefixes envelope.schema_ref values.
	SchemaRefPrefix = "EVENT_CONTRACTS.md#"
)

// Event versions produced and consumed by this service.
const (
	// DefaultEventVersion is the version of every contract used here.
	DefaultEventVersion = 1
)

var (
	eventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	schemaRefPattern = regexp.MustCompile(`^EVENT_CONTRACTS\.md#[a-z][a-z0-9_]*-v[0-9]+$`)
)

// Envelope is the single reusable event envelope shared by every produced and
// consumed message (docs/architecture.md "Event envelope").
type Envelope struct {
	// EventID uniquely identifies the event and is the consumer dedupe key.
	EventID string `json:"event_id"`
	// EventType is the snake_case contract name.
	EventType string `json:"event_type"`
	// EventVersion is the contract version (>= 1).
	EventVersion int `json:"event_version"`
	// OccurredAt is the producer clock at emit time (RFC3339, UTC).
	OccurredAt time.Time `json:"occurred_at"`
	// Producer is the producing service name.
	Producer string `json:"producer"`
	// TraceID is propagated or minted at the flow edge; never empty.
	TraceID string `json:"trace_id"`
	// CorrelationID groups every event of one business flow.
	CorrelationID string `json:"correlation_id"`
	// CausationID is the event id of the causing event, nil for a flow start.
	CausationID *string `json:"causation_id"`
	// SchemaRef points at the contract definition.
	SchemaRef string `json:"schema_ref"`
}

// Message is one wire message: exactly the two top-level keys "envelope" and
// "payload".
type Message struct {
	// Envelope is the mandatory envelope.
	Envelope Envelope `json:"envelope"`
	// Payload is the contract-specific body.
	Payload any `json:"payload"`
	// Key is the Kafka partition key (the channel or community id). It is
	// never serialized into the JSON body.
	Key string `json:"-"`
}

// Marshal renders the {"envelope","payload"} JSON body.
func (m Message) Marshal() ([]byte, error) {
	if err := m.Envelope.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(struct {
		Envelope Envelope `json:"envelope"`
		Payload  any      `json:"payload"`
	}{Envelope: m.Envelope, Payload: m.Payload})
	if err != nil {
		return nil, fmt.Errorf("marshal event: %w", err)
	}
	return data, nil
}

// WireMessage is a decoded incoming message whose payload is still raw.
type WireMessage struct {
	// Envelope is the validated envelope.
	Envelope Envelope `json:"envelope"`
	// Payload is the raw payload for contract-specific decoding.
	Payload json.RawMessage `json:"payload"`
}

// ParseWireMessage decodes and validates an incoming {"envelope","payload"}
// message. A missing or malformed trace id is replaced so downstream emissions
// always carry a non-empty trace id.
func ParseWireMessage(data []byte) (*WireMessage, error) {
	if len(data) == 0 {
		return nil, errors.New("empty message")
	}
	var wire WireMessage
	if err := json.Unmarshal(data, &wire); err != nil {
		return nil, fmt.Errorf("decode event: %w", err)
	}
	// A producer that violated the envelope contract must not stall the
	// consumer: the trace id is repaired before the envelope is validated.
	if !ValidTraceID(wire.Envelope.TraceID) {
		wire.Envelope.TraceID = NewTraceID()
	}
	if err := wire.Envelope.Validate(); err != nil {
		return nil, err
	}
	return &wire, nil
}

// DecodePayload unmarshals the raw payload into target.
func (w *WireMessage) DecodePayload(target any) error {
	if len(w.Payload) == 0 {
		return errors.New("missing payload")
	}
	if err := json.Unmarshal(w.Payload, target); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}
	return nil
}

// EnvelopeOptions describes one envelope to build.
type EnvelopeOptions struct {
	// EventType is the snake_case contract name.
	EventType string
	// EventVersion defaults to DefaultEventVersion when zero.
	EventVersion int
	// TraceID is propagated from the originating flow; minted when empty.
	TraceID string
	// CorrelationID groups the flow; minted when empty.
	CorrelationID string
	// CausationID is the causing event id, if any.
	CausationID *string
	// OccurredAt defaults to the current time in UTC when zero.
	OccurredAt time.Time
}

// NewEnvelope builds a valid envelope: it mints the event id, the trace id
// (never empty), and the correlation id, and derives the schema reference.
func NewEnvelope(opts EnvelopeOptions) (Envelope, error) {
	version := opts.EventVersion
	if version == 0 {
		version = DefaultEventVersion
	}
	traceID := opts.TraceID
	if !ValidTraceID(traceID) {
		traceID = NewTraceID()
	}
	correlationID := opts.CorrelationID
	if _, err := uuid.Parse(correlationID); err != nil {
		correlationID = uuid.NewString()
	}
	occurredAt := opts.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	env := Envelope{
		EventID:       uuid.NewString(),
		EventType:     opts.EventType,
		EventVersion:  version,
		OccurredAt:    occurredAt.UTC(),
		Producer:      ProducerName,
		TraceID:       traceID,
		CorrelationID: correlationID,
		CausationID:   opts.CausationID,
		SchemaRef:     fmt.Sprintf("%s%s-v%d", SchemaRefPrefix, opts.EventType, version),
	}
	if err := env.Validate(); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// Validate checks the envelope against the platform contract.
func (e Envelope) Validate() error {
	if _, err := uuid.Parse(e.EventID); err != nil {
		return fmt.Errorf("event_id %q is not a uuid", e.EventID)
	}
	if !eventTypePattern.MatchString(e.EventType) {
		return fmt.Errorf("event_type %q is not snake_case", e.EventType)
	}
	if e.EventVersion < 1 {
		return fmt.Errorf("event_version %d must be >= 1", e.EventVersion)
	}
	if e.OccurredAt.IsZero() {
		return errors.New("occurred_at is required")
	}
	if e.Producer == "" || !eventTypePattern.MatchString(e.Producer) {
		return fmt.Errorf("producer %q is not snake_case", e.Producer)
	}
	if err := ValidateTraceID(e.TraceID); err != nil {
		return err
	}
	if _, err := uuid.Parse(e.CorrelationID); err != nil {
		return fmt.Errorf("correlation_id %q is not a uuid", e.CorrelationID)
	}
	if e.CausationID != nil {
		if _, err := uuid.Parse(*e.CausationID); err != nil {
			return fmt.Errorf("causation_id %q is not a uuid", *e.CausationID)
		}
	}
	if !schemaRefPattern.MatchString(e.SchemaRef) {
		return fmt.Errorf("schema_ref %q is not a contract reference", e.SchemaRef)
	}
	return nil
}
