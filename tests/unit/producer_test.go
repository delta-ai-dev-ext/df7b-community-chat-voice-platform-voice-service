package unit

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"voice_service/internal/events"
)

// recordingProducer stands in for the sarama sync producer.
type recordingProducer struct {
	messages []*sarama.ProducerMessage
	failWith error
	closed   bool
}

// SendMessage records one produced message.
func (p *recordingProducer) SendMessage(message *sarama.ProducerMessage) (int32, int64, error) {
	if p.failWith != nil {
		return 0, 0, p.failWith
	}
	p.messages = append(p.messages, message)
	return 0, int64(len(p.messages)), nil
}

// Close records the shutdown.
func (p *recordingProducer) Close() error {
	p.closed = true
	return nil
}

// TestKafkaProducerPublishesEnvelopeAndHeaders covers item "EVT-010 envelope
// and producer": the wire body is {"envelope","payload"} and the trace id and
// event type are mirrored into the message headers.
func TestKafkaProducerPublishesEnvelopeAndHeaders(t *testing.T) {
	t.Parallel()
	recorder := &recordingProducer{}
	producer := events.NewKafkaProducerWithClient(recorder, events.TopicVoiceEvents, nil)

	channelID := uuidMustParse(t, "11111111-1111-1111-1111-111111111111")
	message, err := events.NewPresenceMessage(events.PresenceEvent{
		CommunityID:      uuidMustParse(t, "22222222-2222-2222-2222-222222222222"),
		ChannelID:        channelID,
		UserID:           uuidMustParse(t, "33333333-3333-3333-3333-333333333333"),
		Change:           events.ChangeJoined,
		ParticipantCount: 1,
		Capacity:         4,
	})
	require.NoError(t, err)
	require.NoError(t, producer.Publish(context.Background(), message))

	require.Len(t, recorder.messages, 1)
	published := recorder.messages[0]
	assert.Equal(t, events.TopicVoiceEvents, published.Topic)
	key, err := published.Key.Encode()
	require.NoError(t, err)
	assert.Equal(t, channelID.String(), string(key), "messages are keyed by channel id")

	headers := make(map[string]string, len(published.Headers))
	for _, header := range published.Headers {
		headers[string(header.Key)] = string(header.Value)
	}
	assert.Equal(t, message.Envelope.TraceID, headers["trace_id"])
	assert.Equal(t, events.EventTypeVoicePresenceChanged, headers["event_type"])

	value, err := published.Value.Encode()
	require.NoError(t, err)
	var body map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(value, &body))
	assert.Len(t, body, 2)
	assert.Contains(t, body, "envelope")
	assert.Contains(t, body, "payload")

	wire, err := events.ParseWireMessage(value)
	require.NoError(t, err)
	assert.Equal(t, events.EventTypeVoicePresenceChanged, wire.Envelope.EventType)
	assert.Equal(t, events.TopicVoiceEvents, events.TopicVoiceEvents)
}

// TestKafkaProducerReportsFailures covers the publish error path.
func TestKafkaProducerReportsFailures(t *testing.T) {
	t.Parallel()
	recorder := &recordingProducer{failWith: errors.New("broker unavailable")}
	producer := events.NewKafkaProducerWithClient(recorder, events.TopicVoiceEvents, nil)
	message, err := events.NewPresenceMessage(events.PresenceEvent{
		CommunityID: uuidMustParse(t, "22222222-2222-2222-2222-222222222222"),
		ChannelID:   uuidMustParse(t, "11111111-1111-1111-1111-111111111111"),
		UserID:      uuidMustParse(t, "33333333-3333-3333-3333-333333333333"),
		Change:      events.ChangeLeft,
		Capacity:    4,
	})
	require.NoError(t, err)
	require.Error(t, producer.Publish(context.Background(), message))

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, producer.Publish(cancelled, message), context.Canceled)

	require.NoError(t, producer.Close())
	assert.True(t, recorder.closed)
}

// TestKafkaProducerRequiresBrokers covers the constructor guard.
func TestKafkaProducerRequiresBrokers(t *testing.T) {
	t.Parallel()
	_, err := events.NewKafkaProducer(nil, events.TopicVoiceEvents, nil)
	require.Error(t, err)
}

// TestHeadersMirrorTheEnvelope covers the header helper.
func TestHeadersMirrorTheEnvelope(t *testing.T) {
	t.Parallel()
	envelope, err := events.NewEnvelope(events.EnvelopeOptions{EventType: events.EventTypeVoicePresenceChanged})
	require.NoError(t, err)
	headers := events.Headers(envelope)
	require.Len(t, headers, 2)
	assert.Equal(t, "trace_id", string(headers[0].Key))
	assert.Equal(t, envelope.TraceID, string(headers[0].Value))
	assert.Equal(t, "event_type", string(headers[1].Key))
	assert.Equal(t, envelope.EventType, string(headers[1].Value))
}
