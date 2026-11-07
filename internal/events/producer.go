package events

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// Producer publishes {"envelope","payload"} messages to a topic.
type Producer interface {
	// Publish marshals and sends one message.
	Publish(ctx context.Context, msg Message) error
	// Close flushes and releases the underlying client.
	Close() error
}

// SyncProducer is the subset of sarama.SyncProducer used by KafkaProducer.
type SyncProducer interface {
	SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error)
	Close() error
}

// KafkaProducer publishes EVT-010 messages to the voice_events topic.
type KafkaProducer struct {
	producer SyncProducer
	topic    string
	log      *zap.Logger
}

// NewKafkaProducer dials the brokers and returns a producer for topic.
func NewKafkaProducer(brokers []string, topic string, log *zap.Logger) (*KafkaProducer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka producer: no brokers configured")
	}
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Idempotent = true
	cfg.Producer.Retry.Max = 3
	cfg.Net.MaxOpenRequests = 1
	cfg.ClientID = ProducerName
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka producer config: %w", err)
	}
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return NewKafkaProducerWithClient(producer, topic, log), nil
}

// NewKafkaProducerWithClient wires a producer around an existing client.
func NewKafkaProducerWithClient(producer SyncProducer, topic string, log *zap.Logger) *KafkaProducer {
	if log == nil {
		log = zap.NewNop()
	}
	return &KafkaProducer{producer: producer, topic: topic, log: log}
}

// Publish sends one message with the envelope mirrored into the message
// headers (trace_id, event_type); the JSON envelope stays the source of truth.
func (p *KafkaProducer) Publish(ctx context.Context, msg Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := msg.Marshal()
	if err != nil {
		return err
	}
	record := &sarama.ProducerMessage{
		Topic:     p.topic,
		Key:       sarama.StringEncoder(msg.Key),
		Value:     sarama.ByteEncoder(data),
		Headers:   Headers(msg.Envelope),
		Timestamp: msg.Envelope.OccurredAt,
	}
	partition, offset, err := p.producer.SendMessage(record)
	if err != nil {
		return fmt.Errorf("publish %s: %w", msg.Envelope.EventType, err)
	}
	p.log.Debug("event published",
		zap.String("event_type", msg.Envelope.EventType),
		zap.String("trace_id", msg.Envelope.TraceID),
		zap.String("correlation_id", msg.Envelope.CorrelationID),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
	)
	return nil
}

// Close releases the underlying producer.
func (p *KafkaProducer) Close() error {
	if p.producer == nil {
		return nil
	}
	return p.producer.Close()
}

// Headers mirrors the envelope into Kafka message headers.
func Headers(env Envelope) []sarama.RecordHeader {
	return []sarama.RecordHeader{
		{Key: []byte("trace_id"), Value: []byte(env.TraceID)},
		{Key: []byte("event_type"), Value: []byte(env.EventType)},
	}
}
