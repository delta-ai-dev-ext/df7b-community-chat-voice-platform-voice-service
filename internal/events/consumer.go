package events

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// Handler processes one decoded community event. A returned error keeps the
// Kafka offset uncommitted so the message is redelivered.
type Handler interface {
	Handle(ctx context.Context, msg *WireMessage) error
}

// LagObserver receives the observed consumer lag after every processed
// message. It is called with low-cardinality labels only.
type LagObserver func(topic, consumerGroup string, lag int64)

// KafkaConsumer runs the shared consumer group over community_events.
type KafkaConsumer struct {
	group   sarama.ConsumerGroup
	topic   string
	groupID string
	handler Handler
	log     *zap.Logger
	observe LagObserver
}

// NewKafkaConsumer dials the brokers and prepares the consumer group.
func NewKafkaConsumer(
	brokers []string,
	groupID string,
	topic string,
	handler Handler,
	log *zap.Logger,
	observe LagObserver,
) (*KafkaConsumer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka consumer: no brokers configured")
	}
	if handler == nil {
		return nil, fmt.Errorf("kafka consumer: handler is required")
	}
	cfg := sarama.NewConfig()
	cfg.ClientID = ProducerName
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = false
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka consumer config: %w", err)
	}
	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer group: %w", err)
	}
	return NewKafkaConsumerWithGroup(group, groupID, topic, handler, log, observe), nil
}

// NewKafkaConsumerWithGroup wires the consumer around an existing group.
func NewKafkaConsumerWithGroup(
	group sarama.ConsumerGroup,
	groupID string,
	topic string,
	handler Handler,
	log *zap.Logger,
	observe LagObserver,
) *KafkaConsumer {
	if log == nil {
		log = zap.NewNop()
	}
	return &KafkaConsumer{
		group:   group,
		topic:   topic,
		groupID: groupID,
		handler: handler,
		log:     log,
		observe: observe,
	}
}

// Run consumes until ctx is cancelled, returning nil on graceful shutdown.
func (c *KafkaConsumer) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if err := c.group.Consume(ctx, []string{c.topic}, &consumerSession{consumer: c}); err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("kafka consume: %w", err)
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// Close leaves the consumer group.
func (c *KafkaConsumer) Close() error {
	if c.group == nil {
		return nil
	}
	return c.group.Close()
}

// consumerSession adapts KafkaConsumer to sarama.ConsumerGroupHandler.
type consumerSession struct {
	consumer *KafkaConsumer
}

// Setup implements sarama.ConsumerGroupHandler.
func (s *consumerSession) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup implements sarama.ConsumerGroupHandler.
func (s *consumerSession) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim implements sarama.ConsumerGroupHandler.
func (s *consumerSession) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			s.handleMessage(session, claim, message)
		case <-session.Context().Done():
			return nil
		}
	}
}

// handleMessage decodes and processes one message, marking it only when the
// handler succeeded so failures are redelivered.
func (s *consumerSession) handleMessage(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
	message *sarama.ConsumerMessage,
) {
	started := time.Now()
	wire, err := ParseWireMessage(message.Value)
	if err != nil {
		// A poison message can never succeed; mark it and move on.
		s.consumer.log.Error("dropping undecodable community event",
			zap.Int32("partition", message.Partition),
			zap.Int64("offset", message.Offset),
			zap.Error(err),
		)
		session.MarkMessage(message, "")
		return
	}
	ctx := session.Context()
	if err := s.consumer.handler.Handle(ctx, wire); err != nil {
		s.consumer.log.Error("community event handling failed",
			zap.String("event_type", wire.Envelope.EventType),
			zap.String("event_id", wire.Envelope.EventID),
			zap.String("trace_id", wire.Envelope.TraceID),
			zap.Error(err),
		)
		return
	}
	session.MarkMessage(message, "")
	if s.consumer.observe != nil {
		lag := claim.HighWaterMarkOffset() - (message.Offset + 1)
		if lag < 0 {
			lag = 0
		}
		s.consumer.observe(s.consumer.topic, s.consumer.groupID, lag)
	}
	s.consumer.log.Debug("community event processed",
		zap.String("event_type", wire.Envelope.EventType),
		zap.String("event_id", wire.Envelope.EventID),
		zap.String("trace_id", wire.Envelope.TraceID),
		zap.Duration("duration", time.Since(started)),
	)
}
