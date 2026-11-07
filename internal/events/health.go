package events

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
)

// KafkaHealthChecker verifies broker reachability for the readiness probe.
type KafkaHealthChecker struct {
	client sarama.Client
}

// NewKafkaHealthChecker dials the brokers once; the returned checker refreshes
// metadata on every readiness probe.
func NewKafkaHealthChecker(brokers []string) (*KafkaHealthChecker, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka health: no brokers configured")
	}
	cfg := sarama.NewConfig()
	cfg.ClientID = ProducerName
	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka health: dial brokers: %w", err)
	}
	return &KafkaHealthChecker{client: client}, nil
}

// Check refreshes the cluster metadata, which fails when no broker answers.
func (c *KafkaHealthChecker) Check(ctx context.Context) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("kafka health: no client")
	}
	done := make(chan error, 1)
	go func() { done <- c.client.RefreshMetadata() }()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("kafka health: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close releases the health-check client.
func (c *KafkaHealthChecker) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Close()
}
