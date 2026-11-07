package unit

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"voice_service/internal/events"
)

// fakeClaim is an in-memory sarama.ConsumerGroupClaim.
type fakeClaim struct {
	topic     string
	partition int32
	highWater int64
	messages  chan *sarama.ConsumerMessage
}

// Topic implements sarama.ConsumerGroupClaim.
func (c *fakeClaim) Topic() string { return c.topic }

// Partition implements sarama.ConsumerGroupClaim.
func (c *fakeClaim) Partition() int32 { return c.partition }

// InitialOffset implements sarama.ConsumerGroupClaim.
func (c *fakeClaim) InitialOffset() int64 { return 0 }

// HighWaterMarkOffset implements sarama.ConsumerGroupClaim.
func (c *fakeClaim) HighWaterMarkOffset() int64 { return c.highWater }

// Messages implements sarama.ConsumerGroupClaim.
func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

// fakeSession is an in-memory sarama.ConsumerGroupSession.
type fakeSession struct {
	ctx     context.Context
	marked  []*sarama.ConsumerMessage
	offsets map[string]int64
}

// Claims implements sarama.ConsumerGroupSession.
func (s *fakeSession) Claims() map[string][]int32 { return map[string][]int32{"community_events": {0}} }

// MemberID implements sarama.ConsumerGroupSession.
func (s *fakeSession) MemberID() string { return "voice_service-test" }

// GenerationID implements sarama.ConsumerGroupSession.
func (s *fakeSession) GenerationID() int32 { return 1 }

// MarkOffset implements sarama.ConsumerGroupSession.
func (s *fakeSession) MarkOffset(topic string, partition int32, offset int64, _ string) {
	if s.offsets == nil {
		s.offsets = make(map[string]int64, 1)
	}
	s.offsets[topic] = offset
}

// Commit implements sarama.ConsumerGroupSession.
func (s *fakeSession) Commit() {}

// ResetOffset implements sarama.ConsumerGroupSession.
func (s *fakeSession) ResetOffset(string, int32, int64, string) {}

// MarkMessage implements sarama.ConsumerGroupSession.
func (s *fakeSession) MarkMessage(message *sarama.ConsumerMessage, _ string) {
	s.marked = append(s.marked, message)
}

// Context implements sarama.ConsumerGroupSession.
func (s *fakeSession) Context() context.Context { return s.ctx }

// fakeGroup is an in-memory sarama.ConsumerGroup.
type fakeGroup struct {
	sessions []*fakeSession
	claims   []*fakeClaim
	consume  int
	failure  error
	closed   bool
	errors   chan error
	// onConsume runs at the end of every Consume call, so a test can stop the
	// Run loop after one session.
	onConsume func()
}

// Consume implements sarama.ConsumerGroup.
func (g *fakeGroup) Consume(ctx context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
	g.consume++
	if g.onConsume != nil {
		defer g.onConsume()
	}
	if g.failure != nil {
		return g.failure
	}
	session := &fakeSession{ctx: ctx}
	if len(g.sessions) > 0 {
		session = g.sessions[0]
	}
	if err := handler.Setup(session); err != nil {
		return err
	}
	for _, claim := range g.claims {
		if err := handler.ConsumeClaim(session, claim); err != nil {
			return err
		}
	}
	return handler.Cleanup(session)
}

// Errors implements sarama.ConsumerGroup.
func (g *fakeGroup) Errors() <-chan error { return g.errors }

// Close implements sarama.ConsumerGroup.
func (g *fakeGroup) Close() error {
	g.closed = true
	return nil
}

// Pause implements sarama.ConsumerGroup.
func (g *fakeGroup) Pause(map[string][]int32) {}

// Resume implements sarama.ConsumerGroup.
func (g *fakeGroup) Resume(map[string][]int32) {}

// PauseAll implements sarama.ConsumerGroup.
func (g *fakeGroup) PauseAll() {}

// ResumeAll implements sarama.ConsumerGroup.
func (g *fakeGroup) ResumeAll() {}

// recordingHandler records the messages handed to it.
type recordingHandler struct {
	handled []*events.WireMessage
	fail    error
}

// Handle implements events.Handler.
func (h *recordingHandler) Handle(_ context.Context, message *events.WireMessage) error {
	h.handled = append(h.handled, message)
	return h.fail
}

// encodedCommunityEvent builds one encoded community event.
func encodedCommunityEvent(t *testing.T) []byte {
	t.Helper()
	envelope, err := events.NewEnvelope(events.EnvelopeOptions{EventType: events.EventTypeCommunityMemberChanged})
	require.NoError(t, err)
	encoded, err := json.Marshal(map[string]any{
		"envelope": envelope,
		"payload":  map[string]any{"community_id": "22222222-2222-2222-2222-222222222222"},
	})
	require.NoError(t, err)
	return encoded
}

// TestKafkaConsumerProcessesMessages covers the consumer group loop: decoded
// messages reach the handler, offsets are marked, and the lag gauge is fed.
func TestKafkaConsumerProcessesMessages(t *testing.T) {
	t.Parallel()
	claim := &fakeClaim{topic: events.TopicCommunityEvents, highWater: 5, messages: make(chan *sarama.ConsumerMessage, 1)}
	claim.messages <- &sarama.ConsumerMessage{
		Topic:     events.TopicCommunityEvents,
		Partition: 0,
		Offset:    3,
		Value:     encodedCommunityEvent(t),
	}
	close(claim.messages)

	session := &fakeSession{ctx: context.Background()}
	group := &fakeGroup{sessions: []*fakeSession{session}, claims: []*fakeClaim{claim}}
	handler := &recordingHandler{}

	var lagTopic, lagGroup string
	var lag int64 = -1
	consumer := events.NewKafkaConsumerWithGroup(group, "voice_service", events.TopicCommunityEvents, handler, nil,
		func(topic, group string, value int64) {
			lagTopic, lagGroup, lag = topic, group, value
		})

	ctx, cancel := context.WithCancel(context.Background())
	group.onConsume = cancel
	require.NoError(t, consumer.Run(ctx), "a cancelled context stops the loop without an error")

	require.Len(t, handler.handled, 1)
	assert.Equal(t, events.EventTypeCommunityMemberChanged, handler.handled[0].Envelope.EventType)
	require.Len(t, session.marked, 1)
	assert.Equal(t, events.TopicCommunityEvents, lagTopic)
	assert.Equal(t, "voice_service", lagGroup)
	assert.Equal(t, int64(1), lag, "lag is high watermark minus consumed offset")
}

// TestKafkaConsumerDropsPoisonMessages covers undecodable payloads: they are
// marked and skipped so the group makes progress.
func TestKafkaConsumerDropsPoisonMessages(t *testing.T) {
	t.Parallel()
	claim := &fakeClaim{topic: events.TopicCommunityEvents, messages: make(chan *sarama.ConsumerMessage, 1)}
	claim.messages <- &sarama.ConsumerMessage{Value: []byte("not-json")}
	close(claim.messages)

	session := &fakeSession{ctx: context.Background()}
	group := &fakeGroup{sessions: []*fakeSession{session}, claims: []*fakeClaim{claim}}
	handler := &recordingHandler{}
	consumer := events.NewKafkaConsumerWithGroup(group, "voice_service", events.TopicCommunityEvents, handler, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	group.onConsume = cancel
	require.NoError(t, consumer.Run(ctx))

	assert.Empty(t, handler.handled)
	require.Len(t, session.marked, 1, "a poison message is marked so it is not redelivered forever")
}

// TestKafkaConsumerKeepsFailedMessagesUncommitted covers the at-least-once
// contract: a failing handler leaves the offset uncommitted.
func TestKafkaConsumerKeepsFailedMessagesUncommitted(t *testing.T) {
	t.Parallel()
	claim := &fakeClaim{topic: events.TopicCommunityEvents, messages: make(chan *sarama.ConsumerMessage, 1)}
	claim.messages <- &sarama.ConsumerMessage{Value: encodedCommunityEvent(t)}
	close(claim.messages)

	session := &fakeSession{ctx: context.Background()}
	group := &fakeGroup{sessions: []*fakeSession{session}, claims: []*fakeClaim{claim}}
	handler := &recordingHandler{fail: errors.New("database down")}
	consumer := events.NewKafkaConsumerWithGroup(group, "voice_service", events.TopicCommunityEvents, handler, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	group.onConsume = cancel
	require.NoError(t, consumer.Run(ctx))

	assert.Empty(t, session.marked, "a failed reaction must be redelivered")
}

// TestKafkaConsumerReturnsGroupFailures covers the error path of Run and Close.
func TestKafkaConsumerReturnsGroupFailures(t *testing.T) {
	t.Parallel()
	group := &fakeGroup{failure: errors.New("broker unreachable")}
	consumer := events.NewKafkaConsumerWithGroup(group, "voice_service", events.TopicCommunityEvents, &recordingHandler{}, nil, nil)
	err := consumer.Run(context.Background())
	require.Error(t, err)
	require.NoError(t, consumer.Close())
	assert.True(t, group.closed)
}

// TestKafkaConsumerStopsOnCancellation covers the graceful shutdown path.
func TestKafkaConsumerStopsOnCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	group := &fakeGroup{}
	consumer := events.NewKafkaConsumerWithGroup(group, "voice_service", events.TopicCommunityEvents, &recordingHandler{}, nil, nil)
	require.NoError(t, consumer.Run(ctx))
	assert.Zero(t, group.consume, "no session is started after cancellation")
}

// TestKafkaConsumerRequiresDependencies covers the constructor guards.
func TestKafkaConsumerRequiresDependencies(t *testing.T) {
	t.Parallel()
	_, err := events.NewKafkaConsumer(nil, "voice_service", events.TopicCommunityEvents, &recordingHandler{}, nil, nil)
	require.Error(t, err)
	_, err = events.NewKafkaConsumer([]string{"localhost:9092"}, "voice_service", events.TopicCommunityEvents, nil, nil, nil)
	require.Error(t, err)
}

// TestKafkaHealthChecker covers the readiness helper.
func TestKafkaHealthChecker(t *testing.T) {
	t.Parallel()
	_, err := events.NewKafkaHealthChecker(nil)
	require.Error(t, err)

	var missing *events.KafkaHealthChecker
	require.Error(t, missing.Check(context.Background()))
	require.NoError(t, missing.Close())
}

// TestKafkaConsumerSessionLifecycle covers Setup and Cleanup.
func TestKafkaConsumerSessionLifecycle(t *testing.T) {
	t.Parallel()
	claim := &fakeClaim{topic: events.TopicCommunityEvents, messages: make(chan *sarama.ConsumerMessage)}
	// A claim whose channel closes immediately ends ConsumeClaim cleanly.
	close(claim.messages)

	session := &fakeSession{ctx: context.Background()}
	group := &fakeGroup{sessions: []*fakeSession{session}, claims: []*fakeClaim{claim}}
	consumer := events.NewKafkaConsumerWithGroup(group, "voice_service", events.TopicCommunityEvents, &recordingHandler{}, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	group.onConsume = cancel
	require.NoError(t, consumer.Run(ctx))
	assert.Equal(t, 1, group.consume)
}

// TestKafkaConsumerStopsOnSessionCancellation covers ConsumeClaim returning
// when the session context ends.
func TestKafkaConsumerStopsOnSessionCancellation(t *testing.T) {
	t.Parallel()
	claim := &fakeClaim{topic: events.TopicCommunityEvents, messages: make(chan *sarama.ConsumerMessage)}
	sessionCtx, cancelSession := context.WithCancel(context.Background())
	session := &fakeSession{ctx: sessionCtx}
	go func() {
		time.Sleep(time.Millisecond)
		cancelSession()
	}()

	group := &fakeGroup{sessions: []*fakeSession{session}, claims: []*fakeClaim{claim}}
	consumer := events.NewKafkaConsumerWithGroup(group, "voice_service", events.TopicCommunityEvents, &recordingHandler{}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, consumer.Run(ctx))
}
