package unit

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"voice_service/internal/events"
	"voice_service/internal/livekit"
	"voice_service/internal/models"
	"voice_service/internal/service"
	"voice_service/tests/mocks"
)

// wireMessage encodes a community event exactly as it arrives from Kafka.
func wireMessage(t *testing.T, eventType string, traceID string, payload any) *events.WireMessage {
	t.Helper()
	envelope, err := events.NewEnvelope(events.EnvelopeOptions{EventType: eventType, TraceID: traceID})
	require.NoError(t, err)
	encoded, err := json.Marshal(map[string]any{"envelope": envelope, "payload": payload})
	require.NoError(t, err)
	wire, err := events.ParseWireMessage(encoded)
	require.NoError(t, err)
	return wire
}

// memberChangedPayload builds an EVT-002 payload.
func memberChangedPayload(communityID, userID uuid.UUID, change, status string, version int64) events.MemberChangedPayload {
	return events.MemberChangedPayload{
		CommunityID:   communityID.String(),
		UserID:        userID.String(),
		Change:        change,
		Status:        status,
		IsBanned:      change == events.MemberChangeBanned,
		MemberVersion: version,
	}
}

// channelChangedPayload builds an EVT-003 payload.
func channelChangedPayload(communityID, channelID uuid.UUID, change, kind string, capacity *int, version int64) events.ChannelChangedPayload {
	return events.ChannelChangedPayload{
		CommunityID:    communityID.String(),
		ChannelID:      channelID.String(),
		Change:         change,
		Kind:           kind,
		Name:           "General",
		Capacity:       capacity,
		ChannelVersion: version,
	}
}

// TestConsumerRemovedMemberIsDisconnected covers US-008: a removed member is
// disconnected from every live room with a caused EVT-010 event.
func TestConsumerRemovedMemberIsDisconnected(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, userID := uuid.New(), uuid.New()
	firstChannel, secondChannel := uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: firstChannel, CommunityID: communityID, Capacity: 4})
	f.Rooms.Seed(models.VoiceRoom{ChannelID: secondChannel, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: firstChannel, UserID: userID, PublishingAllowed: true})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: secondChannel, UserID: userID, PublishingAllowed: true})

	trace := events.NewTraceID()
	message := wireMessage(t, events.EventTypeCommunityMemberChanged, trace,
		memberChangedPayload(communityID, userID, events.MemberChangeRemoved, models.MemberStatusRemoved, 2))

	consumer := service.NewCommunityEventConsumer(f.Collaborators()).WithRetryPolicy(service.RetryPolicy{Attempts: 1})
	require.NoError(t, consumer.Handle(context.Background(), message))

	assert.Equal(t, 2, f.LiveKit.RemovedCount())
	assert.Equal(t, 0, f.Presence.Len())
	assert.Equal(t, 2, f.Producer.Len())
	assert.Equal(t, float64(2), metricValue(t, f.Registry, "voice_moderation_removals_total", map[string]string{"action": "removed"}))

	published, ok := f.Producer.Last()
	require.True(t, ok)
	assert.Equal(t, trace, published.Envelope.TraceID, "the causing trace id is copied")
	require.NotNil(t, published.Envelope.CausationID)
	assert.Equal(t, message.Envelope.EventID, *published.Envelope.CausationID)
	assert.Equal(t, 1, f.Dedupe.EventsLen())
}

// TestConsumerBannedMemberIsDisconnected covers the banned variant.
func TestConsumerBannedMemberIsDisconnected(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, userID, channelID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID})

	message := wireMessage(t, events.EventTypeCommunityMemberChanged, events.NewTraceID(),
		memberChangedPayload(communityID, userID, events.MemberChangeBanned, models.MemberStatusActive, 3))

	require.NoError(t, service.NewCommunityEventConsumer(f.Collaborators()).Handle(context.Background(), message))
	assert.Equal(t, 1, f.LiveKit.RemovedCount())
	assert.Equal(t, 0, f.Presence.Len())
}

// TestConsumerStaleMemberVersionIsIgnored covers the version guard.
func TestConsumerStaleMemberVersionIsIgnored(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, userID, channelID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID})
	_, err := f.Projections.UpsertMember(context.Background(), models.MemberProjection{
		CommunityID: communityID, UserID: userID, Status: models.MemberStatusActive, MemberVersion: 7,
	})
	require.NoError(t, err)

	message := wireMessage(t, events.EventTypeCommunityMemberChanged, events.NewTraceID(),
		memberChangedPayload(communityID, userID, events.MemberChangeRemoved, models.MemberStatusRemoved, 3))
	require.NoError(t, service.NewCommunityEventConsumer(f.Collaborators()).Handle(context.Background(), message))

	assert.Equal(t, 1, f.Projections.StaleWrites)
	assert.Equal(t, 0, f.LiveKit.RemovedCount(), "a stale event must not disconnect anyone")
	assert.Equal(t, 1, f.Presence.Len())
	assert.Equal(t, 0, f.Producer.Len())
}

// TestConsumerDuplicateEventIsIgnored covers Kafka dedupe on envelope.event_id.
func TestConsumerDuplicateEventIsIgnored(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, userID, channelID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID})
	message := wireMessage(t, events.EventTypeCommunityMemberChanged, events.NewTraceID(),
		memberChangedPayload(communityID, userID, events.MemberChangeRemoved, models.MemberStatusRemoved, 2))

	consumer := service.NewCommunityEventConsumer(f.Collaborators())
	require.NoError(t, consumer.Handle(context.Background(), message))
	require.NoError(t, consumer.Handle(context.Background(), message))

	assert.Equal(t, 1, f.Producer.Len(), "the duplicate must not publish a second event")
	assert.Equal(t, 1, f.Dedupe.EventsLen())
}

// TestConsumerMutedMemberLosesPublishingInPlace covers the in-place revocation.
func TestConsumerMutedMemberLosesPublishingInPlace(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, userID, channelID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{
		CommunityID: communityID, ChannelID: channelID, UserID: userID,
		CameraEnabled: true, PublishingAllowed: true,
	})
	mutedUntil := f.Now().Add(time.Hour)
	payload := memberChangedPayload(communityID, userID, events.MemberChangeMuted, models.MemberStatusActive, 4)
	payload.MutedUntil = &mutedUntil
	message := wireMessage(t, events.EventTypeCommunityMemberChanged, events.NewTraceID(), payload)

	require.NoError(t, service.NewCommunityEventConsumer(f.Collaborators()).Handle(context.Background(), message))

	assert.Equal(t, 1, f.LiveKit.RevokedCount())
	assert.Contains(t, f.LiveKit.Revoked[0], "/revoked")
	session, active := f.Presence.Active(communityID, channelID, userID)
	require.True(t, active, "a muted member stays in the room as a listener")
	assert.False(t, session.PublishingAllowed)
	assert.Equal(t, float64(1), metricValue(t, f.Registry, "voice_moderation_removals_total", map[string]string{"action": "publish_revoked"}))

	presenceEvent, ok := f.Producer.LastPresence()
	require.True(t, ok)
	assert.Equal(t, events.ChangeMediaChanged, presenceEvent.Change)
	assert.False(t, presenceEvent.PublishingAllowed)
}

// TestConsumerUnmuteOnlyUpdatesTheProjection covers the "no silent upgrade"
// rule: publishing returns at the next join, not mid-session.
func TestConsumerUnmuteOnlyUpdatesTheProjection(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, userID, channelID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID, PublishingAllowed: false})
	message := wireMessage(t, events.EventTypeCommunityMemberChanged, events.NewTraceID(),
		memberChangedPayload(communityID, userID, events.MemberChangeUnmuted, models.MemberStatusActive, 5))

	require.NoError(t, service.NewCommunityEventConsumer(f.Collaborators()).Handle(context.Background(), message))

	assert.Equal(t, 0, f.LiveKit.RevokedCount())
	assert.Equal(t, 0, f.Producer.Len())
	session, active := f.Presence.Active(communityID, channelID, userID)
	require.True(t, active)
	assert.False(t, session.PublishingAllowed, "an existing session is not upgraded")
	member, err := f.Projections.GetMember(context.Background(), communityID, userID)
	require.NoError(t, err)
	assert.Equal(t, int64(5), member.MemberVersion)
}

// TestConsumerDeletedVoiceChannelClosesRoom covers US-009.
func TestConsumerDeletedVoiceChannelClosesRoom(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID := uuid.New(), uuid.New()
	first, second := uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: first, PublishingAllowed: true})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: second, PublishingAllowed: true})
	message := wireMessage(t, events.EventTypeCommunityChannelChanged, events.NewTraceID(),
		channelChangedPayload(communityID, channelID, events.ChannelChangeDeleted, models.ChannelKindVoice, nil, 6))

	require.NoError(t, service.NewCommunityEventConsumer(f.Collaborators()).Handle(context.Background(), message))

	assert.Equal(t, 1, f.LiveKit.DeletedCount())
	assert.Equal(t, 0, f.Presence.Len())
	assert.Equal(t, 2, f.Producer.Len())
}

// TestConsumerDeletedUnknownChannelIsNoOp covers a deletion of a room that was
// never created.
func TestConsumerDeletedUnknownChannelIsNoOp(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID := uuid.New(), uuid.New()
	message := wireMessage(t, events.EventTypeCommunityChannelChanged, events.NewTraceID(),
		channelChangedPayload(communityID, channelID, events.ChannelChangeDeleted, models.ChannelKindVoice, nil, 2))

	require.NoError(t, service.NewCommunityEventConsumer(f.Collaborators()).Handle(context.Background(), message))
	assert.Equal(t, 0, f.LiveKit.DeletedCount())
	assert.Equal(t, 0, f.Producer.Len())
}

// TestConsumerUpdatedCapacityRefreshesRoom covers the capacity update rule.
func TestConsumerUpdatedCapacityRefreshesRoom(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID := uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	capacity := 8
	message := wireMessage(t, events.EventTypeCommunityChannelChanged, events.NewTraceID(),
		channelChangedPayload(communityID, channelID, events.ChannelChangeUpdated, models.ChannelKindVoice, &capacity, 3))

	require.NoError(t, service.NewCommunityEventConsumer(f.Collaborators()).Handle(context.Background(), message))

	room, err := f.Rooms.GetRoom(context.Background(), channelID)
	require.NoError(t, err)
	assert.Equal(t, 8, room.Capacity)
}

// TestConsumerCreatedVoiceChannelRegistersRoom covers lazy room registration.
func TestConsumerCreatedVoiceChannelRegistersRoom(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID := uuid.New(), uuid.New()
	capacity := 3
	message := wireMessage(t, events.EventTypeCommunityChannelChanged, events.NewTraceID(),
		channelChangedPayload(communityID, channelID, events.ChannelChangeCreated, models.ChannelKindVoice, &capacity, 1))

	require.NoError(t, service.NewCommunityEventConsumer(f.Collaborators()).Handle(context.Background(), message))

	room, err := f.Rooms.GetRoom(context.Background(), channelID)
	require.NoError(t, err)
	assert.Equal(t, 3, room.Capacity)
	assert.Equal(t, service.RoomName(channelID), room.LiveKitRoomName)
}

// TestConsumerIgnoresTextChannels covers the "text channels are ignored" rule.
func TestConsumerIgnoresTextChannels(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID := uuid.New(), uuid.New()
	message := wireMessage(t, events.EventTypeCommunityChannelChanged, events.NewTraceID(),
		channelChangedPayload(communityID, channelID, events.ChannelChangeCreated, models.ChannelKindText, nil, 1))

	require.NoError(t, service.NewCommunityEventConsumer(f.Collaborators()).Handle(context.Background(), message))
	assert.Equal(t, 0, f.Rooms.Len())
}

// TestConsumerDropsMalformedPayloads keeps poison messages from looping.
func TestConsumerDropsMalformedPayloads(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	envelope, err := events.NewEnvelope(events.EnvelopeOptions{EventType: events.EventTypeCommunityMemberChanged})
	require.NoError(t, err)
	encoded, err := json.Marshal(map[string]any{
		"envelope": envelope,
		"payload":  map[string]any{"community_id": "not-a-uuid"},
	})
	require.NoError(t, err)
	wire, err := events.ParseWireMessage(encoded)
	require.NoError(t, err)

	require.NoError(t, service.NewCommunityEventConsumer(f.Collaborators()).Handle(context.Background(), wire))
	assert.Equal(t, 0, f.Producer.Len())
}

// TestConsumerIgnoresUnknownEventTypes covers unrelated community events.
func TestConsumerIgnoresUnknownEventTypes(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	message := wireMessage(t, "community_roles_changed", events.NewTraceID(), map[string]any{})
	require.NoError(t, service.NewCommunityEventConsumer(f.Collaborators()).Handle(context.Background(), message))
	assert.Equal(t, 0, f.Producer.Len())
}

// TestConsumerFailureForgetsDedupeRow covers redelivery after a failure.
func TestConsumerFailureForgetsDedupeRow(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, userID, channelID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID})
	f.Producer.Fail = errors.New("kafka unavailable")
	message := wireMessage(t, events.EventTypeCommunityMemberChanged, events.NewTraceID(),
		memberChangedPayload(communityID, userID, events.MemberChangeRemoved, models.MemberStatusRemoved, 2))

	err := service.NewCommunityEventConsumer(f.Collaborators()).Handle(context.Background(), message)
	require.Error(t, err)
	assert.Equal(t, 0, f.Dedupe.EventsLen(), "the dedupe row must be dropped so the redelivery is processed")
}

// TestConsumerToleratesVanishedParticipant covers an idempotent removal.
func TestConsumerToleratesVanishedParticipant(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, userID, channelID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID})
	f.LiveKit.FailRemove = livekit.ErrParticipantNotFound
	message := wireMessage(t, events.EventTypeCommunityMemberChanged, events.NewTraceID(),
		memberChangedPayload(communityID, userID, events.MemberChangeRemoved, models.MemberStatusRemoved, 2))

	require.NoError(t, service.NewCommunityEventConsumer(f.Collaborators()).Handle(context.Background(), message))
	assert.Equal(t, 0, f.Presence.Len())
}

// TestConsumerReportsLiveKitFailures covers the retry policy surfacing errors.
func TestConsumerReportsLiveKitFailures(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, userID, channelID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID})
	f.LiveKit.FailRemove = errors.New("livekit unavailable")
	message := wireMessage(t, events.EventTypeCommunityMemberChanged, events.NewTraceID(),
		memberChangedPayload(communityID, userID, events.MemberChangeRemoved, models.MemberStatusRemoved, 2))

	consumer := service.NewCommunityEventConsumer(f.Collaborators()).WithRetryPolicy(service.RetryPolicy{Attempts: 1})
	require.Error(t, consumer.Handle(context.Background(), message))
	assert.Equal(t, 1, f.Presence.Len(), "the participant stays until LiveKit confirms the removal")
	assert.Equal(t, 0, f.Dedupe.EventsLen())
}

// TestConsumerNilMessage covers the guard.
func TestConsumerNilMessage(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	require.Error(t, service.NewCommunityEventConsumer(f.Collaborators()).Handle(context.Background(), nil))
}
