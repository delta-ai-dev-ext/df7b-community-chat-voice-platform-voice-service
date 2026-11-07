package unit

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"voice_service/internal/events"
	"voice_service/internal/models"
	"voice_service/internal/service"
	"voice_service/tests/mocks"
)

// TestLeaveHappyPath covers US-005: the participant is removed from the room,
// the seat is freed, and a presence event is published.
func TestLeaveHappyPath(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	trace := events.NewTraceID()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{
		CommunityID:        communityID,
		ChannelID:          channelID,
		UserID:             userID,
		CameraEnabled:      true,
		ScreenShareEnabled: false,
		PublishingAllowed:  true,
	})

	leaver := service.NewLeaveService(f.Collaborators())
	require.NoError(t, leaver.Leave(context.Background(), service.LeaveInput{
		ChannelID: channelID,
		UserID:    userID,
		TraceID:   trace,
	}))

	assert.Equal(t, 1, f.LiveKit.RemovedCount())
	assert.Equal(t, 0, f.Presence.Len())
	require.Equal(t, 1, f.Producer.Len())

	message, ok := f.Producer.Last()
	require.True(t, ok)
	assert.Equal(t, channelID.String(), message.Key, "EVT-010 is keyed by channel id")
	assert.Equal(t, trace, message.Envelope.TraceID, "the inbound trace id is propagated")

	payload, ok := f.Producer.LastPresence()
	require.True(t, ok)
	assert.Equal(t, events.ChangeLeft, payload.Change)
	assert.Equal(t, userID.String(), payload.UserID)
	assert.Equal(t, 0, payload.ParticipantCount)
	assert.Equal(t, 4, payload.Capacity)
	assert.True(t, payload.CameraEnabled)
}

// TestLeaveIsIdempotent covers the second US-005 criterion: leaving a channel
// the caller is not in succeeds without side effects.
func TestLeaveIsIdempotent(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	leaver := service.NewLeaveService(f.Collaborators())

	require.NoError(t, leaver.Leave(context.Background(), service.LeaveInput{ChannelID: channelID, UserID: userID}))
	require.NoError(t, leaver.Leave(context.Background(), service.LeaveInput{ChannelID: channelID, UserID: userID}))

	assert.Equal(t, 0, f.Presence.Len())
	assert.Equal(t, 0, f.Producer.Len(), "a no-op leave must not publish a presence event")
}

// TestLeaveUnknownChannel covers the 404 path.
func TestLeaveUnknownChannel(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	err := service.NewLeaveService(f.Collaborators()).Leave(context.Background(), service.LeaveInput{
		ChannelID: uuid.New(),
		UserID:    uuid.New(),
	})
	require.ErrorIs(t, err, models.ErrChannelNotFound)
}

// TestLeaveFreesSeatEvenWhenLiveKitFails covers the best-effort removal: the
// seat must be freed even when the provider call fails.
func TestLeaveFreesSeatEvenWhenLiveKitFails(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID})
	f.LiveKit.FailRemove = errors.New("livekit unavailable")

	require.NoError(t, service.NewLeaveService(f.Collaborators()).Leave(context.Background(), service.LeaveInput{
		ChannelID: channelID,
		UserID:    userID,
	}))
	assert.Equal(t, 0, f.Presence.Len())
}

// TestLeaveSucceedsWhenPresenceEventCannotBePublished documents that a lost
// presence event does not fail the client's leave.
func TestLeaveSucceedsWhenPresenceEventCannotBePublished(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID})
	f.Producer.Fail = errors.New("kafka unavailable")

	require.NoError(t, service.NewLeaveService(f.Collaborators()).Leave(context.Background(), service.LeaveInput{
		ChannelID: channelID,
		UserID:    userID,
	}))
	assert.Equal(t, 0, f.Presence.Len())
}

// TestLeaveRequiresIdentifiers covers boundary validation.
func TestLeaveRequiresIdentifiers(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	require.ErrorIs(t,
		service.NewLeaveService(f.Collaborators()).Leave(context.Background(), service.LeaveInput{}),
		models.ErrInvalidRequest,
	)
}

// TestLeaveRepositoryFailureIsReported covers an internal failure.
func TestLeaveRepositoryFailureIsReported(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Err = errors.New("database down")

	err := service.NewLeaveService(f.Collaborators()).Leave(context.Background(), service.LeaveInput{
		ChannelID: channelID,
		UserID:    userID,
	})
	require.Error(t, err)
}
