package unit

import (
	"context"
	"errors"
	"sync"
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

// TestReconcilerHealsBothDirections covers US-010: stale local presence is
// closed and participants LiveKit reports but the local store missed are added.
func TestReconcilerHealsBothDirections(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID := uuid.New(), uuid.New()
	stale, missed := uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: stale, PublishingAllowed: true})
	f.LiveKit.SetParticipants(service.RoomName(channelID), livekit.Participant{
		Identity:      missed.String(),
		CameraEnabled: true,
	})

	reconciler := service.NewReconciler(f.Collaborators(), time.Minute, 24*time.Hour)
	require.NoError(t, reconciler.Sweep(context.Background()))

	_, staleActive := f.Presence.Active(communityID, channelID, stale)
	assert.False(t, staleActive, "the stale participant is closed")
	session, missedActive := f.Presence.Active(communityID, channelID, missed)
	require.True(t, missedActive, "the missed participant is recorded")
	assert.True(t, session.CameraEnabled)
	assert.Equal(t, 2, f.Producer.Len())

	message, ok := f.Producer.Last()
	require.True(t, ok)
	assert.True(t, events.ValidTraceID(message.Envelope.TraceID), "a scheduled flow mints its own trace id")
	assert.Equal(t, 1, f.Dedupe.PurgeCalls(), "the retention sweep runs with reconciliation")
}

// TestReconcilerNeverMutatesOnLiveKitFailure covers the healing guarantee.
func TestReconcilerNeverMutatesOnLiveKitFailure(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID})
	f.LiveKit.FailList = errors.New("livekit unavailable")

	reconciler := service.NewReconciler(f.Collaborators(), time.Minute, 24*time.Hour)
	require.NoError(t, reconciler.Sweep(context.Background()))

	assert.Equal(t, 1, f.Presence.Len(), "local presence is untouched when the provider is unreachable")
	assert.Equal(t, 0, f.Producer.Len())
}

// TestReconcilerIgnoresMissingRooms covers rooms LiveKit already dropped.
func TestReconcilerIgnoresMissingRooms(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID})

	reconciler := service.NewReconciler(f.Collaborators(), time.Minute, 24*time.Hour)
	require.NoError(t, reconciler.Sweep(context.Background()))
	assert.Equal(t, 1, f.Presence.Len())
}

// TestReconcilerIgnoresNonUserIdentities covers agent participants.
func TestReconcilerIgnoresNonUserIdentities(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID})
	f.LiveKit.SetParticipants(service.RoomName(channelID),
		livekit.Participant{Identity: userID.String()},
		livekit.Participant{Identity: "agent-worker"},
	)

	reconciler := service.NewReconciler(f.Collaborators(), time.Minute, 24*time.Hour)
	require.NoError(t, reconciler.Sweep(context.Background()))
	assert.Equal(t, 1, f.Presence.Len())
	assert.Equal(t, 0, f.Producer.Len())
}

// TestReconcilerIsSingleFlight covers the single-flight guard.
func TestReconcilerIsSingleFlight(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID})
	f.LiveKit.SetParticipants(service.RoomName(channelID), livekit.Participant{Identity: userID.String()})

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	f.LiveKit.BeforeList = func(string) {
		once.Do(func() { close(entered) })
		<-release
	}

	reconciler := service.NewReconciler(f.Collaborators(), time.Minute, 24*time.Hour)
	firstDone := make(chan error, 1)
	go func() { firstDone <- reconciler.Sweep(context.Background()) }()

	<-entered
	err := reconciler.Sweep(context.Background())
	require.ErrorIs(t, err, service.ErrSweepInFlight, "a second sweep must not run concurrently")

	close(release)
	require.NoError(t, <-firstDone)

	// The guard is released again.
	require.NoError(t, reconciler.Sweep(context.Background()))
}

// TestReconcilerRunPeriodicStopsWithContext covers the worker lifecycle.
func TestReconcilerRunPeriodicStopsWithContext(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID})
	f.LiveKit.SetParticipants(service.RoomName(channelID), livekit.Participant{Identity: userID.String()})

	ctx, cancel := context.WithCancel(context.Background())
	reconciler := service.NewReconciler(f.Collaborators(), 10*time.Millisecond, time.Hour)
	done := make(chan struct{})
	go func() {
		defer close(done)
		reconciler.RunPeriodic(ctx)
	}()

	require.Eventually(t, func() bool { return f.Dedupe.PurgeCalls() > 0 }, time.Second, 10*time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the periodic worker did not stop on context cancellation")
	}
}

// TestReconcilerSweepFailuresAreReported covers repository failures.
func TestReconcilerSweepFailuresAreReported(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	f.Rooms.Err = errors.New("database down")
	reconciler := service.NewReconciler(f.Collaborators(), time.Minute, time.Hour)
	require.Error(t, reconciler.Sweep(context.Background()))
}

// TestReconcilerRelistsOnlyRoomsWithPresence documents the sweep scope.
func TestReconcilerRelistsOnlyRoomsWithPresence(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, idleChannel := uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: idleChannel, CommunityID: communityID, Capacity: 4})

	reconciler := service.NewReconciler(f.Collaborators(), time.Minute, time.Hour)
	require.NoError(t, reconciler.Sweep(context.Background()))
	assert.Equal(t, 0, f.Producer.Len())
}

// TestReconcilerEmitsMediaFlagsOfMissedParticipants covers the healed payload.
func TestReconcilerEmitsMediaFlagsOfMissedParticipants(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: uuid.New()})
	f.LiveKit.SetParticipants(service.RoomName(channelID), livekit.Participant{
		Identity:           userID.String(),
		ScreenShareEnabled: true,
	})

	reconciler := service.NewReconciler(f.Collaborators(), time.Minute, time.Hour)
	require.NoError(t, reconciler.Sweep(context.Background()))

	payload, ok := f.Producer.LastPresence()
	require.True(t, ok)
	assert.Equal(t, events.ChangeJoined, payload.Change)
	assert.True(t, payload.ScreenShareEnabled)
	assert.Equal(t, 1, payload.ParticipantCount, "the stale participant was closed before the healed one joined")
	assert.Zero(t, metricValue(t, f.Registry, "external_call_errors_total", map[string]string{
		"provider": "livekit", "operation": "list_participants", "error_class": "unavailable",
	}))
}

// TestReconcilerPublishesNoEventsOnFailure covers silence on provider errors.
func TestReconcilerPublishesNoEventsOnFailure(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID})
	f.LiveKit.FailList = errors.New("boom")
	f.Rooms.Err = nil

	reconciler := service.NewReconciler(f.Collaborators(), time.Minute, time.Hour)
	require.NoError(t, reconciler.Sweep(context.Background()))
	assert.Equal(t, 0, f.Producer.Len())
	assert.Equal(t, 1, f.Presence.Len())

	// A missing room is treated as an empty participant list, not an error.
	f.LiveKit.FailList = nil
	delete(f.LiveKit.Participants, service.RoomName(channelID))
	reconcilerNoRoom := service.NewReconciler(f.Collaborators(), time.Minute, time.Hour)
	require.NoError(t, reconcilerNoRoom.Sweep(context.Background()))
}

// TestModelsSeatReservationKeepsCapacityBounds covers the domain constants.
func TestModelsSeatReservationKeepsCapacityBounds(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 1, models.MinCapacity)
	assert.Equal(t, 10, models.MaxCapacity)
}
