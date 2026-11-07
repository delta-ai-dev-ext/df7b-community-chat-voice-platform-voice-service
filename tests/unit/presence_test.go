package unit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"voice_service/internal/models"
	"voice_service/internal/service"
	"voice_service/tests/mocks"
)

// TestPresenceSnapshot covers US-006: one entry per voice channel with
// participants, media flags, and seats used out of capacity.
func TestPresenceSnapshot(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, userID := uuid.New(), uuid.New()
	firstChannel, secondChannel := uuid.New(), uuid.New()

	speaker, listener := uuid.New(), uuid.New()
	capacity := 6
	void := 3
	_, err := f.Projections.UpsertMember(context.Background(), models.MemberProjection{
		CommunityID: communityID, UserID: userID, Status: models.MemberStatusActive, MemberVersion: 1,
	})
	require.NoError(t, err)
	_, err = f.Projections.UpsertChannel(context.Background(), models.ChannelProjection{
		CommunityID: communityID, ChannelID: firstChannel, Kind: models.ChannelKindVoice,
		Name: "General", Capacity: &capacity, ChannelVersion: 1,
	})
	require.NoError(t, err)
	_, err = f.Projections.UpsertChannel(context.Background(), models.ChannelProjection{
		CommunityID: communityID, ChannelID: secondChannel, Kind: models.ChannelKindVoice,
		Name: "Quiet", Capacity: &void, ChannelVersion: 1,
	})
	require.NoError(t, err)

	joinedAt := f.Now().Add(-5 * time.Minute)
	f.Presence.Seed(models.Presence{
		CommunityID: communityID, ChannelID: firstChannel, UserID: speaker,
		SessionJoinedAt: joinedAt, CameraEnabled: true, ScreenShareEnabled: true, PublishingAllowed: true,
	})
	f.Presence.Seed(models.Presence{
		CommunityID: communityID, ChannelID: firstChannel, UserID: listener,
		SessionJoinedAt: joinedAt, PublishingAllowed: false,
	})

	snapshot, err := service.NewPresenceService(f.Collaborators()).Snapshot(context.Background(), communityID, userID)
	require.NoError(t, err)
	assert.Equal(t, communityID, snapshot.CommunityID)
	require.Len(t, snapshot.Channels, 2)

	byID := make(map[uuid.UUID]models.ChannelPresence, len(snapshot.Channels))
	for _, channel := range snapshot.Channels {
		byID[channel.ChannelID] = channel
	}
	general := byID[firstChannel]
	assert.Equal(t, 6, general.Capacity)
	assert.Equal(t, 2, general.SeatsUsed)
	require.Len(t, general.Participants, 2)

	quiet := byID[secondChannel]
	assert.Equal(t, 0, quiet.SeatsUsed)
	assert.NotNil(t, quiet.Participants)
	assert.Empty(t, quiet.Participants)
}

// TestPresenceSnapshotRefusesNonMembers covers the 403 path: membership comes
// from the local projection only.
func TestPresenceSnapshotRefusesNonMembers(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, userID := uuid.New(), uuid.New()
	presence := service.NewPresenceService(f.Collaborators())

	_, err := presence.Snapshot(context.Background(), communityID, userID)
	require.ErrorIs(t, err, models.ErrNotAMember)

	_, err = f.Projections.UpsertMember(context.Background(), models.MemberProjection{
		CommunityID: communityID, UserID: userID, Status: models.MemberStatusRemoved, MemberVersion: 2,
	})
	require.NoError(t, err)
	_, err = presence.Snapshot(context.Background(), communityID, userID)
	require.ErrorIs(t, err, models.ErrNotAMember)

	_, err = f.Projections.UpsertMember(context.Background(), models.MemberProjection{
		CommunityID: communityID, UserID: userID, Status: models.MemberStatusActive, IsBanned: true, MemberVersion: 3,
	})
	require.NoError(t, err)
	_, err = presence.Snapshot(context.Background(), communityID, userID)
	require.ErrorIs(t, err, models.ErrNotAMember)
}

// TestPresenceSnapshotReportsRepositoryFailures covers internal failures.
func TestPresenceSnapshotReportsRepositoryFailures(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, userID := uuid.New(), uuid.New()
	_, err := f.Projections.UpsertMember(context.Background(), models.MemberProjection{
		CommunityID: communityID, UserID: userID, Status: models.MemberStatusActive, MemberVersion: 1,
	})
	require.NoError(t, err)
	f.Presence.Err = errors.New("database down")

	_, err = service.NewPresenceService(f.Collaborators()).Snapshot(context.Background(), communityID, userID)
	require.Error(t, err)
}

// TestPresenceSnapshotRequiresIdentifiers covers boundary validation.
func TestPresenceSnapshotRequiresIdentifiers(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	_, err := service.NewPresenceService(f.Collaborators()).Snapshot(context.Background(), uuid.Nil, uuid.New())
	require.ErrorIs(t, err, models.ErrInvalidRequest)
}

// TestPresenceChannelCapacityFallsBackToMaximum covers a projection without a
// capacity value.
func TestPresenceChannelCapacityFallsBackToMaximum(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, userID, channelID := uuid.New(), uuid.New(), uuid.New()
	_, err := f.Projections.UpsertMember(context.Background(), models.MemberProjection{
		CommunityID: communityID, UserID: userID, Status: models.MemberStatusActive, MemberVersion: 1,
	})
	require.NoError(t, err)
	_, err = f.Projections.UpsertChannel(context.Background(), models.ChannelProjection{
		CommunityID: communityID, ChannelID: channelID, Kind: models.ChannelKindVoice, Name: "General", ChannelVersion: 1,
	})
	require.NoError(t, err)

	snapshot, err := service.NewPresenceService(f.Collaborators()).Snapshot(context.Background(), communityID, userID)
	require.NoError(t, err)
	require.Len(t, snapshot.Channels, 1)
	assert.Equal(t, models.MaxCapacity, snapshot.Channels[0].Capacity)
}
