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

	"voice_service/internal/community"
	"voice_service/internal/models"
	"voice_service/internal/service"
	"voice_service/tests/mocks"
)

// allowedCheck builds an allowed API-002 answer with the given channels.
func allowedCheck(communityID, channelID uuid.UUID, capacity int, channels ...models.ChannelSummary) *community.AccessCheckResult {
	channel := models.ChannelSummary{ChannelID: channelID, Name: "General", Capacity: capacity}
	if len(channels) == 0 {
		channels = []models.ChannelSummary{channel}
	}
	return &community.AccessCheckResult{
		CommunityID:   communityID,
		Channel:       &channel,
		Allowed:       true,
		VoiceChannels: channels,
	}
}

// TestJoinHappyPath covers US-001: a member receives the LiveKit endpoint, the
// room name of the channel, and a short-lived publishing token.
func TestJoinHappyPath(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Access.Result = allowedCheck(communityID, channelID, 5)

	joiner := service.NewJoinService(f.Collaborators())
	response, err := joiner.Join(context.Background(), service.JoinInput{
		CommunityID: communityID,
		ChannelID:   channelID,
		UserID:      userID,
		TraceID:     "trace-1",
	})
	require.NoError(t, err)

	assert.Equal(t, "wss://livekit.test", response.LiveKitURL)
	assert.Equal(t, "participant-token", response.Token)
	assert.Equal(t, service.RoomName(channelID), response.RoomName)
	assert.True(t, response.CanPublishAudio)
	assert.True(t, response.CanPublishVideo)
	assert.True(t, response.CanPublishScreen)
	assert.Nil(t, response.MutedUntil)
	assert.WithinDuration(t, f.Now().Add(5*time.Minute), response.TokenExpiresAt, time.Minute)

	// The room is created with the channel capacity as its participant backstop.
	assert.Equal(t, 5, f.LiveKit.MaxParticipants[service.RoomName(channelID)])

	tokenRequest, ok := f.Tokens.Last()
	require.True(t, ok)
	assert.Equal(t, service.RoomName(channelID), tokenRequest.RoomName)
	assert.Equal(t, userID.String(), tokenRequest.Identity)
	assert.True(t, tokenRequest.CanPublish)
	assert.Equal(t, 5*time.Minute, tokenRequest.TTL)

	session, active := f.Presence.Active(communityID, channelID, userID)
	require.True(t, active)
	assert.True(t, session.PublishingAllowed)
	assert.False(t, session.CameraEnabled)

	// The reservation is completed, so it no longer holds a seat.
	assert.Equal(t, 1, f.Seats.Completes)
	assert.Equal(t, 0, f.Seats.LiveReservations())
	assert.Equal(t, 1, f.Presence.Len())

	accessRequest, ok := f.Access.LastRequest()
	require.True(t, ok)
	assert.Equal(t, "trace-1", accessRequest.TraceID)
	assert.Equal(t, userID, accessRequest.UserID)

	assert.Equal(t, float64(1), metricValue(t, f.Registry, "voice_joins_total", map[string]string{"result": "joined"}))
}

// TestJoinMutedMemberIsListenOnly covers US-003: an active mute grants join and
// subscribe but no publishing, and the response carries the mute deadline.
func TestJoinMutedMemberIsListenOnly(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	mutedUntil := f.Now().Add(30 * time.Minute)
	check := allowedCheck(communityID, channelID, 3)
	check.MutedUntil = &mutedUntil
	f.Access.Result = check

	joiner := service.NewJoinService(f.Collaborators())
	response, err := joiner.Join(context.Background(), service.JoinInput{
		CommunityID: communityID,
		ChannelID:   channelID,
		UserID:      userID,
	})
	require.NoError(t, err)

	assert.False(t, response.CanPublishAudio)
	assert.False(t, response.CanPublishVideo)
	assert.False(t, response.CanPublishScreen)
	require.NotNil(t, response.MutedUntil)
	assert.WithinDuration(t, mutedUntil, *response.MutedUntil, time.Second)

	tokenRequest, ok := f.Tokens.Last()
	require.True(t, ok)
	assert.False(t, tokenRequest.CanPublish)

	session, active := f.Presence.Active(communityID, channelID, userID)
	require.True(t, active)
	assert.False(t, session.PublishingAllowed)
}

// TestJoinExpiredMutePublishesAgain covers the "mute expired" rule: publishing
// returns at the next join once the mute deadline is in the past.
func TestJoinExpiredMutePublishesAgain(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	past := f.Now().Add(-time.Minute)
	check := allowedCheck(communityID, channelID, 3)
	check.MutedUntil = &past
	f.Access.Result = check

	response, err := service.NewJoinService(f.Collaborators()).Join(context.Background(), service.JoinInput{
		CommunityID: communityID,
		ChannelID:   channelID,
		UserID:      userID,
	})
	require.NoError(t, err)
	assert.True(t, response.CanPublishAudio)
	assert.Nil(t, response.MutedUntil)
}

// TestJoinDenials covers US-004 and the documented 403/404 mapping.
func TestJoinDenials(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		reason   string
		expected error
	}{
		{name: "not a member", reason: "not_a_member", expected: models.ErrNotAMember},
		{name: "banned", reason: "banned", expected: models.ErrNotAMember},
		{name: "unknown channel", reason: "channel_not_found", expected: models.ErrChannelNotFound},
		{name: "text channel", reason: "not_a_voice_channel", expected: models.ErrNotAVoiceChannel},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			f := mocks.NewFixture()
			communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
			f.Access.Result = &community.AccessCheckResult{
				CommunityID:  communityID,
				Allowed:      false,
				DenialReason: testCase.reason,
			}

			_, err := service.NewJoinService(f.Collaborators()).Join(context.Background(), service.JoinInput{
				CommunityID: communityID,
				ChannelID:   channelID,
				UserID:      userID,
			})
			require.ErrorIs(t, err, testCase.expected)

			// No seat, no token, no room, no presence.
			assert.Equal(t, 0, f.Seats.LiveReservations())
			assert.Empty(t, f.Tokens.Requests)
			assert.Equal(t, 0, f.LiveKit.CreatedCount())
			assert.Equal(t, 0, f.Presence.Len())
			assert.Equal(t, float64(1), metricValue(t, f.Registry, "voice_joins_total", map[string]string{"result": "forbidden"}))
		})
	}
}

// TestJoinFailsClosedWhenAuthorizationIsUnavailable covers the fail-closed
// rule: no seat is reserved and no token is issued.
func TestJoinFailsClosedWhenAuthorizationIsUnavailable(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	f.Access.Err = community.ErrUnavailable

	_, err := service.NewJoinService(f.Collaborators()).Join(context.Background(), service.JoinInput{
		CommunityID: uuid.New(),
		ChannelID:   uuid.New(),
		UserID:      uuid.New(),
	})
	require.ErrorIs(t, err, models.ErrAuthorizationUnavailable)

	assert.Equal(t, 0, f.Seats.LiveReservations())
	assert.Zero(t, f.Seats.Completes)
	assert.Equal(t, 0, f.LiveKit.CreatedCount())
	assert.Empty(t, f.Tokens.Requests)
	assert.Equal(t, 0, f.Presence.Len())
	assert.Equal(t, 0, f.Rooms.Len())
}

// TestJoinUnknownCommunityMapsToNotFound covers API-002 answering 404.
func TestJoinUnknownCommunityMapsToNotFound(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	f.Access.Err = community.ErrCommunityNotFound

	_, err := service.NewJoinService(f.Collaborators()).Join(context.Background(), service.JoinInput{
		CommunityID: uuid.New(),
		ChannelID:   uuid.New(),
		UserID:      uuid.New(),
	})
	require.ErrorIs(t, err, models.ErrChannelNotFound)
}

// TestJoinFullChannelOffersAlternatives covers US-002.
func TestJoinFullChannelOffersAlternatives(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	freeChannelID, fullChannelID := uuid.New(), uuid.New()

	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 1})
	f.Rooms.Seed(models.VoiceRoom{ChannelID: freeChannelID, CommunityID: communityID, Capacity: 4})
	f.Rooms.Seed(models.VoiceRoom{ChannelID: fullChannelID, CommunityID: communityID, Capacity: 2})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: uuid.New()})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: fullChannelID, UserID: uuid.New()})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: fullChannelID, UserID: uuid.New()})

	f.Access.Result = allowedCheck(communityID, channelID, 1,
		models.ChannelSummary{ChannelID: channelID, Name: "General", Capacity: 1},
		models.ChannelSummary{ChannelID: freeChannelID, Name: "Chill", Capacity: 4},
		models.ChannelSummary{ChannelID: fullChannelID, Name: "Music", Capacity: 2},
	)

	_, err := service.NewJoinService(f.Collaborators()).Join(context.Background(), service.JoinInput{
		CommunityID: communityID,
		ChannelID:   channelID,
		UserID:      userID,
	})
	var full *models.ChannelFullError
	require.ErrorAs(t, err, &full)
	require.Len(t, full.Alternatives, 1)
	assert.Equal(t, freeChannelID, full.Alternatives[0].ChannelID)
	assert.Equal(t, "Chill", full.Alternatives[0].Name)

	// No seat is taken and existing presence is unchanged.
	assert.Equal(t, 0, f.Seats.LiveReservations())
	assert.Equal(t, 3, f.Presence.Len())
	assert.Equal(t, float64(1), metricValue(t, f.Registry, "voice_joins_total", map[string]string{"result": "full"}))
}

// TestJoinFullOnlyChannelHasEmptyAlternatives covers the single-channel case.
func TestJoinFullOnlyChannelHasEmptyAlternatives(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 1})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: uuid.New()})
	f.Access.Result = allowedCheck(communityID, channelID, 1)

	_, err := service.NewJoinService(f.Collaborators()).Join(context.Background(), service.JoinInput{
		CommunityID: communityID,
		ChannelID:   channelID,
		UserID:      userID,
	})
	var full *models.ChannelFullError
	require.ErrorAs(t, err, &full)
	assert.Empty(t, full.Alternatives)
}

// TestJoinFullChannelSurvivesAlternativeLookupFailure ensures a repository error
// while computing alternatives never turns the 409 into a 500.
func TestJoinFullChannelSurvivesAlternativeLookupFailure(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	otherChannelID := uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 1})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: uuid.New()})
	f.Access.Result = allowedCheck(communityID, channelID, 1,
		models.ChannelSummary{ChannelID: channelID, Name: "General", Capacity: 1},
		models.ChannelSummary{ChannelID: otherChannelID, Name: "Chill", Capacity: 4},
	)

	joiner := service.NewJoinService(f.Collaborators())
	// A full channel is detected first; the occupancy lookup for alternatives
	// then fails.
	f.Seats.CountErr = errors.New("database down")
	_, err := joiner.Join(context.Background(), service.JoinInput{
		CommunityID: communityID,
		ChannelID:   channelID,
		UserID:      userID,
	})
	var full *models.ChannelFullError
	require.ErrorAs(t, err, &full)
	assert.Empty(t, full.Alternatives)
}

// TestJoinReservationTTLExpiry covers the TTL rule: an abandoned reservation
// stops holding the seat once it expires.
func TestJoinReservationTTLExpiry(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID := uuid.New(), uuid.New()
	abandonedUser, joiningUser := uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 1})
	f.Access.Result = allowedCheck(communityID, channelID, 1)

	// A client requested a seat and never connected.
	_, err := f.Seats.ReserveSeat(context.Background(), models.SeatReservationRequest{
		CommunityID: communityID,
		ChannelID:   channelID,
		UserID:      abandonedUser,
		TTL:         60 * time.Second,
	})
	require.NoError(t, err)

	joiner := service.NewJoinService(f.Collaborators())
	_, err = joiner.Join(context.Background(), service.JoinInput{
		CommunityID: communityID,
		ChannelID:   channelID,
		UserID:      joiningUser,
	})
	var full *models.ChannelFullError
	require.ErrorAs(t, err, &full, "the abandoned reservation must hold the seat until it expires")

	f.Advance(61 * time.Second)
	response, err := joiner.Join(context.Background(), service.JoinInput{
		CommunityID: communityID,
		ChannelID:   channelID,
		UserID:      joiningUser,
	})
	require.NoError(t, err, "an expired reservation must free the seat")
	assert.NotEmpty(t, response.Token)
}

// TestJoinRollsBackSeatWhenLiveKitFails covers the rollback rule for both
// LiveKit failures of the join flow.
func TestJoinRollsBackSeatWhenLiveKitFails(t *testing.T) {
	t.Parallel()

	t.Run("room creation fails", func(t *testing.T) {
		t.Parallel()
		f := mocks.NewFixture()
		f.LiveKit.FailCreate = errors.New("livekit unavailable")
		assertJoinRollsBack(t, f)
	})
	t.Run("token minting fails", func(t *testing.T) {
		t.Parallel()
		f := mocks.NewFixture()
		f.Tokens.Fail = errors.New("signing failed")
		assertJoinRollsBack(t, f)
	})
}

// assertJoinRollsBack checks that a failed join left no seat, token, or
// presence behind.
func assertJoinRollsBack(t *testing.T, f *mocks.Fixture) {
	t.Helper()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Access.Result = allowedCheck(communityID, channelID, 4)

	_, err := service.NewJoinService(f.Collaborators()).Join(context.Background(), service.JoinInput{
		CommunityID: communityID,
		ChannelID:   channelID,
		UserID:      userID,
	})
	require.ErrorIs(t, err, models.ErrMediaUnavailable)

	assert.Equal(t, 0, f.Seats.LiveReservations(), "the reservation must be released")
	assert.Equal(t, 1, f.Seats.Releases)
	assert.Zero(t, f.Seats.Completes)
	assert.Equal(t, 0, f.Presence.Len())
}

// TestJoinPresenceFailureIsAnInternalError covers a failing presence write.
func TestJoinPresenceFailureIsAnInternalError(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Access.Result = allowedCheck(communityID, channelID, 4)
	f.Presence.UpsertErr = errors.New("database down")

	_, err := service.NewJoinService(f.Collaborators()).Join(context.Background(), service.JoinInput{
		CommunityID: communityID,
		ChannelID:   channelID,
		UserID:      userID,
	})
	require.Error(t, err)
	assert.NotErrorIs(t, err, models.ErrMediaUnavailable)
	assert.Equal(t, 1, f.Seats.Releases, "the seat must be released when presence cannot be recorded")
}

// TestJoinRoomCapacityUpdateKeepsRoomName ensures a capacity change refreshes
// the stored room without renaming the LiveKit room.
func TestJoinRoomCapacityUpdateKeepsRoomName(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{
		ChannelID:       channelID,
		CommunityID:     communityID,
		LiveKitRoomName: service.RoomName(channelID),
		Capacity:        2,
	})
	f.Access.Result = allowedCheck(communityID, channelID, 6)

	_, err := service.NewJoinService(f.Collaborators()).Join(context.Background(), service.JoinInput{
		CommunityID: communityID,
		ChannelID:   channelID,
		UserID:      userID,
	})
	require.NoError(t, err)
	room, err := f.Rooms.GetRoom(context.Background(), channelID)
	require.NoError(t, err)
	assert.Equal(t, 6, room.Capacity)
	assert.Equal(t, service.RoomName(channelID), room.LiveKitRoomName)
	assert.Equal(t, 6, f.LiveKit.MaxParticipants[service.RoomName(channelID)])
}

// TestJoinReconnectDoesNotConsumeASecondSeat covers rejoin idempotency.
func TestJoinReconnectDoesNotConsumeASecondSeat(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	f.Access.Result = allowedCheck(communityID, channelID, 1)
	joiner := service.NewJoinService(f.Collaborators())

	_, err := joiner.Join(context.Background(), service.JoinInput{CommunityID: communityID, ChannelID: channelID, UserID: userID})
	require.NoError(t, err)
	_, err = joiner.Join(context.Background(), service.JoinInput{CommunityID: communityID, ChannelID: channelID, UserID: userID})
	require.NoError(t, err, "a reconnecting participant keeps their seat")

	assert.Equal(t, 1, f.Presence.Len())
	assert.Equal(t, 2, f.Seats.Completes)
}

// TestJoinLastSeatRace covers the concurrent join of the last seat: the
// serialized transaction admits exactly one participant.
func TestJoinLastSeatRace(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	communityID, channelID := uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 1})
	f.Access.Respond = func(request community.AccessCheckRequest) (*community.AccessCheckResult, error) {
		return allowedCheck(request.CommunityID, request.ChannelID, 1), nil
	}

	joiner := service.NewJoinService(f.Collaborators())
	const contenders = 8
	var (
		waitGroup sync.WaitGroup
		mutex     sync.Mutex
		joined    int
		full      int
		failures  []error
	)
	for i := 0; i < contenders; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			_, err := joiner.Join(context.Background(), service.JoinInput{
				CommunityID: communityID,
				ChannelID:   channelID,
				UserID:      uuid.New(),
			})
			var fullErr *models.ChannelFullError
			mutex.Lock()
			defer mutex.Unlock()
			switch {
			case err == nil:
				joined++
			case errors.As(err, &fullErr):
				full++
			default:
				failures = append(failures, err)
			}
		}()
	}
	waitGroup.Wait()

	require.Empty(t, failures)
	assert.Equal(t, 1, joined, "exactly one contender wins the last seat")
	assert.Equal(t, contenders-1, full)
	assert.Equal(t, 1, f.Presence.Len())
	assert.Equal(t, 0, f.Seats.LiveReservations())
}

// TestJoinRequiresIdentifiers covers boundary validation.
func TestJoinRequiresIdentifiers(t *testing.T) {
	t.Parallel()
	f := mocks.NewFixture()
	_, err := service.NewJoinService(f.Collaborators()).Join(context.Background(), service.JoinInput{})
	require.ErrorIs(t, err, models.ErrInvalidRequest)
}
