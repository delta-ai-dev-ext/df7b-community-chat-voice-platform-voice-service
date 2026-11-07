package unit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"voice_service/internal/models"
	"voice_service/internal/repository"
)

// roomValues builds a voice_rooms row in scan order.
func roomValues(channelID, communityID uuid.UUID) []any {
	return []any{channelID, communityID, "voice-" + channelID.String(), 4, sampleTime, sampleTime}
}

// presenceValues builds a voice_participants row in scan order.
func presenceValues(communityID, channelID, userID uuid.UUID) []any {
	return []any{communityID, channelID, userID, sampleTime, nil, true, false, true, sampleTime}
}

// Exec tag helpers.
var (
	oneRow  = pgconn.NewCommandTag("UPDATE 1")
	noRows  = pgconn.NewCommandTag("UPDATE 0")
	twoRows = pgconn.NewCommandTag("DELETE 2")
)

// TestRoomsGetRoom covers the room lookup and its not-found mapping.
func TestRoomsGetRoom(t *testing.T) {
	t.Parallel()
	channelID, communityID := uuid.New(), uuid.New()
	querier := &fakeQuerier{results: []pgResult{{values: roomValues(channelID, communityID)}}}
	room, err := repository.NewRooms(querier).GetRoom(context.Background(), channelID)
	require.NoError(t, err)
	assert.Equal(t, channelID, room.ChannelID)
	assert.Equal(t, 4, room.Capacity)
	assert.Equal(t, "voice-"+channelID.String(), room.LiveKitRoomName)

	missing := &fakeQuerier{results: []pgResult{{err: pgx.ErrNoRows}}}
	_, err = repository.NewRooms(missing).GetRoom(context.Background(), channelID)
	require.ErrorIs(t, err, repository.ErrNotFound)

	broken := &fakeQuerier{results: []pgResult{{err: errors.New("connection reset")}}}
	_, err = repository.NewRooms(broken).GetRoom(context.Background(), channelID)
	require.Error(t, err)
}

// TestRoomsGetRoomByLiveKitName covers the webhook room lookup.
func TestRoomsGetRoomByLiveKitName(t *testing.T) {
	t.Parallel()
	channelID, communityID := uuid.New(), uuid.New()
	querier := &fakeQuerier{results: []pgResult{{values: roomValues(channelID, communityID)}}}
	room, err := repository.NewRooms(querier).GetRoomByLiveKitName(context.Background(), "voice-1")
	require.NoError(t, err)
	assert.Equal(t, channelID, room.ChannelID)

	missing := &fakeQuerier{results: []pgResult{{err: pgx.ErrNoRows}}}
	_, err = repository.NewRooms(missing).GetRoomByLiveKitName(context.Background(), "voice-1")
	require.ErrorIs(t, err, repository.ErrNotFound)
}

// TestRoomsUpsertAndCapacity covers the write paths.
func TestRoomsUpsertAndCapacity(t *testing.T) {
	t.Parallel()
	channelID, communityID := uuid.New(), uuid.New()
	querier := &fakeQuerier{results: []pgResult{{tag: oneRow}, {tag: oneRow}, {tag: noRows}}}
	rooms := repository.NewRooms(querier)

	room := models.VoiceRoom{
		ChannelID:       channelID,
		CommunityID:     communityID,
		LiveKitRoomName: "voice-" + channelID.String(),
		Capacity:        4,
	}
	require.NoError(t, rooms.UpsertRoom(context.Background(), &room))
	require.Error(t, rooms.UpsertRoom(context.Background(), nil))
	require.NoError(t, rooms.UpdateRoomCapacity(context.Background(), channelID, 6))
	require.ErrorIs(t, rooms.UpdateRoomCapacity(context.Background(), channelID, 6), repository.ErrNotFound)
}

// TestRoomsLists covers the list queries and their error paths.
func TestRoomsLists(t *testing.T) {
	t.Parallel()
	channelID, communityID := uuid.New(), uuid.New()
	querier := &fakeQuerier{results: []pgResult{
		{rows: [][]any{roomValues(channelID, communityID), roomValues(uuid.New(), communityID)}},
		{rows: [][]any{roomValues(channelID, communityID)}},
		{err: errors.New("boom")},
	}}
	rooms := repository.NewRooms(querier)

	byCommunity, err := rooms.ListRoomsByCommunity(context.Background(), communityID)
	require.NoError(t, err)
	assert.Len(t, byCommunity, 2)

	withPresence, err := rooms.ListRoomsWithPresence(context.Background())
	require.NoError(t, err)
	require.Len(t, withPresence, 1)

	_, err = rooms.ListRoomsWithPresence(context.Background())
	require.Error(t, err)
}

// TestSeatsReserveSeat covers the transactional reservation: room lock, expiry
// sweep, capacity check, and the upserting insert.
func TestSeatsReserveSeat(t *testing.T) {
	t.Parallel()
	channelID, communityID, userID := uuid.New(), uuid.New(), uuid.New()
	reservationID := uuid.New()
	querier := &fakeQuerier{results: []pgResult{
		{values: []any{4}},     // SELECT capacity ... FOR UPDATE
		{tag: oneRow},          // sweep expired reservations
		{values: []any{false}}, // holdsSeat
		{values: []any{1}},     // occupied
		{values: []any{reservationID, communityID, channelID, userID, sampleTime, sampleTime.Add(time.Minute)}},
	}}
	reservation, err := repository.NewSeats(querier).ReserveSeat(context.Background(), seatRequest(communityID, channelID, userID))
	require.NoError(t, err)
	assert.Equal(t, reservationID, reservation.ReservationID)
	assert.Equal(t, communityID, reservation.CommunityID)
}

// seatRequest builds a reservation request.
func seatRequest(communityID, channelID, userID uuid.UUID) models.SeatReservationRequest {
	return models.SeatReservationRequest{
		CommunityID: communityID,
		ChannelID:   channelID,
		UserID:      userID,
		TTL:         60 * time.Second,
	}
}

// TestSeatsReserveSeatFull covers the ErrChannelFull mapping.
func TestSeatsReserveSeatFull(t *testing.T) {
	t.Parallel()
	channelID, communityID, userID := uuid.New(), uuid.New(), uuid.New()
	querier := &fakeQuerier{results: []pgResult{
		{values: []any{1}},
		{tag: oneRow},
		{values: []any{false}},
		{values: []any{1}},
	}}
	_, err := repository.NewSeats(querier).ReserveSeat(context.Background(), seatRequest(communityID, channelID, userID))
	require.ErrorIs(t, err, repository.ErrChannelFull)
}

// TestSeatsReserveSeatReusesTheCallersSeat covers the reconnect path: an
// existing seat skips the capacity query.
func TestSeatsReserveSeatReusesTheCallersSeat(t *testing.T) {
	t.Parallel()
	channelID, communityID, userID := uuid.New(), uuid.New(), uuid.New()
	querier := &fakeQuerier{results: []pgResult{
		{values: []any{2}},
		{tag: oneRow},
		{values: []any{true}},
		{values: []any{uuid.New(), communityID, channelID, userID, sampleTime, sampleTime}},
	}}
	_, err := repository.NewSeats(querier).ReserveSeat(context.Background(), seatRequest(communityID, channelID, userID))
	require.NoError(t, err)
	assert.Len(t, querier.queries, 4, "the capacity query is skipped when the caller already holds a seat")
}

// TestSeatsReserveSeatFailures covers the guards and error paths.
func TestSeatsReserveSeatFailures(t *testing.T) {
	t.Parallel()
	channelID, communityID, userID := uuid.New(), uuid.New(), uuid.New()
	seats := repository.NewSeats(&fakeQuerier{})

	_, err := seats.ReserveSeat(context.Background(), models.SeatReservationRequest{
		CommunityID: communityID,
		ChannelID:   channelID,
		UserID:      userID,
	})
	require.Error(t, err, "a non-positive TTL is rejected")

	unknownRoom := &fakeQuerier{results: []pgResult{{err: pgx.ErrNoRows}}}
	_, err = repository.NewSeats(unknownRoom).ReserveSeat(context.Background(), seatRequest(communityID, channelID, userID))
	require.ErrorIs(t, err, repository.ErrNotFound)

	beginFailure := &fakeQuerier{beginErr: errors.New("pool exhausted")}
	_, err = repository.NewSeats(beginFailure).ReserveSeat(context.Background(), seatRequest(communityID, channelID, userID))
	require.Error(t, err)

	commitFailure := &fakeQuerier{commitErr: errors.New("commit failed"), results: []pgResult{
		{values: []any{4}}, {tag: oneRow}, {values: []any{true}},
		{values: []any{uuid.New(), communityID, channelID, userID, sampleTime, sampleTime}},
	}}
	_, err = repository.NewSeats(commitFailure).ReserveSeat(context.Background(), seatRequest(communityID, channelID, userID))
	require.Error(t, err)
}

// TestSeatsCompleteSeat covers the atomic "release reservation and record
// presence" step.
func TestSeatsCompleteSeat(t *testing.T) {
	t.Parallel()
	channelID, communityID, userID := uuid.New(), uuid.New(), uuid.New()
	reservationID := uuid.New()
	querier := &fakeQuerier{results: []pgResult{
		{values: []any{communityID, channelID, userID}},
		{tag: oneRow},
		{values: presenceValues(communityID, channelID, userID)},
	}}
	presence := models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID}
	completed, err := repository.NewSeats(querier).CompleteSeat(context.Background(), reservationID, &presence)
	require.NoError(t, err)
	assert.Equal(t, userID, completed.UserID)

	missing := &fakeQuerier{results: []pgResult{{err: pgx.ErrNoRows}}}
	_, err = repository.NewSeats(missing).CompleteSeat(context.Background(), reservationID, &presence)
	require.ErrorIs(t, err, repository.ErrNotFound)

	mismatch := &fakeQuerier{results: []pgResult{{values: []any{communityID, channelID, uuid.New()}}}}
	_, err = repository.NewSeats(mismatch).CompleteSeat(context.Background(), reservationID, &presence)
	require.Error(t, err, "a reservation of another participant is rejected")

	_, err = repository.NewSeats(&fakeQuerier{}).CompleteSeat(context.Background(), reservationID, nil)
	require.Error(t, err)
}

// TestSeatsReleaseAndCount covers the release helpers and occupancy counting.
func TestSeatsReleaseAndCount(t *testing.T) {
	t.Parallel()
	channelID, userID := uuid.New(), uuid.New()
	otherChannel := uuid.New()
	querier := &fakeQuerier{results: []pgResult{
		{tag: oneRow},
		{tag: oneRow},
		{rows: [][]any{{channelID, 2}, {otherChannel, 0}}},
	}}
	seats := repository.NewSeats(querier)

	require.NoError(t, seats.ReleaseSeat(context.Background(), uuid.New()))
	require.NoError(t, seats.ReleaseUserSeats(context.Background(), channelID, userID))

	occupied, err := seats.CountOccupied(context.Background(), []uuid.UUID{channelID, otherChannel})
	require.NoError(t, err)
	assert.Equal(t, 2, occupied[channelID])
	assert.Equal(t, 0, occupied[otherChannel])

	empty, err := seats.CountOccupied(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

// TestPresenceWrites covers the presence write paths.
func TestPresenceWrites(t *testing.T) {
	t.Parallel()
	channelID, communityID, userID := uuid.New(), uuid.New(), uuid.New()
	querier := &fakeQuerier{results: []pgResult{
		{values: presenceValues(communityID, channelID, userID)}, // UpsertActive
		{tag: oneRow}, // MarkLeft active
		{tag: noRows}, // MarkLeft no-op
		{rows: [][]any{presenceValues(communityID, channelID, userID)}}, // MarkChannelLeft
		{tag: oneRow}, // SetMedia active
		{tag: noRows}, // SetMedia inactive
	}}
	presence := repository.NewPresence(querier)

	stored, err := presence.UpsertActive(context.Background(), &models.Presence{
		CommunityID: communityID, ChannelID: channelID, UserID: userID,
	})
	require.NoError(t, err)
	assert.Equal(t, userID, stored.UserID)

	_, err = presence.UpsertActive(context.Background(), nil)
	require.Error(t, err)

	wasActive, err := presence.MarkLeft(context.Background(), communityID, channelID, userID, sampleTime)
	require.NoError(t, err)
	assert.True(t, wasActive)

	wasActive, err = presence.MarkLeft(context.Background(), communityID, channelID, userID, sampleTime)
	require.NoError(t, err)
	assert.False(t, wasActive)

	left, err := presence.MarkChannelLeft(context.Background(), communityID, channelID, sampleTime)
	require.NoError(t, err)
	assert.Len(t, left, 1)

	updated, err := presence.SetMedia(context.Background(), communityID, channelID, userID, true, true, true, sampleTime)
	require.NoError(t, err)
	assert.True(t, updated)

	updated, err = presence.SetMedia(context.Background(), communityID, channelID, userID, true, true, true, sampleTime)
	require.NoError(t, err)
	assert.False(t, updated)
}

// TestPresenceReads covers the presence queries.
func TestPresenceReads(t *testing.T) {
	t.Parallel()
	channelID, communityID, userID := uuid.New(), uuid.New(), uuid.New()
	row := presenceValues(communityID, channelID, userID)
	querier := &fakeQuerier{results: []pgResult{
		{rows: [][]any{row}},
		{rows: [][]any{row}},
		{rows: [][]any{row}},
		{values: []any{3}},
		{values: []any{3, 2}},
		{err: errors.New("boom")},
	}}
	presence := repository.NewPresence(querier)

	byCommunity, err := presence.ListActiveByCommunity(context.Background(), communityID)
	require.NoError(t, err)
	assert.Len(t, byCommunity, 1)

	byChannel, err := presence.ListActiveByChannel(context.Background(), communityID, channelID)
	require.NoError(t, err)
	assert.Len(t, byChannel, 1)

	byUser, err := presence.ListActiveByUser(context.Background(), communityID, userID)
	require.NoError(t, err)
	assert.Len(t, byUser, 1)

	count, err := presence.CountActive(context.Background(), communityID, channelID)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	participants, rooms, err := presence.CountTotal(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, participants)
	assert.Equal(t, 2, rooms)

	_, err = presence.ListActiveByCommunity(context.Background(), communityID)
	require.Error(t, err)
}

// TestProjectionsWrites covers the version-guarded upserts.
func TestProjectionsWrites(t *testing.T) {
	t.Parallel()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	querier := &fakeQuerier{results: []pgResult{
		{values: []any{int64(2)}}, // UpsertMember applied
		{err: pgx.ErrNoRows},      // UpsertMember stale
		{values: []any{int64(3)}}, // UpsertChannel applied
		{err: pgx.ErrNoRows},      // UpsertChannel stale
	}}
	projections := repository.NewProjections(querier)

	member := func(version int64) models.MemberProjection {
		return models.MemberProjection{
			CommunityID:   communityID,
			UserID:        userID,
			Status:        models.MemberStatusActive,
			MemberVersion: version,
			UpdatedAt:     sampleTime,
		}
	}
	applied, err := projections.UpsertMember(context.Background(), member(2))
	require.NoError(t, err)
	assert.True(t, applied)

	applied, err = projections.UpsertMember(context.Background(), member(1))
	require.NoError(t, err)
	assert.False(t, applied, "an older member_version is ignored")

	capacity := 4
	channel := func(version int64) models.ChannelProjection {
		return models.ChannelProjection{
			CommunityID:    communityID,
			ChannelID:      channelID,
			Kind:           models.ChannelKindVoice,
			Name:           "General",
			Capacity:       &capacity,
			ChannelVersion: version,
			UpdatedAt:      sampleTime,
		}
	}
	applied, err = projections.UpsertChannel(context.Background(), channel(3))
	require.NoError(t, err)
	assert.True(t, applied)

	applied, err = projections.UpsertChannel(context.Background(), channel(1))
	require.NoError(t, err)
	assert.False(t, applied, "an older channel_version is ignored")
}

// TestProjectionsReads covers the projection reads.
func TestProjectionsReads(t *testing.T) {
	t.Parallel()
	communityID, channelID, userID := uuid.New(), uuid.New(), uuid.New()
	mutedUntil := sampleTime.Add(time.Hour)
	querier := &fakeQuerier{results: []pgResult{
		{values: []any{communityID, userID, "active", false, &mutedUntil, int64(4), sampleTime}},
		{values: []any{communityID, channelID, "voice", "General", 4, false, int64(4), sampleTime}},
		{rows: [][]any{{communityID, channelID, "voice", "General", 4, false, int64(4), sampleTime}}},
		{err: pgx.ErrNoRows},
		{err: errors.New("boom")},
	}}
	projections := repository.NewProjections(querier)

	member, err := projections.GetMember(context.Background(), communityID, userID)
	require.NoError(t, err)
	assert.Equal(t, "active", member.Status)
	require.NotNil(t, member.MutedUntil)

	channel, err := projections.GetChannel(context.Background(), communityID, channelID)
	require.NoError(t, err)
	assert.True(t, channel.IsVoice())

	channels, err := projections.ListVoiceChannels(context.Background(), communityID)
	require.NoError(t, err)
	assert.Len(t, channels, 1)

	_, err = projections.GetMember(context.Background(), communityID, userID)
	require.ErrorIs(t, err, repository.ErrNotFound)

	_, err = projections.ListVoiceChannels(context.Background(), communityID)
	require.Error(t, err)
}

// TestDedupe covers the dedupe repository.
func TestDedupe(t *testing.T) {
	t.Parallel()
	eventID := uuid.New()
	querier := &fakeQuerier{results: []pgResult{
		{values: []any{eventID}}, // first delivery
		{err: pgx.ErrNoRows},     // duplicate
		{values: []any{"evt-1"}}, // first webhook delivery
		{err: pgx.ErrNoRows},     // duplicate webhook
		{tag: oneRow},            // forget event
		{tag: oneRow},            // forget webhook
		{tag: twoRows},           // purge
		{err: errors.New("boom")},
	}}
	dedupe := repository.NewDedupe(querier)

	first, err := dedupe.MarkEventProcessed(context.Background(), eventID, "community_events")
	require.NoError(t, err)
	assert.True(t, first)

	first, err = dedupe.MarkEventProcessed(context.Background(), eventID, "community_events")
	require.NoError(t, err)
	assert.False(t, first)

	firstWebhook, err := dedupe.MarkWebhookProcessed(context.Background(), "evt-1", "participant_joined")
	require.NoError(t, err)
	assert.True(t, firstWebhook)

	firstWebhook, err = dedupe.MarkWebhookProcessed(context.Background(), "evt-1", "participant_joined")
	require.NoError(t, err)
	assert.False(t, firstWebhook)

	require.NoError(t, dedupe.ForgetEvent(context.Background(), eventID))
	require.NoError(t, dedupe.ForgetWebhook(context.Background(), "evt-1"))

	removed, err := dedupe.PurgeProcessedEvents(context.Background(), sampleTime)
	require.NoError(t, err)
	assert.Equal(t, int64(2), removed)

	_, err = dedupe.PurgeProcessedEvents(context.Background(), sampleTime)
	require.Error(t, err)
}

// TestRepositoryPingAndPool covers the connection helpers.
func TestRepositoryPingAndPool(t *testing.T) {
	t.Parallel()
	require.Error(t, repository.Ping(context.Background(), nil))
	_, err := repository.NewPool(context.Background(), "://not-a-url")
	require.Error(t, err)
}

// TestRepositoryNotFoundMapping covers the shared scan helpers.
func TestRepositoryNotFoundMapping(t *testing.T) {
	t.Parallel()
	empty := &fakeQuerier{results: []pgResult{{rows: nil}}}
	presence, err := repository.NewPresence(empty).ListActiveByCommunity(context.Background(), uuid.New())
	require.NoError(t, err, "an empty list is not an error")
	assert.Empty(t, presence)

	broken := &fakeQuerier{results: []pgResult{{rows: [][]any{{uuid.New()}}}}}
	_, err = repository.NewPresence(broken).ListActiveByCommunity(context.Background(), uuid.New())
	require.Error(t, err, "a column mismatch surfaces as a scan error")
}
