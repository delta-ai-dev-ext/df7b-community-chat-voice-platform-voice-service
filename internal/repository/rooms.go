package repository

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"voice_service/internal/models"
)

// Rooms is the pgx implementation of RoomRepository.
type Rooms struct {
	q Querier
}

// NewRooms builds the room repository.
func NewRooms(q Querier) *Rooms { return &Rooms{q: q} }

// GetRoom returns the room of a channel.
func (r *Rooms) GetRoom(ctx context.Context, channelID uuid.UUID) (*models.VoiceRoom, error) {
	row := r.q.QueryRow(ctx, "SELECT "+roomColumns+" FROM voice_rooms WHERE channel_id = $1", channelID)
	return scanRoom(row)
}

// GetRoomByLiveKitName resolves a room by its LiveKit name.
func (r *Rooms) GetRoomByLiveKitName(ctx context.Context, roomName string) (*models.VoiceRoom, error) {
	row := r.q.QueryRow(ctx, "SELECT "+roomColumns+" FROM voice_rooms WHERE livekit_room_name = $1", roomName)
	return scanRoom(row)
}

// UpsertRoom stores or refreshes the room. The LiveKit room name is immutable
// once written so an existing room is never renamed under a live call.
func (r *Rooms) UpsertRoom(ctx context.Context, room *models.VoiceRoom) error {
	if room == nil {
		return fmt.Errorf("repository: nil room")
	}
	_, err := r.q.Exec(ctx, `
		INSERT INTO voice_rooms (channel_id, community_id, livekit_room_name, capacity, created_at, updated_at)
		VALUES ($1, $2, $3, $4, now(), now())
		ON CONFLICT (channel_id) DO UPDATE
		SET community_id = EXCLUDED.community_id,
		    capacity = EXCLUDED.capacity,
		    updated_at = now()`,
		room.ChannelID, room.CommunityID, room.LiveKitRoomName, room.Capacity)
	if err != nil {
		return fmt.Errorf("repository: upsert room: %w", err)
	}
	return nil
}

// UpdateRoomCapacity updates the stored capacity of a known room.
func (r *Rooms) UpdateRoomCapacity(ctx context.Context, channelID uuid.UUID, capacity int) error {
	tag, err := r.q.Exec(ctx,
		"UPDATE voice_rooms SET capacity = $2, updated_at = now() WHERE channel_id = $1",
		channelID, capacity)
	if err != nil {
		return fmt.Errorf("repository: update room capacity: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ListRoomsByCommunity lists the rooms of a community.
func (r *Rooms) ListRoomsByCommunity(ctx context.Context, communityID uuid.UUID) ([]models.VoiceRoom, error) {
	rows, err := r.q.Query(ctx,
		"SELECT "+roomColumns+" FROM voice_rooms WHERE community_id = $1 ORDER BY channel_id", communityID)
	if err != nil {
		return nil, fmt.Errorf("repository: list rooms: %w", err)
	}
	return collectRooms(rows)
}

// ListRoomsWithPresence lists the rooms that currently hold participants; they
// are the rooms the reconciliation sweep has to compare with LiveKit.
func (r *Rooms) ListRoomsWithPresence(ctx context.Context) ([]models.VoiceRoom, error) {
	rows, err := r.q.Query(ctx, `
		SELECT DISTINCT r.channel_id, r.community_id, r.livekit_room_name, r.capacity, r.created_at, r.updated_at
		FROM voice_rooms r
		JOIN voice_participants p ON p.channel_id = r.channel_id
		WHERE p.left_at IS NULL
		ORDER BY r.channel_id`)
	if err != nil {
		return nil, fmt.Errorf("repository: list rooms with presence: %w", err)
	}
	return collectRooms(rows)
}

// collectRooms drains room rows into a slice.
func collectRooms(rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}) ([]models.VoiceRoom, error) {
	defer rows.Close()
	rooms := make([]models.VoiceRoom, 0, 8)
	for rows.Next() {
		room, err := scanRoom(rows)
		if err != nil {
			return nil, err
		}
		rooms = append(rooms, *room)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: read room rows: %w", err)
	}
	return rooms, nil
}
