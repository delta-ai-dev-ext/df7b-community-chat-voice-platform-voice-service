// Package repository holds every PostgreSQL access of voice_service behind
// small interfaces so the service layer never touches SQL or driver types.
package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"voice_service/internal/models"
)

// Repository errors.
var (
	// ErrNotFound reports a missing row.
	ErrNotFound = errors.New("repository: not found")
	// ErrChannelFull reports that no seat is available in the channel.
	ErrChannelFull = errors.New("repository: channel full")
)

// Querier is the subset of *pgxpool.Pool used by the repositories.
type Querier interface {
	// Begin starts a transaction.
	Begin(ctx context.Context) (pgx.Tx, error)
	// Exec executes a statement.
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	// Query runs a query returning rows.
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	// QueryRow runs a query returning at most one row.
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// RoomRepository stores the voice channel to LiveKit room mapping.
type RoomRepository interface {
	// GetRoom returns the room of a channel, or ErrNotFound.
	GetRoom(ctx context.Context, channelID uuid.UUID) (*models.VoiceRoom, error)
	// GetRoomByLiveKitName resolves a room by its LiveKit name, or ErrNotFound.
	GetRoomByLiveKitName(ctx context.Context, roomName string) (*models.VoiceRoom, error)
	// UpsertRoom stores or refreshes the room capacity.
	UpsertRoom(ctx context.Context, room *models.VoiceRoom) error
	// UpdateRoomCapacity updates the stored capacity, or ErrNotFound.
	UpdateRoomCapacity(ctx context.Context, channelID uuid.UUID, capacity int) error
	// ListRoomsByCommunity lists the rooms of a community.
	ListRoomsByCommunity(ctx context.Context, communityID uuid.UUID) ([]models.VoiceRoom, error)
	// ListRoomsWithPresence lists the rooms that currently have participants.
	ListRoomsWithPresence(ctx context.Context) ([]models.VoiceRoom, error)
}

// SeatRepository enforces channel capacity transactionally.
type SeatRepository interface {
	// ReserveSeat claims a seat, or returns ErrChannelFull/ErrNotFound.
	ReserveSeat(ctx context.Context, req models.SeatReservationRequest) (*models.SeatReservation, error)
	// CompleteSeat atomically releases the reservation and records presence so
	// the seat is never momentarily free.
	CompleteSeat(ctx context.Context, reservationID uuid.UUID, presence *models.Presence) (*models.Presence, error)
	// ReleaseSeat releases one reservation; releasing twice is a no-op.
	ReleaseSeat(ctx context.Context, reservationID uuid.UUID) error
	// ReleaseUserSeats releases the live reservations of a user in a channel.
	ReleaseUserSeats(ctx context.Context, channelID, userID uuid.UUID) error
	// CountOccupied returns seats held per channel (live reservations plus
	// active participants).
	CountOccupied(ctx context.Context, channelIDs []uuid.UUID) (map[uuid.UUID]int, error)
}

// PresenceRepository stores who is in which voice channel.
type PresenceRepository interface {
	// UpsertActive records an active presence row.
	UpsertActive(ctx context.Context, presence *models.Presence) (*models.Presence, error)
	// MarkLeft closes the active presence row of a user; it reports whether a
	// row was active.
	MarkLeft(ctx context.Context, communityID, channelID, userID uuid.UUID, at time.Time) (bool, error)
	// MarkChannelLeft closes every active presence row of a channel and
	// returns the rows that were still active.
	MarkChannelLeft(ctx context.Context, communityID, channelID uuid.UUID, at time.Time) ([]models.Presence, error)
	// SetMedia updates the media flags of an active participant; it reports
	// whether the participant was active.
	SetMedia(ctx context.Context, communityID, channelID, userID uuid.UUID, camera, screen, publishing bool, at time.Time) (bool, error)
	// ListActiveByCommunity lists active participants of a community.
	ListActiveByCommunity(ctx context.Context, communityID uuid.UUID) ([]models.Presence, error)
	// ListActiveByChannel lists active participants of one channel.
	ListActiveByChannel(ctx context.Context, communityID, channelID uuid.UUID) ([]models.Presence, error)
	// ListActiveByUser lists the active sessions of a user in a community.
	ListActiveByUser(ctx context.Context, communityID, userID uuid.UUID) ([]models.Presence, error)
	// CountActive counts active participants of one channel.
	CountActive(ctx context.Context, communityID, channelID uuid.UUID) (int, error)
	// CountTotal counts all active participants and the rooms they occupy.
	CountTotal(ctx context.Context) (participants, rooms int, err error)
}

// ProjectionRepository stores the event-derived member and channel views.
type ProjectionRepository interface {
	// UpsertMember applies a member projection if its version is newer. It
	// reports whether the row was applied.
	UpsertMember(ctx context.Context, member models.MemberProjection) (bool, error)
	// GetMember returns the projection of one member, or ErrNotFound.
	GetMember(ctx context.Context, communityID, userID uuid.UUID) (*models.MemberProjection, error)
	// UpsertChannel applies a channel projection if its version is newer. It
	// reports whether the row was applied.
	UpsertChannel(ctx context.Context, channel models.ChannelProjection) (bool, error)
	// GetChannel returns the projection of one channel, or ErrNotFound.
	GetChannel(ctx context.Context, communityID, channelID uuid.UUID) (*models.ChannelProjection, error)
	// ListVoiceChannels lists the live voice channels of a community.
	ListVoiceChannels(ctx context.Context, communityID uuid.UUID) ([]models.ChannelProjection, error)
}

// DedupeRepository stores the consumed Kafka and webhook event ids.
type DedupeRepository interface {
	// MarkEventProcessed records a consumed event id and reports whether it is
	// the first delivery.
	MarkEventProcessed(ctx context.Context, eventID uuid.UUID, topic string) (bool, error)
	// MarkWebhookProcessed records a webhook event id and reports whether it
	// is the first delivery.
	MarkWebhookProcessed(ctx context.Context, webhookEventID, eventKind string) (bool, error)
	// ForgetEvent removes a Kafka dedupe row so a failed reaction can be
	// retried on redelivery.
	ForgetEvent(ctx context.Context, eventID uuid.UUID) error
	// ForgetWebhook removes a webhook dedupe row so a failed reaction can be
	// retried on redelivery.
	ForgetWebhook(ctx context.Context, webhookEventID string) error
	// PurgeProcessedEvents drops dedupe rows older than the retention window.
	PurgeProcessedEvents(ctx context.Context, olderThan time.Time) (int64, error)
}

// NewPool opens a pgx pool for databaseURL and verifies it once.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("repository: parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MaxConnLifetime = time.Hour
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("repository: open pool: %w", err)
	}
	return pool, nil
}

// Ping verifies database connectivity (readiness probe).
func Ping(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return fmt.Errorf("repository: no database pool")
	}
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("repository: ping: %w", err)
	}
	return nil
}

// columns of voice_rooms in scan order.
const roomColumns = "channel_id, community_id, livekit_room_name, capacity, created_at, updated_at"

// columns of voice_participants in scan order.
const presenceColumns = "community_id, channel_id, user_id, session_joined_at, left_at, " +
	"camera_enabled, screen_share_enabled, publishing_allowed, last_seen_at"

// scanRoom scans one voice_rooms row.
func scanRoom(row pgx.Row) (*models.VoiceRoom, error) {
	var room models.VoiceRoom
	err := row.Scan(&room.ChannelID, &room.CommunityID, &room.LiveKitRoomName, &room.Capacity,
		&room.CreatedAt, &room.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("repository: scan room: %w", err)
	}
	return &room, nil
}

// scanPresence scans one voice_participants row.
func scanPresence(row pgx.Row) (*models.Presence, error) {
	var presence models.Presence
	err := row.Scan(&presence.CommunityID, &presence.ChannelID, &presence.UserID,
		&presence.SessionJoinedAt, &presence.LeftAt, &presence.CameraEnabled,
		&presence.ScreenShareEnabled, &presence.PublishingAllowed, &presence.LastSeenAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("repository: scan presence: %w", err)
	}
	return &presence, nil
}

// collectPresence drains rows into a slice.
func collectPresence(rows pgx.Rows) ([]models.Presence, error) {
	defer rows.Close()
	presences := make([]models.Presence, 0, 8)
	for rows.Next() {
		presence, err := scanPresence(rows)
		if err != nil {
			return nil, err
		}
		presences = append(presences, *presence)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: read presence rows: %w", err)
	}
	return presences, nil
}
