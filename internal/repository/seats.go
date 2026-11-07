package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"voice_service/internal/models"
)

// Seats is the pgx implementation of SeatRepository.
type Seats struct {
	q   Querier
	now func() time.Time
}

// NewSeats builds the seat repository.
func NewSeats(q Querier) *Seats { return &Seats{q: q, now: time.Now} }

// ReserveSeat claims a seat inside one transaction.
//
// The transaction first locks the voice_rooms row, which serializes every join
// of the channel across replicas. Expired reservations of this user are swept
// so the partial unique index cannot collide, then capacity is checked against
// live reservations plus active participants (the caller's own rows excluded,
// so a reconnect never counts twice). A full channel returns ErrChannelFull
// without leaving a reservation behind.
func (s *Seats) ReserveSeat(ctx context.Context, req models.SeatReservationRequest) (*models.SeatReservation, error) {
	if req.TTL <= 0 {
		return nil, fmt.Errorf("repository: seat TTL must be positive")
	}
	tx, err := s.q.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("repository: begin seat transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var capacity int
	err = tx.QueryRow(ctx, "SELECT capacity FROM voice_rooms WHERE channel_id = $1 FOR UPDATE", req.ChannelID).
		Scan(&capacity)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("repository: lock room: %w", err)
	}

	// Sweep this user's abandoned reservations so the partial unique index on
	// (channel_id, user_id) WHERE released_at IS NULL cannot reject the insert.
	if _, err := tx.Exec(ctx, `
		UPDATE seat_reservations
		SET released_at = now()
		WHERE channel_id = $1 AND user_id = $2 AND released_at IS NULL AND expires_at <= now()`,
		req.ChannelID, req.UserID); err != nil {
		return nil, fmt.Errorf("repository: sweep expired reservations: %w", err)
	}

	var holdsSeat bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
		    SELECT 1 FROM voice_participants
		    WHERE channel_id = $1 AND user_id = $2 AND left_at IS NULL
		) OR EXISTS (
		    SELECT 1 FROM seat_reservations
		    WHERE channel_id = $1 AND user_id = $2 AND released_at IS NULL AND expires_at > now()
		)`, req.ChannelID, req.UserID).Scan(&holdsSeat); err != nil {
		return nil, fmt.Errorf("repository: check existing seat: %w", err)
	}

	if !holdsSeat {
		var occupied int
		if err := tx.QueryRow(ctx, `
			SELECT
			    (SELECT count(*) FROM seat_reservations
			     WHERE channel_id = $1 AND released_at IS NULL AND expires_at > now()) +
			    (SELECT count(*) FROM voice_participants
			     WHERE channel_id = $1 AND left_at IS NULL)`, req.ChannelID).Scan(&occupied); err != nil {
			return nil, fmt.Errorf("repository: count occupied seats: %w", err)
		}
		if occupied >= capacity {
			return nil, ErrChannelFull
		}
	}

	reservation := &models.SeatReservation{
		ReservationID: uuid.New(),
		CommunityID:   req.CommunityID,
		ChannelID:     req.ChannelID,
		UserID:        req.UserID,
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO seat_reservations
		    (reservation_id, community_id, channel_id, user_id, reserved_at, expires_at)
		VALUES ($1, $2, $3, $4, now(), now() + make_interval(secs => $5))
		ON CONFLICT (channel_id, user_id) WHERE released_at IS NULL DO UPDATE
		SET reserved_at = now(),
		    expires_at = EXCLUDED.expires_at
		RETURNING reservation_id, community_id, channel_id, user_id, reserved_at, expires_at`,
		reservation.ReservationID, req.CommunityID, req.ChannelID, req.UserID, req.TTL.Seconds()).
		Scan(&reservation.ReservationID, &reservation.CommunityID, &reservation.ChannelID,
			&reservation.UserID, &reservation.ReservedAt, &reservation.ExpiresAt)
	if err != nil {
		return nil, fmt.Errorf("repository: insert seat reservation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("repository: commit seat transaction: %w", err)
	}
	return reservation, nil
}

// CompleteSeat releases the reservation and records presence in one
// transaction, so the seat is never momentarily free.
func (s *Seats) CompleteSeat(ctx context.Context, reservationID uuid.UUID, presence *models.Presence) (*models.Presence, error) {
	if presence == nil {
		return nil, fmt.Errorf("repository: nil presence")
	}
	tx, err := s.q.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("repository: begin complete transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var communityID, channelID, userID uuid.UUID
	err = tx.QueryRow(ctx,
		"SELECT community_id, channel_id, user_id FROM seat_reservations WHERE reservation_id = $1",
		reservationID).Scan(&communityID, &channelID, &userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("repository: load reservation: %w", err)
	}
	if communityID != presence.CommunityID || channelID != presence.ChannelID || userID != presence.UserID {
		return nil, fmt.Errorf("repository: reservation does not belong to the participant")
	}
	if _, err := tx.Exec(ctx,
		"UPDATE seat_reservations SET released_at = now() WHERE reservation_id = $1 AND released_at IS NULL",
		reservationID); err != nil {
		return nil, fmt.Errorf("repository: release reservation: %w", err)
	}
	joined := presence.SessionJoinedAt
	if joined.IsZero() {
		joined = s.now().UTC()
	}
	lastSeen := presence.LastSeenAt
	if lastSeen.IsZero() {
		lastSeen = joined
	}
	row := tx.QueryRow(ctx, `
		INSERT INTO voice_participants
		    (community_id, channel_id, user_id, session_joined_at, camera_enabled,
		     screen_share_enabled, publishing_allowed, last_seen_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (community_id, channel_id, user_id) WHERE left_at IS NULL DO UPDATE
		SET camera_enabled = EXCLUDED.camera_enabled,
		    screen_share_enabled = EXCLUDED.screen_share_enabled,
		    publishing_allowed = EXCLUDED.publishing_allowed,
		    last_seen_at = EXCLUDED.last_seen_at
		RETURNING `+presenceColumns,
		presence.CommunityID, presence.ChannelID, presence.UserID, joined,
		presence.CameraEnabled, presence.ScreenShareEnabled, presence.PublishingAllowed, lastSeen)
	completed, err := scanPresence(row)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("repository: commit complete transaction: %w", err)
	}
	return completed, nil
}

// ReleaseSeat releases one reservation; a second release is a no-op.
func (s *Seats) ReleaseSeat(ctx context.Context, reservationID uuid.UUID) error {
	if _, err := s.q.Exec(ctx,
		"UPDATE seat_reservations SET released_at = now() WHERE reservation_id = $1 AND released_at IS NULL",
		reservationID); err != nil {
		return fmt.Errorf("repository: release reservation: %w", err)
	}
	return nil
}

// ReleaseUserSeats releases every live reservation of a user in a channel.
func (s *Seats) ReleaseUserSeats(ctx context.Context, channelID, userID uuid.UUID) error {
	if _, err := s.q.Exec(ctx, `
		UPDATE seat_reservations
		SET released_at = now()
		WHERE channel_id = $1 AND user_id = $2 AND released_at IS NULL`, channelID, userID); err != nil {
		return fmt.Errorf("repository: release user reservations: %w", err)
	}
	return nil
}

// CountOccupied returns the seats held per channel: live reservations plus
// active participants.
func (s *Seats) CountOccupied(ctx context.Context, channelIDs []uuid.UUID) (map[uuid.UUID]int, error) {
	occupied := make(map[uuid.UUID]int, len(channelIDs))
	if len(channelIDs) == 0 {
		return occupied, nil
	}
	rows, err := s.q.Query(ctx, `
		SELECT channels.channel_id,
		       (SELECT count(*) FROM seat_reservations r
		        WHERE r.channel_id = channels.channel_id AND r.released_at IS NULL AND r.expires_at > now()) +
		       (SELECT count(*) FROM voice_participants p
		        WHERE p.channel_id = channels.channel_id AND p.left_at IS NULL)
		FROM unnest($1::uuid[]) AS channels(channel_id)`, channelIDs)
	if err != nil {
		return nil, fmt.Errorf("repository: count occupied seats: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			channelID uuid.UUID
			count     int
		)
		if err := rows.Scan(&channelID, &count); err != nil {
			return nil, fmt.Errorf("repository: scan occupied seats: %w", err)
		}
		occupied[channelID] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: read occupied seats: %w", err)
	}
	return occupied, nil
}
