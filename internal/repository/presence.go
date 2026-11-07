package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"voice_service/internal/models"
)

// Presence is the pgx implementation of PresenceRepository.
type Presence struct {
	q Querier
}

// NewPresence builds the presence repository.
func NewPresence(q Querier) *Presence { return &Presence{q: q} }

// UpsertActive records an active presence row. A repeated join of the same
// participant refreshes the existing active session instead of creating a
// second one (partial unique index on the active row).
func (p *Presence) UpsertActive(ctx context.Context, presence *models.Presence) (*models.Presence, error) {
	if presence == nil {
		return nil, fmt.Errorf("repository: nil presence")
	}
	joined := presence.SessionJoinedAt
	if joined.IsZero() {
		joined = time.Now().UTC()
	}
	lastSeen := presence.LastSeenAt
	if lastSeen.IsZero() {
		lastSeen = joined
	}
	row := p.q.QueryRow(ctx, `
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
	return scanPresence(row)
}

// MarkLeft closes the active presence row of a user.
func (p *Presence) MarkLeft(ctx context.Context, communityID, channelID, userID uuid.UUID, at time.Time) (bool, error) {
	tag, err := p.q.Exec(ctx, `
		UPDATE voice_participants
		SET left_at = $4
		WHERE community_id = $1 AND channel_id = $2 AND user_id = $3 AND left_at IS NULL`,
		communityID, channelID, userID, at.UTC())
	if err != nil {
		return false, fmt.Errorf("repository: mark presence left: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// MarkChannelLeft closes every active presence row of a channel.
func (p *Presence) MarkChannelLeft(ctx context.Context, communityID, channelID uuid.UUID, at time.Time) ([]models.Presence, error) {
	rows, err := p.q.Query(ctx, `
		UPDATE voice_participants
		SET left_at = $3
		WHERE community_id = $1 AND channel_id = $2 AND left_at IS NULL
		RETURNING `+presenceColumns, communityID, channelID, at.UTC())
	if err != nil {
		return nil, fmt.Errorf("repository: mark channel left: %w", err)
	}
	return collectPresence(rows)
}

// SetMedia updates the media flags of an active participant.
func (p *Presence) SetMedia(
	ctx context.Context,
	communityID, channelID, userID uuid.UUID,
	camera, screen, publishing bool,
	at time.Time,
) (bool, error) {
	tag, err := p.q.Exec(ctx, `
		UPDATE voice_participants
		SET camera_enabled = $4,
		    screen_share_enabled = $5,
		    publishing_allowed = $6,
		    last_seen_at = $7
		WHERE community_id = $1 AND channel_id = $2 AND user_id = $3 AND left_at IS NULL`,
		communityID, channelID, userID, camera, screen, publishing, at.UTC())
	if err != nil {
		return false, fmt.Errorf("repository: update presence media: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// ListActiveByCommunity lists active participants of a community.
func (p *Presence) ListActiveByCommunity(ctx context.Context, communityID uuid.UUID) ([]models.Presence, error) {
	rows, err := p.q.Query(ctx,
		"SELECT "+presenceColumns+" FROM voice_participants WHERE community_id = $1 AND left_at IS NULL "+
			"ORDER BY channel_id, session_joined_at", communityID)
	if err != nil {
		return nil, fmt.Errorf("repository: list presence by community: %w", err)
	}
	return collectPresence(rows)
}

// ListActiveByChannel lists active participants of one channel.
func (p *Presence) ListActiveByChannel(ctx context.Context, communityID, channelID uuid.UUID) ([]models.Presence, error) {
	rows, err := p.q.Query(ctx,
		"SELECT "+presenceColumns+" FROM voice_participants "+
			"WHERE community_id = $1 AND channel_id = $2 AND left_at IS NULL ORDER BY session_joined_at",
		communityID, channelID)
	if err != nil {
		return nil, fmt.Errorf("repository: list presence by channel: %w", err)
	}
	return collectPresence(rows)
}

// ListActiveByUser lists the active sessions of a user in a community.
func (p *Presence) ListActiveByUser(ctx context.Context, communityID, userID uuid.UUID) ([]models.Presence, error) {
	rows, err := p.q.Query(ctx,
		"SELECT "+presenceColumns+" FROM voice_participants "+
			"WHERE community_id = $1 AND user_id = $2 AND left_at IS NULL ORDER BY channel_id",
		communityID, userID)
	if err != nil {
		return nil, fmt.Errorf("repository: list presence by user: %w", err)
	}
	return collectPresence(rows)
}

// CountActive counts the active participants of one channel.
func (p *Presence) CountActive(ctx context.Context, communityID, channelID uuid.UUID) (int, error) {
	var count int
	err := p.q.QueryRow(ctx, `
		SELECT count(*) FROM voice_participants
		WHERE community_id = $1 AND channel_id = $2 AND left_at IS NULL`, communityID, channelID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("repository: count active participants: %w", err)
	}
	return count, nil
}

// CountTotal counts all active participants and the rooms they occupy.
func (p *Presence) CountTotal(ctx context.Context) (int, int, error) {
	var participants, rooms int
	err := p.q.QueryRow(ctx, `
		SELECT count(*), count(DISTINCT channel_id) FROM voice_participants WHERE left_at IS NULL`).
		Scan(&participants, &rooms)
	if err != nil {
		return 0, 0, fmt.Errorf("repository: count total presence: %w", err)
	}
	return participants, rooms, nil
}
