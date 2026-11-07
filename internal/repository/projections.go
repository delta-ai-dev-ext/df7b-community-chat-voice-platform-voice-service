package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"voice_service/internal/models"
)

// Projections is the pgx implementation of ProjectionRepository.
type Projections struct {
	q Querier
}

// NewProjections builds the projection repository.
func NewProjections(q Querier) *Projections { return &Projections{q: q} }

// UpsertMember applies a member projection only when the incoming
// member_version is newer, so stale or re-delivered events are ignored.
func (p *Projections) UpsertMember(ctx context.Context, member models.MemberProjection) (bool, error) {
	var version int64
	err := p.q.QueryRow(ctx, `
		INSERT INTO member_projections
		    (community_id, user_id, status, is_banned, muted_until, member_version, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		ON CONFLICT (community_id, user_id) DO UPDATE
		SET status = EXCLUDED.status,
		    is_banned = EXCLUDED.is_banned,
		    muted_until = EXCLUDED.muted_until,
		    member_version = EXCLUDED.member_version,
		    updated_at = now()
		WHERE member_projections.member_version < EXCLUDED.member_version
		RETURNING member_version`,
		member.CommunityID, member.UserID, member.Status, member.IsBanned, member.MutedUntil, member.MemberVersion).
		Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("repository: upsert member projection: %w", err)
	}
	return true, nil
}

// GetMember returns the projection of one member.
func (p *Projections) GetMember(ctx context.Context, communityID, userID uuid.UUID) (*models.MemberProjection, error) {
	var member models.MemberProjection
	err := p.q.QueryRow(ctx, `
		SELECT community_id, user_id, status, is_banned, muted_until, member_version, updated_at
		FROM member_projections
		WHERE community_id = $1 AND user_id = $2`, communityID, userID).
		Scan(&member.CommunityID, &member.UserID, &member.Status, &member.IsBanned,
			&member.MutedUntil, &member.MemberVersion, &member.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("repository: get member projection: %w", err)
	}
	return &member, nil
}

// UpsertChannel applies a channel projection only when the incoming
// channel_version is newer.
func (p *Projections) UpsertChannel(ctx context.Context, channel models.ChannelProjection) (bool, error) {
	var version int64
	err := p.q.QueryRow(ctx, `
		INSERT INTO channel_projections
		    (community_id, channel_id, kind, name, capacity, deleted, channel_version, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now())
		ON CONFLICT (community_id, channel_id) DO UPDATE
		SET kind = EXCLUDED.kind,
		    name = EXCLUDED.name,
		    capacity = EXCLUDED.capacity,
		    deleted = EXCLUDED.deleted,
		    channel_version = EXCLUDED.channel_version,
		    updated_at = now()
		WHERE channel_projections.channel_version < EXCLUDED.channel_version
		RETURNING channel_version`,
		channel.CommunityID, channel.ChannelID, channel.Kind, channel.Name, channel.Capacity,
		channel.Deleted, channel.ChannelVersion).
		Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("repository: upsert channel projection: %w", err)
	}
	return true, nil
}

// GetChannel returns the projection of one channel.
func (p *Projections) GetChannel(ctx context.Context, communityID, channelID uuid.UUID) (*models.ChannelProjection, error) {
	var channel models.ChannelProjection
	err := p.q.QueryRow(ctx, `
		SELECT community_id, channel_id, kind, name, capacity, deleted, channel_version, updated_at
		FROM channel_projections
		WHERE community_id = $1 AND channel_id = $2`, communityID, channelID).
		Scan(&channel.CommunityID, &channel.ChannelID, &channel.Kind, &channel.Name,
			&channel.Capacity, &channel.Deleted, &channel.ChannelVersion, &channel.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("repository: get channel projection: %w", err)
	}
	return &channel, nil
}

// ListVoiceChannels lists the live voice channels of a community.
func (p *Projections) ListVoiceChannels(ctx context.Context, communityID uuid.UUID) ([]models.ChannelProjection, error) {
	rows, err := p.q.Query(ctx, `
		SELECT community_id, channel_id, kind, name, capacity, deleted, channel_version, updated_at
		FROM channel_projections
		WHERE community_id = $1 AND kind = 'voice' AND deleted = false
		ORDER BY name`, communityID)
	if err != nil {
		return nil, fmt.Errorf("repository: list voice channels: %w", err)
	}
	defer rows.Close()
	channels := make([]models.ChannelProjection, 0, 8)
	for rows.Next() {
		var channel models.ChannelProjection
		if err := rows.Scan(&channel.CommunityID, &channel.ChannelID, &channel.Kind, &channel.Name,
			&channel.Capacity, &channel.Deleted, &channel.ChannelVersion, &channel.UpdatedAt); err != nil {
			return nil, fmt.Errorf("repository: scan voice channel: %w", err)
		}
		channels = append(channels, channel)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: read voice channels: %w", err)
	}
	return channels, nil
}
