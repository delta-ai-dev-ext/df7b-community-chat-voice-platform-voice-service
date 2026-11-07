package events

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"voice_service/internal/models"
)

// Member change values of EVT-002.
const (
	// MemberChangeJoined is a member joining the community.
	MemberChangeJoined = "joined"
	// MemberChangeLeft is a member leaving the community.
	MemberChangeLeft = "left"
	// MemberChangeRemoved is a moderator removal.
	MemberChangeRemoved = "removed"
	// MemberChangeBanned is a ban.
	MemberChangeBanned = "banned"
	// MemberChangeUnbanned is a ban lift.
	MemberChangeUnbanned = "unbanned"
	// MemberChangeMuted is an in-place mute.
	MemberChangeMuted = "muted"
	// MemberChangeUnmuted is a mute lift.
	MemberChangeUnmuted = "unmuted"
	// MemberChangeRolesChanged is a role assignment change.
	MemberChangeRolesChanged = "roles_changed"
)

// Channel change values of EVT-003.
const (
	// ChannelChangeCreated is a channel creation.
	ChannelChangeCreated = "created"
	// ChannelChangeUpdated is a channel update.
	ChannelChangeUpdated = "updated"
	// ChannelChangeDeleted is a channel deletion.
	ChannelChangeDeleted = "deleted"
)

// MemberChangedPayload is the EVT-002 payload contract.
type MemberChangedPayload struct {
	// CommunityID owns the membership.
	CommunityID string `json:"community_id"`
	// UserID is the member.
	UserID string `json:"user_id"`
	// Change is the membership change kind.
	Change string `json:"change"`
	// Status is active or removed.
	Status string `json:"status"`
	// IsBanned reports an active ban.
	IsBanned bool `json:"is_banned"`
	// MutedUntil is the mute deadline, if any.
	MutedUntil *time.Time `json:"muted_until"`
	// RoleIDs are the member's role ids.
	RoleIDs []string `json:"role_ids"`
	// SanctionID is the sanction that caused the change, if any.
	SanctionID *string `json:"sanction_id"`
	// MemberVersion orders member updates.
	MemberVersion int64 `json:"member_version"`
}

// ChannelChangedPayload is the EVT-003 payload contract.
type ChannelChangedPayload struct {
	// CommunityID owns the channel.
	CommunityID string `json:"community_id"`
	// ChannelID is the channel.
	ChannelID string `json:"channel_id"`
	// Change is created, updated, or deleted.
	Change string `json:"change"`
	// Kind is text or voice.
	Kind string `json:"kind"`
	// Name is the display name.
	Name string `json:"name"`
	// Position is the channel order in the sidebar.
	Position int `json:"position"`
	// Capacity is the voice seat count, nil for text channels.
	Capacity *int `json:"capacity"`
	// ChannelVersion orders channel updates.
	ChannelVersion int64 `json:"channel_version"`
}

// MemberProjection converts the payload into a projection row.
func (p MemberChangedPayload) MemberProjection(now time.Time) (models.MemberProjection, error) {
	communityID, err := uuid.Parse(p.CommunityID)
	if err != nil {
		return models.MemberProjection{}, fmt.Errorf("community_id: %w", err)
	}
	userID, err := uuid.Parse(p.UserID)
	if err != nil {
		return models.MemberProjection{}, fmt.Errorf("user_id: %w", err)
	}
	if p.MemberVersion < 1 {
		return models.MemberProjection{}, fmt.Errorf("member_version %d must be >= 1", p.MemberVersion)
	}
	status := p.Status
	if status != models.MemberStatusActive && status != models.MemberStatusRemoved {
		return models.MemberProjection{}, fmt.Errorf("status %q is not supported", p.Status)
	}
	return models.MemberProjection{
		CommunityID:   communityID,
		UserID:        userID,
		Status:        status,
		IsBanned:      p.IsBanned,
		MutedUntil:    p.MutedUntil,
		MemberVersion: p.MemberVersion,
		UpdatedAt:     now.UTC(),
	}, nil
}

// ChannelProjection converts the payload into a projection row.
func (p ChannelChangedPayload) ChannelProjection(now time.Time) (models.ChannelProjection, error) {
	communityID, err := uuid.Parse(p.CommunityID)
	if err != nil {
		return models.ChannelProjection{}, fmt.Errorf("community_id: %w", err)
	}
	channelID, err := uuid.Parse(p.ChannelID)
	if err != nil {
		return models.ChannelProjection{}, fmt.Errorf("channel_id: %w", err)
	}
	if p.ChannelVersion < 1 {
		return models.ChannelProjection{}, fmt.Errorf("channel_version %d must be >= 1", p.ChannelVersion)
	}
	if p.Kind != models.ChannelKindText && p.Kind != models.ChannelKindVoice {
		return models.ChannelProjection{}, fmt.Errorf("kind %q is not supported", p.Kind)
	}
	if p.Capacity != nil && (*p.Capacity < models.MinCapacity || *p.Capacity > models.MaxCapacity) {
		return models.ChannelProjection{}, fmt.Errorf("capacity %d is out of range", *p.Capacity)
	}
	return models.ChannelProjection{
		CommunityID:    communityID,
		ChannelID:      channelID,
		Kind:           p.Kind,
		Name:           p.Name,
		Capacity:       p.Capacity,
		Deleted:        p.Change == ChannelChangeDeleted,
		ChannelVersion: p.ChannelVersion,
		UpdatedAt:      now.UTC(),
	}, nil
}

// IsRemoval reports whether the change removes the member from the community
// and therefore from every live voice room.
func (p MemberChangedPayload) IsRemoval() bool {
	if p.IsBanned || p.Status == models.MemberStatusRemoved {
		return true
	}
	switch p.Change {
	case MemberChangeRemoved, MemberChangeBanned, MemberChangeLeft:
		return true
	default:
		return false
	}
}
