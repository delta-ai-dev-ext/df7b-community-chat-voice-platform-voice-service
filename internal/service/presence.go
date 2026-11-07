package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"voice_service/internal/models"
	"voice_service/internal/repository"
)

// PresenceService serves the voice presence snapshot of a community.
type PresenceService struct {
	c Collaborators
}

// NewPresenceService builds the presence service.
func NewPresenceService(c Collaborators) *PresenceService { return &PresenceService{c: c} }

// Snapshot returns one entry per voice channel of the community with its
// participants and seat usage (US-006). Membership is checked against the local
// member projection; unknown members are refused.
func (s *PresenceService) Snapshot(ctx context.Context, communityID, userID uuid.UUID) (*models.PresenceResponse, error) {
	if communityID == uuid.Nil || userID == uuid.Nil {
		return nil, fmt.Errorf("%w: community and user ids are required", models.ErrInvalidRequest)
	}
	member, err := s.c.Projections.GetMember(ctx, communityID, userID)
	if errors.Is(err, repository.ErrNotFound) {
		return nil, models.ErrNotAMember
	}
	if err != nil {
		return nil, fmt.Errorf("presence: load member projection: %w", err)
	}
	if !member.IsActive() {
		return nil, models.ErrNotAMember
	}

	channels, err := s.c.Projections.ListVoiceChannels(ctx, communityID)
	if err != nil {
		return nil, fmt.Errorf("presence: list voice channels: %w", err)
	}
	active, err := s.c.Presence.ListActiveByCommunity(ctx, communityID)
	if err != nil {
		return nil, fmt.Errorf("presence: list presence: %w", err)
	}

	byChannel := make(map[uuid.UUID][]models.PresenceParticipant, len(channels))
	for _, participant := range active {
		byChannel[participant.ChannelID] = append(byChannel[participant.ChannelID], models.PresenceParticipant{
			UserID:             participant.UserID,
			JoinedAt:           participant.SessionJoinedAt.UTC(),
			CameraEnabled:      participant.CameraEnabled,
			ScreenShareEnabled: participant.ScreenShareEnabled,
			PublishingAllowed:  participant.PublishingAllowed,
		})
	}

	response := &models.PresenceResponse{CommunityID: communityID, Channels: make([]models.ChannelPresence, 0, len(channels))}
	for _, channel := range channels {
		participants := byChannel[channel.ChannelID]
		if participants == nil {
			participants = []models.PresenceParticipant{}
		}
		response.Channels = append(response.Channels, models.ChannelPresence{
			ChannelID:    channel.ChannelID,
			Name:         channel.Name,
			Capacity:     channelCapacity(channel),
			SeatsUsed:    len(participants),
			Participants: participants,
		})
	}
	return response, nil
}

// channelCapacity reports the seat count of a projected voice channel. The
// contract guarantees a capacity for voice channels; a missing value is
// reported as the platform maximum instead of an invalid zero.
func channelCapacity(channel models.ChannelProjection) int {
	if channel.Capacity == nil {
		return models.MaxCapacity
	}
	return *channel.Capacity
}
