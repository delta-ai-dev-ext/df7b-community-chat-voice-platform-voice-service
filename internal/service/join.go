package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"voice_service/internal/community"
	"voice_service/internal/livekit"
	"voice_service/internal/models"
	"voice_service/internal/repository"
)

// JoinInput is one join request after access-token verification.
type JoinInput struct {
	// CommunityID is the community that owns the channel.
	CommunityID uuid.UUID
	// ChannelID is the requested voice channel.
	ChannelID uuid.UUID
	// UserID is the verified caller.
	UserID uuid.UUID
	// TraceID is propagated from the inbound request.
	TraceID string
}

// JoinService orchestrates the join flow of docs/architecture.md §3.1:
// access check, transactional seat reservation, LiveKit room creation, token
// minting, and the presence record.
type JoinService struct {
	c Collaborators
}

// NewJoinService builds the join orchestrator.
func NewJoinService(c Collaborators) *JoinService { return &JoinService{c: c} }

// Join authorizes and admits one participant. The seat reservation is released
// again when LiveKit cannot serve the join, so an unserved join never occupies
// a seat.
func (s *JoinService) Join(ctx context.Context, in JoinInput) (*models.JoinResponse, error) {
	if in.CommunityID == uuid.Nil || in.ChannelID == uuid.Nil || in.UserID == uuid.Nil {
		return nil, fmt.Errorf("%w: community, channel, and user ids are required", models.ErrInvalidRequest)
	}
	log := s.c.log().With(
		zap.String("operation", "join_voice_channel"),
		zap.String("trace_id", in.TraceID),
		zap.String("channel_id", in.ChannelID.String()),
		zap.String("community_id", in.CommunityID.String()),
	)

	check, err := s.c.Access.CheckVoiceAccess(ctx, community.AccessCheckRequest{
		CommunityID: in.CommunityID,
		ChannelID:   in.ChannelID,
		UserID:      in.UserID,
		TraceID:     in.TraceID,
	})
	if err != nil {
		if errors.Is(err, community.ErrCommunityNotFound) {
			s.c.Metrics.RecordJoin(JoinResultForbidden)
			return nil, models.ErrChannelNotFound
		}
		log.Error("voice access check failed; failing closed", zap.Error(err))
		return nil, fmt.Errorf("%w: %v", models.ErrAuthorizationUnavailable, err)
	}
	if !check.Allowed {
		s.c.Metrics.RecordJoin(JoinResultForbidden)
		log.Info("join denied by access check", zap.String("denial_reason", check.DenialReason))
		return nil, denialError(check.DenialReason)
	}
	if check.Channel == nil {
		s.c.Metrics.RecordJoin(JoinResultForbidden)
		return nil, fmt.Errorf("%w: access check allowed without a channel", models.ErrAuthorizationUnavailable)
	}

	now := s.c.clock()
	mutedUntil := activeMute(check.MutedUntil, now)
	canPublish := mutedUntil == nil

	room := &models.VoiceRoom{
		ChannelID:       in.ChannelID,
		CommunityID:     in.CommunityID,
		LiveKitRoomName: RoomName(in.ChannelID),
		Capacity:        check.Channel.Capacity,
	}
	if err := s.c.Rooms.UpsertRoom(ctx, room); err != nil {
		return nil, fmt.Errorf("join: register room: %w", err)
	}

	reservation, err := s.c.Seats.ReserveSeat(ctx, models.SeatReservationRequest{
		CommunityID: in.CommunityID,
		ChannelID:   in.ChannelID,
		UserID:      in.UserID,
		TTL:         s.c.SeatTTL,
	})
	if err != nil {
		if errors.Is(err, repository.ErrChannelFull) {
			s.c.Metrics.RecordJoin(JoinResultFull)
			log.Info("voice channel is full")
			return nil, &models.ChannelFullError{
				Alternatives: s.alternatives(ctx, check.VoiceChannels, in.ChannelID),
			}
		}
		return nil, fmt.Errorf("join: reserve seat: %w", err)
	}

	if err := s.c.LiveKit.CreateRoom(ctx, livekit.CreateRoomRequest{
		RoomName:        room.LiveKitRoomName,
		MaxParticipants: room.Capacity,
	}); err != nil {
		s.releaseSeat(ctx, reservation, log)
		return nil, fmt.Errorf("%w: %v", models.ErrMediaUnavailable, err)
	}

	token, err := s.c.Tokens.MintParticipantToken(ctx, livekit.TokenRequest{
		RoomName:   room.LiveKitRoomName,
		Identity:   in.UserID.String(),
		Name:       in.UserID.String(),
		CanPublish: canPublish,
		TTL:        s.c.TokenTTL,
	})
	if err != nil {
		s.releaseSeat(ctx, reservation, log)
		return nil, fmt.Errorf("%w: %v", models.ErrMediaUnavailable, err)
	}

	presence := &models.Presence{
		CommunityID:        in.CommunityID,
		ChannelID:          in.ChannelID,
		UserID:             in.UserID,
		SessionJoinedAt:    now,
		CameraEnabled:      false,
		ScreenShareEnabled: false,
		PublishingAllowed:  canPublish,
		LastSeenAt:         now,
	}
	if _, err := s.c.Seats.CompleteSeat(ctx, reservation.ReservationID, presence); err != nil {
		s.releaseSeat(ctx, reservation, log)
		return nil, fmt.Errorf("join: record presence: %w", err)
	}

	s.c.Metrics.RecordJoin(JoinResultJoined)
	if participants, rooms, err := s.c.Presence.CountTotal(ctx); err == nil {
		s.c.refreshGauges(participants, rooms, nil)
	}
	log.Info("participant joined",
		zap.Bool("listen_only", !canPublish),
		zap.String("room_name", room.LiveKitRoomName),
	)

	return &models.JoinResponse{
		LiveKitURL:       s.c.LiveKitURL,
		Token:            token.Value,
		RoomName:         room.LiveKitRoomName,
		CanPublishAudio:  canPublish,
		CanPublishVideo:  canPublish,
		CanPublishScreen: canPublish,
		MutedUntil:       mutedUntil,
		TokenExpiresAt:   token.ExpiresAt,
	}, nil
}

// releaseSeat rolls the reservation back after a failed join. The TTL would
// free it anyway; releasing immediately keeps the seat available.
func (s *JoinService) releaseSeat(ctx context.Context, reservation *models.SeatReservation, log *zap.Logger) {
	if reservation == nil {
		return
	}
	if err := s.c.Seats.ReleaseSeat(ctx, reservation.ReservationID); err != nil {
		log.Warn("could not release seat reservation after failed join", zap.Error(err))
	}
}

// alternatives lists the community's other voice channels with free seats. A
// failure to compute them never turns the 409 into a 500.
func (s *JoinService) alternatives(ctx context.Context, channels []models.ChannelSummary, exclude uuid.UUID) []models.ChannelAlternative {
	alternatives := make([]models.ChannelAlternative, 0, len(channels))
	ids := make([]uuid.UUID, 0, len(channels))
	for _, channel := range channels {
		if channel.ChannelID != exclude && channel.ChannelID != uuid.Nil {
			ids = append(ids, channel.ChannelID)
		}
	}
	if len(ids) == 0 {
		return alternatives
	}
	occupied, err := s.c.Seats.CountOccupied(ctx, ids)
	if err != nil {
		s.c.log().Warn("cannot compute voice channel alternatives", zap.Error(err))
		return alternatives
	}
	for _, channel := range channels {
		if channel.ChannelID == exclude || channel.ChannelID == uuid.Nil {
			continue
		}
		if occupied[channel.ChannelID] >= channel.Capacity {
			continue
		}
		alternatives = append(alternatives, models.ChannelAlternative(channel))
	}
	return alternatives
}

// denialError maps an API-002 denial reason onto the public error.
func denialError(reason string) error {
	switch reason {
	case models.ErrCodeChannelNotFound:
		return models.ErrChannelNotFound
	case models.ErrCodeNotAVoiceChannel:
		return models.ErrNotAVoiceChannel
	default:
		// not_a_member, banned, and any unknown reason are reported the same
		// way: the caller may not join.
		return models.ErrNotAMember
	}
}

// activeMute returns the mute deadline when it is still in the future.
func activeMute(mutedUntil *time.Time, now time.Time) *time.Time {
	if mutedUntil == nil || !mutedUntil.After(now) {
		return nil
	}
	utc := mutedUntil.UTC()
	return &utc
}
