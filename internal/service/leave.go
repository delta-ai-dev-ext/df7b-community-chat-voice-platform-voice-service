package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"voice_service/internal/events"
	"voice_service/internal/livekit"
	"voice_service/internal/models"
	"voice_service/internal/repository"
)

// LeaveInput is one leave request after access-token verification.
type LeaveInput struct {
	// ChannelID is the voice channel to leave.
	ChannelID uuid.UUID
	// UserID is the verified caller.
	UserID uuid.UUID
	// TraceID is propagated from the inbound request.
	TraceID string
}

// LeaveService removes a participant from a live room and frees the seat. It is
// idempotent: leaving a channel the caller is not in succeeds without side
// effects (US-005).
type LeaveService struct {
	c Collaborators
}

// NewLeaveService builds the leave service.
func NewLeaveService(c Collaborators) *LeaveService { return &LeaveService{c: c} }

// Leave performs the leave flow of docs/architecture.md §3.2.
func (s *LeaveService) Leave(ctx context.Context, in LeaveInput) error {
	if in.ChannelID == uuid.Nil || in.UserID == uuid.Nil {
		return fmt.Errorf("%w: channel and user ids are required", models.ErrInvalidRequest)
	}
	log := s.c.log().With(
		zap.String("operation", "leave_voice_channel"),
		zap.String("trace_id", in.TraceID),
		zap.String("channel_id", in.ChannelID.String()),
	)

	room, err := s.c.Rooms.GetRoom(ctx, in.ChannelID)
	if errors.Is(err, repository.ErrNotFound) {
		return models.ErrChannelNotFound
	}
	if err != nil {
		return fmt.Errorf("leave: load room: %w", err)
	}

	now := s.c.clock()
	sessions, err := s.c.Presence.ListActiveByUser(ctx, room.CommunityID, in.UserID)
	if err != nil {
		return fmt.Errorf("leave: load presence: %w", err)
	}
	var session *models.Presence
	for i := range sessions {
		if sessions[i].ChannelID == in.ChannelID {
			session = &sessions[i]
			break
		}
	}

	// The participant is disconnected even when local presence is already
	// gone, so a stale seat cannot keep a ghost in the room.
	if err := s.c.LiveKit.RemoveParticipant(ctx, room.LiveKitRoomName, in.UserID.String()); err != nil &&
		!errors.Is(err, livekit.ErrParticipantNotFound) && !errors.Is(err, livekit.ErrRoomNotFound) {
		// Best effort: the seat must be freed and the presence cleared even if
		// LiveKit is briefly unavailable; reconciliation heals residual drift.
		log.Warn("could not remove participant from livekit room", zap.Error(err))
	}

	wasActive, err := s.c.Presence.MarkLeft(ctx, room.CommunityID, in.ChannelID, in.UserID, now)
	if err != nil {
		return fmt.Errorf("leave: mark presence left: %w", err)
	}
	if err := s.c.Seats.ReleaseUserSeats(ctx, in.ChannelID, in.UserID); err != nil {
		return fmt.Errorf("leave: release seat: %w", err)
	}

	if !wasActive {
		log.Debug("leave was a no-op; participant was not present")
		return nil
	}
	if session != nil {
		s.c.recordParticipantMinutes(*session, now)
	}
	count, err := s.c.Presence.CountActive(ctx, room.CommunityID, in.ChannelID)
	if err != nil {
		return fmt.Errorf("leave: count participants: %w", err)
	}
	if err := s.publishLeft(ctx, room, in.UserID, session, count, in.TraceID); err != nil {
		// Presence already changed; a lost presence event is a transient
		// realtime gap, not a client-visible failure.
		log.Error("could not publish presence left event", zap.Error(err))
	}
	if participants, rooms, err := s.c.Presence.CountTotal(ctx); err == nil {
		s.c.refreshGauges(participants, rooms, nil)
	}
	log.Info("participant left", zap.String("room_name", room.LiveKitRoomName))
	return nil
}

// publishLeft emits the EVT-010 left event of a departure.
func (s *LeaveService) publishLeft(
	ctx context.Context,
	room *models.VoiceRoom,
	userID uuid.UUID,
	session *models.Presence,
	count int,
	traceID string,
) error {
	presence := models.Presence{CommunityID: room.CommunityID, ChannelID: room.ChannelID, UserID: userID}
	if session != nil {
		presence = *session
	}
	message, err := events.NewPresenceMessage(events.PresenceEvent{
		CommunityID:        room.CommunityID,
		ChannelID:          room.ChannelID,
		UserID:             userID,
		Change:             events.ChangeLeft,
		CameraEnabled:      presence.CameraEnabled,
		ScreenShareEnabled: presence.ScreenShareEnabled,
		PublishingAllowed:  presence.PublishingAllowed,
		ParticipantCount:   count,
		Capacity:           room.Capacity,
		TraceID:            traceID,
		OccurredAt:         s.c.clock(),
	})
	if err != nil {
		return err
	}
	return s.c.Producer.Publish(ctx, message)
}
