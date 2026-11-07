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

// CommunityEventConsumer applies community_events (EVT-002 and EVT-003).
type CommunityEventConsumer struct {
	c       Collaborators
	retry   RetryPolicy
	produce events.Producer
}

// NewCommunityEventConsumer builds the consumer of community_events.
func NewCommunityEventConsumer(c Collaborators) *CommunityEventConsumer {
	return &CommunityEventConsumer{c: c, retry: DefaultRetryPolicy(), produce: c.Producer}
}

// WithRetryPolicy replaces the LiveKit retry policy (used by tests).
func (h *CommunityEventConsumer) WithRetryPolicy(policy RetryPolicy) *CommunityEventConsumer {
	h.retry = policy
	return h
}

// Handle processes one decoded community event. Events are deduplicated on
// envelope.event_id and projections only apply newer versions.
func (h *CommunityEventConsumer) Handle(ctx context.Context, message *events.WireMessage) error {
	if message == nil {
		return errors.New("community_events: nil message")
	}
	eventID, err := uuid.Parse(message.Envelope.EventID)
	if err != nil {
		return fmt.Errorf("community_events: invalid event id: %w", err)
	}
	log := h.c.log().With(
		zap.String("operation", "community_event"),
		zap.String("event_type", message.Envelope.EventType),
		zap.String("event_id", message.Envelope.EventID),
		zap.String("trace_id", message.Envelope.TraceID),
	)

	first, err := h.c.Dedupe.MarkEventProcessed(ctx, eventID, events.TopicCommunityEvents)
	if err != nil {
		return fmt.Errorf("community_events: record event id: %w", err)
	}
	if !first {
		log.Debug("duplicate event ignored")
		return nil
	}

	var handleErr error
	switch message.Envelope.EventType {
	case events.EventTypeCommunityMemberChanged:
		handleErr = h.handleMemberChanged(ctx, message, log)
	case events.EventTypeCommunityChannelChanged:
		handleErr = h.handleChannelChanged(ctx, message, log)
	default:
		log.Debug("event type ignored")
	}
	if handleErr != nil {
		// Drop the dedupe row so a redelivery can retry the reaction.
		if forgetErr := h.c.Dedupe.ForgetEvent(ctx, eventID); forgetErr != nil {
			log.Warn("could not forget failed event id", zap.Error(forgetErr))
		}
		return handleErr
	}
	return nil
}

// handleMemberChanged applies EVT-002: projection update plus the moderation
// reaction of docs/architecture.md §3.4.
func (h *CommunityEventConsumer) handleMemberChanged(ctx context.Context, message *events.WireMessage, log *zap.Logger) error {
	var payload events.MemberChangedPayload
	if err := message.DecodePayload(&payload); err != nil {
		// A malformed payload can never succeed; dropping it avoids a poison
		// message loop.
		log.Error("dropping undecodable community_member_changed payload", zap.Error(err))
		return nil
	}
	projection, err := payload.MemberProjection(h.c.clock())
	if err != nil {
		log.Error("dropping invalid community_member_changed payload", zap.Error(err))
		return nil
	}
	applied, err := h.c.Projections.UpsertMember(ctx, projection)
	if err != nil {
		return fmt.Errorf("community_events: upsert member projection: %w", err)
	}
	if !applied {
		log.Debug("stale member version ignored", zap.Int64("member_version", projection.MemberVersion))
		return nil
	}

	switch {
	case payload.IsRemoval():
		return h.removeMember(ctx, message.Envelope, payload, log)
	case payload.Change == events.MemberChangeMuted:
		return h.revokePublishing(ctx, message.Envelope, payload, log)
	default:
		return nil
	}
}

// handleChannelChanged applies EVT-003 (rooms and room capacity).
func (h *CommunityEventConsumer) handleChannelChanged(ctx context.Context, message *events.WireMessage, log *zap.Logger) error {
	var payload events.ChannelChangedPayload
	if err := message.DecodePayload(&payload); err != nil {
		log.Error("dropping undecodable community_channel_changed payload", zap.Error(err))
		return nil
	}
	projection, err := payload.ChannelProjection(h.c.clock())
	if err != nil {
		log.Error("dropping invalid community_channel_changed payload", zap.Error(err))
		return nil
	}
	applied, err := h.c.Projections.UpsertChannel(ctx, projection)
	if err != nil {
		return fmt.Errorf("community_events: upsert channel projection: %w", err)
	}
	if !applied {
		log.Debug("stale channel version ignored", zap.Int64("channel_version", projection.ChannelVersion))
		return nil
	}
	if projection.Kind != models.ChannelKindVoice {
		return nil
	}

	switch payload.Change {
	case events.ChannelChangeDeleted:
		return h.closeChannel(ctx, message.Envelope, payload, log)
	case events.ChannelChangeCreated, events.ChannelChangeUpdated:
		capacity := models.MaxCapacity
		if payload.Capacity != nil {
			capacity = *payload.Capacity
		}
		if err := h.c.Rooms.UpsertRoom(ctx, &models.VoiceRoom{
			ChannelID:       projection.ChannelID,
			CommunityID:     projection.CommunityID,
			LiveKitRoomName: RoomName(projection.ChannelID),
			Capacity:        capacity,
		}); err != nil {
			return fmt.Errorf("community_events: register room capacity: %w", err)
		}
		return nil
	default:
		return nil
	}
}

// removeMember removes a removed or banned member from every live room of the
// community (US-008).
func (h *CommunityEventConsumer) removeMember(
	ctx context.Context,
	cause events.Envelope,
	payload events.MemberChangedPayload,
	log *zap.Logger,
) error {
	communityID, _ := uuid.Parse(payload.CommunityID)
	userID, _ := uuid.Parse(payload.UserID)
	sessions, err := h.c.Presence.ListActiveByUser(ctx, communityID, userID)
	if err != nil {
		return fmt.Errorf("community_events: load presence: %w", err)
	}
	for i := range sessions {
		session := sessions[i]
		room, err := h.c.Rooms.GetRoom(ctx, session.ChannelID)
		if errors.Is(err, repository.ErrNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("community_events: load room: %w", err)
		}
		removeErr := h.retry.Do(ctx, log, OperationRemoveParticipant, func(ctx context.Context) error {
			return h.c.LiveKit.RemoveParticipant(ctx, room.LiveKitRoomName, userID.String())
		})
		if removeErr != nil && !errors.Is(removeErr, livekit.ErrParticipantNotFound) &&
			!errors.Is(removeErr, livekit.ErrRoomNotFound) {
			return fmt.Errorf("community_events: remove participant: %w", removeErr)
		}
		now := h.c.clock()
		wasActive, err := h.c.Presence.MarkLeft(ctx, room.CommunityID, room.ChannelID, userID, now)
		if err != nil {
			return fmt.Errorf("community_events: mark removed participant left: %w", err)
		}
		if err := h.c.Seats.ReleaseUserSeats(ctx, room.ChannelID, userID); err != nil {
			return fmt.Errorf("community_events: release seat: %w", err)
		}
		if !wasActive {
			continue
		}
		h.c.recordParticipantMinutes(session, now)
		h.c.Metrics.RecordModeration(ModerationActionRemoved)
		if err := h.emit(ctx, room, session, events.ChangeLeft, cause); err != nil {
			return err
		}
		log.Info("removed member disconnected",
			zap.String("channel_id", room.ChannelID.String()),
			zap.String("member_change", payload.Change),
		)
	}
	return nil
}

// revokePublishing revokes publishing of a muted member in place (US-008).
func (h *CommunityEventConsumer) revokePublishing(
	ctx context.Context,
	cause events.Envelope,
	payload events.MemberChangedPayload,
	log *zap.Logger,
) error {
	communityID, _ := uuid.Parse(payload.CommunityID)
	userID, _ := uuid.Parse(payload.UserID)
	sessions, err := h.c.Presence.ListActiveByUser(ctx, communityID, userID)
	if err != nil {
		return fmt.Errorf("community_events: load presence: %w", err)
	}
	for i := range sessions {
		session := sessions[i]
		room, err := h.c.Rooms.GetRoom(ctx, session.ChannelID)
		if errors.Is(err, repository.ErrNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("community_events: load room: %w", err)
		}
		revokeErr := h.retry.Do(ctx, log, OperationUpdatePermissions, func(ctx context.Context) error {
			return h.c.LiveKit.UpdateParticipantPermissions(ctx, room.LiveKitRoomName, userID.String(), false)
		})
		if revokeErr != nil && !errors.Is(revokeErr, livekit.ErrParticipantNotFound) &&
			!errors.Is(revokeErr, livekit.ErrRoomNotFound) {
			return fmt.Errorf("community_events: revoke publishing: %w", revokeErr)
		}
		now := h.c.clock()
		wasActive, err := h.c.Presence.SetMedia(ctx, room.CommunityID, room.ChannelID, userID, false, false, false, now)
		if err != nil {
			return fmt.Errorf("community_events: update presence publishing: %w", err)
		}
		if !wasActive {
			continue
		}
		h.c.Metrics.RecordModeration(ModerationActionPublishRevoked)
		updated := session
		updated.CameraEnabled = false
		updated.ScreenShareEnabled = false
		updated.PublishingAllowed = false
		if err := h.emit(ctx, room, updated, events.ChangeMediaChanged, cause); err != nil {
			return err
		}
		log.Info("publishing revoked in place", zap.String("channel_id", room.ChannelID.String()))
	}
	return nil
}

// closeChannel closes the room of a deleted voice channel (US-009).
func (h *CommunityEventConsumer) closeChannel(
	ctx context.Context,
	cause events.Envelope,
	payload events.ChannelChangedPayload,
	log *zap.Logger,
) error {
	channelID, err := uuid.Parse(payload.ChannelID)
	if err != nil {
		return nil
	}
	room, err := h.c.Rooms.GetRoom(ctx, channelID)
	if errors.Is(err, repository.ErrNotFound) {
		// The room was never created: nothing to close.
		return nil
	}
	if err != nil {
		return fmt.Errorf("community_events: load room: %w", err)
	}
	if err := h.c.LiveKit.DeleteRoom(ctx, room.LiveKitRoomName); err != nil && !errors.Is(err, livekit.ErrRoomNotFound) {
		return fmt.Errorf("community_events: delete room: %w", err)
	}
	now := h.c.clock()
	left, err := h.c.Presence.MarkChannelLeft(ctx, room.CommunityID, room.ChannelID, now)
	if err != nil {
		return fmt.Errorf("community_events: close room presence: %w", err)
	}
	for i := range left {
		session := left[i]
		if err := h.c.Seats.ReleaseUserSeats(ctx, room.ChannelID, session.UserID); err != nil {
			return fmt.Errorf("community_events: release seat: %w", err)
		}
		h.c.recordParticipantMinutes(session, now)
		if err := h.emit(ctx, room, session, events.ChangeLeft, cause); err != nil {
			return err
		}
	}
	log.Info("voice channel room closed", zap.Int("participants_left", len(left)))
	return nil
}

// emit publishes one EVT-010 event caused by a consumed community event.
func (h *CommunityEventConsumer) emit(
	ctx context.Context,
	room *models.VoiceRoom,
	session models.Presence,
	change string,
	cause events.Envelope,
) error {
	count, err := h.c.Presence.CountActive(ctx, room.CommunityID, room.ChannelID)
	if err != nil {
		return fmt.Errorf("community_events: count participants: %w", err)
	}
	message, err := events.NewPresenceMessage(events.PresenceEvent{
		CommunityID:        room.CommunityID,
		ChannelID:          room.ChannelID,
		UserID:             session.UserID,
		Change:             change,
		CameraEnabled:      session.CameraEnabled,
		ScreenShareEnabled: session.ScreenShareEnabled,
		PublishingAllowed:  session.PublishingAllowed,
		ParticipantCount:   count,
		Capacity:           room.Capacity,
		Cause:              &cause,
		OccurredAt:         h.c.clock(),
	})
	if err != nil {
		return err
	}
	if err := h.produce.Publish(ctx, message); err != nil {
		return fmt.Errorf("community_events: publish presence event: %w", err)
	}
	if participants, rooms, err := h.c.Presence.CountTotal(ctx); err == nil {
		h.c.refreshGauges(participants, rooms, nil)
	}
	return nil
}
