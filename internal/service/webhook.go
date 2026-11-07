package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	livekitproto "github.com/livekit/protocol/livekit"
	"go.uber.org/zap"

	"voice_service/internal/events"
	"voice_service/internal/livekit"
	"voice_service/internal/models"
	"voice_service/internal/repository"
)

// LiveKit webhook event names mapped onto presence by this service.
const (
	// WebhookParticipantJoined reports a participant entering a room.
	WebhookParticipantJoined = "participant_joined"
	// WebhookParticipantLeft reports a participant leaving a room.
	WebhookParticipantLeft = "participant_left"
	// WebhookTrackPublished reports a newly published track.
	WebhookTrackPublished = "track_published"
	// WebhookTrackUnpublished reports an unpublished track.
	WebhookTrackUnpublished = "track_unpublished"
	// WebhookRoomFinished reports that a room closed.
	WebhookRoomFinished = "room_finished"
)

// WebhookProcessor maps verified LiveKit webhooks onto presence state and
// EVT-010 (docs/architecture.md §3.3).
type WebhookProcessor struct {
	c Collaborators
}

// NewWebhookProcessor builds the webhook processor.
func NewWebhookProcessor(c Collaborators) *WebhookProcessor { return &WebhookProcessor{c: c} }

// Process applies one verified webhook event. Duplicates (same webhook event
// id) are ignored. The trace id is minted at the webhook edge by the handler
// because no originating user request exists.
func (p *WebhookProcessor) Process(ctx context.Context, event *livekitproto.WebhookEvent, traceID string) error {
	if event == nil {
		return fmt.Errorf("%w: nil webhook event", livekit.ErrMalformedWebhook)
	}
	if event.GetId() == "" {
		return fmt.Errorf("%w: missing webhook event id", livekit.ErrMalformedWebhook)
	}
	log := p.c.log().With(
		zap.String("operation", "livekit_webhook"),
		zap.String("trace_id", traceID),
		zap.String("webhook_event_id", event.GetId()),
		zap.String("event_kind", event.GetEvent()),
	)

	first, err := p.c.Dedupe.MarkWebhookProcessed(ctx, event.GetId(), event.GetEvent())
	if err != nil {
		return fmt.Errorf("webhook: record event id: %w", err)
	}
	if !first {
		log.Debug("duplicate webhook ignored")
		return nil
	}
	if err := p.apply(ctx, event, traceID, log); err != nil {
		// Drop the dedupe row so LiveKit's retry can process the event again.
		if forgetErr := p.c.Dedupe.ForgetWebhook(ctx, event.GetId()); forgetErr != nil {
			log.Warn("could not forget failed webhook event id", zap.Error(forgetErr))
		}
		return err
	}
	return nil
}

// apply performs the presence change of one webhook event.
func (p *WebhookProcessor) apply(
	ctx context.Context,
	event *livekitproto.WebhookEvent,
	traceID string,
	log *zap.Logger,
) error {
	roomName := event.GetRoom().GetName()
	if roomName == "" {
		log.Warn("webhook without room name ignored")
		return nil
	}
	room, err := p.c.Rooms.GetRoomByLiveKitName(ctx, roomName)
	if errors.Is(err, repository.ErrNotFound) {
		log.Warn("webhook for unknown room ignored")
		return nil
	}
	if err != nil {
		return fmt.Errorf("webhook: load room: %w", err)
	}
	now := p.c.clock()
	switch event.GetEvent() {
	case WebhookParticipantJoined:
		return p.participantJoined(ctx, room, event, traceID, now, log)
	case WebhookParticipantLeft:
		return p.participantLeft(ctx, room, event, traceID, now, log)
	case WebhookTrackPublished, WebhookTrackUnpublished:
		return p.trackChanged(ctx, room, event, traceID, now, log)
	case WebhookRoomFinished:
		return p.roomFinished(ctx, room, traceID, now, log)
	default:
		log.Debug("webhook event kind ignored")
		return nil
	}
}

// participantJoined records a participant entering the room.
func (p *WebhookProcessor) participantJoined(
	ctx context.Context,
	room *models.VoiceRoom,
	event *livekitproto.WebhookEvent,
	traceID string,
	now time.Time,
	log *zap.Logger,
) error {
	userID, ok := participantUserID(event)
	if !ok {
		log.Debug("ignoring non-user participant")
		return nil
	}
	camera, screen := trackFlags(event.GetParticipant().GetTracks())
	presence, err := p.c.Presence.UpsertActive(ctx, &models.Presence{
		CommunityID:        room.CommunityID,
		ChannelID:          room.ChannelID,
		UserID:             userID,
		SessionJoinedAt:    participantJoinedAt(event.GetParticipant(), now),
		CameraEnabled:      camera,
		ScreenShareEnabled: screen,
		PublishingAllowed:  publishingGranted(event.GetParticipant()),
		LastSeenAt:         now,
	})
	if err != nil {
		return fmt.Errorf("webhook: record participant join: %w", err)
	}
	if err := p.c.Seats.ReleaseUserSeats(ctx, room.ChannelID, userID); err != nil {
		return fmt.Errorf("webhook: release seat reservation: %w", err)
	}
	return p.emit(ctx, room, presence, events.ChangeJoined, traceID, nil)
}

// participantLeft closes the presence session of a departing participant.
func (p *WebhookProcessor) participantLeft(
	ctx context.Context,
	room *models.VoiceRoom,
	event *livekitproto.WebhookEvent,
	traceID string,
	now time.Time,
	log *zap.Logger,
) error {
	userID, ok := participantUserID(event)
	if !ok {
		log.Debug("ignoring non-user participant")
		return nil
	}
	session, err := p.session(ctx, room, userID)
	if err != nil {
		return err
	}
	wasActive, err := p.c.Presence.MarkLeft(ctx, room.CommunityID, room.ChannelID, userID, now)
	if err != nil {
		return fmt.Errorf("webhook: mark participant left: %w", err)
	}
	if err := p.c.Seats.ReleaseUserSeats(ctx, room.ChannelID, userID); err != nil {
		return fmt.Errorf("webhook: release seat: %w", err)
	}
	if !wasActive {
		log.Debug("participant left for an unknown session")
		return nil
	}
	if session != nil {
		p.c.recordParticipantMinutes(*session, now)
	}
	return p.emit(ctx, room, session, events.ChangeLeft, traceID, nil)
}

// trackChanged maps camera and screen-share toggles onto presence flags.
func (p *WebhookProcessor) trackChanged(
	ctx context.Context,
	room *models.VoiceRoom,
	event *livekitproto.WebhookEvent,
	traceID string,
	now time.Time,
	log *zap.Logger,
) error {
	userID, ok := participantUserID(event)
	if !ok {
		log.Debug("ignoring non-user participant")
		return nil
	}
	published := event.GetEvent() == WebhookTrackPublished
	var camera, screen *bool
	switch event.GetTrack().GetSource() {
	case livekitproto.TrackSource_CAMERA:
		camera = &published
	case livekitproto.TrackSource_SCREEN_SHARE, livekitproto.TrackSource_SCREEN_SHARE_AUDIO:
		screen = &published
	default:
		// Microphone tracks do not change a presence flag.
		return nil
	}

	session, err := p.session(ctx, room, userID)
	if err != nil {
		return err
	}
	if session == nil {
		log.Debug("media change for an unknown session ignored")
		return nil
	}
	cameraEnabled := session.CameraEnabled
	if camera != nil {
		cameraEnabled = *camera
	}
	screenEnabled := session.ScreenShareEnabled
	if screen != nil {
		screenEnabled = *screen
	}
	wasActive, err := p.c.Presence.SetMedia(ctx, room.CommunityID, room.ChannelID, userID,
		cameraEnabled, screenEnabled, session.PublishingAllowed, now)
	if err != nil {
		return fmt.Errorf("webhook: update presence media: %w", err)
	}
	if !wasActive {
		return nil
	}
	updated := *session
	updated.CameraEnabled = cameraEnabled
	updated.ScreenShareEnabled = screenEnabled
	return p.emit(ctx, room, &updated, events.ChangeMediaChanged, traceID, nil)
}

// roomFinished closes every presence session of a finished room.
func (p *WebhookProcessor) roomFinished(
	ctx context.Context,
	room *models.VoiceRoom,
	traceID string,
	now time.Time,
	log *zap.Logger,
) error {
	left, err := p.c.Presence.MarkChannelLeft(ctx, room.CommunityID, room.ChannelID, now)
	if err != nil {
		return fmt.Errorf("webhook: close room presence: %w", err)
	}
	for i := range left {
		session := left[i]
		if err := p.c.Seats.ReleaseUserSeats(ctx, room.ChannelID, session.UserID); err != nil {
			return fmt.Errorf("webhook: release seat after room finish: %w", err)
		}
		p.c.recordParticipantMinutes(session, now)
	}
	for i := range left {
		session := left[i]
		if err := p.emit(ctx, room, &session, events.ChangeLeft, traceID, nil); err != nil {
			return err
		}
	}
	log.Info("room finished", zap.Int("participants_left", len(left)))
	return nil
}

// session loads the active presence row of a user in a room, if any.
func (p *WebhookProcessor) session(ctx context.Context, room *models.VoiceRoom, userID uuid.UUID) (*models.Presence, error) {
	sessions, err := p.c.Presence.ListActiveByUser(ctx, room.CommunityID, userID)
	if err != nil {
		return nil, fmt.Errorf("webhook: load presence: %w", err)
	}
	for i := range sessions {
		if sessions[i].ChannelID == room.ChannelID {
			return &sessions[i], nil
		}
	}
	return nil, nil
}

// emit publishes one EVT-010 event for a presence change.
func (p *WebhookProcessor) emit(
	ctx context.Context,
	room *models.VoiceRoom,
	presence *models.Presence,
	change string,
	traceID string,
	cause *events.Envelope,
) error {
	count, err := p.c.Presence.CountActive(ctx, room.CommunityID, room.ChannelID)
	if err != nil {
		return fmt.Errorf("webhook: count participants: %w", err)
	}
	event := events.PresenceEvent{
		CommunityID:      room.CommunityID,
		ChannelID:        room.ChannelID,
		UserID:           presenceUserID(presence, room.ChannelID),
		Change:           change,
		ParticipantCount: count,
		Capacity:         room.Capacity,
		TraceID:          traceID,
		Cause:            cause,
		OccurredAt:       p.c.clock(),
	}
	if presence != nil {
		event.UserID = presence.UserID
		event.CameraEnabled = presence.CameraEnabled
		event.ScreenShareEnabled = presence.ScreenShareEnabled
		event.PublishingAllowed = presence.PublishingAllowed
	}
	message, err := events.NewPresenceMessage(event)
	if err != nil {
		return err
	}
	if err := p.c.Producer.Publish(ctx, message); err != nil {
		return fmt.Errorf("webhook: publish presence event: %w", err)
	}
	if participants, rooms, err := p.c.Presence.CountTotal(ctx); err == nil {
		p.c.refreshGauges(participants, rooms, nil)
	}
	return nil
}

// presenceUserID falls back to the nil uuid when no presence row is known.
func presenceUserID(presence *models.Presence, _ uuid.UUID) uuid.UUID {
	if presence == nil {
		return uuid.Nil
	}
	return presence.UserID
}

// participantUserID extracts the user id of a participant, ignoring agent and
// service participants whose identity is not a user id.
func participantUserID(event *livekitproto.WebhookEvent) (uuid.UUID, bool) {
	participant := event.GetParticipant()
	if participant == nil {
		return uuid.Nil, false
	}
	if participant.GetKind() != livekitproto.ParticipantInfo_STANDARD {
		return uuid.Nil, false
	}
	userID, err := uuid.Parse(participant.GetIdentity())
	if err != nil {
		return uuid.Nil, false
	}
	return userID, true
}

// trackFlags derives the camera and screen-share flags of a participant.
func trackFlags(tracks []*livekitproto.TrackInfo) (camera, screen bool) {
	for _, track := range tracks {
		if track.GetMuted() {
			continue
		}
		switch track.GetSource() {
		case livekitproto.TrackSource_CAMERA:
			camera = true
		case livekitproto.TrackSource_SCREEN_SHARE, livekitproto.TrackSource_SCREEN_SHARE_AUDIO:
			screen = true
		}
	}
	return camera, screen
}

// publishingGranted reports whether the participant may publish media. A
// missing permission block defaults to granted, matching LiveKit's default.
func publishingGranted(participant *livekitproto.ParticipantInfo) bool {
	if participant.GetPermission() == nil {
		return true
	}
	return participant.GetPermission().GetCanPublish()
}

// participantJoinedAt converts the LiveKit join timestamp, falling back to now.
func participantJoinedAt(participant *livekitproto.ParticipantInfo, now time.Time) time.Time {
	if ms := participant.GetJoinedAtMs(); ms > 0 {
		return time.UnixMilli(ms).UTC()
	}
	return now
}
