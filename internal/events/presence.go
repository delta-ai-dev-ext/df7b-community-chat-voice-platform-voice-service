package events

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"voice_service/internal/models"
)

// Presence change kinds of EVT-010.
const (
	// ChangeJoined reports a participant entering the channel.
	ChangeJoined = "joined"
	// ChangeLeft reports a participant leaving the channel.
	ChangeLeft = "left"
	// ChangeMediaChanged reports a camera or screen-share toggle.
	ChangeMediaChanged = "media_changed"
)

// PresencePayload is the EVT-010 payload contract.
type PresencePayload struct {
	// CommunityID owns the channel.
	CommunityID string `json:"community_id"`
	// ChannelID is the voice channel.
	ChannelID string `json:"channel_id"`
	// UserID is the participant whose presence changed.
	UserID string `json:"user_id"`
	// Change is joined, left, or media_changed.
	Change string `json:"change"`
	// CameraEnabled reports a published camera track.
	CameraEnabled bool `json:"camera_enabled"`
	// ScreenShareEnabled reports a published screen-share track.
	ScreenShareEnabled bool `json:"screen_share_enabled"`
	// PublishingAllowed is false for muted (listen-only) participants.
	PublishingAllowed bool `json:"publishing_allowed"`
	// ParticipantCount is the occupant count after the change.
	ParticipantCount int `json:"participant_count"`
	// Capacity is the channel seat count.
	Capacity int `json:"capacity"`
}

// PresenceEvent describes one EVT-010 emission.
type PresenceEvent struct {
	// CommunityID owns the channel.
	CommunityID uuid.UUID
	// ChannelID is the voice channel.
	ChannelID uuid.UUID
	// UserID is the participant whose presence changed.
	UserID uuid.UUID
	// Change is joined, left, or media_changed.
	Change string
	// CameraEnabled reports a published camera track.
	CameraEnabled bool
	// ScreenShareEnabled reports a published screen-share track.
	ScreenShareEnabled bool
	// PublishingAllowed is false for muted participants.
	PublishingAllowed bool
	// ParticipantCount is the occupant count after the change.
	ParticipantCount int
	// Capacity is the channel seat count.
	Capacity int
	// Cause is the consumed event that triggered this emission, if any. When
	// set, trace_id and correlation_id are copied and causation_id is its id.
	Cause *Envelope
	// TraceID is used when Cause is nil; minted when empty.
	TraceID string
	// CorrelationID is used when Cause is nil; minted when empty.
	CorrelationID string
	// OccurredAt defaults to now when zero.
	OccurredAt time.Time
}

// NewPresenceMessage builds an EVT-010 message keyed by channel id.
func NewPresenceMessage(in PresenceEvent) (Message, error) {
	switch in.Change {
	case ChangeJoined, ChangeLeft, ChangeMediaChanged:
	default:
		return Message{}, fmt.Errorf("invalid presence change %q", in.Change)
	}
	if in.CommunityID == uuid.Nil || in.ChannelID == uuid.Nil || in.UserID == uuid.Nil {
		return Message{}, fmt.Errorf("presence event requires community, channel, and user ids")
	}

	opts := EnvelopeOptions{
		EventType:     EventTypeVoicePresenceChanged,
		EventVersion:  DefaultEventVersion,
		TraceID:       in.TraceID,
		CorrelationID: in.CorrelationID,
		OccurredAt:    in.OccurredAt,
	}
	if in.Cause != nil {
		opts.TraceID = in.Cause.TraceID
		opts.CorrelationID = in.Cause.CorrelationID
		causeID := in.Cause.EventID
		opts.CausationID = &causeID
	}
	env, err := NewEnvelope(opts)
	if err != nil {
		return Message{}, err
	}

	count := in.ParticipantCount
	if count < 0 {
		count = 0
	}
	if count > models.MaxCapacity {
		count = models.MaxCapacity
	}
	capacity := in.Capacity
	if capacity < models.MinCapacity || capacity > models.MaxCapacity {
		return Message{}, fmt.Errorf("presence event capacity %d is out of range", in.Capacity)
	}

	return Message{
		Envelope: env,
		Payload: PresencePayload{
			CommunityID:        in.CommunityID.String(),
			ChannelID:          in.ChannelID.String(),
			UserID:             in.UserID.String(),
			Change:             in.Change,
			CameraEnabled:      in.CameraEnabled,
			ScreenShareEnabled: in.ScreenShareEnabled,
			PublishingAllowed:  in.PublishingAllowed,
			ParticipantCount:   count,
			Capacity:           capacity,
		},
		Key: in.ChannelID.String(),
	}, nil
}
