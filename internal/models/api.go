package models

import (
	"time"

	"github.com/google/uuid"
)

// JoinRequest is the body of POST /voice/channels/{channel_id}/join.
type JoinRequest struct {
	// CommunityID is the community that owns the voice channel.
	CommunityID uuid.UUID `json:"community_id"`
}

// JoinResponse is the 200 body of POST /voice/channels/{channel_id}/join.
type JoinResponse struct {
	// LiveKitURL is the LiveKit endpoint the client connects to directly.
	LiveKitURL string `json:"livekit_url"`
	// Token is the short-lived participant token.
	Token string `json:"token"`
	// RoomName is the LiveKit room backing the channel.
	RoomName string `json:"room_name"`
	// CanPublishAudio reports whether microphone publishing is granted.
	CanPublishAudio bool `json:"can_publish_audio"`
	// CanPublishVideo reports whether camera publishing is granted.
	CanPublishVideo bool `json:"can_publish_video"`
	// CanPublishScreen reports whether screen sharing is granted.
	CanPublishScreen bool `json:"can_publish_screen"`
	// MutedUntil is set when the member joined listen-only.
	MutedUntil *time.Time `json:"muted_until"`
	// TokenExpiresAt is when the participant token expires.
	TokenExpiresAt time.Time `json:"token_expires_at"`
}

// ErrorResponse is the shared error body {"error","detail"}.
type ErrorResponse struct {
	// Error is the stable snake_case error code.
	Error string `json:"error"`
	// Detail is the human-readable explanation.
	Detail string `json:"detail"`
}

// ChannelFullResponse is the 409 body listing seat alternatives.
type ChannelFullResponse struct {
	// Error is always voice_channel_full.
	Error string `json:"error"`
	// Detail is the human-readable explanation.
	Detail string `json:"detail"`
	// Alternatives lists other voice channels with free seats.
	Alternatives []ChannelAlternative `json:"alternatives"`
}

// PresenceParticipant is one participant of a voice channel.
type PresenceParticipant struct {
	// UserID is the participant identity.
	UserID uuid.UUID `json:"user_id"`
	// JoinedAt is when the current presence session started.
	JoinedAt time.Time `json:"joined_at"`
	// CameraEnabled reports a published camera track.
	CameraEnabled bool `json:"camera_enabled"`
	// ScreenShareEnabled reports a published screen-share track.
	ScreenShareEnabled bool `json:"screen_share_enabled"`
	// PublishingAllowed is false for muted participants.
	PublishingAllowed bool `json:"publishing_allowed"`
}

// ChannelPresence is the presence snapshot of one voice channel.
type ChannelPresence struct {
	// ChannelID is the voice channel id.
	ChannelID uuid.UUID `json:"channel_id"`
	// Name is the display name.
	Name string `json:"name"`
	// Capacity is the seat count.
	Capacity int `json:"capacity"`
	// SeatsUsed is the number of present participants.
	SeatsUsed int `json:"seats_used"`
	// Participants are the present participants.
	Participants []PresenceParticipant `json:"participants"`
}

// PresenceResponse is the body of GET /communities/{community_id}/voice-presence.
type PresenceResponse struct {
	// CommunityID is the community of the snapshot.
	CommunityID uuid.UUID `json:"community_id"`
	// Channels holds one entry per voice channel.
	Channels []ChannelPresence `json:"channels"`
}
