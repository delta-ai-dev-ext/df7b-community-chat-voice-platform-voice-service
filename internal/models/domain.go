package models

import (
	"time"

	"github.com/google/uuid"
)

// Voice room capacity bounds (docs/architecture.md §4).
const (
	// MinCapacity is the smallest allowed voice channel capacity.
	MinCapacity = 1
	// MaxCapacity is the largest allowed voice channel capacity.
	MaxCapacity = 10
)

// Member status values stored in member_projections.
const (
	// MemberStatusActive marks an active member.
	MemberStatusActive = "active"
	// MemberStatusRemoved marks a removed member.
	MemberStatusRemoved = "removed"
)

// Channel kinds stored in channel_projections.
const (
	// ChannelKindText marks a text channel.
	ChannelKindText = "text"
	// ChannelKindVoice marks a voice channel.
	ChannelKindVoice = "voice"
)

// VoiceRoom is the local capacity mapping of a voice channel to a LiveKit room.
type VoiceRoom struct {
	// ChannelID is the voice channel id (primary key).
	ChannelID uuid.UUID
	// CommunityID owns the channel.
	CommunityID uuid.UUID
	// LiveKitRoomName is the LiveKit room backing the channel.
	LiveKitRoomName string
	// Capacity is the seat count of the channel (1..10).
	Capacity int
	// CreatedAt is the row creation time.
	CreatedAt time.Time
	// UpdatedAt is the last mutation time.
	UpdatedAt time.Time
}

// SeatReservation is a short-lived claim on a channel seat.
type SeatReservation struct {
	// ReservationID identifies the reservation.
	ReservationID uuid.UUID
	// CommunityID owns the channel.
	CommunityID uuid.UUID
	// ChannelID is the voice channel.
	ChannelID uuid.UUID
	// UserID holds the reservation.
	UserID uuid.UUID
	// ReservedAt is when the seat was claimed.
	ReservedAt time.Time
	// ExpiresAt is the reservation TTL deadline.
	ExpiresAt time.Time
}

// SeatReservationRequest describes a seat claim.
type SeatReservationRequest struct {
	// CommunityID owns the channel.
	CommunityID uuid.UUID
	// ChannelID is the voice channel.
	ChannelID uuid.UUID
	// UserID requests the seat.
	UserID uuid.UUID
	// TTL is how long an unconnected reservation stays live.
	TTL time.Duration
}

// Presence is an active participant record in voice_participants.
type Presence struct {
	// CommunityID owns the channel.
	CommunityID uuid.UUID
	// ChannelID is the voice channel.
	ChannelID uuid.UUID
	// UserID is the participant identity in LiveKit.
	UserID uuid.UUID
	// SessionJoinedAt is the start of this presence session.
	SessionJoinedAt time.Time
	// LeftAt is set when the participant leaves.
	LeftAt *time.Time
	// CameraEnabled reports a published camera track.
	CameraEnabled bool
	// ScreenShareEnabled reports a published screen-share track.
	ScreenShareEnabled bool
	// PublishingAllowed is false for muted (listen-only) participants.
	PublishingAllowed bool
	// LastSeenAt is the last activity timestamp.
	LastSeenAt time.Time
}

// MemberProjection is the local view of a community member (EVT-002).
type MemberProjection struct {
	// CommunityID is part one of the key.
	CommunityID uuid.UUID
	// UserID is part two of the key.
	UserID uuid.UUID
	// Status is active or removed.
	Status string
	// IsBanned reports an active ban.
	IsBanned bool
	// MutedUntil is non-nil and in the future for muted members.
	MutedUntil *time.Time
	// MemberVersion orders member updates.
	MemberVersion int64
	// UpdatedAt is the last projection update time.
	UpdatedAt time.Time
}

// IsActive reports whether the projection describes an active member.
func (m MemberProjection) IsActive() bool {
	return m.Status == MemberStatusActive && !m.IsBanned
}

// IsMuted reports whether the member is muted at the given instant.
func (m MemberProjection) IsMuted(now time.Time) bool {
	return m.MutedUntil != nil && m.MutedUntil.After(now)
}

// ChannelProjection is the local view of a community channel (EVT-003).
type ChannelProjection struct {
	// CommunityID is part one of the key.
	CommunityID uuid.UUID
	// ChannelID is part two of the key.
	ChannelID uuid.UUID
	// Kind is text or voice.
	Kind string
	// Name is the display name (<= 100 chars).
	Name string
	// Capacity is set for voice channels only.
	Capacity *int
	// Deleted marks a deleted channel.
	Deleted bool
	// ChannelVersion orders channel updates.
	ChannelVersion int64
	// UpdatedAt is the last projection update time.
	UpdatedAt time.Time
}

// IsVoice reports whether the projection is a live voice channel.
func (c ChannelProjection) IsVoice() bool {
	return c.Kind == ChannelKindVoice && !c.Deleted
}

// ChannelSummary is the shared VoiceChannelSummary contract of API-002.
type ChannelSummary struct {
	// ChannelID is the voice channel id.
	ChannelID uuid.UUID `json:"channel_id"`
	// Name is the display name.
	Name string `json:"name"`
	// Capacity is the seat count (1..10).
	Capacity int `json:"capacity"`
}

// ChannelAlternative is one suggestion returned with a full-channel error.
type ChannelAlternative struct {
	// ChannelID is the alternative voice channel id.
	ChannelID uuid.UUID `json:"channel_id"`
	// Name is the display name.
	Name string `json:"name"`
	// Capacity is the seat count.
	Capacity int `json:"capacity"`
}

// AccessTokenClaims are the verified identity claims of the caller.
type AccessTokenClaims struct {
	// UserID is the sub claim (user id).
	UserID uuid.UUID
	// SessionID is the sid claim (session id).
	SessionID string
	// EmailVerified mirrors the email_verified claim.
	EmailVerified bool
	// Issuer is the iss claim.
	Issuer string
	// Audience is the aud claim.
	Audience []string
	// ExpiresAt is the exp claim.
	ExpiresAt time.Time
	// IssuedAt is the iat claim.
	IssuedAt time.Time
	// TraceID is the trace id derived from the inbound request.
	TraceID string
}
