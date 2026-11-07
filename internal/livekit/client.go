// Package livekit adapts LiveKit Cloud (official Go server SDK, v2) to the
// small domain interface used by voice_service.
package livekit

import (
	"context"
	"errors"
	"time"
)

// MaxParticipantTokenTTL is the hard upper bound of minted participant tokens
// (pinned decision: short-lived, <= 5 minutes).
const MaxParticipantTokenTTL = 5 * time.Minute

// Errors reported by the LiveKit adapters.
var (
	// ErrRoomNotFound reports that LiveKit does not know the room.
	ErrRoomNotFound = errors.New("livekit: room not found")
	// ErrParticipantNotFound reports that LiveKit does not know the participant.
	ErrParticipantNotFound = errors.New("livekit: participant not found")
	// ErrInvalidSignature reports an unverifiable webhook Authorization header.
	ErrInvalidSignature = errors.New("livekit: invalid webhook signature")
	// ErrMalformedWebhook reports a signed but undecodable webhook payload.
	ErrMalformedWebhook = errors.New("livekit: malformed webhook payload")
)

// Participant is a room participant normalized out of the SDK types. SDK
// responses are untrusted input at this boundary.
type Participant struct {
	// Identity is the token identity (the user id).
	Identity string
	// CameraEnabled reports a published, unmuted camera track.
	CameraEnabled bool
	// ScreenShareEnabled reports a published, unmuted screen-share track.
	ScreenShareEnabled bool
}

// CreateRoomRequest describes a room to create.
type CreateRoomRequest struct {
	// RoomName is the LiveKit room backing the voice channel.
	RoomName string
	// MaxParticipants is the capacity backstop of the room (1..10).
	MaxParticipants int
}

// TokenRequest describes a participant token to mint.
type TokenRequest struct {
	// RoomName is the room the participant joins.
	RoomName string
	// Identity is the participant identity (the user id).
	Identity string
	// Name is the display name carried in the token, if any.
	Name string
	// CanPublish grants microphone, camera, and screen-share publishing.
	CanPublish bool
	// TTL is the token lifetime; it is capped at MaxParticipantTokenTTL.
	TTL time.Duration
}

// Token is a minted participant token.
type Token struct {
	// Value is the signed JWT handed to the client.
	Value string
	// ExpiresAt is when the token expires.
	ExpiresAt time.Time
}

// RoomClient performs the outbound LiveKit room operations of this service.
type RoomClient interface {
	// CreateRoom creates the room with its max-participant limit. An existing
	// room is not an error.
	CreateRoom(ctx context.Context, req CreateRoomRequest) error
	// DeleteRoom closes the room and disconnects its participants.
	DeleteRoom(ctx context.Context, roomName string) error
	// RemoveParticipant disconnects one participant.
	RemoveParticipant(ctx context.Context, roomName, identity string) error
	// UpdateParticipantPermissions grants or revokes publishing in place.
	UpdateParticipantPermissions(ctx context.Context, roomName, identity string, canPublish bool) error
	// ListParticipants returns the current participants of a room.
	ListParticipants(ctx context.Context, roomName string) ([]Participant, error)
}

// TokenMinter mints short-lived LiveKit participant tokens.
type TokenMinter interface {
	// MintParticipantToken mints one participant token.
	MintParticipantToken(ctx context.Context, req TokenRequest) (Token, error)
}
