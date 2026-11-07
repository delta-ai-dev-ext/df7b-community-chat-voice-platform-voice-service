// Package models holds the domain types, typed errors, and request/response
// payloads of voice_service.
package models

import "errors"

// Stable machine-readable error codes exposed by the HTTP API
// (see docs/openapi_spec.yaml).
const (
	// ErrCodeNotAMember is returned when the caller is not an active member.
	ErrCodeNotAMember = "not_a_member"
	// ErrCodeChannelNotFound is returned for unknown channels.
	ErrCodeChannelNotFound = "channel_not_found"
	// ErrCodeNotAVoiceChannel is returned when the channel is not a voice channel.
	ErrCodeNotAVoiceChannel = "not_a_voice_channel"
	// ErrCodeVoiceChannelFull is returned when no seat is available.
	ErrCodeVoiceChannelFull = "voice_channel_full"
	// ErrCodeAuthorizationUnavailable is returned when API-002 cannot be used.
	ErrCodeAuthorizationUnavailable = "voice_authorization_unavailable"
	// ErrCodeMediaUnavailable is returned when LiveKit cannot serve the join.
	ErrCodeMediaUnavailable = "voice_media_unavailable"
	// ErrCodeUnauthorized is returned for missing or invalid access tokens.
	ErrCodeUnauthorized = "unauthorized"
	// ErrCodeInvalidRequest is returned for malformed requests.
	ErrCodeInvalidRequest = "invalid_request"
	// ErrCodeInvalidSignature is returned for unverifiable webhooks.
	ErrCodeInvalidSignature = "invalid_signature"
	// ErrCodeInternal is returned for unexpected server errors.
	ErrCodeInternal = "internal_error"
	// ErrCodeShuttingDown is used by readiness while draining.
	ErrCodeShuttingDown = "shutting_down"
)

// Domain errors shared by the service and handler layers.
var (
	// ErrNotAMember reports that the caller is not an active community member.
	ErrNotAMember = errors.New(ErrCodeNotAMember)
	// ErrChannelNotFound reports an unknown channel.
	ErrChannelNotFound = errors.New(ErrCodeChannelNotFound)
	// ErrNotAVoiceChannel reports that the channel is not a voice channel.
	ErrNotAVoiceChannel = errors.New(ErrCodeNotAVoiceChannel)
	// ErrAuthorizationUnavailable reports a failed-closed API-002 check.
	ErrAuthorizationUnavailable = errors.New(ErrCodeAuthorizationUnavailable)
	// ErrMediaUnavailable reports that LiveKit could not serve the join.
	ErrMediaUnavailable = errors.New(ErrCodeMediaUnavailable)
	// ErrUnauthorized reports a missing, invalid, or expired access token.
	ErrUnauthorized = errors.New(ErrCodeUnauthorized)
	// ErrInvalidRequest reports a malformed request.
	ErrInvalidRequest = errors.New(ErrCodeInvalidRequest)
)

// ChannelFullError reports an exhausted voice channel together with the
// community's other voice channels that still have free seats.
type ChannelFullError struct {
	// Alternatives are the channels with at least one free seat.
	Alternatives []ChannelAlternative
}

// Error implements the error interface.
func (e *ChannelFullError) Error() string { return ErrCodeVoiceChannelFull }
