package livekit

import (
	"testing"

	"github.com/livekit/protocol/livekit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRoomRequestCarriesTheCapacityBackstop covers the documented room
// creation rule: MaxParticipants equals the channel capacity, the backstop that
// makes LiveKit itself refuse an eleventh occupant.
func TestRoomRequestCarriesTheCapacityBackstop(t *testing.T) {
	t.Parallel()
	request := roomRequest(CreateRoomRequest{RoomName: "voice-abc", MaxParticipants: 7})
	assert.Equal(t, "voice-abc", request.GetName())
	assert.Equal(t, uint32(7), request.GetMaxParticipants())

	minimum := roomRequest(CreateRoomRequest{RoomName: "voice-min", MaxParticipants: 1})
	assert.Equal(t, uint32(1), minimum.GetMaxParticipants())
}

// TestNormalizeParticipantMapsMedia covers the untrusted SDK response mapping.
func TestNormalizeParticipantMapsMedia(t *testing.T) {
	t.Parallel()
	participant := normalizeParticipant(&livekit.ParticipantInfo{
		Identity: "user",
		Tracks: []*livekit.TrackInfo{
			{Source: livekit.TrackSource_CAMERA},
			{Source: livekit.TrackSource_SCREEN_SHARE},
			{Source: livekit.TrackSource_MICROPHONE, Muted: true},
		},
	})
	assert.Equal(t, "user", participant.Identity)
	assert.True(t, participant.CameraEnabled)
	assert.True(t, participant.ScreenShareEnabled)

	muted := normalizeParticipant(&livekit.ParticipantInfo{
		Identity: "quiet",
		Tracks: []*livekit.TrackInfo{
			{Source: livekit.TrackSource_CAMERA, Muted: true},
		},
	})
	assert.False(t, muted.CameraEnabled)
	assert.False(t, muted.ScreenShareEnabled)
	require.NotNil(t, muted)
}

// TestNotFoundAndAlreadyExistsHelpers covers the provider error mapping.
func TestNotFoundAndAlreadyExistsHelpers(t *testing.T) {
	t.Parallel()
	assert.True(t, notFound(assertAnError("room not found")))
	assert.False(t, notFound(nil))
	assert.True(t, alreadyExists(assertAnError("Room already exists")))
	assert.False(t, alreadyExists(assertAnError("permission denied")))
}

// assertAnError wraps a message into an error.
func assertAnError(message string) error { return &simpleError{message: message} }

// simpleError is a minimal error implementation for the helper tests.
type simpleError struct{ message string }

// Error implements the error interface.
func (e *simpleError) Error() string { return e.message }
