// Package service implements the business logic of voice_service: the join
// orchestrator, leave, presence, webhook processing, event consumption, and the
// reconciliation worker.
package service

import (
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"voice_service/internal/community"
	"voice_service/internal/events"
	"voice_service/internal/livekit"
	"voice_service/internal/models"
	"voice_service/internal/repository"
)

// Collaborators bundles the injected dependencies of the service layer. Each
// service uses the subset it needs; unused fields are simply ignored.
type Collaborators struct {
	// Access performs the API-002 voice access check.
	Access community.AccessChecker
	// Rooms stores the voice channel to LiveKit room mapping.
	Rooms repository.RoomRepository
	// Seats enforces channel capacity transactionally.
	Seats repository.SeatRepository
	// Presence stores active participants.
	Presence repository.PresenceRepository
	// Projections stores the event-derived member and channel views.
	Projections repository.ProjectionRepository
	// Dedupe stores consumed event and webhook ids.
	Dedupe repository.DedupeRepository
	// LiveKit performs outbound room operations.
	LiveKit livekit.RoomClient
	// Tokens mints participant tokens.
	Tokens livekit.TokenMinter
	// Producer publishes EVT-010.
	Producer events.Producer
	// Metrics collects business and technical metrics.
	Metrics *Metrics
	// Log is the structured logger; defaults to a no-op logger.
	Log *zap.Logger
	// Now is the clock; defaults to time.Now.
	Now func() time.Time
	// LiveKitURL is the endpoint handed to clients.
	LiveKitURL string
	// SeatTTL is the lifetime of a seat reservation.
	SeatTTL time.Duration
	// TokenTTL is the lifetime of minted participant tokens.
	TokenTTL time.Duration
}

// clock returns the injected clock in UTC.
func (c Collaborators) clock() time.Time {
	if c.Now == nil {
		return time.Now().UTC()
	}
	return c.Now().UTC()
}

// log returns the injected logger or a no-op logger.
func (c Collaborators) log() *zap.Logger {
	if c.Log == nil {
		return zap.NewNop()
	}
	return c.Log
}

// RoomName derives the LiveKit room name of a voice channel. It is
// deterministic so every replica and every consumer resolves the same room.
func RoomName(channelID uuid.UUID) string {
	return "voice-" + channelID.String()
}

// gaugeRefresher reloads the presence gauges after a presence change.
func (c Collaborators) refreshGauges(participants, rooms int, err error) {
	if c.Metrics == nil || err != nil {
		return
	}
	c.Metrics.SetActivePresence(participants, rooms)
}

// recordParticipantMinutes adds the media minutes of a finished session.
func (c Collaborators) recordParticipantMinutes(presence models.Presence, leftAt time.Time) {
	if c.Metrics == nil {
		return
	}
	ended := leftAt
	if presence.LeftAt != nil {
		ended = *presence.LeftAt
	}
	c.Metrics.RecordParticipantMinutes(ended.Sub(presence.SessionJoinedAt), presence.CameraEnabled, presence.ScreenShareEnabled)
}
