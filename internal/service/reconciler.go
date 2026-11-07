package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"voice_service/internal/events"
	"voice_service/internal/livekit"
	"voice_service/internal/models"
)

// ErrSweepInFlight reports that a reconciliation sweep is already running.
var ErrSweepInFlight = errors.New("reconcile: sweep already in flight")

// Reconciler diffs local presence against LiveKit's participant list and heals
// drift from missed webhooks (US-010). It never mutates local state when
// LiveKit cannot be reached.
type Reconciler struct {
	c         Collaborators
	interval  time.Duration
	retention time.Duration
	mutex     sync.Mutex
	running   bool
}

// NewReconciler builds the reconciliation worker. interval is the sweep period
// and retention the dedupe window of processed Kafka event ids.
func NewReconciler(c Collaborators, interval, retention time.Duration) *Reconciler {
	return &Reconciler{c: c, interval: interval, retention: retention}
}

// RunPeriodic sweeps until ctx is cancelled, honoring the shutdown context.
func (r *Reconciler) RunPeriodic(ctx context.Context) {
	if r.interval <= 0 {
		return
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.Sweep(ctx); err != nil && !errors.Is(err, ErrSweepInFlight) && ctx.Err() == nil {
				r.c.log().Warn("reconciliation sweep failed", zap.Error(err))
			}
		}
	}
}

// Sweep runs one reconciliation pass. It is single-flight: a second caller
// returns ErrSweepInFlight instead of running concurrently.
func (r *Reconciler) Sweep(ctx context.Context) error {
	if !r.tryStart() {
		return ErrSweepInFlight
	}
	defer r.finish()

	log := r.c.log().With(zap.String("operation", "reconcile_presence"))
	traceID := events.NewTraceID()

	rooms, err := r.c.Rooms.ListRoomsWithPresence(ctx)
	if err != nil {
		return err
	}
	for i := range rooms {
		room := rooms[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.reconcileRoom(ctx, room, traceID, log); err != nil {
			// A single unreachable room never aborts the sweep and never
			// mutates local state.
			log.Warn("room reconciliation skipped",
				zap.String("channel_id", room.ChannelID.String()),
				zap.Error(err),
			)
		}
	}
	r.purge(ctx, log)
	return nil
}

// reconcileRoom diffs one room against LiveKit.
func (r *Reconciler) reconcileRoom(ctx context.Context, room models.VoiceRoom, traceID string, log *zap.Logger) error {
	remote, err := r.c.LiveKit.ListParticipants(ctx, room.LiveKitRoomName)
	if err != nil {
		if errors.Is(err, livekit.ErrRoomNotFound) {
			return nil
		}
		return err
	}
	local, err := r.c.Presence.ListActiveByChannel(ctx, room.CommunityID, room.ChannelID)
	if err != nil {
		return err
	}

	remoteUsers := make(map[uuid.UUID]livekit.Participant, len(remote))
	for _, participant := range remote {
		userID, parseErr := uuid.Parse(participant.Identity)
		if parseErr != nil {
			continue
		}
		remoteUsers[userID] = participant
	}
	localUsers := make(map[uuid.UUID]models.Presence, len(local))
	for _, presence := range local {
		localUsers[presence.UserID] = presence
	}

	for userID, presence := range localUsers {
		if _, present := remoteUsers[userID]; present {
			continue
		}
		now := r.c.clock()
		wasActive, err := r.c.Presence.MarkLeft(ctx, room.CommunityID, room.ChannelID, userID, now)
		if err != nil {
			return err
		}
		if err := r.c.Seats.ReleaseUserSeats(ctx, room.ChannelID, userID); err != nil {
			return err
		}
		if !wasActive {
			continue
		}
		r.c.recordParticipantMinutes(presence, now)
		if err := r.emit(ctx, room, presence, events.ChangeLeft, traceID); err != nil {
			return err
		}
		log.Info("reconciled stale participant",
			zap.String("channel_id", room.ChannelID.String()),
			zap.String("user_id", userID.String()),
		)
	}

	for userID, participant := range remoteUsers {
		if _, present := localUsers[userID]; present {
			continue
		}
		now := r.c.clock()
		presence, err := r.c.Presence.UpsertActive(ctx, &models.Presence{
			CommunityID:        room.CommunityID,
			ChannelID:          room.ChannelID,
			UserID:             userID,
			SessionJoinedAt:    now,
			CameraEnabled:      participant.CameraEnabled,
			ScreenShareEnabled: participant.ScreenShareEnabled,
			PublishingAllowed:  true,
			LastSeenAt:         now,
		})
		if err != nil {
			return err
		}
		if err := r.emit(ctx, room, *presence, events.ChangeJoined, traceID); err != nil {
			return err
		}
		log.Info("reconciled missed participant",
			zap.String("channel_id", room.ChannelID.String()),
			zap.String("user_id", userID.String()),
		)
	}
	return nil
}

// emit publishes one reconciliation EVT-010 event; the trace id is minted at
// the scheduled edge because no user request caused it.
func (r *Reconciler) emit(ctx context.Context, room models.VoiceRoom, presence models.Presence, change, traceID string) error {
	count, err := r.c.Presence.CountActive(ctx, room.CommunityID, room.ChannelID)
	if err != nil {
		return err
	}
	message, err := events.NewPresenceMessage(events.PresenceEvent{
		CommunityID:        room.CommunityID,
		ChannelID:          room.ChannelID,
		UserID:             presence.UserID,
		Change:             change,
		CameraEnabled:      presence.CameraEnabled,
		ScreenShareEnabled: presence.ScreenShareEnabled,
		PublishingAllowed:  presence.PublishingAllowed,
		ParticipantCount:   count,
		Capacity:           room.Capacity,
		TraceID:            traceID,
		OccurredAt:         r.c.clock(),
	})
	if err != nil {
		return err
	}
	return r.c.Producer.Publish(ctx, message)
}

// purge drops expired dedupe rows.
func (r *Reconciler) purge(ctx context.Context, log *zap.Logger) {
	if r.retention <= 0 {
		return
	}
	removed, err := r.c.Dedupe.PurgeProcessedEvents(ctx, r.c.clock().Add(-r.retention))
	if err != nil {
		log.Warn("could not purge processed events", zap.Error(err))
		return
	}
	if removed > 0 {
		log.Info("purged processed events", zap.Int64("rows", removed))
	}
	if participants, rooms, err := r.c.Presence.CountTotal(ctx); err == nil {
		r.c.refreshGauges(participants, rooms, nil)
	}
}

// tryStart acquires the single-flight guard.
func (r *Reconciler) tryStart() bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.running {
		return false
	}
	r.running = true
	return true
}

// finish releases the single-flight guard.
func (r *Reconciler) finish() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.running = false
}
