package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Dedupe is the pgx implementation of DedupeRepository.
type Dedupe struct {
	q Querier
}

// NewDedupe builds the dedupe repository.
func NewDedupe(q Querier) *Dedupe { return &Dedupe{q: q} }

// MarkEventProcessed records a consumed Kafka event id. It returns true when
// this delivery is the first one and false for a duplicate.
func (d *Dedupe) MarkEventProcessed(ctx context.Context, eventID uuid.UUID, topic string) (bool, error) {
	var stored uuid.UUID
	err := d.q.QueryRow(ctx, `
		INSERT INTO processed_events (event_id, topic)
		VALUES ($1, $2)
		ON CONFLICT (event_id) DO NOTHING
		RETURNING event_id`, eventID, topic).Scan(&stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("repository: mark event processed: %w", err)
	}
	return true, nil
}

// MarkWebhookProcessed records a LiveKit webhook event id. It returns true
// when this delivery is the first one and false for a duplicate.
func (d *Dedupe) MarkWebhookProcessed(ctx context.Context, webhookEventID, eventKind string) (bool, error) {
	var stored string
	err := d.q.QueryRow(ctx, `
		INSERT INTO processed_webhook_events (webhook_event_id, event_kind)
		VALUES ($1, $2)
		ON CONFLICT (webhook_event_id) DO NOTHING
		RETURNING webhook_event_id`, webhookEventID, eventKind).Scan(&stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("repository: mark webhook processed: %w", err)
	}
	return true, nil
}

// ForgetEvent removes a Kafka dedupe row so a redelivery can retry a failed
// reaction.
func (d *Dedupe) ForgetEvent(ctx context.Context, eventID uuid.UUID) error {
	if _, err := d.q.Exec(ctx, "DELETE FROM processed_events WHERE event_id = $1", eventID); err != nil {
		return fmt.Errorf("repository: forget processed event: %w", err)
	}
	return nil
}

// ForgetWebhook removes a webhook dedupe row so a redelivery can retry a failed
// reaction.
func (d *Dedupe) ForgetWebhook(ctx context.Context, webhookEventID string) error {
	if _, err := d.q.Exec(ctx,
		"DELETE FROM processed_webhook_events WHERE webhook_event_id = $1", webhookEventID); err != nil {
		return fmt.Errorf("repository: forget processed webhook: %w", err)
	}
	return nil
}

// PurgeProcessedEvents drops dedupe rows older than olderThan.
func (d *Dedupe) PurgeProcessedEvents(ctx context.Context, olderThan time.Time) (int64, error) {
	tag, err := d.q.Exec(ctx, "DELETE FROM processed_events WHERE processed_at < $1", olderThan.UTC())
	if err != nil {
		return 0, fmt.Errorf("repository: purge processed events: %w", err)
	}
	return tag.RowsAffected(), nil
}

// Set is the bundle of pgx repositories handed to the service layer.
type Set struct {
	// Rooms stores the channel to LiveKit room mapping.
	Rooms RoomRepository
	// Seats enforces channel capacity.
	Seats SeatRepository
	// Presence stores active participants.
	Presence PresenceRepository
	// Projections stores the event-derived views.
	Projections ProjectionRepository
	// Dedupe stores consumed event and webhook ids.
	Dedupe DedupeRepository
}

// NewSet builds the full pgx repository set.
func NewSet(q Querier) Set {
	return Set{
		Rooms:       NewRooms(q),
		Seats:       NewSeats(q),
		Presence:    NewPresence(q),
		Projections: NewProjections(q),
		Dedupe:      NewDedupe(q),
	}
}
