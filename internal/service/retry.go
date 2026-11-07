package service

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"voice_service/internal/livekit"
)

// DefaultRetryAttempts is the back-off budget of a LiveKit side effect.
const DefaultRetryAttempts = 3

// DefaultRetryBase is the first back-off delay of a LiveKit side effect.
const DefaultRetryBase = 50 * time.Millisecond

// RetryPolicy retries a LiveKit side effect with bounded exponential back-off.
// "Not found" answers are not retried: they are idempotent outcomes.
type RetryPolicy struct {
	// Attempts is the total number of attempts (>= 1).
	Attempts int
	// Base is the first back-off delay.
	Base time.Duration
}

// DefaultRetryPolicy returns the policy used by the event consumer.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{Attempts: DefaultRetryAttempts, Base: DefaultRetryBase}
}

// Do runs fn until it succeeds, fails permanently, or exhausts the attempts.
func (p RetryPolicy) Do(ctx context.Context, log *zap.Logger, operation string, fn func(context.Context) error) error {
	attempts := p.Attempts
	if attempts <= 0 {
		attempts = 1
	}
	base := p.Base
	if base <= 0 {
		base = DefaultRetryBase
	}
	if log == nil {
		log = zap.NewNop()
	}
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		err = fn(ctx)
		if err == nil {
			return nil
		}
		if permanent(err) {
			return err
		}
		if attempt == attempts {
			break
		}
		wait := base << (attempt - 1)
		log.Warn("retrying livekit operation",
			zap.String("operation", operation),
			zap.Int("attempt", attempt),
			zap.Duration("retry_in", wait),
			zap.Error(err),
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return err
}

// permanent reports failures that a retry cannot fix.
func permanent(err error) bool {
	switch {
	case err == nil:
		return true
	case errors.Is(err, livekit.ErrParticipantNotFound), errors.Is(err, livekit.ErrRoomNotFound):
		return true
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return true
	default:
		return false
	}
}
