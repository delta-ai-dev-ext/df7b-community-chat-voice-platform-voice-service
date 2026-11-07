package mocks

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"voice_service/internal/service"
)

// Fixture bundles the fake collaborators of the service layer together with a
// controllable clock, so the join transaction and the event paths can be
// exercised deterministically without any infrastructure.
type Fixture struct {
	// Rooms is the in-memory room repository.
	Rooms *Rooms
	// Seats is the in-memory seat repository.
	Seats *Seats
	// Presence is the in-memory presence repository.
	Presence *Presence
	// Projections is the in-memory projection repository.
	Projections *Projections
	// Dedupe is the in-memory dedupe repository.
	Dedupe *Dedupe
	// LiveKit is the in-memory LiveKit client.
	LiveKit *LiveKitClient
	// Tokens is the in-memory token minter.
	Tokens *TokenMinter
	// Producer is the in-memory event producer.
	Producer *Producer
	// Access is the in-memory API-002 checker.
	Access *AccessChecker
	// Metrics holds the collectors under test.
	Metrics *service.Metrics
	// Registry gathers the fixture metrics.
	Registry *prometheus.Registry

	mutex sync.Mutex
	clock time.Time
}

// NewFixture builds a fixture with a fixed clock and a fresh metrics registry.
func NewFixture() *Fixture {
	registry := prometheus.NewRegistry()
	f := &Fixture{
		Rooms:       NewRooms(),
		Presence:    NewPresence(),
		Projections: NewProjections(),
		Dedupe:      NewDedupe(),
		LiveKit:     NewLiveKitClient(),
		Tokens:      NewTokenMinter(),
		Producer:    NewProducer(),
		Access:      &AccessChecker{},
		Metrics:     service.NewMetrics(registry),
		Registry:    registry,
		clock:       time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC),
	}
	f.Seats = NewSeats(f.Rooms, f.Presence, f.Now)
	f.Tokens.Clock = f.Now
	f.Rooms.SetPresenceSource(func(channelID uuid.UUID) bool {
		sessions, err := f.Presence.ListActiveByChannel(context.Background(), uuid.Nil, channelID)
		return err == nil && len(sessions) > 0
	})
	return f
}

// Now reports the fixture clock.
func (f *Fixture) Now() time.Time {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.clock
}

// Clock is a convenience alias of Now for service injection.
func (f *Fixture) Clock() func() time.Time {
	return func() time.Time { return f.Now() }
}

// Advance moves the fixture clock forward.
func (f *Fixture) Advance(d time.Duration) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.clock = f.clock.Add(d)
}

// Set moves the fixture clock to a fixed instant.
func (f *Fixture) Set(instant time.Time) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.clock = instant.UTC()
}

// Collaborators builds the service dependencies wired to the fakes.
func (f *Fixture) Collaborators() service.Collaborators {
	return service.Collaborators{
		Access:      f.Access,
		Rooms:       f.Rooms,
		Seats:       f.Seats,
		Presence:    f.Presence,
		Projections: f.Projections,
		Dedupe:      f.Dedupe,
		LiveKit:     f.LiveKit,
		Tokens:      f.Tokens,
		Producer:    f.Producer,
		Metrics:     f.Metrics,
		Now:         f.Now,
		LiveKitURL:  "wss://livekit.test",
		SeatTTL:     60 * time.Second,
		TokenTTL:    5 * time.Minute,
	}
}
