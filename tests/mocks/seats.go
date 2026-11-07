package mocks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"voice_service/internal/models"
	"voice_service/internal/repository"
)

// Seats is an in-memory repository.SeatRepository that reproduces the capacity
// accounting of the SQL implementation: live reservations plus active
// participants, one live reservation per user per channel, expired
// reservations treated as absent, and a full channel reported before any
// reservation is stored.
type Seats struct {
	mutex        sync.Mutex
	reservations map[uuid.UUID]*models.SeatReservation
	released     map[uuid.UUID]time.Time
	rooms        *Rooms
	presence     *Presence
	now          func() time.Time
	// Err, when set, is returned by every call.
	Err error
	// CountErr, when set, is returned by CountOccupied only.
	CountErr error
	// Completes counts completed reservations.
	Completes int
	// Releases counts released reservations.
	Releases int
}

// NewSeats wires the seat store to the room and presence stores.
func NewSeats(rooms *Rooms, presence *Presence, now func() time.Time) *Seats {
	if now == nil {
		now = time.Now
	}
	return &Seats{
		reservations: make(map[uuid.UUID]*models.SeatReservation),
		released:     make(map[uuid.UUID]time.Time),
		rooms:        rooms,
		presence:     presence,
		now:          now,
	}
}

// isReleased reports whether a reservation was released.
func (s *Seats) isReleased(reservationID uuid.UUID) bool {
	_, released := s.released[reservationID]
	return released
}

// live reports whether a reservation still holds a seat.
func (s *Seats) live(reservation *models.SeatReservation, now time.Time) bool {
	return !s.isReleased(reservation.ReservationID) && reservation.ExpiresAt.After(now)
}

// markReleased releases a reservation at instant.
func (s *Seats) markReleased(reservationID uuid.UUID, instant time.Time) {
	if _, released := s.released[reservationID]; !released {
		s.released[reservationID] = instant
	}
}

// ReserveSeat implements repository.SeatRepository.
func (s *Seats) ReserveSeat(_ context.Context, req models.SeatReservationRequest) (*models.SeatReservation, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.Err != nil {
		return nil, s.Err
	}
	room, err := s.rooms.GetRoom(context.Background(), req.ChannelID)
	if err != nil {
		return nil, err
	}
	now := s.now().UTC()

	// Sweep this user's abandoned reservation of the channel.
	for _, reservation := range s.reservations {
		if reservation.ChannelID == req.ChannelID && reservation.UserID == req.UserID &&
			!s.isReleased(reservation.ReservationID) && !reservation.ExpiresAt.After(now) {
			s.markReleased(reservation.ReservationID, now)
		}
	}

	holdsSeat := false
	for _, reservation := range s.reservations {
		if reservation.ChannelID == req.ChannelID && reservation.UserID == req.UserID && s.live(reservation, now) {
			holdsSeat = true
		}
	}
	if _, active := s.presence.Active(req.CommunityID, req.ChannelID, req.UserID); active {
		holdsSeat = true
	}

	if !holdsSeat {
		occupied := 0
		for _, reservation := range s.reservations {
			if reservation.ChannelID == req.ChannelID && s.live(reservation, now) {
				occupied++
			}
		}
		sessions, err := s.presence.ListActiveByChannel(context.Background(), req.CommunityID, req.ChannelID)
		if err != nil {
			return nil, err
		}
		occupied += len(sessions)
		if occupied >= room.Capacity {
			return nil, repository.ErrChannelFull
		}
	}

	for _, reservation := range s.reservations {
		if reservation.ChannelID == req.ChannelID && reservation.UserID == req.UserID &&
			!s.isReleased(reservation.ReservationID) {
			reservation.ExpiresAt = now.Add(req.TTL)
			reservation.ReservedAt = now
			copied := *reservation
			return &copied, nil
		}
	}
	reservation := &models.SeatReservation{
		ReservationID: uuid.New(),
		CommunityID:   req.CommunityID,
		ChannelID:     req.ChannelID,
		UserID:        req.UserID,
		ReservedAt:    now,
		ExpiresAt:     now.Add(req.TTL),
	}
	s.reservations[reservation.ReservationID] = reservation
	copied := *reservation
	return &copied, nil
}

// CompleteSeat implements repository.SeatRepository. The reservation is
// released and the presence row recorded in one step, so the seat is never
// momentarily free.
func (s *Seats) CompleteSeat(ctx context.Context, reservationID uuid.UUID, presence *models.Presence) (*models.Presence, error) {
	s.mutex.Lock()
	if s.Err != nil {
		s.mutex.Unlock()
		return nil, s.Err
	}
	reservation, ok := s.reservations[reservationID]
	if !ok {
		s.mutex.Unlock()
		return nil, repository.ErrNotFound
	}
	if presence == nil {
		s.mutex.Unlock()
		return nil, fmt.Errorf("mocks: nil presence")
	}
	if reservation.ChannelID != presence.ChannelID || reservation.UserID != presence.UserID {
		s.mutex.Unlock()
		return nil, fmt.Errorf("mocks: reservation does not belong to the participant")
	}
	s.markReleased(reservationID, s.now().UTC())
	s.Completes++
	s.mutex.Unlock()
	return s.presence.UpsertActive(ctx, presence)
}

// ReleaseSeat implements repository.SeatRepository.
func (s *Seats) ReleaseSeat(_ context.Context, reservationID uuid.UUID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.Err != nil {
		return s.Err
	}
	s.Releases++
	if _, ok := s.reservations[reservationID]; ok {
		s.markReleased(reservationID, s.now().UTC())
	}
	return nil
}

// ReleaseUserSeats implements repository.SeatRepository.
func (s *Seats) ReleaseUserSeats(_ context.Context, channelID, userID uuid.UUID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.Err != nil {
		return s.Err
	}
	now := s.now().UTC()
	for _, reservation := range s.reservations {
		if reservation.ChannelID == channelID && reservation.UserID == userID &&
			!s.isReleased(reservation.ReservationID) {
			s.markReleased(reservation.ReservationID, now)
		}
	}
	return nil
}

// CountOccupied implements repository.SeatRepository.
func (s *Seats) CountOccupied(ctx context.Context, channelIDs []uuid.UUID) (map[uuid.UUID]int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.Err != nil {
		return nil, s.Err
	}
	if s.CountErr != nil {
		return nil, s.CountErr
	}
	now := s.now().UTC()
	occupied := make(map[uuid.UUID]int, len(channelIDs))
	for _, channelID := range channelIDs {
		count := 0
		for _, reservation := range s.reservations {
			if reservation.ChannelID == channelID && s.live(reservation, now) {
				count++
			}
		}
		sessions, err := s.presence.ListActiveByChannel(ctx, uuid.Nil, channelID)
		if err != nil {
			return nil, err
		}
		occupied[channelID] = count + len(sessions)
	}
	return occupied, nil
}

// LiveReservations reports how many reservations still hold a seat.
func (s *Seats) LiveReservations() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	now := s.now().UTC()
	live := 0
	for _, reservation := range s.reservations {
		if s.live(reservation, now) {
			live++
		}
	}
	return live
}
