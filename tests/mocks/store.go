// Package mocks provides in-memory fakes for the repository, provider, and
// client interfaces of voice_service. They copy the semantics of the real
// implementations (capacity checks, version guards, dedupe) so the join
// transaction and the event paths can be tested without any infrastructure.
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

// Rooms is an in-memory repository.RoomRepository.
type Rooms struct {
	mutex    sync.Mutex
	rooms    map[uuid.UUID]models.VoiceRoom
	occupied func(channelID uuid.UUID) bool
	// Err, when set, is returned by every call.
	Err error
}

// NewRooms builds an empty room store.
func NewRooms() *Rooms {
	return &Rooms{rooms: make(map[uuid.UUID]models.VoiceRoom)}
}

// SetPresenceSource wires the predicate ListRoomsWithPresence uses.
func (r *Rooms) SetPresenceSource(occupied func(channelID uuid.UUID) bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.occupied = occupied
}

// GetRoom implements repository.RoomRepository.
func (r *Rooms) GetRoom(_ context.Context, channelID uuid.UUID) (*models.VoiceRoom, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.Err != nil {
		return nil, r.Err
	}
	room, ok := r.rooms[channelID]
	if !ok {
		return nil, repository.ErrNotFound
	}
	copied := room
	return &copied, nil
}

// GetRoomByLiveKitName implements repository.RoomRepository.
func (r *Rooms) GetRoomByLiveKitName(_ context.Context, roomName string) (*models.VoiceRoom, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.Err != nil {
		return nil, r.Err
	}
	for _, room := range r.rooms {
		if room.LiveKitRoomName == roomName {
			copied := room
			return &copied, nil
		}
	}
	return nil, repository.ErrNotFound
}

// UpsertRoom implements repository.RoomRepository.
func (r *Rooms) UpsertRoom(_ context.Context, room *models.VoiceRoom) error {
	if room == nil {
		return fmt.Errorf("mocks: nil room")
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.Err != nil {
		return r.Err
	}
	stored := *room
	if existing, ok := r.rooms[room.ChannelID]; ok {
		stored.CreatedAt = existing.CreatedAt
		stored.LiveKitRoomName = existing.LiveKitRoomName
	}
	stored.UpdatedAt = time.Now().UTC()
	r.rooms[room.ChannelID] = stored
	return nil
}

// UpdateRoomCapacity implements repository.RoomRepository.
func (r *Rooms) UpdateRoomCapacity(_ context.Context, channelID uuid.UUID, capacity int) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.Err != nil {
		return r.Err
	}
	room, ok := r.rooms[channelID]
	if !ok {
		return repository.ErrNotFound
	}
	room.Capacity = capacity
	r.rooms[channelID] = room
	return nil
}

// ListRoomsByCommunity implements repository.RoomRepository.
func (r *Rooms) ListRoomsByCommunity(_ context.Context, communityID uuid.UUID) ([]models.VoiceRoom, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.Err != nil {
		return nil, r.Err
	}
	rooms := make([]models.VoiceRoom, 0, len(r.rooms))
	for _, room := range r.rooms {
		if room.CommunityID == communityID {
			rooms = append(rooms, room)
		}
	}
	return rooms, nil
}

// ListRoomsWithPresence implements repository.RoomRepository.
func (r *Rooms) ListRoomsWithPresence(_ context.Context) ([]models.VoiceRoom, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.Err != nil {
		return nil, r.Err
	}
	rooms := make([]models.VoiceRoom, 0, len(r.rooms))
	for _, room := range r.rooms {
		if r.occupied == nil || r.occupied(room.ChannelID) {
			rooms = append(rooms, room)
		}
	}
	return rooms, nil
}

// Seed stores a room without going through UpsertRoom.
func (r *Rooms) Seed(room models.VoiceRoom) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if room.LiveKitRoomName == "" {
		room.LiveKitRoomName = "voice-" + room.ChannelID.String()
	}
	r.rooms[room.ChannelID] = room
}

// Len reports how many rooms are stored.
func (r *Rooms) Len() int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return len(r.rooms)
}

// Presence is an in-memory repository.PresenceRepository.
type Presence struct {
	mutex    sync.Mutex
	sessions map[string]models.Presence
	// Err, when set, is returned by every call.
	Err error
	// UpsertErr, when set, is returned by UpsertActive only.
	UpsertErr error
	// SetMediaCalls counts media updates (used by moderation tests).
	SetMediaCalls int
}

// NewPresence builds an empty presence store.
func NewPresence() *Presence {
	return &Presence{sessions: make(map[string]models.Presence)}
}

// presenceKey builds the active-session key of a participant.
func presenceKey(communityID, channelID, userID uuid.UUID) string {
	return communityID.String() + "|" + channelID.String() + "|" + userID.String()
}

// UpsertActive implements repository.PresenceRepository.
func (p *Presence) UpsertActive(_ context.Context, presence *models.Presence) (*models.Presence, error) {
	if presence == nil {
		return nil, fmt.Errorf("mocks: nil presence")
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.Err != nil {
		return nil, p.Err
	}
	if p.UpsertErr != nil {
		return nil, p.UpsertErr
	}
	key := presenceKey(presence.CommunityID, presence.ChannelID, presence.UserID)
	stored := *presence
	if existing, ok := p.sessions[key]; ok {
		stored.SessionJoinedAt = existing.SessionJoinedAt
	} else if stored.SessionJoinedAt.IsZero() {
		stored.SessionJoinedAt = time.Now().UTC()
	}
	if stored.LastSeenAt.IsZero() {
		stored.LastSeenAt = stored.SessionJoinedAt
	}
	stored.LeftAt = nil
	p.sessions[key] = stored
	copied := stored
	return &copied, nil
}

// MarkLeft implements repository.PresenceRepository.
func (p *Presence) MarkLeft(_ context.Context, communityID, channelID, userID uuid.UUID, at time.Time) (bool, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.Err != nil {
		return false, p.Err
	}
	key := presenceKey(communityID, channelID, userID)
	session, ok := p.sessions[key]
	if !ok || session.LeftAt != nil {
		return false, nil
	}
	left := at.UTC()
	session.LeftAt = &left
	p.sessions[key] = session
	return true, nil
}

// MarkChannelLeft implements repository.PresenceRepository.
func (p *Presence) MarkChannelLeft(_ context.Context, communityID, channelID uuid.UUID, at time.Time) ([]models.Presence, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.Err != nil {
		return nil, p.Err
	}
	left := make([]models.Presence, 0, 4)
	for key, session := range p.sessions {
		if session.CommunityID != communityID || session.ChannelID != channelID || session.LeftAt != nil {
			continue
		}
		stamp := at.UTC()
		session.LeftAt = &stamp
		p.sessions[key] = session
		left = append(left, session)
	}
	return left, nil
}

// SetMedia implements repository.PresenceRepository.
func (p *Presence) SetMedia(
	_ context.Context,
	communityID, channelID, userID uuid.UUID,
	camera, screen, publishing bool,
	at time.Time,
) (bool, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.Err != nil {
		return false, p.Err
	}
	p.SetMediaCalls++
	key := presenceKey(communityID, channelID, userID)
	session, ok := p.sessions[key]
	if !ok || session.LeftAt != nil {
		return false, nil
	}
	session.CameraEnabled = camera
	session.ScreenShareEnabled = screen
	session.PublishingAllowed = publishing
	session.LastSeenAt = at.UTC()
	p.sessions[key] = session
	return true, nil
}

// ListActiveByCommunity implements repository.PresenceRepository.
func (p *Presence) ListActiveByCommunity(_ context.Context, communityID uuid.UUID) ([]models.Presence, error) {
	return p.list(func(session models.Presence) bool {
		return session.CommunityID == communityID
	})
}

// ListActiveByChannel implements repository.PresenceRepository.
func (p *Presence) ListActiveByChannel(_ context.Context, communityID, channelID uuid.UUID) ([]models.Presence, error) {
	return p.list(func(session models.Presence) bool {
		return session.ChannelID == channelID && (communityID == uuid.Nil || session.CommunityID == communityID)
	})
}

// ListActiveByUser implements repository.PresenceRepository.
func (p *Presence) ListActiveByUser(_ context.Context, communityID, userID uuid.UUID) ([]models.Presence, error) {
	return p.list(func(session models.Presence) bool {
		return session.CommunityID == communityID && session.UserID == userID
	})
}

// CountActive implements repository.PresenceRepository.
func (p *Presence) CountActive(ctx context.Context, communityID, channelID uuid.UUID) (int, error) {
	sessions, err := p.ListActiveByChannel(ctx, communityID, channelID)
	if err != nil {
		return 0, err
	}
	return len(sessions), nil
}

// CountTotal implements repository.PresenceRepository.
func (p *Presence) CountTotal(_ context.Context) (int, int, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.Err != nil {
		return 0, 0, p.Err
	}
	rooms := make(map[uuid.UUID]struct{}, 2)
	participants := 0
	for _, session := range p.sessions {
		if session.LeftAt != nil {
			continue
		}
		participants++
		rooms[session.ChannelID] = struct{}{}
	}
	return participants, len(rooms), nil
}

// list returns the active sessions matching keep.
func (p *Presence) list(keep func(models.Presence) bool) ([]models.Presence, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.Err != nil {
		return nil, p.Err
	}
	sessions := make([]models.Presence, 0, 4)
	for _, session := range p.sessions {
		if session.LeftAt == nil && keep(session) {
			sessions = append(sessions, session)
		}
	}
	return sessions, nil
}

// Seed stores an active session without going through UpsertActive.
func (p *Presence) Seed(session models.Presence) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if session.SessionJoinedAt.IsZero() {
		session.SessionJoinedAt = time.Now().UTC()
	}
	session.LeftAt = nil
	p.sessions[presenceKey(session.CommunityID, session.ChannelID, session.UserID)] = session
}

// Active reports the stored session of a participant, if any.
func (p *Presence) Active(communityID, channelID, userID uuid.UUID) (*models.Presence, bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	session, ok := p.sessions[presenceKey(communityID, channelID, userID)]
	if !ok || session.LeftAt != nil {
		return nil, false
	}
	copied := session
	return &copied, true
}

// Len reports how many active sessions are stored.
func (p *Presence) Len() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	active := 0
	for _, session := range p.sessions {
		if session.LeftAt == nil {
			active++
		}
	}
	return active
}
