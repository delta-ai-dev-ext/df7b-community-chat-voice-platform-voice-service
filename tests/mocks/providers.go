package mocks

import (
	"context"
	"sync"
	"time"

	"voice_service/internal/community"
	"voice_service/internal/events"
	"voice_service/internal/livekit"
	"voice_service/internal/models"
)

// LiveKitClient is an in-memory livekit.RoomClient.
type LiveKitClient struct {
	mutex           sync.Mutex
	MaxParticipants map[string]int
	Participants    map[string][]livekit.Participant
	Created         []string
	Deleted         []string
	Removed         []string
	Revoked         []string
	// FailCreate, FailDelete, FailRemove, FailUpdate, and FailList inject a
	// failure into the matching operation.
	FailCreate error
	FailDelete error
	FailRemove error
	FailUpdate error
	FailList   error
	// BeforeList, when set, runs before ListParticipants answers; tests use it
	// to hold a call open.
	BeforeList func(roomName string)
}

// NewLiveKitClient builds an empty LiveKit fake.
func NewLiveKitClient() *LiveKitClient {
	return &LiveKitClient{
		MaxParticipants: make(map[string]int),
		Participants:    make(map[string][]livekit.Participant),
	}
}

// CreateRoom implements livekit.RoomClient.
func (c *LiveKitClient) CreateRoom(_ context.Context, req livekit.CreateRoomRequest) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.FailCreate != nil {
		return c.FailCreate
	}
	c.Created = append(c.Created, req.RoomName)
	c.MaxParticipants[req.RoomName] = req.MaxParticipants
	return nil
}

// DeleteRoom implements livekit.RoomClient.
func (c *LiveKitClient) DeleteRoom(_ context.Context, roomName string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.FailDelete != nil {
		return c.FailDelete
	}
	c.Deleted = append(c.Deleted, roomName)
	return nil
}

// RemoveParticipant implements livekit.RoomClient.
func (c *LiveKitClient) RemoveParticipant(_ context.Context, roomName, identity string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.FailRemove != nil {
		return c.FailRemove
	}
	c.Removed = append(c.Removed, roomName+"/"+identity)
	return nil
}

// UpdateParticipantPermissions implements livekit.RoomClient.
func (c *LiveKitClient) UpdateParticipantPermissions(_ context.Context, roomName, identity string, canPublish bool) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.FailUpdate != nil {
		return c.FailUpdate
	}
	state := "/revoked"
	if canPublish {
		state = "/granted"
	}
	c.Revoked = append(c.Revoked, roomName+"/"+identity+state)
	return nil
}

// ListParticipants implements livekit.RoomClient.
func (c *LiveKitClient) ListParticipants(_ context.Context, roomName string) ([]livekit.Participant, error) {
	c.mutex.Lock()
	hook := c.BeforeList
	failure := c.FailList
	participants, ok := c.Participants[roomName]
	c.mutex.Unlock()

	if hook != nil {
		hook(roomName)
	}
	if failure != nil {
		return nil, failure
	}
	if !ok {
		return nil, livekit.ErrRoomNotFound
	}
	copied := make([]livekit.Participant, len(participants))
	copy(copied, participants)
	return copied, nil
}

// SetParticipants overrides the participant list of a room.
func (c *LiveKitClient) SetParticipants(roomName string, participants ...livekit.Participant) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.Participants[roomName] = participants
}

// CreatedCount reports how many rooms were created.
func (c *LiveKitClient) CreatedCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.Created)
}

// RemovedCount reports how many participants were removed.
func (c *LiveKitClient) RemovedCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.Removed)
}

// RevokedCount reports how many permission updates were issued.
func (c *LiveKitClient) RevokedCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.Revoked)
}

// DeletedCount reports how many rooms were deleted.
func (c *LiveKitClient) DeletedCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.Deleted)
}

// TokenMinter is an in-memory livekit.TokenMinter.
type TokenMinter struct {
	mutex    sync.Mutex
	Requests []livekit.TokenRequest
	// Value is the minted token value.
	Value string
	// Clock stamps the token expiry; defaults to time.Now.
	Clock func() time.Time
	// Fail, when set, is returned by every call.
	Fail error
}

// NewTokenMinter builds a token minter fake.
func NewTokenMinter() *TokenMinter {
	return &TokenMinter{Value: "participant-token", Clock: time.Now}
}

// MintParticipantToken implements livekit.TokenMinter.
func (m *TokenMinter) MintParticipantToken(_ context.Context, req livekit.TokenRequest) (livekit.Token, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Requests = append(m.Requests, req)
	if m.Fail != nil {
		return livekit.Token{}, m.Fail
	}
	issuedAt := time.Now
	if m.Clock != nil {
		issuedAt = m.Clock
	}
	return livekit.Token{Value: m.Value, ExpiresAt: issuedAt().UTC().Add(req.TTL)}, nil
}

// Last reports the last mint request.
func (m *TokenMinter) Last() (livekit.TokenRequest, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if len(m.Requests) == 0 {
		return livekit.TokenRequest{}, false
	}
	return m.Requests[len(m.Requests)-1], true
}

// Producer is an in-memory events.Producer.
type Producer struct {
	mutex    sync.Mutex
	Messages []events.Message
	// Fail, when set, is returned by every publish.
	Fail error
	// Closed reports whether Close was called.
	Closed bool
}

// NewProducer builds an empty producer fake.
func NewProducer() *Producer { return &Producer{Messages: make([]events.Message, 0, 4)} }

// Publish implements events.Producer.
func (p *Producer) Publish(_ context.Context, message events.Message) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.Fail != nil {
		return p.Fail
	}
	p.Messages = append(p.Messages, message)
	return nil
}

// Close implements events.Producer.
func (p *Producer) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.Closed = true
	return nil
}

// Len reports how many messages were published.
func (p *Producer) Len() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return len(p.Messages)
}

// Last reports the last published message.
func (p *Producer) Last() (events.Message, bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if len(p.Messages) == 0 {
		return events.Message{}, false
	}
	return p.Messages[len(p.Messages)-1], true
}

// LastPresence reports the payload of the last published presence event.
func (p *Producer) LastPresence() (events.PresencePayload, bool) {
	message, ok := p.Last()
	if !ok {
		return events.PresencePayload{}, false
	}
	payload, ok := message.Payload.(events.PresencePayload)
	return payload, ok
}

// AccessChecker is an in-memory community.AccessChecker.
type AccessChecker struct {
	mutex    sync.Mutex
	Result   *community.AccessCheckResult
	Err      error
	Requests []community.AccessCheckRequest
	// Respond, when set, replaces the static result.
	Respond func(req community.AccessCheckRequest) (*community.AccessCheckResult, error)
}

// CheckVoiceAccess implements community.AccessChecker.
func (a *AccessChecker) CheckVoiceAccess(_ context.Context, req community.AccessCheckRequest) (*community.AccessCheckResult, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.Requests = append(a.Requests, req)
	if a.Respond != nil {
		return a.Respond(req)
	}
	if a.Err != nil {
		return nil, a.Err
	}
	if a.Result == nil {
		channel := models.ChannelSummary{ChannelID: req.ChannelID, Name: "General", Capacity: 5}
		return &community.AccessCheckResult{
			CommunityID:   req.CommunityID,
			UserID:        req.UserID,
			Allowed:       true,
			Channel:       &channel,
			VoiceChannels: []models.ChannelSummary{channel},
		}, nil
	}
	return a.Result, nil
}

// LastRequest reports the last access check request.
func (a *AccessChecker) LastRequest() (community.AccessCheckRequest, bool) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if len(a.Requests) == 0 {
		return community.AccessCheckRequest{}, false
	}
	return a.Requests[len(a.Requests)-1], true
}
