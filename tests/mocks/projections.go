package mocks

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"voice_service/internal/models"
	"voice_service/internal/repository"
)

// Projections is an in-memory repository.ProjectionRepository with the version
// guards of the SQL implementation.
type Projections struct {
	mutex    sync.Mutex
	members  map[string]models.MemberProjection
	channels map[string]models.ChannelProjection
	// Err, when set, is returned by every call.
	Err error
	// StaleWrites counts updates ignored because of the version guard.
	StaleWrites int
}

// NewProjections builds an empty projection store.
func NewProjections() *Projections {
	return &Projections{
		members:  make(map[string]models.MemberProjection),
		channels: make(map[string]models.ChannelProjection),
	}
}

// memberKey builds the key of a member projection.
func memberKey(communityID, userID uuid.UUID) string {
	return communityID.String() + "|" + userID.String()
}

// channelKey builds the key of a channel projection.
func channelKey(communityID, channelID uuid.UUID) string {
	return communityID.String() + "|" + channelID.String()
}

// UpsertMember implements repository.ProjectionRepository.
func (p *Projections) UpsertMember(_ context.Context, member models.MemberProjection) (bool, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.Err != nil {
		return false, p.Err
	}
	key := memberKey(member.CommunityID, member.UserID)
	if existing, ok := p.members[key]; ok && existing.MemberVersion >= member.MemberVersion {
		p.StaleWrites++
		return false, nil
	}
	p.members[key] = member
	return true, nil
}

// GetMember implements repository.ProjectionRepository.
func (p *Projections) GetMember(_ context.Context, communityID, userID uuid.UUID) (*models.MemberProjection, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.Err != nil {
		return nil, p.Err
	}
	member, ok := p.members[memberKey(communityID, userID)]
	if !ok {
		return nil, repository.ErrNotFound
	}
	copied := member
	return &copied, nil
}

// UpsertChannel implements repository.ProjectionRepository.
func (p *Projections) UpsertChannel(_ context.Context, channel models.ChannelProjection) (bool, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.Err != nil {
		return false, p.Err
	}
	key := channelKey(channel.CommunityID, channel.ChannelID)
	if existing, ok := p.channels[key]; ok && existing.ChannelVersion >= channel.ChannelVersion {
		p.StaleWrites++
		return false, nil
	}
	p.channels[key] = channel
	return true, nil
}

// GetChannel implements repository.ProjectionRepository.
func (p *Projections) GetChannel(_ context.Context, communityID, channelID uuid.UUID) (*models.ChannelProjection, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.Err != nil {
		return nil, p.Err
	}
	channel, ok := p.channels[channelKey(communityID, channelID)]
	if !ok {
		return nil, repository.ErrNotFound
	}
	copied := channel
	return &copied, nil
}

// ListVoiceChannels implements repository.ProjectionRepository.
func (p *Projections) ListVoiceChannels(_ context.Context, communityID uuid.UUID) ([]models.ChannelProjection, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.Err != nil {
		return nil, p.Err
	}
	channels := make([]models.ChannelProjection, 0, 4)
	for _, channel := range p.channels {
		if channel.CommunityID == communityID && channel.IsVoice() {
			channels = append(channels, channel)
		}
	}
	return channels, nil
}

// Dedupe is an in-memory repository.DedupeRepository.
type Dedupe struct {
	mutex    sync.Mutex
	events   map[uuid.UUID]string
	webhooks map[string]string
	purged   int
	// Err, when set, is returned by every call.
	Err error
}

// NewDedupe builds an empty dedupe store.
func NewDedupe() *Dedupe {
	return &Dedupe{
		events:   make(map[uuid.UUID]string),
		webhooks: make(map[string]string),
	}
}

// MarkEventProcessed implements repository.DedupeRepository.
func (d *Dedupe) MarkEventProcessed(_ context.Context, eventID uuid.UUID, topic string) (bool, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if d.Err != nil {
		return false, d.Err
	}
	if _, seen := d.events[eventID]; seen {
		return false, nil
	}
	d.events[eventID] = topic
	return true, nil
}

// MarkWebhookProcessed implements repository.DedupeRepository.
func (d *Dedupe) MarkWebhookProcessed(_ context.Context, webhookEventID, eventKind string) (bool, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if d.Err != nil {
		return false, d.Err
	}
	if _, seen := d.webhooks[webhookEventID]; seen {
		return false, nil
	}
	d.webhooks[webhookEventID] = eventKind
	return true, nil
}

// ForgetEvent implements repository.DedupeRepository.
func (d *Dedupe) ForgetEvent(_ context.Context, eventID uuid.UUID) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	delete(d.events, eventID)
	return nil
}

// ForgetWebhook implements repository.DedupeRepository.
func (d *Dedupe) ForgetWebhook(_ context.Context, webhookEventID string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	delete(d.webhooks, webhookEventID)
	return nil
}

// PurgeProcessedEvents implements repository.DedupeRepository.
func (d *Dedupe) PurgeProcessedEvents(_ context.Context, olderThan time.Time) (int64, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if d.Err != nil {
		return 0, d.Err
	}
	if olderThan.IsZero() {
		return 0, nil
	}
	d.purged++
	return 1, nil
}

// PurgeCalls reports how often the retention sweep ran.
func (d *Dedupe) PurgeCalls() int {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.purged
}

// EventsLen reports how many Kafka event ids are recorded.
func (d *Dedupe) EventsLen() int {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return len(d.events)
}

// WebhooksLen reports how many webhook event ids are recorded.
func (d *Dedupe) WebhooksLen() int {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return len(d.webhooks)
}
