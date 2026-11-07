package unit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	lkproto "github.com/livekit/protocol/livekit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"voice_service/internal/events"
	"voice_service/internal/livekit"
	"voice_service/internal/models"
	"voice_service/internal/service"
	"voice_service/tests/mocks"
)

// webhookEvent builds a LiveKit webhook event of the given kind.
func webhookEvent(kind, roomName, identity string) *lkproto.WebhookEvent {
	event := &lkproto.WebhookEvent{
		Id:    uuid.NewString(),
		Event: kind,
		Room:  &lkproto.Room{Name: roomName},
	}
	if identity != "" {
		event.Participant = &lkproto.ParticipantInfo{
			Identity: identity,
			Kind:     lkproto.ParticipantInfo_STANDARD,
			Permission: &lkproto.ParticipantPermission{
				CanPublish:   true,
				CanSubscribe: true,
			},
		}
	}
	return event
}

// seededWebhookFixture builds a fixture with one known room.
func seededWebhookFixture() (*mocks.Fixture, uuid.UUID, uuid.UUID) {
	f := mocks.NewFixture()
	communityID, channelID := uuid.New(), uuid.New()
	f.Rooms.Seed(models.VoiceRoom{ChannelID: channelID, CommunityID: communityID, Capacity: 4})
	return f, communityID, channelID
}

// TestWebhookParticipantJoined covers US-007: presence recorded and EVT-010
// published with a trace id minted at the webhook edge.
func TestWebhookParticipantJoined(t *testing.T) {
	t.Parallel()
	f, communityID, channelID := seededWebhookFixture()
	userID := uuid.New()
	trace := events.NewTraceID()
	processor := service.NewWebhookProcessor(f.Collaborators())

	event := webhookEvent(service.WebhookParticipantJoined, service.RoomName(channelID), userID.String())
	require.NoError(t, processor.Process(context.Background(), event, trace))

	session, active := f.Presence.Active(communityID, channelID, userID)
	require.True(t, active)
	assert.True(t, session.PublishingAllowed)

	payload, ok := f.Producer.LastPresence()
	require.True(t, ok)
	assert.Equal(t, events.ChangeJoined, payload.Change)
	assert.Equal(t, 1, payload.ParticipantCount)
	assert.Equal(t, 4, payload.Capacity)

	message, ok := f.Producer.Last()
	require.True(t, ok)
	assert.Equal(t, trace, message.Envelope.TraceID)
	assert.Equal(t, channelID.String(), message.Key)
	assert.Equal(t, 1, f.Dedupe.WebhooksLen())
}

// TestWebhookDuplicateIsIgnored covers the webhook dedupe rule.
func TestWebhookDuplicateIsIgnored(t *testing.T) {
	t.Parallel()
	f, _, channelID := seededWebhookFixture()
	processor := service.NewWebhookProcessor(f.Collaborators())
	event := webhookEvent(service.WebhookParticipantJoined, service.RoomName(channelID), uuid.New().String())

	require.NoError(t, processor.Process(context.Background(), event, events.NewTraceID()))
	require.NoError(t, processor.Process(context.Background(), event, events.NewTraceID()))

	assert.Equal(t, 1, f.Producer.Len(), "a duplicate webhook must not publish a second event")
}

// TestWebhookUnknownRoomIsIgnored covers webhooks for rooms this service does
// not know.
func TestWebhookUnknownRoomIsIgnored(t *testing.T) {
	t.Parallel()
	f, _, _ := seededWebhookFixture()
	processor := service.NewWebhookProcessor(f.Collaborators())
	event := webhookEvent(service.WebhookParticipantJoined, "voice-unknown", uuid.New().String())

	require.NoError(t, processor.Process(context.Background(), event, events.NewTraceID()))
	assert.Equal(t, 0, f.Producer.Len())
	assert.Equal(t, 0, f.Presence.Len())
}

// TestWebhookIgnoresNonUserParticipants covers agent and service participants.
func TestWebhookIgnoresNonUserParticipants(t *testing.T) {
	t.Parallel()
	f, _, channelID := seededWebhookFixture()
	processor := service.NewWebhookProcessor(f.Collaborators())

	agentEvent := webhookEvent(service.WebhookParticipantJoined, service.RoomName(channelID), uuid.New().String())
	agentEvent.Participant.Kind = lkproto.ParticipantInfo_AGENT
	require.NoError(t, processor.Process(context.Background(), agentEvent, events.NewTraceID()))

	malformedIdentity := webhookEvent(service.WebhookParticipantJoined, service.RoomName(channelID), "not-a-uuid")
	require.NoError(t, processor.Process(context.Background(), malformedIdentity, events.NewTraceID()))

	assert.Equal(t, 0, f.Presence.Len())
	assert.Equal(t, 0, f.Producer.Len())
}

// TestWebhookParticipantLeft covers the left mapping.
func TestWebhookParticipantLeft(t *testing.T) {
	t.Parallel()
	f, communityID, channelID := seededWebhookFixture()
	userID := uuid.New()
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: userID, PublishingAllowed: true})
	processor := service.NewWebhookProcessor(f.Collaborators())

	event := webhookEvent(service.WebhookParticipantLeft, service.RoomName(channelID), userID.String())
	require.NoError(t, processor.Process(context.Background(), event, events.NewTraceID()))

	assert.Equal(t, 0, f.Presence.Len())
	payload, ok := f.Producer.LastPresence()
	require.True(t, ok)
	assert.Equal(t, events.ChangeLeft, payload.Change)
	assert.Equal(t, 0, payload.ParticipantCount)
}

// TestWebhookTrackChanges covers camera and screen-share toggles (US-006).
func TestWebhookTrackChanges(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		kind            string
		source          lkproto.TrackSource
		wantCamera      bool
		wantScreen      bool
		wantPublished   bool
		wantEventIssued bool
	}{
		{name: "camera on", kind: service.WebhookTrackPublished, source: lkproto.TrackSource_CAMERA, wantCamera: true, wantEventIssued: true},
		{name: "screen on", kind: service.WebhookTrackPublished, source: lkproto.TrackSource_SCREEN_SHARE, wantScreen: true, wantEventIssued: true},
		{name: "microphone is ignored", kind: service.WebhookTrackPublished, source: lkproto.TrackSource_MICROPHONE, wantEventIssued: false},
	}
	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			f, communityID, channelID := seededWebhookFixture()
			userID := uuid.New()
			f.Presence.Seed(models.Presence{
				CommunityID: communityID, ChannelID: channelID, UserID: userID, PublishingAllowed: true,
			})
			processor := service.NewWebhookProcessor(f.Collaborators())

			event := webhookEvent(testCase.kind, service.RoomName(channelID), userID.String())
			event.Track = &lkproto.TrackInfo{Source: testCase.source}
			require.NoError(t, processor.Process(context.Background(), event, events.NewTraceID()))

			if !testCase.wantEventIssued {
				assert.Equal(t, 0, f.Producer.Len())
				return
			}
			session, active := f.Presence.Active(communityID, channelID, userID)
			require.True(t, active)
			assert.Equal(t, testCase.wantCamera, session.CameraEnabled)
			assert.Equal(t, testCase.wantScreen, session.ScreenShareEnabled)
			payload, ok := f.Producer.LastPresence()
			require.True(t, ok)
			assert.Equal(t, events.ChangeMediaChanged, payload.Change)
		})
	}
}

// TestWebhookTrackUnpublishedClearsTheFlag covers the reverse toggle.
func TestWebhookTrackUnpublishedClearsTheFlag(t *testing.T) {
	t.Parallel()
	f, communityID, channelID := seededWebhookFixture()
	userID := uuid.New()
	f.Presence.Seed(models.Presence{
		CommunityID: communityID, ChannelID: channelID, UserID: userID,
		CameraEnabled: true, ScreenShareEnabled: true, PublishingAllowed: true,
	})
	processor := service.NewWebhookProcessor(f.Collaborators())

	event := webhookEvent(service.WebhookTrackUnpublished, service.RoomName(channelID), userID.String())
	event.Track = &lkproto.TrackInfo{Source: lkproto.TrackSource_SCREEN_SHARE}
	require.NoError(t, processor.Process(context.Background(), event, events.NewTraceID()))

	session, active := f.Presence.Active(communityID, channelID, userID)
	require.True(t, active)
	assert.False(t, session.ScreenShareEnabled)
	assert.True(t, session.CameraEnabled)
}

// TestWebhookMediaChangeForUnknownSessionIsIgnored covers unknown participants.
func TestWebhookMediaChangeForUnknownSessionIsIgnored(t *testing.T) {
	t.Parallel()
	f, _, channelID := seededWebhookFixture()
	processor := service.NewWebhookProcessor(f.Collaborators())
	event := webhookEvent(service.WebhookTrackPublished, service.RoomName(channelID), uuid.New().String())
	event.Track = &lkproto.TrackInfo{Source: lkproto.TrackSource_CAMERA}

	require.NoError(t, processor.Process(context.Background(), event, events.NewTraceID()))
	assert.Equal(t, 0, f.Producer.Len())
}

// TestWebhookRoomFinished covers the room_finished mapping.
func TestWebhookRoomFinished(t *testing.T) {
	t.Parallel()
	f, communityID, channelID := seededWebhookFixture()
	first, second := uuid.New(), uuid.New()
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: first, PublishingAllowed: true})
	f.Presence.Seed(models.Presence{CommunityID: communityID, ChannelID: channelID, UserID: second, PublishingAllowed: true})
	processor := service.NewWebhookProcessor(f.Collaborators())

	event := webhookEvent(service.WebhookRoomFinished, service.RoomName(channelID), "")
	require.NoError(t, processor.Process(context.Background(), event, events.NewTraceID()))

	assert.Equal(t, 0, f.Presence.Len())
	assert.Equal(t, 2, f.Producer.Len())
}

// TestWebhookUnknownKindIsIgnored covers unrelated LiveKit events.
func TestWebhookUnknownKindIsIgnored(t *testing.T) {
	t.Parallel()
	f, _, channelID := seededWebhookFixture()
	processor := service.NewWebhookProcessor(f.Collaborators())
	event := webhookEvent("egress_started", service.RoomName(channelID), uuid.New().String())

	require.NoError(t, processor.Process(context.Background(), event, events.NewTraceID()))
	assert.Equal(t, 0, f.Producer.Len())
}

// TestWebhookFailureForgetsDedupeRow covers retryability: a failed reaction
// must be reprocessable when LiveKit retries the delivery.
func TestWebhookFailureForgetsDedupeRow(t *testing.T) {
	t.Parallel()
	f, _, channelID := seededWebhookFixture()
	f.Producer.Fail = errors.New("kafka unavailable")
	processor := service.NewWebhookProcessor(f.Collaborators())
	event := webhookEvent(service.WebhookParticipantJoined, service.RoomName(channelID), uuid.New().String())

	err := processor.Process(context.Background(), event, events.NewTraceID())
	require.Error(t, err)
	assert.Equal(t, 0, f.Dedupe.WebhooksLen(), "the dedupe row must be dropped so the retry is processed")

	f.Producer.Fail = nil
	require.NoError(t, processor.Process(context.Background(), event, events.NewTraceID()))
	assert.Equal(t, 1, f.Producer.Len())
}

// TestWebhookRejectsMissingEventID covers malformed webhook input.
func TestWebhookRejectsMissingEventID(t *testing.T) {
	t.Parallel()
	f, _, channelID := seededWebhookFixture()
	processor := service.NewWebhookProcessor(f.Collaborators())
	event := webhookEvent(service.WebhookParticipantJoined, service.RoomName(channelID), uuid.New().String())
	event.Id = ""

	err := processor.Process(context.Background(), event, events.NewTraceID())
	require.ErrorIs(t, err, livekit.ErrMalformedWebhook)
	assert.Equal(t, 0, f.Presence.Len())
}

// TestWebhookDedupeRepositoryFailureIsReported covers a failing dedupe store.
func TestWebhookDedupeRepositoryFailureIsReported(t *testing.T) {
	t.Parallel()
	f, _, channelID := seededWebhookFixture()
	f.Dedupe.Err = errors.New("database down")
	processor := service.NewWebhookProcessor(f.Collaborators())
	event := webhookEvent(service.WebhookParticipantJoined, service.RoomName(channelID), uuid.New().String())

	require.Error(t, processor.Process(context.Background(), event, events.NewTraceID()))
	assert.Equal(t, 0, f.Presence.Len())
}

// TestWebhookRecordsParticipantMediaAtJoin covers the flags of a join that
// already carries tracks.
func TestWebhookRecordsParticipantMediaAtJoin(t *testing.T) {
	t.Parallel()
	f, communityID, channelID := seededWebhookFixture()
	userID := uuid.New()
	processor := service.NewWebhookProcessor(f.Collaborators())

	event := webhookEvent(service.WebhookParticipantJoined, service.RoomName(channelID), userID.String())
	event.Participant.JoinedAtMs = f.Now().Add(-time.Minute).UnixMilli()
	event.Participant.Tracks = []*lkproto.TrackInfo{
		{Source: lkproto.TrackSource_CAMERA},
		{Source: lkproto.TrackSource_SCREEN_SHARE},
		{Source: lkproto.TrackSource_MICROPHONE, Muted: true},
	}
	require.NoError(t, processor.Process(context.Background(), event, events.NewTraceID()))

	session, active := f.Presence.Active(communityID, channelID, userID)
	require.True(t, active)
	assert.True(t, session.CameraEnabled)
	assert.True(t, session.ScreenShareEnabled)
	assert.WithinDuration(t, f.Now().Add(-time.Minute), session.SessionJoinedAt, time.Second)
}
