package service

import (
	"context"
	"time"

	"voice_service/internal/livekit"
)

// InstrumentedRoomClient records external_call_* metrics around every LiveKit
// room operation (provider="livekit").
type InstrumentedRoomClient struct {
	inner   livekit.RoomClient
	metrics *Metrics
}

// NewInstrumentedRoomClient wraps inner with LiveKit call metrics. A nil inner
// client is returned unchanged so partially wired processes stay usable.
func NewInstrumentedRoomClient(inner livekit.RoomClient, metrics *Metrics) livekit.RoomClient {
	if inner == nil {
		return nil
	}
	return &InstrumentedRoomClient{inner: inner, metrics: metrics}
}

// CreateRoom implements livekit.RoomClient.
func (c *InstrumentedRoomClient) CreateRoom(ctx context.Context, req livekit.CreateRoomRequest) error {
	started := time.Now()
	err := c.inner.CreateRoom(ctx, req)
	c.metrics.ObserveExternal(OperationCreateRoom, time.Since(started), err)
	return err
}

// DeleteRoom implements livekit.RoomClient.
func (c *InstrumentedRoomClient) DeleteRoom(ctx context.Context, roomName string) error {
	started := time.Now()
	err := c.inner.DeleteRoom(ctx, roomName)
	c.metrics.ObserveExternal(OperationDeleteRoom, time.Since(started), err)
	return err
}

// RemoveParticipant implements livekit.RoomClient.
func (c *InstrumentedRoomClient) RemoveParticipant(ctx context.Context, roomName, identity string) error {
	started := time.Now()
	err := c.inner.RemoveParticipant(ctx, roomName, identity)
	c.metrics.ObserveExternal(OperationRemoveParticipant, time.Since(started), err)
	return err
}

// UpdateParticipantPermissions implements livekit.RoomClient.
func (c *InstrumentedRoomClient) UpdateParticipantPermissions(ctx context.Context, roomName, identity string, canPublish bool) error {
	started := time.Now()
	err := c.inner.UpdateParticipantPermissions(ctx, roomName, identity, canPublish)
	c.metrics.ObserveExternal(OperationUpdatePermissions, time.Since(started), err)
	return err
}

// ListParticipants implements livekit.RoomClient.
func (c *InstrumentedRoomClient) ListParticipants(ctx context.Context, roomName string) ([]livekit.Participant, error) {
	started := time.Now()
	participants, err := c.inner.ListParticipants(ctx, roomName)
	c.metrics.ObserveExternal(OperationListParticipants, time.Since(started), err)
	return participants, err
}
