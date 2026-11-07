package livekit

import (
	"context"
	"fmt"
	"strings"

	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

// SDKClient is the RoomClient implementation backed by the official LiveKit
// Go server SDK (github.com/livekit/server-sdk-go/v2).
type SDKClient struct {
	api *lksdk.LiveKitAPI
}

// NewSDKClient builds a LiveKit room client from the deployment credentials.
func NewSDKClient(url, apiKey, apiSecret string) (*SDKClient, error) {
	api, err := lksdk.NewLiveKitAPI(
		lksdk.WithURL(url),
		lksdk.WithAPIKey(apiKey, apiSecret),
	)
	if err != nil {
		return nil, fmt.Errorf("livekit: create room client: %w", err)
	}
	return &SDKClient{api: api}, nil
}

// CreateRoom creates the room with max participants equal to the channel
// capacity. Creating an already existing room is treated as success so a
// rejoin never fails on room lifecycle state.
func (c *SDKClient) CreateRoom(ctx context.Context, req CreateRoomRequest) error {
	_, err := c.api.Room().CreateRoom(ctx, roomRequest(req))
	if err != nil && !alreadyExists(err) {
		return fmt.Errorf("livekit: create room %q: %w", req.RoomName, err)
	}
	return nil
}

// roomRequest maps the domain request onto the SDK request. The channel
// capacity becomes the room's max-participant backstop.
func roomRequest(req CreateRoomRequest) *livekit.CreateRoomRequest {
	return &livekit.CreateRoomRequest{
		Name:            req.RoomName,
		MaxParticipants: uint32(req.MaxParticipants),
	}
}

// DeleteRoom closes the room.
func (c *SDKClient) DeleteRoom(ctx context.Context, roomName string) error {
	if _, err := c.api.Room().DeleteRoom(ctx, &livekit.DeleteRoomRequest{Room: roomName}); err != nil {
		if notFound(err) {
			return ErrRoomNotFound
		}
		return fmt.Errorf("livekit: delete room %q: %w", roomName, err)
	}
	return nil
}

// RemoveParticipant disconnects one participant.
func (c *SDKClient) RemoveParticipant(ctx context.Context, roomName, identity string) error {
	_, err := c.api.Room().RemoveParticipant(ctx, &livekit.RoomParticipantIdentity{
		Room:     roomName,
		Identity: identity,
	})
	if err != nil {
		if notFound(err) {
			return ErrParticipantNotFound
		}
		return fmt.Errorf("livekit: remove participant %q from %q: %w", identity, roomName, err)
	}
	return nil
}

// UpdateParticipantPermissions grants or revokes publishing in place.
func (c *SDKClient) UpdateParticipantPermissions(ctx context.Context, roomName, identity string, canPublish bool) error {
	_, err := c.api.Room().UpdateParticipant(ctx, &livekit.UpdateParticipantRequest{
		Room:     roomName,
		Identity: identity,
		Permission: &livekit.ParticipantPermission{
			CanSubscribe:   true,
			CanPublish:     canPublish,
			CanPublishData: canPublish,
		},
	})
	if err != nil {
		if notFound(err) {
			return ErrParticipantNotFound
		}
		return fmt.Errorf("livekit: update permissions of %q in %q: %w", identity, roomName, err)
	}
	return nil
}

// ListParticipants returns the current participants of a room.
func (c *SDKClient) ListParticipants(ctx context.Context, roomName string) ([]Participant, error) {
	resp, err := c.api.Room().ListParticipants(ctx, &livekit.ListParticipantsRequest{Room: roomName})
	if err != nil {
		if notFound(err) {
			return nil, ErrRoomNotFound
		}
		return nil, fmt.Errorf("livekit: list participants of %q: %w", roomName, err)
	}
	participants := make([]Participant, 0, len(resp.GetParticipants()))
	for _, info := range resp.GetParticipants() {
		participants = append(participants, normalizeParticipant(info))
	}
	return participants, nil
}

// normalizeParticipant maps SDK participant state onto the domain type.
func normalizeParticipant(info *livekit.ParticipantInfo) Participant {
	participant := Participant{Identity: info.GetIdentity()}
	for _, track := range info.GetTracks() {
		if track.GetMuted() {
			continue
		}
		switch track.GetSource() {
		case livekit.TrackSource_CAMERA:
			participant.CameraEnabled = true
		case livekit.TrackSource_SCREEN_SHARE, livekit.TrackSource_SCREEN_SHARE_AUDIO:
			participant.ScreenShareEnabled = true
		}
	}
	return participant
}

// alreadyExists reports whether LiveKit refused to create an existing room.
func alreadyExists(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "already exists")
}

// notFound reports whether LiveKit answered that the entity does not exist.
func notFound(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "not found")
}
