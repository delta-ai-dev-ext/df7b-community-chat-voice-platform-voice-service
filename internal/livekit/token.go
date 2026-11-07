package livekit

import (
	"context"
	"fmt"
	"time"

	"github.com/livekit/protocol/auth"
)

// Track sources granted to a publishing participant.
var publishSources = []string{
	"microphone",
	"camera",
	"screen_share",
}

// SDKTokenMinter mints participant tokens with github.com/livekit/protocol/auth.
type SDKTokenMinter struct {
	apiKey    string
	apiSecret string
	now       func() time.Time
}

// NewSDKTokenMinter builds a token minter from the deployment credentials. The
// secret is never logged or exposed to clients.
func NewSDKTokenMinter(apiKey, apiSecret string) *SDKTokenMinter {
	return &SDKTokenMinter{apiKey: apiKey, apiSecret: apiSecret, now: time.Now}
}

// WithClock replaces the minter clock (used by tests).
func (m *SDKTokenMinter) WithClock(now func() time.Time) *SDKTokenMinter {
	if now != nil {
		m.now = now
	}
	return m
}

// MintParticipantToken mints a short-lived token that joins one room, may
// subscribe, and may publish only when publishing is granted. A muted member
// receives a listen-only token.
func (m *SDKTokenMinter) MintParticipantToken(_ context.Context, req TokenRequest) (Token, error) {
	if req.RoomName == "" {
		return Token{}, fmt.Errorf("livekit: token requires a room name")
	}
	if req.Identity == "" {
		return Token{}, fmt.Errorf("livekit: token requires an identity")
	}
	ttl := req.TTL
	if ttl <= 0 || ttl > MaxParticipantTokenTTL {
		ttl = MaxParticipantTokenTTL
	}
	issuedAt := m.now().UTC()
	canPublish := req.CanPublish
	grant := &auth.VideoGrant{
		Room:           req.RoomName,
		RoomJoin:       true,
		CanSubscribe:   ptr(true),
		CanPublish:     &canPublish,
		CanPublishData: &canPublish,
	}
	if canPublish {
		grant.CanPublishSources = publishSources
	}

	at := auth.NewAccessToken(m.apiKey, m.apiSecret).
		SetIdentity(req.Identity).
		SetValidFor(ttl).
		SetVideoGrant(grant)
	if req.Name != "" {
		at.SetName(req.Name)
	}
	value, err := at.ToJWT()
	if err != nil {
		return Token{}, fmt.Errorf("livekit: mint participant token: %w", err)
	}
	return Token{Value: value, ExpiresAt: issuedAt.Add(ttl)}, nil
}

// ptr returns a pointer to v.
func ptr[T any](v T) *T { return &v }
