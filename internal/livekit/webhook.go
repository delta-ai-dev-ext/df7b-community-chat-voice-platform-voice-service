package livekit

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/utils/protojson"
	"github.com/livekit/protocol/webhook"
)

// WebhookVerifier verifies LiveKit webhook signatures.
type WebhookVerifier struct {
	provider auth.KeyProvider
}

// NewWebhookVerifier builds a verifier for the deployment webhook secret. The
// API key is the key id LiveKit signs webhook tokens with.
func NewWebhookVerifier(apiKey, webhookSecret string) *WebhookVerifier {
	return &WebhookVerifier{provider: auth.NewSimpleKeyProvider(apiKey, webhookSecret)}
}

// Verify reads the body, verifies the signed Authorization header against the
// API secret and the sha256 body hash, and decodes the webhook event. Any
// signature problem is reported as ErrInvalidSignature; a signed but
// undecodable body is reported as ErrMalformedWebhook.
func (v *WebhookVerifier) Verify(r *http.Request) (*livekit.WebhookEvent, error) {
	data, err := webhook.Receive(r, v.provider)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSignature, err)
	}
	event := &livekit.WebhookEvent{}
	opts := protojson.UnmarshalOptions{DiscardUnknown: true, AllowPartial: true}
	if err := opts.Unmarshal(data, event); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedWebhook, err)
	}
	if event.GetEvent() == "" {
		return nil, fmt.Errorf("%w: missing event type", ErrMalformedWebhook)
	}
	if event.GetId() == "" {
		return nil, fmt.Errorf("%w: missing webhook event id", ErrMalformedWebhook)
	}
	return event, nil
}

// IsInvalidSignature reports whether err is a verification failure.
func IsInvalidSignature(err error) bool {
	return errors.Is(err, ErrInvalidSignature)
}

// IsMalformedWebhook reports whether err is a payload decoding failure.
func IsMalformedWebhook(err error) bool {
	return errors.Is(err, ErrMalformedWebhook)
}
