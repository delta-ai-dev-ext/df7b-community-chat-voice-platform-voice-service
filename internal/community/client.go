// Package community is the typed client for community_service's API-002 voice
// access check.
package community

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"

	"voice_service/internal/events"
	"voice_service/internal/models"
)

// Errors reported by the API-002 client.
var (
	// ErrUnavailable reports that the check could not be performed (timeout,
	// 5xx, malformed response). Callers must fail closed.
	ErrUnavailable = errors.New("community: voice access check unavailable")
	// ErrCommunityNotFound reports API-002 answering 404.
	ErrCommunityNotFound = errors.New("community: community not found")
)

// AccessCheckRequest is one API-002 request.
type AccessCheckRequest struct {
	// CommunityID is the community that owns the channel.
	CommunityID uuid.UUID
	// ChannelID is the requested voice channel.
	ChannelID uuid.UUID
	// UserID is the caller.
	UserID uuid.UUID
	// TraceID is propagated to the outbound call.
	TraceID string
}

// AccessCheckResult is the decoded 200 response of API-002.
type AccessCheckResult struct {
	// CommunityID echoes the community.
	CommunityID uuid.UUID
	// UserID echoes the member.
	UserID uuid.UUID
	// Allowed reports whether the join may proceed.
	Allowed bool
	// DenialReason is set when Allowed is false.
	DenialReason string
	// Channel is the requested voice channel (nil when it does not exist or is
	// not a voice channel).
	Channel *models.ChannelSummary
	// MutedUntil is non-nil and in the future for a muted (listen-only) member.
	MutedUntil *time.Time
	// VoiceChannels lists every voice channel of the community.
	VoiceChannels []models.ChannelSummary
}

// AccessChecker performs the API-002 voice access check.
type AccessChecker interface {
	// CheckVoiceAccess performs one access check for the join hot path.
	CheckVoiceAccess(ctx context.Context, req AccessCheckRequest) (*AccessCheckResult, error)
}

// Client is the HTTP implementation of AccessChecker.
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
	timeout    time.Duration
	now        func() time.Time
}

// maxAttempts is the check budget: the first call plus exactly one retry.
const maxAttempts = 2

// NewClient builds an API-002 client. The timeout bounds one whole check
// including its single retry (docs/architecture.md §Inter-Service Contracts).
func NewClient(baseURL, serviceToken string, timeout time.Duration) *Client {
	return &Client{
		baseURL:    baseURL,
		token:      serviceToken,
		httpClient: &http.Client{},
		timeout:    timeout,
		now:        time.Now,
	}
}

// WithHTTPClient replaces the HTTP client (used by tests).
func (c *Client) WithHTTPClient(client *http.Client) *Client {
	if client != nil {
		c.httpClient = client
	}
	return c
}

// voiceAccessCheckBody is the API-002 request body.
type voiceAccessCheckBody struct {
	UserID string `json:"user_id"`
}

// voiceAccessCheckResponse is the API-002 200 response.
type voiceAccessCheckResponse struct {
	CommunityID   string                  `json:"community_id"`
	UserID        string                  `json:"user_id"`
	Allowed       bool                    `json:"allowed"`
	DenialReason  *string                 `json:"denial_reason"`
	Channel       *models.ChannelSummary  `json:"channel"`
	MutedUntil    *time.Time              `json:"muted_until"`
	VoiceChannels []models.ChannelSummary `json:"voice_channels"`
}

// CheckVoiceAccess calls API-002 with the service bearer token, a bounded
// budget, and exactly one retry on a 5xx or transport error. Any failure other
// than a 404 is reported as ErrUnavailable so the join fails closed.
func (c *Client) CheckVoiceAccess(ctx context.Context, req AccessCheckRequest) (*AccessCheckResult, error) {
	if req.CommunityID == uuid.Nil || req.ChannelID == uuid.Nil || req.UserID == uuid.Nil {
		return nil, fmt.Errorf("community: access check requires community, channel, and user ids")
	}
	checkCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	body, err := json.Marshal(voiceAccessCheckBody{UserID: req.UserID.String()})
	if err != nil {
		return nil, fmt.Errorf("community: encode access check: %w", err)
	}
	endpoint := fmt.Sprintf("%s/internal/communities/%s/channels/%s/voice-access-checks",
		c.baseURL, req.CommunityID, req.ChannelID)

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		result, err := c.do(checkCtx, endpoint, body, req.TraceID)
		if err == nil {
			return result, nil
		}
		if errors.Is(err, ErrCommunityNotFound) {
			return nil, err
		}
		lastErr = err
		if !retryable(err) || attempt == maxAttempts {
			break
		}
		if checkCtx.Err() != nil {
			break
		}
	}
	if lastErr == nil {
		lastErr = ErrUnavailable
	}
	return nil, fmt.Errorf("%w: %v", ErrUnavailable, lastErr)
}

// retryable reports whether the failure is safe to retry once (read-only call).
func retryable(err error) bool {
	var statusErr *statusError
	if errors.As(err, &statusErr) {
		return statusErr.status >= http.StatusInternalServerError
	}
	// Transport errors and timeouts are retryable.
	return true
}

// statusError carries a non-200 API-002 status.
type statusError struct {
	status int
	detail string
}

// Error implements the error interface.
func (e *statusError) Error() string {
	return fmt.Sprintf("api-002 status %d: %s", e.status, e.detail)
}

// do performs one HTTP attempt.
func (c *Client) do(ctx context.Context, endpoint string, body []byte, traceID string) (*AccessCheckResult, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("community: build access check request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	httpReq.Header.Set(events.TraceparentHeader, events.Traceparent(traceID))
	httpReq.Header.Set(events.TraceIDHeader, traceID)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("community: call api-002: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, ErrCommunityNotFound
	default:
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, &statusError{status: resp.StatusCode, detail: string(detail)}
	}

	var payload voiceAccessCheckResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		return nil, fmt.Errorf("community: decode access check: %w", err)
	}
	return payload.toResult()
}

// toResult validates and converts the untrusted API-002 body.
func (p voiceAccessCheckResponse) toResult() (*AccessCheckResult, error) {
	communityID, err := uuid.Parse(p.CommunityID)
	if err != nil {
		return nil, fmt.Errorf("community: invalid community_id in response: %w", err)
	}
	userID, err := uuid.Parse(p.UserID)
	if err != nil {
		return nil, fmt.Errorf("community: invalid user_id in response: %w", err)
	}
	result := &AccessCheckResult{
		CommunityID:   communityID,
		UserID:        userID,
		Allowed:       p.Allowed,
		Channel:       p.Channel,
		MutedUntil:    p.MutedUntil,
		VoiceChannels: p.VoiceChannels,
	}
	if p.DenialReason != nil {
		result.DenialReason = *p.DenialReason
	}
	if result.Channel == nil && result.Allowed {
		return nil, fmt.Errorf("community: allowed response carries no channel")
	}
	if result.Channel != nil {
		if result.Channel.Capacity < models.MinCapacity || result.Channel.Capacity > models.MaxCapacity {
			return nil, fmt.Errorf("community: channel capacity %d is out of range", result.Channel.Capacity)
		}
	}
	return result, nil
}

// Now reports the client clock (used to reason about mute deadlines in tests).
func (c *Client) Now() time.Time { return c.now() }
