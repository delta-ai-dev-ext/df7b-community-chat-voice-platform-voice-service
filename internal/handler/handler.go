// Package handler exposes the HTTP surface of voice_service: the voice
// endpoints of docs/openapi_spec.yaml, LiveKit webhook ingestion, and probes.
package handler

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"voice_service/internal/livekit"
	"voice_service/internal/models"
	"voice_service/internal/service"
)

// Gin context keys.
const (
	// contextTraceID holds the trace id derived from the inbound request.
	contextTraceID = "voice.trace_id"
	// contextClaims holds the verified access-token claims.
	contextClaims = "voice.claims"
)

// Deps are the HTTP dependencies of the service.
type Deps struct {
	// Join orchestrates the join flow.
	Join *service.JoinService
	// Leave orchestrates the leave flow.
	Leave *service.LeaveService
	// Presence serves the presence snapshot.
	Presence *service.PresenceService
	// Webhook maps verified LiveKit webhooks onto presence.
	Webhook *service.WebhookProcessor
	// Verifier verifies LiveKit webhook signatures.
	Verifier *livekit.WebhookVerifier
	// Tokens verifies identity access tokens locally.
	Tokens *service.TokenVerifier
	// Readiness answers /readyz.
	Readiness *service.Readiness
	// Metrics collects HTTP and business metrics.
	Metrics *service.Metrics
	// Log is the structured logger.
	Log *zap.Logger
	// Gatherer exposes the Prometheus registry on /metrics.
	Gatherer prometheus.Gatherer
}

// Handler holds the HTTP dependencies of the service.
type Handler struct {
	deps Deps
}

// New builds the HTTP handler.
func New(deps Deps) *Handler {
	if deps.Log == nil {
		deps.Log = zap.NewNop()
	}
	return &Handler{deps: deps}
}

// Router builds the gin engine with every route of the service.
func (h *Handler) Router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery(), h.observe())

	engine.GET("/healthz", h.healthz)
	engine.GET("/readyz", h.readyz)
	if h.deps.Gatherer != nil {
		engine.GET("/metrics", gin.WrapH(promhttp.HandlerFor(h.deps.Gatherer, promhttp.HandlerOpts{})))
	}

	// The webhook endpoint authenticates with LiveKit's signed Authorization
	// header, never with an identity access token.
	engine.POST("/voice/livekit/webhooks", h.receiveWebhook)

	voice := engine.Group("/voice", h.authenticate)
	voice.POST("/channels/:channel_id/join", h.joinChannel)
	voice.POST("/channels/:channel_id/leave", h.leaveChannel)

	communities := engine.Group("/communities", h.authenticate)
	communities.GET("/:community_id/voice-presence", h.voicePresence)

	engine.NoRoute(func(c *gin.Context) {
		writeError(c, apiErr{status: http.StatusNotFound, code: "not_found", detail: "The requested endpoint does not exist."})
	})
	return engine
}

// observe derives the trace id and records RED metrics for every request.
func (h *Handler) observe() gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		traceID := traceIDFromHeaders(c)
		c.Set(contextTraceID, traceID)
		c.Writer.Header().Set("X-Trace-Id", traceID)

		c.Next()

		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		h.deps.Metrics.ObserveHTTP(route, c.Request.Method, c.Writer.Status(), time.Since(started))
	}
}

// apiErr is one {"error","detail"} response with its HTTP status.
type apiErr struct {
	status int
	code   string
	detail string
}

// Error detail texts of the public error codes.
var errorDetails = map[string]string{
	models.ErrCodeNotAMember:               "You are not a member of this community.",
	models.ErrCodeChannelNotFound:          "This voice channel does not exist.",
	models.ErrCodeNotAVoiceChannel:         "This channel is not a voice channel.",
	models.ErrCodeVoiceChannelFull:         "This voice channel is full.",
	models.ErrCodeAuthorizationUnavailable: "Could not verify access to the voice channel. Try again shortly.",
	models.ErrCodeMediaUnavailable:         "The realtime media service is temporarily unavailable. Try again shortly.",
	models.ErrCodeUnauthorized:             "Authentication required.",
	models.ErrCodeInvalidRequest:           "The request body could not be parsed.",
	models.ErrCodeInvalidSignature:         "Webhook signature verification failed.",
	models.ErrCodeInternal:                 "Something went wrong. Please try again.",
}

// writeError writes one {"error","detail"} response.
func writeError(c *gin.Context, failure apiErr) {
	detail := failure.detail
	if detail == "" {
		detail = errorDetails[failure.code]
	}
	c.AbortWithStatusJSON(failure.status, models.ErrorResponse{Error: failure.code, Detail: detail})
}

// traceIDFromHeaders derives the trace id of the inbound request, minting one
// at the edge when the caller propagates nothing usable.
func traceIDFromHeaders(c *gin.Context) string {
	return traceIDFromHTTPHeader(c.Request.Header)
}
