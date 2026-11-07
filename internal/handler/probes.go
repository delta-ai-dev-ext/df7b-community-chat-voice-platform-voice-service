package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"voice_service/internal/livekit"
	"voice_service/internal/models"
)

// receiveWebhook handles POST /voice/livekit/webhooks. The signed Authorization
// header (API secret + body hash) is verified before any state change; the
// trace id is minted at this edge because no user request caused the event.
func (h *Handler) receiveWebhook(c *gin.Context) {
	event, err := h.deps.Verifier.Verify(c.Request)
	if err != nil {
		if livekit.IsInvalidSignature(err) {
			h.deps.Log.Warn("livekit webhook rejected",
				zap.String("trace_id", traceID(c)),
				zap.Bool("invalid_signature", true),
			)
			writeError(c, apiErr{status: http.StatusUnauthorized, code: models.ErrCodeInvalidSignature})
			return
		}
		writeError(c, apiErr{status: http.StatusBadRequest, code: models.ErrCodeInvalidRequest})
		return
	}
	if err := h.deps.Webhook.Process(c.Request.Context(), event, traceIDOf(c)); err != nil {
		if livekit.IsMalformedWebhook(err) {
			writeError(c, apiErr{status: http.StatusBadRequest, code: models.ErrCodeInvalidRequest})
			return
		}
		h.deps.Log.Error("livekit webhook processing failed",
			zap.String("trace_id", traceID(c)),
			zap.String("event_kind", event.GetEvent()),
			zap.Error(err),
		)
		writeError(c, apiErr{status: http.StatusInternalServerError, code: models.ErrCodeInternal})
		return
	}
	c.Status(http.StatusOK)
}

// healthz handles GET /healthz. It only proves the process is responsive and
// never fails on transient dependency problems.
func (h *Handler) healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// readyz handles GET /readyz: 503 once shutdown started or a critical
// dependency (PostgreSQL, Kafka) is unreachable.
func (h *Handler) readyz(c *gin.Context) {
	if h.deps.Readiness == nil {
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
		return
	}
	if err := h.deps.Readiness.Check(c.Request.Context()); err != nil {
		c.JSON(http.StatusServiceUnavailable, models.ErrorResponse{
			Error:  "not_ready",
			Detail: err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}
