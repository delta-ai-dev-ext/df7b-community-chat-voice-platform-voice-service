package handler

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"voice_service/internal/events"
	"voice_service/internal/models"
)

// authenticate verifies the bearer access token locally and stores the claims
// in the request context. Missing, invalid, or expired tokens yield 401.
func (h *Handler) authenticate(c *gin.Context) {
	raw, ok := bearerToken(c.GetHeader("Authorization"))
	if !ok {
		writeError(c, apiErr{status: http.StatusUnauthorized, code: models.ErrCodeUnauthorized})
		return
	}
	claims, err := h.deps.Tokens.Verify(raw)
	if err != nil {
		h.deps.Log.Debug("access token rejected",
			zap.String("trace_id", traceID(c)),
			zap.String("route", c.FullPath()),
			zap.Error(err),
		)
		writeError(c, apiErr{status: http.StatusUnauthorized, code: models.ErrCodeUnauthorized})
		return
	}
	claims.TraceID = traceID(c)
	c.Set(contextClaims, claims)
	c.Next()
}

// claims returns the verified claims of the current request.
func claims(c *gin.Context) *models.AccessTokenClaims {
	value, ok := c.Get(contextClaims)
	if !ok {
		return nil
	}
	verified, ok := value.(*models.AccessTokenClaims)
	if !ok {
		return nil
	}
	return verified
}

// traceID returns the trace id of the current request.
func traceID(c *gin.Context) string {
	value, ok := c.Get(contextTraceID)
	if !ok {
		return events.NewTraceID()
	}
	derived, ok := value.(string)
	if !ok || derived == "" {
		return events.NewTraceID()
	}
	return derived
}

// traceIDFromHTTPHeader derives the trace id from request headers, minting one
// when the caller propagates nothing usable (webhooks and scheduled flows).
func traceIDFromHTTPHeader(header http.Header) string {
	return events.TraceIDFromHeader(header)
}

// bearerToken extracts the credential of an "Authorization: Bearer <token>"
// header.
func bearerToken(header string) (string, bool) {
	const prefix = "bearer "
	trimmed := strings.TrimSpace(header)
	if len(trimmed) <= len(prefix) || !strings.EqualFold(trimmed[:len(prefix)], prefix) {
		return "", false
	}
	token := strings.TrimSpace(trimmed[len(prefix):])
	return token, token != ""
}
