package handler

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"voice_service/internal/events"
	"voice_service/internal/models"
	"voice_service/internal/service"
)

// joinChannel handles POST /voice/channels/{channel_id}/join.
func (h *Handler) joinChannel(c *gin.Context) {
	verified := claims(c)
	if verified == nil {
		writeError(c, apiErr{status: http.StatusUnauthorized, code: models.ErrCodeUnauthorized})
		return
	}
	channelID, ok := uuidParam(c, "channel_id")
	if !ok {
		writeError(c, apiErr{status: http.StatusBadRequest, code: models.ErrCodeInvalidRequest})
		return
	}
	var body models.JoinRequest
	if err := c.ShouldBindJSON(&body); err != nil {
		writeError(c, apiErr{status: http.StatusBadRequest, code: models.ErrCodeInvalidRequest})
		return
	}
	if body.CommunityID == uuid.Nil {
		writeError(c, apiErr{status: http.StatusBadRequest, code: models.ErrCodeInvalidRequest})
		return
	}

	response, err := h.deps.Join.Join(c.Request.Context(), service.JoinInput{
		CommunityID: body.CommunityID,
		ChannelID:   channelID,
		UserID:      verified.UserID,
		TraceID:     verified.TraceID,
	})
	if err != nil {
		h.writeJoinError(c, err)
		return
	}
	c.JSON(http.StatusOK, response)
}

// writeJoinError maps a join failure onto the documented response.
func (h *Handler) writeJoinError(c *gin.Context, err error) {
	var full *models.ChannelFullError
	if errors.As(err, &full) {
		alternatives := full.Alternatives
		if alternatives == nil {
			alternatives = []models.ChannelAlternative{}
		}
		c.AbortWithStatusJSON(http.StatusConflict, models.ChannelFullResponse{
			Error:        models.ErrCodeVoiceChannelFull,
			Detail:       errorDetails[models.ErrCodeVoiceChannelFull],
			Alternatives: alternatives,
		})
		return
	}
	switch {
	case errors.Is(err, models.ErrNotAMember):
		writeError(c, apiErr{status: http.StatusForbidden, code: models.ErrCodeNotAMember})
	case errors.Is(err, models.ErrChannelNotFound):
		writeError(c, apiErr{status: http.StatusNotFound, code: models.ErrCodeChannelNotFound})
	case errors.Is(err, models.ErrNotAVoiceChannel):
		writeError(c, apiErr{status: http.StatusNotFound, code: models.ErrCodeNotAVoiceChannel})
	case errors.Is(err, models.ErrAuthorizationUnavailable):
		writeError(c, apiErr{status: http.StatusServiceUnavailable, code: models.ErrCodeAuthorizationUnavailable})
	case errors.Is(err, models.ErrMediaUnavailable):
		writeError(c, apiErr{status: http.StatusServiceUnavailable, code: models.ErrCodeMediaUnavailable})
	case errors.Is(err, models.ErrInvalidRequest):
		writeError(c, apiErr{status: http.StatusBadRequest, code: models.ErrCodeInvalidRequest})
	case errors.Is(err, models.ErrUnauthorized):
		writeError(c, apiErr{status: http.StatusUnauthorized, code: models.ErrCodeUnauthorized})
	default:
		h.deps.Log.Error("join failed",
			zap.String("trace_id", traceID(c)),
			zap.String("channel_id", c.Param("channel_id")),
			zap.Error(err),
		)
		writeError(c, apiErr{status: http.StatusInternalServerError, code: models.ErrCodeInternal})
	}
}

// leaveChannel handles POST /voice/channels/{channel_id}/leave.
func (h *Handler) leaveChannel(c *gin.Context) {
	verified := claims(c)
	if verified == nil {
		writeError(c, apiErr{status: http.StatusUnauthorized, code: models.ErrCodeUnauthorized})
		return
	}
	channelID, ok := uuidParam(c, "channel_id")
	if !ok {
		writeError(c, apiErr{status: http.StatusBadRequest, code: models.ErrCodeInvalidRequest})
		return
	}
	err := h.deps.Leave.Leave(c.Request.Context(), service.LeaveInput{
		ChannelID: channelID,
		UserID:    verified.UserID,
		TraceID:   verified.TraceID,
	})
	switch {
	case err == nil:
		c.Status(http.StatusNoContent)
	case errors.Is(err, models.ErrChannelNotFound):
		writeError(c, apiErr{status: http.StatusNotFound, code: models.ErrCodeChannelNotFound})
	case errors.Is(err, models.ErrInvalidRequest):
		writeError(c, apiErr{status: http.StatusBadRequest, code: models.ErrCodeInvalidRequest})
	default:
		h.deps.Log.Error("leave failed",
			zap.String("trace_id", traceID(c)),
			zap.String("channel_id", c.Param("channel_id")),
			zap.Error(err),
		)
		writeError(c, apiErr{status: http.StatusInternalServerError, code: models.ErrCodeInternal})
	}
}

// voicePresence handles GET /communities/{community_id}/voice-presence.
func (h *Handler) voicePresence(c *gin.Context) {
	verified := claims(c)
	if verified == nil {
		writeError(c, apiErr{status: http.StatusUnauthorized, code: models.ErrCodeUnauthorized})
		return
	}
	communityID, ok := uuidParam(c, "community_id")
	if !ok {
		writeError(c, apiErr{status: http.StatusBadRequest, code: models.ErrCodeInvalidRequest})
		return
	}
	response, err := h.deps.Presence.Snapshot(c.Request.Context(), communityID, verified.UserID)
	switch {
	case err == nil:
		c.JSON(http.StatusOK, response)
	case errors.Is(err, models.ErrNotAMember):
		writeError(c, apiErr{status: http.StatusForbidden, code: models.ErrCodeNotAMember})
	case errors.Is(err, models.ErrInvalidRequest):
		writeError(c, apiErr{status: http.StatusBadRequest, code: models.ErrCodeInvalidRequest})
	default:
		h.deps.Log.Error("voice presence failed",
			zap.String("trace_id", traceID(c)),
			zap.String("community_id", c.Param("community_id")),
			zap.Error(err),
		)
		writeError(c, apiErr{status: http.StatusInternalServerError, code: models.ErrCodeInternal})
	}
}

// uuidParam parses a uuid path parameter.
func uuidParam(c *gin.Context, name string) (uuid.UUID, bool) {
	parsed, err := uuid.Parse(c.Param(name))
	if err != nil {
		return uuid.Nil, false
	}
	return parsed, true
}

// traceIDOf derives the trace id of a request for webhook-triggered flows: the
// webhook edge mints one when the caller propagates nothing.
func traceIDOf(c *gin.Context) string {
	trace := traceID(c)
	if events.ValidTraceID(trace) {
		return trace
	}
	return events.NewTraceID()
}
