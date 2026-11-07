// Package events owns the mandatory {"envelope","payload"} wire format, the
// EVT-010 producer, and the community_events consumer of voice_service.
package events

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

// Header names used for trace propagation.
const (
	// TraceparentHeader carries the W3C trace context.
	TraceparentHeader = "traceparent"
	// TraceIDHeader carries a plain 32-hex trace id.
	TraceIDHeader = "X-Trace-Id"
)

// traceIDLength is the length of a 32-hex trace id.
const traceIDLength = 32

var traceIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

// NewTraceID mints a fresh lowercase-hex trace id.
func NewTraceID() string {
	return randomHex(16)
}

// NewSpanID mints a fresh lowercase-hex span id (16 hex characters).
func NewSpanID() string {
	return randomHex(8)
}

// ValidTraceID reports whether id is a 32-character lowercase-hex trace id.
func ValidTraceID(id string) bool {
	return traceIDPattern.MatchString(id)
}

// randomHex returns n random bytes encoded as lowercase hex.
func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failing means the process cannot mint credentials; the
		// trace id still has to be non-empty, so fall back to a uuid.
		return strings.ReplaceAll(uuid.NewString(), "-", "")
	}
	return hex.EncodeToString(buf)
}

// TraceIDFromTraceparent extracts the trace id from a W3C traceparent header
// value such as "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".
func TraceIDFromTraceparent(value string) (string, bool) {
	parts := strings.Split(strings.TrimSpace(value), "-")
	if len(parts) < 4 {
		return "", false
	}
	trace := strings.ToLower(parts[1])
	if !ValidTraceID(trace) || trace == strings.Repeat("0", traceIDLength) {
		return "", false
	}
	return trace, true
}

// TraceIDFromHeader derives a trace id from the inbound request headers. It
// prefers the W3C traceparent header, then X-Trace-Id, and mints a fresh id
// when neither carries a usable value, so the result is never empty.
func TraceIDFromHeader(header http.Header) string {
	if trace, ok := TraceIDFromTraceparent(header.Get(TraceparentHeader)); ok {
		return trace
	}
	if candidate := strings.ToLower(strings.TrimSpace(header.Get(TraceIDHeader))); ValidTraceID(candidate) &&
		candidate != strings.Repeat("0", traceIDLength) {
		return candidate
	}
	return NewTraceID()
}

// Traceparent renders a traceparent header value for an outbound call. An
// empty or malformed trace id is replaced by a freshly minted one.
func Traceparent(traceID string) string {
	if !ValidTraceID(traceID) {
		traceID = NewTraceID()
	}
	return fmt.Sprintf("00-%s-%s-01", traceID, NewSpanID())
}

// ValidateTraceID returns an error when id is not a usable trace id.
func ValidateTraceID(id string) error {
	if !ValidTraceID(id) {
		return errors.New("trace_id must be 32 lowercase hex characters")
	}
	return nil
}
