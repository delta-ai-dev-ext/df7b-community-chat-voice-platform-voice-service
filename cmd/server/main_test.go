package main

import (
	"strings"
	"testing"
)

// TestRunFailsFastWithoutRequiredConfiguration pins the startup contract: an
// empty environment is rejected with one error naming every missing variable,
// and nothing is dialed first (US-012, docs/engineering_guide.md §Configuration).
func TestRunFailsFastWithoutRequiredConfiguration(t *testing.T) {
	t.Parallel()

	err := run(func(string) (string, bool) { return "", false })
	if err == nil {
		t.Fatal("run returned no error for an empty environment")
	}
	if !strings.Contains(err.Error(), "invalid configuration") {
		t.Fatalf("error does not report invalid configuration: %v", err)
	}
	for _, name := range []string{
		"DATABASE_URL",
		"KAFKA_BROKERS",
		"COMMUNITY_SERVICE_URL",
		"SERVICE_TOKEN",
		"LIVEKIT_URL",
		"LIVEKIT_API_KEY",
		"LIVEKIT_API_SECRET",
		"LIVEKIT_WEBHOOK_SECRET",
		"IDENTITY_JWT_PUBLIC_KEYS",
	} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error does not name the missing %s: %v", name, err)
		}
	}
}

// TestRunRejectsUnreadableLogLevel pins that an invalid LOG_LEVEL fails startup
// instead of being silently ignored.
func TestRunRejectsUnreadableLogLevel(t *testing.T) {
	t.Parallel()

	environment := map[string]string{
		"DATABASE_URL":             "postgres://postgres:postgres@localhost:5432/voice_service?sslmode=disable",
		"KAFKA_BROKERS":            "localhost:9092",
		"COMMUNITY_SERVICE_URL":    "http://localhost:8081",
		"SERVICE_TOKEN":            "service-token",
		"LIVEKIT_URL":              "wss://example.livekit.cloud",
		"LIVEKIT_API_KEY":          "api-key",
		"LIVEKIT_API_SECRET":       "api-secret",
		"LIVEKIT_WEBHOOK_SECRET":   "webhook-secret",
		"IDENTITY_JWT_PUBLIC_KEYS": "not-a-key",
		"LOG_LEVEL":                "verbose",
	}
	err := run(func(key string) (string, bool) {
		value, ok := environment[key]
		return value, ok
	})
	if err == nil {
		t.Fatal("run returned no error for an unknown LOG_LEVEL")
	}
	if !strings.Contains(err.Error(), "LOG_LEVEL") {
		t.Fatalf("error does not report LOG_LEVEL: %v", err)
	}
}

// TestNewLogger covers the accepted and rejected log levels.
func TestNewLogger(t *testing.T) {
	t.Parallel()

	for _, level := range []string{"debug", "info", "warn", "error"} {
		logger, err := newLogger(level)
		if err != nil {
			t.Fatalf("newLogger(%q) returned error: %v", level, err)
		}
		if logger == nil {
			t.Fatalf("newLogger(%q) returned a nil logger", level)
		}
		if err := logger.Sync(); err != nil {
			t.Logf("sync of %q logger reported: %v", level, err)
		}
	}
	if _, err := newLogger("verbose"); err == nil {
		t.Fatal("newLogger accepted an unknown level")
	} else if !strings.Contains(err.Error(), "LOG_LEVEL") {
		t.Fatalf("error does not report LOG_LEVEL: %v", err)
	}
}
