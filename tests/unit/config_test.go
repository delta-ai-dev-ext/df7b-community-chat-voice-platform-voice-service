package unit

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"voice_service/internal/config"
)

// lookupFrom builds an env lookup function from a map.
func lookupFrom(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

// minimalEnv is the smallest valid environment.
func minimalEnv() map[string]string {
	return map[string]string{
		"DATABASE_URL":             "postgres://voice:voice@localhost:5432/voice_service",
		"KAFKA_BROKERS":            "localhost:9092,localhost:9093",
		"COMMUNITY_SERVICE_URL":    "http://community:8081",
		"SERVICE_TOKEN":            "service-token",
		"LIVEKIT_URL":              "wss://livekit.test",
		"LIVEKIT_API_KEY":          "key",
		"LIVEKIT_API_SECRET":       "secret",
		"LIVEKIT_WEBHOOK_SECRET":   "webhook-secret",
		"IDENTITY_JWT_PUBLIC_KEYS": "-----BEGIN PUBLIC KEY-----\nMFww\n-----END PUBLIC KEY-----",
	}
}

// TestLoadDefaults covers the documented defaults.
func TestLoadDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := config.LoadFrom(lookupFrom(minimalEnv()))
	require.NoError(t, err)

	assert.Equal(t, config.DefaultPort, cfg.Port)
	assert.Equal(t, config.DefaultKafkaConsumerGroup, cfg.KafkaConsumerGroup)
	assert.Equal(t, config.DefaultSeatReservationTTL, cfg.SeatReservationTTL)
	assert.Equal(t, config.DefaultParticipantTokenTTL, cfg.ParticipantTokenTTL)
	assert.Equal(t, config.DefaultCommunityCheckTimeout, cfg.CommunityCheckTimeout)
	assert.Equal(t, config.DefaultReconcileInterval, cfg.ReconcileInterval)
	assert.Equal(t, config.DefaultShutdownTimeout, cfg.ShutdownTimeout)
	assert.Equal(t, config.DefaultEventRetention, cfg.EventRetention)
	assert.Equal(t, config.DefaultLogLevel, cfg.LogLevel)
	assert.Equal(t, []string{"localhost:9092", "localhost:9093"}, cfg.KafkaBrokers)
	assert.Len(t, cfg.IdentityJWTPublicKeys, 1)
	assert.Equal(t, "http://community:8081", cfg.CommunityServiceURL)
}

// TestLoadRejectsMissingRequiredVariables covers the fail-fast rule.
func TestLoadRejectsMissingRequiredVariables(t *testing.T) {
	t.Parallel()
	env := minimalEnv()
	delete(env, "DATABASE_URL")
	delete(env, "KAFKA_BROKERS")
	delete(env, "IDENTITY_JWT_PUBLIC_KEYS")
	delete(env, "LIVEKIT_WEBHOOK_SECRET")

	_, err := config.LoadFrom(lookupFrom(env))
	require.Error(t, err)
	for _, variable := range []string{"DATABASE_URL", "KAFKA_BROKERS", "IDENTITY_JWT_PUBLIC_KEYS", "LIVEKIT_WEBHOOK_SECRET"} {
		assert.Contains(t, err.Error(), variable)
	}
}

// TestLoadRejectsInvalidValues covers parsing and range guards.
func TestLoadRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	cases := map[string]map[string]string{
		"bad duration":      {"SEAT_RESERVATION_TTL": "sixty"},
		"zero ttl":          {"SEAT_RESERVATION_TTL": "0s"},
		"long token ttl":    {"PARTICIPANT_TOKEN_TTL": "10m"},
		"bad port":          {"PORT": "not-a-port"},
		"out of range port": {"PORT": "70000"},
		"bad reconcile":     {"RECONCILE_INTERVAL": "-1s"},
	}
	for name, overrides := range cases {
		overrides := overrides
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := minimalEnv()
			for key, value := range overrides {
				env[key] = value
			}
			_, err := config.LoadFrom(lookupFrom(env))
			require.Error(t, err)
		})
	}
}

// TestLoadAcceptsTuningOverrides covers the operational tuning knobs.
func TestLoadAcceptsTuningOverrides(t *testing.T) {
	t.Parallel()
	env := minimalEnv()
	env["PORT"] = "9000"
	env["SEAT_RESERVATION_TTL"] = "30s"
	env["PARTICIPANT_TOKEN_TTL"] = "2m"
	env["COMMUNITY_CHECK_TIMEOUT"] = "1500ms"
	env["RECONCILE_INTERVAL"] = "15s"
	env["SHUTDOWN_TIMEOUT"] = "10s"
	env["EVENT_RETENTION"] = "24h"
	env["LOG_LEVEL"] = "debug"
	env["IDENTITY_JWT_ISSUER"] = "https://identity.test"
	env["IDENTITY_JWT_AUDIENCE"] = "platform"
	env["COMMUNITY_SERVICE_URL"] = "http://community:8081/"

	cfg, err := config.LoadFrom(lookupFrom(env))
	require.NoError(t, err)
	assert.Equal(t, "9000", cfg.Port)
	assert.Equal(t, 30*time.Second, cfg.SeatReservationTTL)
	assert.Equal(t, 2*time.Minute, cfg.ParticipantTokenTTL)
	assert.Equal(t, 1500*time.Millisecond, cfg.CommunityCheckTimeout)
	assert.Equal(t, 15*time.Second, cfg.ReconcileInterval)
	assert.Equal(t, 10*time.Second, cfg.ShutdownTimeout)
	assert.Equal(t, 24*time.Hour, cfg.EventRetention)
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, "https://identity.test", cfg.IdentityJWTIssuer)
	assert.Equal(t, "platform", cfg.IdentityJWTAudience)
	assert.Equal(t, "http://community:8081", cfg.CommunityServiceURL, "a trailing slash is trimmed")
}

// TestLoadTrimsValues covers whitespace tolerance of the env parsing.
func TestLoadTrimsValues(t *testing.T) {
	t.Parallel()
	env := minimalEnv()
	env["PORT"] = " 8080 "
	env["SERVICE_TOKEN"] = "  token  "
	cfg, err := config.LoadFrom(lookupFrom(env))
	require.NoError(t, err)
	assert.Equal(t, "8080", cfg.Port)
	assert.Equal(t, "token", cfg.ServiceToken)
	assert.False(t, strings.Contains(cfg.Port, " "))
}
