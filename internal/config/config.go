// Package config loads and validates the service configuration from the
// process environment.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Operational defaults. They match the values documented in
// docs/engineering_guide.md.
const (
	// DefaultPort is the HTTP listen port used when PORT is unset.
	DefaultPort = "8080"
	// DefaultKafkaConsumerGroup is the shared consumer group of the service.
	DefaultKafkaConsumerGroup = "voice_service"
	// DefaultSeatReservationTTL bounds how long a reserved seat stays live
	// while the client has not connected yet.
	DefaultSeatReservationTTL = 60 * time.Second
	// DefaultParticipantTokenTTL is the lifetime of minted LiveKit tokens.
	DefaultParticipantTokenTTL = 5 * time.Minute
	// MaxParticipantTokenTTL is the hard upper bound for participant tokens
	// (pinned decision: short-lived, <= 5 minutes).
	MaxParticipantTokenTTL = 5 * time.Minute
	// DefaultCommunityCheckTimeout is the budget of one API-002 access check,
	// including its single retry.
	DefaultCommunityCheckTimeout = 2 * time.Second
	// DefaultReconcileInterval is how often the presence sweep runs.
	DefaultReconcileInterval = 60 * time.Second
	// DefaultShutdownTimeout bounds the graceful drain.
	DefaultShutdownTimeout = 20 * time.Second
	// DefaultLiveKitCallTimeout bounds a single LiveKit API call.
	DefaultLiveKitCallTimeout = 5 * time.Second
	// DefaultEventRetention is how long processed event ids are kept for
	// Kafka deduplication.
	DefaultEventRetention = 7 * 24 * time.Hour
	// DefaultLogLevel is the zap level used when LOG_LEVEL is unset.
	DefaultLogLevel = "info"
)

// Config is the fully validated runtime configuration of voice_service.
type Config struct {
	// Port is the HTTP listen port.
	Port string
	// DatabaseURL is the PostgreSQL connection string.
	DatabaseURL string
	// KafkaBrokers is the bootstrap broker list.
	KafkaBrokers []string
	// KafkaConsumerGroup is the shared consumer group for community_events.
	KafkaConsumerGroup string
	// CommunityServiceURL is the base URL of community_service (API-002).
	CommunityServiceURL string
	// ServiceToken is the per-edge bearer token presented on API-002 calls.
	ServiceToken string
	// IdentityJWTPublicKeys holds the identity verification keys (current and
	// next, for rotation) as PEM blocks, base64-encoded PEM blocks, or RSA
	// JWK JSON objects.
	IdentityJWTPublicKeys []string
	// IdentityJWTIssuer, when set, is the required iss claim.
	IdentityJWTIssuer string
	// IdentityJWTAudience, when set, is the required aud claim.
	IdentityJWTAudience string
	// LiveKitURL is the LiveKit Cloud endpoint handed to clients.
	LiveKitURL string
	// LiveKitAPIKey and LiveKitAPISecret are the server credentials.
	LiveKitAPIKey string
	// LiveKitAPISecret must never be logged or exposed to clients.
	LiveKitAPISecret string
	// LiveKitWebhookSecret verifies signed webhook Authorization headers.
	LiveKitWebhookSecret string
	// SeatReservationTTL is the lifetime of a seat reservation.
	SeatReservationTTL time.Duration
	// ParticipantTokenTTL is the lifetime of minted participant tokens.
	ParticipantTokenTTL time.Duration
	// CommunityCheckTimeout is the API-002 budget including one retry.
	CommunityCheckTimeout time.Duration
	// ReconcileInterval is the period of the reconciliation sweep.
	ReconcileInterval time.Duration
	// ShutdownTimeout bounds the graceful shutdown drain.
	ShutdownTimeout time.Duration
	// LiveKitCallTimeout bounds a single LiveKit API call.
	LiveKitCallTimeout time.Duration
	// EventRetention is the retention window of processed Kafka event ids.
	EventRetention time.Duration
	// LogLevel is the zap level name.
	LogLevel string
}

// Load reads the configuration from the process environment.
func Load() (*Config, error) {
	return LoadFrom(os.LookupEnv)
}

// LoadFrom builds a configuration from the supplied lookup function. It is the
// testable core of Load and reports every missing or invalid variable at once.
func LoadFrom(lookup func(string) (string, bool)) (*Config, error) {
	get := func(key string) string {
		v, ok := lookup(key)
		if !ok {
			return ""
		}
		return strings.TrimSpace(v)
	}

	cfg := &Config{
		Port:                  valueOr(get("PORT"), DefaultPort),
		DatabaseURL:           get("DATABASE_URL"),
		KafkaBrokers:          splitList(get("KAFKA_BROKERS")),
		KafkaConsumerGroup:    valueOr(get("KAFKA_CONSUMER_GROUP"), DefaultKafkaConsumerGroup),
		CommunityServiceURL:   strings.TrimSuffix(get("COMMUNITY_SERVICE_URL"), "/"),
		ServiceToken:          get("SERVICE_TOKEN"),
		IdentityJWTPublicKeys: splitList(get("IDENTITY_JWT_PUBLIC_KEYS")),
		IdentityJWTIssuer:     get("IDENTITY_JWT_ISSUER"),
		IdentityJWTAudience:   get("IDENTITY_JWT_AUDIENCE"),
		LiveKitURL:            get("LIVEKIT_URL"),
		LiveKitAPIKey:         get("LIVEKIT_API_KEY"),
		LiveKitAPISecret:      get("LIVEKIT_API_SECRET"),
		LiveKitWebhookSecret:  get("LIVEKIT_WEBHOOK_SECRET"),
		LogLevel:              valueOr(get("LOG_LEVEL"), DefaultLogLevel),
	}

	var errs []error
	required := map[string]string{
		"DATABASE_URL":           cfg.DatabaseURL,
		"COMMUNITY_SERVICE_URL":  cfg.CommunityServiceURL,
		"SERVICE_TOKEN":          cfg.ServiceToken,
		"LIVEKIT_URL":            cfg.LiveKitURL,
		"LIVEKIT_API_KEY":        cfg.LiveKitAPIKey,
		"LIVEKIT_API_SECRET":     cfg.LiveKitAPISecret,
		"LIVEKIT_WEBHOOK_SECRET": cfg.LiveKitWebhookSecret,
	}
	for _, name := range sortedKeys(required) {
		if required[name] == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	if len(cfg.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required"))
	}
	if len(cfg.IdentityJWTPublicKeys) == 0 {
		errs = append(errs, errors.New("IDENTITY_JWT_PUBLIC_KEYS is required"))
	}

	var err error
	if cfg.SeatReservationTTL, err = duration(get, "SEAT_RESERVATION_TTL", DefaultSeatReservationTTL); err != nil {
		errs = append(errs, err)
	}
	if cfg.ParticipantTokenTTL, err = duration(get, "PARTICIPANT_TOKEN_TTL", DefaultParticipantTokenTTL); err != nil {
		errs = append(errs, err)
	} else if cfg.ParticipantTokenTTL > MaxParticipantTokenTTL {
		errs = append(errs, fmt.Errorf("PARTICIPANT_TOKEN_TTL must not exceed %s", MaxParticipantTokenTTL))
	}
	if cfg.CommunityCheckTimeout, err = duration(get, "COMMUNITY_CHECK_TIMEOUT", DefaultCommunityCheckTimeout); err != nil {
		errs = append(errs, err)
	}
	if cfg.ReconcileInterval, err = duration(get, "RECONCILE_INTERVAL", DefaultReconcileInterval); err != nil {
		errs = append(errs, err)
	}
	if cfg.ShutdownTimeout, err = duration(get, "SHUTDOWN_TIMEOUT", DefaultShutdownTimeout); err != nil {
		errs = append(errs, err)
	}
	if cfg.LiveKitCallTimeout, err = duration(get, "LIVEKIT_CALL_TIMEOUT", DefaultLiveKitCallTimeout); err != nil {
		errs = append(errs, err)
	}
	if cfg.EventRetention, err = duration(get, "EVENT_RETENTION", DefaultEventRetention); err != nil {
		errs = append(errs, err)
	}
	if cfg.SeatReservationTTL <= 0 {
		errs = append(errs, errors.New("SEAT_RESERVATION_TTL must be positive"))
	}
	if cfg.ParticipantTokenTTL <= 0 {
		errs = append(errs, errors.New("PARTICIPANT_TOKEN_TTL must be positive"))
	}
	if cfg.CommunityCheckTimeout <= 0 {
		errs = append(errs, errors.New("COMMUNITY_CHECK_TIMEOUT must be positive"))
	}
	if cfg.ReconcileInterval <= 0 {
		errs = append(errs, errors.New("RECONCILE_INTERVAL must be positive"))
	}
	if port, convErr := strconv.Atoi(cfg.Port); convErr != nil || port <= 0 || port > 65535 {
		errs = append(errs, fmt.Errorf("PORT %q is not a valid TCP port", cfg.Port))
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("invalid configuration: %w", errors.Join(errs...))
	}
	return cfg, nil
}

// valueOr returns v when non-empty, otherwise fallback.
func valueOr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

// splitList splits a comma separated list, dropping empty entries. Newlines are
// preserved because a PEM public key spans several lines.
func splitList(raw string) []string {
	if raw == "" {
		return nil
	}
	fields := strings.Split(raw, ",")
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		if trimmed := strings.TrimSpace(f); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// duration parses an optional Go duration, falling back to def.
func duration(get func(string) string, key string, def time.Duration) (time.Duration, error) {
	raw := get(key)
	if raw == "" {
		return def, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return def, fmt.Errorf("%s: %w", key, err)
	}
	return parsed, nil
}

// sortedKeys returns the keys of m in a deterministic order.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
