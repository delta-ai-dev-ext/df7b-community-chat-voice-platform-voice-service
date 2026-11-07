// Command server is the entrypoint of voice_service. It loads and validates the
// configuration, wires the adapters, runs the HTTP server, the community_events
// consumer, and the reconciliation worker, and shuts every component down
// gracefully on SIGTERM/SIGINT (docs/architecture.md §5).
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"voice_service/internal/community"
	"voice_service/internal/config"
	"voice_service/internal/events"
	"voice_service/internal/handler"
	"voice_service/internal/livekit"
	"voice_service/internal/repository"
	"voice_service/internal/service"
	"voice_service/migrations"
)

const (
	// startupTimeout bounds the startup phase: migrations, pool handshake,
	// Kafka clients, and the HTTP listener bind.
	startupTimeout = 30 * time.Second
	// probeTimeout bounds one readiness probe.
	probeTimeout = 2 * time.Second
	// readHeaderTimeout bounds the header read of one HTTP request.
	readHeaderTimeout = 10 * time.Second
	// idleTimeout bounds an idle keep-alive connection.
	idleTimeout = 60 * time.Second
)

// main runs the service and terminates the process with a non-zero exit code
// when startup fails or a component stops unexpectedly.
func main() {
	if err := run(os.LookupEnv); err != nil {
		fmt.Fprintf(os.Stderr, "voice_service: %v\n", err)
		os.Exit(1)
	}
}

// run builds and serves the whole process. The lookup function is the
// environment accessor, so the fail-fast startup path is testable without
// infrastructure.
func run(lookup func(string) (string, bool)) error {
	cfg, err := config.LoadFrom(lookup)
	if err != nil {
		return err
	}
	logger, err := newLogger(cfg.LogLevel)
	if err != nil {
		return err
	}
	defer func() { _ = logger.Sync() }()
	logger = logger.With(zap.String("service_name", events.ProducerName))

	// The signal context only decides when the drain starts; the startup phase
	// derives from it so a SIGTERM during startup is honored immediately.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := migrations.Up(cfg.DatabaseURL); err != nil {
		logger.Error("could not apply the database migrations", zap.Error(err))
		return err
	}

	startupCtx, cancelStartup := context.WithTimeout(ctx, startupTimeout)
	defer cancelStartup()

	pool, err := repository.NewPool(startupCtx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("could not open the postgres pool", zap.Error(err))
		return err
	}
	defer pool.Close()
	if err := repository.Ping(startupCtx, pool); err != nil {
		logger.Error("could not reach postgres", zap.Error(err))
		return err
	}
	repos := repository.NewSet(pool)

	// LiveKit is a runtime dependency only: constructing the SDK client does
	// not probe the cloud and must never block startup (US-012 out of scope).
	rooms, err := livekit.NewSDKClient(cfg.LiveKitURL, cfg.LiveKitAPIKey, cfg.LiveKitAPISecret)
	if err != nil {
		logger.Error("could not build the livekit room client", zap.Error(err))
		return err
	}

	tokens, err := service.NewTokenVerifier(cfg.IdentityJWTPublicKeys, cfg.IdentityJWTIssuer, cfg.IdentityJWTAudience)
	if err != nil {
		logger.Error("could not build the access-token verifier", zap.Error(err))
		return err
	}

	registry := prometheus.NewRegistry()
	metrics := service.NewMetrics(registry)

	producer, err := events.NewKafkaProducer(cfg.KafkaBrokers, events.TopicVoiceEvents, logger)
	if err != nil {
		logger.Error("could not build the kafka producer", zap.Error(err))
		return err
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Warn("could not close the kafka producer", zap.Error(err))
		}
	}()

	kafkaHealth, err := events.NewKafkaHealthChecker(cfg.KafkaBrokers)
	if err != nil {
		logger.Error("could not build the kafka readiness check", zap.Error(err))
		return err
	}
	defer func() {
		if err := kafkaHealth.Close(); err != nil {
			logger.Warn("could not close the kafka health client", zap.Error(err))
		}
	}()

	collaborators := service.Collaborators{
		Access:      community.NewClient(cfg.CommunityServiceURL, cfg.ServiceToken, cfg.CommunityCheckTimeout),
		Rooms:       repos.Rooms,
		Seats:       repos.Seats,
		Presence:    repos.Presence,
		Projections: repos.Projections,
		Dedupe:      repos.Dedupe,
		LiveKit:     service.NewInstrumentedRoomClient(rooms, metrics),
		Tokens:      livekit.NewSDKTokenMinter(cfg.LiveKitAPIKey, cfg.LiveKitAPISecret),
		Producer:    producer,
		Metrics:     metrics,
		Log:         logger,
		LiveKitURL:  cfg.LiveKitURL,
		SeatTTL:     cfg.SeatReservationTTL,
		TokenTTL:    cfg.ParticipantTokenTTL,
	}

	consumer, err := events.NewKafkaConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaConsumerGroup,
		events.TopicCommunityEvents,
		service.NewCommunityEventConsumer(collaborators),
		logger,
		metrics.SetKafkaLag,
	)
	if err != nil {
		logger.Error("could not build the community_events consumer", zap.Error(err))
		return err
	}

	reconciler := service.NewReconciler(collaborators, cfg.ReconcileInterval, cfg.EventRetention)

	// Readiness covers the critical dependencies (PostgreSQL, Kafka) and fails
	// as soon as shutdown starts, so the load balancer drains this instance.
	readiness := service.NewReadiness([]service.Probe{
		service.NewProbe("postgres", func(ctx context.Context) error {
			return repository.Ping(ctx, pool)
		}),
		service.NewProbe("kafka", kafkaHealth.Check),
	}, probeTimeout)

	router := handler.New(handler.Deps{
		Join:      service.NewJoinService(collaborators),
		Leave:     service.NewLeaveService(collaborators),
		Presence:  service.NewPresenceService(collaborators),
		Webhook:   service.NewWebhookProcessor(collaborators),
		Verifier:  livekit.NewWebhookVerifier(cfg.LiveKitAPIKey, cfg.LiveKitWebhookSecret),
		Tokens:    tokens,
		Readiness: readiness,
		Metrics:   metrics,
		Gatherer:  registry,
		Log:       logger,
	}).Router()

	srv := &http.Server{
		Addr:              net.JoinHostPort("", cfg.Port),
		Handler:           router,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
	}

	// Bind before starting the workers so a port conflict fails fast instead
	// of leaving background loops running on a process that cannot serve.
	listener, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		logger.Error("could not bind the http listener", zap.String("addr", srv.Addr), zap.Error(err))
		return err
	}

	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()

	// fatal receives the first unexpected component failure; it is buffered so
	// a stopping component never blocks on an already draining process.
	fatal := make(chan error, 2)
	reportFatal := func(err error) {
		if err == nil {
			return
		}
		select {
		case fatal <- err:
		default:
		}
	}

	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		if err := consumer.Run(workerCtx); err != nil {
			logger.Error("the community_events consumer stopped", zap.Error(err))
			reportFatal(err)
		}
	}()
	go func() {
		defer workers.Done()
		// The sweep honors the shutdown context and never runs concurrently
		// with itself, which keeps replicas safe.
		reconciler.RunPeriodic(workerCtx)
	}()
	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("the http server stopped", zap.Error(err))
			reportFatal(err)
		}
	}()

	logger.Info("voice_service started",
		zap.String("addr", srv.Addr),
		zap.String("livekit_url", cfg.LiveKitURL),
		zap.String("kafka_consumer_group", cfg.KafkaConsumerGroup),
		zap.Duration("reconcile_interval", cfg.ReconcileInterval),
		zap.Duration("shutdown_timeout", cfg.ShutdownTimeout),
	)

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received; draining")
	case err := <-fatal:
		runErr = err
	}

	// 1. Fail readiness immediately so no new traffic is routed here.
	readiness.BeginShutdown()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()

	// 2. Stop accepting requests and drain the in-flight ones.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("the http server did not drain cleanly", zap.Error(err))
	}

	// 3. Pause the consumer and the scheduled sweep, then wait for them within
	// the shutdown budget.
	cancelWorkers()
	drained := make(chan struct{})
	go func() {
		workers.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-shutdownCtx.Done():
		logger.Warn("workers did not stop within the shutdown timeout",
			zap.Duration("shutdown_timeout", cfg.ShutdownTimeout))
	}
	if err := consumer.Close(); err != nil {
		logger.Warn("could not close the kafka consumer", zap.Error(err))
	}

	// 4. The deferred closers release the kafka producer, the kafka health
	// client, and the postgres pool in reverse order before returning.
	logger.Info("voice_service stopped", zap.Bool("fatal", runErr != nil))
	return runErr
}

// newLogger builds the structured JSON logger of the service. LOG_LEVEL is
// validated here so an unknown level fails startup instead of being ignored.
func newLogger(level string) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "json"
	if err := cfg.Level.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid LOG_LEVEL %q: %w", level, err)
	}
	logger, err := cfg.Build()
	if err != nil {
		return nil, fmt.Errorf("build logger: %w", err)
	}
	return logger, nil
}
