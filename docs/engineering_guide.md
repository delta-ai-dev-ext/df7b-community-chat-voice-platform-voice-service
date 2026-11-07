# Voice Service — Engineering Guide

Persistent technical memory for `voice_service`. Runtime, libraries & SDKs, container lifecycle,
testing strategy, conventions, configuration, observability, and append-only pinned decisions.

## Runtime & Toolchain

- **Language / runtime:** Go (module `voice_service`, kebab-case module name, no VCS-host prefix;
  `go mod init voice_service`). Target the current stable Go toolchain (the language skill pins
  `golang:1.26-alpine` for builds).
- **Package manager:** Go modules (`go.mod` / `go.sum` in repo root).
- **Entry points:**
  - Build: `make build` (docker build) / `CGO_ENABLED=0 go build -o server ./cmd/server`
  - Run: `make run` (`go run ./cmd/server`)
  - Test: `make test` (`go test ./... -race -count=1 -parallel 4`)
  - Lint: `make lint` (`golangci-lint run ./...`)
  - Install: `make install` (`go mod download`) — always run before building/testing.
- **Dockerfile:** multi-stage `golang:1.26-alpine` builder → `distroless/static-debian12`
  runtime, `USER nonroot:nonroot`.

## Libraries & External Provider SDKs

| Concern | Package / Library | Source | Usage Notes |
|---|---|---|---|
| HTTP framework | `github.com/gin-gonic/gin` | language-skill | Router, request binding/validation at the boundary, error mapping. |
| Access-token verification | `github.com/golang-jwt/jwt/v5` | tech-lead-research | Local JWT verification with the identity service's public keys (current + next, from deployment config). Do NOT call identity at request time. Validate `exp`, `iss`, `aud`; expose `sub`, `sid`. |
| PostgreSQL driver | `github.com/jackc/pgx/v5` | language-skill | `pgxpool` with parameterized queries everywhere (no `fmt.Sprintf` SQL). `DATABASE_URL`. |
| Migrations | `github.com/golang-migrate/migrate/v4` | language-skill | Versioned SQL migrations for the schema in `docs/architecture.md` §4. |
| Kafka client | `github.com/IBM/sarama` | language-skill | Consumer group for `community_events` (one shared group); producer for `voice_events`. Set `trace_id`/`event_type` message headers alongside the JSON envelope. |
| LiveKit Cloud SDK | `github.com/livekit/server-sdk-go/v2` + `github.com/livekit/protocol/auth` | tech-lead-research | Official server SDK. Token minting: `auth.NewAccessToken(apiKey, apiSecret)` with `VideoGrant` (room join/subscribe/publish flags, TTL ≤ 5 min) then `ToJWT()`. Room service API: `lksdk.NewLiveKitAPI(WithURL(...), WithAPIKey(...), WithAPISecret(...))` → `api.Room().CreateRoom/ListRooms/DeleteRoom` and participant operations (remove participant, update permissions, list participants). Treat SDK responses as untrusted input at the boundary. Never expose the API key/secret to clients. |
| Webhook verification | LiveKit signed `Authorization` header (via `livekit/protocol` webhook helpers or verified HMAC with the API secret + body hash) | tech-lead-research | Reject unsigned/invalid webhooks with `401` before any state change; dedupe by webhook event id. |
| Logging | `go.uber.org/zap` | language-skill | Structured JSON logging; logger injected, never global. |
| Metrics | `github.com/prometheus/client_golang/prometheus` | language-skill | Business + RED metrics per `docs/architecture.md` §6; low-cardinality labels only. |
| Testing | standard `testing` + `github.com/stretchr/testify` | language-skill | Table-driven tests, `t.Parallel()`; `testify/mock` for interfaces. |
| HTTP mocking | `github.com/jarcoal/httpmock` | language-skill | Stub the community-service API-002 client in tests. |
| Env loading (dev only) | `github.com/joho/godotenv` | language-skill | Local development only; `.env` never committed, never in production. |

Dependency resolution protocol: `Pinned Decisions` (below) > `go.mod`/`go.sum` > this table >
language-skill approved list. Add dependencies with `go get <module>@latest` + `go mod tidy`, then
verify the exact API via `technical_research_agent(command="fetch_library_docs", ...)`.

## Container Lifecycle & Probes

- **Statelessness:** no local disk or in-memory cross-request state. All state in PostgreSQL;
  LiveKit room/participant state lives in LiveKit Cloud. Safe with `replicas > 1`.
- **`GET /healthz` (liveness):** process-responsive check only; never fails on transient
  PostgreSQL/Kafka/LiveKit hiccups.
- **`GET /readyz` (readiness):** verifies PostgreSQL pool and Kafka connectivity; returns `503`
  as soon as shutdown starts so the load balancer drains this instance.
- **Graceful shutdown:** trap `SIGTERM`/`SIGINT` via `signal.NotifyContext`:
  1. Mark readiness failing immediately.
  2. Stop accepting new HTTP requests; pause/commit Kafka consumer loops.
  3. Drain in-flight requests and workers within a bounded timeout (15–30 s).
  4. Close the PostgreSQL pool and Kafka producer/consumer cleanly; exit `0`.
- The reconciliation sweep is a worker in the same process; it honors the shutdown context and
  never runs concurrently with itself (single-flight guard), which keeps replicas safe.

## Testing Strategy

- **Unit tests (majority):** pure logic — seat accounting rules, token grant derivation from
  mute state, envelope construction, webhook signature verification, event mapping, version
  comparison for projections. External dependencies mocked via `testify/mock`; the API-002 HTTP
  client stubbed with `httpmock`. No network calls, no real infrastructure, deterministic,
  `t.Parallel()` everywhere.
- **Integration tests:** handler-level tests with mocked PostgreSQL (in-repo fake
  repositories/implementations of the repository interfaces), mocked Kafka producer/consumer, and
  a fake LiveKit client. They must exercise the same errors the real dependencies produce
  (capacity violation, stale event version, duplicate webhook, LiveKit `5xx`).
- **Mocks live in their own folder** (`tests/mocks/` or `tests/fixtures/`); repository and
  LiveKit-room interfaces are defined in `internal/` so mocks implement them.
- **The join transaction** is the highest-risk path: test it exhaustively — allowed/denied check
  results, full channel (with and without alternatives), reservation TTL expiry, LiveKit failure
  rollback, concurrent join to the last seat (serialized transaction behavior expressed through
  the fake repository).
- **Coverage ≥ 80 %**, focused on the join orchestrator, webhook processor, and event consumers.
- All tests run via `make test`; no test may start containers (no Docker daemon in the agent
  runtime) — containerized dependencies (PostgreSQL, Kafka, LiveKit stub) are for developer/CI
  environments only.

## Coding Conventions & Project Structure

```
voice_service/
├── docs/                      # specs (this folder)
├── cmd/server/main.go         # entrypoint: config, DI, HTTP server, consumers, workers
├── internal/
│   ├── config/config.go       # typed Config from env, validated at startup
│   ├── handler/               # gin handlers (thin): join, leave, presence, webhook, probes
│   ├── service/               # business logic: join orchestrator, seat manager, room manager,
│   │                          #   webhook processor, event consumers/producer, reconciler
│   ├── repository/            # pgx-backed repositories + interfaces
│   ├── livekit/               # LiveKit SDK adapter (token minting + room service client iface)
│   ├── community/             # API-002 client (typed, timeout + single retry)
│   ├── events/                # envelope struct, EVT-010 producer, EVT-002/003 consumers
│   └── models/                # domain types, request/response structs
├── migrations/                # SQL migrations (golang-migrate)
├── tests/{unit,integration,mocks}/
├── Makefile, Dockerfile, go.mod, go.sum
```

- Layering: handler → service → repository/livekit/community adapters. Services never touch
  HTTP/DB/SDK types directly; dependencies are injected as interfaces.
- All exported types and functions have Go doc comments. Errors are returned, never panicked.
- Input validation at the boundary (gin binding + explicit checks); internal code trusts
  validated types. Treat LiveKit webhook bodies and API-002 responses as untrusted input.
- Parameterized SQL only; transactions for seat reservation and projection upserts.
- No `TODO`s, no commented-out code, no hardcoded configuration.

## Configuration & Secrets Management

12-factor env vars, validated at startup (fail fast with a structured log error):

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string (portable). |
| `KAFKA_BROKERS` | Comma-separated bootstrap brokers; `KAFKA_CONSUMER_GROUP=voice_service` default. |
| `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET` | LiveKit Cloud endpoint and server credentials (token minting + room service API). Secret never exposed to clients or logs. |
| `LIVEKIT_WEBHOOK_SECRET` | API secret used to verify signed webhook Authorization headers / body hashes. |
| `IDENTITY_JWT_PUBLIC_KEYS` | Public verification keys of the identity service (current + next, PEM/JWK), for local access-token verification; supports rotation. |
| `SERVICE_TOKEN` | Bearer token this service presents on API-002 calls. |
| `PORT` | HTTP listen port. |
| `SEAT_RESERVATION_TTL` (default 60s), `RECONCILE_INTERVAL`, `SHUTDOWN_TIMEOUT` | Operational tuning. |

`.env` is allowed for local development only and is `.gitignore`d; production reads direct env
vars injected by the container runtime.

## Observability & Logging Conventions

- **Structured JSON logging** (`zap`): fields `service_name`, `level`, `ts`, `msg`, `operation`,
  `status`, `trace_id`, `correlation_id`, `causation_id`, plus low-cardinality context. Never log
  tokens, API keys, or webhook signatures.
- **`trace_id` propagation:** read `traceparent` / `X-Trace-Id` from inbound HTTP requests and
  propagate to the API-002 call. Every produced Kafka event carries a non-empty `trace_id`.
- **Mandatory event envelope:** every message produced or consumed is
  `{"envelope": {...}, "payload": {...}}`. The envelope is modeled as a single reusable struct in
  `internal/events` (never ad-hoc map construction) and carries exactly: `event_id`,
  `event_type`, `event_version`, `occurred_at`, `producer` (`voice_service`), `trace_id`,
  `correlation_id`, `causation_id`, `schema_ref`. `trace_id` is **never empty**: propagated from
  the originating HTTP request, or minted at the edge of webhook-triggered and scheduled
  (reconciliation) flows. When emitting an event caused by a consumed event, copy the causing
  event's `trace_id`/`correlation_id` and set `causation_id` to its `event_id`. A payload MUST
  NOT carry an envelope field name at its top level. The envelope is mirrored into Kafka headers
  (`trace_id`, `event_type`) while the JSON envelope stays the source of truth.
- **Consumers** dedupe on `envelope.event_id` (`processed_events`) and apply state changes
  idempotently, ignoring stale `member_version`/`channel_version`.
- **Metrics registration:** business metrics (`voice_joins_total`, `voice_participant_minutes_total`,
  `voice_active_participants`, `voice_active_rooms`, `voice_moderation_removals_total`), RED
  metrics, `kafka_consumer_lag{topic,consumer_group}`, and
  `external_call_duration_seconds` / `external_call_errors_total` with
  `provider="livekit"` and per-operation labels — all low-cardinality.

## Pinned Decisions

| Dependency / Area | Pin / Constraint | Rationale | Added By | Date |
|---|---|---|---|---|
| LiveKit integration | Official Go server SDK `github.com/livekit/server-sdk-go/v2` (v2 major; do not use v1 paths such as legacy `CreateRoom` client helpers) | SYSTEM_DESIGN requires the official, actively maintained SDK for token minting, Room service API, and webhook verification; v2 is the current major with breaking v1 changes documented by LiveKit. | tech-lead | 2026-07-14 |
| Participant token TTL | Short-lived (≤ 5 min); clients re-join for a fresh token | Matches the platform's short-lived credential model and LiveKit best practice; limits abuse of a leaked token. | tech-lead | 2026-07-14 |
| Capacity enforcement | Transactional PostgreSQL seat reservations (short TTL) + LiveKit room max-participants as backstop | Correctness under concurrent joins across replicas; TTL releases abandoned seats; LiveKit limit protects against drift between reservation and connection. | tech-lead | 2026-07-14 |
| Access-token verification | Local JWT verification with configured identity public keys (current + next); no runtime call to `identity_service` | Platform-wide authentication contract; keeps the join hot path independent of identity availability. | tech-lead | 2026-07-14 |
| Fail-closed authorization | API-002 unreachable/timed out ⇒ `503`, no seat reserved, no token issued | Voice access must be decided in real time; availability target is met by the 2 s timeout + single retry. | tech-lead | 2026-07-14 |
