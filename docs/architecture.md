# Voice Service — Architecture

Clean service-level specification for `voice_service`: responsibilities, flows, inter-service
contracts, database schema, operational model, and observability. Source of truth for the
Diagram Agent. Library and tooling choices live in `docs/engineering_guide.md`.

## 1. Service Responsibilities

`voice_service` owns everything about *being in a voice channel*:

- **Join authorization** — every join is authorized against `community_service` (API-002: active
  membership, channel exists and is a voice channel, mute state, community channel list). The
  check fails closed: if it cannot be performed, the join is refused with `503`.
- **Seat capacity** — capacity 1–10 per voice channel, enforced transactionally in PostgreSQL via
  short-lived reservations and backed by the LiveKit room's max-participant limit. A full call
  returns `409 voice_channel_full` plus the community's other voice channels with free seats.
- **Token minting** — on successful join, mints a short-lived LiveKit participant token (signed
  with the LiveKit API key/secret, never exposed to clients): join room, subscribe, publish
  microphone/camera/screen. Publishing grants are removed when the member is muted
  (listen-only). Audio-only participation is always permitted; blocked-device fallback is a
  client concern.
- **Leave & presence** — removes participants on explicit leave; tracks presence ("who is here",
  camera/screen-share flags) from LiveKit webhooks, deduplicated by webhook event id.
- **Membership reactions (EVT-002)** — kicked/banned members are removed from any active LiveKit
  room in the community; muted members have publishing revoked in place; unmuted members regain
  publishing at their next join (existing listen-only sessions are not silently upgraded).
- **Channel reactions (EVT-003)** — deleting a voice channel closes its LiveKit room; capacity
  changes update the stored room capacity.
- **Reconciliation** — periodically diffs recorded presence against LiveKit's participant list
  and heals drift from missed webhooks.

**Out of scope:** media transport (browsers connect directly to LiveKit Cloud; media never flows
through this service), channel/community management, sanction decisions, device-permission UX,
calls with more than 10 participants.

## 2. Internal Components

| Component | Responsibility |
|---|---|
| HTTP handlers | Public endpoints (`/voice/*`, `/communities/*/voice-presence`), webhook ingestion, probes. Thin: validate, delegate, map errors. |
| Access-token verifier | Verifies identity access tokens locally (JWT, asymmetric signature) with the public verification keys distributed via deployment configuration (current + next key for rotation). Claims: `sub`, `sid`, `email_verified`, `exp`, `iat`, `iss`, `aud`. |
| Join orchestrator | Core transaction: API-002 check → seat reservation → LiveKit room creation (max participants) → token minting → presence record. |
| Seat manager | Transactional seat accounting with short-TTL reservations; computes full-channel alternatives from the local channel projection. |
| Room manager | Outbound LiveKit operations: create room, remove participant, update permissions (revoke publish), list participants (reconciliation), delete room. |
| Webhook processor | Verifies signatures (API secret + body hash), dedupes by webhook event id, maps events to presence state and EVT-010. |
| Event consumers | Consume `community_events` (EVT-002/003); maintain projections; trigger removals and room closures. |
| Event producer | Publishes `voice_events` (EVT-010) with the mandatory envelope. |
| Reconciliation worker | Scheduled job: diff local presence vs LiveKit participant list; heal drift. |
| Repositories | All PostgreSQL access (rooms, reservations, presence, projections, dedupe). |

## 3. Request & Event Flows

### 3.1 Join a voice channel (happy path)

```
Browser        voice_service                 community_service     LiveKit Cloud
  │ POST /voice/channels/{id}/join           │                       │
  │───────────────►│                         │                       │
  │                │ voice-access-check ────►│ (API-002, 2s timeout) │
  │                │◄ allowed, muted_until,
  │                │  voice_channels
  │                │ reserve seat (transactional, TTL)
  │                │ create room (max participants) ────────────────►│
  │                │ mint participant token
  │◄─ 200 livekit_url, token, room_name, publish flags
  │────────── connect directly (WebRTC) ──────────────────────────►│
  │                │◄══ webhook participant_joined (signed)
  │                │ presence upsert + EVT-010 `joined`
```

Failure paths:
- Access check denied → `403 not_a_member` or `404` (`channel_not_found` / `not_a_voice_channel`);
  no seat, no token.
- Access check unavailable → `503 voice_authorization_unavailable` (fail closed; one retry on
  `5xx`/timeout inside the 2 s window).
- Seats exhausted → `409 voice_channel_full` + alternatives from the API-002 channel list.

### 3.2 Leave

`POST /voice/channels/{id}/leave` → remove participant from the room → mark presence left → free
seat → publish EVT-010 `left`. Idempotent when the user is not present.

### 3.3 Webhook ingestion (presence)

`POST /voice/livekit/webhooks` — verify signed Authorization header and body hash → dedupe on
webhook event id → apply mapped change (`participant_joined`, `participant_left`,
`track_published`/`track_unpublished` → camera/screen flags, `room_finished` → close room) →
publish EVT-010. Webhook-triggered flows mint a `trace_id` at the edge (no originating HTTP user
request). Malformed/unsigned requests are rejected `400`/`401` with no state changes.

### 3.4 Moderation reactions (EVT-002 consumption)

```
EVT-002 ─► dedupe on envelope.event_id ─► apply only if member_version is newer
  ├─ removed | banned → for every active room of (community, user): remove participant
  │                     + mark left + EVT-010 `left`   {action=removed}
  ├─ muted            → revoke publish permission in place {action=publish_revoked}
  └─ unmuted          → projection update only; publishing returns at next join
```

### 3.5 Room lifecycle (EVT-003 consumption)

`deleted` (voice channel) → close room, disconnect participants, mark presence left, EVT-010 per
participant. `updated` → update stored capacity/name. `created` → register the room-capacity
mapping lazily on first join.

### 3.6 Reconciliation

Scheduled: list LiveKit participants per active room → diff against local presence → apply
additions/removals through the same presence + EVT-010 paths as webhooks. LiveKit unavailable →
retry with back-off; never mutate local state on failure.

## Inter-Service Contracts

This service's slice of the project-level `API_CONTRACTS.md` and `EVENT_CONTRACTS.md`, copied
verbatim. Registry edges where `voice_service` is `From` or `To`:

| Edge ID | From | To | Kind | Contract |
|---|---|---|---|---|
| DEP-002 | `voice_service` | `community_service` | sync | `API-002` |
| DEP-013 | `voice_service` | `realtime_gateway` | async | `EVT-010` |
| DEP-005 | `community_service` | `messaging_service` | async | `EVT-002` (fan-out includes `voice_service`) |
| DEP-006 | `community_service` | `realtime_gateway` | async | `EVT-003` (fan-out includes `voice_service`) |

### API-002 — Voice channel access check (voice_service is the caller)

Provider: `community_service`. Operation:
`POST /internal/communities/{community_id}/channels/{channel_id}/voice-access-checks`.
Auth: per-edge service token, `Authorization: Bearer`. Caller uses a 2 s timeout with at most one
retry on `5xx`/timeout (read-only, safe to retry). Internal endpoints are not exposed through the
public ingress.

**Request** (`application/json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["user_id"],
  "properties": {
    "user_id": {"type": "string", "format": "uuid"}
  }
}
```

**Response 200** (`application/json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["community_id", "user_id", "allowed", "denial_reason", "muted_until", "voice_channels"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "user_id": {"type": "string", "format": "uuid"},
    "allowed": {"type": "boolean", "description": "True when the user is an active, non-banned member and the channel is a voice channel. Mute does not deny access; it restricts publishing."},
    "denial_reason": {
      "type": ["string", "null"],
      "enum": ["not_a_member", "banned", "channel_not_found", "not_a_voice_channel", null]
    },
    "channel": {
      "$ref": "shared:VoiceChannelSummary",
      "description": "The requested voice channel; omitted when denial_reason is channel_not_found or not_a_voice_channel."
    },
    "muted_until": {
      "type": ["string", "null"],
      "format": "date-time",
      "description": "Non-null and in the future when the member is currently muted (listen-only)."
    },
    "voice_channels": {
      "type": "array",
      "description": "All voice channels of the community (including the requested one); empty when allowed is false.",
      "items": {"$ref": "shared:VoiceChannelSummary"}
    }
  }
}
```

**Errors** — `400` (malformed body), `401` (missing or invalid service token), `404` (`community_not_found`), `500`, `503`, all `application/problem+json` referencing `shared:ErrorResponse`. A denied access returns `200` with `allowed: false`.

**Shared schema referenced above** (`shared:VoiceChannelSummary`):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["channel_id", "name", "capacity"],
  "properties": {
    "channel_id": {"type": "string", "format": "uuid"},
    "name": {"type": "string", "minLength": 1, "maxLength": 100},
    "capacity": {"type": "integer", "minimum": 1, "maximum": 10}
  }
}
```

### Event envelope (mandatory, every produced and consumed message)

Every message on every topic is a JSON object with exactly two top-level keys: `envelope` and `payload`.

| Field | Type | Required | Rule |
|---|---|---|---|
| `event_id` | string (uuid) | yes | Unique per event. Consumer deduplication key. |
| `event_type` | string (snake_case) | yes | Must match a registered contract. |
| `event_version` | integer | yes | >= 1. A breaking payload change bumps it. |
| `occurred_at` | string (RFC3339 UTC) | yes | Producer clock at emit time. |
| `producer` | string (snake_case) | yes | Must equal the registered producer. |
| `trace_id` | string (32 lowercase hex) | yes | **Never empty.** Propagated from the originating request; minted at the edge when absent. |
| `correlation_id` | string (uuid) | yes | Groups every event of one business flow. |
| `causation_id` | string (uuid) or null | yes | `event_id` of the causing event. `null` only for the flow's first event. |
| `schema_ref` | string | yes | `EVENT_CONTRACTS.md#<event_type>-v<version>`. |

Rules:
- The envelope is not optional and not per-topic. A topic needing a field the envelope lacks adds it to `payload`, never to the envelope.
- A payload schema MUST NOT declare an envelope field name at its top level.
- `trace_id` is mandatory even when no HTTP request originated the flow: the producer mints one at the edge of its own flow (LiveKit-webhook-triggered presence changes, scheduled reconciliation).
- When this service emits an event because of another event, it copies `trace_id` and `correlation_id` from the causing event and sets `causation_id` to the causing `event_id`.
- The envelope is also mirrored into Kafka message headers (`trace_id`, `event_type`). The JSON envelope remains the source of truth.
- Delivery is at-least-once. Consumers dedupe on `envelope.event_id` and apply changes idempotently; projections ignore stale entity versions.

Envelope schema (the `envelope` object):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["event_id", "event_type", "event_version", "occurred_at", "producer", "trace_id", "correlation_id", "causation_id", "schema_ref"],
  "properties": {
    "event_id": {"type": "string", "format": "uuid"},
    "event_type": {"type": "string", "pattern": "^[a-z][a-z0-9_]*$"},
    "event_version": {"type": "integer", "minimum": 1},
    "occurred_at": {"type": "string", "format": "date-time"},
    "producer": {"type": "string", "pattern": "^[a-z][a-z0-9_]*$"},
    "trace_id": {"type": "string", "pattern": "^[0-9a-f]{32}$"},
    "correlation_id": {"type": "string", "format": "uuid"},
    "causation_id": {"type": ["string", "null"], "format": "uuid"},
    "schema_ref": {"type": "string", "pattern": "^EVENT_CONTRACTS\\.md#[a-z][a-z0-9_]*-v[0-9]+$"}
  }
}
```

### EVT-010 — `voice_presence_changed` v1 (voice_service produces)

Topic: `voice_events`. Key: `channel_id`. Consumer: `realtime_gateway`. Delivery: at-least-once;
consumers dedupe on `envelope.event_id`. Emitted when a participant joins or leaves a voice
channel, or toggles camera or screen share. The trigger is a LiveKit webhook, an explicit leave,
a moderation removal, or reconciliation. For webhook-triggered flows, the producer mints the `trace_id`.

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["community_id", "channel_id", "user_id", "change", "camera_enabled", "screen_share_enabled", "publishing_allowed", "participant_count", "capacity"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "channel_id": {"type": "string", "format": "uuid"},
    "user_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["joined", "left", "media_changed"]},
    "camera_enabled": {"type": "boolean"},
    "screen_share_enabled": {"type": "boolean"},
    "publishing_allowed": {"type": "boolean", "description": "False when the member is muted (listen-only)."},
    "participant_count": {"type": "integer", "minimum": 0, "maximum": 10},
    "capacity": {"type": "integer", "minimum": 1, "maximum": 10}
  }
}
```

### EVT-002 — `community_member_changed` v1 (voice_service consumes)

Producer: `community_service`. Topic: `community_events`. Key: `community_id`. Delivery:
at-least-once; consumers dedupe on `envelope.event_id`; apply only if `member_version` is newer.

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["community_id", "user_id", "change", "status", "is_banned", "muted_until", "role_ids", "sanction_id", "member_version"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "user_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["joined", "left", "removed", "banned", "unbanned", "muted", "unmuted", "roles_changed"]},
    "status": {"type": "string", "enum": ["active", "removed"]},
    "is_banned": {"type": "boolean"},
    "muted_until": {"type": ["string", "null"], "format": "date-time"},
    "role_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "sanction_id": {"type": ["string", "null"], "format": "uuid"},
    "member_version": {"type": "integer", "minimum": 1}
  }
}
```

### EVT-003 — `community_channel_changed` v1 (voice_service consumes)

Producer: `community_service`. Topic: `community_events`. Key: `community_id`. Delivery:
at-least-once; consumers dedupe on `envelope.event_id`; apply only if `channel_version` is newer.

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["community_id", "channel_id", "change", "kind", "name", "position", "capacity", "channel_version"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "channel_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["created", "updated", "deleted"]},
    "kind": {"type": "string", "enum": ["text", "voice"]},
    "name": {"type": "string", "minLength": 1, "maxLength": 100},
    "position": {"type": "integer", "minimum": 0},
    "capacity": {"type": ["integer", "null"], "minimum": 1, "maximum": 10, "description": "Voice channels only; null for text channels."},
    "channel_version": {"type": "integer", "minimum": 1}
  }
}
```

## 4. PostgreSQL Database Schema

All tables owned by `voice_service`; timestamps are `timestamptz`; identifiers are `uuid`.
Projections are rebuilt from events and are the only local source of membership/mute/channel
state (no synchronous calls outside the join authorization check).

### `voice_rooms`

| Column | Type | Constraints |
|---|---|---|
| `channel_id` | uuid | PK |
| `community_id` | uuid | NOT NULL, indexed |
| `livekit_room_name` | text | NOT NULL, UNIQUE, ≤ 200 chars |
| `capacity` | integer | NOT NULL, CHECK (1–10) |
| `created_at` | timestamptz | NOT NULL, default now() |
| `updated_at` | timestamptz | NOT NULL |

One row per known voice channel (EVT-003 rows with `kind = text` are ignored). Created lazily on
first join or on EVT-003 `created`/`updated`.

### `seat_reservations`

| Column | Type | Constraints |
|---|---|---|
| `reservation_id` | uuid | PK |
| `community_id` | uuid | NOT NULL |
| `channel_id` | uuid | NOT NULL, FK → `voice_rooms(channel_id)` |
| `user_id` | uuid | NOT NULL |
| `reserved_at` | timestamptz | NOT NULL, default now() |
| `expires_at` | timestamptz | NOT NULL (short TTL, e.g. 60 s) |
| `released_at` | timestamptz | NULL (set when the join completes or is abandoned) |

Indexes: `(channel_id, expires_at)` for capacity accounting and TTL sweeping; partial UNIQUE
`(channel_id, user_id) WHERE released_at IS NULL AND expires_at > now()` — one live reservation
per user per channel. Capacity is enforced inside the reservation transaction: the count of live
reservations for the channel must stay `< capacity` (upsert-guarded/serializable transaction).

### `voice_participants`

| Column | Type | Constraints |
|---|---|---|
| `community_id` | uuid | NOT NULL |
| `channel_id` | uuid | NOT NULL, FK → `voice_rooms(channel_id)` |
| `user_id` | uuid | NOT NULL |
| `session_joined_at` | timestamptz | NOT NULL, default now() |
| `left_at` | timestamptz | NULL |
| `camera_enabled` | boolean | NOT NULL, default false |
| `screen_share_enabled` | boolean | NOT NULL, default false |
| `publishing_allowed` | boolean | NOT NULL |
| `last_seen_at` | timestamptz | NOT NULL |

PK `(community_id, channel_id, user_id, session_joined_at)` (history preserved). Active presence
is `left_at IS NULL`; one active row per user per channel (partial UNIQUE
`(community_id, channel_id, user_id) WHERE left_at IS NULL`). Indexed on
`(community_id, left_at)` for the presence endpoint.

### `member_projections` (mute/membership, from EVT-002)

| Column | Type | Constraints |
|---|---|---|
| `community_id` | uuid | PK part 1 |
| `user_id` | uuid | PK part 2 |
| `status` | text | NOT NULL, CHECK (`active` / `removed`) |
| `is_banned` | boolean | NOT NULL |
| `muted_until` | timestamptz | NULL |
| `member_version` | bigint | NOT NULL, CHECK (> 0) |
| `updated_at` | timestamptz | NOT NULL |

Upsert only when incoming `member_version` is newer (stale versions ignored). Publishing grants
are derived from `muted_until` at join time; in-place publish revocation on `muted` is issued to
LiveKit directly.

### `channel_projections` (from EVT-003)

| Column | Type | Constraints |
|---|---|---|
| `community_id` | uuid | PK part 1 |
| `channel_id` | uuid | PK part 2 |
| `kind` | text | NOT NULL, CHECK (`text` / `voice`) |
| `name` | text | NOT NULL, ≤ 100 chars |
| `capacity` | integer | NULL, CHECK (1–10); voice only |
| `deleted` | boolean | NOT NULL, default false |
| `channel_version` | bigint | NOT NULL, CHECK (> 0) |
| `updated_at` | timestamptz | NOT NULL |

Drives `404` for unknown/non-voice channels and the capacity shown in the presence endpoint.

### `processed_events` (Kafka dedupe)

| Column | Type | Constraints |
|---|---|---|
| `event_id` | uuid | PK |
| `topic` | text | NOT NULL |
| `processed_at` | timestamptz | NOT NULL, default now() |

Rows older than a bounded retention window are purged by the reconciliation sweep.

### `processed_webhook_events` (LiveKit dedupe)

| Column | Type | Constraints |
|---|---|---|
| `webhook_event_id` | text | PK |
| `event_kind` | text | NOT NULL |
| `received_at` | timestamptz | NOT NULL, default now() |

### Relationships

`seat_reservations.channel_id` → `voice_rooms.channel_id`; `voice_participants` and
`seat_reservations` reference `voice_rooms` by FK. `channel_projections` links to `voice_rooms`
by `channel_id` (logical). All state transitions are idempotent (at-least-once events and
duplicated webhooks).

## 5. Operational Model (Stateless Containers, Failure Modes & Resilience)

- **Statelessness:** all state lives in PostgreSQL; LiveKit room/participant state lives in
  LiveKit Cloud. Multiple replicas are safe: capacity is enforced transactionally, webhook
  processing dedupes by webhook event id, event consumption dedupes by `envelope.event_id`.
- **Probes:** `GET /healthz` (liveness — process responsive; never fails on dependency hiccups);
  `GET /readyz` (readiness — PostgreSQL pool and Kafka connectivity verified; `503` once
  shutdown starts).
- **Graceful shutdown:** trap `SIGTERM`/`SIGINT` → mark readiness failing → stop accepting HTTP
  requests and pause Kafka consumer loops → drain in-flight requests/workers within a bounded
  timeout → close the DB pool and Kafka producer/clients cleanly → exit `0`.
- **LiveKit unavailability:** join failure (room creation/token minting) → seat reservation
  rolled back, `503` to the client (retry safe — reservations are TTL-bound and released on
  abandonment). Moderation removals and permission updates retry with bounded exponential
  back-off; residual drift is compensated by the reconciliation sweep. Reconciliation itself
  retries with back-off and never mutates local state on failure.
- **Seat reservation TTL:** abandoned reservations (client never connected) expire automatically,
  freeing the seat; expired reservations are treated as absent by capacity accounting.
- **Idempotency & dedupe:** duplicate webhooks (`processed_webhook_events`), re-delivered events
  (`processed_events`), stale versions ignored via `member_version`/`channel_version`; leave is
  idempotent.
- **Fail-closed authorization:** API-002 unreachable → `503`; no join without a successful check.
- **Key rotation:** two configured identity public verification keys (current + next) are
  accepted during rotation; LiveKit API key/secret and webhook secret are deployment secrets,
  never logged.

## 6. Observability

**Business metrics**

| Metric | Type | Labels |
|---|---|---|
| `voice_joins_total` | counter | `result` = `joined\|full\|forbidden` |
| `voice_participant_minutes_total` | counter | `media` = `audio\|video\|screen` |
| `voice_active_participants` | gauge | — |
| `voice_active_rooms` | gauge | — |
| `voice_moderation_removals_total` | counter | `action` = `removed\|publish_revoked` |

**Technical metrics** (platform-wide; low-cardinality labels only — never user, community,
message, or request ids):

- `http_requests_total{route,method,status_class}`, `http_request_duration_seconds{route,method}`
- `kafka_consumer_lag{topic,consumer_group}` (topic: `community_events`)
- `external_call_duration_seconds{provider="livekit",operation}` and
  `external_call_errors_total{provider="livekit",operation,error_class}` — operations:
  `create_room`, `remove_participant`, `update_participant_permissions`, `list_participants`,
  `delete_room`
- SLOs: join p95 < 300 ms (including API-002 and token minting); webhook processing p95 < 100 ms;
  presence event lag p95 < 1 s; LiveKit API error rate < 0.5 %.

**Tracing & logging:** `traceparent` / `X-Trace-Id` propagated on every HTTP call (including the
outbound API-002 call). Every produced event carries a non-empty `trace_id` in its envelope —
propagated from the originating HTTP request when one exists, minted at the edge of
webhook-triggered and scheduled flows otherwise; `correlation_id`/`causation_id` follow the
envelope rules. Logs are structured JSON with `trace_id`, correlation ids, operation, and status;
secrets (tokens, API keys) are never logged.
