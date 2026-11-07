# ADR-001: Integrate LiveKit Cloud through the official Go server SDK with webhook-driven presence

## Status
Accepted

## Date
2026-07-14

## Context
`voice_service` must mint participant tokens, create/close rooms, remove participants, revoke
publishing permissions, and reconcile presence against LiveKit Cloud. Media must never flow
through the backend — browsers connect directly to LiveKit. Presence ("who is here", camera and
screen-share flags) must be accurate enough for other members to see live channel occupancy.

## Decision
- Use the **official Go server SDK** (`github.com/livekit/server-sdk-go/v2` with
  `github.com/livekit/protocol/auth`) for token minting and the Room service API (create room with
  max participants, remove participant, update participant permissions, list participants,
  delete room). No custom HTTP wrapper around LiveKit endpoints.
- Track presence from **LiveKit webhooks** (`participant_joined`, `participant_left`,
  `track_published`, `track_unpublished`, `room_finished`) delivered to
  `POST /voice/livekit/webhooks`, verified by the signed Authorization header (API secret) and
  body hash, and processed idempotently by webhook event id.
- A **scheduled reconciliation sweep** (list participants per active room and diff against local
  presence) compensates for missed webhook deliveries; at-least-once Kafka delivery means
  consumers must dedupe regardless.

## Alternatives Considered
- **Polling LiveKit's participant list as the only presence source** — rejected: adds constant
  API load and pushes latency above the presence-lag p95 < 1 s target; kept only as the healing
  mechanism.
- **Backend-relayed media (SFU through our service)** — rejected: violates the platform boundary;
  LiveKit Cloud is the managed media path.
- **Custom REST client against the LiveKit HTTP API** — rejected by engineering standards: the
  official SDK exists and covers every required operation.

## Consequences
- The service depends on LiveKit SDK v2 APIs (breaking changes from v1 are documented by LiveKit;
  do not use v1 helpers).
- API key/secret and webhook secret are deployment secrets, never logged or exposed to clients.
- LiveKit unavailability degrades gracefully: joins fail closed (`503`), moderation removals
  retry with back-off and are reconciled later.
- Presence can be transiently wrong between a missed webhook and the next reconciliation sweep;
  acceptable because participants also self-report through the client.
