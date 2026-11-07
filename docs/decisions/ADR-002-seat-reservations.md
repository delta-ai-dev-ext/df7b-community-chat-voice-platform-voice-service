# ADR-002: Transactional PostgreSQL seat reservations with TTL plus LiveKit max-participants backstop

## Status
Accepted

## Date
2026-07-14

## Context
Voice channels are capped at 1–10 participants. The system runs several stateless
`voice_service` replicas concurrently, so seat accounting cannot live in memory. A client may
request a token and never actually connect (abandoned join), and LiveKit webhooks confirming a
real join may arrive late or be lost. Both over-counting (permanently full channels) and
under-counting (capacity exceeded) must be impossible or self-healing.

## Decision
1. On join, reserve a seat in a PostgreSQL transaction: the count of live reservations for the
   channel must remain `< capacity`; otherwise the join returns `409 voice_channel_full` with
   alternative channels (from the community's channel list provided by API-002).
2. Reservations are short-TTL (e.g. 60 s) and are released when the join completes, is abandoned,
   or the reservation expires — abandoned requests automatically free their seat.
3. The LiveKit room is created with `max participants = capacity`, so even if local accounting
   drifts, LiveKit refuses the excess participant.
4. The reconciliation sweep treats expired reservations as absent and re-derives truth from
   LiveKit's live participant list, publishing the compensating presence events.

## Alternatives Considered
- **In-memory counters per replica** — rejected: violates statelessness; replicas would disagree.
- **Reservations without TTL** — rejected: abandoned joins would permanently occupy seats.
- **Relying only on LiveKit max-participants** — rejected: the `409` must be deterministic and
  fast (join p95 < 300 ms) with alternative-channel suggestions, and LiveKit's rejection would
  surface as an opaque provider error without the alternatives payload.

## Consequences
- Capacity is enforced transactionally and is replica-safe; `409` semantics are deterministic.
- A small window exists where a seat is reserved but not yet occupied; the TTL bounds it.
- The reservation table needs a TTL sweep and careful partial-unique indexing (documented in
  `docs/architecture.md` §4).
