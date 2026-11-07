# Voice Service — User Stories

Derived from the platform system design (`SYSTEM_DESIGN.md` §2.6) and this service's contract slice
(API-002 as consumer, EVT-010 as producer, EVT-002/EVT-003 as consumer). Roles: **member** (any signed-in,
email-verified platform user), **muted member** (member with an active community mute), **moderator/owner**
(indirectly, via moderation actions that reach this service as events), **LiveKit** (external real-time
media provider), **system** (scheduled reconciliation).

---

## US-001: Join a voice channel

**As a** member of a community,
**I want to** join one of its voice channels,
**So that** I can talk and video call with other members in real time.

### Acceptance Criteria
- Given I am an active member of the community and the channel exists and is a voice channel, when I request to join, then I receive the real-time service URL, a room name matching that channel, and a short-lived participant token that grants me to join the room, subscribe to others, and publish my microphone, camera, and screen share.
- Given my token request succeeds, when I open the media connection, then I connect directly to the managed real-time media cloud — no media flows through this backend.
- Given the channel is not found or is not a voice channel, when I request to join, then I receive a `404` with an error code and no token is issued.
- Given the real-time authorization service is temporarily unavailable, when I request to join, then I receive a `503` and no seat is reserved and no token is issued (the check fails closed).

### Out of Scope
- Device permission handling (blocked microphone/camera) and audio-only fallback — these are client concerns; the token always permits audio-only participation.
- Calls with more than 10 participants.

---

## US-002: Full channel offers alternatives

**As a** member of a community,
**I want to** be told which other voice channels have free seats when a call is full,
**So that** I can hop into an alternative without hunting through the channel list.

### Acceptance Criteria
- Given the voice channel already has participants equal to its capacity, when I request to join, then I receive a `409 voice_channel_full` response listing the other voice channels of the community that still have free seats (channel id, name, capacity).
- Given the channel is full and it is the community's only voice channel, when I request to join, then the alternatives list in the response is empty.
- Given a full channel, when I request to join, then no seat is taken and the presence of existing participants is unchanged.

### Out of Scope
- Automatically moving me to another channel; the client decides based on the alternatives list.

---

## US-003: Muted member joins listen-only

**As a** muted member of a community,
**I want to** still join voice channels and listen,
**So that** I remain part of the conversation while sanctioned.

### Acceptance Criteria
- Given I have an active mute in the community (mute end time in the future), when I request to join a voice channel, then I succeed and my token grants joining and subscribing but **no** publishing of microphone, camera, or screen share (listen-only).
- Given I am muted, when I request to join, then the response tells me I am listen-only and until when.
- Given my mute expires while I am in a room, when I next join any voice channel, then my token grants publishing again (regained on next join).

### Out of Scope
- Regaining publishing mid-session without re-joining.

---

## US-004: Non-members cannot join

**As a** platform user,
**I want to** be refused when I try to join a voice channel of a community I do not (or no longer) belong to,
**So that** community boundaries are respected.

### Acceptance Criteria
- Given I am not an active member of the community, when I request to join, then I receive a `403 not_a_member` and no token is issued.
- Given I was kicked or banned from the community, when I request to join, then I receive a `403 not_a_member`.
- Given I am signed out or my access token is invalid or expired, when I call any voice endpoint, then I receive a `401`.

### Out of Scope
- Identity issuance and token refresh (owned by the identity service; verified locally here).

---

## US-005: Leave a voice channel at any time

**As a** member in a voice call,
**I want to** leave at any time,
**So that** my seat frees up for someone else.

### Acceptance Criteria
- Given I am present in a voice channel, when I leave, then I am removed from the live room, my seat is freed, and my presence disappears for other members.
- Given I leave a channel I am not currently in, when I call leave, then the request still succeeds (idempotent) without side effects.
- Given I leave, when other members view presence, then the participant count has decreased by one.

### Out of Scope
- Session-wide "leave all channels" behaviour.

---

## US-006: See who is in each voice channel

**As a** member of a community,
**I want to** see who is currently present in each voice channel, with camera and screen-share indicators,
**So that** I can decide whether to hop in.

### Acceptance Criteria
- Given I am an active member of the community, when I fetch its voice presence, then I receive one entry per voice channel with the participants (user id, joined-at, camera flag, screen-share flag) and seats used out of capacity.
- Given a channel with nobody present, when I fetch presence, then its entry shows zero participants.
- Given participants toggled camera or screen share, when I fetch presence, then the flags reflect their current media state.

### Out of Scope
- Rendering or notification of presence (the realtime gateway pushes live updates; this endpoint is the snapshot/initial load).

---

## US-007: Live presence updates for the community

**As a** member of a community,
**I want to** presence changes in voice channels to be published as events,
**So that** my open app session updates the channel lists in real time without polling.

### Acceptance Criteria
- Given a participant joins, leaves, or toggles camera/screen share, when the change is recorded, then a `voice_presence_changed` event carrying community, channel, user, change kind, media flags, publishing permission, current participant count, and capacity is published.
- Given a webhook-triggered change, when the event is published, then it carries a trace id minted at the edge of the webhook flow (non-empty).
- Given the same change is delivered twice, when the gateway consumes it, then it deduplicates by event id (at-least-once delivery is safe).

### Out of Scope
- Pushing events to browsers (owned by the realtime gateway).

---

## US-008: Removed or banned members are removed from live calls

**As a** community moderator (via the moderation flow),
**I want to** kicked and banned members to be removed from any live voice room and muted members to lose their publishing rights,
**So that** sanctions apply immediately, not just at next join.

### Acceptance Criteria
- Given a member is kicked or banned (membership-removed event received), when they are present in any voice room of that community, then they are removed from the room and can no longer reconnect with their old token.
- Given a member is muted while in a voice room, when the mute event is received, then their publishing permission is revoked in place (they can keep listening).
- Given a member is unmuted while in a voice room, then their existing listen-only session is not silently upgraded; publishing returns on their next join.
- Given a removal event is re-delivered (at-least-once), when processed again, then it is a no-op.

### Out of Scope
- Deciding *who* to sanction (owned by the moderation service via the community service).

---

## US-009: Deleting a voice channel closes its live room

**As a** community moderator (via the community flow),
**I want to** the live room of a deleted voice channel to be closed,
**So that** nobody lingers in a room whose channel no longer exists.

### Acceptance Criteria
- Given a voice channel is deleted (channel-changed event received), when the room is live, then the room is closed and all its participants are disconnected.
- Given the room for a deleted channel is already gone, when the deletion event is processed, then it is a no-op.
- Given a channel's capacity is changed while a room is live, then the stored room capacity is updated to the new value.

### Out of Scope
- Creating or renaming channels (owned by the community service).

---

## US-010: Presence heals from missed webhooks

**As a** system operator,
**I want to** periodically reconcile recorded presence against the media provider's live participant list,
**So that** missed or lost webhook deliveries cannot leave stale participants occupying seats forever.

### Acceptance Criteria
- Given the reconciliation job runs, when it finds recorded participants that the provider no longer reports, then those participants are marked as left, their seats are freed, and a presence event is published.
- Given the job finds participants present in the provider but missing locally, then they are recorded as joined and a presence event is published.
- Given the provider is unreachable, when the job runs, then it retries with back-off and does not corrupt local presence state.

### Out of Scope
- LiveKit webhook delivery guarantees themselves (mitigated by reconciliation + at-least-once events).

---

## US-011: Only authentic webhooks are processed

**As a** system,
**I want to** process only webhooks verifiably signed by the media provider,
**So that** an attacker cannot forge join/leave activity or exhaust seats.

### Acceptance Criteria
- Given a webhook with a valid signed Authorization header and matching body hash, when it is delivered, then it is processed idempotently by its webhook event id and returns `200`.
- Given a webhook with a missing, invalid, or unverifiable signature, when it is delivered, then it is rejected with `401` and no state changes.
- Given the same webhook is delivered twice, when processed, then the duplicate is ignored and `200` is still returned.

### Out of Scope
- Signing configuration and key rotation of the provider account.

---

## US-012: Health probes

**As a** deployment platform,
**I want to** liveness and readiness probes,
**So that** traffic is only routed to instances that can do their job.

### Acceptance Criteria
- Given the process is running, when the liveness probe is called, then it returns `200` unless the process itself is unrecoverable (it does not fail on transient dependency hiccups).
- Given PostgreSQL and Kafka are reachable, when the readiness probe is called, then it returns `200`.
- Given a critical dependency (PostgreSQL or Kafka) is unreachable, or shutdown has started, when the readiness probe is called, then it returns `503`.

### Out of Scope
- Probing the media provider's cloud availability (join failures surface to users instead; the provider is not a startup blocker).
