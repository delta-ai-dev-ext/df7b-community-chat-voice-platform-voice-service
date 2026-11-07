-- voice_service initial schema (docs/architecture.md §4).
--
-- Note on the seat reservation index: PostgreSQL requires index predicates to
-- be IMMUTABLE, so a partial index cannot carry ``expires_at > now()``. The
-- unique index therefore only covers live (unreleased) reservations and every
-- capacity query additionally filters ``expires_at > now()``. Expired
-- reservations are swept inside the reservation transaction, which keeps the
-- documented guarantee (one live reservation per user per channel, expired
-- reservations treated as absent).

CREATE TABLE IF NOT EXISTS voice_rooms (
    channel_id        uuid PRIMARY KEY,
    community_id      uuid NOT NULL,
    livekit_room_name text NOT NULL UNIQUE,
    capacity          integer NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL,
    CONSTRAINT voice_rooms_capacity_range CHECK (capacity BETWEEN 1 AND 10),
    CONSTRAINT voice_rooms_room_name_length CHECK (char_length(livekit_room_name) <= 200)
);

CREATE INDEX IF NOT EXISTS voice_rooms_community_id_idx ON voice_rooms (community_id);

CREATE TABLE IF NOT EXISTS seat_reservations (
    reservation_id uuid PRIMARY KEY,
    community_id   uuid NOT NULL,
    channel_id     uuid NOT NULL REFERENCES voice_rooms (channel_id),
    user_id        uuid NOT NULL,
    reserved_at    timestamptz NOT NULL DEFAULT now(),
    expires_at     timestamptz NOT NULL,
    released_at    timestamptz
);

CREATE INDEX IF NOT EXISTS seat_reservations_channel_expiry_idx
    ON seat_reservations (channel_id, expires_at);

CREATE UNIQUE INDEX IF NOT EXISTS seat_reservations_live_seat_idx
    ON seat_reservations (channel_id, user_id)
    WHERE released_at IS NULL;

CREATE TABLE IF NOT EXISTS voice_participants (
    community_id         uuid NOT NULL,
    channel_id           uuid NOT NULL REFERENCES voice_rooms (channel_id),
    user_id              uuid NOT NULL,
    session_joined_at    timestamptz NOT NULL DEFAULT now(),
    left_at              timestamptz,
    camera_enabled       boolean NOT NULL DEFAULT false,
    screen_share_enabled boolean NOT NULL DEFAULT false,
    publishing_allowed   boolean NOT NULL,
    last_seen_at         timestamptz NOT NULL,
    PRIMARY KEY (community_id, channel_id, user_id, session_joined_at)
);

CREATE UNIQUE INDEX IF NOT EXISTS voice_participants_active_session_idx
    ON voice_participants (community_id, channel_id, user_id)
    WHERE left_at IS NULL;

CREATE INDEX IF NOT EXISTS voice_participants_community_presence_idx
    ON voice_participants (community_id, left_at);

CREATE TABLE IF NOT EXISTS member_projections (
    community_id   uuid NOT NULL,
    user_id        uuid NOT NULL,
    status         text NOT NULL,
    is_banned      boolean NOT NULL,
    muted_until    timestamptz,
    member_version bigint NOT NULL,
    updated_at     timestamptz NOT NULL,
    PRIMARY KEY (community_id, user_id),
    CONSTRAINT member_projections_status_allowed CHECK (status IN ('active', 'removed')),
    CONSTRAINT member_projections_version_positive CHECK (member_version > 0)
);

CREATE TABLE IF NOT EXISTS channel_projections (
    community_id    uuid NOT NULL,
    channel_id      uuid NOT NULL,
    kind            text NOT NULL,
    name            text NOT NULL,
    capacity        integer,
    deleted         boolean NOT NULL DEFAULT false,
    channel_version bigint NOT NULL,
    updated_at      timestamptz NOT NULL,
    PRIMARY KEY (community_id, channel_id),
    CONSTRAINT channel_projections_kind_allowed CHECK (kind IN ('text', 'voice')),
    CONSTRAINT channel_projections_name_length CHECK (char_length(name) <= 100),
    CONSTRAINT channel_projections_capacity_range CHECK (capacity IS NULL OR capacity BETWEEN 1 AND 10),
    CONSTRAINT channel_projections_version_positive CHECK (channel_version > 0)
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     uuid PRIMARY KEY,
    topic        text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS processed_webhook_events (
    webhook_event_id text PRIMARY KEY,
    event_kind       text NOT NULL,
    received_at      timestamptz NOT NULL DEFAULT now()
);
