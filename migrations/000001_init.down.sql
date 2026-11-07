-- Roll back the initial voice_service schema.

DROP TABLE IF EXISTS processed_webhook_events;
DROP TABLE IF EXISTS processed_events;
DROP TABLE IF EXISTS channel_projections;
DROP TABLE IF EXISTS member_projections;
DROP TABLE IF EXISTS voice_participants;
DROP TABLE IF EXISTS seat_reservations;
DROP TABLE IF EXISTS voice_rooms;
