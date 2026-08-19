DROP INDEX IF EXISTS idx_events_event_id;
ALTER TABLE events ADD CONSTRAINT uq_events_event_id UNIQUE (event_id);
