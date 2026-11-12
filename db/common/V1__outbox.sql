-- seatwave-saga, PostgreSQL 18: the part every service database shares. Each service database
-- (booking, inventory, payment) runs this file first, then its own files; compose.yaml mounts
-- exactly those files into each container's /docker-entrypoint-initdb.d, which runs them once, in
-- name order, on a new volume.

-- The outbox: the event of every state change, written in the same transaction as the change.
CREATE TABLE outbox (
  outbox_id BIGINT GENERATED ALWAYS AS IDENTITY,
  aggregate_id TEXT NOT NULL,          -- the broker key: booking_id, hold_id or provider key
  aggregate_version BIGINT NOT NULL,   -- consumers apply by version, so relay order is not trusted
  event_type TEXT NOT NULL,
  schema_version INT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ,            -- NULL until the relay has the broker's acknowledgement
  CONSTRAINT outbox_pkey PRIMARY KEY (outbox_id),
  CONSTRAINT one_event_per_version UNIQUE (aggregate_id, aggregate_version)
);

-- The relay's only query reads unpublished rows oldest first; the partial index stays small
-- because published rows leave it, and a nightly job deletes rows published over 7 days ago.
CREATE INDEX outbox_unpublished ON outbox (outbox_id) WHERE published_at IS NULL;

-- One row: the highest fencing token (Module 23) any relay has marked with. A stale relay's mark
-- matches no row and changes nothing.
CREATE TABLE outbox_relay_fence (
  only_row BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (only_row),
  highest_token BIGINT NOT NULL
);
INSERT INTO outbox_relay_fence (highest_token) VALUES (0);

-- Every state change has its event: a deferred trigger, checked at COMMIT, refuses a transaction
-- that changed an aggregate's version without writing the outbox row for that version. Each
-- service's own file attaches it to its tables, which name their key in a column aggregate_key.
CREATE FUNCTION require_outbox_row() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1 FROM outbox WHERE aggregate_id = NEW.aggregate_key AND aggregate_version = NEW.version;
  IF NOT FOUND THEN
    RAISE EXCEPTION '% version % changed without its outbox row', NEW.aggregate_key, NEW.version
      USING ERRCODE = 'integrity_constraint_violation';
  END IF;
  RETURN NULL;
END;
$$;
