-- seatwave-saga, inventory database: Module 13's holds gain the saga's semantic lock (CONFIRMING),
-- the saga that holds it, a version for the outbox, and the event rule. Runs after V2.
ALTER TABLE holds DROP CONSTRAINT known_hold_state;
ALTER TABLE holds ADD CONSTRAINT known_hold_state
  CHECK (state IN ('ACTIVE', 'CONFIRMING', 'RELEASED', 'EXPIRED', 'CONFIRMED'));
-- A CONFIRMING hold is still open: it has not ended, whatever its expires_at says.
ALTER TABLE holds DROP CONSTRAINT only_active_holds_are_open;
ALTER TABLE holds ADD CONSTRAINT only_open_holds_have_no_end
  CHECK ((state IN ('ACTIVE', 'CONFIRMING')) = (ended_at IS NULL));
ALTER TABLE holds ADD COLUMN saga_id UUID;
ALTER TABLE holds ADD COLUMN version BIGINT NOT NULL DEFAULT 1;
ALTER TABLE holds ADD COLUMN aggregate_key TEXT GENERATED ALWAYS AS (hold_id::TEXT) STORED;
-- A lock always names the saga that holds it, so only that saga can confirm or release it.
ALTER TABLE holds ADD CONSTRAINT confirming_names_its_saga
  CHECK (state <> 'CONFIRMING' OR saga_id IS NOT NULL);
-- Module 13's "one claim per seat" now counts the lock as a claim, so no one can hold a seat that
-- a saga is paying for. The expiry sweep updates only ACTIVE rows.
DROP INDEX one_claim_per_seat;
CREATE UNIQUE INDEX one_claim_per_seat ON holds (event_id, seat_label)
  WHERE state IN ('ACTIVE', 'CONFIRMING', 'CONFIRMED');
-- The expiry sweep's query: only ACTIVE holds, by their end.
CREATE INDEX active_holds_by_expiry ON holds (expires_at) WHERE state = 'ACTIVE';
CREATE CONSTRAINT TRIGGER hold_change_has_event AFTER INSERT OR UPDATE ON holds
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_outbox_row();
