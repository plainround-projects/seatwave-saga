-- seatwave-saga, payment database: one row per provider key (Module 7's seatwave-hold-<holdId>).
-- Runs after db/common/V1__outbox.sql.
CREATE TABLE payments (
  provider_key TEXT NOT NULL,
  state TEXT NOT NULL,
  amount NUMERIC(12, 2) NOT NULL,
  currency CHAR(3) NOT NULL,
  version BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  aggregate_key TEXT GENERATED ALWAYS AS (provider_key) STORED,
  CONSTRAINT payments_pkey PRIMARY KEY (provider_key),
  CONSTRAINT known_payment_state CHECK (state IN ('CAPTURED', 'DECLINED', 'REFUNDED'))
);
CREATE CONSTRAINT TRIGGER payment_change_has_event AFTER INSERT OR UPDATE ON payments
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_outbox_row();
