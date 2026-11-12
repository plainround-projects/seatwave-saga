-- seatwave-saga, booking database: the orchestrator's persisted state machine, the bookings it
-- creates, and two tables of other owners kept here so the lab needs three databases, not five:
-- the email consumer's processed_messages (Module 27's notification service) and the read model
-- bookings_by_customer (Module 18's index, now a projection). Runs after db/common/V1__outbox.sql.

CREATE TABLE booking_sagas (
  booking_id UUID NOT NULL,
  hold_id UUID NOT NULL,
  provider_key TEXT NOT NULL,
  state TEXT NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL,     -- the last transition; a retry does not move it
  version BIGINT NOT NULL,
  aggregate_key TEXT GENERATED ALWAYS AS (booking_id::TEXT) STORED,
  CONSTRAINT booking_sagas_pkey PRIMARY KEY (booking_id),
  CONSTRAINT one_saga_per_hold UNIQUE (hold_id),
  CONSTRAINT known_saga_state CHECK (state IN ('SEAT_HELD', 'PAYMENT_REQUESTED',
    'PAYMENT_SUCCEEDED', 'PAYMENT_FAILED', 'REFUND_REQUESTED', 'PAYMENT_REFUNDED', 'COMPLETED',
    'COMPENSATED'))
);
-- The stuck-saga query of Section 10: not final, and older than the 300-second hold.
CREATE INDEX sagas_open_by_age ON booking_sagas (updated_at)
  WHERE state NOT IN ('COMPLETED', 'COMPENSATED');
CREATE CONSTRAINT TRIGGER saga_change_has_event AFTER INSERT OR UPDATE ON booking_sagas
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_outbox_row();

-- A final saga never moves again: the two end states are the only ends.
CREATE FUNCTION refuse_leaving_a_final_state() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.state IN ('COMPLETED', 'COMPENSATED') AND NEW.state <> OLD.state THEN
    RAISE EXCEPTION 'saga % is % and final', OLD.booking_id, OLD.state
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER final_states_are_final BEFORE UPDATE OF state ON booking_sagas
  FOR EACH ROW EXECUTE FUNCTION refuse_leaving_a_final_state();

-- The booking a COMPLETED saga creates, in the same commit as the saga's last transition. Module
-- 13's bookings table moved to the booking service with the split: the hold lives in inventory's
-- database now, so the foreign key to it is gone and one_booking_per_hold is the guard here; the
-- seat's own guard stays in inventory (one_claim_per_seat). price_paid is Module 35's name.
CREATE TABLE bookings (
  booking_id UUID NOT NULL,
  hold_id UUID NOT NULL,
  price_paid NUMERIC(12, 2) NOT NULL,
  currency CHAR(3) NOT NULL,
  confirmed_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT bookings_pkey PRIMARY KEY (booking_id),
  CONSTRAINT one_booking_per_hold UNIQUE (hold_id),
  CONSTRAINT paid_price_not_negative CHECK (price_paid >= 0),
  CONSTRAINT paid_currency_format CHECK (currency ~ '^[A-Z]{3}$')
);

-- Module 27's processed-message claim, unchanged: one effect per key per consumer group.
CREATE TABLE processed_messages (
  consumer_group  VARCHAR(64)  NOT NULL,
  dedupe_key      VARCHAR(100) NOT NULL,
  message_id      VARCHAR(100) NOT NULL,   -- the message that claimed it, for tracing
  status          VARCHAR(8)   NOT NULL CHECK (status IN ('CLAIMED', 'DONE')),
  claimed_by      VARCHAR(64)  NOT NULL,
  claimed_at      TIMESTAMPTZ  NOT NULL,
  done_at         TIMESTAMPTZ,
  CONSTRAINT one_effect_per_key PRIMARY KEY (consumer_group, dedupe_key)
);

-- The read model "my tickets": one row per booking, written only from booking events, by version.
CREATE TABLE bookings_by_customer (
  booking_id TEXT NOT NULL,
  customer_id TEXT NOT NULL,
  seat_label TEXT NOT NULL,
  status TEXT NOT NULL,
  booking_version BIGINT NOT NULL,
  CONSTRAINT bookings_by_customer_pkey PRIMARY KEY (booking_id),
  CONSTRAINT known_booking_status CHECK (status IN ('CONFIRMED', 'CANCELED'))
);
CREATE INDEX bookings_by_customer_customer ON bookings_by_customer (customer_id);
