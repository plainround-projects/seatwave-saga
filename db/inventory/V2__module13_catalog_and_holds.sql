-- seatwave-saga, inventory database: Module 13's catalog and holds, copied from the project
-- seatwave-transactions at commit d78af3a (its migrations V1 and V2, without the bookings table,
-- which moved to the booking service's database with the split). Runs after db/common/V1__outbox.sql.

-- SeatWave's catalog: where an event happens, what it sells and at what price (Module 13).
-- Rule of this schema: one fact in one place. A seat row says only where a seat is; whether it is
-- held or sold is a fact about a hold or a booking, so it lives in those tables (V2).

CREATE TABLE venues (
  venue_id BIGINT PRIMARY KEY,
  name VARCHAR(120) NOT NULL,
  CONSTRAINT venue_name_not_blank CHECK (btrim(name) <> '')
);

-- An event happens at one venue; a venue hosts many events.
CREATE TABLE events (
  event_id BIGINT PRIMARY KEY,
  venue_id BIGINT NOT NULL,
  name VARCHAR(120) NOT NULL,
  -- An absolute instant, stored in UTC; Module 10 changed Module 1's epoch milliseconds to this.
  starts_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT event_at_a_real_venue FOREIGN KEY (venue_id) REFERENCES venues (venue_id),
  CONSTRAINT event_name_not_blank CHECK (btrim(name) <> '')
);

-- A section belongs to one event, not to the venue: the same arena is set up differently for a
-- concert with a stage than for a match, so each event carries its own plan. The price is a fact
-- about the section, so it is stored here once, never on each seat.
CREATE TABLE sections (
  event_id BIGINT NOT NULL,
  section_code VARCHAR(4) NOT NULL,
  -- Money as an exact decimal, never a floating-point number.
  price NUMERIC(12, 2) NOT NULL,
  currency CHAR(3) NOT NULL,
  CONSTRAINT sections_pkey PRIMARY KEY (event_id, section_code),
  CONSTRAINT section_of_a_real_event FOREIGN KEY (event_id) REFERENCES events (event_id),
  CONSTRAINT section_code_format CHECK (section_code ~ '^[A-Z]{1,4}$'),
  CONSTRAINT price_not_negative CHECK (price >= 0),
  CONSTRAINT currency_format CHECK (currency ~ '^[A-Z]{3}$')
);

-- One row per sellable seat of an event. The key (event_id, seat_label) is Module 1's, kept so
-- every earlier module's queries still name a seat the same way.
CREATE TABLE seats (
  event_id BIGINT NOT NULL,
  seat_label VARCHAR(16) NOT NULL,
  section_code VARCHAR(4) NOT NULL,
  seat_number INT NOT NULL,
  CONSTRAINT seats_pkey PRIMARY KEY (event_id, seat_label),
  CONSTRAINT seat_in_a_real_section FOREIGN KEY (event_id, section_code)
    REFERENCES sections (event_id, section_code),
  CONSTRAINT seat_number_positive CHECK (seat_number > 0),
  -- The label repeats the section and the number (a deliberate copy, Chapter 13.2); this check
  -- makes the copy impossible to get wrong.
  CONSTRAINT seat_label_matches CHECK (seat_label = section_code || '-' || seat_number),
  CONSTRAINT one_seat_per_number UNIQUE (event_id, section_code, seat_number)
);

-- Who holds and who bought (Module 13). The invariant of the whole course lives here:
-- a seat has at most one active hold or booking, and a customer has at most four active holds.

CREATE TABLE customers (
  customer_id BIGINT GENERATED ALWAYS AS IDENTITY,
  -- Module 7's customer string, for example fan_1; the number is the key other tables use.
  customer_ref VARCHAR(64) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT customers_pkey PRIMARY KEY (customer_id),
  CONSTRAINT one_customer_per_ref UNIQUE (customer_ref),
  CONSTRAINT customer_ref_format CHECK (customer_ref ~ '^[A-Za-z0-9_-]{1,64}$')
);

-- Every hold ever granted, kept after it ends: a regulated venue must be able to show who held
-- which seat and when. A hold moves ACTIVE -> RELEASED, EXPIRED or CONFIRMED, and never back.
CREATE TABLE holds (
  hold_id UUID NOT NULL,
  event_id BIGINT NOT NULL,
  seat_label VARCHAR(16) NOT NULL,
  customer_id BIGINT NOT NULL,
  state VARCHAR(10) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  ended_at TIMESTAMPTZ,
  CONSTRAINT holds_pkey PRIMARY KEY (hold_id),
  CONSTRAINT hold_names_a_real_seat FOREIGN KEY (event_id, seat_label)
    REFERENCES seats (event_id, seat_label),
  CONSTRAINT hold_by_a_real_customer FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
  CONSTRAINT known_hold_state CHECK (state IN ('ACTIVE', 'RELEASED', 'EXPIRED', 'CONFIRMED')),
  CONSTRAINT hold_expires_after_it_starts CHECK (expires_at > created_at),
  -- An active hold has not ended; every other state has an end time.
  CONSTRAINT only_active_holds_are_open CHECK ((state = 'ACTIVE') = (ended_at IS NULL)),
  CONSTRAINT hold_ends_after_it_starts CHECK (ended_at IS NULL OR ended_at >= created_at),
  -- The target of the booking's foreign key below, which proves the booking's copies match.
  CONSTRAINT hold_identity UNIQUE (hold_id, event_id, seat_label, customer_id)
);

-- Invariant, part 1: at most one claim per seat. A claim is an ACTIVE hold or a CONFIRMED one
-- (a booking). Expiry is lazy: an ACTIVE hold past expires_at still occupies the index until the
-- next hold on that seat marks it EXPIRED in the same transaction. The index, not Java, decides:
-- two transactions inserting a claim for one seat cannot both commit, whatever the code does.
CREATE UNIQUE INDEX one_claim_per_seat ON holds (event_id, seat_label)
  WHERE state IN ('ACTIVE', 'CONFIRMED');

-- The limit check below reads a customer's active holds on every insert; without this index it
-- would scan every hold. Module 14 explains how indexes are chosen.
CREATE INDEX active_holds_by_customer ON holds (customer_id) WHERE state = 'ACTIVE';

-- Invariant, part 2: at most four active holds per customer. A CHECK constraint sees one row and
-- a unique index compares one key, so a rule over many rows needs a lock: this trigger locks the
-- customer's row, then counts. Every hold of one customer therefore waits its turn, and the count
-- it reads is fresh, because at read committed each statement of a VOLATILE function takes a new
-- snapshot. At repeatable read the count would come from the transaction's first snapshot and
-- could be stale, so that level is refused outright. At serializable the database itself detects
-- two concurrent counts that both miss each other's insert.
CREATE FUNCTION enforce_hold_limit() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  hold_limit CONSTANT INT := TG_ARGV[0]::INT;
  active_holds INT;
BEGIN
  IF TG_OP = 'UPDATE' THEN
    -- A hold never becomes active again, and never moves to another customer.
    IF OLD.state <> 'ACTIVE' OR OLD.customer_id <> NEW.customer_id THEN
      RAISE EXCEPTION 'hold % cannot become active again or change customer', NEW.hold_id
        USING ERRCODE = 'check_violation', CONSTRAINT = 'hold_limit_per_customer';
    END IF;
    RETURN NEW;
  END IF;
  IF current_setting('transaction_isolation') = 'repeatable read' THEN
    RAISE EXCEPTION 'holds are written at read committed or serializable, not repeatable read'
      USING ERRCODE = 'feature_not_supported';
  END IF;
  PERFORM 1 FROM customers WHERE customer_id = NEW.customer_id FOR UPDATE;
  -- The new hold's own creation time is "now": the same clock the code used for expires_at.
  SELECT count(*) INTO active_holds FROM holds
    WHERE customer_id = NEW.customer_id AND state = 'ACTIVE' AND expires_at > NEW.created_at;
  IF active_holds >= hold_limit THEN
    RAISE EXCEPTION 'customer % already has % active holds', NEW.customer_id, active_holds
      USING ERRCODE = 'check_violation', CONSTRAINT = 'hold_limit_per_customer';
  END IF;
  RETURN NEW;
END;
$$;

-- The argument is the limit: 4 active holds per customer, the rule of Module 13's brief.
CREATE TRIGGER hold_limit_per_customer
  BEFORE INSERT OR UPDATE OF state, customer_id ON holds
  FOR EACH ROW WHEN (NEW.state = 'ACTIVE')
  EXECUTE FUNCTION enforce_hold_limit('4');
