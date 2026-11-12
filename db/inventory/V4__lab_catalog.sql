-- seatwave-saga, inventory database: the lab's catalog. One venue and one event with one section
-- of 40,000 seats at Module 2's average ticket of $80.00, and 40,000 customers, one per hold the
-- busiest drill takes (Lab 1's 500 holds a second for 60 s is 30,000), so Module 13's limit of four
-- active holds per customer never refuses a lab hold. Runs after V3.
INSERT INTO venues (venue_id, name) VALUES (1, 'SeatWave Arena');
INSERT INTO events (event_id, venue_id, name, starts_at)
  VALUES (1, 1, 'Opening night', '2026-12-05T20:00:00Z');
INSERT INTO sections (event_id, section_code, price, currency) VALUES (1, 'A', 80.00, 'USD');
INSERT INTO seats (event_id, seat_label, section_code, seat_number)
  SELECT 1, 'A-' || seat_number, 'A', seat_number FROM generate_series(1, 40000) AS seat_number;
INSERT INTO customers (customer_ref, created_at)
  SELECT 'fan_' || customer_number, now() FROM generate_series(1, 40000) AS customer_number;
