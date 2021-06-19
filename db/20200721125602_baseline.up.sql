CREATE TABLE IF NOT EXISTS certmagic_locks (
   key text PRIMARY KEY,
   expires timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS certmagic_data (
  key text PRIMARY KEY,
  value bytea NOT NULL,
  modified timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);