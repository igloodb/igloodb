-- Seed fixture for IGLOO_TEST_POSTGRES_URI-gated integration tests.
--
-- Single source of truth for the `my_pg_table` test data used by both CI
-- (.github/workflows/rust.yml) and local developers running the
-- integration_* tests in src/postgres_table.rs. Safe to re-run: it creates
-- the table if missing and truncates it before seeding, so repeated runs
-- always leave exactly these three rows.
--
-- Usage:
--   psql 'postgres://postgres:postgres@localhost:5432/mydb' -f scripts/seed_test_db.sql

CREATE TABLE IF NOT EXISTS my_pg_table (user_id BIGINT NOT NULL, extra_info TEXT);

TRUNCATE my_pg_table;

INSERT INTO my_pg_table (user_id, extra_info) VALUES
    (42, 'answer to everything'),
    (7, 'lucky number'),
    (100, NULL);

-- Reference table for the crypto-metrics federated integration test
-- (src/crypto_metrics.rs): maps asset symbols to display names.
CREATE TABLE IF NOT EXISTS crypto_assets (asset TEXT NOT NULL, name TEXT NOT NULL);

TRUNCATE crypto_assets;

INSERT INTO crypto_assets (asset, name) VALUES
    ('BTC', 'Bitcoin'),
    ('ETH', 'Ethereum'),
    ('SOL', 'Solana');
