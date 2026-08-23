-- Seed fixture for IGLOO_TEST_POSTGRES_URI-gated integration tests.
--
-- Seeds the `crypto_assets` reference table used by the crypto-metrics
-- federated integration test (src/crypto_metrics.rs), which maps asset
-- symbols to display names. Safe to re-run: it creates the table if missing
-- and truncates it before seeding, so repeated runs always leave exactly
-- these three rows.
--
-- Every other live-database test creates and drops its own uniquely-named
-- fixture table at runtime, so no other seeding is required.
--
-- Usage:
--   psql 'postgres://postgres:postgres@localhost:5432/mydb' -f scripts/seed_test_db.sql

CREATE TABLE IF NOT EXISTS crypto_assets (asset TEXT NOT NULL, name TEXT NOT NULL);

TRUNCATE crypto_assets;

INSERT INTO crypto_assets (asset, name) VALUES
    ('BTC', 'Bitcoin'),
    ('ETH', 'Ethereum'),
    ('SOL', 'Solana');
