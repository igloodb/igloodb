# Native PostgreSQL CDC foundation (unreleased)

Igloo now contains a direct `pgoutput` consumer and an operator readiness probe.
They verify that a transactional logical marker reaches its containing Commit
and maintain volatile, source-wide change epochs. They do **not** yet make
pgwire or agent query caching CDC-consistent. Agent queries continue to bypass
the cache; incremental materializations and coordinated source read snapshots
remain planned.

## Prerequisites

- PostgreSQL 15 or newer with `wal_level=logical` and available replication slots.
- An existing publication including the tables being evaluated. The probe never
  creates or changes publications, source tables or grants.
- Operator credentials permitting logical replication, temporary slot creation,
  and `pg_logical_emit_message`. Provision them separately from agent read roles.
- One explicitly configured host, user and database. Multi-host and `hostaddr`
  routing are rejected by the configuration adapter.

For a disposable development table, an operator can provision a publication:

```sql
CREATE PUBLICATION igloo_changes FOR TABLE public.orders;
```

For eventual strict caching, publication coverage must include every dependent
table and operation without filters that omit relevant changes. Successful
`cdc-check` verifies marker delivery, not publication completeness, authorization
of future queries or a freshness guarantee for query results.

## Run the readiness probe

Use an operator-owned TOML configuration, with credentials supplied through the
normal per-source environment variable:

```toml
[[sources]]
name = "app"
schemas = ["public"]
# Supply IGLOO_SOURCE_APP_URI securely in this operator process.
```

```sh
igloo cdc-check --source app --publication igloo_changes --timeout-ms 10000
```

With exactly one native PostgreSQL source, `--source` may be omitted. The command
creates a uniquely named temporary slot, starts native replication, commits one
WAL marker, waits for its containing Commit, and closes the slot-owning session.
Stdout contains one JSON document; errors exit 2 with a sanitized code/message/hint.
The total deadline is 1–30000 ms, default 10000 ms.

Successful output reports a generation UUID, committed-change epoch, temporary
slot policy, `query_cache_integration: false` and `cross_source_snapshot: false`.
Epochs compare only within the same live generation. They are not PostgreSQL LSNs,
timestamps, durable checkpoints or a shared position across databases.

The CLI control connection currently uses the same `NoTls` transport as the
native source adapter: use a permitted local development connection. Required
TLS is rejected rather than downgraded. Library callers can provide an encrypted
control client and replication settings; that route needs separate deployment
certification before use with production sources.

## Library lifecycle and safety

`postgres_cdc::PgCdcSource` owns the replication task; its handle reports health,
generation and change epoch and requests committed barriers. Marker-only
transactions do not advance the change epoch. Insert/update/delete/truncate
transactions advance it on commit. A marker Message frame alone never resolves
a barrier.

Stream loss and barrier timeout make the generation unusable. Startup, barrier
and shutdown waits are bounded; abandoned barrier registrations are removed.
Shutdown closes the temporary-slot owner. Derived state is volatile and must be
discarded after generation changes. Existing-slot mode is an embedding option;
operators remain responsible for permanent-slot lifecycle and WAL retention.

The next integration must validate source identity, publication coverage,
schema and privileges; use query-scoped snapshots; and fence cache admission
before/after execution. A replication heartbeat or an emitted marker alone
does not establish that integration.

## Verification

The live suite creates unique development tables/publications and requires
`IGLOO_TEST_POSTGRES_URI` pointing to a disposable PostgreSQL service with logical
replication enabled:

```sh
cargo test --locked --test postgres_cdc
```

Hermetic state tests verify marker-before-Commit ordering, rollback handling and
generation invalidation. The live suite exercises committed row changes,
marker-only barriers, shutdown and temporary-slot cleanup. Required CI integration
jobs enable logical replication before executing the suite.
