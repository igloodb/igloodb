//! Opt-in PostgreSQL logical replication freshness tracking.
//!
//! This module owns a direct pgoutput connection and a volatile per-source
//! commit epoch. It does not persist cache state, interpret row values, or
//! coordinate snapshots across databases. The configured publication must
//! include every table whose changes should invalidate cached results.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pgwire_replication::client::{ReplicationClient, ReplicationEvent};
use pgwire_replication::config::{ReplicationConfig, SslMode as ReplicationSslMode, TlsConfig};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_postgres::Client;
use uuid::Uuid;

const BARRIER_PREFIX: &str = "igloo.cache.barrier";
const MIN_SERVER_VERSION: i64 = 150_000;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_PENDING_BARRIERS: usize = 1024;
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

/// How the replication slot is provisioned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotMode {
    /// Create a temporary `pgoutput` logical slot on the replication session.
    /// PostgreSQL removes it when that session ends.
    Temporary,
    /// Use an operator-created logical `pgoutput` slot.
    Existing,
}

/// Settings for one PostgreSQL CDC stream.
pub struct PgCdcConfig {
    /// Direct pgwire replication settings. `slot` names the slot to use or,
    /// in temporary mode, create. The named publication must already exist.
    pub replication: ReplicationConfig,
    /// Slot lifecycle policy.
    pub slot_mode: SlotMode,
    /// Maximum time a barrier waits for its committed marker to be decoded.
    pub barrier_timeout: Duration,
}

impl fmt::Debug for PgCdcConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgCdcConfig")
            .field("replication", &"<redacted>")
            .field("slot_mode", &self.slot_mode)
            .field("barrier_timeout", &self.barrier_timeout)
            .finish()
    }
}

impl PgCdcConfig {
    /// Builds replication settings from the parsed PostgreSQL connection
    /// configuration used by Igloo's normal source client. Multi-host and
    /// hostaddr routing are rejected because the replication client cannot
    /// preserve those routing semantics. Required TLS remains required.
    pub fn from_postgres_config(
        source: &tokio_postgres::Config,
        slot: &str,
        publication: &str,
        barrier_timeout: Duration,
    ) -> Result<Self, PgCdcError> {
        use tokio_postgres::config::{Host, SslMode, SslNegotiation};

        let [host] = source.get_hosts() else {
            return Err(PgCdcError::InvalidConfiguration);
        };
        if !source.get_hostaddrs().is_empty()
            || source.get_ports().len() > 1
            || source.get_ssl_negotiation() == SslNegotiation::Direct
            || !valid_slot_name(slot)
            || publication.is_empty()
            || barrier_timeout.is_zero()
        {
            return Err(PgCdcError::InvalidConfiguration);
        }
        let host = match host {
            Host::Tcp(host) => host.clone(),
            #[cfg(unix)]
            Host::Unix(path) => path
                .to_str()
                .ok_or(PgCdcError::InvalidConfiguration)?
                .to_owned(),
        };
        let user = source.get_user().ok_or(PgCdcError::InvalidConfiguration)?;
        let password = source
            .get_password()
            .map(std::str::from_utf8)
            .transpose()
            .map_err(|_| PgCdcError::InvalidConfiguration)?
            .unwrap_or_default();
        let database = source
            .get_dbname()
            .ok_or(PgCdcError::InvalidConfiguration)?;
        let port = source.get_ports().first().copied().unwrap_or(5432);
        let tls = match source.get_ssl_mode() {
            SslMode::Disable => TlsConfig::disabled(),
            SslMode::Prefer => TlsConfig {
                mode: ReplicationSslMode::Prefer,
                ..TlsConfig::default()
            },
            SslMode::Require => TlsConfig::require(),
            _ => return Err(PgCdcError::InvalidConfiguration),
        };
        let mut replication =
            ReplicationConfig::new(host, user, password, database, slot, publication)
                .with_port(port)
                .with_tls(tls);
        if let Some(options) = source.get_options() {
            replication = replication.with_options(options);
        }
        if let Some(name) = source.get_application_name() {
            replication = replication.with_application_name(name);
        }

        Ok(Self {
            replication,
            slot_mode: SlotMode::Temporary,
            barrier_timeout,
        })
    }
}

/// A volatile source position. Compare both fields; epochs from different
/// generations are never comparable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceEpoch {
    /// Changes whenever the stream becomes unhealthy or is re-created.
    pub generation: Uuid,
    /// Number of committed transactions observed in this generation.
    pub committed_epoch: u64,
}

/// Health of the replication stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcHealth {
    /// Connection setup is in progress.
    Starting,
    /// The replication stream is active and its epoch can be used.
    Healthy,
    /// The stream ended or failed; cached positions from it are unusable.
    Unhealthy,
}

/// Errors intentionally omit server details, connection strings and secrets.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PgCdcError {
    #[error("PostgreSQL CDC configuration is invalid")]
    InvalidConfiguration,
    #[error("PostgreSQL CDC requires server version 15 or newer")]
    UnsupportedServerVersion,
    #[error("PostgreSQL CDC replication connection failed")]
    ReplicationConnection,
    #[error("PostgreSQL CDC replication stream failed")]
    ReplicationStream,
    #[error("PostgreSQL CDC marker could not be committed")]
    MarkerCommit,
    #[error("PostgreSQL CDC source is unhealthy")]
    Unhealthy,
    #[error("PostgreSQL CDC marker was not observed before the barrier timeout")]
    BarrierTimeout,
    #[error("PostgreSQL CDC has too many concurrent barriers")]
    TooManyPendingBarriers,
    #[error("PostgreSQL CDC control client addresses a different database")]
    WrongSource,
}

struct State {
    health: CdcHealth,
    epoch: SourceEpoch,
    pending: HashMap<String, oneshot::Sender<SourceEpoch>>,
}

struct Shared {
    state: Mutex<State>,
    barrier_timeout: Duration,
    marker_prefix: String,
    database: String,
}

/// Running per-source CDC stream. Keep this value alive for the desired
/// lifetime of the stream; clones of [`PgCdcHandle`] can be used by request
/// handlers to inspect health and request barriers.
pub struct PgCdcSource {
    handle: PgCdcHandle,
    task: JoinHandle<()>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl fmt::Debug for PgCdcSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgCdcSource")
            .field("health", &self.handle.health())
            .field("epoch", &self.handle.epoch())
            .finish_non_exhaustive()
    }
}

impl PgCdcSource {
    /// Starts a direct pgoutput stream. The publication is never created or
    /// modified here. Temporary slots are created on the same replication
    /// connection that holds them; existing slots must be operator-created.
    pub async fn start(config: PgCdcConfig) -> Result<Self, PgCdcError> {
        tokio::time::timeout(STARTUP_TIMEOUT, Self::start_inner(config))
            .await
            .map_err(|_| PgCdcError::ReplicationConnection)?
    }

    async fn start_inner(config: PgCdcConfig) -> Result<Self, PgCdcError> {
        if config.barrier_timeout.is_zero() || !valid_slot_name(&config.replication.slot) {
            return Err(PgCdcError::InvalidConfiguration);
        }

        let generation = Uuid::new_v4();
        let serial = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                health: CdcHealth::Starting,
                epoch: SourceEpoch {
                    generation,
                    committed_epoch: 0,
                },
                pending: HashMap::new(),
            }),
            barrier_timeout: config.barrier_timeout,
            marker_prefix: format!("{BARRIER_PREFIX}.{serial}"),
            database: config.replication.database.clone(),
        });

        let mut client = ReplicationClient::connect_gated(config.replication.clone())
            .await
            .map_err(|_| PgCdcError::ReplicationConnection)?;
        let version_rows = client
            .simple_query("SHOW server_version_num")
            .await
            .map_err(|_| PgCdcError::ReplicationConnection)?;
        let version = version_rows
            .first()
            .and_then(|row| row.first())
            .and_then(Option::as_ref)
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(|value| value.parse::<i64>().ok())
            .ok_or(PgCdcError::ReplicationConnection)?;
        if version < MIN_SERVER_VERSION {
            return Err(PgCdcError::UnsupportedServerVersion);
        }

        if config.slot_mode == SlotMode::Temporary {
            let slot = quote_slot_name(&config.replication.slot)?;
            client
                .simple_query(&format!(
                    "CREATE_REPLICATION_SLOT {slot} TEMPORARY LOGICAL pgoutput"
                ))
                .await
                .map_err(|_| PgCdcError::ReplicationConnection)?;
        }

        client
            .start()
            .await
            .map_err(|_| PgCdcError::ReplicationConnection)?;
        client
            .wait_started()
            .await
            .map_err(|_| PgCdcError::ReplicationConnection)?;

        {
            let mut state = shared.state.lock().expect("CDC state lock poisoned");
            state.health = CdcHealth::Healthy;
        }

        let task_shared = shared.clone();
        let (shutdown, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _health_guard = HealthGuard(task_shared.clone());
            let mut events = StreamState::default();
            loop {
                let received = tokio::select! {
                    _ = &mut shutdown_rx => {
                        client.stop();
                        while let Ok(Some(event)) = client.recv().await {
                            if let Some(end_lsn) = apply_event(&task_shared, &mut events, event) {
                                client.update_applied_lsn(end_lsn);
                            }
                        }
                        let _ = client.join().await;
                        break;
                    }
                    value = client.recv() => value,
                };
                match received {
                    Ok(Some(event)) => {
                        if let Some(end_lsn) = apply_event(&task_shared, &mut events, event) {
                            client.update_applied_lsn(end_lsn);
                        }
                    }
                    Ok(None) | Err(_) => break,
                }
            }
        });

        Ok(Self {
            handle: PgCdcHandle { shared },
            task,
            shutdown: Some(shutdown),
        })
    }

    /// Returns a handle suitable for sharing with request handlers.
    pub fn handle(&self) -> PgCdcHandle {
        self.handle.clone()
    }

    /// Stops and joins the replication task. A temporary slot is removed when
    /// PostgreSQL closes its owning replication connection.
    pub async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
            let _ = (&mut self.task).await;
        }
    }
}

impl Drop for PgCdcSource {
    fn drop(&mut self) {
        // Invalidate synchronously even if the newly spawned coordinator has
        // never been polled and therefore has not constructed its task guard.
        mark_unhealthy(&self.handle.shared);
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        // Dropping ReplicationClient requests graceful worker shutdown.
        // Aborting this coordinator prevents an un-awaited source drop from
        // detaching its task; the task guard invalidates shared positions.
        self.task.abort();
    }
}

#[derive(Default)]
struct StreamState {
    transaction_markers: HashSet<String>,
    transaction_changed: bool,
}

struct HealthGuard(Arc<Shared>);

impl Drop for HealthGuard {
    fn drop(&mut self) {
        mark_unhealthy(&self.0);
    }
}

fn apply_event(
    shared: &Shared,
    stream: &mut StreamState,
    event: ReplicationEvent,
) -> Option<pgwire_replication::lsn::Lsn> {
    match event {
        ReplicationEvent::Begin { .. } => {
            stream.transaction_markers.clear();
            stream.transaction_changed = false;
            None
        }
        ReplicationEvent::Message {
            transactional: true,
            prefix,
            content,
            ..
        } if prefix == shared.marker_prefix => {
            if let Ok(id) = std::str::from_utf8(&content) {
                if id.len() == 36
                    && Uuid::parse_str(id).is_ok()
                    && stream.transaction_markers.len() < MAX_PENDING_BARRIERS
                    && shared
                        .state
                        .lock()
                        .expect("CDC state lock poisoned")
                        .pending
                        .contains_key(id)
                {
                    stream.transaction_markers.insert(id.to_owned());
                }
            }
            None
        }
        ReplicationEvent::Message { .. } | ReplicationEvent::KeepAlive { .. } => None,
        ReplicationEvent::XLogData { data, .. } => {
            if data
                .first()
                .is_some_and(|tag| !matches!(*tag, b'R' | b'Y' | b'O' | b'M'))
            {
                stream.transaction_changed = true;
            }
            None
        }
        ReplicationEvent::Commit { end_lsn, .. } => {
            let mut state = shared.state.lock().expect("CDC state lock poisoned");
            if stream.transaction_changed {
                state.epoch.committed_epoch = state.epoch.committed_epoch.saturating_add(1);
            }
            let epoch = state.epoch;
            for id in stream.transaction_markers.drain() {
                if let Some(waiter) = state.pending.remove(&id) {
                    let _ = waiter.send(epoch);
                }
            }
            stream.transaction_changed = false;
            Some(end_lsn)
        }
        ReplicationEvent::StoppedAt { .. } => None,
    }
}

fn mark_unhealthy(shared: &Shared) {
    let mut state = shared.state.lock().expect("CDC state lock poisoned");
    if state.health == CdcHealth::Unhealthy {
        return;
    }
    state.health = CdcHealth::Unhealthy;
    state.epoch.generation = Uuid::new_v4();
    let epoch = state.epoch;
    for (_, waiter) in state.pending.drain() {
        let _ = waiter.send(epoch);
    }
}

fn same_database(expected: &str, actual: &str) -> bool {
    expected == actual
}

/// Cloneable access to a running source's volatile health and commit epoch.
#[derive(Clone)]
pub struct PgCdcHandle {
    shared: Arc<Shared>,
}

impl fmt::Debug for PgCdcHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgCdcHandle")
            .field("health", &self.health())
            .field("epoch", &self.epoch())
            .finish()
    }
}

impl PgCdcHandle {
    /// Current stream health.
    pub fn health(&self) -> CdcHealth {
        self.shared
            .state
            .lock()
            .expect("CDC state lock poisoned")
            .health
    }

    /// Current volatile source position.
    pub fn epoch(&self) -> SourceEpoch {
        self.shared
            .state
            .lock()
            .expect("CDC state lock poisoned")
            .epoch
    }

    /// Commits a transactional marker through an idle, dedicated
    /// `sql_client` and waits until the pgoutput stream observes its
    /// containing Commit. The client must address the same PostgreSQL
    /// database and must not have an active caller-owned transaction; a
    /// same-named database on another cluster cannot be distinguished here.
    /// This method uses one autocommit SELECT and never manages caller
    /// transactions.
    pub async fn barrier(&self, sql_client: &Client) -> Result<SourceEpoch, PgCdcError> {
        let deadline = tokio::time::Instant::now() + self.shared.barrier_timeout;
        let current_database = match tokio::time::timeout_at(
            deadline,
            sql_client.query_one("SELECT current_database()", &[]),
        )
        .await
        {
            Ok(Ok(row)) => row.get::<_, String>(0),
            Ok(Err(_)) => return Err(PgCdcError::WrongSource),
            Err(_) => {
                mark_unhealthy(&self.shared);
                return Err(PgCdcError::BarrierTimeout);
            }
        };
        if !same_database(&self.shared.database, &current_database) {
            return Err(PgCdcError::WrongSource);
        }
        let id = Uuid::new_v4().to_string();
        let (sender, receiver) = oneshot::channel();
        {
            let mut state = self.shared.state.lock().expect("CDC state lock poisoned");
            if state.health != CdcHealth::Healthy {
                return Err(PgCdcError::Unhealthy);
            }
            if state.pending.len() >= MAX_PENDING_BARRIERS {
                return Err(PgCdcError::TooManyPendingBarriers);
            }
            state.pending.insert(id.clone(), sender);
        }
        let mut pending_guard = PendingGuard {
            shared: self.shared.clone(),
            id: id.clone(),
        };

        match tokio::time::timeout_at(
            deadline,
            sql_client.query_one(
                "SELECT pg_logical_emit_message(true, $1, $2)",
                &[&self.shared.marker_prefix, &id],
            ),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(_)) => return Err(PgCdcError::MarkerCommit),
            Err(_) => {
                mark_unhealthy(&self.shared);
                return Err(PgCdcError::BarrierTimeout);
            }
        }

        match tokio::time::timeout_at(deadline, receiver).await {
            Ok(Ok(epoch)) if self.health() == CdcHealth::Healthy => {
                pending_guard.disarm();
                Ok(epoch)
            }
            Ok(Ok(_)) | Ok(Err(_)) => Err(PgCdcError::Unhealthy),
            Err(_) => {
                mark_unhealthy(&self.shared);
                Err(PgCdcError::BarrierTimeout)
            }
        }
    }
}

struct PendingGuard {
    shared: Arc<Shared>,
    id: String,
}

impl PendingGuard {
    fn disarm(&mut self) {
        self.id.clear();
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if !self.id.is_empty() {
            self.shared
                .state
                .lock()
                .expect("CDC state lock poisoned")
                .pending
                .remove(&self.id);
        }
    }
}

fn valid_slot_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn quote_slot_name(name: &str) -> Result<String, PgCdcError> {
    if valid_slot_name(name) {
        Ok(format!("\"{name}\""))
    } else {
        Err(PgCdcError::InvalidConfiguration)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_event, mark_unhealthy, quote_slot_name, same_database, valid_slot_name, CdcHealth,
        PendingGuard, PgCdcError, Shared, SourceEpoch, State, StreamState,
    };
    use pgwire_replication::client::ReplicationEvent;
    use pgwire_replication::lsn::Lsn;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::oneshot;
    use uuid::Uuid;

    fn fixture() -> (Arc<Shared>, String, oneshot::Receiver<SourceEpoch>) {
        let id = Uuid::new_v4().to_string();
        let (sender, receiver) = oneshot::channel();
        let mut pending = HashMap::new();
        pending.insert(id.clone(), sender);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                health: CdcHealth::Healthy,
                epoch: SourceEpoch {
                    generation: Uuid::new_v4(),
                    committed_epoch: 0,
                },
                pending,
            }),
            barrier_timeout: Duration::from_secs(1),
            marker_prefix: "test.marker".into(),
            database: "test_db".into(),
        });
        (shared, id, receiver)
    }

    #[test]
    fn slot_names_are_restricted_before_sql_construction() {
        assert_eq!(quote_slot_name("igloo_slot_1").unwrap(), "\"igloo_slot_1\"");
        for invalid in [
            "",
            "x; DROP DATABASE postgres",
            "has-dash",
            "UpperCase",
            "é",
            &"x".repeat(64),
        ] {
            assert!(!valid_slot_name(invalid));
            assert_eq!(
                quote_slot_name(invalid),
                Err(PgCdcError::InvalidConfiguration)
            );
        }
    }

    #[test]
    fn epochs_are_scoped_to_a_generation() {
        let first = SourceEpoch {
            generation: Uuid::new_v4(),
            committed_epoch: 8,
        };
        let next_generation = SourceEpoch {
            generation: Uuid::new_v4(),
            committed_epoch: 0,
        };
        assert_ne!(first.generation, next_generation.generation);
        assert_ne!(first, next_generation);
        assert_ne!(CdcHealth::Healthy, CdcHealth::Unhealthy);
    }

    #[tokio::test]
    async fn dropping_an_unpolled_source_invalidates_handles_immediately() {
        let (shared, _, _) = fixture();
        let handle = super::PgCdcHandle {
            shared: shared.clone(),
        };
        let previous = handle.epoch();
        let source = super::PgCdcSource {
            handle: handle.clone(),
            task: tokio::spawn(std::future::pending()),
            shutdown: None,
        };
        drop(source);
        assert_eq!(handle.health(), CdcHealth::Unhealthy);
        assert_ne!(handle.epoch().generation, previous.generation);
        let invalidated = handle.epoch();
        mark_unhealthy(&shared);
        assert_eq!(handle.epoch(), invalidated, "cleanup is idempotent");
    }

    #[test]
    fn source_database_mismatch_is_detectable_without_touching_stream_state() {
        assert!(same_database("orders", "orders"));
        assert!(!same_database("orders", "billing"));
    }

    #[test]
    fn connection_conversion_preserves_tls_preference_and_rejects_routing_loss() {
        use super::PgCdcConfig;
        use pgwire_replication::config::SslMode as ReplicationSslMode;

        let preferred: tokio_postgres::Config =
            "host=localhost user=replicator dbname=app sslmode=prefer"
                .parse()
                .unwrap();
        let converted = PgCdcConfig::from_postgres_config(
            &preferred,
            "igloo_slot",
            "igloo_pub",
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(converted.replication.tls.mode, ReplicationSslMode::Prefer);

        let required: tokio_postgres::Config =
            "host=localhost user=replicator dbname=app sslmode=require"
                .parse()
                .unwrap();
        let converted = PgCdcConfig::from_postgres_config(
            &required,
            "igloo_slot",
            "igloo_pub",
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(converted.replication.tls.mode, ReplicationSslMode::Require);

        let multi_port: tokio_postgres::Config =
            "host=localhost port=5432,5433 user=replicator dbname=app"
                .parse()
                .unwrap();
        assert_eq!(
            PgCdcConfig::from_postgres_config(
                &multi_port,
                "igloo_slot",
                "igloo_pub",
                Duration::from_secs(1),
            )
            .unwrap_err(),
            PgCdcError::InvalidConfiguration
        );
    }

    #[test]
    fn disconnect_invalidates_generation_and_resolves_waiters() {
        let (shared, _, receiver) = fixture();
        let generation = shared.state.lock().unwrap().epoch.generation;
        mark_unhealthy(&shared);
        assert_eq!(shared.state.lock().unwrap().health, CdcHealth::Unhealthy);
        assert_ne!(shared.state.lock().unwrap().epoch.generation, generation);
        assert_eq!(
            receiver.blocking_recv().unwrap(),
            shared.state.lock().unwrap().epoch
        );
    }

    #[test]
    fn cancelled_barrier_removes_its_waiter() {
        let (shared, id, receiver) = fixture();
        {
            let _guard = PendingGuard {
                shared: shared.clone(),
                id,
            };
        }
        assert!(shared.state.lock().unwrap().pending.is_empty());
        assert!(receiver.blocking_recv().is_err());
    }

    #[test]
    fn transactional_marker_resolves_only_after_commit_without_advancing_epoch() {
        let (shared, id, mut receiver) = fixture();
        let mut stream = StreamState::default();
        apply_event(
            &shared,
            &mut stream,
            ReplicationEvent::Begin {
                final_lsn: Lsn(1),
                xid: 1,
                commit_time_micros: 0,
            },
        );
        apply_event(
            &shared,
            &mut stream,
            ReplicationEvent::Message {
                transactional: true,
                lsn: Lsn(2),
                prefix: "test.marker".into(),
                content: id.into_bytes().into(),
            },
        );
        assert!(
            receiver.try_recv().is_err(),
            "Message alone cannot finish a barrier"
        );
        assert_eq!(shared.state.lock().unwrap().epoch.committed_epoch, 0);

        apply_event(
            &shared,
            &mut stream,
            ReplicationEvent::Commit {
                lsn: Lsn(3),
                end_lsn: Lsn(4),
                commit_time_micros: 0,
            },
        );
        assert_eq!(receiver.try_recv().unwrap().committed_epoch, 0);
    }

    #[test]
    fn row_change_advances_at_commit_but_marker_and_metadata_do_not() {
        let (shared, _, _receiver) = fixture();
        let mut stream = StreamState::default();
        let begin = || ReplicationEvent::Begin {
            final_lsn: Lsn(1),
            xid: 2,
            commit_time_micros: 0,
        };
        let commit = || ReplicationEvent::Commit {
            lsn: Lsn(3),
            end_lsn: Lsn(4),
            commit_time_micros: 0,
        };

        apply_event(&shared, &mut stream, begin());
        apply_event(
            &shared,
            &mut stream,
            ReplicationEvent::XLogData {
                wal_start: Lsn(2),
                wal_end: Lsn(2),
                server_time_micros: 0,
                data: bytes::Bytes::from_static(b"R"),
            },
        );
        apply_event(&shared, &mut stream, commit());
        assert_eq!(shared.state.lock().unwrap().epoch.committed_epoch, 0);

        apply_event(&shared, &mut stream, begin());
        apply_event(
            &shared,
            &mut stream,
            ReplicationEvent::XLogData {
                wal_start: Lsn(5),
                wal_end: Lsn(5),
                server_time_micros: 0,
                data: bytes::Bytes::from_static(b"I"),
            },
        );
        assert_eq!(shared.state.lock().unwrap().epoch.committed_epoch, 0);
        apply_event(&shared, &mut stream, commit());
        assert_eq!(shared.state.lock().unwrap().epoch.committed_epoch, 1);

        // A rollback produces no pgoutput Commit, so its pending change cannot
        // move the committed epoch.
        apply_event(&shared, &mut stream, begin());
        apply_event(
            &shared,
            &mut stream,
            ReplicationEvent::XLogData {
                wal_start: Lsn(6),
                wal_end: Lsn(6),
                server_time_micros: 0,
                data: bytes::Bytes::from_static(b"D"),
            },
        );
        assert_eq!(shared.state.lock().unwrap().epoch.committed_epoch, 1);
    }
}
