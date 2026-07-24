// src/cache_layer.rs
//! In-memory query-result cache: Arrow-native, bounded, thread-safe.
//!
//! Values are the actual Arrow record batches a query produced (not
//! display strings). Keys are canonicalized SQL text — a parse round-trip
//! with a lexical fallback, an interim scheme until plan fingerprinting
//! lands (roadmap F1.4). Entries expire after a TTL and the
//! least-recently-used entry is evicted once the configured capacity is
//! reached.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

/// Canonicalizes SQL text for cache keying.
///
/// Primary path: parse with sqlparser's [`PostgreSqlDialect`] (re-exported
/// by DataFusion — no extra dependency) and use the statements' canonical
/// `Display` form, so whitespace, keyword casing, and trailing semicolons
/// all normalize away — "select 1", "SELECT  1" and "SELECT 1;" share one
/// key. Unquoted identifier case is deliberately NOT folded: that failure
/// mode is a spurious miss (re-execution), never a wrong hit.
///
/// Fallback: input sqlparser cannot parse (pgwire clients may send
/// anything) is normalized lexically by `normalize_lexical` so non-SQL
/// keys still cache consistently.
pub fn normalize_query(sql: &str) -> String {
    match Parser::parse_sql(&PostgreSqlDialect {}, sql) {
        Ok(statements) if !statements.is_empty() => statements
            .iter()
            .map(|statement| statement.to_string())
            .collect::<Vec<_>>()
            .join("; "),
        _ => normalize_lexical(sql),
    }
}

/// Lexical fallback normalization: trims, drops a trailing `;`, and
/// collapses whitespace runs to single spaces — but never inside quoted
/// string literals (`'...'`) or quoted identifiers (`"..."`), where
/// whitespace is significant.
fn normalize_lexical(sql: &str) -> String {
    #[derive(PartialEq)]
    enum QuoteState {
        None,
        Single,
        Double,
    }

    let trimmed = sql.trim();
    let trimmed = trimmed.strip_suffix(';').map_or(trimmed, str::trim_end);

    let mut out = String::with_capacity(trimmed.len());
    let mut state = QuoteState::None;
    let mut pending_space = false;
    for ch in trimmed.chars() {
        match state {
            QuoteState::None => {
                if ch.is_whitespace() {
                    pending_space = true;
                    continue;
                }
                if pending_space && !out.is_empty() {
                    out.push(' ');
                }
                pending_space = false;
                match ch {
                    '\'' => state = QuoteState::Single,
                    '"' => state = QuoteState::Double,
                    _ => {}
                }
                out.push(ch);
            }
            QuoteState::Single => {
                // A doubled '' is an escaped quote; treating each ' as a
                // state flip handles that correctly for normalization.
                if ch == '\'' {
                    state = QuoteState::None;
                }
                out.push(ch);
            }
            QuoteState::Double => {
                if ch == '"' {
                    state = QuoteState::None;
                }
                out.push(ch);
            }
        }
    }
    out
}

/// Snapshot of cache counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions_lru: u64,
    pub evictions_ttl: u64,
    pub entries: usize,
}

struct Entry {
    batches: Arc<Vec<RecordBatch>>,
    inserted_at: Instant,
    last_used: u64,
}

struct Inner {
    store: HashMap<String, Entry>,
    tick: u64,
    hits: u64,
    misses: u64,
    evictions_lru: u64,
    evictions_ttl: u64,
}

type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// Thread-safe query-result cache; share it as `Arc<Cache>`.
pub struct Cache {
    inner: Mutex<Inner>,
    max_entries: usize,
    ttl: Duration,
    clock: Clock,
}

impl Cache {
    /// `max_entries` and `ttl` must be positive; the config layer
    /// validates user input, this asserts against internal misuse.
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self::with_clock(max_entries, ttl, Arc::new(Instant::now))
    }

    /// Injectable clock so TTL behavior is testable without sleeping.
    pub fn with_clock(max_entries: usize, ttl: Duration, clock: Clock) -> Self {
        assert!(max_entries > 0, "cache max_entries must be positive");
        assert!(!ttl.is_zero(), "cache ttl must be positive");
        log::debug!(
            "Initializing cache: max_entries={}, ttl={:?}",
            max_entries,
            ttl
        );
        Self {
            inner: Mutex::new(Inner {
                store: HashMap::new(),
                tick: 0,
                hits: 0,
                misses: 0,
                evictions_lru: 0,
                evictions_ttl: 0,
            }),
            max_entries,
            ttl,
            clock,
        }
    }

    pub fn get(&self, sql: &str) -> Option<Arc<Vec<RecordBatch>>> {
        let key = normalize_query(sql);
        let now = (self.clock)();
        let mut inner = self.inner.lock().expect("cache lock poisoned");
        inner.tick += 1;
        let tick = inner.tick;

        enum Lookup {
            Hit(Arc<Vec<RecordBatch>>),
            Expired,
            Absent,
        }
        let lookup = match inner.store.get_mut(&key) {
            Some(entry) => {
                if now.duration_since(entry.inserted_at) <= self.ttl {
                    entry.last_used = tick;
                    Lookup::Hit(entry.batches.clone())
                } else {
                    Lookup::Expired
                }
            }
            None => Lookup::Absent,
        };
        match lookup {
            Lookup::Hit(batches) => {
                inner.hits += 1;
                Some(batches)
            }
            Lookup::Expired => {
                inner.store.remove(&key);
                inner.evictions_ttl += 1;
                inner.misses += 1;
                None
            }
            Lookup::Absent => {
                inner.misses += 1;
                None
            }
        }
    }

    pub fn set(&self, sql: &str, batches: Vec<RecordBatch>) {
        let key = normalize_query(sql);
        let now = (self.clock)();
        let mut inner = self.inner.lock().expect("cache lock poisoned");
        inner.tick += 1;
        let tick = inner.tick;

        // Evict the least-recently-used entry if inserting a NEW key would
        // exceed capacity. O(n) scan — fine at the configured scales.
        if !inner.store.contains_key(&key) && inner.store.len() >= self.max_entries {
            if let Some(lru_key) = inner
                .store
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone())
            {
                inner.store.remove(&lru_key);
                inner.evictions_lru += 1;
            }
        }

        inner.store.insert(
            key,
            Entry {
                batches: Arc::new(batches),
                inserted_at: now,
                last_used: tick,
            },
        );
    }

    /// Removes every cached entry. Used when CDC signals that underlying
    /// data changed and cached results can no longer be trusted.
    pub fn clear(&self) {
        let mut inner = self.inner.lock().expect("cache lock poisoned");
        let evicted = inner.store.len();
        inner.store.clear();
        log::debug!("Cache cleared; {} entries evicted.", evicted);
    }

    pub fn len(&self) -> usize {
        self.inner.lock().expect("cache lock poisoned").store.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.lock().expect("cache lock poisoned");
        CacheStats {
            hits: inner.hits,
            misses: inner.misses,
            evictions_lru: inner.evictions_lru,
            evictions_ttl: inner.evictions_ttl,
            entries: inner.store.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize_query, Cache};
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    fn batch(value: i64) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "v",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap()
    }

    fn small_cache() -> Cache {
        Cache::new(16, Duration::from_secs(300))
    }

    // --- normalization ---

    #[test]
    fn normalization_equivalence_pairs() {
        let pairs = [
            ("SELECT 1", "  SELECT 1  "),
            ("SELECT 1", "SELECT 1;"),
            ("SELECT 1", "SELECT\n\t1"),
            ("SELECT a, b FROM t", "SELECT  a,  b\nFROM   t"),
            (
                "SELECT * FROM t WHERE x = 'lit'",
                "SELECT *  FROM t\nWHERE x = 'lit' ;",
            ),
        ];
        for (a, b) in pairs {
            assert_eq!(normalize_query(a), normalize_query(b), "{:?} vs {:?}", a, b);
        }
    }

    #[test]
    fn normalization_non_equivalence_pairs() {
        let pairs = [
            // Different literals.
            ("SELECT 'a'", "SELECT 'b'"),
            // Whitespace inside a string literal is significant.
            ("SELECT 'a b'", "SELECT 'a  b'"),
            // Case inside a literal is significant.
            ("SELECT 'ABC'", "SELECT 'abc'"),
            // Whitespace inside a quoted identifier is significant.
            ("SELECT \"a b\" FROM t", "SELECT \"a  b\" FROM t"),
        ];
        for (a, b) in pairs {
            assert_ne!(normalize_query(a), normalize_query(b), "{:?} vs {:?}", a, b);
        }
    }

    #[test]
    fn normalization_handles_escaped_quotes() {
        // 'it''s  x' contains an escaped quote; its inner whitespace must
        // survive normalization.
        let sql = "SELECT 'it''s  x'  FROM t";
        assert_eq!(normalize_query(sql), "SELECT 'it''s  x' FROM t");
    }

    #[test]
    fn normalization_folds_keyword_case() {
        // Only the parse round-trip gives this: keyword case folds while
        // literal case (tested above) stays significant.
        assert_eq!(normalize_query("select 1"), normalize_query("SELECT  1"));

        let cache = small_cache();
        cache.set("SELECT  1", vec![batch(1)]);
        assert!(cache.get("select 1").is_some(), "keyword-case variant hits");
    }

    #[test]
    fn normalization_falls_back_for_non_sql() {
        // Unparseable input takes the lexical path and still keys
        // consistently (pgwire clients may send anything).
        assert_eq!(
            normalize_query("  not sql   at all!!  "),
            normalize_query("not sql at all!!")
        );

        let cache = small_cache();
        cache.set("not sql at all!!", vec![batch(1)]);
        assert!(cache.get("  not sql   at all!!  ").is_some());
    }

    // --- basic behavior ---

    #[test]
    fn get_returns_none_for_missing_key() {
        let cache = small_cache();
        assert!(cache.get("SELECT 1").is_none());
        assert_eq!(cache.stats().misses, 1);
    }

    #[test]
    fn set_then_get_round_trips_batches() {
        let cache = small_cache();
        cache.set("SELECT 1", vec![batch(42)]);
        let got = cache.get("SELECT   1 ;").expect("normalized hit");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].num_rows(), 1);
        assert_eq!(cache.stats().hits, 1);
    }

    #[test]
    fn set_overwrites_existing_entry() {
        let cache = small_cache();
        cache.set("SELECT 1", vec![batch(1)]);
        cache.set("SELECT 1", vec![batch(2)]);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn clear_evicts_all_entries() {
        let cache = small_cache();
        cache.set("a", vec![batch(1)]);
        cache.set("b", vec![batch(2)]);
        cache.clear();
        assert!(cache.is_empty());
        assert!(cache.get("a").is_none());
    }

    // --- LRU ---

    #[test]
    fn lru_evicts_least_recently_used() {
        let cache = Cache::new(2, Duration::from_secs(300));
        cache.set("a", vec![batch(1)]);
        cache.set("b", vec![batch(2)]);
        // Touch "a" so "b" becomes least recently used.
        assert!(cache.get("a").is_some());
        cache.set("c", vec![batch(3)]);

        assert!(cache.get("a").is_some(), "recently used entry kept");
        assert!(cache.get("b").is_none(), "LRU entry evicted");
        assert!(cache.get("c").is_some());
        assert_eq!(cache.stats().evictions_lru, 1);
        assert_eq!(cache.len(), 2);
    }

    // --- TTL ---

    #[test]
    fn ttl_expires_entries_without_sleeping() {
        let now = Arc::new(Mutex::new(Instant::now()));
        let clock_now = now.clone();
        let cache = Cache::with_clock(
            16,
            Duration::from_secs(60),
            Arc::new(move || *clock_now.lock().unwrap()),
        );

        cache.set("SELECT 1", vec![batch(1)]);
        assert!(cache.get("SELECT 1").is_some(), "fresh entry hits");

        *now.lock().unwrap() += Duration::from_secs(61);
        assert!(cache.get("SELECT 1").is_none(), "expired entry misses");
        let stats = cache.stats();
        assert_eq!(stats.evictions_ttl, 1);
        assert_eq!(stats.entries, 0);
    }

    // --- concurrency ---

    #[test]
    fn concurrent_get_set_smoke() {
        let cache = Arc::new(Cache::new(32, Duration::from_secs(300)));
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let cache = cache.clone();
                std::thread::spawn(move || {
                    for i in 0..200 {
                        let key = format!("SELECT {}", i % 40);
                        if (i + t) % 3 == 0 {
                            cache.set(&key, vec![batch(i as i64)]);
                        } else {
                            let _ = cache.get(&key);
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert!(cache.len() <= 32);
        let stats = cache.stats();
        assert!(stats.hits + stats.misses > 0);
    }
}
