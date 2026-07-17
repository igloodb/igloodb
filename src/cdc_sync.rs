// src/cdc_sync.rs
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::cache_layer::Cache;

/// Watches a location for Change Data Capture (CDC) events and invalidates
/// cache entries when the underlying data changes.
///
/// For local development the location is a directory of JSON event files
/// (see `dummy_iceberg_cdc/`). Remote locations (e.g. an Iceberg table on
/// S3) are not implemented yet.
///
/// The listener remembers which event files it has already processed, so
/// repeated [`CdcListener::sync`] calls (e.g. from the polling loop in
/// serve mode) only react to NEW events instead of re-invalidating the
/// cache for the same file forever.
pub struct CdcListener {
    iceberg_path: String,
    processed: Mutex<HashSet<PathBuf>>,
}

impl CdcListener {
    pub fn new(iceberg_path: &str) -> Self {
        Self {
            iceberg_path: iceberg_path.to_string(),
            processed: Mutex::new(HashSet::new()),
        }
    }

    /// Processes pending (not-yet-seen) CDC events. Returns the number of
    /// new events found.
    ///
    /// Any new event means the source data may have changed, so all cached
    /// results are conservatively invalidated. Finer-grained invalidation
    /// (per table / per key) is on the roadmap (F2.2).
    pub fn sync(&self, cache: &Cache) -> usize {
        let path = Path::new(&self.iceberg_path);
        if !path.is_dir() {
            // Remote (e.g. s3://...) CDC sources are not supported yet.
            log::warn!(
                "CDC path {:?} is not a local directory; remote CDC sync is not implemented.",
                self.iceberg_path
            );
            return 0;
        }

        let events = self.read_new_local_events(path);
        if events.is_empty() {
            log::debug!("No new CDC events found in {:?}.", self.iceberg_path);
        } else {
            for event in &events {
                log::info!("CDC event: {}", event.trim_end());
            }
            if !cache.is_empty() {
                log::info!(
                    "{} CDC event(s) processed; invalidating {} cached result(s).",
                    events.len(),
                    cache.len()
                );
                cache.clear();
            }
        }
        events.len()
    }

    /// Spawns a background task that syncs every `poll_interval` until the
    /// runtime shuts down. Used by `igloo serve` to keep the cache fresh.
    pub fn spawn_polling(
        self: Arc<Self>,
        cache: Arc<Cache>,
        poll_interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        log::info!(
            "CDC polling started: watching {:?} every {:?}",
            self.iceberg_path,
            poll_interval
        );
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            // The first tick fires immediately, picking up pre-existing
            // events before the first query can populate the cache.
            loop {
                interval.tick().await;
                self.sync(&cache);
            }
        })
    }

    fn read_new_local_events(&self, dir: &Path) -> Vec<String> {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) => {
                log::error!("Failed to read CDC directory {:?}: {}", dir, e);
                return Vec::new();
            }
        };

        let mut processed = self.processed.lock().expect("cdc lock poisoned");
        let mut events = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json") && !processed.contains(&path) {
                match std::fs::read_to_string(&path) {
                    Ok(content) => {
                        processed.insert(path);
                        events.push(content);
                    }
                    Err(e) => log::error!("Failed to read CDC event {:?}: {}", path, e),
                }
            }
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::CdcListener;
    use crate::cache_layer::Cache;
    use std::time::Duration;

    fn test_cache() -> Cache {
        Cache::new(16, Duration::from_secs(300))
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("igloo_cdc_test_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn sync_invalidates_cache_when_events_present() {
        let dir = temp_dir("events");
        std::fs::write(dir.join("event1.json"), r#"{"event": "update"}"#).unwrap();

        let cache = test_cache();
        cache.set("SELECT 1", Vec::new());

        let listener = CdcListener::new(dir.to_str().unwrap());
        let n = listener.sync(&cache);

        assert_eq!(n, 1);
        assert!(
            cache.is_empty(),
            "cache should be invalidated by CDC events"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn sync_processes_each_event_only_once() {
        let dir = temp_dir("dedup");
        std::fs::write(dir.join("event1.json"), r#"{"event": "update"}"#).unwrap();

        let cache = test_cache();
        let listener = CdcListener::new(dir.to_str().unwrap());
        assert_eq!(listener.sync(&cache), 1);

        // The same file must not count as a new event on the next sync, so
        // freshly cached results survive.
        cache.set("SELECT 1", Vec::new());
        assert_eq!(listener.sync(&cache), 0);
        assert_eq!(cache.len(), 1, "cache must survive an already-seen event");

        // A NEW event invalidates again.
        std::fs::write(dir.join("event2.json"), r#"{"event": "delete"}"#).unwrap();
        assert_eq!(listener.sync(&cache), 1);
        assert!(cache.is_empty());

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn sync_keeps_cache_when_no_events() {
        let dir = temp_dir("no_events");

        let cache = test_cache();
        cache.set("SELECT 1", Vec::new());

        let listener = CdcListener::new(dir.to_str().unwrap());
        let n = listener.sync(&cache);

        assert_eq!(n, 0);
        assert_eq!(cache.len(), 1, "cache should be untouched without events");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn sync_handles_missing_directory() {
        let cache = test_cache();
        cache.set("SELECT 1", Vec::new());

        let listener = CdcListener::new("s3://some-bucket/does-not-exist");
        let n = listener.sync(&cache);

        assert_eq!(n, 0);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn sync_ignores_non_json_files() {
        let dir = temp_dir("non_json");
        std::fs::write(dir.join("notes.txt"), "not an event").unwrap();

        let cache = test_cache();
        cache.set("SELECT 1", Vec::new());

        let listener = CdcListener::new(dir.to_str().unwrap());
        let n = listener.sync(&cache);

        assert_eq!(n, 0);
        assert_eq!(cache.len(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
