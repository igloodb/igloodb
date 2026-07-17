// src/cdc_sync.rs
use crate::cache_layer::Cache;
use std::path::Path;

/// Watches a location for Change Data Capture (CDC) events and invalidates
/// cache entries when the underlying data changes.
///
/// For local development the location is a directory of JSON event files
/// (see `dummy_iceberg_cdc/`). Remote locations (e.g. an Iceberg table on
/// S3) are not implemented yet.
pub struct CdcListener {
    iceberg_path: String,
}

impl CdcListener {
    pub fn new(iceberg_path: &str) -> Self {
        Self {
            iceberg_path: iceberg_path.to_string(),
        }
    }

    /// Processes pending CDC events. Returns the number of events found.
    ///
    /// Any event means the source data may have changed, so all cached
    /// results are conservatively invalidated. Finer-grained invalidation
    /// (per table / per key) is on the roadmap.
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

        let events = Self::read_local_events(path);
        if events.is_empty() {
            log::info!("No CDC events found in {:?}.", self.iceberg_path);
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

    fn read_local_events(dir: &Path) -> Vec<String> {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) => {
                log::error!("Failed to read CDC directory {:?}: {}", dir, e);
                return Vec::new();
            }
        };

        let mut events = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                match std::fs::read_to_string(&path) {
                    Ok(content) => events.push(content),
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
