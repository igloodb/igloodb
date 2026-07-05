// src/cache_layer.rs
// A simple in-memory cache for query results.
// Currently, operations are infallible, but could return Result in the future
// if storage involved I/O or other fallible operations.
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct Cache {
    store: HashMap<String, String>, // Key: query string, Value: serialized result string
}

impl Cache {
    pub fn new() -> Self {
        log::debug!("Initializing new in-memory cache instance.");
        Self::default()
    }

    pub fn get(&self, query: &str) -> Option<&String> {
        self.store.get(query)
    }

    pub fn set(&mut self, query: &str, result: &str) {
        self.store.insert(query.to_string(), result.to_string());
    }

    /// Removes every cached entry. Used when CDC signals that underlying
    /// data changed and cached results can no longer be trusted.
    pub fn clear(&mut self) {
        let evicted = self.store.len();
        self.store.clear();
        log::debug!("Cache cleared; {} entries evicted.", evicted);
    }

    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::Cache;

    #[test]
    fn get_returns_none_for_missing_key() {
        let cache = Cache::new();
        assert!(cache.get("SELECT 1").is_none());
    }

    #[test]
    fn set_then_get_round_trips() {
        let mut cache = Cache::new();
        cache.set("SELECT 1", "result");
        assert_eq!(cache.get("SELECT 1").map(String::as_str), Some("result"));
    }

    #[test]
    fn set_overwrites_existing_entry() {
        let mut cache = Cache::new();
        cache.set("SELECT 1", "old");
        cache.set("SELECT 1", "new");
        assert_eq!(cache.get("SELECT 1").map(String::as_str), Some("new"));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn clear_evicts_all_entries() {
        let mut cache = Cache::new();
        cache.set("a", "1");
        cache.set("b", "2");
        cache.clear();
        assert!(cache.is_empty());
        assert!(cache.get("a").is_none());
    }
}
