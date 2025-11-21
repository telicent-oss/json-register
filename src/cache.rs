use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// A thread-safe Least Recently Used (LRU) cache.
///
/// This struct wraps an `LruCache` in a `Mutex` to allow concurrent access
/// from multiple threads. It maps canonicalised JSON strings to their
/// corresponding database IDs. It also tracks hit and miss statistics.
pub struct Cache {
    inner: Mutex<LruCache<String, i32>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl Cache {
    /// Creates a new `Cache` with the specified capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - The maximum number of items the cache can hold. Minimum capacity is 1.
    pub fn new(capacity: usize) -> Self {
        // Ensure capacity is at least 1 to avoid panic
        let safe_capacity = capacity.max(1);
        Self {
            inner: Mutex::new(LruCache::new(
                NonZeroUsize::new(safe_capacity).expect("capacity should be non-zero after max(1)"),
            )),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Retrieves an ID from the cache if it exists.
    ///
    /// # Arguments
    ///
    /// * `key` - The canonicalised JSON string key.
    ///
    /// # Returns
    ///
    /// `Some(i32)` if the key exists, `None` otherwise.
    /// Returns `None` if the cache mutex is poisoned (treated as cache miss).
    pub fn get(&self, key: &str) -> Option<i32> {
        // Handle poisoned mutex gracefully by treating it as a cache miss
        let mut cache = self.inner.lock().ok()?;
        let result = cache.get(key).copied();

        if result.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }

        result
    }

    /// Inserts a key-value pair into the cache.
    ///
    /// # Arguments
    ///
    /// * `key` - The canonicalised JSON string key.
    /// * `value` - The database ID associated with the key.
    ///
    /// If the cache mutex is poisoned, the operation is silently skipped.
    pub fn put(&self, key: String, value: i32) {
        // Handle poisoned mutex gracefully by skipping the cache update
        if let Ok(mut cache) = self.inner.lock() {
            cache.put(key, value);
        }
    }

    /// Returns the number of cache hits.
    ///
    /// # Returns
    ///
    /// The total number of successful cache lookups.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Returns the number of cache misses.
    ///
    /// # Returns
    ///
    /// The total number of unsuccessful cache lookups.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Returns the cache hit rate as a percentage.
    ///
    /// # Returns
    ///
    /// The hit rate as a float between 0.0 and 100.0.
    /// Returns 0.0 if no cache operations have occurred.
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits();
        let misses = self.misses();
        let total = hits + misses;

        if total == 0 {
            0.0
        } else {
            (hits as f64 / total as f64) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_zero_capacity_does_not_panic() {
        // Verifies that creating a cache with capacity 0 doesn't panic
        // and is automatically adjusted to minimum capacity of 1
        let cache = Cache::new(0);
        cache.put("test".to_string(), 42);
        assert_eq!(cache.get("test"), Some(42));
    }

    #[test]
    fn test_cache_basic_operations() {
        // Verifies basic cache get/put operations
        let cache = Cache::new(10);

        assert_eq!(cache.get("key1"), None);

        cache.put("key1".to_string(), 100);
        assert_eq!(cache.get("key1"), Some(100));

        cache.put("key2".to_string(), 200);
        assert_eq!(cache.get("key2"), Some(200));
        assert_eq!(cache.get("key1"), Some(100));
    }

    #[test]
    fn test_cache_lru_eviction() {
        // Verifies that LRU eviction works correctly with small capacity
        let cache = Cache::new(2);

        cache.put("key1".to_string(), 1);
        cache.put("key2".to_string(), 2);
        cache.put("key3".to_string(), 3); // Should evict key1

        assert_eq!(cache.get("key1"), None); // Evicted
        assert_eq!(cache.get("key2"), Some(2));
        assert_eq!(cache.get("key3"), Some(3));
    }

    #[test]
    fn test_cache_hit_miss_tracking() {
        // Verifies that cache hit/miss statistics are tracked correctly
        let cache = Cache::new(10);

        // Initially, no hits or misses
        assert_eq!(cache.hits(), 0);
        assert_eq!(cache.misses(), 0);
        assert_eq!(cache.hit_rate(), 0.0);

        // First lookup should be a miss
        assert_eq!(cache.get("key1"), None);
        assert_eq!(cache.hits(), 0);
        assert_eq!(cache.misses(), 1);
        assert_eq!(cache.hit_rate(), 0.0);

        // Add an entry
        cache.put("key1".to_string(), 100);

        // Second lookup should be a hit
        assert_eq!(cache.get("key1"), Some(100));
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.misses(), 1);
        assert_eq!(cache.hit_rate(), 50.0);

        // Another hit
        assert_eq!(cache.get("key1"), Some(100));
        assert_eq!(cache.hits(), 2);
        assert_eq!(cache.misses(), 1);
        assert!((cache.hit_rate() - 66.666).abs() < 0.01);

        // Another miss
        assert_eq!(cache.get("key2"), None);
        assert_eq!(cache.hits(), 2);
        assert_eq!(cache.misses(), 2);
        assert_eq!(cache.hit_rate(), 50.0);
    }
}
