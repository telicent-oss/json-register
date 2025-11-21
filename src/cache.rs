use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Mutex;

/// A thread-safe Least Recently Used (LRU) cache.
///
/// This struct wraps an `LruCache` in a `Mutex` to allow concurrent access
/// from multiple threads. It maps canonicalised JSON strings to their
/// corresponding database IDs.
pub struct Cache {
    inner: Mutex<LruCache<String, i64>>,
}

impl Cache {
    /// Creates a new `Cache` with the specified capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - The maximum number of items the cache can hold.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(LruCache::new(NonZeroUsize::new(capacity).unwrap())),
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
    /// `Some(i64)` if the key exists, `None` otherwise.
    pub fn get(&self, key: &str) -> Option<i64> {
        let mut cache = self.inner.lock().unwrap();
        cache.get(key).copied()
    }

    /// Inserts a key-value pair into the cache.
    ///
    /// # Arguments
    ///
    /// * `key` - The canonicalised JSON string key.
    /// * `value` - The database ID associated with the key.
    pub fn put(&self, key: String, value: i64) {
        let mut cache = self.inner.lock().unwrap();
        cache.put(key, value);
    }
}
