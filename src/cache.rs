use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Mutex;

pub struct Cache {
    inner: Mutex<LruCache<String, i64>>,
}

impl Cache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(LruCache::new(NonZeroUsize::new(capacity).unwrap())),
        }
    }

    pub fn get(&self, key: &str) -> Option<i64> {
        let mut cache = self.inner.lock().unwrap();
        cache.get(key).copied()
    }

    pub fn put(&self, key: String, value: i64) {
        let mut cache = self.inner.lock().unwrap();
        cache.put(key, value);
    }
}
