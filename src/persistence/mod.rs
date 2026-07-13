//! Persistence layer -- storage backend abstraction.
//!
//! L2Actor (and future L1/L0/GKL actors) talk to a `StorageBackend`
//! trait so the in-memory, file-based, and RocksDB implementations can be swapped
//! without changing actor logic.
//!
//! Default backend is `FileBackend` (JSON files on disk). The in-memory backend
//! is available for tests. The RocksDB backend lives behind the `rocksdb` cargo feature.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use async_trait::async_trait;
use anyhow::Result;

mod file_backend;
pub use file_backend::FileBackend;

#[cfg(feature = "rocksdb")]
mod rocksdb;
#[cfg(feature = "rocksdb")]
pub use rocksdb::RocksDBBackend;

/// Backend-agnostic key/value storage used by storage actors.
///
/// All operations are async because RocksDB is async-friendly and we
/// want a uniform interface for future backends (sled, redb, etc.).
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Get the value at `key`, or `None` if absent.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Put a value at `key`, overwriting any existing value.
    async fn put(&self, key: &str, value: Vec<u8>) -> Result<()>;

    /// Delete a key. No-op if the key does not exist.
    async fn delete(&self, key: &str) -> Result<()>;

    /// List all keys with the given prefix, in lexicographic order.
    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>>;

    /// Number of keys stored (across all prefixes).
    async fn count(&self) -> Result<usize>;

    /// Human-readable backend name (for logging).
    fn name(&self) -> &str;
}

/// Simple in-memory backend. Thread-safe via `tokio::sync::RwLock`.
///
/// Useful for tests and environments where RocksDB cannot be compiled.
/// Data is lost on restart -- this is intentional.
#[derive(Default)]
pub struct InMemoryBackend {
    inner: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StorageBackend for InMemoryBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.inner.read().await.get(key).cloned())
    }

    async fn put(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.inner.write().await.insert(key.to_string(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.write().await.remove(key);
        Ok(())
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys: Vec<String> = self
            .inner
            .read()
            .await
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn count(&self) -> Result<usize> {
        Ok(self.inner.read().await.len())
    }

    fn name(&self) -> &str {
        "InMemoryBackend"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_get_roundtrip() {
        let b = InMemoryBackend::new();
        b.put("foo", b"bar".to_vec()).await.unwrap();
        assert_eq!(b.get("foo").await.unwrap(), Some(b"bar".to_vec()));
    }

    #[tokio::test]
    async fn test_get_missing_returns_none() {
        let b = InMemoryBackend::new();
        assert!(b.get("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_delete_removes_key() {
        let b = InMemoryBackend::new();
        b.put("k", b"v".to_vec()).await.unwrap();
        b.delete("k").await.unwrap();
        assert!(b.get("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_delete_missing_is_noop() {
        let b = InMemoryBackend::new();
        b.delete("nope").await.unwrap();
        assert_eq!(b.count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_list_keys_prefix_filter() {
        let b = InMemoryBackend::new();
        b.put("node:a", b"1".to_vec()).await.unwrap();
        b.put("node:b", b"2".to_vec()).await.unwrap();
        b.put("edge:x", b"3".to_vec()).await.unwrap();

        let keys = b.list_keys("node:").await.unwrap();
        assert_eq!(keys, vec!["node:a".to_string(), "node:b".to_string()]);
    }

    #[tokio::test]
    async fn test_count() {
        let b = InMemoryBackend::new();
        assert_eq!(b.count().await.unwrap(), 0);
        b.put("a", b"1".to_vec()).await.unwrap();
        b.put("b", b"2".to_vec()).await.unwrap();
        assert_eq!(b.count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_concurrent_writes() {
        use std::sync::Arc;
        let b = Arc::new(InMemoryBackend::new());
        let mut handles = Vec::new();
        for i in 0..50 {
            let bb = b.clone();
            handles.push(tokio::spawn(async move {
                bb.put(&format!("k-{i}"), format!("v-{i}").into_bytes()).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(b.count().await.unwrap(), 50);
    }
}
