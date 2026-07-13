// RocksDB persistence backend with column families: current, history, wal

use crate::persistence::StorageBackend;
use anyhow::Result;
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Column family names
pub const CF_CURRENT: &str = "current";
pub const CF_HISTORY: &str = "history";
pub const CF_WAL: &str = "wal";

/// RocksDB backend with column families
pub struct RocksDBBackend {
    db: Arc<DB>,
    path: PathBuf,
}

impl RocksDBBackend {
    /// Open or create RocksDB at given path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_CURRENT, Options::default()),
            ColumnFamilyDescriptor::new(CF_HISTORY, Options::default()),
            ColumnFamilyDescriptor::new(CF_WAL, Options::default()),
        ];
        
        let db = DB::open_cf_descriptors(&opts, &path, cf_descriptors)?;
        
        Ok(Self {
            db: Arc::new(db),
            path,
        })
    }
    
    fn get_from_cf(&self, key: &str, cf: &str) -> Result<Option<Vec<u8>>> {
        let cf_handle = self.db.cf_handle(cf)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", cf))?;
        
        Ok(self.db.get_cf(&cf_handle, key.as_bytes())?)
    }
    
    fn put_into_cf(&self, key: &str, value: Vec<u8>, cf: &str) -> Result<()> {
        let cf_handle = self.db.cf_handle(cf)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", cf))?;
        
        self.db.put_cf(&cf_handle, key.as_bytes(), &value)?;
        Ok(())
    }
    
    fn delete_from_cf(&self, key: &str, cf: &str) -> Result<()> {
        let cf_handle = self.db.cf_handle(cf)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", cf))?;
        
        self.db.delete_cf(&cf_handle, key.as_bytes())?;
        Ok(())
    }
    
    fn list_keys_from_cf(&self, prefix: &str, cf: &str) -> Result<Vec<String>> {
        let cf_handle = self.db.cf_handle(cf)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", cf))?;
        
        let mut keys = Vec::new();
        let prefix_bytes = prefix.as_bytes();
        
        let mut iter = self.db.prefix_iterator_cf(&cf_handle, prefix_bytes);
        for item in iter {
            let (key, _) = item?;
            if let Ok(key_str) = std::str::from_utf8(&key) {
                if key.starts_with(prefix_bytes) {
                    keys.push(key_str.to_string());
                }
            }
        }
        
        Ok(keys)
    }
    
    fn count_in_cf(&self, prefix: &str, cf: &str) -> Result<usize> {
        let keys = self.list_keys_from_cf(prefix, cf)?;
        Ok(keys.len())
    }
    
    pub async fn write_wal(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.put_into_cf(key, value, CF_WAL)?;
        Ok(())
    }
    
    pub async fn commit_from_wal(&self, key: &str) -> Result<()> {
        if let Some(value) = self.get_from_cf(key, CF_WAL)? {
            self.put_into_cf(key, value.clone(), CF_CURRENT)?;
            
            let history_key = format!("{}_{}", key, chrono::Utc::now().timestamp_millis());
            self.put_into_cf(&history_key, value, CF_HISTORY)?;
            
            self.delete_from_cf(key, CF_WAL)?;
        }
        Ok(())
    }
    
    pub fn get_current(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.get_from_cf(key, CF_CURRENT)
    }
    
    pub fn get_history(&self, key_prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let cf_handle = self.db.cf_handle(CF_HISTORY)
            .ok_or_else(|| anyhow::anyhow!("Column family {} not found", CF_HISTORY))?;
        
        let mut results = Vec::new();
        let prefix_bytes = key_prefix.as_bytes();
        
        let mut iter = self.db.prefix_iterator_cf(&cf_handle, prefix_bytes);
        for item in iter {
            let (key, value) = item?;
            if let Ok(key_str) = std::str::from_utf8(&key) {
                if key_str.starts_with(key_prefix) {
                    results.push((key_str.to_string(), value.to_vec()));
                }
            }
        }
        
        Ok(results)
    }
    
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

#[async_trait::async_trait]
impl StorageBackend for RocksDBBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.get_current(key)
    }
    
    async fn put(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.write_wal(key, value.clone()).await?;
        self.put_into_cf(key, value, CF_CURRENT)?;
        self.delete_from_cf(key, CF_WAL)?;
        Ok(())
    }
    
    async fn delete(&self, key: &str) -> Result<()> {
        self.delete_from_cf(key, CF_CURRENT)?;
        Ok(())
    }
    
    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        self.list_keys_from_cf(prefix, CF_CURRENT)
    }
    
    async fn count(&self) -> Result<usize> {
        self.count_in_cf("", CF_CURRENT)
    }
    
    fn name(&self) -> &str {
        "RocksDBBackend"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    async fn create_test_backend() -> (RocksDBBackend, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let backend = RocksDBBackend::open(temp_dir.path()).unwrap();
        (backend, temp_dir)
    }

    #[tokio::test]
    async fn test_open_creates_column_families() {
        let temp_dir = TempDir::new().unwrap();
        let backend = RocksDBBackend::open(temp_dir.path()).unwrap();
        
        assert!(backend.db.cf_handle(CF_CURRENT).is_some());
        assert!(backend.db.cf_handle(CF_HISTORY).is_some());
        assert!(backend.db.cf_handle(CF_WAL).is_some());
    }

    #[tokio::test]
    async fn test_put_get_roundtrip() {
        let (backend, _temp_dir) = create_test_backend().await;
        
        backend.put("key1", b"value1".to_vec()).await.unwrap();
        let value = backend.get("key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_get_missing_returns_none() {
        let (backend, _temp_dir) = create_test_backend().await;
        let value = backend.get("missing").await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_delete_removes_key() {
        let (backend, _temp_dir) = create_test_backend().await;
        
        backend.put("key1", b"value1".to_vec()).await.unwrap();
        backend.delete("key1").await.unwrap();
        
        let value = backend.get("key1").await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_delete_missing_is_noop() {
        let (backend, _temp_dir) = create_test_backend().await;
        backend.delete("missing").await.unwrap();
    }

    #[tokio::test]
    async fn test_list_keys_prefix_filter() {
        let (backend, _temp_dir) = create_test_backend().await;
        
        backend.put("node:1", b"value1".to_vec()).await.unwrap();
        backend.put("node:2", b"value2".to_vec()).await.unwrap();
        backend.put("edge:1", b"value3".to_vec()).await.unwrap();
        
        let keys = backend.list_keys("node:").await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"node:1".to_string()));
        assert!(keys.contains(&"node:2".to_string()));
    }

    #[tokio::test]
    async fn test_count() {
        let (backend, _temp_dir) = create_test_backend().await;
        
        backend.put("node:1", b"value1".to_vec()).await.unwrap();
        backend.put("node:2", b"value2".to_vec()).await.unwrap();
        
        let count = backend.count().await.unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_wal_workflow() {
        let (backend, _temp_dir) = create_test_backend().await;
        
        backend.write_wal("key1", b"value1".to_vec()).await.unwrap();
        
        let current_value = backend.get_current("key1").unwrap();
        assert_eq!(current_value, None);
        
        let wal_value = backend.get_from_cf("key1", CF_WAL).unwrap();
        assert_eq!(wal_value, Some(b"value1".to_vec()));
        
        backend.commit_from_wal("key1").await.unwrap();
        
        let current_value = backend.get_current("key1").unwrap();
        assert_eq!(current_value, Some(b"value1".to_vec()));
        
        let history = backend.get_history("key1").unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].1, b"value1".to_vec());
        
        let wal_value = backend.get_from_cf("key1", CF_WAL).unwrap();
        assert_eq!(wal_value, None);
    }

    #[tokio::test]
    async fn test_concurrent_writes() {
        let (backend, _temp_dir) = create_test_backend().await;
        // RocksDB держит эксклюзивный lock на директорию — нельзя открыть один и тот же
        // путь несколькими инстансами. Настоящая конкурентность = один backend, общий Arc.
        let backend = Arc::new(backend);

        let mut handles = Vec::new();
        for i in 0..10 {
            let backend_clone = backend.clone();
            let handle = tokio::spawn(async move {
                let key = format!("key{}", i);
                let value = format!("value{}", i).into_bytes();
                backend_clone.put(&key, value).await.unwrap();
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.await.unwrap();
        }
        
        let count = backend.count().await.unwrap();
        assert_eq!(count, 10);
    }
}
