//! File-based persistence backend — stores data as JSON files on disk.

use crate::persistence::StorageBackend;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

pub struct FileBackend {
    root: PathBuf,
    cache: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl FileBackend {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        if !root.exists() {
            fs::create_dir_all(&root)?;
            info!("FileBackend: created directory {}", root.display());
        }
        let cache = Self::load_from_disk(&root)?;
        info!("FileBackend: loaded {} keys from {}", cache.len(), root.display());
        Ok(Self { root, cache: Arc::new(RwLock::new(cache)) })
    }
    
    fn load_from_disk(root: &Path) -> Result<HashMap<String, Vec<u8>>> {
        let mut cache = HashMap::new();
        if root.exists() {
            for entry in walkdir::WalkDir::new(root) {
                let entry = entry?;
                if entry.file_type().is_file() {
                    let path = entry.path();
                    if let Some(key) = Self::path_to_key(path, root) {
                        if let Ok(contents) = fs::read(path) {
                            cache.insert(key, contents);
                        }
                    }
                }
            }
        }
        Ok(cache)
    }
    
    fn path_to_key(path: &Path, root: &Path) -> Option<String> {
        path.strip_prefix(root).ok().and_then(|rel| {
            let key = rel.to_str()?.replace(std::path::MAIN_SEPARATOR, ":").trim_end_matches(".json").to_string();
            Some(key)
        })
    }
    
    fn key_to_path(&self, key: &str) -> PathBuf {
        let relative = key.replace(":", std::path::MAIN_SEPARATOR_STR);
        self.root.join(format!("{}.json", relative))
    }
    
    fn ensure_parent_dir(&self, key: &str) -> Result<()> {
        let path = self.key_to_path(key);
        if let Some(parent) = path.parent() {
            if !parent.exists() { fs::create_dir_all(parent)?; }
        }
        Ok(())
    }
}

#[async_trait]
impl StorageBackend for FileBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.cache.read().await.get(key).cloned())
    }

    async fn put(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.cache.write().await.insert(key.to_string(), value.clone());
        self.ensure_parent_dir(key)?;
        fs::write(self.key_to_path(key), &value)?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.cache.write().await.remove(key);
        let path = self.key_to_path(key);
        if path.exists() { fs::remove_file(&path)?; }
        Ok(())
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let cache = self.cache.read().await;
        let mut keys: Vec<String> = cache.keys().filter(|k| k.starts_with(prefix)).cloned().collect();
        keys.sort();
        Ok(keys)
    }

    async fn count(&self) -> Result<usize> {
        Ok(self.cache.read().await.len())
    }

    fn name(&self) -> &str { "FileBackend" }
}