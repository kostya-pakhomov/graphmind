//! Pending actions store — read/write pending_actions.json with file locking

use std::path::PathBuf;
use tokio::fs;
use anyhow::Result;

use super::{PendingAction, PendingStatus, queue_path};

/// File-backed store for pending actions
#[derive(Debug, Clone)]
pub struct PendingStore {
    path: PathBuf,
}

impl PendingStore {
    pub fn new(workspace_root: PathBuf) -> Self {
        Self {
            path: queue_path(&workspace_root),
        }
    }

    /// Read all pending actions from file
    pub async fn read_all(&self) -> Result<Vec<PendingAction>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let content = fs::read_to_string(&self.path).await?;
        let actions: Vec<PendingAction> = serde_json::from_str(&content)?;
        Ok(actions)
    }

    /// Write all actions back to file (atomic via temp file)
    pub async fn write_all(&self, actions: &[PendingAction]) -> Result<()> {
        // Ensure parent dir exists
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let json = serde_json::to_string_pretty(actions)?;
        let temp_path = self.path.with_extension("json.tmp");

        fs::write(&temp_path, json.as_bytes()).await?;
        std::fs::rename(&temp_path, &self.path)?;

        Ok(())
    }

    /// Append a single action (creates file if needed)
    pub async fn append(&self, action: &PendingAction) -> Result<()> {
        let mut actions = self.read_all().await?;
        actions.push(action.clone());
        self.write_all(&actions).await
    }

    /// Get only pending (not yet processed) actions
    pub async fn read_pending(&self) -> Result<Vec<PendingAction>> {
        let all = self.read_all().await?;
        Ok(all.into_iter().filter(|a| a.status == PendingStatus::Pending).collect())
    }

    /// Mark an action as processed
    pub async fn mark_done(&self, action_id: &str) -> Result<()> {
        let mut actions = self.read_all().await?;
        for action in &mut actions {
            if action.id == action_id {
                action.status = PendingStatus::Done;
                break;
            }
        }
        self.write_all(&actions).await
    }

    /// Remove processed actions (cleanup)
    pub async fn cleanup_processed(&self) -> Result<()> {
        let actions = self.read_all().await?;
        let remaining: Vec<_> = actions.into_iter().filter(|a| a.status != PendingStatus::Done).collect();
        self.write_all(&remaining).await
    }
}
