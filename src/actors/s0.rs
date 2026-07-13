//! S0Actor — short-term memory (ephemeral ring buffer)
//!
//! Based on TECH-SPEC.md §4.1 S0: ~20 most recent actions, FIFO eviction.
//! In-memory only — no persistence. Cleared on restart.

use std::collections::VecDeque;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use super::Actor;

/// Default capacity for S0 ring buffer (TECH-SPEC.md §4.1).
pub const S0_CAPACITY: usize = 20;

/// A single short-term memory entry.
///
/// Mirrors the fields we care about from `queue::PendingAction` but is
/// owned by S0 — once an action lands here, the queue entry can be marked
/// Done and eventually cleaned up.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S0Entry {
    pub id: String,
    pub source: String,
    pub summary: String,
    pub timestamp: DateTime<Utc>,
}

/// S0Actor — bounded FIFO ring buffer of recent actions.
///
/// Thread-safe via `tokio::sync::Mutex`. When capacity is exceeded, the
/// oldest entry is evicted (FIFO).
pub struct S0Actor {
    capacity: usize,
    inner: Mutex<VecDeque<S0Entry>>,
}

impl S0Actor {
    /// Create a new S0Actor with the default capacity (20).
    pub fn new() -> Self {
        Self::with_capacity(S0_CAPACITY)
    }

    /// Create a new S0Actor with a custom capacity (useful for tests).
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "S0 capacity must be > 0");
        Self {
            capacity,
            inner: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    /// Push a new entry. If the buffer is full, the oldest entry is evicted.
    /// Returns the evicted entry, if any.
    pub async fn push(&self, entry: S0Entry) -> Option<S0Entry> {
        let mut guard = self.inner.lock().await;
        let evicted = if guard.len() >= self.capacity {
            guard.pop_front()
        } else {
            None
        };
        guard.push_back(entry);
        evicted
    }

    /// Get the most recent N entries (newest first).
    pub async fn get_recent(&self, n: usize) -> Vec<S0Entry> {
        let guard = self.inner.lock().await;
        guard.iter().rev().take(n).cloned().collect()
    }

    /// Get all entries in insertion order (oldest first).
    pub async fn get_all(&self) -> Vec<S0Entry> {
        let guard = self.inner.lock().await;
        guard.iter().cloned().collect()
    }

    /// Clear all entries.
    pub async fn clear(&self) {
        let mut guard = self.inner.lock().await;
        guard.clear();
    }

    /// Current number of entries.
    pub async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }

    /// Configured capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Default for S0Actor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Actor for S0Actor {
    fn name(&self) -> &str {
        "S0Actor"
    }

    async fn size(&self) -> usize {
        self.len().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, summary: &str) -> S0Entry {
        S0Entry {
            id: id.to_string(),
            source: "test".to_string(),
            summary: summary.to_string(),
            timestamp: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_push_and_size() {
        let actor = S0Actor::with_capacity(3);
        assert_eq!(actor.size().await, 0);

        actor.push(entry("a", "first")).await;
        actor.push(entry("b", "second")).await;

        assert_eq!(actor.size().await, 2);
    }

    #[tokio::test]
    async fn test_fifo_eviction_when_full() {
        let actor = S0Actor::with_capacity(2);

        actor.push(entry("a", "first")).await;
        actor.push(entry("b", "second")).await;
        let evicted = actor.push(entry("c", "third")).await;

        assert!(evicted.is_some());
        assert_eq!(evicted.unwrap().id, "a");
        assert_eq!(actor.size().await, 2);

        let all = actor.get_all().await;
        assert_eq!(all[0].id, "b");
        assert_eq!(all[1].id, "c");
    }

    #[tokio::test]
    async fn test_get_recent_newest_first() {
        let actor = S0Actor::with_capacity(5);
        actor.push(entry("a", "1")).await;
        actor.push(entry("b", "2")).await;
        actor.push(entry("c", "3")).await;

        let recent = actor.get_recent(2).await;
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].id, "c");
        assert_eq!(recent[1].id, "b");
    }

    #[tokio::test]
    async fn test_clear() {
        let actor = S0Actor::with_capacity(3);
        actor.push(entry("a", "1")).await;
        actor.push(entry("b", "2")).await;

        actor.clear().await;
        assert_eq!(actor.size().await, 0);
        assert!(actor.get_all().await.is_empty());
    }

    #[tokio::test]
    async fn test_default_capacity_is_20() {
        let actor = S0Actor::new();
        assert_eq!(actor.capacity(), 20);
    }

    #[tokio::test]
    async fn test_concurrent_push() {
        use std::sync::Arc;
        let actor = Arc::new(S0Actor::with_capacity(100));

        let mut handles = Vec::new();
        for i in 0..50 {
            let a = actor.clone();
            handles.push(tokio::spawn(async move {
                a.push(entry(&format!("id-{i}"), "summary")).await;
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(actor.size().await, 50);
    }
}
