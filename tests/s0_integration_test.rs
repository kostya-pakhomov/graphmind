//! Integration tests: PendingStore -> QueueProcessor -> S0Actor
//!
//! End-to-end: write RecordAction to pending_actions.json,
//! run process_once, assert S0Actor contains the entry.

use std::sync::Arc;

use graphmind_v2::actors::{Actor, S0Actor};
use graphmind_v2::queue::{PendingAction, PendingStatus, PendingStore, QueueProcessor};
use tempfile::TempDir;

fn tmp_store() -> (PendingStore, TempDir) {
    let tmp = TempDir::new().unwrap();
    let store = PendingStore::new(tmp.path().to_path_buf());
    (store, tmp)
}

#[tokio::test]
async fn test_record_action_lands_in_s0() {
    let (store, _tmp) = tmp_store();
    let s0 = Arc::new(S0Actor::with_capacity(10));
    let processor = QueueProcessor::new(store, 60).with_s0(s0.clone());

    let action = PendingAction::new_record("integration test", "test-source");
    let action_id = action.id.clone();
    processor
        .store()
        .write_all(&[action])
        .await
        .unwrap();

    let stats = processor.process_once().await.unwrap();
    assert_eq!(stats.done, 1);
    assert_eq!(stats.failed, 0);

    let recent = s0.get_recent(10).await;
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].id, action_id);
    assert_eq!(recent[0].summary, "integration test");
    assert_eq!(recent[0].source, "test-source");
}

#[tokio::test]
async fn test_processed_action_marked_done_in_queue() {
    let (store, _tmp) = tmp_store();
    let s0 = Arc::new(S0Actor::new());
    let processor = QueueProcessor::new(store, 60).with_s0(s0.clone());

    let action = PendingAction::new_record("to be done", "src");
    let action_id = action.id.clone();
    processor
        .store()
        .write_all(&[action])
        .await
        .unwrap();

    processor.process_once().await.unwrap();

    // After processing, the queue file should be cleaned up (only non-Done remain).
    let remaining = processor.store().read_all().await.unwrap();
    assert!(
        remaining.iter().all(|a| a.status != PendingStatus::Done),
        "Done actions should be cleaned up, got: {:?}",
        remaining.iter().map(|a| &a.status).collect::<Vec<_>>()
    );
    // Sanity: S0 still has the entry.
    let recent = s0.get_recent(10).await;
    assert!(recent.iter().any(|e| e.id == action_id));
}

#[tokio::test]
async fn test_multiple_actions_all_land_in_s0() {
    let (store, _tmp) = tmp_store();
    let s0 = Arc::new(S0Actor::with_capacity(20));
    let processor = QueueProcessor::new(store, 60).with_s0(s0.clone());

    let actions: Vec<PendingAction> = (0..5)
        .map(|i| PendingAction::new_record(format!("action-{i}"), "src"))
        .collect();
    processor.store().write_all(&actions).await.unwrap();

    let stats = processor.process_once().await.unwrap();
    assert_eq!(stats.done, 5);

    let recent = s0.get_recent(10).await;
    assert_eq!(recent.len(), 5);
    // Newest first, so order is reversed.
    assert_eq!(recent[0].summary, "action-4");
    assert_eq!(recent[4].summary, "action-0");
}

#[tokio::test]
async fn test_s0_evicts_oldest_when_full_via_queue() {
    let (store, _tmp) = tmp_store();
    let s0 = Arc::new(S0Actor::with_capacity(2));
    let processor = QueueProcessor::new(store, 60).with_s0(s0.clone());

    let actions: Vec<PendingAction> = (0..3)
        .map(|i| PendingAction::new_record(format!("a-{i}"), "src"))
        .collect();
    processor.store().write_all(&actions).await.unwrap();

    processor.process_once().await.unwrap();

    let all = s0.get_all().await;
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].summary, "a-1");
    assert_eq!(all[1].summary, "a-2");
}

#[tokio::test]
async fn test_propose_action_does_not_land_in_s0() {
    // propose_new_memory still goes to L2 (not yet implemented) — must NOT pollute S0.
    let (store, _tmp) = tmp_store();
    let s0 = Arc::new(S0Actor::new());
    let processor = QueueProcessor::new(store, 60).with_s0(s0.clone());

    let action = PendingAction::new_propose(
        "summary", "content", "L2", "atom", "L1_general", "workspace", "src",
    );
    processor.store().write_all(&[action]).await.unwrap();

    let stats = processor.process_once().await.unwrap();
    assert_eq!(stats.done, 1);
    assert_eq!(s0.size().await, 0);
}

#[tokio::test]
async fn test_processor_without_s0_still_works() {
    // Backward compat: processor without attached S0 must not crash.
    let (store, _tmp) = tmp_store();
    let processor = QueueProcessor::new(store, 60); // no S0

    let action = PendingAction::new_record("discarded", "src");
    processor.store().write_all(&[action]).await.unwrap();

    let stats = processor.process_once().await.unwrap();
    assert_eq!(stats.done, 1);
}
