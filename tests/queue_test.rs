//! Queue integration tests

use std::path::PathBuf;
use tempfile::TempDir;

use graphmind_v2::queue::{PendingAction, PendingStatus, PendingStore, queue_path, ActionType};

fn tmp_store() -> (PendingStore, TempDir) {
    let tmp = TempDir::new().unwrap();
    let store = PendingStore::new(tmp.path().to_path_buf());
    (store, tmp)
}

#[tokio::test]
async fn test_empty_store_returns_empty_vec() {
    let (store, _tmp) = tmp_store();
    let all = store.read_all().await.unwrap();
    assert!(all.is_empty());
}

#[tokio::test]
async fn test_write_and_read_back() {
    let (store, _tmp) = tmp_store();

    let action = PendingAction::new_record("test summary", "test-source");
    store.write_all(&[action.clone()]).await.unwrap();

    let read = store.read_all().await.unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].summary, "test summary");
    assert_eq!(read[0].source, "test-source");
    assert_eq!(read[0].status, PendingStatus::Pending);
}

#[tokio::test]
async fn test_read_pending_filters_correctly() {
    let (store, _tmp) = tmp_store();

    let action = PendingAction::new_record("test", "src");
    store.write_all(&[action.clone()]).await.unwrap();

    let pending = store.read_pending().await.unwrap();
    assert_eq!(pending.len(), 1);

    store.mark_done(&action.id).await.unwrap();

    let pending_after = store.read_pending().await.unwrap();
    assert!(pending_after.is_empty());
}

#[tokio::test]
async fn test_mark_done_updates_status() {
    let (store, _tmp) = tmp_store();

    let action = PendingAction::new_record("test", "src");
    store.write_all(&[action.clone()]).await.unwrap();

    store.mark_done(&action.id).await.unwrap();

    let all = store.read_all().await.unwrap();
    assert_eq!(all[0].status, PendingStatus::Done);
}

#[tokio::test]
async fn test_cleanup_removes_done() {
    let (store, _tmp) = tmp_store();

    let action1 = PendingAction::new_record("test1", "src");
    store.write_all(&[action1.clone()]).await.unwrap();

    store.mark_done(&action1.id).await.unwrap();
    store.cleanup_processed().await.unwrap();

    let all = store.read_all().await.unwrap();
    assert!(all.is_empty());
}

#[tokio::test]
async fn test_propose_new_memory_has_extra_fields() {
    let (store, _tmp) = tmp_store();

    let action = PendingAction::new_propose(
        "fix for bug X",
        "## Problem\n...\n## Solution\n...",
        "L2",
        "atom",
        "L1_general",
        "workspace",
        "project:code-review",
    );

    store.write_all(&[action.clone()]).await.unwrap();

    let read = store.read_all().await.unwrap();
    assert_eq!(read[0].action_type, ActionType::ProposeNewMemory);
    assert_eq!(read[0].level.as_deref(), Some("L2"));
    assert_eq!(read[0].node_type.as_deref(), Some("atom"));
    assert_eq!(read[0].parent_id.as_deref(), Some("L1_general"));
    assert_eq!(read[0].scope.as_deref(), Some("workspace"));
}

#[tokio::test]
async fn test_queue_path_constructs_correctly() {
    let root = PathBuf::from("/workspace/test");
    let path = queue_path(&root);
    assert_eq!(path, PathBuf::from("/workspace/test/S0/pending_actions.json"));
}

#[tokio::test]
async fn test_multiple_actions_mark_one_done() {
    let (store, _tmp) = tmp_store();

    let action1 = PendingAction::new_record("action 1", "src");
    let action2 = PendingAction::new_record("action 2", "src");
    store.write_all(&[action1.clone(), action2.clone()]).await.unwrap();

    store.mark_done(&action1.id).await.unwrap();

    let pending = store.read_pending().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].summary, "action 2");
}

#[tokio::test]
async fn test_new_record_action_type() {
    let action = PendingAction::new_record("summary", "source");
    assert_eq!(action.action_type, ActionType::RecordAction);
    assert_eq!(action.status, PendingStatus::Pending);
    assert!(action.raw_text.is_none());
    assert!(action.level.is_none());
}

#[tokio::test]
async fn test_new_propose_action_type() {
    let action = PendingAction::new_propose(
        "summary", "content", "L2", "atom", "L1_x", "workspace", "src",
    );
    assert_eq!(action.action_type, ActionType::ProposeNewMemory);
    assert!(action.content.is_some());
    assert!(action.parent_id.is_some());
}

#[tokio::test]
async fn test_id_is_uuid() {
    let action = PendingAction::new_record("s", "src");
    assert!(uuid::Uuid::parse_str(&action.id).is_ok());
}
