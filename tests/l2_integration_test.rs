//! Integration tests: PendingStore -> QueueProcessor -> L2Actor (and S0 in parallel).

use std::sync::Arc;

use graphmind_v2::actors::{Actor, L2Actor, S0Actor};
use graphmind_v2::persistence::InMemoryBackend;
use graphmind_v2::queue::{PendingAction, PendingStore, QueueProcessor};
use tempfile::TempDir;

fn setup() -> (QueueProcessor, Arc<S0Actor>, Arc<L2Actor>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let store = PendingStore::new(tmp.path().to_path_buf());
    let s0 = Arc::new(S0Actor::new());
    let backend = Arc::new(InMemoryBackend::new());
    let l2 = Arc::new(L2Actor::new(backend));
    let processor = QueueProcessor::new(store, 60)
        .with_s0(s0.clone())
        .with_l2(l2.clone());
    (processor, s0, l2, tmp)
}

#[tokio::test]
async fn test_propose_new_memory_lands_in_l2() {
    let (processor, s0, l2, _tmp) = setup();

    let action = PendingAction::new_propose(
        "fix for bug X",
        "## Problem\n...",
        "L2",
        "atom",
        "L1_general",
        "workspace",
        "project:code-review",
    );
    processor.store().write_all(&[action]).await.unwrap();

    let stats = processor.process_once().await.unwrap();
    assert_eq!(stats.done, 1);
    assert_eq!(stats.failed, 0);

    // L2 has the new node
    let atoms = l2.list_by_type(graphmind_v2::graph::NodeType::Atom).await.unwrap();
    assert_eq!(atoms.len(), 1);
    assert!(atoms[0].content.contains("Problem"));

    // S0 did NOT receive this (it goes to L2 only)
    assert_eq!(s0.size().await, 0);
}

#[tokio::test]
async fn test_record_action_still_lands_in_s0() {
    let (processor, s0, l2, _tmp) = setup();

    let action = PendingAction::new_record("user did X", "test");
    processor.store().write_all(&[action]).await.unwrap();

    processor.process_once().await.unwrap();

    assert_eq!(s0.size().await, 1);
    assert_eq!(l2.node_count().await.unwrap(), 0);
}

#[tokio::test]
async fn test_mixed_actions_go_to_correct_actors() {
    let (processor, s0, l2, _tmp) = setup();

    let actions = vec![
        PendingAction::new_record("session note 1", "test"),
        PendingAction::new_propose("summary A", "content A", "L2", "atom", "L1_general", "workspace", "src"),
        PendingAction::new_record("session note 2", "test"),
        PendingAction::new_propose("summary B", "content B", "L2", "cause", "L1_general", "workspace", "src"),
    ];
    processor.store().write_all(&actions).await.unwrap();

    let stats = processor.process_once().await.unwrap();
    assert_eq!(stats.done, 4);

    assert_eq!(s0.size().await, 2);
    assert_eq!(l2.node_count().await.unwrap(), 2);
    assert_eq!(l2.list_by_type(graphmind_v2::graph::NodeType::Atom).await.unwrap().len(), 1);
    assert_eq!(l2.list_by_type(graphmind_v2::graph::NodeType::Cause).await.unwrap().len(), 1);
}

#[tokio::test]
async fn test_propose_idempotent_on_same_content() {
    // Same parent_id + content should map to the same NodeId, so the
    // second write overwrites the first instead of creating a duplicate.
    let (processor, _s0, l2, _tmp) = setup();

    let a1 = PendingAction::new_propose("fix", "same content", "L2", "atom", "L1_general", "workspace", "src");
    let a2 = PendingAction::new_propose("fix", "same content", "L2", "atom", "L1_general", "workspace", "src");
    processor.store().write_all(&[a1, a2]).await.unwrap();

    processor.process_once().await.unwrap();

    let atoms = l2.list_by_type(graphmind_v2::graph::NodeType::Atom).await.unwrap();
    assert_eq!(atoms.len(), 1, "duplicate propose must collapse to one node");
}

#[tokio::test]
async fn test_propose_with_missing_fields_fails_cleanly() {
    // Manually craft a broken propose action: missing parent_id.
    let (processor, s0, l2, _tmp) = setup();

    let mut action = PendingAction::new_propose(
        "summary", "content", "L2", "atom", "L1_x", "workspace", "src",
    );
    action.parent_id = None; // simulate malformed input
    processor.store().write_all(&[action]).await.unwrap();

    let stats = processor.process_once().await.unwrap();
    assert_eq!(stats.done, 0);
    assert_eq!(stats.failed, 1);
    assert_eq!(l2.node_count().await.unwrap(), 0);
    assert_eq!(s0.size().await, 0);
}

#[tokio::test]
async fn test_propose_with_unknown_node_type_fails() {
    let (processor, _s0, l2, _tmp) = setup();

    let action = PendingAction::new_propose(
        "summary", "content", "L2", "totally_bogus", "L1_x", "workspace", "src",
    );
    processor.store().write_all(&[action]).await.unwrap();

    let stats = processor.process_once().await.unwrap();
    assert_eq!(stats.failed, 1);
    assert_eq!(l2.node_count().await.unwrap(), 0);
}

#[tokio::test]
async fn test_processor_without_l2_keeps_propose_as_noop() {
    let tmp = TempDir::new().unwrap();
    let store = PendingStore::new(tmp.path().to_path_buf());
    let s0 = Arc::new(S0Actor::new());
    let processor = QueueProcessor::new(store, 60).with_s0(s0.clone());

    let action = PendingAction::new_propose(
        "summary", "content", "L2", "atom", "L1_x", "workspace", "src",
    );
    processor.store().write_all(&[action]).await.unwrap();

    let stats = processor.process_once().await.unwrap();
    assert_eq!(stats.done, 1); // processed but discarded
    assert_eq!(s0.size().await, 0);
}

#[tokio::test]
async fn test_l2_persists_across_actor_instances_via_backend() {
    // The same backend shared between two L2Actor instances should expose
    // nodes written by the first one to the second one.
    let backend = Arc::new(InMemoryBackend::new());
    let l2_a = Arc::new(L2Actor::new(backend.clone()));
    let l2_b = Arc::new(L2Actor::new(backend.clone()));

    let mut n = graphmind_v2::graph::Node::new(
        graphmind_v2::graph::NodeType::Atom,
        "shared atom",
    );
    n.metadata.workspace_id = Some("ws_shared".to_string());
    l2_a.add_node(&n).await.unwrap();

    let from_b = l2_b.list_by_parent("ws_shared").await.unwrap();
    assert_eq!(from_b.len(), 1);
    assert_eq!(from_b[0].content, "shared atom");
}
