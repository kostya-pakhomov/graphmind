//! Интеграционные тесты причинного слоя НА ДАННЫХ (не `*_empty`).
//!
//! Главное — регрессия на фикс `L2Actor::edges_from`: смежность рёбер держалась в
//! per-instance `edge_index`, из-за чего разные L2Actor над одним backend (ChainActor /
//! InferenceActor держат СВОЙ) не видели рёбра, добавленные через L2Actor хендлера →
//! `get_chain`/`predict_risks` возвращали пусто на живых данных. Теперь `edges_from`
//! читает рёбра из backend, и обход причинных цепочек работает между инстансами.

use std::sync::Arc;

use graphmind_v2::actors::{ChainActor, InferenceActor, L2Actor, RiskLevel};
use graphmind_v2::graph::{Edge, Node, NodeType, Relation};
use graphmind_v2::persistence::InMemoryBackend;

/// Cause → (leads_to) → Effect, ребро добавлено через ОДИН L2Actor, а обход идёт через
/// InferenceActor со СВОИМ L2Actor над тем же backend. Должен найти эффект.
#[tokio::test]
async fn predict_risks_traverses_edge_across_l2_instances() {
    let backend = Arc::new(InMemoryBackend::new());

    // Writer — как l2 хендлера: сюда пишем узлы и ребро.
    let l2_writer = L2Actor::new(backend.clone());
    let cause = Node::new(NodeType::Cause, "Развёрнут кэш без прогрева");
    let effect = Node::new(NodeType::Effect, "Всплеск ошибок таймаута на старте");
    let cid = cause.id.clone();
    let eid = effect.id.clone();
    l2_writer.add_node(&cause).await.unwrap();
    l2_writer.add_node(&effect).await.unwrap();
    l2_writer
        .add_edge(&Edge::new(cid.clone(), eid.clone(), Relation::LeadsTo))
        .await
        .unwrap();

    // InferenceActor на СВОЁМ L2Actor над тем же backend (как в main.rs).
    let inference = InferenceActor::new(Arc::new(L2Actor::new(backend.clone())));
    let pred = inference.predict_risks(&cid).await.unwrap();

    assert!(
        !pred.predicted_effects.is_empty(),
        "predict_risks должен пройти по ребру, добавленному через другой L2Actor (регрессия edges_from)"
    );
    assert!(
        pred.predicted_effects
            .iter()
            .any(|e| e.description.contains("таймаута")),
        "среди эффектов должен быть связанный по причинной цепочке"
    );
    // Уровень риска — валидный вариант перечисления.
    assert!(matches!(
        pred.risk_level,
        RiskLevel::Low | RiskLevel::Medium | RiskLevel::High | RiskLevel::Critical
    ));
}

/// ChainActor.forward_pre между инстансами L2Actor: цепочка cause→effect непуста.
#[tokio::test]
async fn chain_forward_pre_non_empty_across_instances() {
    let backend = Arc::new(InMemoryBackend::new());

    let l2_writer = L2Actor::new(backend.clone());
    let cause = Node::new(NodeType::Cause, "Отключили индексы для ускорения записи");
    let effect = Node::new(NodeType::Effect, "Деградация скорости чтения");
    let cid = cause.id.clone();
    let eid = effect.id.clone();
    l2_writer.add_node(&cause).await.unwrap();
    l2_writer.add_node(&effect).await.unwrap();
    l2_writer
        .add_edge(&Edge::new(cid.clone(), eid.clone(), Relation::LeadsTo))
        .await
        .unwrap();

    // Отдельный ChainActor со своим L2Actor над тем же backend.
    let chain = ChainActor::new(Arc::new(L2Actor::new(backend.clone())));
    let result = chain.chain_forward_pre(&cid, 3).await.unwrap();

    assert!(
        !result.entries.is_empty(),
        "forward_pre должен обойти ребро между инстансами L2Actor (регрессия edges_from)"
    );
}
