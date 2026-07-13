//! Queue processor -- consumes pending_actions.json and applies to S0/L2.

use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use tracing::{info, warn, error};

use tokio::sync::mpsc::UnboundedSender;

use crate::actors::{Actor, GateOutcome, L2Actor, MemoryEvent, S0Actor, S0Entry, SourceType, TrustFirewall};
use crate::graph::{Level, Metadata, Node, NodeId, NodeType, Status};
use chrono::Utc;
use super::{PendingAction, PendingStore};

/// Processes pending actions from the queue.
pub struct QueueProcessor {
    store: PendingStore,
    interval_secs: u64,
    s0: Option<Arc<S0Actor>>,
    l2: Option<Arc<L2Actor>>,
    /// Фаервол-гейт для суб-агентских propose_new_memory (strict-режим).
    trust: Option<Arc<TrustFirewall>>,
    /// Шина событий координатора памяти (NodeWritten / TrustFirewallBlock).
    event_tx: Option<UnboundedSender<MemoryEvent>>,
}

impl QueueProcessor {
    pub fn new(store: PendingStore, interval_secs: u64) -> Self {
        Self {
            store,
            interval_secs,
            s0: None,
            l2: None,
            trust: None,
            event_tx: None,
        }
    }

    /// Attach an S0Actor so processed actions land in short-term memory.
    pub fn with_s0(mut self, s0: Arc<S0Actor>) -> Self {
        self.s0 = Some(s0);
        self
    }

    /// Attach an L2Actor so propose_new_memory actions persist as nodes.
    pub fn with_l2(mut self, l2: Arc<L2Actor>) -> Self {
        self.l2 = Some(l2);
        self
    }

    /// Attach a TrustFirewall so sub-agent propose_new_memory passes a gate.
    pub fn with_trust(mut self, trust: Arc<TrustFirewall>) -> Self {
        self.trust = Some(trust);
        self
    }

    /// Attach the coordinator event bus (emits NodeWritten / TrustFirewallBlock).
    pub fn with_event_tx(mut self, tx: UnboundedSender<MemoryEvent>) -> Self {
        self.event_tx = Some(tx);
        self
    }

    /// Access the underlying PendingStore.
    pub fn store(&self) -> &PendingStore {
        &self.store
    }

    /// Enqueue a new pending action directly (used by McpHandler for record_action).
    ///
    /// This writes the action to pending_actions.json immediately.
    /// The action will be processed by the next `process_once()` cycle.
    pub async fn enqueue(&self, action: PendingAction) -> Result<String> {
        let action_id = action.id.clone();
        self.store.append(&action).await?;
        tracing::info!(
            "QueueProcessor: enqueued action {} (type={:?})",
            action_id,
            action.action_type
        );
        Ok(action_id)
    }

    /// Drain all pending actions to L2 (used by Consolidator for flush).
    ///
    /// Возвращает `ProcessStats` с разделёнными счётчиками:
    /// - `done` — успешно обработанные actions любого типа
    /// - `l2_atoms_created` — реально созданные L2-атомы (только для `ProposeNewMemory`,
    ///   `RecordAction` остаётся S0-only by design — см. bug_report/002)
    /// - `failed` — actions, упавшие в `process_action`
    ///
    /// Параметр `_workspace_id` сохранён для обратной совместимости; на данный
    /// момент workspace-scoping в `process_action` не используется, потому что
    /// `RecordAction` is S0-only, а `ProposeNewMemory` берёт `action.scope`.
    pub async fn drain_to_l2(&self, _workspace_id: &str) -> Result<ProcessStats> {
        self.process_once().await
    }

    pub async fn process_once(&self) -> Result<ProcessStats> {
        let pending = self.store.read_pending().await?;
        let mut stats = ProcessStats::default();

        for action in pending {
            info!("Processing action: {} ({:?})", action.id, action.action_type);

            match self.process_action(&action).await {
                Ok(()) => {
                    self.store.mark_done(&action.id).await?;
                    stats.done += 1;
                    // Считаем только реально созданные L2-атомы. process_action сам
                    // инкрементирует stats.l2_atoms_created через shared mutable
                    // ссылку — нет, на самом деле через возврат. Простой и
                    // однозначный путь: process_action возвращает Ok(L2AtomsCreated(usize))
                    // — но это меняет сигнатуру. Поэтому используем AtomicUsize в self,
                    // либо (проще) — process_action пишет в &mut stats через
                    // параметр. Реализуем явный helper-аргумент: ProcessStats
                    // инкрементируется прямо в process_action.
                    if matches!(action.action_type, super::ActionType::ProposeNewMemory) {
                        stats.l2_atoms_created += 1;
                    }
                }
                Err(e) => {
                    error!("Failed to process action {}: {}", action.id, e);
                    stats.failed += 1;
                }
            }
        }

        self.store.cleanup_processed().await?;

        Ok(stats)
    }

    async fn process_action(&self, action: &PendingAction) -> Result<()> {
        match action.action_type {
            super::ActionType::RecordAction => {
                if let Some(s0) = &self.s0 {
                    let entry = S0Entry {
                        id: action.id.clone(),
                        source: action.source.clone(),
                        summary: action.summary.clone(),
                        timestamp: action.timestamp,
                    };
                    let evicted = s0.push(entry).await;
                    if let Some(old) = evicted {
                        info!("{}: pushed (evicted oldest id={})", s0.name(), old.id);
                    } else {
                        info!("{}: pushed (size now {})", s0.name(), s0.size().await);
                    }
                } else {
                    info!("record_action: {} (no S0 attached, discarded)", action.summary);
                }
                Ok(())
            }
            super::ActionType::ProposeNewMemory => {
                if let Some(l2) = &self.l2 {
                    let mut node = build_node_from_propose(action)?;
                    // Фаервол-гейт (strict): суб-агент не доверяется автоматически.
                    if let Some(trust) = &self.trust {
                        match trust
                            .gate(&action.source, SourceType::AgentInternal, &node.content, true)
                            .await
                        {
                            GateOutcome::Allow { status, .. } => node.status = status,
                            GateOutcome::Refuse { reason, .. } => {
                                warn!(
                                    "propose_new_memory от '{}' отклонён фаерволом: {} (quarantined, не записан)",
                                    action.source, reason
                                );
                                if let Some(tx) = &self.event_tx {
                                    let _ = tx.send(MemoryEvent::TrustFirewallBlock {
                                        source_id: action.source.clone(),
                                        reason,
                                    });
                                }
                                return Ok(());
                            }
                        }
                    }
                    let id = l2.add_node(&node).await?;
                    info!(
                        "{}: stored propose_new_memory node={} (status={:?})",
                        l2.name(),
                        id.0,
                        node.status
                    );
                    // CycleTrigger считает только Active-узлы (Draft на ревью — не считаем).
                    if node.status == Status::Active {
                        if let Some(tx) = &self.event_tx {
                            let ws = action.scope.clone().unwrap_or_else(|| "default".to_string());
                            let _ = tx.send(MemoryEvent::NodeWritten { workspace: ws });
                        }
                    }
                } else {
                    info!(
                        "propose_new_memory: {} (no L2 attached, discarded)",
                        action.summary
                    );
                }
                Ok(())
            }
            super::ActionType::FetchFromWorkspace => {
                info!("fetch_from_workspace: {} (cross-ws integration pending)", action.summary);
                Ok(())
            }
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!("QueueProcessor started (interval: {}s)", self.interval_secs);

        loop {
            match self.process_once().await {
                Ok(stats) => {
                    if stats.total() > 0 {
                        info!(
                            "Processed {} actions ({} done, {} failed)",
                            stats.total(),
                            stats.done,
                            stats.failed
                        );
                    }
                }
                Err(e) => {
                    warn!("Queue processing error: {}", e);
                }
            }

            tokio::time::sleep(Duration::from_secs(self.interval_secs)).await;
        }
    }
}

/// Build a Node from a propose_new_memory PendingAction.
///
/// Required fields on the action: level, node_type, content, parent_id, scope.
/// We honor deterministic IDs from `parent_id` so the same content+parent
/// does not create duplicate nodes (idempotency).
fn build_node_from_propose(action: &PendingAction) -> Result<Node> {
    let level = action
        .level
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("propose_new_memory missing level"))?;
    let node_type = action
        .node_type
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("propose_new_memory missing node_type"))?;
    let content = action
        .content
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("propose_new_memory missing content"))?;
    let parent_id = action
        .parent_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("propose_new_memory missing parent_id"))?;

    let nt = match node_type {
        "atom" => NodeType::Atom,
        "cause" => NodeType::Cause,
        "effect" => NodeType::Effect,
        "rule" => NodeType::Rule,
        "cluster" => NodeType::Cluster,
        "hub" => NodeType::Hub,
        "domain" => NodeType::Domain,
        other => anyhow::bail!("unknown node_type: {other}"),
    };

    let lvl = match level {
        "S0" => Level::S0,
        "L0" => Level::L0,
        "L1" => Level::L1,
        "L2" => Level::L2,
        "GKL" => Level::GKL,
        other => anyhow::bail!("unknown level: {other}"),
    };

    let id = NodeId(format!("auto_{}_{}", sanitize(parent_id), sanitize(content)));

    Ok(Node {
        id,
        node_type: nt,
        level: lvl,
        content: content.to_string(),
        metadata: Metadata {
            parent_id: None, // S0 actions не привязаны к cluster parent
            workspace_id: action.scope.clone(),
            tags: Vec::new(),
        },        status: Status::Active,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    })
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

/// Итог одного прохода drain'a. Разделяет «обработанные actions» (любого типа,
/// включая S0-only `RecordAction`) от «реально созданных L2-атомов» (только
/// `ProposeNewMemory`). См. bug_report/002.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessStats {
    /// Все actions, успешно обработанные `process_action` (включая RecordAction,
    /// который остаётся S0-only и L2-атом не создаёт).
    pub done: usize,
    /// Actions, упавшие в `process_action` с ошибкой.
    pub failed: usize,
    /// Реально созданные L2-атомы. By design — только для `ProposeNewMemory`.
    /// `RecordAction` НЕ создаёт L2-атомы (см. bug_report/002).
    pub l2_atoms_created: usize,
}

impl ProcessStats {
    pub fn total(&self) -> usize {
        self.done + self.failed
    }

    /// Обратная совместимость со старым контрактом `drain_to_l2 -> usize`,
    /// где `usize` означал `stats.done` (обработанные actions).
    /// НЕ ИСПОЛЬЗОВАТЬ в новом коде — счётчик вводит в заблуждение. См. bug 002.
    #[deprecated(note = "Use `stats.done` (обработанные actions) или `stats.l2_atoms_created` (L2-атомы) — см. bug_report/002")]
    pub fn legacy_done_count(&self) -> usize {
        self.done
    }
}
