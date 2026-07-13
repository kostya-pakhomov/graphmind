//! ConsolidateRunner — переиспользуемая последовательность консолидации workspace.
//!
//! Раньше вся логика жила внутри `McpHandler::consolidate_workspace`. Теперь
//! вынесена сюда, чтобы её звали и MCP-инструмент, и координатор памяти
//! (`MemoryOrchestrator`) при срабатывании CycleTrigger — без дублирования.
//!
//! Шаги: drain очереди → сбор L2-атомов и рёбер → L1 autogen → L0 autogen.

use std::sync::Arc;
use anyhow::Result;
use tokio::sync::RwLock;

use super::{L0Actor, L1Actor, L2Actor};
use crate::graph::{Edge, Node};
use crate::queue::QueueProcessor;

/// Итог одного прохода консолидации.
///
/// Поля разделены на "что произошло за этот прогон" (created) и
/// "что накопилось в workspace" (total). См. bug_report/002.
#[derive(Debug, Clone, Default)]
pub struct ConsolidateStats {
    /// Сколько actions обработано QueueProcessor'ом (обработанные actions,
    /// не созданные L2-атомы). By design — больше `l2_atoms`, если в
    /// queue были RecordAction (S0-only).
    pub drained_from_queue: usize,
    /// Реально созданные L2-атомы за этот прогон (через ProposeNewMemory).
    pub l2_atoms: usize,
    /// Рёбра, найденные внутри workspace.
    pub l2_edges: usize,
    /// Созданные L1-домены (autogen).
    pub new_l1_count: usize,
    /// Созданные L0-кластеры (autogen).
    pub new_l0_count: usize,
}

/// Держит Arc-хэндлы на очередь и слои L2/L1/L0 и умеет прогонять консолидацию.
pub struct ConsolidateRunner {
    queue: Option<Arc<QueueProcessor>>,
    l2: Arc<RwLock<L2Actor>>,
    l1: Option<Arc<RwLock<L1Actor>>>,
    l0: Option<Arc<RwLock<L0Actor>>>,
}

impl ConsolidateRunner {
    pub fn new(
        l2: Arc<RwLock<L2Actor>>,
        queue: Option<Arc<QueueProcessor>>,
        l1: Option<Arc<RwLock<L1Actor>>>,
        l0: Option<Arc<RwLock<L0Actor>>>,
    ) -> Self {
        Self { queue, l2, l1, l0 }
    }

    /// Прогнать консолидацию workspace. Поведение идентично прежнему коду в
    /// `consolidate_workspace` (те же шаги и подсчёты).
    pub async fn run(&self, workspace_id: &str) -> Result<ConsolidateStats> {
        // 1. Drain pending queue → L2 (через QueueProcessor).
        //    ProcessStats разделены: `drained` — обработанные actions (legacy-счётчик,
        //    использовался в bug 002 как «new_l2_count», но вводил в заблуждение);
        //    `l2_atoms_created` — реально созданные L2-атомы (только ProposeNewMemory).
        let (drained, l2_atoms_created) = if let Some(q) = &self.queue {
            let stats = q.drain_to_l2(workspace_id).await?;
            (stats.done, stats.l2_atoms_created)
        } else {
            (0, 0)
        };

        // 2. Собрать L2-атомы workspace.
        let l2_atoms = self.l2.read().await.list_by_workspace(workspace_id).await?;
        let l2_atom_count = l2_atoms.len();

        // 3. Собрать рёбра между L2-атомами workspace (только out-edges внутри ws).
        let mut l2_edges: Vec<Edge> = Vec::new();
        {
            let l2_guard = self.l2.read().await;
            for atom in &l2_atoms {
                if let Ok(edges) = l2_guard.edges_from(&atom.id).await {
                    for e in edges {
                        if l2_atoms.iter().any(|a| a.id == e.target) {
                            l2_edges.push(e);
                        }
                    }
                }
            }
        }
        let l2_edge_count = l2_edges.len();

        // 4. L1 autogen (L1Actor).
        let l1_result = if let Some(l1_arc) = &self.l1 {
            Some(
                l1_arc
                    .read()
                    .await
                    .autogenerate_domains(workspace_id, &l2_atoms, &l2_edges)
                    .await?,
            )
        } else {
            None
        };
        let new_l1_count = l1_result
            .as_ref()
            .map(|r| r.created_domains.len())
            .unwrap_or(0);

        // 5. L0 autogen (L0Actor) — нужны L1-домены как Node.
        let new_l0_count = if let (Some(l0_arc), Some(l1r)) = (&self.l0, l1_result.as_ref()) {
            let l1_nodes: Vec<Node> = l1r.created_domains.iter().map(|d| d.node.clone()).collect();
            let r = l0_arc
                .read()
                .await
                .autogenerate_l0(workspace_id, &l1_nodes)
                .await?;
            r.clusters.len()
        } else {
            0
        };

        Ok(ConsolidateStats {
            drained_from_queue: drained,
            l2_atoms: l2_atoms_created,
            l2_edges: l2_edge_count,
            new_l1_count,
            new_l0_count,
        })
    }
}
