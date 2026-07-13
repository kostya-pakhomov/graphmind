//! Edge — связь между узлами графа
//!
//! Based on TECH-SPEC.md §3 Model Data

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};

use super::node::NodeId;



/// Unique edge identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EdgeId(pub String);

impl EdgeId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for EdgeId {
    fn default() -> Self {
        Self::new()
    }
}


/// Relation type — тип причинной связи
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Relation {
    RelatedTo,     // общая связь
    LeadsTo,       // причина → следствие
    ExplainedBy,   // следствие → причина
    DerivedFrom,   // правило → причина/следствие
    DependsOn,     // зависимость
    Inhibits,      // блокирует
    Contradicts,   // конфликтует
    Implements,    // реализует
    Supersedes,    // заменяет
}


/// Provenance — откуда связь
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Provenance {
    Manual,                    // явно создана пользователем/агентом
    Inferred,                  // предложена CausalEngine
    DerivedFromConsolidation,   // создана при агрегации L2→L1
}

impl Default for Provenance {
    fn default() -> Self {
        Self::Manual
    }
}


/// Edge — связь между двумя узлами
///
/// `workspace_id` привязывает ребро к storage partition (см. bug_report/001).
/// До введения этого поля BFS в `suggest_related` не находил рёбра, потому что
/// обход не мог отфильтровать edges по workspace, к которому принадлежат их узлы.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub id: EdgeId,
    pub source: NodeId,
    pub target: NodeId,
    pub relation: Relation,
    pub confidence: f32,        // 0.0–1.0
    pub provenance: Provenance,
    pub workspace_id: Option<String>,
    pub created_at: DateTime<Utc>,
}


impl Edge {
    /// Create a new edge with auto-generated ID
    pub fn new(source: NodeId, target: NodeId, relation: Relation) -> Self {
        Self {
            id: EdgeId::new(),
            source,
            target,
            relation,
            confidence: 1.0,
            provenance: Provenance::Manual,
            workspace_id: None, // выставляется через with_workspace() или link_nodes
            created_at: Utc::now(),
        }
    }

    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = confidence.clamp(0.0, 1.0);
        self
    }

    pub fn with_provenance(mut self, provenance: Provenance) -> Self {
        self.provenance = provenance;
        self
    }

    /// Привязать ребро к workspace (storage partition). Нужно для BFS в
    /// `suggest_related`, чтобы обход не вылезал за пределы активного workspace.
    pub fn with_workspace(mut self, workspace_id: impl Into<String>) -> Self {
        self.workspace_id = Some(workspace_id.into());
        self
    }
}
