//! Node вЂ” Р±Р°Р·РѕРІС‹Р№ Р±Р»РѕРє РіСЂР°С„Р°
//!
//! Based on TECH-SPEC.md В§3 Model Data

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};


/// Unique node identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);


impl NodeId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn from_string(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}


/// Node type вЂ” РѕРїСЂРµРґРµР»СЏРµС‚ СЃРµРјР°РЅС‚РёРєСѓ СѓР·Р»Р°
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum NodeType {
    Atom,    // L2: Р±Р°Р·РѕРІС‹Р№ С„Р°РєС‚
    Cause,   // L2: РїСЂРёС‡РёРЅР°
    Effect,  // L2: СЃР»РµРґСЃС‚РІРёРµ
    Rule,    // L2: РїСЂР°РІРёР»Рѕ (if в†’ then)
    Cluster, // L0: РіСЂСѓРїРїРёСЂРѕРІРєР°
    Hub,     // L0: Р°РіСЂРµРіР°С‚РѕСЂ РєР»Р°СЃС‚РµСЂРѕРІ
    Domain,  // L1: РґРѕРјРµРЅ (Р°РІС‚РѕРіРµРЅ РёР· L2)
}



/// Level вЂ” РёРµСЂР°СЂС…РёСЏ РїР°РјСЏС‚Рё
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Level {
    S0,  // РљСЂР°С‚РєРѕРІСЂРµРјРµРЅРЅР°СЏ (~20 РґРµР№СЃС‚РІРёР№, ephemeral)
    L0,  // РҐР°Р±С‹ Рё РєР»Р°СЃС‚РµСЂС‹
    L1,  // Р”РѕРјРµРЅС‹
    L2,  // РђС‚РѕРјС‹, cause, effect, rule
    GKL, // Р“Р»РѕР±Р°Р»СЊРЅР°СЏ РїР°РјСЏС‚СЊ
}


/// Status вЂ” Р¶РёР·РЅРµРЅРЅС‹Р№ С†РёРєР» СѓР·Р»Р°
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Status {
    Active,
    Draft,
    Archived,
}


impl Default for Status {
    fn default() -> Self {
        Self::Active
    }
}

impl Default for Level {
    fn default() -> Self {
        Self::L2
    }
}


/// Metadata вЂ” РґРѕРїРѕР»РЅРёС‚РµР»СЊРЅС‹Рµ РґР°РЅРЅС‹Рµ СѓР·Р»Р°
///
/// РҐСЂР°РЅРёС‚ РґРІР° РѕСЂС‚РѕРіРѕРЅР°Р»СЊРЅС‹С… РёР·РјРµСЂРµРЅРёСЏ:
/// - `parent_id` вЂ” СѓР·РµР»-СЂРѕРґРёС‚РµР»СЊ РІ cluster hierarchy (L0 в†’ L1 в†’ L2).
/// - `workspace_id` вЂ” storage partition (РІ РєР°РєРѕРј workspace Р¶РёРІС‘С‚ СѓР·РµР»).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Metadata {
    pub tags: Vec<String>,
    pub parent_id: Option<String>,
    pub workspace_id: Option<String>,
}



/// Node вЂ” СѓР·РµР» РіСЂР°С„Р°
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub node_type: NodeType,
    pub level: Level,
    pub content: String,
    pub metadata: Metadata,
    pub status: Status,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}


impl Node {
    /// Create a new node with auto-generated ID and timestamps
    pub fn new(node_type: NodeType, content: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: NodeId::new(),
            node_type,
            level: Level::L2, // default
            content: content.into(),
            metadata: Metadata::default(),
            status: Status::Active,
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a node with custom ID (for deterministic IDs like gkl_L2_*)
    pub fn with_id(id: NodeId, node_type: NodeType, content: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id,
            node_type,
            level: Level::L2,
            content: content.into(),
            metadata: Metadata::default(),
            status: Status::Active,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn with_level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.metadata.tags = tags;
        self
    }

    pub fn with_workspace(mut self, workspace_id: impl Into<String>) -> Self {
        self.metadata.workspace_id = Some(workspace_id.into());
        self
    }

    /// Set cluster parent (L0 в†’ L1 в†’ L2 hierarchy). Separate from `with_workspace`,
    /// which controls storage partition. See bug_report/001.
    pub fn with_parent(mut self, parent_id: impl Into<String>) -> Self {
        self.metadata.parent_id = Some(parent_id.into());
        self
    }
}

/// РЎРµСЂРёР°Р»РёР·СѓРµРјР°СЏ РІРµСЂСЃРёСЏ Node РґР»СЏ С…СЂР°РЅРµРЅРёСЏ РІ backend.
/// РЎРѕРґРµСЂР¶РёС‚ durable-РїРѕР»СЏ: `parent_id` (cluster parent), `workspace_id` (storage partition),
/// `tags`. Status/updated_at РЅРµ СЃРѕС…СЂР°РЅСЏСЋС‚СЃСЏ вЂ” СЌС‚Рѕ runtime-РјРµС‚Р°РґР°РЅРЅС‹Рµ.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredNode {
    pub id: NodeId,
    pub node_type: NodeType,
    pub content: String,
    // L0-кластеры не имеют parent_id (у них member_ids), нужен default для десериализации
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl From<&Node> for StoredNode {
    fn from(n: &Node) -> Self {
        Self {
            id: n.id.clone(),
            node_type: n.node_type,
            content: n.content.clone(),
            parent_id: n.metadata.parent_id.clone(),
            workspace_id: n.metadata.workspace_id.clone(),
            tags: n.metadata.tags.clone(),
            created_at: n.created_at,
        }
    }
}

impl From<StoredNode> for Node {
    fn from(stored: StoredNode) -> Self {
        let now = Utc::now();
        Self {
            id: stored.id,
            node_type: stored.node_type,
            level: Level::L2, // StoredNode РІСЃРµРіРґР° L2
            content: stored.content,
            metadata: Metadata {
                tags: stored.tags,
                parent_id: stored.parent_id,
                workspace_id: stored.workspace_id,
            },
            status: Status::Active,
            created_at: stored.created_at,
            updated_at: now,
        }
    }
}

