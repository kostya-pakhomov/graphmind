//! WorkspaceManager -- управление workspace'ами (создание, переключение, архивация, листинг).
//!
//! Workspace = изолированное пространство памяти с собственным графом, S0, L2, L1, L0.
//! Активный workspace используется по умолчанию для всех операций.

use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;
use chrono::{DateTime, Utc};

use crate::graph::{Graph, NodeId};
use crate::persistence::StorageBackend;

use super::Actor;

/// Статус workspace
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WorkspaceStatus {
    Active,
    Archived,
}

/// Метаданные workspace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub id: String,
    pub name: String,
    pub path: Option<String>,
    pub status: WorkspaceStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub node_count: usize,
    pub edge_count: usize,
}

impl Workspace {
    pub fn new(name: String, path: Option<String>) -> Self {
        let now = Utc::now();
        Self {
            id: format!("ws_{}", Uuid::new_v4().to_string().replace("-", "")),
            name,
            path,
            status: WorkspaceStatus::Active,
            created_at: now,
            updated_at: now,
            node_count: 0,
            edge_count: 0,
        }
    }
}

/// WorkspaceManager -- управление workspace'ами
pub struct WorkspaceManager {
    backend: Arc<dyn StorageBackend>,
    /// Кэш workspace'ов (id -> Workspace)
    workspaces: RwLock<HashMap<String, Workspace>>,
    /// Активный workspace id
    active_workspace_id: RwLock<Option<String>>,
}

impl WorkspaceManager {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            backend,
            workspaces: RwLock::new(HashMap::new()),
            active_workspace_id: RwLock::new(None),
        }
    }

    /// Создать новый workspace
    pub async fn create_workspace(&self, name: String, path: Option<String>) -> anyhow::Result<Workspace> {
        let workspace = Workspace::new(name, path);
        
        // Сохранить в backend
        let key = format!("workspace:{}", workspace.id);
        let bytes = serde_json::to_vec(&workspace)?;
        self.backend.put(&key, bytes).await?;
        
        // Добавить в кэш
        self.workspaces.write().await.insert(workspace.id.clone(), workspace.clone());
        
        // Если это первый workspace - сделать активным
        let mut active = self.active_workspace_id.write().await;
        if active.is_none() {
            *active = Some(workspace.id.clone());
        }
        drop(active);
        
        Ok(workspace)
    }

    /// Получить workspace по ID
    pub async fn get_workspace(&self, id: &str) -> anyhow::Result<Option<Workspace>> {
        // Проверяем кэш
        if let Some(ws) = self.workspaces.read().await.get(id) {
            return Ok(Some(ws.clone()));
        }
        
        // Пытаемся загрузить из backend
        let key = format!("workspace:{}", id);
        match self.backend.get(&key).await? {
            Some(bytes) => {
                let workspace: Workspace = serde_json::from_slice(&bytes)?;
                self.workspaces.write().await.insert(id.to_string(), workspace.clone());
                Ok(Some(workspace))
            }
            None => Ok(None),
        }
    }

    /// Переключить активный workspace
    pub async fn switch_workspace(&self, workspace_id: &str) -> anyhow::Result<bool> {
        // Проверяем существование
        if self.get_workspace(workspace_id).await?.is_none() {
            return Ok(false);
        }
        
        let mut active = self.active_workspace_id.write().await;
        *active = Some(workspace_id.to_string());
        drop(active);
        
        Ok(true)
    }

    /// Получить активный workspace ID
    pub async fn get_active_workspace_id(&self) -> Option<String> {
        self.active_workspace_id.read().await.clone()
    }

    /// Архивировать workspace
    pub async fn archive_workspace(&self, workspace_id: &str) -> anyhow::Result<bool> {
        match self.get_workspace(workspace_id).await? {
            Some(mut workspace) => {
                workspace.status = WorkspaceStatus::Archived;
                workspace.updated_at = Utc::now();
                
                // Сохранить обновлённый workspace
                let key = format!("workspace:{}", workspace.id);
                let bytes = serde_json::to_vec(&workspace)?;
                self.backend.put(&key, bytes).await?;
                
                // Обновить кэш
                self.workspaces.write().await.insert(workspace.id.clone(), workspace);
                
                // Если архивировали активный - сбросить активный
                let mut active = self.active_workspace_id.write().await;
                if active.as_ref() == Some(&workspace_id.to_string()) {
                    *active = None;
                }
                drop(active);
                
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Список всех workspace'ов
    pub async fn list_workspaces(&self, status_filter: Option<WorkspaceStatus>) -> anyhow::Result<Vec<Workspace>> {
        let all_workspaces = self.workspaces.read().await;
        let results = all_workspaces
            .values()
            .filter(|ws| {
                match &status_filter {
                    Some(s) => &ws.status == s,
                    None => true,
                }
            })
            .cloned()
            .collect();
        Ok(results)
    }

    /// Детектировать workspace по cwd
    pub async fn detect_workspace(&self, cwd: &str) -> anyhow::Result<Option<String>> {
        // Ищем workspace с matching path
        let workspaces = self.workspaces.read().await;
        let matched_id: Option<String> = workspaces.iter()
            .filter(|(_, ws)| ws.status != WorkspaceStatus::Archived)
            .find_map(|(id, ws)| {
                ws.path.as_ref().and_then(|path| {
                    // Windows: пути case-insensitive (C:\Skills == c:\skills)
                let path_lc = path.to_lowercase();
                let cwd_lc = cwd.to_lowercase();
                if path_lc == cwd_lc || cwd_lc.contains(&path_lc) || path_lc.contains(&cwd_lc) {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
            });
        drop(workspaces);

        // Нашли существующий — активируем и возвращаем
        if let Some(id) = matched_id {
            self.switch_workspace(&id).await?;
            return Ok(Some(id));
        }

        // Не нашли — создаём новый (create_workspace делает его активным, если active=None)
        let workspace = self.create_workspace(
            cwd.split(['/', '\\']).last().unwrap_or(cwd).to_string(),
            Some(cwd.to_string()),
        ).await?;

        // Явно активируем новый workspace
        self.switch_workspace(&workspace.id).await?;

        Ok(Some(workspace.id))
    }

    /// Загрузить все workspace'ы из backend
    pub async fn load_all(&self) -> anyhow::Result<usize> {
        let prefix = "workspace:";
        let keys = self.backend.list_keys(prefix).await?;
        let mut count = 0;
        
        for key in keys {
            if let Some(bytes) = self.backend.get(&key).await? {
                if let Ok(workspace) = serde_json::from_slice::<Workspace>(&bytes) {
                    self.workspaces.write().await.insert(workspace.id.clone(), workspace);
                    count += 1;
                }
            }
        }
        
        Ok(count)
    }

    /// Инкрементировать счётчик узлов workspace (и persist в backend).
    /// Вызывается при создании/удалении узлов в этом workspace.
    pub async fn bump_node_count(&self, workspace_id: &str, delta: isize) -> anyhow::Result<()> {
        let mut workspaces = self.workspaces.write().await;
        if let Some(workspace) = workspaces.get_mut(workspace_id) {
            if delta >= 0 {
                workspace.node_count = workspace.node_count.saturating_add(delta as usize);
            } else {
                workspace.node_count = workspace.node_count.saturating_sub((-delta) as usize);
            }
            workspace.updated_at = Utc::now();

            let key = format!("workspace:{}", workspace_id);
            let bytes = serde_json::to_vec(workspace)?;
            self.backend.put(&key, bytes).await?;

            Ok(())
        } else {
            drop(workspaces);
            if let Some(mut workspace) = self.get_workspace(workspace_id).await? {
                if delta >= 0 {
                    workspace.node_count = workspace.node_count.saturating_add(delta as usize);
                } else {
                    workspace.node_count = workspace.node_count.saturating_sub((-delta) as usize);
                }
                workspace.updated_at = Utc::now();

                let key = format!("workspace:{}", workspace_id);
                let bytes = serde_json::to_vec(&workspace)?;
                self.backend.put(&key, bytes).await?;

                self.workspaces.write().await.insert(workspace_id.to_string(), workspace);
                Ok(())
            } else {
                anyhow::bail!("Workspace {} not found", workspace_id)
            }
        }
    }

    /// Инкрементировать счётчик рёбер workspace (и persist в backend).
    pub async fn bump_edge_count(&self, workspace_id: &str, delta: isize) -> anyhow::Result<()> {
        let mut workspaces = self.workspaces.write().await;
        if let Some(workspace) = workspaces.get_mut(workspace_id) {
            if delta >= 0 {
                workspace.edge_count = workspace.edge_count.saturating_add(delta as usize);
            } else {
                workspace.edge_count = workspace.edge_count.saturating_sub((-delta) as usize);
            }
            workspace.updated_at = Utc::now();

            let key = format!("workspace:{}", workspace_id);
            let bytes = serde_json::to_vec(workspace)?;
            self.backend.put(&key, bytes).await?;

            Ok(())
        } else {
            drop(workspaces);
            if let Some(mut workspace) = self.get_workspace(workspace_id).await? {
                if delta >= 0 {
                    workspace.edge_count = workspace.edge_count.saturating_add(delta as usize);
                } else {
                    workspace.edge_count = workspace.edge_count.saturating_sub((-delta) as usize);
                }
                workspace.updated_at = Utc::now();

                let key = format!("workspace:{}", workspace_id);
                let bytes = serde_json::to_vec(&workspace)?;
                self.backend.put(&key, bytes).await?;

                self.workspaces.write().await.insert(workspace_id.to_string(), workspace);
                Ok(())
            } else {
                anyhow::bail!("Workspace {} not found", workspace_id)
            }
        }
    }

    /// Получить статистику workspace
    pub async fn workspace_stats(&self, workspace_id: &str) -> anyhow::Result<(usize, usize)> {
        if let Some(ws) = self.workspaces.read().await.get(workspace_id) {
            Ok((ws.node_count, ws.edge_count))
        } else if let Some(ws) = self.get_workspace(workspace_id).await? {
            Ok((ws.node_count, ws.edge_count))
        } else {
            Ok((0, 0))
        }
    }
}

#[async_trait]
impl Actor for WorkspaceManager {
    fn name(&self) -> &str {
        "WorkspaceManager"
    }

    async fn size(&self) -> usize {
        self.workspaces.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::InMemoryBackend;

    fn backend() -> Arc<dyn StorageBackend> {
        Arc::new(InMemoryBackend::new())
    }

    #[tokio::test]
    async fn test_create_workspace() {
        let manager = WorkspaceManager::new(backend());
        let ws = manager.create_workspace("Test Workspace".to_string(), Some("/path/to/project".to_string())).await.unwrap();
        
        assert_eq!(ws.name, "Test Workspace");
        assert_eq!(ws.status, WorkspaceStatus::Active);
        assert!(ws.id.starts_with("ws_"));
    }

    #[tokio::test]
    async fn test_get_workspace() {
        let manager = WorkspaceManager::new(backend());
        let ws = manager.create_workspace("Test".to_string(), None).await.unwrap();
        
        let fetched = manager.get_workspace(&ws.id).await.unwrap().unwrap();
        assert_eq!(fetched.id, ws.id);
        assert_eq!(fetched.name, "Test");
    }

    #[tokio::test]
    async fn test_switch_workspace() {
        let manager = WorkspaceManager::new(backend());
        let ws1 = manager.create_workspace("WS1".to_string(), None).await.unwrap();
        let ws2 = manager.create_workspace("WS2".to_string(), None).await.unwrap();
        
        assert!(manager.switch_workspace(&ws2.id).await.unwrap());
        assert_eq!(manager.get_active_workspace_id().await, Some(ws2.id.clone()));
    }

    #[tokio::test]
    async fn test_archive_workspace() {
        let manager = WorkspaceManager::new(backend());
        let ws = manager.create_workspace("Test".to_string(), None).await.unwrap();
        
        assert!(manager.archive_workspace(&ws.id).await.unwrap());
        
        let fetched = manager.get_workspace(&ws.id).await.unwrap().unwrap();
        assert_eq!(fetched.status, WorkspaceStatus::Archived);
    }

    #[tokio::test]
    async fn test_list_workspaces() {
        let manager = WorkspaceManager::new(backend());
        manager.create_workspace("WS1".to_string(), None).await.unwrap();
        manager.create_workspace("WS2".to_string(), None).await.unwrap();
        
        let all = manager.list_workspaces(None).await.unwrap();
        assert_eq!(all.len(), 2);
        
        let active = manager.list_workspaces(Some(WorkspaceStatus::Active)).await.unwrap();
        assert_eq!(active.len(), 2);
    }

    #[tokio::test]
    async fn test_detect_workspace() {
        let manager = WorkspaceManager::new(backend());
        let cwd = "/home/user/my-project";
        
        let ws_id = manager.detect_workspace(cwd).await.unwrap().unwrap();
        let ws = manager.get_workspace(&ws_id).await.unwrap().unwrap();
        
        assert_eq!(ws.path, Some(cwd.to_string()));
        assert_eq!(ws.name, "my-project");
    }

    #[tokio::test]
    async fn test_bump_node_count() {
        let manager = WorkspaceManager::new(backend());
        let ws = manager.create_workspace("Test".to_string(), None).await.unwrap();

        // Изначально node_count = 0
        assert_eq!(ws.node_count, 0);

        // Инкрементируем на +1
        manager.bump_node_count(&ws.id, 1).await.unwrap();

        // Проверяем, что счётчик обновился
        let updated = manager.get_workspace(&ws.id).await.unwrap().unwrap();
        assert_eq!(updated.node_count, 1);
        assert!(updated.updated_at > updated.created_at, "updated_at должен сдвинуться");

        // Инкрементируем ещё на +3
        manager.bump_node_count(&ws.id, 3).await.unwrap();
        let again = manager.get_workspace(&ws.id).await.unwrap().unwrap();
        assert_eq!(again.node_count, 4);

        // Декрементируем на -2
        manager.bump_node_count(&ws.id, -2).await.unwrap();
        let final_ws = manager.get_workspace(&ws.id).await.unwrap().unwrap();
        assert_eq!(final_ws.node_count, 2);

        // Декрементируем ниже нуля — должно остаться 0 (saturating_sub)
        manager.bump_node_count(&ws.id, -10).await.unwrap();
        let zero_ws = manager.get_workspace(&ws.id).await.unwrap().unwrap();
        assert_eq!(zero_ws.node_count, 0);
    }
}
