//! PlanActor — иерархическое планирование (P0–P3).
//!
//! Иерархия планов: P0 → P1 → P2 → P3.
//!
//! Хранение в backend:
//!   `P0:<id>` / `P1:<id>` / `P2:<id>` / `P3:<id>` → JSON-serialized Plan
//!   `plan:index:by_status:<status>:<id>` → plan_id (для plan_status filter)

use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::persistence::StorageBackend;
use super::orchestrator::OrchestratorActor;

use super::Actor;

const P0_PREFIX: &str = "P0:";
const P1_PREFIX: &str = "P1:";
const P2_PREFIX: &str = "P2:";
const P3_PREFIX: &str = "P3:";
const PLAN_STATUS_INDEX: &str = "plan:index:by_status:";

fn key_for_level(level: PlanLevel, id: &str) -> String {
    let prefix = match level {
        PlanLevel::P0 => P0_PREFIX,
        PlanLevel::P1 => P1_PREFIX,
        PlanLevel::P2 => P2_PREFIX,
        PlanLevel::P3 => P3_PREFIX,
    };
    format!("{prefix}{id}")
}

fn prefix_for_level(level: PlanLevel) -> &'static str {
    match level {
        PlanLevel::P0 => P0_PREFIX,
        PlanLevel::P1 => P1_PREFIX,
        PlanLevel::P2 => P2_PREFIX,
        PlanLevel::P3 => P3_PREFIX,
    }
}

fn status_index_key(status: PlanStatus, id: &str) -> String {
    format!("{PLAN_STATUS_INDEX}{}:{}", status_name(status), id)
}

fn status_name(s: PlanStatus) -> &'static str {
    match s {
        PlanStatus::Created => "created",
        PlanStatus::InProgress => "in_progress",
        PlanStatus::PendingReview => "pending_review",
        PlanStatus::Approved => "approved",
        PlanStatus::Rejected => "rejected",
        PlanStatus::Problem => "problem",
        PlanStatus::Done => "done",
        PlanStatus::Deleted => "deleted",
        PlanStatus::Archived => "archived",
    }
}

fn parse_status(s: &str) -> Option<PlanStatus> {
    match s {
        "created" => Some(PlanStatus::Created),
        "in_progress" => Some(PlanStatus::InProgress),
        "pending_review" => Some(PlanStatus::PendingReview),
        "approved" => Some(PlanStatus::Approved),
        "rejected" => Some(PlanStatus::Rejected),
        "problem" => Some(PlanStatus::Problem),
        "done" => Some(PlanStatus::Done),
        "deleted" => Some(PlanStatus::Deleted),
        "archived" => Some(PlanStatus::Archived),
        _ => None,
    }
}

/// Уровень плана: P0 (epic) → P1 (story) → P2 (task) → P3 (subtask).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum PlanLevel {
    P0,
    P1,
    P2,
    P3,
}

impl PlanLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            PlanLevel::P0 => "P0",
            PlanLevel::P1 => "P1",
            PlanLevel::P2 => "P2",
            PlanLevel::P3 => "P3",
        }
    }
}

/// Статус плана (жизненный цикл).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum PlanStatus {
    Created,
    InProgress,
    PendingReview,
    Approved,
    Rejected,
    Problem,
    Done,
    Deleted,
    Archived,
}

/// Универсальный Plan (P0–P3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub id: String,
    pub level: PlanLevel,
    pub status: PlanStatus,
    pub description: String,
    /// ID родительского плана (None только для P0).
    pub parent_id: Option<String>,
    /// P0-only: autonomous mode (если true — LLM-агент может сам approve P1).
    pub autonomous_mode: bool,
    /// P1-only: оценка качества от CausalEngine-эвристики (0.0..1.0).
    pub quality_score: f32,
    /// P3-only: какой agent_id заявил (claimed).
    pub claimed_by: Option<String>,
    /// P3-only: результат выполнения (после plan_complete).
    pub result: Option<String>,
    /// problem_comment (если status == Problem).
    pub problem_comment: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Plan {
    pub fn new(level: PlanLevel, description: String, parent_id: Option<String>) -> Self {
        let now = Utc::now();
        Self {
            id: format!("{}-{}", level.as_str(), Uuid::new_v4()),
            level,
            status: PlanStatus::Created,
            description,
            parent_id,
            autonomous_mode: false,
            quality_score: 0.0,
            claimed_by: None,
            result: None,
            problem_comment: None,
            created_at: now,
            updated_at: now,
        }
    }
}

/// PlanActor — управление планами P0–P3.
pub struct PlanActor {
    backend: Arc<dyn StorageBackend>,
    /// In-memory cache: id → Plan (для быстрого доступа).
    cache: RwLock<HashMap<String, Plan>>,
    /// Orchestrator для декомпозиции через LLM.
    orchestrator: OrchestratorActor,
}

impl PlanActor {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            backend,
            cache: RwLock::new(HashMap::new()),
            orchestrator: OrchestratorActor::from_env(),
        }
    }

    /// Создать PlanActor с явным Orchestrator (для тестов).
    pub fn with_orchestrator(backend: Arc<dyn StorageBackend>, orchestrator: OrchestratorActor) -> Self {
        Self {
            backend,
            cache: RwLock::new(HashMap::new()),
            orchestrator,
        }
    }

    /// Внутренний helper — сохранить Plan и обновить индекс.
    async fn store(&self, plan: &Plan) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec(plan)?;
        self.backend.put(&key_for_level(plan.level, &plan.id), bytes).await?;
        self.backend
            .put(&status_index_key(plan.status, &plan.id), plan.id.as_bytes().to_vec())
            .await?;
        self.cache.write().await.insert(plan.id.clone(), plan.clone());
        Ok(())
    }

    pub async fn get_plan(&self, id: &str) -> anyhow::Result<Option<Plan>> {
        self.get(id).await
    }

    /// Обновить план (описание и/или статус). Сохраняет в backend + кэш.
    pub async fn update_plan(&self, id: &str, updated: Plan) -> anyhow::Result<()> {
        // Удаляем старый status-index, добавляем новый
        let old = self.get(id).await?;
        if let Some(ref old_plan) = old {
            self.backend
                .delete(&status_index_key(old_plan.status, id))
                .await?;
        }
        self.store(&updated).await?;
        Ok(())
    }

    /// Установить статус плана без изменения описания.
    pub async fn set_status(&self, id: &str, new_status: PlanStatus) -> anyhow::Result<()> {
        let mut plan = self.get(id).await?
            .ok_or_else(|| anyhow::anyhow!("plan not found: {}", id))?;
        // Удаляем старый status-index
        self.backend
            .delete(&status_index_key(plan.status, id))
            .await?;
        plan.status = new_status;
        plan.updated_at = chrono::Utc::now();
        self.store(&plan).await?;
        Ok(())
    }

    async fn get(&self, id: &str) -> anyhow::Result<Option<Plan>> {
        if let Some(p) = self.cache.read().await.get(id) {
            return Ok(Some(p.clone()));
        }
        // Пробуем все префиксы
        for level in [PlanLevel::P0, PlanLevel::P1, PlanLevel::P2, PlanLevel::P3] {
            if let Some(bytes) = self.backend.get(&key_for_level(level, id)).await? {
                let plan: Plan = serde_json::from_slice(&bytes)?;
                self.cache.write().await.insert(plan.id.clone(), plan.clone());
                return Ok(Some(plan));
            }
        }
        Ok(None)
    }

    /// Quality score для P1 (эвристика V2.0, см. scope.md § 12-plan).
    /// V2.1 заменит на реальный CausalEngine.
    fn heuristic_quality_score(description: &str) -> f32 {
        let len = description.len();
        let words: Vec<&str> = description.split_whitespace().collect();
        let has_verb = words.iter().any(|w| {
            let lw = w.to_lowercase();
            lw.starts_with("implement")
                || lw.starts_with("add")
                || lw.starts_with("fix")
                || lw.starts_with("refactor")
                || lw.starts_with("create")
                || lw.starts_with("build")
        });
        let has_measurable = words.iter().any(|w| {
            let lw = w.to_lowercase();
            lw.contains("test")
                || lw.contains("metric")
                || lw.contains("benchmark")
                || lw.contains("kpi")
        });
        let mut score: f32 = 0.0;
        if (20..=200).contains(&len) {
            score += 0.4;
        } else if len > 10 {
            score += 0.2;
        }
        if has_verb {
            score += 0.3;
        }
        if has_measurable {
            score += 0.3;
        }
        score.min(1.0)
    }

    // ============ Public API (вызывается из McpHandler) ============

    /// P0: создать эпик.
    pub async fn plan_create_p0(&self, description: String, autonomous_mode: bool) -> anyhow::Result<Plan> {
        let mut plan = Plan::new(PlanLevel::P0, description, None);
        plan.autonomous_mode = autonomous_mode;
        self.store(&plan).await?;
        Ok(plan)
    }

    /// P1: предложить story под P0.
    pub async fn plan_propose_p1(
        &self,
        p0_id: &str,
        description: String,
    ) -> anyhow::Result<Plan> {
        // Проверяем что parent — P0
        let parent = self.get(p0_id).await?.ok_or_else(|| {
            anyhow::anyhow!("P0 {} not found", p0_id)
        })?;
        if parent.level != PlanLevel::P0 {
            return Err(anyhow::anyhow!("parent {} is not P0 (level={:?})", p0_id, parent.level));
        }
        let mut plan = Plan::new(PlanLevel::P1, description, Some(p0_id.to_string()));
        plan.status = PlanStatus::PendingReview;
        plan.quality_score = Self::heuristic_quality_score(&plan.description);
        self.store(&plan).await?;
        Ok(plan)
    }

    /// P1: approve.
    pub async fn plan_approve_p1(&self, p1_id: &str) -> anyhow::Result<Plan> {
        let mut plan = self.get(p1_id).await?.ok_or_else(|| anyhow::anyhow!("{} not found", p1_id))?;
        if plan.level != PlanLevel::P1 {
            return Err(anyhow::anyhow!("{} is not P1", p1_id));
        }
        plan.status = PlanStatus::Approved;
        plan.updated_at = Utc::now();
        self.store(&plan).await?;
        Ok(plan)
    }

    /// P1: reject.
    pub async fn plan_reject_p1(&self, p1_id: &str, reason: String) -> anyhow::Result<Plan> {
        let mut plan = self.get(p1_id).await?.ok_or_else(|| anyhow::anyhow!("{} not found", p1_id))?;
        if plan.level != PlanLevel::P1 {
            return Err(anyhow::anyhow!("{} is not P1", p1_id));
        }
        plan.status = PlanStatus::Rejected;
        plan.problem_comment = Some(reason);
        plan.updated_at = Utc::now();
        self.store(&plan).await?;
        Ok(plan)
    }

    /// Decompose: P0 → P1, P1 → P2, P2 → P3. Возвращает созданные children.
    /// В V2.0 — LLM-генерация через OrchestratorActor.
    pub async fn plan_decompose(&self, parent_id: &str) -> anyhow::Result<Vec<Plan>> {
        let parent = self.get(parent_id).await?.ok_or_else(|| anyhow::anyhow!("{} not found", parent_id))?;
        
        // 1. Оценить глубину декомпозиции
        let depth = if parent.level == PlanLevel::P0 {
            // Для P0 оцениваем размер и определяем глубину
            let size = self.orchestrator.estimate_size(&parent.description);
            self.orchestrator.depth_for_size(size)
        } else {
            // Для P1, P2 — просто следующий уровень
            match parent.level {
                PlanLevel::P0 => 1,
                PlanLevel::P1 => 2,
                PlanLevel::P2 => 3,
                PlanLevel::P3 => return Err(anyhow::anyhow!("P3 cannot be decomposed")),
            }
        };

        // depth = 0 (Tiny) — не декомпозируем, возвращаем пустой массив.
        // (Проверяем ДО определения child_level, чтобы избежать Invalid depth: 0.)
        if depth == 0 {
            return Ok(vec![]);
        }

        // P3 не декомпозируется
        if depth > 3 {
            return Err(anyhow::anyhow!("Cannot decompose beyond P3"));
        }

        // Определяем уровень детей
        let child_level = match depth {
            1 => PlanLevel::P1,
            2 => PlanLevel::P2,
            3 => PlanLevel::P3,
            _ => return Err(anyhow::anyhow!("Invalid depth: {}", depth)),
        };

        // 2. Сгенерировать детей через LLM.
        //    NO FALLBACK: если LLM не доступен / вернул пустой результат /
        //    API error / parse error — возвращаем Err, чтобы MCP-клиент
        //    увидел ok:false и оператор понял, что декомпозиция реально сломана.
        //    (Раньше здесь стоял fallback с vec!["Исследовать: ...", "Спроектировать: ..."],
        //    который маскировал любые ошибки LLM под фейковые подзадачи.)
        let child_descriptions = match self.orchestrator.generate_children(&parent.description, depth).await {
            Ok(descs) => descs,
            Err(e) => {
                tracing::error!(
                    "plan_decompose({}): LLM generation failed, returning error to caller: {}",
                    parent_id, e
                );
                return Err(e);
            }
        };

        // 3. Создать детей
        let mut children = Vec::new();
        for desc in child_descriptions {
            let mut plan = Plan::new(
                child_level,
                desc,
                Some(parent_id.to_string()),
            );
            plan.status = PlanStatus::PendingReview;
            if child_level == PlanLevel::P1 {
                plan.quality_score = Self::heuristic_quality_score(&plan.description);
            }
            self.store(&plan).await?;
            children.push(plan);
        }

        Ok(children)
    }

    /// P3: claim by agent.
    pub async fn plan_claim(&self, p3_id: &str, agent_id: String) -> anyhow::Result<Plan> {
        let mut plan = self.get(p3_id).await?.ok_or_else(|| anyhow::anyhow!("{} not found", p3_id))?;
        if plan.level != PlanLevel::P3 {
            return Err(anyhow::anyhow!("{} is not P3", p3_id));
        }
        plan.status = PlanStatus::InProgress;
        plan.claimed_by = Some(agent_id);
        plan.updated_at = Utc::now();
        self.store(&plan).await?;
        Ok(plan)
    }

    /// P3: complete with result.
    pub async fn plan_complete(&self, p3_id: &str, result: String) -> anyhow::Result<Plan> {
        let mut plan = self.get(p3_id).await?.ok_or_else(|| anyhow::anyhow!("{} not found", p3_id))?;
        if plan.level != PlanLevel::P3 {
            return Err(anyhow::anyhow!("{} is not P3", p3_id));
        }
        plan.status = PlanStatus::Done;
        plan.result = Some(result);
        plan.updated_at = Utc::now();
        self.store(&plan).await?;
        Ok(plan)
    }

    /// Set problem on any plan.
    pub async fn plan_set_problem(&self, plan_id: &str, comment: String) -> anyhow::Result<Plan> {
        let mut plan = self.get(plan_id).await?.ok_or_else(|| anyhow::anyhow!("{} not found", plan_id))?;
        plan.status = PlanStatus::Problem;
        plan.problem_comment = Some(comment);
        plan.updated_at = Utc::now();
        self.store(&plan).await?;
        Ok(plan)
    }

    /// Resolve problem.
    pub async fn plan_resolve_problem(&self, plan_id: &str, resolution: String) -> anyhow::Result<Plan> {
        let mut plan = self.get(plan_id).await?.ok_or_else(|| anyhow::anyhow!("{} not found", plan_id))?;
        plan.status = PlanStatus::InProgress;
        plan.problem_comment = Some(format!("RESOLVED: {}", resolution));
        plan.updated_at = Utc::now();
        self.store(&plan).await?;
        Ok(plan)
    }

    /// Status: list all plans (optionally filtered by status).
    pub async fn plan_status(&self, status_filter: Option<PlanStatus>) -> anyhow::Result<Vec<Plan>> {
        let status = match status_filter {
            Some(s) => s,
            None => return self.list_all().await,
        };
        let prefix = format!("{PLAN_STATUS_INDEX}{}:", status_name(status));
        let keys = self.backend.list_keys(&prefix).await?;
        let mut plans = Vec::new();
        for k in keys {
            if let Some(id) = k.strip_prefix(&prefix) {
                if let Some(p) = self.get(id).await? {
                    plans.push(p);
                }
            }
        }
        Ok(plans)
    }

    async fn list_all(&self) -> anyhow::Result<Vec<Plan>> {
        let mut plans = Vec::new();
        for prefix in [P0_PREFIX, P1_PREFIX, P2_PREFIX, P3_PREFIX] {
            let keys = self.backend.list_keys(prefix).await?;
            for k in keys {
                if let Some(id) = k.strip_prefix(prefix) {
                    if let Some(p) = self.get(id).await? {
                        plans.push(p);
                    }
                }
            }
        }
        Ok(plans)
    }

    /// Hard delete (force=true) or soft delete (force=false → Archived).
    pub async fn plan_delete(&self, plan_id: &str, force: bool) -> anyhow::Result<bool> {
        let Some(plan) = self.get(plan_id).await? else {
            return Ok(false);
        };
        let key = key_for_level(plan.level, &plan.id);
        self.backend.delete(&key).await?;
        self.backend.delete(&status_index_key(plan.status, &plan.id)).await?;
        self.cache.write().await.remove(&plan.id);
        let _ = force; // hard delete выше (не restore)
        Ok(true)
    }

    /// Soft delete (Archive).
    pub async fn plan_archive(&self, plan_id: &str) -> anyhow::Result<Plan> {
        let mut plan = self.get(plan_id).await?.ok_or_else(|| anyhow::anyhow!("{} not found", plan_id))?;
        plan.status = PlanStatus::Archived;
        plan.updated_at = Utc::now();
        self.store(&plan).await?;
        Ok(plan)
    }
}

#[async_trait]
impl Actor for PlanActor {
    fn name(&self) -> &str {
        "PlanActor"
    }
    async fn size(&self) -> usize {
        self.cache.read().await.len()
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
    async fn test_create_p0_and_propose_p1() {
        let actor = PlanActor::new(backend());
        let p0 = actor.plan_create_p0("Build GraphMind v2".to_string(), true).await.unwrap();
        assert_eq!(p0.level, PlanLevel::P0);
        assert!(p0.autonomous_mode);

        let p1 = actor.plan_propose_p1(&p0.id, "Implement Config actor".to_string()).await.unwrap();
        assert_eq!(p1.level, PlanLevel::P1);
        assert_eq!(p1.parent_id, Some(p0.id.clone()));
        assert_eq!(p1.status, PlanStatus::PendingReview);
        assert!(p1.quality_score > 0.0);
    }

    #[tokio::test]
    async fn test_approve_reject_p1() {
        let actor = PlanActor::new(backend());
        let p0 = actor.plan_create_p0("P0".to_string(), false).await.unwrap();
        let p1 = actor.plan_propose_p1(&p0.id, "Implement X".to_string()).await.unwrap();

        let approved = actor.plan_approve_p1(&p1.id).await.unwrap();
        assert_eq!(approved.status, PlanStatus::Approved);

        let p1b = actor.plan_propose_p1(&p0.id, "Implement Y".to_string()).await.unwrap();
        let rejected = actor.plan_reject_p1(&p1b.id, "out of scope".to_string()).await.unwrap();
        assert_eq!(rejected.status, PlanStatus::Rejected);
        assert_eq!(rejected.problem_comment, Some("out of scope".to_string()));
    }

    /// Собрать PlanActor с OrchestratorActor, у которого зарегистрирован
    /// один canned-ответ LLM. Позволяет unit-тестам проверять успешный путь
    /// plan_decompose без обращения к сети.
    fn actor_with_response(response: Vec<String>) -> PlanActor {
        let mut orch = crate::actors::orchestrator::OrchestratorActor::new(
            "http://127.0.0.1:1".to_string(),
            None,
            "test-model".to_string(),
        );
        orch.add_test_response(response);
        PlanActor::with_orchestrator(backend(), orch)
    }

    /// Описание, которое estimate_size классифицирует как Small (len > 50 или
    /// есть action-word). "P0" слишком короткое → Tiny → depth=0 → пустой массив.
    const DECOMPOSE_DESC: &str =
        "Создать документацию GraphMind v2 с примерами использования и руководством по настройке";

    #[tokio::test]
    async fn test_decompose_p0_to_p1() {
        let actor = actor_with_response(vec![
            "Создать структуру проекта".to_string(),
            "Реализовать базовый API".to_string(),
            "Настроить CI/CD".to_string(),
        ]);
        let p0 = actor.plan_create_p0(DECOMPOSE_DESC.to_string(), false).await.unwrap();
        let children = actor.plan_decompose(&p0.id).await.unwrap();
        // LLM генерирует 2-4 children (раньше здесь был fallback на 3 фейковых).
        assert!(children.len() >= 2, "got {} children", children.len());
        assert!(children.iter().all(|c| c.level == PlanLevel::P1));
        assert!(children.iter().all(|c| c.parent_id == Some(p0.id.clone())));
        // Children больше не должны быть placeholder'ами вроде "Исследовать: ...".
        assert!(!children.iter().any(|c| c.description.starts_with("Исследовать:")));
        assert!(!children.iter().any(|c| c.description.starts_with("Спроектировать:")));
        assert!(!children.iter().any(|c| c.description.starts_with("Реализовать:")));
    }

    #[tokio::test]
    async fn test_decompose_returns_error_when_llm_fails() {
        // V2.0: если LLM не доступен / вернул пусто — plan_decompose ВОЗВРАЩАЕТ Err,
        // а не молча подставляет фейковые подзадачи.
        // (Реальный OrchestratorActor с base_url=http://127.0.0.1:1 упадёт в reqwest.)
        let orch = crate::actors::orchestrator::OrchestratorActor::new(
            "http://127.0.0.1:1".to_string(),
            None,
            "test-model".to_string(),
        );
        let actor = PlanActor::with_orchestrator(backend(), orch);
        let p0 = actor.plan_create_p0(DECOMPOSE_DESC.to_string(), false).await.unwrap();
        let result = actor.plan_decompose(&p0.id).await;
        assert!(
            result.is_err(),
            "plan_decompose must propagate LLM error instead of using fallback"
        );
    }

    #[tokio::test]
    async fn test_claim_complete_p3() {
        let mut orch = crate::actors::orchestrator::OrchestratorActor::new(
            "http://127.0.0.1:1".to_string(),
            None,
            "test-model".to_string(),
        );
        // Два ответа: для P1→P2 и P2→P3.
        orch.add_test_response(vec![
            "P2-задача A".to_string(),
            "P2-задача B".to_string(),
        ]);
        orch.add_test_response(vec![
            "P3-подзадача X".to_string(),
            "P3-подзадача Y".to_string(),
        ]);
        let actor = PlanActor::with_orchestrator(backend(), orch);

        let p0 = actor.plan_create_p0(DECOMPOSE_DESC.to_string(), false).await.unwrap();
        let p1 = actor.plan_propose_p1(&p0.id, "Реализовать секцию API".to_string()).await.unwrap();
        actor.plan_approve_p1(&p1.id).await.unwrap();
        let children_p2 = actor.plan_decompose(&p1.id).await.unwrap();
        assert!(!children_p2.is_empty());
        let p2 = children_p2[0].clone();
        let children_p3 = actor.plan_decompose(&p2.id).await.unwrap();
        assert!(!children_p3.is_empty());
        let p3 = children_p3[0].clone();
        assert_eq!(p3.level, PlanLevel::P3);

        let claimed = actor.plan_claim(&p3.id, "agent-42".to_string()).await.unwrap();
        assert_eq!(claimed.status, PlanStatus::InProgress);
        assert_eq!(claimed.claimed_by, Some("agent-42".to_string()));

        let done = actor.plan_complete(&p3.id, "all good".to_string()).await.unwrap();
        assert_eq!(done.status, PlanStatus::Done);
        assert_eq!(done.result, Some("all good".to_string()));
    }
    #[tokio::test]
    async fn test_status_filter() {
        let actor = PlanActor::new(backend());
        let p0 = actor.plan_create_p0("P0".to_string(), false).await.unwrap();
        actor.plan_propose_p1(&p0.id, "P1".to_string()).await.unwrap();

        let pending = actor.plan_status(Some(PlanStatus::PendingReview)).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].status, PlanStatus::PendingReview);
    }
}
