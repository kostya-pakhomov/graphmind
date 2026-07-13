//! TrustFirewall -- защита памяти от манипуляций и недостоверной информации.
//!
//! Firewall оценивает доверие к источникам, проверяет консистентность,
//! детектирует аномалии тона и блокирует sensitive операции без верификации.

use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use crate::graph::Status;
use super::{Actor, L2Actor};

/// Ключ калибровки в backend (namespace `trust:`, не пересекается с `node:`/`edge:`).
const TRUST_CALIBRATION_KEY: &str = "trust:calibration";
/// Префикс ключей репутации источников.
const TRUST_REPUTATION_PREFIX: &str = "trust:reputation:";

/// Тип источника информации
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SourceType {
    UserDirect,      // Пользователь ввёл напрямую
    UserDocument,    // Прикреплён файл/URL
    WebSearch,       // Найдено в вебе
    AgentInternal,   // Вывод другой системы
    ExternalAPI,     // Внешний API
    Unknown,
}

/// Доверие к источнику
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceTrust {
    pub source_id: String,
    pub source_type: SourceType,
    pub base_score: f32,           // 0.0-1.0
    pub reputation_history: f32,   // 0.0-1.0
    pub corroboration_count: usize,
    pub last_verification: DateTime<Utc>,
}

impl SourceTrust {
    pub fn new(source_id: String, source_type: SourceType) -> Self {
        let now = Utc::now();
        Self {
            source_id,
            source_type,
            base_score: 0.5,
            reputation_history: 0.5,
            corroboration_count: 0,
            last_verification: now,
        }
    }

    /// Эффективный trust score (weighted combination)
    pub fn effective_trust(&self) -> f32 {
        let base = self.base_score;
        let reputation = self.reputation_history;
        let corroboration_bonus = (self.corroboration_count as f32 * 0.05).min(0.3);
        
        ((base * 0.5) + (reputation * 0.3) + corroboration_bonus).min(1.0).max(0.0)
    }
}

/// Стартовое доверие к источнику до накопления репутации.
/// Суб-агенты (`project:*`) — 0.5 (AgentInternal, не доверяются автоматически),
/// пользователь — выше, внешние источники — ниже, неизвестные — 0.3.
/// (по `docs/SUBAGENTS-MEMORY-INTEGRATION.md`)
fn default_base_score(source_id: &str, source_type: &SourceType) -> f32 {
    if source_id.starts_with("project:") {
        return 0.5;
    }
    match source_type {
        SourceType::UserDirect | SourceType::UserDocument => 0.6,
        SourceType::AgentInternal => 0.5,
        SourceType::WebSearch | SourceType::ExternalAPI => 0.4,
        SourceType::Unknown => 0.3,
    }
}

/// Решение фаервола
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TrustDecision {
    Accept,  // trust_score >= 0.8 AND firewall_confidence >= 0.7
    Verify,  // 0.4 <= trust_score < 0.8 OR anomaly detected
    Block,   // trust_score < 0.4 OR protected_action AND !verified
}

/// Итог гейта записи: с каким статусом писать узел, либо отказать.
#[derive(Debug, Clone)]
pub enum GateOutcome {
    /// Писать узел с этим статусом (`Active` — обычно, `Draft` — «на ревью»).
    Allow {
        status: Status,
        trust_score: f32,
        decision: TrustDecision,
        warning: Option<String>,
    },
    /// Не писать: явный сигнал манипуляции.
    Refuse { reason: String, trust_score: f32 },
}

/// Альтернативная гипотеза
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrustAlternative {
    pub hypothesis: String,
    pub probability: f32,
    pub supporting_evidence: Vec<String>,
}

/// Отчёт о доверии
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrustReport {
    pub trust_score: f32,              // 0.0-1.0
    pub firewall_confidence: f32,      // 0.0-1.0
    pub source_trust: SourceTrust,
    pub consistency_score: f32,
    pub verifiability_score: f32,
    pub intent_score: f32,
    pub tone_anomaly_detected: bool,
    pub action_blocked: bool,
    pub alternative_explanations: Vec<TrustAlternative>,
    pub recommendation: TrustDecision,
    pub message: String,
}

impl TrustReport {
    pub fn new(source_trust: SourceTrust) -> Self {
        Self {
            trust_score: source_trust.effective_trust(),
            firewall_confidence: 0.5,
            source_trust,
            consistency_score: 0.5,
            verifiability_score: 0.5,
            intent_score: 0.5,
            tone_anomaly_detected: false,
            action_blocked: false,
            alternative_explanations: vec![],
            recommendation: TrustDecision::Verify,
            message: String::new(),
        }
    }

    /// Определить рекомендацию на основе scores
    pub fn determine_recommendation(&mut self) {
        let trust = self.trust_score;
        let confidence = self.firewall_confidence;
        
        self.recommendation = if trust >= 0.8 && confidence >= 0.7 {
            TrustDecision::Accept
        } else if trust < 0.4 || (self.action_blocked && trust < 0.8) {
            TrustDecision::Block
        } else {
            TrustDecision::Verify
        };
    }
}

/// Репутация источника
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceReputation {
    pub source_id: String,
    pub total_claims: usize,
    pub verified_claims: usize,
    pub false_claims: usize,
}

impl SourceReputation {
    pub fn new(source_id: String) -> Self {
        Self {
            source_id,
            total_claims: 0,
            verified_claims: 0,
            false_claims: 0,
        }
    }

    pub fn accuracy(&self) -> f32 {
        if self.total_claims == 0 {
            return 0.5;
        }
        (self.verified_claims as f32) / (self.total_claims as f32)
    }

    pub fn update(&mut self, was_correct: bool) {
        self.total_claims += 1;
        if was_correct {
            self.verified_claims += 1;
        } else {
            self.false_claims += 1;
        }
    }
}

/// Калибровка фаервола
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirewallCalibration {
    pub trust_threshold_accept: f32,
    pub trust_threshold_verify: f32,
    pub confidence_threshold: f32,
    pub overall_accuracy: f32,
    pub total_assessments: usize,
    pub correct_assessments: usize,
}

impl Default for FirewallCalibration {
    fn default() -> Self {
        Self {
            trust_threshold_accept: 0.8,
            trust_threshold_verify: 0.4,
            confidence_threshold: 0.7,
            overall_accuracy: 0.5,
            total_assessments: 0,
            correct_assessments: 0,
        }
    }
}

/// TrustFirewall: защита памяти от манипуляций
pub struct TrustFirewall {
    /// Граф для проверки консистентности — общий backend через L2Actor
    /// (раньше отдельный in-memory `Graph`, пустой на MCP-контуре).
    l2: Arc<L2Actor>,
    /// Калибровка за `RwLock` (interior mutability): фаервол держится как
    /// `Arc<TrustFirewall>`, поэтому `recalibrate` не может брать `&mut self`.
    calibration: RwLock<FirewallCalibration>,
    /// Кэш доверия к источникам (source_id -> SourceTrust)
    source_trust_cache: RwLock<HashMap<String, SourceTrust>>,
    /// Репутация источников
    source_reputations: RwLock<HashMap<String, SourceReputation>>,
    /// Счётчики аномалий тона по пользователям
    tone_anomaly_counts: RwLock<HashMap<String, usize>>,
}

impl TrustFirewall {
    pub fn new(l2: Arc<L2Actor>) -> Self {
        Self {
            l2,
            calibration: RwLock::new(FirewallCalibration::default()),
            source_trust_cache: RwLock::new(HashMap::new()),
            source_reputations: RwLock::new(HashMap::new()),
            tone_anomaly_counts: RwLock::new(HashMap::new()),
        }
    }

    /// Верифицировать input от источника
    pub async fn verify_input(&self, source_id: &str, source_type: SourceType, content: &str) -> TrustReport {
        // 1. Получить или создать SourceTrust
        let source_trust = self.get_or_create_source_trust(source_id, source_type).await;
        
        let mut report = TrustReport::new(source_trust);
        
        // 2. Оценить консистентность с графом
        report.consistency_score = self.check_consistency(content).await;
        
        // 3. Оценить verifiability
        report.verifiability_score = self.assess_verifiability(content);
        
        // 4. Оценить intent
        report.intent_score = self.assess_intent(content);
        
        // 5. Проверить tone anomaly
        report.tone_anomaly_detected = self.detect_tone_anomaly(source_id, content).await;
        
        // 6. Пересчитать trust_score на основе всех сигналов
        report.trust_score = self.calculate_trust_score(&report);
        
        // 7. Установить firewall_confidence
        report.firewall_confidence = self.calibration.read().await.overall_accuracy;
        
        // 8. Определить рекомендацию
        report.determine_recommendation();
        
        // 9. Сгенерировать message
        report.message = format!(
            "Trust: {:.2}, Confidence: {:.2}, Decision: {:?}",
            report.trust_score, report.firewall_confidence, report.recommendation
        );
        
        report
    }

    /// Мягкий гейт записи. Прогоняет `verify_input` и маппит решение в действие.
    ///
    /// - `strict = false` — доверенный прямой путь (главный агент за пользователя):
    ///   пишем `Active`; в `Draft` роняем ТОЛЬКО при явной манипуляции; отказа нет.
    /// - `strict = true` — путь суб-агентской очереди: `Accept` → `Active`,
    ///   `Verify` → `Draft` (на ревью), `Block` → отказ при манипуляции, иначе `Draft`.
    ///
    /// «Явная манипуляция» = аномалия тона И низкий intent (`< 0.5`) — узкий сигнал,
    /// чтобы жёсткий отказ был редким (низкое доверие само по себе → Draft, не отказ).
    pub async fn gate(
        &self,
        source_id: &str,
        source_type: SourceType,
        content: &str,
        strict: bool,
    ) -> GateOutcome {
        let report = self.verify_input(source_id, source_type, content).await;
        let manipulative = report.tone_anomaly_detected && report.intent_score < 0.5;
        let score = report.trust_score;

        if !strict {
            return if manipulative {
                GateOutcome::Allow {
                    status: Status::Draft,
                    trust_score: score,
                    decision: report.recommendation,
                    warning: Some(format!(
                        "записано как Draft (на ревью): сигнал манипуляции, trust={score:.2}"
                    )),
                }
            } else {
                GateOutcome::Allow {
                    status: Status::Active,
                    trust_score: score,
                    decision: report.recommendation,
                    warning: None,
                }
            };
        }

        match report.recommendation {
            TrustDecision::Accept => GateOutcome::Allow {
                status: Status::Active,
                trust_score: score,
                decision: TrustDecision::Accept,
                warning: None,
            },
            TrustDecision::Verify => GateOutcome::Allow {
                status: Status::Draft,
                trust_score: score,
                decision: TrustDecision::Verify,
                warning: Some(format!("записано как Draft (на ревью): trust={score:.2}")),
            },
            TrustDecision::Block => {
                if manipulative {
                    GateOutcome::Refuse {
                        reason: format!(
                            "отклонено фаерволом: сигнал манипуляции, trust={score:.2}"
                        ),
                        trust_score: score,
                    }
                } else {
                    GateOutcome::Allow {
                        status: Status::Draft,
                        trust_score: score,
                        decision: TrustDecision::Block,
                        warning: Some(format!(
                            "записано как Draft (на ревью): низкое доверие, trust={score:.2}"
                        )),
                    }
                }
            }
        }
    }

    /// Получить или создать SourceTrust
    async fn get_or_create_source_trust(&self, source_id: &str, source_type: SourceType) -> SourceTrust {
        let cache = self.source_trust_cache.read().await;
        if let Some(trust) = cache.get(source_id) {
            return trust.clone();
        }
        drop(cache);
        
        let mut trust = SourceTrust::new(source_id.to_string(), source_type);
        trust.base_score = default_base_score(source_id, &trust.source_type);

        // Проверить репутацию
        let reputations = self.source_reputations.read().await;
        if let Some(rep) = reputations.get(source_id) {
            trust.reputation_history = rep.accuracy();
        }
        drop(reputations);
        
        // Кэшировать
        self.source_trust_cache.write().await.insert(source_id.to_string(), trust.clone());
        
        trust
    }

    /// Проверить консистентность с графом
    async fn check_consistency(&self, content: &str) -> f32 {
        // Упрощённая эвристика: если контент содержит противоречивые утверждения
        // В полной реализации: semantic similarity с существующими узлами
        
        let nodes = self.l2.list_all_nodes().await.unwrap_or_default();

        // Проверяем есть ли узлы с похожим content
        let content_lower = content.to_lowercase();
        let mut match_count = 0;

        for node in &nodes {
            let node_content_lower = node.content.to_lowercase();
            if content_lower.contains(&node_content_lower) || node_content_lower.contains(&content_lower) {
                match_count += 1;
            }
        }
        
        // Чем больше совпадений, тем выше consistency
        if match_count == 0 {
            0.3 // Новый контент, нет пересечений
        } else {
            let score = 0.5 + (match_count as f32 * 0.1);
            if score > 1.0 { 1.0 } else { score }
        }
    }

    /// Оценить verifiability
    fn assess_verifiability(&self, content: &str) -> f32 {
        // Эвристика: конкретные утверждения с числами/фактами более верифицируемы
        let has_numbers = content.chars().any(|c| c.is_numeric());
        let has_specific_terms = content.contains("по данным") 
            || content.contains("источник") 
            || content.contains("исследование");
        
        let mut score: f32 = 0.3;
        if has_numbers {
            score += 0.2;
        }
        if has_specific_terms {
            score += 0.3;
        }
        
        if score > 1.0 { 1.0 } else { score }
    }

    /// Оценить intent
    fn assess_intent(&self, content: &str) -> f32 {
        // Эвристика: детекция потенциально вредоносных интентов
        let suspicious_patterns = [
            "срочно", "немедленно", "без вопросов",
            "никто не узнает", "только между нами",
            "гарантирую", "точно", "абсолютно",
        ];
        
        let content_lower = content.to_lowercase();
        let suspicious_count = suspicious_patterns.iter()
            .filter(|pattern| content_lower.contains(*pattern))
            .count();
        
        if suspicious_count == 0 {
            0.8 // Нормальный интент
        } else {
            (0.8 - (suspicious_count as f32 * 0.15)).max(0.1)
        }
    }

    /// Детектировать tone anomaly
    async fn detect_tone_anomaly(&self, source_id: &str, content: &str) -> bool {
        // Упрощённая эвристика: если источник ранее имел аномалии
        let anomaly_counts = self.tone_anomaly_counts.read().await;
        let prev_anomalies = anomaly_counts.get(source_id).copied().unwrap_or(0);
        drop(anomaly_counts);
        
        // Детекция срочности/давления
        let urgent_patterns = ["срочно", "быстро", "немедленно", "сейчас же"];
        let content_lower = content.to_lowercase();
        let has_urgency = urgent_patterns.iter().any(|p| content_lower.contains(p));
        
        if has_urgency || prev_anomalies > 2 {
            // Обновить счётчик
            let mut counts = self.tone_anomaly_counts.write().await;
            *counts.entry(source_id.to_string()).or_insert(0) += 1;
            drop(counts);
            true
        } else {
            false
        }
    }

    /// Рассчитать итоговый trust score
    fn calculate_trust_score(&self, report: &TrustReport) -> f32 {
        // Weighted combination всех сигналов
        let source_weight = 0.3;
        let consistency_weight = 0.25;
        let verifiability_weight = 0.2;
        let intent_weight = 0.15;
        let tone_penalty = if report.tone_anomaly_detected { 0.1 } else { 0.0 };
        
        let score = (
            report.source_trust.effective_trust() * source_weight
            + report.consistency_score * consistency_weight
            + report.verifiability_score * verifiability_weight
            + report.intent_score * intent_weight
        ) - tone_penalty;
        
        score.clamp(0.0, 1.0)
    }

    /// Обновить репутацию источника
    pub async fn update_reputation(&self, source_id: &str, was_correct: bool) {
        let (accuracy, snapshot) = {
            let mut reputations = self.source_reputations.write().await;
            let rep = reputations.entry(source_id.to_string()).or_insert_with(|| {
                SourceReputation::new(source_id.to_string())
            });
            rep.update(was_correct);
            (rep.accuracy(), rep.clone())
        };

        // Обновить кэш доверия
        if let Some(trust) = self.source_trust_cache.write().await.get_mut(source_id) {
            trust.reputation_history = accuracy;
        }

        // Персист (переживает рестарт процесса)
        self.persist_reputation(&snapshot).await;
    }

    /// Рекалибровать фаервол после обратной связи.
    /// `&self` (не `&mut`) — доступно через `Arc<TrustFirewall>` благодаря `RwLock`.
    pub async fn recalibrate(&self, was_correct: bool) {
        {
            let mut cal = self.calibration.write().await;
            cal.total_assessments += 1;
            if was_correct {
                cal.correct_assessments += 1;
            }
            cal.overall_accuracy =
                cal.correct_assessments as f32 / cal.total_assessments as f32;
        }
        self.persist_calibration().await;
    }

    // --- Персистентность состояния фаервола (backend через L2Actor) ---

    /// Загрузить калибровку и репутации из backend. Вызывать один раз на старте
    /// (в `main.rs`, до передачи в handler), чтобы состояние переживало рестарт.
    pub async fn load_state(&self) {
        let backend = self.l2.backend();

        if let Ok(Some(bytes)) = backend.get(TRUST_CALIBRATION_KEY).await {
            if let Ok(cal) = serde_json::from_slice::<FirewallCalibration>(&bytes) {
                *self.calibration.write().await = cal;
            }
        }

        if let Ok(keys) = backend.list_keys(TRUST_REPUTATION_PREFIX).await {
            let mut reps = self.source_reputations.write().await;
            for key in keys {
                if let Ok(Some(bytes)) = backend.get(&key).await {
                    if let Ok(rep) = serde_json::from_slice::<SourceReputation>(&bytes) {
                        reps.insert(rep.source_id.clone(), rep);
                    }
                }
            }
        }
    }

    async fn persist_reputation(&self, rep: &SourceReputation) {
        let key = format!("{TRUST_REPUTATION_PREFIX}{}", rep.source_id);
        if let Ok(bytes) = serde_json::to_vec(rep) {
            let _ = self.l2.backend().put(&key, bytes).await;
        }
    }

    async fn persist_calibration(&self) {
        let cal = self.calibration.read().await.clone();
        if let Ok(bytes) = serde_json::to_vec(&cal) {
            let _ = self.l2.backend().put(TRUST_CALIBRATION_KEY, bytes).await;
        }
    }

    /// Получить отчёт о состоянии TrustFirewall
    pub async fn get_report(&self) -> TrustFirewallReport {
        let source_count = self.source_trust_cache.read().await.len();
        let reputation_count = self.source_reputations.read().await.len();
        
        TrustFirewallReport {
            total_sources: source_count,
            sources_with_reputation: reputation_count,
            calibration: self.calibration.read().await.clone(),
        }
    }
}

/// Отчёт о состоянии TrustFirewall
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrustFirewallReport {
    pub total_sources: usize,
    pub sources_with_reputation: usize,
    pub calibration: FirewallCalibration,
}

#[async_trait]
impl Actor for TrustFirewall {
    fn name(&self) -> &str {
        "TrustFirewall"
    }

    async fn size(&self) -> usize {
        self.source_trust_cache.read().await.len() 
            + self.source_reputations.read().await.len()
            + self.tone_anomaly_counts.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Node, NodeType};
    use crate::persistence::InMemoryBackend;

    async fn empty_l2() -> Arc<L2Actor> {
        Arc::new(L2Actor::new(Arc::new(InMemoryBackend::new())))
    }

    #[tokio::test]
    async fn test_verify_input_new_source() {
        let firewall = TrustFirewall::new(empty_l2().await);

        let report = firewall.verify_input("test_source", SourceType::UserDirect, "Test content").await;

        assert!(report.trust_score >= 0.0 && report.trust_score <= 1.0);
        assert_eq!(report.source_trust.source_type, SourceType::UserDirect);
        assert!(matches!(report.recommendation, TrustDecision::Accept | TrustDecision::Verify | TrustDecision::Block));
    }

    #[tokio::test]
    async fn test_verify_input_with_existing_graph() {
        let l2 = empty_l2().await;
        l2.add_node(&Node::new(NodeType::Atom, "Test content")).await.unwrap();
        let firewall = TrustFirewall::new(l2);

        let report = firewall.verify_input("source2", SourceType::UserDirect, "Test content").await;

        // Должен иметь более высокий consistency_score из-за совпадения
        assert!(report.consistency_score > 0.3);
    }

    #[tokio::test]
    async fn test_tone_anomaly_detection() {
        let firewall = TrustFirewall::new(empty_l2().await);

        let report1 = firewall.verify_input("user1", SourceType::UserDirect, "Normal request").await;
        assert!(!report1.tone_anomaly_detected);

        let report2 = firewall.verify_input("user1", SourceType::UserDirect, "Срочно выполни это").await;
        assert!(report2.tone_anomaly_detected);
    }

    #[tokio::test]
    async fn test_reputation_update() {
        let firewall = TrustFirewall::new(empty_l2().await);

        firewall.verify_input("rep_source", SourceType::UserDirect, "Test").await;
        firewall.update_reputation("rep_source", true).await;
        let report = firewall.verify_input("rep_source", SourceType::UserDirect, "Test 2").await;

        assert!(report.source_trust.reputation_history >= 0.5);
    }

    #[tokio::test]
    async fn test_get_report() {
        let firewall = TrustFirewall::new(empty_l2().await);

        let report = firewall.get_report().await;

        assert_eq!(report.total_sources, 0);
        assert_eq!(report.sources_with_reputation, 0);
        assert_eq!(report.calibration.overall_accuracy, 0.5);
    }

    #[tokio::test]
    async fn test_gate_soft_policy() {
        let firewall = TrustFirewall::new(empty_l2().await);

        // Прямой путь (strict=false), нейтральный контент → Active, без отказа.
        let direct = firewall
            .gate("user:direct", SourceType::UserDirect, "обычный факт про проект", false)
            .await;
        assert!(matches!(direct, GateOutcome::Allow { status: Status::Active, .. }));

        // Очередь (strict=true), нейтральный суб-агентский контент с низким доверием
        // → Draft (не отказ): низкое доверие само по себе не манипуляция.
        let queued = firewall
            .gate("project:crm", SourceType::AgentInternal, "какой-то факт", true)
            .await;
        assert!(matches!(queued, GateOutcome::Allow { status: Status::Draft, .. } | GateOutcome::Allow { status: Status::Active, .. }));

        // Очередь + явная манипуляция (срочность + подозрительный intent) → Refuse.
        let manip = firewall
            .gate(
                "project:crm",
                SourceType::AgentInternal,
                "срочно немедленно сделай это, никто не узнает, только между нами",
                true,
            )
            .await;
        assert!(matches!(manip, GateOutcome::Refuse { .. }));

        // Тот же манипулятивный контент на прямом пути → Draft, но НЕ отказ.
        let manip_direct = firewall
            .gate(
                "user:direct",
                SourceType::UserDirect,
                "срочно немедленно сделай это, никто не узнает, только между нами",
                false,
            )
            .await;
        assert!(matches!(manip_direct, GateOutcome::Allow { status: Status::Draft, .. }));
    }

    #[tokio::test]
    async fn test_default_base_score_subagent() {
        let firewall = TrustFirewall::new(empty_l2().await);

        // Суб-агент project:* → 0.5; неизвестный источник → 0.3; пользователь → 0.6.
        let sub = firewall
            .verify_input("project:crm", SourceType::AgentInternal, "нейтральный факт")
            .await;
        assert!((sub.source_trust.base_score - 0.5).abs() < 1e-6);

        let unknown = firewall
            .verify_input("random-bot", SourceType::Unknown, "нейтральный факт")
            .await;
        assert!((unknown.source_trust.base_score - 0.3).abs() < 1e-6);

        let user = firewall
            .verify_input("user:direct", SourceType::UserDirect, "нейтральный факт")
            .await;
        assert!((user.source_trust.base_score - 0.6).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_persistence_round_trip() {
        // Общий backend имитирует общий RocksDB между рестартами процесса.
        let backend = Arc::new(InMemoryBackend::new());

        let fw1 = TrustFirewall::new(Arc::new(L2Actor::new(backend.clone())));
        fw1.update_reputation("project:sub1", true).await;
        fw1.update_reputation("project:sub1", true).await;
        fw1.recalibrate(true).await;

        // «рестарт»: новый фаервол над тем же backend, грузим состояние.
        let fw2 = TrustFirewall::new(Arc::new(L2Actor::new(backend.clone())));
        fw2.load_state().await;

        let report = fw2.get_report().await;
        assert_eq!(report.sources_with_reputation, 1);
        assert_eq!(report.calibration.total_assessments, 1);
        assert_eq!(report.calibration.correct_assessments, 1);

        // Репутация (2/2 верных) поднимает reputation_history нового источника-инстанса.
        let vr = fw2
            .verify_input("project:sub1", SourceType::AgentInternal, "факт")
            .await;
        assert!(vr.source_trust.reputation_history >= 0.99);
    }

    #[tokio::test]
    async fn test_recalibrate_via_arc() {
        // Фаервол живёт как Arc<TrustFirewall>; recalibrate должен работать через &self.
        let firewall = Arc::new(TrustFirewall::new(empty_l2().await));

        let before = firewall.get_report().await.calibration.overall_accuracy;
        assert_eq!(before, 0.5); // default

        firewall.recalibrate(true).await;
        firewall.recalibrate(true).await;
        firewall.recalibrate(false).await;

        let after = firewall.get_report().await.calibration;
        assert_eq!(after.total_assessments, 3);
        assert_eq!(after.correct_assessments, 2);
        assert!((after.overall_accuracy - (2.0 / 3.0)).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_verifiability_assessment() {
        let firewall = TrustFirewall::new(empty_l2().await);

        let score1 = firewall.assess_verifiability("5 пользователей сообщили о проблеме");
        let score2 = firewall.assess_verifiability("что-то произошло");

        assert!(score1 > score2);
    }
}
