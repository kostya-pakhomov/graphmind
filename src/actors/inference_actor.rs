// InferenceActor — автономные сценарии: dream_reflection, predict_risks, find_contradictions

use crate::actors::causal_engine::{CausalEngine, Contradiction, ConflictType, InferenceContext, InferredRelation};
use crate::actors::chain::{ChainActor, ChainResult};
use crate::actors::l2::L2Actor;
use crate::actors::llm_client::LlmClient;
use crate::graph::{Node, NodeId, NodeType, Relation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Результат dream_reflection — сгенерированные правила
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratedRule {
    /// ID цепочки-источника
    pub source_chain: String,
    /// Условие (IF)
    pub if_conditions: Vec<String>,
    /// Следствие (THEN)
    pub then_effect: String,
    /// Уровень обобщения
    pub generalization_level: GeneralizationLevel,
    /// Уверенность правила
    pub confidence: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GeneralizationLevel {
    Narrow,   // специфично для одного workspace
    Medium,   // применимо к нескольким проектам
    Broad,    // универсальное правило (GKL)
}

/// Результат predict_risks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskPrediction {
    /// Узел-причина
    pub cause_id: NodeId,
    /// Предсказанные эффекты
    pub predicted_effects: Vec<PredictedEffect>,
    /// Общая оценка риска
    pub risk_level: RiskLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictedEffect {
    pub description: String,
    pub confidence: f32,
    pub severity: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

/// InferenceActor — автономные причинные сценарии
pub struct InferenceActor {
    l2: Arc<L2Actor>,
    chain: ChainActor,
    /// LLM для смыслового причинного вывода. При None/Disabled — эвристический фолбэк.
    llm: Option<LlmClient>,
}

impl InferenceActor {
    pub fn new(l2: Arc<L2Actor>) -> Self {
        let chain = ChainActor::new(Arc::clone(&l2));
        Self { l2, chain, llm: None }
    }

    /// Подключить LLM для причинного инференса (find_contradictions / predict_risks /
    /// dream_reflection / propose_causal_link). Образец — `L1Actor::with_llm` (волна D).
    pub fn with_llm(mut self, llm: LlmClient) -> Self {
        self.llm = Some(llm);
        self
    }

    /// LLM, если подключён И включён (провайдер не Disabled/Mock, есть base_url).
    fn llm(&self) -> Option<&LlmClient> {
        self.llm.as_ref().filter(|c| c.is_enabled())
    }

    /// Dream reflection — анализ цепочек и генерация правил
    pub async fn dream_reflection(&self) -> anyhow::Result<Vec<GeneratedRule>> {
        let mut rules = Vec::new();

        // 1. Собираем все cause узлы
        let cause_nodes = self.l2.list_by_type(NodeType::Cause).await?;
        
        // 2. Для каждого cause ищем цепочку до effect
        let mut pattern_counts: HashMap<String, Vec<&Node>> = HashMap::new();
        
        for cause in &cause_nodes {
            let chain_result = self.chain.chain_forward_pre(&cause.id, 3).await?;
            
            if !chain_result.entries.is_empty() {
                let pattern_key = Self::extract_pattern_key(&cause.content);
                pattern_counts.entry(pattern_key).or_insert_with(Vec::new).push(cause);
            }
        }

        // 3. Для паттернов с 3+ вхождениями генерируем правила
        for (pattern_key, causes) in pattern_counts.iter() {
            if causes.len() >= 3 {
                let rule = self.generate_rule_from_pattern(&pattern_key, &causes).await?;
                rules.push(rule);
            }
        }

        Ok(rules)
    }

    /// Predict risks — прогноз рисков от изменения.
    /// Кандидаты-эффекты берём из цепочки; severity с LLM оценивается по смыслу
    /// (без LLM — эвристика по ключевым словам).
    pub async fn predict_risks(&self, cause_id: &NodeId) -> anyhow::Result<RiskPrediction> {
        let chain_result = self.chain.chain_forward_pre(cause_id, 3).await?;

        let mut predicted_effects = Vec::new();
        for entry in &chain_result.entries {
            if let Some(node) = self.l2.get_node(&entry.node_id).await? {
                if node.node_type == NodeType::Effect {
                    let severity = Self::assess_severity(&node.content);
                    let confidence: f32 = self.assess_confidence(&entry, &chain_result);
                    predicted_effects.push(PredictedEffect {
                        description: node.content.clone(),
                        confidence,
                        severity,
                    });
                }
            }
        }

        // LLM переоценивает severity по смыслу (эвристика keyword — базлайн/фолбэк).
        if let Some(llm) = self.llm() {
            if !predicted_effects.is_empty() {
                if let Some(cause) = self.l2.get_node(cause_id).await? {
                    Self::llm_rate_severities(llm, &cause, &mut predicted_effects).await;
                }
            }
        }

        let max_severity = predicted_effects
            .iter()
            .map(|e| e.severity)
            .fold(0.0f32, f32::max);
        let risk_level = match max_severity {
            s if s >= 0.8 => RiskLevel::Critical,
            s if s >= 0.6 => RiskLevel::High,
            s if s >= 0.4 => RiskLevel::Medium,
            _ => RiskLevel::Low,
        };

        Ok(RiskPrediction {
            cause_id: cause_id.clone(),
            predicted_effects,
            risk_level,
        })
    }

    /// LLM переоценивает severity каждого эффекта по смыслу (in-place).
    async fn llm_rate_severities(llm: &LlmClient, cause: &Node, effects: &mut [PredictedEffect]) {
        let mut prompt = format!(
            "Причина: «{}»\n\nОцени СЕРЬЁЗНОСТЬ каждого последствия по смыслу \
             (0.0 незначимо … 1.0 критично):\n\n",
            cause.content
        );
        for (i, e) in effects.iter().enumerate() {
            prompt.push_str(&format!("{i}: {}\n", e.description));
        }
        prompt.push_str("\nВерни ТОЛЬКО JSON-массив: [{\"i\": <индекс>, \"severity\": <0..1>}].");
        let system = "Ты — риск-аналитик. Оцениваешь серьёзность последствий по смыслу. Строго JSON.";
        let resp = match llm.chat(system, &prompt).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("predict_risks LLM severity failed: {e} — эвристика");
                return;
            }
        };
        let (start, end) = match (resp.find('['), resp.rfind(']')) {
            (Some(s), Some(e)) if e > s => (s, e),
            _ => return,
        };
        let arr = match serde_json::from_str::<serde_json::Value>(&resp[start..=end]) {
            Ok(v) => v,
            Err(_) => return,
        };
        if let Some(items) = arr.as_array() {
            for it in items {
                if let (Some(i), Some(sev)) = (
                    it.get("i").and_then(|x| x.as_u64()),
                    it.get("severity").and_then(|x| x.as_f64()),
                ) {
                    if let Some(e) = effects.get_mut(i as usize) {
                        e.severity = (sev as f32).clamp(0.0, 1.0);
                    }
                }
            }
        }
    }

    /// Find contradictions — поиск противоречий во всех узлах.
    /// С LLM: смысловые противоречия (разными словами). Без — эвристика (5 антонимов).
    pub async fn find_contradictions(&self) -> anyhow::Result<Vec<Contradiction>> {
        let mut all_nodes = Vec::new();
        all_nodes.extend(self.l2.list_by_type(NodeType::Atom).await?);
        all_nodes.extend(self.l2.list_by_type(NodeType::Cause).await?);
        all_nodes.extend(self.l2.list_by_type(NodeType::Effect).await?);
        all_nodes.extend(self.l2.list_by_type(NodeType::Rule).await?);

        if let Some(llm) = self.llm() {
            // Ограничиваем размер промпта (индексация 1:1 с subset).
            let subset: Vec<&Node> = all_nodes.iter().take(40).collect();
            if subset.len() >= 2 {
                let system = "Ты — аналитик знаний. Находишь СМЫСЛОВЫЕ противоречия между \
                              утверждениями — по смыслу, а не по совпадению слов. Отвечай строго JSON-массивом.";
                let prompt = Self::build_contradictions_prompt(&subset);
                match llm.chat(system, &prompt).await {
                    Ok(resp) => {
                        let found = Self::parse_contradictions_response(&resp, &subset);
                        if !found.is_empty() {
                            return Ok(found);
                        }
                        // Пусто/непарсибельно → эвристика как страховка.
                    }
                    Err(e) => {
                        tracing::warn!("find_contradictions LLM failed: {e} — фолбэк на эвристику");
                    }
                }
            }
        }
        Ok(CausalEngine::find_contradictions(&all_nodes))
    }

    fn build_contradictions_prompt(nodes: &[&Node]) -> String {
        let mut s = String::from(
            "Ниже пронумерованный список утверждений из памяти. Найди ПАРЫ, которые смыслово \
             противоречат друг другу (одно отрицает/исключает другое), даже если сформулированы \
             разными словами.\n\n",
        );
        for (i, n) in nodes.iter().enumerate() {
            s.push_str(&format!("{i}: {}\n", n.content));
        }
        s.push_str(
            "\nВключай пару ТОЛЬКО если утверждения ДЕЙСТВИТЕЛЬНО несовместимы — одно исключает \
             другое. Если они лишь дополняют друг друга, согласуются или связаны причинно — НЕ включай. \
             При сомнении не включай.\n\
             Верни ТОЛЬКО JSON-массив: [{\"a\": <индекс>, \"b\": <индекс>, \
             \"explanation\": \"<кратко почему несовместимы>\"}]. Если противоречий нет — верни [].",
        );
        s
    }

    fn parse_contradictions_response(response: &str, nodes: &[&Node]) -> Vec<Contradiction> {
        let (start, end) = match (response.find('['), response.rfind(']')) {
            (Some(s), Some(e)) if e > s => (s, e),
            _ => return Vec::new(),
        };
        let parsed: serde_json::Value = match serde_json::from_str(&response[start..=end]) {
            Ok(v) => v,
            Err(_) => return Vec::new(),
        };
        let arr = match parsed.as_array() {
            Some(a) => a,
            None => return Vec::new(),
        };
        let mut out = Vec::new();
        for pair in arr {
            let a = pair.get("a").and_then(|v| v.as_u64());
            let b = pair.get("b").and_then(|v| v.as_u64());
            let expl = pair
                .get("explanation")
                .and_then(|v| v.as_str())
                .unwrap_or("смысловое противоречие")
                .to_string();
            if let (Some(a), Some(b)) = (a, b) {
                if a != b {
                    if let (Some(na), Some(nb)) = (nodes.get(a as usize), nodes.get(b as usize)) {
                        out.push(Contradiction {
                            node_a: na.id.clone(),
                            node_b: nb.id.clone(),
                            conflict_type: ConflictType::SemanticOpposition,
                            explanation: expl,
                        });
                    }
                }
            }
        }
        out
    }

    /// Предложить причинную связь между двумя узлами (Трек 4). LLM-инференс по смыслу;
    /// фолбэк — статическая эвристика `CausalEngine::infer_relation`. НЕ создаёт ребро —
    /// только предлагает (человек подтверждает через `link_nodes`).
    pub async fn propose_causal_link(
        &self,
        source_id: &NodeId,
        target_id: &NodeId,
    ) -> anyhow::Result<InferredRelation> {
        let source = self
            .l2
            .get_node(source_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("source node not found: {}", source_id.0))?;
        let target = self
            .l2
            .get_node(target_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("target node not found: {}", target_id.0))?;

        if let Some(llm) = self.llm() {
            let system = "Ты — аналитик причинности. Определяешь тип связи между двумя \
                          утверждениями ПО СМЫСЛУ. Отвечай строго JSON-объектом.";
            let prompt = Self::build_relation_prompt(&source, &target);
            match llm.chat(system, &prompt).await {
                Ok(resp) => {
                    if let Some(rel) = Self::parse_relation_response(&resp) {
                        return Ok(rel);
                    }
                }
                Err(e) => tracing::warn!("propose_causal_link LLM failed: {e} — фолбэк на эвристику"),
            }
        }
        let ctx = InferenceContext {
            source_node: source,
            target_node: target,
            context: None,
        };
        Ok(CausalEngine::infer_relation(&ctx))
    }

    fn build_relation_prompt(source: &Node, target: &Node) -> String {
        format!(
            "Источник: «{}»\nЦель: «{}»\n\nКакая связь ведёт от источника к цели? Выбери ОДИН тип:\n\
             - leads_to (источник приводит к цели: причина→следствие)\n\
             - explained_by (цель объясняет источник)\n\
             - inhibits (источник препятствует цели)\n\
             - derived_from (источник выведен из цели)\n\
             - contradicts (источник противоречит цели)\n\
             - related_to (связаны, но без причинности)\n\n\
             Верни ТОЛЬКО JSON: {{\"relation\": \"<тип>\", \"confidence\": <0..1>, \"reasoning\": \"<кратко>\"}}.",
            source.content, target.content
        )
    }

    fn parse_relation_response(response: &str) -> Option<InferredRelation> {
        let (start, end) = match (response.find('{'), response.rfind('}')) {
            (Some(s), Some(e)) if e > s => (s, e),
            _ => return None,
        };
        let v: serde_json::Value = serde_json::from_str(&response[start..=end]).ok()?;
        let rel_str = v.get("relation").and_then(|x| x.as_str())?;
        let relation = match rel_str.to_lowercase().replace('-', "_").as_str() {
            "leads_to" | "leadsto" => Relation::LeadsTo,
            "explained_by" | "explainedby" => Relation::ExplainedBy,
            "inhibits" => Relation::Inhibits,
            "derived_from" | "derivedfrom" => Relation::DerivedFrom,
            "contradicts" => Relation::Contradicts,
            _ => Relation::RelatedTo,
        };
        let confidence = v.get("confidence").and_then(|x| x.as_f64()).unwrap_or(0.5) as f32;
        let reasoning = v
            .get("reasoning")
            .and_then(|x| x.as_str())
            .unwrap_or("LLM-инференс")
            .to_string();
        Some(InferredRelation {
            relation,
            confidence: confidence.clamp(0.0, 1.0),
            reasoning,
        })
    }

    async fn generate_rule_from_pattern(&self, pattern_key: &str, causes: &[&Node]) -> anyhow::Result<GeneratedRule> {
        let generalization_level = match causes.len() {
            n if n >= 10 => GeneralizationLevel::Broad,
            n if n >= 5 => GeneralizationLevel::Medium,
            _ => GeneralizationLevel::Narrow,
        };
        let confidence = ((causes.len() as f32 / 20.0).min(1.0) * 100.0).round() / 100.0;

        // LLM обобщает похожие причины в осмысленное правило IF/THEN.
        if let Some(llm) = self.llm() {
            let mut prompt =
                String::from("Ниже похожие ПРИЧИНЫ из памяти. Обобщи их в одно правило IF … THEN … .\n\n");
            for c in causes {
                prompt.push_str(&format!("- {}\n", c.content));
            }
            prompt.push_str(
                "\nВерни ТОЛЬКО JSON: {\"if_conditions\": [\"...\"], \"then_effect\": \"...\"}.",
            );
            let system = "Ты — аналитик паттернов. Обобщаешь причины в правило IF/THEN по смыслу. Строго JSON.";
            if let Ok(resp) = llm.chat(system, &prompt).await {
                if let Some((ifs, then)) = Self::parse_rule_response(&resp) {
                    return Ok(GeneratedRule {
                        source_chain: format!("pattern_{}", pattern_key),
                        if_conditions: ifs,
                        then_effect: then,
                        generalization_level,
                        confidence,
                    });
                }
            }
        }

        // Фолбэк — механическая склейка первых слов.
        let mut if_conditions = Vec::new();
        for cause in causes {
            let words: Vec<&str> = cause.content.split_whitespace().take(2).collect();
            if !words.is_empty() {
                if_conditions.push(words.join(" "));
            }
        }
        if_conditions.sort();
        if_conditions.dedup();

        Ok(GeneratedRule {
            source_chain: format!("pattern_{}", pattern_key),
            if_conditions,
            then_effect: format!("IF {} THEN potential issue detected", pattern_key),
            generalization_level,
            confidence,
        })
    }

    fn parse_rule_response(response: &str) -> Option<(Vec<String>, String)> {
        let (start, end) = match (response.find('{'), response.rfind('}')) {
            (Some(s), Some(e)) if e > s => (s, e),
            _ => return None,
        };
        let v: serde_json::Value = serde_json::from_str(&response[start..=end]).ok()?;
        let then = v.get("then_effect").and_then(|x| x.as_str())?.to_string();
        if then.is_empty() {
            return None;
        }
        let ifs = v
            .get("if_conditions")
            .and_then(|x| x.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect::<Vec<_>>())
            .unwrap_or_default();
        Some((ifs, then))
    }

    fn extract_pattern_key(content: &str) -> String {
        content
            .to_lowercase()
            .split_whitespace()
            .take(3)
            .collect::<Vec<_>>()
            .join("_")
    }

    fn assess_severity(content: &str) -> f32 {
        let lower = content.to_lowercase();
        
        let critical_keywords = ["crash", "outage", "data loss", "security", "авария", "потеря данных"];
        let high_keywords = ["error", "fail", "bug", "broken", "ошибка", "сломан"];
        let medium_keywords = ["slow", "delay", "warning", "медленно", "предупреждение"];
        
        for kw in &critical_keywords {
            if lower.contains(kw) { return 0.9; }
        }
        for kw in &high_keywords {
            if lower.contains(kw) { return 0.7; }
        }
        for kw in &medium_keywords {
            if lower.contains(kw) { return 0.5; }
        }
        
        0.3
    }

    fn assess_confidence(&self, entry: &crate::actors::chain::ChainEntry, chain: &ChainResult) -> f32 {
        let depth_factor = 1.0 - (entry.depth as f32 * 0.15);
        let chain_factor = if chain.reached_root { 1.1 } else { 1.0 };
        ((depth_factor * chain_factor).clamp(0.1, 1.0) * 100.0).round() / 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::l2::L2Actor;
    use crate::graph::Node;
    use crate::persistence::InMemoryBackend;
    use std::sync::Arc;

    async fn create_test_inference_actor() -> InferenceActor {
        let backend = Arc::new(InMemoryBackend::new());
        let l2 = Arc::new(L2Actor::new(backend));
        InferenceActor::new(l2)
    }

    #[tokio::test]
    async fn test_dream_reflection_empty() {
        let actor = create_test_inference_actor().await;
        let rules = actor.dream_reflection().await.unwrap();
        assert!(rules.is_empty());
    }

    #[tokio::test]
    async fn test_predict_risks_empty_chain() {
        let actor = create_test_inference_actor().await;
        
        let cause_id = actor.l2.add_node(
            &Node::new(NodeType::Cause, "Test cause")
        ).await.unwrap();
        
        let prediction = actor.predict_risks(&cause_id).await.unwrap();
        assert_eq!(prediction.cause_id, cause_id);
        assert!(prediction.predicted_effects.is_empty());
        assert_eq!(prediction.risk_level, RiskLevel::Low);
    }

    #[tokio::test]
    async fn test_find_contradictions_empty() {
        let actor = create_test_inference_actor().await;
        let contradictions = actor.find_contradictions().await.unwrap();
        assert!(contradictions.is_empty());
    }

    #[test]
    fn test_extract_pattern_key() {
        assert_eq!(
            InferenceActor::extract_pattern_key("Docker cache invalidation bug"),
            "docker_cache_invalidation"
        );
    }

    #[test]
    fn test_assess_severity_critical() {
        assert!(InferenceActor::assess_severity("Production crash due to memory leak") >= 0.8);
    }

    #[test]
    fn test_assess_severity_high() {
        let severity = InferenceActor::assess_severity("API error rate increased");
        assert!(severity >= 0.6 && severity < 0.8);
    }

    #[test]
    fn test_assess_severity_medium() {
        let severity = InferenceActor::assess_severity("Slow response times");
        assert!(severity >= 0.4 && severity < 0.6);
    }

    #[test]
    fn test_assess_severity_low() {
        let severity = InferenceActor::assess_severity("Feature update deployed");
        assert!(severity < 0.4);
    }

    #[test]
    fn test_parse_contradictions_response() {
        let nodes = vec![
            Node::new(NodeType::Effect, "деплой прошёл успешно"),
            Node::new(NodeType::Effect, "выкатка откатилась с ошибкой"),
        ];
        let refs: Vec<&Node> = nodes.iter().collect();
        // LLM часто оборачивает в ```json — парсер должен резать по [ … ]
        let resp = "Вот результат:\n```json\n[{\"a\":0,\"b\":1,\"explanation\":\"одно об успехе, другое об откате\"}]\n```";
        let out = InferenceActor::parse_contradictions_response(resp, &refs);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].node_a, nodes[0].id);
        assert_eq!(out[0].node_b, nodes[1].id);
    }

    #[test]
    fn test_parse_contradictions_empty_and_bad() {
        let nodes = vec![Node::new(NodeType::Atom, "x")];
        let refs: Vec<&Node> = nodes.iter().collect();
        assert!(InferenceActor::parse_contradictions_response("[]", &refs).is_empty());
        assert!(InferenceActor::parse_contradictions_response("no json here", &refs).is_empty());
    }

    #[test]
    fn test_parse_relation_response() {
        let r = InferenceActor::parse_relation_response(
            "{\"relation\": \"leads_to\", \"confidence\": 0.82, \"reasoning\": \"причина ведёт к следствию\"}",
        )
        .unwrap();
        assert!(matches!(r.relation, Relation::LeadsTo));
        assert!((r.confidence - 0.82).abs() < 1e-4);
    }

    #[test]
    fn test_parse_relation_unknown_defaults_related() {
        let r = InferenceActor::parse_relation_response("{\"relation\": \"whatever\"}").unwrap();
        assert!(matches!(r.relation, Relation::RelatedTo));
    }

    #[test]
    fn test_parse_rule_response() {
        let (ifs, then) = InferenceActor::parse_rule_response(
            "{\"if_conditions\": [\"кэш не сбрасывается\"], \"then_effect\": \"потеря записей при сбое\"}",
        )
        .unwrap();
        assert_eq!(ifs.len(), 1);
        assert_eq!(then, "потеря записей при сбое");
    }
}
