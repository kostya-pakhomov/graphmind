// CausalEngine — ядро причинной логики
// infer_relation, calibrate_confidence, find_contradictions

use crate::graph::{Node, NodeId, NodeType, Relation};
use serde::{Deserialize, Serialize};

/// Контекст для логического вывода связи
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceContext {
    /// Исходный узел (причина или эффект)
    pub source_node: Node,
    /// Целевой узел
    pub target_node: Node,
    /// Дополнительный контекст (например, текст действия)
    pub context: Option<String>,
}

/// Результат вывода связи
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferredRelation {
    /// Тип связи
    pub relation: Relation,
    /// Уверенность (0.0–1.0)
    pub confidence: f32,
    /// Обоснование вывода
    pub reasoning: String,
}

/// Результат калибровки уверенности
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibratedConfidence {
    /// Исходная уверенность
    pub original: f32,
    /// Откалиброванная уверенность
    pub calibrated: f32,
    /// Факторы калибровки
    pub factors: Vec<String>,
}

/// Результат поиска противоречий
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Contradiction {
    pub node_a: NodeId,
    pub node_b: NodeId,
    pub conflict_type: ConflictType,
    pub explanation: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictType {
    /// Узлы имеют противоположные содержания
    SemanticOpposition,
    /// Одна причина ведёт к разным эффектам
    DivergentEffects,
    /// Разные причины объясняют один эффект
    CompetingCauses,
    /// Правило противоречит причине
    RuleViolatesCause,
}

/// CausalEngine — причинная логика
pub struct CausalEngine;

impl CausalEngine {
    /// Вывод типа связи между узлами на основе их типов и содержания
    /// 
    /// Алгоритм:
    /// 1. Определяем типы узлов (cause/effect/rule/atom)
    /// 2. Анализируем семантику содержания
    /// 3. Применяем эвристики из 07-CAUSAL-ENGINE.md
    pub fn infer_relation(context: &InferenceContext) -> InferredRelation {
        let source_type = context.source_node.node_type;
        let target_type = context.target_node.node_type;

        // Эвристика 1: cause → effect через leads_to
        if source_type == NodeType::Cause && target_type == NodeType::Effect {
            let confidence = Self::semantic_similarity(&context.source_node.content, &context.target_node.content);
            return InferredRelation {
                relation: Relation::LeadsTo,
                confidence,
                reasoning: format!(
                    "Cause→Effect: семантическая близость {:.2}",
                    confidence
                ),
            };
        }

        // Эвристика 2: effect → cause через explained_by
        if source_type == NodeType::Effect && target_type == NodeType::Cause {
            let confidence = Self::semantic_similarity(&context.source_node.content, &context.target_node.content);
            return InferredRelation {
                relation: Relation::ExplainedBy,
                confidence,
                reasoning: format!(
                    "Effect←Cause: обратная связь, уверенность {:.2}",
                    confidence
                ),
            };
        }

        // Эвристика 3: rule → cause through inhibits
        if source_type == NodeType::Rule && target_type == NodeType::Cause {
            let inhibits = Self::contains_prevention_keywords(&context.source_node.content);
            return InferredRelation {
                relation: if inhibits { Relation::Inhibits } else { Relation::DerivedFrom },
                confidence: if inhibits { 0.85 } else { 0.6 },
                reasoning: if inhibits {
                    "Rule содержит prevention keywords → inhibits Cause".to_string()
                } else {
                    "Rule → DerivedFrom Cause (нет явных prevention keywords)".to_string()
                },
            };
        }

        // Эвристика 4: rule → effect through derived_from
        if source_type == NodeType::Rule && target_type == NodeType::Effect {
            return InferredRelation {
                relation: Relation::DerivedFrom,
                confidence: 0.7,
                reasoning: "Rule → Effect: правило описывает следствие".to_string(),
            };
        }

        // Эвристика 5: atom → atom through related_to (по умолчанию)
        if source_type == NodeType::Atom && target_type == NodeType::Atom {
            let similarity = Self::semantic_similarity(&context.source_node.content, &context.target_node.content);
            return InferredRelation {
                relation: Relation::RelatedTo,
                confidence: similarity * 0.8,
                reasoning: format!(
                    "Atom→Atom: семантическая связь {:.2}",
                    similarity
                ),
            };
        }

        // По умолчанию — related_to с низкой уверенностью
        InferredRelation {
            relation: Relation::RelatedTo,
            confidence: 0.3,
            reasoning: "Нет явного паттерна — default related_to".to_string(),
        }
    }

    /// Калибровка уверенности на основе источников и доказательств
    pub fn calibrate_confidence(
        original_confidence: f32,
        source: Option<&str>,
        evidence_count: usize,
        is_consistent: bool,
    ) -> CalibratedConfidence {
        let mut factors = Vec::new();
        let mut multiplier = 1.0;

        // Фактор 1: источник
        let source_factor = match source {
            Some("observed") => {
                factors.push("source=observed (+0.2)".to_string());
                1.2
            }
            Some("derived") => {
                factors.push("source=derived (+0.1)".to_string());
                1.1
            }
            Some("hypothesized") => {
                factors.push("source=hypothesized (-0.2)".to_string());
                0.8
            }
            _ => {
                factors.push("source=unknown (×1.0)".to_string());
                1.0
            }
        };
        multiplier *= source_factor;

        // Фактор 2: количество доказательств
        let evidence_factor = if evidence_count >= 3 {
            factors.push(format!("evidence_count={} (+0.15)", evidence_count));
            1.15
        } else if evidence_count >= 1 {
            factors.push(format!("evidence_count={} (+0.05)", evidence_count));
            1.05
        } else {
            factors.push("evidence_count=0 (−0.1)".to_string());
            0.9
        };
        multiplier *= evidence_factor;

        // Фактор 3: непротиворечивость
        if is_consistent {
            factors.push("consistent (+0.1)".to_string());
            multiplier *= 1.1;
        } else {
            factors.push("inconsistent (−0.15)".to_string());
            multiplier *= 0.85;
        }

        let calibrated = ((original_confidence * multiplier).clamp(0.0, 1.0) * 100.0).round() / 100.0;

        CalibratedConfidence {
            original: original_confidence,
            calibrated,
            factors,
        }
    }

    /// Поиск противоречий между узлами
    pub fn find_contradictions(nodes: &[Node]) -> Vec<Contradiction> {
        let mut contradictions = Vec::new();

        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                if let Some(contradiction) = Self::check_pair(&nodes[i], &nodes[j]) {
                    contradictions.push(contradiction);
                }
            }
        }

        contradictions
    }

    /// Проверка пары узлов на противоречие
    fn check_pair(a: &Node, b: &Node) -> Option<Contradiction> {
        // Проверка 1: семантическая оппозиция
        if Self::are_semantically_opposite(&a.content, &b.content) {
            return Some(Contradiction {
                node_a: a.id.clone(),
                node_b: b.id.clone(),
                conflict_type: ConflictType::SemanticOpposition,
                explanation: "Узлы содержат противоположные утверждения".to_string(),
            });
        }

        // Проверка 2: rule vs cause
        if a.node_type == NodeType::Rule && b.node_type == NodeType::Cause {
            if Self::contains_prevention_keywords(&a.content) {
                return Some(Contradiction {
                    node_a: a.id.clone(),
                    node_b: b.id.clone(),
                    conflict_type: ConflictType::RuleViolatesCause,
                    explanation: "Rule ингибирует Cause".to_string(),
                });
            }
        }

        None
    }

    /// Семантическая близость текстов (упрощённая эвристика Jaccard)
    fn semantic_similarity(a: &str, b: &str) -> f32 {
        let a_lower = a.to_lowercase();
        let b_lower = b.to_lowercase();
        let words_a: std::collections::HashSet<_> = a_lower.split_whitespace().collect();
        let words_b: std::collections::HashSet<_> = b_lower.split_whitespace().collect();

        if words_a.is_empty() || words_b.is_empty() {
            return 0.0;
        }

        let intersection = words_a.intersection(&words_b).count();
        let union = words_a.union(&words_b).count();

        if union == 0 {
            return 0.0;
        }

        (intersection as f32 / union as f32).clamp(0.1, 0.95)
    }

    /// Проверка на противоположность содержаний
    fn are_semantically_opposite(a: &str, b: &str) -> bool {
        let a_lower = a.to_lowercase();
        let b_lower = b.to_lowercase();

        let opposition_pairs = [
            ("работает", "не работает"),
            ("успех", "провал"),
            ("исправлен", "сломан"),
            ("включён", "выключен"),
            ("есть", "нет"),
        ];

        for (word_a, word_b) in &opposition_pairs {
            if (a_lower.contains(word_a) && b_lower.contains(word_b))
                || (a_lower.contains(word_b) && b_lower.contains(word_a))
            {
                return true;
            }
        }

        false
    }

    /// Проверка на ключевые слова предотвращения
    fn contains_prevention_keywords(content: &str) -> bool {
        let lower = content.to_lowercase();
        let prevention_keywords = [
            "всегда добавляй",
            "никогда не",
            "обязательно",
            "запрети",
            "избегай",
            "предотврати",
            "не используй",
            "always",
            "never",
            "avoid",
            "prevent",
            "don't use",
            "must not",
        ];

        prevention_keywords.iter().any(|kw| lower.contains(kw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Node;

    #[test]
    fn test_infer_relation_cause_to_effect() {
        let context = InferenceContext {
            source_node: Node::new(NodeType::Cause, "Docker cache не инвалидируется"),
            target_node: Node::new(NodeType::Effect, "Docker образ содержит старую версию"),
            context: None,
        };

        let result = CausalEngine::infer_relation(&context);
        assert_eq!(result.relation, Relation::LeadsTo);
        assert!(result.confidence > 0.0);
        assert!(!result.reasoning.is_empty());
    }

    #[test]
    fn test_infer_relation_effect_to_cause() {
        let context = InferenceContext {
            source_node: Node::new(NodeType::Effect, "Пользователи видят баги"),
            target_node: Node::new(NodeType::Cause, "Тесты не покрывают edge cases"),
            context: None,
        };

        let result = CausalEngine::infer_relation(&context);
        assert_eq!(result.relation, Relation::ExplainedBy);
    }

    #[test]
    fn test_infer_relation_rule_to_cause_with_prevention() {
        let context = InferenceContext {
            source_node: Node::new(NodeType::Rule, "IF Docker build THEN always add --no-cache"),
            target_node: Node::new(NodeType::Cause, "Docker cache не инвалидируется"),
            context: None,
        };

        let result = CausalEngine::infer_relation(&context);
        assert_eq!(result.relation, Relation::Inhibits);
        assert!(result.confidence >= 0.8);
    }

    #[test]
    fn test_calibrate_confidence_observed_with_evidence() {
        let result = CausalEngine::calibrate_confidence(
            0.7,
            Some("observed"),
            3,
            true,
        );

        assert!(result.calibrated > result.original);
        assert!(result.calibrated <= 1.0);
        assert_eq!(result.factors.len(), 3);
    }

    #[test]
    fn test_calibrate_confidence_hypothesized_no_evidence() {
        let result = CausalEngine::calibrate_confidence(
            0.6,
            Some("hypothesized"),
            0,
            false,
        );

        assert!(result.calibrated < result.original);
        assert!(result.calibrated >= 0.0);
    }

    #[test]
    fn test_find_contradictions_semantic_opposition() {
        let nodes = vec![
            Node::new(NodeType::Atom, "Сервис работает"),
            Node::new(NodeType::Atom, "Сервис не работает"),
        ];

        let contradictions = CausalEngine::find_contradictions(&nodes);
        assert_eq!(contradictions.len(), 1);
        assert_eq!(contradictions[0].conflict_type, ConflictType::SemanticOpposition);
    }

    #[test]
    fn test_contains_prevention_keywords() {
        assert!(CausalEngine::contains_prevention_keywords("Always add --no-cache"));
        assert!(CausalEngine::contains_prevention_keywords("Никогда не используй ENV для паролей"));
        assert!(!CausalEngine::contains_prevention_keywords("Просто сделай это"));
    }

    #[test]
    fn test_semantic_similarity_identical() {
        let sim = CausalEngine::semantic_similarity("Docker cache invalidation", "Docker cache invalidation");
        assert!(sim >= 0.9);
    }

    #[test]
    fn test_semantic_similarity_different() {
        let sim = CausalEngine::semantic_similarity("Docker cache", "Python package");
        assert!(sim < 0.5);
    }
}
