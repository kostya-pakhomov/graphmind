//! OrchestratorActor — минимальная версия для V2.0
//!
//! Отвечает за:
//! - Оценку размера задачи (estimate_size)
//! - Генерацию подзадач через LLM (generate_children)
//! - Интеграцию с plan_decompose

use std::env;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use super::LlmClient;

/// Убрать reasoning-блоки `<think>…</think>` из ответа LLM (qwen3, deepseek-R и др.
/// reasoning-модели их эмитят перед финальным ответом). Регистронезависимо; если
/// закрывающего тега нет — режем от `<think>` до конца (незакрытый reasoning).
fn strip_reasoning(text: &str) -> String {
    let lower = text.to_lowercase();
    let mut out = String::with_capacity(text.len());
    let mut i = 0usize;
    while let Some(rel) = lower[i..].find("<think>") {
        let open = i + rel;
        out.push_str(&text[i..open]);
        match lower[open..].find("</think>") {
            Some(crel) => {
                i = open + crel + "</think>".len();
            }
            None => {
                i = text.len(); // незакрытый тег — отбросить хвост
                break;
            }
        }
    }
    out.push_str(&text[i..]);
    out.trim().to_string()
}

/// Категория размера задачи для оценки глубины декомпозиции.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeCategory {
    /// Tiny: не требует декомпозиции (depth = 0)
    Tiny,
    /// Small: P0 → P1 (depth = 1)
    Small,
    /// Medium: P0 → P1 → P2 (depth = 2)
    Medium,
    /// Large: P0 → P1 → P2 → P3 (depth = 3)
    Large,
}

impl SizeCategory {
    /// Глубина декомпозиции из текущей категории.
    pub fn depth(&self) -> usize {
        match self {
            SizeCategory::Tiny => 0,
            SizeCategory::Small => 1,
            SizeCategory::Medium => 2,
            SizeCategory::Large => 3,
        }
    }
}

/// Минимальный OrchestratorActor для V2.0.
///
/// В V2.0 предоставляет только эвристики и LLM-генерацию.
/// В V2.1 добавится EventBus, PolicyEngine, BudgetTracker.
pub struct OrchestratorActor {
    /// Единый LLM-клиент: нормализация /v1, гейт провайдера, таймаут из конфига.
    llm: LlmClient,
    /// Test-only: очередь предзаписанных ответов LLM (для unit-тестов
    /// `PlanActor::plan_decompose`, чтобы не мокать HTTP).
    #[cfg(test)]
    pub test_responses: std::sync::Mutex<Vec<Vec<String>>>,
}

impl OrchestratorActor {
    /// Создать OrchestratorActor из переменных окружения.
    pub fn from_env() -> Self {
        // Те же имена, что в Config::from_env (actors/config.rs:327-329),
        // иначе Config::validate() зарежет запуск, а Orchestrator всё равно
        // читает по другим именам → silent LLM-fallback.
        let llm_base_url = env::var("GRAPHMIND_LLM_BASE_URL")
            .unwrap_or_else(|_| "http://localhost:8000".to_string());
        let llm_api_key = env::var("GRAPHMIND_LLM_API_KEY").ok();
        let llm_model = env::var("GRAPHMIND_LLM_MODEL")
            .unwrap_or_else(|_| "kat-coder-pro-v2".to_string());
        let llm_timeout = env::var("GRAPHMIND_LLM_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);

        Self {
            llm: LlmClient::new(llm_base_url, llm_api_key, llm_model).with_timeout(llm_timeout),
            #[cfg(test)]
            test_responses: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Создать с явными параметрами (для тестов).
    pub fn new(llm_base_url: String, llm_api_key: Option<String>, llm_model: String) -> Self {
        Self {
            llm: LlmClient::new(llm_base_url, llm_api_key, llm_model),
            #[cfg(test)]
            test_responses: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Test-only: зарегистрировать очередь предзаписанных ответов LLM.
    /// `generate_children` сначала дёрнет очередь, и только если она пуста —
    /// пойдёт в сеть. Это позволяет unit-тестам plan_actor проверять оба
    /// сценария: LLM-success и LLM-error, без моков HTTP.
    #[cfg(test)]
    pub fn with_test_responses(self, responses: Vec<Vec<String>>) -> Self {
        *self.test_responses.lock().unwrap() = responses;
        self
    }

    /// Test-only: добавить один предзаписанный ответ (для пошаговых тестов).
    #[cfg(test)]
    pub fn add_test_response(&self, response: Vec<String>) {
        self.test_responses.lock().unwrap().push(response);
    }

    /// Оценить размер задачи по описанию (эвристика V2.0).
    ///
    /// Критерии:
    /// - Длина описания
    /// - Наличие action words (создать, сделать, реализовать)
    /// - Количество доменов (оценивается по ключевым словам)
    pub fn estimate_size(&self, description: &str) -> SizeCategory {
        let len = description.len();
        let has_action_words = description.contains("создать")
            || description.contains("сделать")
            || description.contains("реализовать")
            || description.contains("implement")
            || description.contains("build")
            || description.contains("create");
        
        // Подсчёт "доменов" по ключевым словам
        let domain_keywords = [
            "frontend", "backend", "database", "api", "ui", "ux",
            "mobile", "web", "devops", "security", "analytics",
            "фронтенд", "бэкенд", "база данных", "мобильный", "веб",
        ];
        let domain_count = domain_keywords
            .iter()
            .filter(|&&kw| description.to_lowercase().contains(kw))
            .count();

        if len <= 50 && !has_action_words {
            SizeCategory::Tiny
        } else if len <= 200 || domain_count <= 1 {
            SizeCategory::Small
        } else if len <= 1000 || domain_count <= 3 {
            SizeCategory::Medium
        } else {
            SizeCategory::Large
        }
    }

    /// Определить глубину декомпозиции для P0 на основе размера.
    pub fn depth_for_size(&self, size: SizeCategory) -> usize {
        size.depth()
    }

    /// Рекомендуемое количество подзадач для данной глубины.
    fn suggested_count(&self, depth: usize) -> usize {
        match depth {
            1 => 3, // P1: 2-4 задачи
            2 => 3, // P2: 2-3 задачи
            3 => 2, // P3: 1-2 задачи
            _ => 2,
        }
    }

    /// Сгенерировать подзадачи через LLM.
    ///
    /// Возвращает список описаний подзадач.
    /// В V2.0 используется синхронный вызов (без EventBus).
    pub async fn generate_children(&self, parent_description: &str, depth: usize) -> Result<Vec<String>> {
        let count = self.suggested_count(depth);
        let level_name = match depth {
            1 => "P1 (стратегические направления)",
            2 => "P2 (конкретные задачи)",
            3 => "P3 (подзадачи для выполнения)",
            _ => "подзадачи",
        };

        let prompt = format!(
            "Декомпозируй задачу '{}' на {} осмысленных подзадач уровня {}.\n\
             Требования:\n\
             1. Каждая подзадача должна быть конкретной и измеримой\n\
             2. Избегай общих фраз вроде 'сделать то же самое'\n\
             3. Подзадачи должны покрывать разные аспекты родительской задачи\n\
             4. Формулируй как императивы (сделай X, создай Y)\n\
             5. Верни ТОЛЬКО список подзадач, по одной на строку, без нумерации и маркеров\n\
             6. Никаких дополнительных объяснений, только список\n\
             \
             Пример формата вывода:\n\
             Создать структуру проекта\n\
             Реализовать базовый API\n\
             Настроить CI/CD пайплайн",
            parent_description, count, level_name
        );

        // Вызов LLM через HTTP
        let response_text = self.call_llm(&prompt).await?;

        // Парсинг ответа: каждая строка — подзадача. Устойчиво к разным моделям:
        // reasoning-модели (qwen/deepseek-R) эмитят <think>…</think>; многие отдают
        // список маркерами (- * •) или в code-fence, несмотря на «без маркеров».
        // Раньше строки с маркерами ОТБРАСЫВАЛИСЬ → у bullet-модели 0 детей → ошибка.
        // Теперь маркеры/нумерацию СНИМАЕМ и оставляем содержимое.
        let children: Vec<String> = strip_reasoning(&response_text)
            .lines()
            .map(|line| line.trim())
            .filter(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with("```"))
            .map(|line| {
                // Снять маркеры списка, затем нумерацию «1.» / «2)»
                line.trim_start_matches(['-', '*', '•', '·', '–', '—'])
                    .trim()
                    .trim_start_matches(|c: char| c.is_ascii_digit())
                    .trim_start_matches(['.', ')'])
                    .trim()
                    .to_string()
            })
            // Отсеять служебные преамбулы вроде «Вот список:» (кончаются двоеточием, без сути)
            .filter(|line| !line.is_empty() && !(line.ends_with(':') && line.chars().count() < 40))
            .collect();

        // Критическая ошибка: LLM не вернул осмысленных подзадач
        if children.is_empty() {
            return Err(anyhow::anyhow!(
                "LLM вернул пустой результат при декомпозиции задачи '{}'. Возможные причины: неверный API endpoint, истёкший ключ, некорректный prompt или проблема с парсингом ответа.",
                parent_description
            ));
        }

        Ok(children)
    }

    /// Вызов LLM через HTTP (kodikRouter API).
    async fn call_llm(&self, prompt: &str) -> Result<String> {
        // Test-only: сначала отдаём предзаписанный ответ (если есть).
        #[cfg(test)]
        {
            if let Some(next) = self.test_responses.lock().unwrap().pop() {
                return Ok(next.join("\n"));
            }
        }
        // Единый клиент: нормализация /v1, гейт провайдера, таймаут из конфига.
        self.llm
            .chat(
                "Ты помощник по декомпозиции задач. Твоя цель — разбивать большие задачи на осмысленные подзадачи.",
                prompt,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_reasoning_removes_think_block() {
        let s = "<think>рассуждаю долго и нудно</think>\nСоздать API\nНастроить CI";
        assert_eq!(strip_reasoning(s), "Создать API\nНастроить CI");
    }

    #[test]
    fn strip_reasoning_handles_unclosed_and_absent() {
        assert_eq!(strip_reasoning("просто список"), "просто список");
        assert_eq!(strip_reasoning("до <think>после без закрытия"), "до");
    }

    #[test]
    fn test_estimate_size_tiny() {
        let actor = OrchestratorActor::new("http://localhost".to_string(), None, "test".to_string());
        let size = actor.estimate_size("Тест");
        assert_eq!(size, SizeCategory::Tiny);
    }

    #[test]
    fn test_estimate_size_small() {
        let actor = OrchestratorActor::new("http://localhost".to_string(), None, "test".to_string());
        let size = actor.estimate_size("Создать документацию");
        assert_eq!(size, SizeCategory::Small);
    }

    #[test]
    fn test_estimate_size_medium() {
        let actor = OrchestratorActor::new("http://localhost".to_string(), None, "test".to_string());
        let desc = "Создать документацию GraphMind с примерами использования, API reference и руководством по настройке";
        let size = actor.estimate_size(desc);
        assert_eq!(size, SizeCategory::Medium);
    }

    #[test]
    fn test_estimate_size_large() {
        let actor = OrchestratorActor::new("http://localhost".to_string(), None, "test".to_string());
        let desc = "Создать полную документацию GraphMind включая frontend, backend, базу данных, API, мобильное приложение, веб-интерфейс, DevOps и безопасность";
        let size = actor.estimate_size(desc);
        assert_eq!(size, SizeCategory::Large);
    }

    #[test]
    fn test_depth_for_size() {
        let actor = OrchestratorActor::new("http://localhost".to_string(), None, "test".to_string());
        assert_eq!(actor.depth_for_size(SizeCategory::Tiny), 0);
        assert_eq!(actor.depth_for_size(SizeCategory::Small), 1);
        assert_eq!(actor.depth_for_size(SizeCategory::Medium), 2);
        assert_eq!(actor.depth_for_size(SizeCategory::Large), 3);
    }
}
