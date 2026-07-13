//! LlmClient — единый клиент к OpenAI-совместимому LLM.
//!
//! Строится из `Config.llm` (единая точка конфигурации env). Один и тот же клиент
//! используют обе LLM-точки — ось планирования (`OrchestratorActor.generate_children`)
//! и консолидатор (именование/группировка L1/L0) — чтобы не расходились три вещи,
//! которые раньше были разными в разных местах:
//!
//! 1. Нормализация `/v1`. `Config` хранит `base_url` уже с `/v1`
//!    (`https://api.openai.com/v1`), а старый `OrchestratorActor::call_llm` дописывал
//!    `/v1` ещё раз → `https://routerai.ru/v1/v1/chat/completions`. Здесь суффикс
//!    добавляется ровно один раз.
//! 2. Гейт по провайдеру. При `Disabled`/`Mock` в сеть не ходим (раньше оркестратор
//!    игнорировал `GRAPHMIND_LLM_PROVIDER` и всё равно бил в дефолтный URL).
//! 3. Таймаут из конфига (`GRAPHMIND_LLM_TIMEOUT_SECS`), а не захардкоженные 30с.

use anyhow::Result;
use super::config::{LlmConfig, LlmProvider};

/// Клиент к chat-completions endpoint (OpenAI-формат: openai / openai-compatible /
/// openrouter; RouterAI проксирует и anthropic-модели этим же форматом).
#[derive(Clone)]
pub struct LlmClient {
    provider: LlmProvider,
    base_url: String,
    api_key: Option<String>,
    model: String,
    timeout_secs: u64,
}

impl LlmClient {
    /// Канонический путь — собрать из `Config.llm`.
    pub fn from_config(cfg: &LlmConfig) -> Self {
        Self {
            provider: cfg.provider.clone(),
            base_url: cfg.base_url.clone().unwrap_or_default(),
            api_key: cfg.api_key.clone(),
            model: cfg.model.clone(),
            timeout_secs: cfg.timeout_secs,
        }
    }

    /// Явные параметры — совместимость с `OrchestratorActor::new`/`from_env` и тестами.
    /// Раз base_url задан явно, провайдер считаем openai-compatible.
    pub fn new(base_url: String, api_key: Option<String>, model: String) -> Self {
        Self {
            provider: LlmProvider::OpenAICompatible,
            base_url,
            api_key,
            model,
            timeout_secs: 30,
        }
    }

    /// Переопределить таймаут (сек). Нужно `OrchestratorActor::from_env`, чтобы честно
    /// применять `GRAPHMIND_LLM_TIMEOUT_SECS`: иначе медленные локальные модели
    /// (qwen на Ollama, холодная загрузка) рвутся на захардкоженных 30с.
    pub fn with_timeout(mut self, secs: u64) -> Self {
        if secs > 0 {
            self.timeout_secs = secs;
        }
        self
    }

    /// LLM реально доступен: провайдер не Disabled/Mock и есть base_url.
    /// Консолидатор проверяет это перед вызовом, чтобы при выключенном LLM
    /// откатиться на эвристику, а не падать в сеть.
    pub fn is_enabled(&self) -> bool {
        !matches!(self.provider, LlmProvider::Disabled | LlmProvider::Mock)
            && !self.base_url.is_empty()
    }

    /// Полный URL chat-completions с нормализацией `/v1`: если base уже
    /// оканчивается на `/v1`, второй раз не добавляем.
    fn chat_completions_url(&self) -> String {
        let base = self.base_url.trim_end_matches('/');
        if base.ends_with("/v1") {
            format!("{}/chat/completions", base)
        } else {
            format!("{}/v1/chat/completions", base)
        }
    }

    /// Один вызов chat-completions. `system` — системный промпт, `user` — запрос.
    /// Возвращает `choices[0].message.content`.
    pub async fn chat(&self, system: &str, user: &str) -> Result<String> {
        if !self.is_enabled() {
            anyhow::bail!(
                "LLM отключён (provider={:?}) — вызов невозможен. Задай \
                 GRAPHMIND_LLM_PROVIDER + GRAPHMIND_LLM_BASE_URL (см. config-and-env.md).",
                self.provider
            );
        }

        let client = reqwest::Client::new();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        if let Some(key) = &self.api_key {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {}", key))?,
            );
        }

        let body = serde_json::json!({
            "model": self.model,
            "messages": [
                { "role": "system", "content": system },
                { "role": "user", "content": user }
            ],
            "temperature": 0.7,
            "max_tokens": 1000
        });

        let url = self.chat_completions_url();
        let response = client
            .post(&url)
            .headers(headers)
            .json(&body)
            .timeout(std::time::Duration::from_secs(self.timeout_secs))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("LLM API error ({}): {}", status, text);
        }

        let json: serde_json::Value = response.json().await?;
        let content = json
            .get("choices")
            .and_then(|c| c.as_array())
            .and_then(|arr| arr.first())
            .and_then(|first| first.get("message"))
            .and_then(|msg| msg.get("content"))
            .and_then(|c| c.as_str())
            .ok_or_else(|| anyhow::anyhow!("Unexpected LLM response format: {:?}", json))?;

        Ok(content.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_does_not_double_v1_when_base_has_v1() {
        let c = LlmClient::new("https://routerai.ru/v1".into(), None, "m".into());
        assert_eq!(c.chat_completions_url(), "https://routerai.ru/v1/chat/completions");
    }

    #[test]
    fn url_adds_v1_when_base_lacks_it() {
        let c = LlmClient::new("http://localhost:8000".into(), None, "m".into());
        assert_eq!(c.chat_completions_url(), "http://localhost:8000/v1/chat/completions");
    }

    #[test]
    fn url_handles_trailing_slash() {
        let c = LlmClient::new("https://routerai.ru/v1/".into(), None, "m".into());
        assert_eq!(c.chat_completions_url(), "https://routerai.ru/v1/chat/completions");
    }

    #[test]
    fn disabled_provider_is_not_enabled() {
        let cfg = LlmConfig { provider: LlmProvider::Disabled, ..Default::default() };
        assert!(!LlmClient::from_config(&cfg).is_enabled());
    }
}
