//! EmbeddingProvider — реальные эмбеддинги через OpenAI-совместимый endpoint.
//!
//! Заменяет `SearchActor::generate_dummy_embedding` (char-bag заглушка, нулевой смысл).
//! Провайдер `OpenAI` бьёт в `{base}/v1/embeddings` — тот же endpoint, что и LLM
//! (RouterAI отдаёт и chat, и embeddings), поэтому base_url/api_key берём из `Config.llm`,
//! а модель/размерность — из `Config.embedding`.
//!
//! Про русский корпус: локальный дефолт GraphMind — англоязычный `all-MiniLM-L6-v2`,
//! который для русского не годится (см. решение `decision_memory_files_not_vectors`).
//! Поэтому вариант `Local` здесь СОЗНАТЕЛЬНО не подключён — рекомендуется `OpenAI`
//! с многоязычной моделью на RouterAI. `Disabled`/`Local` → is_enabled()=false → в
//! SearchActor остаётся keyword/char-bag fallback (без сети).

use anyhow::Result;
use super::config::{EmbeddingConfig, EmbeddingProviderKind, LlmConfig};

#[derive(Clone)]
pub struct EmbeddingProvider {
    enabled: bool,
    base_url: String,
    api_key: Option<String>,
    model: String,
    dim: usize,
    timeout_secs: u64,
}

impl EmbeddingProvider {
    /// Собрать из конфигов. OpenAI-совместимый endpoint: `embedding.base_url` при наличии,
    /// иначе фолбэк на `llm.base_url` (RouterAI отдаёт и chat, и embeddings). Ключ — так же.
    /// Развязка нужна, когда LLM и эмбеддинги на разных серверах (LLM=RouterAI, эмбеддинги=Ollama).
    /// Модель/размерность — из `Config.embedding`. Включён только при provider=OpenAI +
    /// непустой base_url + модель.
    pub fn from_config(emb: &EmbeddingConfig, llm: &LlmConfig) -> Self {
        let base_url = emb
            .base_url
            .clone()
            .or_else(|| llm.base_url.clone())
            .unwrap_or_default();
        let api_key = emb.api_key.clone().or_else(|| llm.api_key.clone());
        let enabled = matches!(emb.provider, EmbeddingProviderKind::OpenAI)
            && !base_url.is_empty()
            && !emb.model.is_empty();
        Self {
            enabled,
            base_url,
            api_key,
            model: emb.model.clone(),
            dim: if emb.dim > 0 { emb.dim } else { 1536 },
            timeout_secs: if llm.timeout_secs > 0 { llm.timeout_secs } else { 30 },
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Метка бэкенда для честного ответа инструментов (было «char_bag_fallback»).
    pub fn backend_label(&self) -> &'static str {
        if self.enabled {
            "openai-compatible"
        } else {
            "char_bag_fallback"
        }
    }

    fn embeddings_url(&self) -> String {
        let base = self.base_url.trim_end_matches('/');
        if base.ends_with("/v1") {
            format!("{}/embeddings", base)
        } else {
            format!("{}/v1/embeddings", base)
        }
    }

    /// Получить эмбеддинг текста. Ошибка → вызывающий откатится на char-bag.
    pub async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        if !self.enabled {
            anyhow::bail!("EmbeddingProvider disabled");
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
        let body = serde_json::json!({ "model": self.model, "input": text });
        let response = client
            .post(self.embeddings_url())
            .headers(headers)
            .json(&body)
            .timeout(std::time::Duration::from_secs(self.timeout_secs))
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let txt = response.text().await.unwrap_or_default();
            anyhow::bail!("Embeddings API error ({}): {}", status, txt);
        }
        let json: serde_json::Value = response.json().await?;
        Self::parse_embedding_response(&json)
            .ok_or_else(|| anyhow::anyhow!("Unexpected embeddings response: {:?}", json))
    }

    /// Разбор ответа `{data:[{embedding:[...]}]}` (чистая функция — юнит-тест без сети).
    pub fn parse_embedding_response(json: &serde_json::Value) -> Option<Vec<f32>> {
        let arr = json
            .get("data")?
            .as_array()?
            .first()?
            .get("embedding")?
            .as_array()?;
        let vec: Vec<f32> = arr.iter().filter_map(|v| v.as_f64().map(|f| f as f32)).collect();
        if vec.is_empty() {
            None
        } else {
            Some(vec)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_openai_embedding_response() {
        let json = serde_json::json!({
            "data": [{"embedding": [0.1, 0.2, 0.3], "index": 0}],
            "model": "m"
        });
        let v = EmbeddingProvider::parse_embedding_response(&json).unwrap();
        assert_eq!(v, vec![0.1f32, 0.2, 0.3]);
    }

    #[test]
    fn bad_response_returns_none() {
        assert!(EmbeddingProvider::parse_embedding_response(&serde_json::json!({"x": 1})).is_none());
        assert!(EmbeddingProvider::parse_embedding_response(&serde_json::json!({"data": []})).is_none());
    }

    #[test]
    fn url_normalizes_v1() {
        let cfg_emb = EmbeddingConfig { provider: EmbeddingProviderKind::OpenAI, model: "m".into(), dim: 3, ..Default::default() };
        let cfg_llm = LlmConfig { base_url: Some("https://routerai.ru/v1".into()), ..Default::default() };
        let p = EmbeddingProvider::from_config(&cfg_emb, &cfg_llm);
        assert!(p.is_enabled());
        assert_eq!(p.embeddings_url(), "https://routerai.ru/v1/embeddings");
    }

    #[test]
    fn disabled_when_provider_not_openai() {
        let cfg_emb = EmbeddingConfig { provider: EmbeddingProviderKind::Disabled, ..Default::default() };
        let cfg_llm = LlmConfig { base_url: Some("https://x/v1".into()), ..Default::default() };
        assert!(!EmbeddingProvider::from_config(&cfg_emb, &cfg_llm).is_enabled());
    }

    #[test]
    fn embedding_endpoint_overrides_llm() {
        // LLM на RouterAI, эмбеддинги — на локальной Ollama: развязка эндпоинтов.
        let cfg_emb = EmbeddingConfig {
            provider: EmbeddingProviderKind::OpenAI,
            model: "bge-m3".into(),
            dim: 1024,
            base_url: Some("http://localhost:11434/v1".into()),
            api_key: Some("ollama".into()),
            ..Default::default()
        };
        let cfg_llm = LlmConfig {
            base_url: Some("https://routerai.ru/api/v1".into()),
            api_key: Some("router-secret".into()),
            ..Default::default()
        };
        let p = EmbeddingProvider::from_config(&cfg_emb, &cfg_llm);
        assert!(p.is_enabled());
        assert_eq!(p.dim(), 1024);
        assert_eq!(p.embeddings_url(), "http://localhost:11434/v1/embeddings");
        assert_eq!(p.api_key.as_deref(), Some("ollama"));
    }

    #[test]
    fn embedding_falls_back_to_llm_endpoint() {
        // Без отдельного embedding.base_url — берём LLM (обратная совместимость со стендом ai-dev).
        let cfg_emb = EmbeddingConfig {
            provider: EmbeddingProviderKind::OpenAI,
            model: "m".into(),
            dim: 3,
            ..Default::default()
        };
        let cfg_llm = LlmConfig {
            base_url: Some("https://routerai.ru/api/v1".into()),
            api_key: Some("router-secret".into()),
            ..Default::default()
        };
        let p = EmbeddingProvider::from_config(&cfg_emb, &cfg_llm);
        assert_eq!(p.embeddings_url(), "https://routerai.ru/api/v1/embeddings");
        assert_eq!(p.api_key.as_deref(), Some("router-secret"));
    }
}
