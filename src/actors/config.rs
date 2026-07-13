//! Config actor — единая точка загрузки и валидации env-переменных.
//!
//! Конфигурация GraphMind v2.

use std::env;
use std::path::PathBuf;
use anyhow::{Context, Result, bail};

/// Режим работы MCP-сервера.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum McpMode {
    #[default]
    Dev,
    Production,
    Test,
}

/// Тип persistence backend.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum BackendKind {
    #[default]
    RocksDb,
    File,
    InMemory,
}

/// LLM провайдер.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum LlmProvider {
    OpenAI,
    OpenAICompatible,
    Anthropic,
    OpenRouter,
    Mock,
    #[default]
    Disabled,
}

/// Конфигурация LLM.
#[derive(Debug, Clone)]
pub struct LlmConfig {
    pub provider: LlmProvider,
    pub api_key: Option<String>,
    pub model: String,
    pub base_url: Option<String>,
    pub budget_per_hour: u32,
    pub timeout_secs: u64,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            provider: LlmProvider::Disabled,
            api_key: None,
            model: String::new(),
            base_url: None,
            budget_per_hour: 1000,
            timeout_secs: 30,
        }
    }
}

/// Конфигурация embedding-провайдера.
#[derive(Debug, Clone, PartialEq)]
pub enum EmbeddingProviderKind {
    Local,
    OpenAI,
    Disabled,
}

impl Default for EmbeddingProviderKind {
    fn default() -> Self {
        Self::Disabled
    }
}

/// Конфигурация embeddings.
#[derive(Debug, Clone, Default)]
pub struct EmbeddingConfig {
    pub provider: EmbeddingProviderKind,
    pub model: String,
    pub dim: usize,
    pub batch_size: usize,
    pub cache_enabled: bool,
    pub cache_max_entries: usize,
    pub local_model_path: Option<PathBuf>,
    pub offline: bool,
    /// Отдельный endpoint эмбеддингов (развязка с LLM). Если None — фолбэк на `Config.llm.base_url`.
    /// Нужно, когда LLM и эмбеддинги живут на разных серверах (напр. LLM=RouterAI, эмбеддинги=Ollama).
    pub base_url: Option<String>,
    /// Отдельный ключ эмбеддингов. Если None — фолбэк на `Config.llm.api_key`.
    pub api_key: Option<String>,
}

/// Конфигурация RocksDB.
#[derive(Debug, Clone)]
pub struct RocksDbConfig {
    pub data_dir: PathBuf,
    pub compression: bool,
    pub parallelism: i32,
    pub memtable_size_mb: u32,
    pub use_fsync: bool,
}

impl Default for RocksDbConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from(".kodik/memory/workspace-default"),
            compression: true,
            parallelism: 0,
            memtable_size_mb: 128,
            use_fsync: false,
        }
    }
}

/// Конфигурация очереди.
#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub interval_secs: u64,
    pub file_path: PathBuf,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            interval_secs: 30,
            file_path: PathBuf::from("pending_actions.json"),
        }
    }
}

/// Основная конфигурация GraphMind.
#[derive(Debug, Clone, Default)]
pub struct Config {
    // Режим
    pub mcp_mode: McpMode,
    pub mcp_http: bool,
    pub mcp_http_addr: String,

    // Persistence
    pub backend: BackendKind,
    pub data_dir: PathBuf,

    // Workspace
    pub workspace_root: PathBuf,

    // gRPC / HTTP
    pub grpc_addr: Option<std::net::SocketAddr>,

    // Логи
    pub mcp_log_level: String,
    pub mcp_log_file: Option<PathBuf>,

    // LLM
    pub llm: LlmConfig,

    // Embeddings
    pub embedding: EmbeddingConfig,

    // Persistence tuning
    pub rocksdb: RocksDbConfig,

    // Queue
    pub queue: QueueConfig,
}

impl Config {
    /// Загрузить конфигурацию из переменных окружения.
    pub fn from_env() -> Result<Self> {
        let data_dir = PathBuf::from(
            env::var("GRAPHMIND_DATA_DIR")
                .unwrap_or_else(|_| ".kodik/memory/workspace-default".into())
        );

        Ok(Self {
            mcp_mode: parse_mcp_mode(),
            mcp_http: env_bool("GRAPHMIND_MCP_HTTP"),
            mcp_http_addr: env::var("GRAPHMIND_MCP_HTTP_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:50052".into()),

            backend: parse_backend(),
            data_dir: data_dir.clone(),

            workspace_root: PathBuf::from(
                env::var("GRAPHMIND_WORKSPACE")
                    .unwrap_or_else(|_| {
                        env::current_dir()
                            .map(|p| p.to_string_lossy().into())
                            .unwrap_or_else(|_| ".".into())
                    })
            ),

            grpc_addr: env::var("GRAPHMIND_GRPC_ADDR").ok()
                .and_then(|s| s.parse().ok()),

            mcp_log_level: env::var("GRAPHMIND_MCP_LOG_LEVEL")
                .or_else(|_| env::var("RUST_LOG"))
                .unwrap_or_else(|_| "info".into()),
            mcp_log_file: env::var("GRAPHMIND_MCP_LOG_FILE").ok().map(PathBuf::from),

            llm: parse_llm_config()?,
            embedding: parse_embedding_config(),
            rocksdb: parse_rocksdb_config(&data_dir),
            queue: parse_queue_config(&data_dir),
        })
    }

    /// Валидация конфигурации (fail-fast в production).
    pub fn validate(&self) -> Result<()> {
        // 1. LLM в production: ключ обязателен (кроме openai-compatible без auth)
        if self.mcp_mode == McpMode::Production {
            match self.llm.provider {
                LlmProvider::Disabled => bail!(
                    "GRAPHMIND_LLM_PROVIDER is required in production mode. \
                     See src/actors/config.rs"
                ),
                LlmProvider::Mock => bail!("Mock LLM is not allowed in production mode"),
                LlmProvider::OpenAI | LlmProvider::Anthropic | LlmProvider::OpenRouter => {
                    if self.llm.api_key.is_none() {
                        bail!("GRAPHMIND_LLM_API_KEY is required for {:?}", self.llm.provider);
                    }
                }
                LlmProvider::OpenAICompatible => {
                    if self.llm.base_url.is_none() {
                        bail!(
                            "GRAPHMIND_LLM_BASE_URL is required for openai-compatible provider. \
                             Examples: http://localhost:1234/v1 (LM Studio), \
                             http://localhost:11434/v1 (Ollama), https://my-vllm.internal/v1"
                        );
                    }
                    if self.llm.model.is_empty() {
                        bail!("GRAPHMIND_LLM_MODEL is required for openai-compatible provider");
                    }
                    if self.llm.api_key.is_none() {
                        tracing::warn!(
                            "openai-compatible without GRAPHMIND_LLM_API_KEY: \
                             ensure the endpoint {} does not require auth",
                            self.llm.base_url.as_deref().unwrap_or("?")
                        );
                    }
                }
            }
        }

        // 1a. openai-compatible: base_url и model проверяем и в dev-режиме
        if self.llm.provider == LlmProvider::OpenAICompatible {
            if self.llm.base_url.is_none() {
                bail!("GRAPHMIND_LLM_BASE_URL is required for openai-compatible provider");
            }
            if self.llm.model.is_empty() {
                bail!("GRAPHMIND_LLM_MODEL is required for openai-compatible provider");
            }
        }

        // 2. Production + RocksDB: use_fsync = true обязателен
        if self.mcp_mode == McpMode::Production && self.backend == BackendKind::RocksDb {
            if !self.rocksdb.use_fsync {
                bail!("GRAPHMIND_ROCKSDB_USE_FSYNC must be true in production mode");
            }
        }

        // 2a. Bug 003 fail-fast: production + RocksDB backend, но бинарь собран
        // БЕЗ фичи `rocksdb` → молчаливый fallback в FileBackend = потеря данных.
        // Если бы это сработало в проде, ноды не пережили бы рестарт MCP-сервера.
        // Здесь это compile-time-известный факт (cfg!), и в release-сборке он
        // статичен — поэтому можем fail-fast на этапе запуска.
        #[cfg(not(feature = "rocksdb"))]
        if self.backend == BackendKind::RocksDb && self.mcp_mode == McpMode::Production {
            bail!(
                "GRAPHMIND_PERSISTENCE=rocksdb in production mode requires a binary built with \
                 `--features mcp-server,rocksdb`. Current binary was built without it and would \
                 silently fall back to FileBackend (deprecated, no durability). \
                 See bug_report/003_rocksdb_silent_fallback.md."
            );
        }

        // 3. Production + FileBackend (deprecated): warn, не паникуем
        if self.mcp_mode == McpMode::Production && self.backend == BackendKind::File {
            tracing::warn!("FileBackend is DEPRECATED, migrate to RocksDB. See rocksdb-rollout.md");
        }

        // 4. data_dir существует (или может быть создан)
        if !self.data_dir.exists() {
            std::fs::create_dir_all(&self.data_dir)
                .with_context(|| format!("Cannot create GRAPHMIND_DATA_DIR={:?}", self.data_dir))?;
        }

        Ok(())
    }
}

fn parse_mcp_mode() -> McpMode {
    match env::var("GRAPHMIND_MCP_MODE").as_deref() {
        Ok("production") | Ok("prod") => McpMode::Production,
        Ok("test") => McpMode::Test,
        Ok("dev") | Ok("") | Err(_) => McpMode::Dev,
        Ok(other) => {
            tracing::warn!("Unknown GRAPHMIND_MCP_MODE: {}. Using dev.", other);
            McpMode::Dev
        }
    }
}

fn parse_backend() -> BackendKind {
    match env::var("GRAPHMIND_PERSISTENCE").as_deref() {
        Ok("rocksdb") | Ok("rocks") => BackendKind::RocksDb,
        Ok("file") | Ok("json") => BackendKind::File,
        Ok("memory") | Ok("inmemory") => BackendKind::InMemory,
        Ok("") | Err(_) => BackendKind::RocksDb, // default для production
        Ok(other) => {
            tracing::warn!("Unknown GRAPHMIND_PERSISTENCE: {}. Using rocksdb.", other);
            BackendKind::RocksDb
        }
    }
}

fn parse_llm_config() -> Result<LlmConfig> {
    let provider = match env::var("GRAPHMIND_LLM_PROVIDER").as_deref() {
        Ok("openai") => LlmProvider::OpenAI,
        Ok("openai-compatible") | Ok("compatible") | Ok("oai-compatible") => LlmProvider::OpenAICompatible,
        Ok("anthropic") => LlmProvider::Anthropic,
        Ok("openrouter") => LlmProvider::OpenRouter,
        Ok("mock") => LlmProvider::Mock,
        Ok("disabled") | Ok("") | Err(_) => LlmProvider::Disabled,
        Ok(other) => bail!("Unknown GRAPHMIND_LLM_PROVIDER: {}", other),
    };

    let default_model = match provider {
        LlmProvider::OpenAI => "gpt-4o",
        LlmProvider::OpenAICompatible => "",
        LlmProvider::Anthropic => "claude-3-5-sonnet-20241022",
        LlmProvider::OpenRouter => "auto",
        LlmProvider::Mock | LlmProvider::Disabled => "mock",
    };

    let default_base_url = match provider {
        LlmProvider::OpenAI => Some("https://api.openai.com/v1".to_string()),
        LlmProvider::OpenAICompatible => None,
        LlmProvider::Anthropic => Some("https://api.anthropic.com/v1".to_string()),
        LlmProvider::OpenRouter => Some("https://openrouter.ai/api/v1".to_string()),
        LlmProvider::Mock | LlmProvider::Disabled => None,
    };

    Ok(LlmConfig {
        provider,
        api_key: env::var("GRAPHMIND_LLM_API_KEY").ok(),
        model: env::var("GRAPHMIND_LLM_MODEL").unwrap_or_else(|_| default_model.into()),
        base_url: env::var("GRAPHMIND_LLM_BASE_URL").ok().or(default_base_url),
        budget_per_hour: env::var("GRAPHMIND_LLM_BUDGET_PER_HOUR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000),
        timeout_secs: env::var("GRAPHMIND_LLM_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30),
    })
}

fn parse_embedding_config() -> EmbeddingConfig {
    let provider = match env::var("GRAPHMIND_EMBEDDING_PROVIDER").as_deref() {
        Ok("local") => EmbeddingProviderKind::Local,
        Ok("openai") => EmbeddingProviderKind::OpenAI,
        Ok("disabled") | Ok("") | Err(_) => EmbeddingProviderKind::Disabled,
        Ok(other) => {
            tracing::warn!("Unknown GRAPHMIND_EMBEDDING_PROVIDER: {}. Using disabled.", other);
            EmbeddingProviderKind::Disabled
        }
    };

    let (default_model, default_dim) = match provider {
        EmbeddingProviderKind::Local => ("sentence-transformers/all-MiniLM-L6-v2", 384),
        EmbeddingProviderKind::OpenAI => ("text-embedding-3-small", 1536),
        EmbeddingProviderKind::Disabled => ("", 0),
    };

    EmbeddingConfig {
        provider,
        model: env::var("GRAPHMIND_EMBEDDING_MODEL")
            .unwrap_or_else(|_| default_model.into()),
        dim: env::var("GRAPHMIND_EMBEDDING_DIM")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default_dim),
        batch_size: env::var("GRAPHMIND_EMBEDDING_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(32),
        cache_enabled: env_bool("GRAPHMIND_EMBEDDING_CACHE_ENABLED"),
        cache_max_entries: env::var("GRAPHMIND_EMBEDDING_CACHE_MAX_ENTRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10000),
        local_model_path: env::var("GRAPHMIND_EMBEDDING_LOCAL_MODEL_PATH").ok().map(PathBuf::from),
        offline: env_bool("GRAPHMIND_EMBEDDING_OFFLINE"),
        // Развязка endpoint эмбеддингов от LLM: если заданы — используются вместо llm.base_url/api_key.
        base_url: env::var("GRAPHMIND_EMBEDDING_BASE_URL").ok().filter(|s| !s.is_empty()),
        api_key: env::var("GRAPHMIND_EMBEDDING_API_KEY").ok().filter(|s| !s.is_empty()),
    }
}

fn parse_rocksdb_config(data_dir: &PathBuf) -> RocksDbConfig {
    RocksDbConfig {
        data_dir: data_dir.clone(),
        compression: env_bool("GRAPHMIND_ROCKSDB_COMPRESSION"),
        parallelism: env::var("GRAPHMIND_ROCKSDB_PARALLELISM")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        memtable_size_mb: env::var("GRAPHMIND_ROCKSDB_MEMTABLE_SIZE_MB")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(128),
        use_fsync: env_bool("GRAPHMIND_ROCKSDB_USE_FSYNC"),
    }
}

fn parse_queue_config(data_dir: &PathBuf) -> QueueConfig {
    QueueConfig {
        interval_secs: env::var("GRAPHMIND_QUEUE_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30),
        file_path: env::var("GRAPHMIND_QUEUE_FILE_PATH")
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.join("pending_actions.json")),
    }
}

fn env_bool(name: &str) -> bool {
    matches!(
        env::var(name).as_deref(),
        Ok("true") | Ok("1") | Ok("yes")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_mcp_mode() {
        // Dev по умолчанию
        std::env::remove_var("GRAPHMIND_MCP_MODE");
        assert_eq!(parse_mcp_mode(), McpMode::Dev);

        // Production
        std::env::set_var("GRAPHMIND_MCP_MODE", "production");
        assert_eq!(parse_mcp_mode(), McpMode::Production);

        // Test
        std::env::set_var("GRAPHMIND_MCP_MODE", "test");
        assert_eq!(parse_mcp_mode(), McpMode::Test);

        std::env::remove_var("GRAPHMIND_MCP_MODE");
    }

    #[test]
    fn test_parse_backend() {
        std::env::remove_var("GRAPHMIND_PERSISTENCE");
        assert_eq!(parse_backend(), BackendKind::RocksDb);

        std::env::set_var("GRAPHMIND_PERSISTENCE", "file");
        assert_eq!(parse_backend(), BackendKind::File);

        std::env::set_var("GRAPHMIND_PERSISTENCE", "memory");
        assert_eq!(parse_backend(), BackendKind::InMemory);

        std::env::remove_var("GRAPHMIND_PERSISTENCE");
    }

    #[test]
    fn test_env_bool() {
        std::env::set_var("TEST_BOOL_TRUE", "true");
        std::env::set_var("TEST_BOOL_FALSE", "false");
        std::env::set_var("TEST_BOOL_EMPTY", "");

        assert!(env_bool("TEST_BOOL_TRUE"));
        assert!(!env_bool("TEST_BOOL_FALSE"));
        assert!(!env_bool("TEST_BOOL_EMPTY"));
        assert!(!env_bool("TEST_BOOL_NONEXISTENT"));

        std::env::remove_var("TEST_BOOL_TRUE");
        std::env::remove_var("TEST_BOOL_FALSE");
        std::env::remove_var("TEST_BOOL_EMPTY");
    }

    #[test]
    fn test_config_validation_production_requires_llm() {
        let config = Config {
            mcp_mode: McpMode::Production,
            llm: LlmConfig {
                provider: LlmProvider::Disabled,
                ..Default::default()
            },
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("GRAPHMIND_LLM_PROVIDER is required"));
    }

    #[test]
    fn test_config_validation_dev_allows_disabled_llm() {
        let config = Config {
            mcp_mode: McpMode::Dev,
            llm: LlmConfig {
                provider: LlmProvider::Disabled,
                ..Default::default()
            },
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_ok());
    }

    /// Bug 003: production mode + RocksDB backend, но бинарь без фичи `rocksdb`
    /// должен fail-fast. Этот тест активен ТОЛЬКО в сборке без фичи `rocksdb`
    /// (имитация того, как был собран исходный баговый бинарь 11.07.2026 02:12).
    #[cfg(not(feature = "rocksdb"))]
    #[test]
    fn test_bug_003_production_rocksdb_without_feature_fails_fast() {
        // LLM должен быть валидным, чтобы validate() дошла до шага 2a (rocksdb-fail-fast).
        // Используем OpenAI-compatible с base_url — пройдёт шаг 1, упадёт на шаге 2a.
        let config = Config {
            mcp_mode: McpMode::Production,
            backend: BackendKind::RocksDb,
            llm: LlmConfig {
                provider: LlmProvider::OpenAICompatible,
                base_url: Some("https://api.kodikrouter.ru".to_string()),
                model: "minimax/minimax-m2.7".to_string(),
                api_key: Some("test-key".to_string()),
                ..Default::default()
            },
            // use_fsync=true обязателен в production + RocksDB (шаг 2).
            rocksdb: RocksDbConfig {
                use_fsync: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("--features") && err.contains("silent"),
            "unexpected error message: {err}"
        );
    }

    /// Bug 003: dev-режим + RocksDB backend + бинарь без фичи — допустимо
    /// (silent fallback допустим только в dev, но должен логироваться в stderr).
    /// В dev `validate()` НЕ должен падать, чтобы не сломать dev-окружение.
    #[cfg(not(feature = "rocksdb"))]
    #[test]
    fn test_bug_003_dev_rocksdb_without_feature_allows_fallback() {
        let config = Config {
            mcp_mode: McpMode::Dev,
            backend: BackendKind::RocksDb,
            ..Default::default()
        };
        let result = config.validate();
        // Dev-режим допускает fallback (с warn), но не падает.
        assert!(result.is_ok());
    }
}
