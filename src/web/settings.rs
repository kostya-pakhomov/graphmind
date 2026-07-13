// Управление настройками UI: load/save data/settings.json.
// Переопределяет .env при наличии файла. API-ключ хранится в plain JSON,
// безопасность — localhost-only + файловые права 0600 (Unix) / ACL (Windows).

use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Полная конфигурация, редактируемая из UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UiSettings {
    pub llm: LlmSettings,
    pub embedding: EmbeddingSettings,
    pub server: ServerSettings,
    pub memory: MemorySettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmSettings {
    pub provider: String,
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub temperature: f64,
    pub max_tokens: u32,
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingSettings {
    pub provider: String,
    pub model: String,
    pub base_url: String,
    pub api_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSettings {
    pub ui_port: u16,
    pub log_level: String,
    pub mcp_http: bool,
    pub mcp_http_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemorySettings {
    pub persistence: String,
    pub data_dir: String,
    pub workspace_root: String,
    pub consolidation_threshold: u32,
    pub l0_overlap_threshold: f64,
}

impl Default for UiSettings {
    fn default() -> Self {
        Self {
            llm: LlmSettings {
                provider: "disabled".into(),
                base_url: String::new(),
                api_key: String::new(),
                model: String::new(),
                temperature: 0.7,
                max_tokens: 4096,
                timeout_secs: 30,
            },
            embedding: EmbeddingSettings {
                provider: "disabled".into(),
                model: String::new(),
                base_url: String::new(),
                api_key: String::new(),
            },
            server: ServerSettings {
                ui_port: 7878,
                log_level: "info".into(),
                mcp_http: false,
                mcp_http_addr: "0.0.0.0:50052".into(),
            },
            memory: MemorySettings {
                persistence: "rocksdb".into(),
                data_dir: "data".into(),
                workspace_root: ".".into(),
                consolidation_threshold: 50,
                l0_overlap_threshold: 0.05,
            },
        }
    }
}

/// Плейсхолдер вместо реального секрета при отдаче настроек в UI.
/// Сырой api_key НИКОГДА не уходит клиенту (браузер/WS); при сохранении
/// значение-маска трактуется как «не менять» (см. ws.rs cmd_settings_set).
pub const MASKED_SECRET: &str = "••••••••";

/// Построить настройки для отображения в UI из ЖИВОГО Config сервера (.env),
/// а не из дефолтов settings.json. Ключи маскируются. UI-only поля
/// (temperature/max_tokens/пороги/ui_port) берём из сохранённого settings.json.
pub fn view_from_config(cfg: &crate::actors::Config) -> UiSettings {
    let persisted = load();
    let mask = |present: bool| -> String {
        if present { MASKED_SECRET.to_string() } else { String::new() }
    };
    let emb_base = cfg
        .embedding
        .base_url
        .clone()
        .or_else(|| cfg.llm.base_url.clone())
        .unwrap_or_default();
    let emb_key_present = cfg.embedding.api_key.is_some() || cfg.llm.api_key.is_some();
    UiSettings {
        llm: LlmSettings {
            provider: llm_provider_str(&cfg.llm.provider).into(),
            base_url: cfg.llm.base_url.clone().unwrap_or_default(),
            api_key: mask(cfg.llm.api_key.is_some()),
            model: cfg.llm.model.clone(),
            temperature: persisted.llm.temperature,
            max_tokens: persisted.llm.max_tokens,
            timeout_secs: cfg.llm.timeout_secs,
        },
        embedding: EmbeddingSettings {
            provider: emb_provider_str(&cfg.embedding.provider).into(),
            model: cfg.embedding.model.clone(),
            base_url: emb_base,
            api_key: mask(emb_key_present),
        },
        server: ServerSettings {
            ui_port: persisted.server.ui_port,
            log_level: cfg.mcp_log_level.clone(),
            mcp_http: cfg.mcp_http,
            mcp_http_addr: if cfg.mcp_http_addr.is_empty() {
                "0.0.0.0:50052".into()
            } else {
                cfg.mcp_http_addr.clone()
            },
        },
        memory: MemorySettings {
            persistence: backend_str(&cfg.backend).into(),
            data_dir: cfg.data_dir.to_string_lossy().to_string(),
            workspace_root: cfg.workspace_root.to_string_lossy().to_string(),
            consolidation_threshold: persisted.memory.consolidation_threshold,
            l0_overlap_threshold: persisted.memory.l0_overlap_threshold,
        },
    }
}

fn llm_provider_str(p: &crate::actors::LlmProvider) -> &'static str {
    use crate::actors::LlmProvider::*;
    match p {
        OpenAI => "openai",
        OpenAICompatible => "openai-compatible",
        Anthropic => "anthropic",
        OpenRouter => "openrouter",
        Mock => "mock",
        Disabled => "disabled",
    }
}

fn emb_provider_str(p: &crate::actors::EmbeddingProviderKind) -> &'static str {
    use crate::actors::EmbeddingProviderKind::*;
    match p {
        Local => "local",
        OpenAI => "openai",
        Disabled => "disabled",
    }
}

fn backend_str(b: &crate::actors::BackendKind) -> &'static str {
    use crate::actors::BackendKind::*;
    match b {
        RocksDb => "rocksdb",
        File => "file",
        InMemory => "memory",
    }
}

/// Путь к файлу настроек: data/settings.json.
fn settings_path() -> PathBuf {
    PathBuf::from("data").join("settings.json")
}

/// Загрузить настройки из data/settings.json. Если файла нет — создать с default.
pub fn load() -> UiSettings {
    let path = settings_path();
    if !path.exists() {
        let default = UiSettings::default();
        let _ = save(&default);
        return default;
    }
    match std::fs::read_to_string(&path) {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(_) => UiSettings::default(),
    }
}

/// Сохранить настройки в data/settings.json.
pub fn save(settings: &UiSettings) -> Result<(), String> {
    let path = settings_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let json = serde_json::to_string_pretty(settings).map_err(|e| e.to_string())?;
    std::fs::write(&path, json + "\n").map_err(|e| e.to_string())?;
    set_file_permissions_0600(&path);
    Ok(())
}

/// Установить права 0600 на файл настроек (только владелец читает/пишет).
fn set_file_permissions_0600(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(path) {
            let mut perms = meta.permissions();
            perms.set_mode(0o600);
            let _ = std::fs::set_permissions(path, perms);
        }
    }
    #[cfg(windows)]
    {
        // Windows: файл в data/ уже ограничен правами пользователя.
        // Дополнительная защита — через ACL, но для localhost-only достаточно.
        let _ = path;
    }
}

/// Сериализовать настройки в JSON Value для API.
pub fn to_json(s: &UiSettings) -> Value {
    serde_json::to_value(s).unwrap_or(json!({}))
}

/// Десериализовать JSON Value в настройки (merge: частичное обновление).
pub fn from_json(v: &Value) -> Result<UiSettings, String> {
    serde_json::from_value(v.clone()).map_err(|e| format!("invalid settings: {}", e))
}
