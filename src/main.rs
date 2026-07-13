mod actors;
mod graph;
mod grpc;
mod persistence;
mod queue;

// MCP Server module (only available with mcp-server feature)
#[cfg(feature = "mcp-server")]
mod mcp_server;

// Web UI server (локальный мониторинг + работа с памятью через браузер)
mod web;

// Include generated gRPC code from proto
pub mod graphmind {
    tonic::include_proto!("graphmind");
}

use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, error, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use actors::{Actor, Config, S0Actor, L2Actor, L1Actor, L0Actor, GKLactor, SearchActor, ChainActor, CausalEngine, InferenceActor, WorkspaceManager, PlanActor, CuriosityEngine, TrustFirewall, LlmClient, EmbeddingProvider, BackendKind, McpMode};
use persistence::{InMemoryBackend, FileBackend, StorageBackend};
#[cfg(feature = "rocksdb")]
use persistence::RocksDBBackend;
use queue::{PendingStore, QueueProcessor};
use grpc::start_grpc_server;
use graph::Graph;
use std::net::SocketAddr;
use tokio::sync::RwLock;

fn build_backend(config: &Config) -> Arc<dyn StorageBackend> {
    match config.backend {
        BackendKind::RocksDb => {
            #[cfg(feature = "rocksdb")]
            {
                Arc::new(RocksDBBackend::open(&config.data_dir)
                    .expect("RocksDBBackend failed to open"))
            }
            #[cfg(not(feature = "rocksdb"))]
            {
                // Bug 003 (rocksdb-silent-fallback): stdio MCP-транспорт не видит
                // tracing-логи до рукопожатия, поэтому warn! тонет. Дублируем в stderr
                // (eprintln!) + повышаем уровень до error!, чтобы пользователь увидел
                // "RocksDB requested but disabled" в логах Kodik немедленно, а не после
                // crash'а L2-стора при рестарте.
                let msg = "RocksDB requested via GRAPHMIND_PERSISTENCE=rocksdb, but feature `rocksdb` \
                           is disabled. Falling back to FileBackend. Rebuild with \
                           `--features mcp-server,rocksdb` for production. \
                           See bug_report/003_rocksdb_silent_fallback.md.";
                eprintln!("graphmind-v2: {}", msg);
                error!("{}", msg);
                warn!("{}", msg);
                Arc::new(FileBackend::open(&config.data_dir)
                    .expect("FileBackend failed to open"))
            }
        }
        BackendKind::File => {
            warn!("FileBackend is DEPRECATED, migrate to RocksDB. See rocksdb-rollout.md");
            Arc::new(FileBackend::open(&config.data_dir)
                .expect("FileBackend failed to open"))
        }
        BackendKind::InMemory => Arc::new(InMemoryBackend::new()),
    }
}

/// Собрать QueueProcessor с правильным путём к файлу очереди и интервалом.
fn build_queue(
    config: &Config,
    s0: Arc<S0Actor>,
    l2: Arc<L2Actor>,
    trust: Option<Arc<TrustFirewall>>,
) -> QueueProcessor {
    let store = PendingStore::new(config.queue.file_path.clone());
    let mut q = QueueProcessor::new(store, config.queue.interval_secs)
        .with_s0(s0)
        .with_l2(l2);
    if let Some(t) = trust {
        q = q.with_trust(t);
    }
    q
}

/// Запустить QueueProcessor в фоне (не блокирует main).
/// Принимает `Arc<QueueProcessor>`, чтобы один и тот же processor можно было
/// шарить между фоновым `run()` и `McpHandler` для `enqueue`/`drain_to_l2`.
fn spawn_queue_processor(processor: Arc<QueueProcessor>) {
    tokio::spawn(async move {
        if let Err(e) = processor.run().await {
            error!("QueueProcessor error: {}", e);
        }
    });
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 0. Load .env (если есть) из директории бинаря.
    //    MCP-сервер запускается Kodik'ом без наследуемого shell-окружения, поэтому
    //    переменные LLM_BASE_URL / LLM_API_KEY / LLM_MODEL без dotenv не доходят
    //    до OrchestratorActor — plan_decompose падает на LLM и уходит в fallback.
    //    Если .env отсутствует — это нормально (dev через shell или CI), просто
    //    пишем warn в stderr и продолжаем.
    if let Some(env_path) = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join(".env")))
    {
        match dotenvy::from_path(&env_path) {
            Ok(_) => eprintln!("graphmind-v2: loaded .env from {:?}", env_path),
            Err(e) if e.not_found() => {
                // Нет .env рядом с бинарником — не критично: переменные могут прийти из
                // окружения процесса (docker env_file / shell / CI). Но warn обещан
                // комментарием выше и раньше НЕ писался — из-за этого «LLM молча выключен»
                // (env в корне/cwd игнорируется, .env ищется только рядом с бинарником).
                eprintln!(
                    "graphmind-v2: .env не найден рядом с бинарником ({:?}); \
                     беру GRAPHMIND_* из окружения процесса (docker env_file / shell / CI)",
                    env_path
                );
            }
            Err(e) => eprintln!("graphmind-v2: failed to load .env from {:?}: {}", env_path, e),
        }
    }

    // 1. Load + validate Config FIRST (всё через единую точку env).
    //    validate() может fail-fast (production без LLM API key, etc.)
    let config = Config::from_env()
        .map_err(|e| {
            eprintln!(
                "Failed to load Config: {}\n\
                 See src/actors/config.rs",
                e
            );
            e
        })?;
    // Доп. диагностика в stderr ДО validate (на случай, если validate() упадёт —
    // видно, в чём дело, даже если стектрейс перехвачен MCP-клиентом).
    eprintln!(
        "graphmind-v2: config loaded: mode={:?}, backend={:?}, data_dir={}",
        config.mcp_mode, config.backend, config.data_dir.display()
    );
    config.validate().map_err(|e| {
        eprintln!("Config validation failed: {}", e);
        e
    })?;

    // Emit a single line of init diagnostics to stderr (stdio transport can't read it).
    eprintln!(
        "graphmind-v2: mcp_mode={:?}, backend={:?}, data_dir={}, http={}, http_addr={}",
        config.mcp_mode,
        config.backend,
        config.data_dir.display(),
        config.mcp_http,
        config.mcp_http_addr
    );

    // 2. MCP transport — stdio по умолчанию, HTTP при GRAPHMIND_MCP_HTTP.
    //    Оба транспорта строят ОДИН И ТОТ ЖЕ полный набор акторов (паритет):
    //    раньше HTTP-путь поднимал лишь S0/L2/Queue/Workspace и был достижим только в
    //    MCP_MODE=test — теперь HTTP выбирается флагом mcp_http и равноправен stdio, что
    //    снимает нужду в gRPC-мосте как способе дотянуться до полного набора tools.
    //    Test-режим MCP пропускает (нативный gRPC ниже). Проверяем ДО init логирования
    //    (stdio-транспорт не должен писать в stderr до рукопожатия).
    #[cfg(feature = "mcp-server")]
    if !matches!(config.mcp_mode, actors::McpMode::Test) {
        // Единый backend — общий кэш для всех L2Actor'ов (иначе устаревшие чтения
        // внутри процесса: писатель видит запись, читатель — нет до рестарта).
        let backend = build_backend(&config);

        // Единый S0 (Arc<S0Actor> потокобезопасен внутри) — общий для очереди и
        // handler'а, иначе get_s0_context/flush читают пустой буфер (split-brain).
        let s0 = Arc::new(S0Actor::new());

        let l2 = Arc::new(RwLock::new(L2Actor::new(backend.clone())));
        let search = Arc::new(RwLock::new(
            SearchActor::with_memory_index(backend.clone())
                .with_embedding_provider(EmbeddingProvider::from_config(&config.embedding, &config.llm)),
        ));

        // Загрузить существующие узлы из backend в SearchActor.
        {
            let search_guard = search.read().await;
            match search_guard.load_all_nodes().await {
                Ok(count) => eprintln!("graphmind-v2: SearchActor loaded {} nodes from backend", count),
                Err(e) => eprintln!("graphmind-v2: SearchActor load failed: {}", e),
            }
        }

        let chain = Arc::new(ChainActor::new(Arc::new(L2Actor::new(backend.clone()))));

        // Шина событий координатора памяти: эмиттеры (handler, очередь) → координатор.
        let (evt_tx, evt_rx) = tokio::sync::mpsc::unbounded_channel::<actors::MemoryEvent>();

        // TrustFirewall создаём ДО очереди: суб-агентские propose_new_memory
        // проходят через гейт в QueueProcessor.
        let trust = {
            let fw = TrustFirewall::new(Arc::new(L2Actor::new(backend.clone())));
            fw.load_state().await; // калибровка/репутации переживают рестарт
            Arc::new(fw)
        };

        // QueueProcessor (durable record_action / flush) — делит s0 с handler'ом.
        // event_tx: очередь эмитит NodeWritten / TrustFirewallBlock.
        let l2_for_queue = Arc::new(L2Actor::new(backend.clone()));
        let processor = Arc::new(
            build_queue(&config, s0.clone(), l2_for_queue, Some(trust.clone()))
                .with_event_tx(evt_tx.clone()),
        );
        spawn_queue_processor(processor.clone());

        // WorkspaceManager — real detect/create/bootstrap (replaces "default" stub).
        let workspace = Arc::new(WorkspaceManager::new(backend.clone()));
        if let Err(e) = workspace.load_all().await {
            eprintln!("graphmind-v2: WorkspaceManager load_all failed: {}", e);
        }

        // L1 / L0 (consolidate_workspace) + PlanActor (12 plan_* tools).
        // LLM инъектируем всегда: is_enabled() внутри решает — при Disabled это no-op
        // и L1/L0 работают как раньше (эвристика/связность).
        let l1_actor = Arc::new(RwLock::new(
            L1Actor::new(backend.clone()).with_llm(LlmClient::from_config(&config.llm)),
        ));
        let l0_actor = Arc::new(RwLock::new(
            L0Actor::new(backend.clone()).with_llm(LlmClient::from_config(&config.llm)),
        ));
        let plan_actor = Arc::new(RwLock::new(PlanActor::new(backend.clone())));
        // Причинный слой над L2 (dream/predict/contradictions/propose_causal_link) — своя
        // Arc<L2Actor> над тем же backend + LLM (is_enabled() внутри решает LLM vs эвристика).
        let inference_actor = Arc::new(
            InferenceActor::new(Arc::new(L2Actor::new(backend.clone())))
                .with_llm(LlmClient::from_config(&config.llm)),
        );

        // Слой познания на MCP-контуре: curiosity/trust над ТЕМ ЖЕ backend (раньше жили
        // только на gRPC-пути над пустым in-memory Graph → на stdio/HTTP MCP их не было).
        let curiosity = Arc::new(CuriosityEngine::with_default_threshold(Arc::new(L2Actor::new(backend.clone()))));

        // Координатор памяти: ConsolidateRunner над теми же слоями (l2/queue/l1/l0) +
        // фоновый цикл на шине событий (CycleTrigger). Запускается до старта транспорта.
        let consolidate_runner = Arc::new(actors::ConsolidateRunner::new(
            l2.clone(),
            Some(processor.clone()),
            Some(l1_actor.clone()),
            Some(l0_actor.clone()),
        ));
        let orchestrator = Arc::new(actors::MemoryOrchestrator::new(
            evt_tx.clone(),
            evt_rx,
            consolidate_runner,
            actors::CoordinatorCfg::default(),
        ));
        orchestrator.clone().spawn_loop();

        // Web UI: локальный HTTP-сервер для мониторинга и работы с памятью.
        // Делит Arc-ссылки на акторы с MCP-handler. Порт из .env (GRAPHMIND_UI_PORT, default 7878).
        let ui_port = std::env::var("GRAPHMIND_UI_PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(7878);
        let web_state = Arc::new(web::WebState {
            s0: s0.clone(),
            l2: l2.clone(),
            search: Some(search.clone()),
            chain: Some(chain.clone()),
            queue: Some(processor.clone()),
            workspace: Some(workspace.clone()),
            inference: Some(inference_actor.clone()),
            plan: Some(plan_actor.clone()),
            config: Some(Arc::new(RwLock::new(config.clone()))),
            llm: Some(LlmClient::from_config(&config.llm)),
            start_time: std::time::Instant::now(),
        });
        tokio::spawn(async move {
            eprintln!("graphmind-v2: spawning Web UI server on port {}", ui_port);
            web::start_web_server(web_state, ui_port).await;
            eprintln!("graphmind-v2: Web UI server task ended");
        });

        let run_result = if config.mcp_http {
            eprintln!(
                "graphmind-v2: starting MCP HTTP server on {} (full parity: search/chain/l1/l0/plan)",
                config.mcp_http_addr
            );
            mcp_server::run_mcp_http_server_full_with_all(
                s0, l2, search, chain,
                Some(processor),
                Some(workspace),
                Some(l1_actor),
                Some(l0_actor),
                Some(plan_actor),
                Some(inference_actor),
                Some(curiosity),
                Some(trust),
                Some(evt_tx.clone()),
                Some(orchestrator.clone()),
                &config.mcp_http_addr,
            )
            .await
        } else {
            eprintln!("graphmind-v2: starting MCP stdio server (queue + workspace + L1/L0/plan attached)");
            mcp_server::run_mcp_server_full_with_all(
                s0, l2, search, chain,
                Some(processor),
                Some(workspace),
                Some(l1_actor),
                Some(l0_actor),
                Some(plan_actor),
                Some(inference_actor),
                Some(curiosity),
                Some(trust),
                Some(evt_tx.clone()),
                Some(orchestrator.clone()),
            )
            .await
        };
        match run_result {
            Ok(()) => {
                // stdio-цикл завершился (Kodik закрыл stdin или ушёл в idle).
                // Процесс НЕ завершаем — web-сервер должен жить дальше.
                eprintln!("graphmind-v2: MCP stdio ended (Ok), keeping Web UI alive on port {}", ui_port);
                // Ждём Ctrl+C — web-сервер (tokio::spawn) держит процесс живым.
                // Не используем std::future::pending — он может не держать runtime.
                tokio::signal::ctrl_c().await.ok();
                eprintln!("graphmind-v2: Ctrl+C received, shutting down");
            }
            Err(e) => {
                eprintln!("MCP server error: {}", e);
                // Не выходим — web-сервер может быть ещё жив
                tokio::signal::ctrl_c().await.ok();
            }
        }
    }

    // 4. Инициализация tracing (только для non-stdio режимов).
    init_tracing(&config);

    info!("GraphMind v2 — Graph-Native Engine");
    info!("Workspace: {}", config.workspace_root.display());

    // 5. Инициализация storage actors (всё через Config).
    let s0 = Arc::new(S0Actor::new());
    info!("{} initialized (capacity {})", s0.name(), s0.capacity());

    let backend = build_backend(&config);
    let l2 = Arc::new(L2Actor::new(backend.clone()));
    info!("{} initialized (backend: {})", l2.name(), l2.backend().name());

    let l1 = Arc::new(L1Actor::new(backend.clone()).with_llm(LlmClient::from_config(&config.llm)));
    info!("{} initialized", l1.name());

    let l0 = Arc::new(L0Actor::new(backend.clone()).with_llm(LlmClient::from_config(&config.llm)));
    info!("{} initialized", l0.name());

    let gkl = Arc::new(GKLactor::new(backend.clone()));
    info!("{} initialized", gkl.name());

    let search = Arc::new(
        SearchActor::with_memory_index(backend.clone())
            .with_embedding_provider(EmbeddingProvider::from_config(&config.embedding, &config.llm)),
    );
    info!("{} initialized", search.name());

    let chain = Arc::new(ChainActor::new(l2.clone()));
    info!("ChainActor initialized");

    let causal_engine = Arc::new(CausalEngine);
    info!("CausalEngine initialized");

    let inference = Arc::new(InferenceActor::new(l2.clone()).with_llm(LlmClient::from_config(&config.llm)));
    info!("InferenceActor initialized");

    let workspace_manager = Arc::new(WorkspaceManager::new(backend.clone()));
    info!("WorkspaceManager initialized");

    match workspace_manager.load_all().await {
        Ok(count) => info!("Loaded {} existing workspaces", count),
        Err(e) => warn!("Failed to load existing workspaces: {}", e),
    }

    // Слой познания над общим backend (раньше — отдельный пустой in-memory Graph).
    let curiosity_engine = Arc::new(CuriosityEngine::with_default_threshold(l2.clone()));
    info!("CuriosityEngine initialized");

    let trust_firewall = {
        let fw = TrustFirewall::new(l2.clone());
        fw.load_state().await; // калибровка/репутации переживают рестарт
        Arc::new(fw)
    };
    info!("TrustFirewall initialized");

    // 6. QueueProcessor (native gRPC-режим).
    let processor = Arc::new(build_queue(&config, s0.clone(), l2.clone(), Some(trust_firewall.clone())));
    spawn_queue_processor(processor);

    // 7. gRPC server (если задан GRAPHMIND_GRPC_ADDR).
    let grpc_addr = config.grpc_addr;
    if let Some(addr) = grpc_addr {
        let grpc_s0 = s0.clone();
        let grpc_l2 = l2.clone();
        let grpc_l1 = l1.clone();
        let grpc_l0 = l0.clone();
        let grpc_gkl = gkl.clone();
        let grpc_search = search.clone();
        let grpc_chain = chain.clone();
        let grpc_causal = causal_engine.clone();
        let grpc_inference = inference.clone();
        let grpc_workspace = workspace_manager.clone();
        let grpc_curiosity = curiosity_engine.clone();
        let grpc_trust = trust_firewall.clone();
        // Legacy gRPC-хендлер всё ещё принимает отдельный in-memory Graph (не на слое
        // познания — curiosity/trust теперь над L2). Держим пустой для совместимости сигнатуры.
        let grpc_graph = Arc::new(RwLock::new(Graph::new()));

        tokio::spawn(async move {
            if let Err(e) = start_grpc_server(
                Some(addr),
                grpc_s0, grpc_l2, grpc_l1, grpc_l0, grpc_gkl,
                grpc_search, grpc_chain, grpc_causal, grpc_inference,
                grpc_workspace, grpc_curiosity, grpc_trust, grpc_graph,
            )
            .await
            {
                error!("gRPC server error: {}", e);
            }
        });

        info!("gRPC MCP Bridge started on {}", addr);
    } else {
        info!("gRPC MCP Bridge disabled (set GRAPHMIND_GRPC_ADDR to enable)");
    }

    // 8. Keep main alive.
    tokio::signal::ctrl_c().await?;
    info!(
        "Shutting down... (S0 final size: {}, L2 size: {})",
        s0.size().await,
        l2.size().await
    );
    Ok(())
}

fn init_tracing(config: &Config) {
    // RUST_LOG имеет приоритет; иначе — GRAPHMIND_MCP_LOG_LEVEL; иначе — info.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new(&config.mcp_log_level))
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let registry = tracing_subscriber::registry().with(filter);

    let stderr_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    match open_log_file(config.mcp_log_file.as_deref()) {
        Some(file) => {
            // Двойной writer: stderr (для dev/`journalctl`) + файл (для долгосрочного хранения).
            // `tracing_subscriber` принимает несколько слоёв с разными `MakeWriter` через `Layer`.
            let file_layer = tracing_subscriber::fmt::layer().with_writer(move || file.try_clone().expect("log file clone"));
            registry.with(stderr_layer).with(file_layer).init();
            eprintln!(
                "graphmind-v2: tracing initialized (RUST_LOG/mcp_log_level) + file writer at {:?}",
                config.mcp_log_file
            );
        }
        None => {
            registry.with(stderr_layer).init();
        }
    }
}

/// Открыть лог-файл (создать родительский каталог при отсутствии).
/// Возвращает `None`, если путь не задан или открыть не удалось (с warn).
fn open_log_file(path: Option<&std::path::Path>) -> Option<std::fs::File> {
    let p = path?;
    if let Some(parent) = p.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                eprintln!(
                    "graphmind-v2: failed to create log file parent dir {:?}: {}",
                    parent, e
                );
                return None;
            }
        }
    }
    match std::fs::OpenOptions::new().create(true).append(true).open(p) {
        Ok(f) => Some(f),
        Err(e) => {
            eprintln!(
                "graphmind-v2: failed to open GRAPHMIND_MCP_LOG_FILE {:?}: {}",
                p, e
            );
            None
        }
    }
}
