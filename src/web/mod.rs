// Web UI сервер: WebSocket + статика.
// WS-канал для команд и событий, HTML отдаётся из вшитых файлов.

mod settings;
mod ws;

use std::net::SocketAddr;
use std::sync::Arc;
use axum::{routing::get, Router, response::IntoResponse};
use axum::extract::{ConnectInfo, Request, State};
use axum::middleware::{self, Next};
use axum::http::StatusCode;
use axum::response::Response;
use rust_embed::RustEmbed;
use tokio::sync::RwLock;

use crate::actors::{S0Actor, L2Actor, SearchActor, ChainActor, WorkspaceManager, InferenceActor, PlanActor, Config, LlmClient};
use crate::queue::QueueProcessor;

/// Статические файлы UI (HTML+JS+CSS), вшитые в бинарь через rust-embed.
#[derive(RustEmbed)]
#[folder = "src/web/static/"]
struct WebAsset;

/// Общее состояние, доступное всем WS-обработчикам.
/// Делит Arc-ссылки с MCP-handler — один backend, один кэш.
pub struct WebState {
    pub s0: Arc<S0Actor>,
    pub l2: Arc<RwLock<L2Actor>>,
    pub search: Option<Arc<RwLock<SearchActor>>>,
    pub chain: Option<Arc<ChainActor>>,
    pub queue: Option<Arc<QueueProcessor>>,
    pub workspace: Option<Arc<WorkspaceManager>>,
    pub inference: Option<Arc<InferenceActor>>,
    pub plan: Option<Arc<RwLock<PlanActor>>>,
    pub config: Option<Arc<RwLock<Config>>>,
    pub llm: Option<LlmClient>,
    pub start_time: std::time::Instant,
}

/// Запустить HTTP+WS сервер на 127.0.0.1:port (не блокирует).
/// Если порт занят (Kodik перезапустил MCP, старый процесс ещё жив),
/// пробует следующие порты (port+1 .. port+10).
pub async fn start_web_server(state: Arc<WebState>, port: u16) {
    // host: 127.0.0.1 по умолчанию (безопасно), 0.0.0.0 в контейнере (GRAPHMIND_UI_HOST).
    let host = std::env::var("GRAPHMIND_UI_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    // Тир-гейт: по умолчанию UI/WS доступны только с локальной машины/приватной сети;
    // GRAPHMIND_ALLOW_EXTERNAL=true открывает наружу (командный/облачный тир).
    let allow_external = crate::mcp_server::net_guard::allow_external_from_env();
    let mut chosen_port = port;
    for offset in 0..10u16 {
        let try_port = port.saturating_add(offset);
        let addr: SocketAddr = format!("{}:{}", host, try_port).parse().unwrap_or_else(|_| ([127, 0, 0, 1], try_port).into());
        match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => {
                eprintln!("graphmind-v2: Web UI (WS) on http://{}", addr);
                chosen_port = try_port;
                eprintln!("graphmind-v2: Web UI starting axum::serve on port {}", try_port);
                let svc = app_with_routes(state.clone(), allow_external)
                    .into_make_service_with_connect_info::<SocketAddr>();
                match axum::serve(l, svc).await {
                    Ok(_) => eprintln!("graphmind-v2: Web UI server exited normally on port {}", try_port),
                    Err(e) => eprintln!("graphmind-v2: Web UI server ERROR on port {}: {}", try_port, e),
                }
                return;
            }
            Err(e) if offset < 9 => {
                eprintln!("graphmind-v2: port {} busy ({}), trying {}", try_port, e, try_port + 1);
                continue;
            }
            Err(e) => {
                eprintln!("graphmind-v2: Web UI bind failed on {} (tried {}-{}): {}", addr, port, try_port, e);
                return;
            }
        }
    }
    let _ = chosen_port;
}

/// Router с маршрутами — вынесен, чтобы не клонировать дважды.
fn app_with_routes(state: Arc<WebState>, allow_external: bool) -> Router {
    Router::new()
        .route("/", get(index_handler))
        .route("/ws", get(ws::ws_handler))
        .layer(middleware::from_fn_with_state(allow_external, guard_external))
        .with_state(state)
}

/// Middleware тир-гейта: отклоняет внешние (публичные) источники, если не выставлен
/// GRAPHMIND_ALLOW_EXTERNAL=true. См. `crate::mcp_server::net_guard`.
async fn guard_external(
    State(allow_external): State<bool>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Response {
    if !crate::mcp_server::net_guard::is_source_allowed(peer.ip(), allow_external) {
        eprintln!(
            "graphmind-v2: Web UI отклонил внешнее подключение {} (GRAPHMIND_ALLOW_EXTERNAL=false, локальный/Free-контур)",
            peer.ip()
        );
        return (StatusCode::FORBIDDEN, "external access disabled (Free/local deployment)").into_response();
    }
    next.run(req).await
}

/// Отдать index.html из вшитых статических файлов.
async fn index_handler() -> impl IntoResponse {
    let asset = WebAsset::get("index.html");
    match asset {
        Some(a) => (
            [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
            a.data.into_owned(),
        )
            .into_response(),
        None => (axum::http::StatusCode::NOT_FOUND, "index.html not found").into_response(),
    }
}
