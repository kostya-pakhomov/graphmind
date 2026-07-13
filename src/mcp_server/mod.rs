//! MCP Server — встроенный MCP сервер для GraphMind v2.
//!
//! Работает через stdio transport, диспетчеризирует вызовы к акторам.
//! Включается через feature flag `mcp-server` и env `GRAPHMIND_MCP_MODE=1`.

mod protocol;
mod handler;
mod server;
mod http_server;
pub mod net_guard;

pub use server::run_mcp_server;
pub use server::run_mcp_server_full;
pub use server::run_mcp_server_full_with_queue;
pub use server::run_mcp_server_full_with_queue_and_workspace;
pub use server::run_mcp_server_full_with_all;
pub use http_server::run_mcp_http_server;
pub use http_server::run_mcp_http_server_with_queue;
pub use http_server::run_mcp_http_server_with_queue_and_workspace;
pub use http_server::run_mcp_http_server_full_with_all;
pub use handler::McpHandler;
