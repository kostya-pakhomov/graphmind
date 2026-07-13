//! GraphMind v2 -- Graph-Native Memory Engine

pub mod actors;
pub mod graph;
pub mod persistence;
pub mod queue;
// mcp_server экспортирован публично, чтобы integration-тесты в tests/
// могли собирать McpHandler напрямую (для bug_report/001 регрессий).
pub mod mcp_server;
