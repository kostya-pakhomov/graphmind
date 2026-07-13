//! gRPC MCP Bridge — интеграция Graph Engine с MCP.
//!
//! Реализует MemoryService из proto/memory.proto,
//! диспетчеризирует вызовы к соответствующим акторам.

mod server;
mod handlers;

pub use server::{start_grpc_server, GrpcServer};
