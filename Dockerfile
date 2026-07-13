# Build stage
# rust:1 (latest stable) — пин 1.85 больше не собирает: часть зависимостей (Cargo.lock)
# требует более новый rustc («rustc 1.85.1 is not supported by ...»).
FROM rust:1-slim-bookworm AS builder

WORKDIR /app

# protoc — build.rs (tonic-build); g++/cmake — нативные сборки (usearch/cxx);
# clang/libclang-dev — bindgen в librocksdb-sys (фича rocksdb);
# pkg-config/libssl-dev — openssl-sys (reqwest/native-tls) в slim-образе.
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    g++ \
    cmake \
    clang \
    libclang-dev \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . .

# Build the binary with MCP server support + RocksDB persistence backend
# (rocksdb реализован в src/persistence/rocksdb.rs, но был за фича-флагом, которого
# не было в сборке → контейнер падал в FileBackend; фича включена для прод-персиста).
RUN cargo build --release --features mcp-server,rocksdb

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# ca-certificates + libssl3 — для HTTPS-вызовов бинарника к LLM (RouterAI) через
# native-tls; без CA-сертификатов проверка сертификата routerai.ru упадёт.
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /app/target/release/graphmind-v2 .

# Expose ports
EXPOSE 50051
EXPOSE 50052
EXPOSE 7878

# Default: run as gRPC server
# Set GRAPHMIND_MCP_MODE=1 to run as MCP server (stdio transport)
# Set GRAPHMIND_MCP_HTTP=1 to run as MCP HTTP server (streamable HTTP)
ENV GRAPHMIND_GRPC_ADDR=0.0.0.0:50051

# Run the server
CMD ["./graphmind-v2"]
