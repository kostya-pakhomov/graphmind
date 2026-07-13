# GraphMind v2

Постоянная общая память для ИИ-агентов, подключается по **MCP** (Model Context Protocol). Один Rust-бинарь: граф знаний с причинными связями, семантический поиск, персист между сессиями. Работает с любым MCP-совместимым агентом — **Kodik, Cursor, Claude Code, Codex, OpenCode**.

**Плагин для Kodik** (MCP + правило + навык + суб-агент + хуки) — в [`kodik-plugin/`](kodik-plugin/).

## Что даёт

- **Память между сессиями** — агент стартует с накопленным контекстом проекта.
- **Общий граф на несколько агентов и людей** — один накопил знание, остальные им пользуются.
- **50 MCP-инструментов**: слои памяти S0→L2→L1→L0 с консолидацией, воркспейсы, семантический поиск (эмбеддинги + косинусная близость), причинный слой на LLM (`find_contradictions`, `predict_risks`, `propose_causal_link`), фильтр доверия (`verify_input`), любопытство, ось планирования (`plan_*`).
- **Персист** между перезапусками (RocksDB) + семантический recall из накопленного.

Полный каталог инструментов — [`docs/TOOLS.md`](docs/TOOLS.md). Модель данных памяти — [`docs/MEMORY.md`](docs/MEMORY.md).

## Требования сборки

- **Rust** — свежий stable (rustup); сборка проверена на 1.96.
- **protoc** (protobuf) — генерация Rust из `.proto`
- **cmake**, **C++-компилятор** (g++/clang), **libclang** — для нативных зависимостей (RocksDB bindgen)

```bash
# macOS
brew install rustup protobuf cmake      # libclang идёт с Xcode CommandLineTools
# Debian/Ubuntu
sudo apt-get install -y protobuf-compiler g++ cmake clang libclang-dev
```

## Быстрый старт

### 1. Сборка

```bash
cargo build --release --features mcp-server,rocksdb
# бинарь: target/release/graphmind-v2
```

`rocksdb` даёт персист; без фичи бэкенд файловый. `mcp-server` включён по умолчанию.

### 2. Конфиг

Бинарь читает `.env` **рядом с собой** (dotenvy из каталога бинаря, не из cwd) — положите `.env` в `target/release/`. За основу возьмите `config.example.env` — в нём рабочие примеры для локальной Ollama/LM Studio и полностью офлайн-режима (`cp config.example.env .env` и впишите свои endpoint'ы + ключи).

**Зависимость от моделей.** Семантический поиск и причинный слой требуют внешних endpoint'ов (OpenAI-совместимый API):
- **LLM** — любой `/v1/chat/completions` (локальная Ollama/LM Studio, vLLM, любой OpenAI-совместимый endpoint).
- **Эмбеддинги** — отдельный `/v1/embeddings`: локальная Ollama `bge-m3` (1024d, многоязычная) или любой OpenAI-совместимый endpoint. `GRAPHMIND_EMBEDDING_DIM` должен совпадать с моделью (bge-m3 → 1024).

Без LLM/эмбеддингов сервер поднимется, но поиск деградирует до ключевых слов, а причинный слой — до эвристики.

### 3. Запуск (HTTP-сервер MCP)

```bash
GRAPHMIND_MCP_HTTP=1 ./target/release/graphmind-v2
# слушает 0.0.0.0:50052 (переопределить: GRAPHMIND_MCP_HTTP_ADDR)
curl -s http://127.0.0.1:50052/health
```

Транспорты одного сервера: `POST /mcp` (Streamable HTTP — Cursor/Codex/OpenCode), `GET /sse` + `/message` (Claude Code), а также **stdio** (без `GRAPHMIND_MCP_HTTP`). Модель работы — **один HTTP-сервер владеет данными, много клиентов** (RocksDB держит эксклюзивный lock на data_dir, поэтому два серверных процесса на одну папку поднять нельзя).

Turnkey в контейнере — [`docker-compose.yml`](docker-compose.yml): `docker compose up -d` (сервер на :50052, том для данных, env из `.env`).

## Подключить агента

Кратко:

| Агент | Транспорт | Как |
|---|---|---|
| Claude Code | SSE | `claude mcp add --transport sse graphmind http://127.0.0.1:50052/sse` |
| Cursor | Streamable HTTP | `.cursor/mcp.json` → `http://127.0.0.1:50052/mcp` |
| Codex | Streamable HTTP | `~/.codex/config.toml` → `/mcp` |
| OpenCode | Streamable HTTP | `opencode.json` → `type: remote`, `/mcp` |
| Kodik | Streamable HTTP | `http://127.0.0.1:50052/mcp` |

Готовые правила/хуки автоматизации памяти для Cursor — [`templates/`](templates/). Собранный **плагин для Kodik** (MCP + правило + навык + суб-агент-куратор + хуки) — [`kodik-plugin/`](kodik-plugin/): установка через `Ctrl+Shift+X` или из этого репозитория.

## Структура

```
.
├── src/                    # Rust: mcp_server/ (handler, http_server, protocol), actors/, persistence/, queue/
├── proto/memory.proto      # protobuf-схема
├── tests/                  # интеграционные тесты
├── docs/                   # документация (TOOLS, MEMORY)
├── templates/              # правила/хуки автоматизации памяти (Cursor)
├── kodik-plugin/           # готовый плагин для Kodik (MCP + правило + навык + суб-агент + хуки)
├── Dockerfile, docker-compose.yml
├── config.example.env
└── LICENSE                 # MIT
```

## Разработка

```bash
cargo test
cargo clippy -- -D warnings && cargo fmt -- --check
```
