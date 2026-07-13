# MCP-инструменты

Справочник по всем инструментам GraphMind v2, доступным через Model Context Protocol.

## Управление памятью

### propose_new_memory

Создаёт новый узел памяти (L2-атом).

**Параметры:**
- `content` (обязательный) — содержимое узла
- `level` — уровень: `L2`, `L1`, `L0`, `GKL` (по умолчанию `L2`)
- `node_type` — тип: `atom`, `cause`, `effect`, `rule`
- `parent_id` — ID родительского кластера
- `scope` — `workspace` или `global`
- `workspace_id` — раздел хранилища

### update_node

Обновляет содержимое существующего узла.

**Параметры:** `node_id`, `content`, `scope`

### fetch_l2_atoms

Получает полный текст узлов по их ID.

**Параметры:** `atom_ids[]`, `scope`

### list_memory

Показывает все карточки в памяти (L2-узлы, новые сверху): id, уровень, тип, статус, содержимое, дата.

**Параметры:** `limit` (по умолчанию 100)

### archive_node / restore_node

Софт-архивация и восстановление узла.

**Параметры:** `node_id`

## Связи и граф

### link_nodes

Создаёт ребро между двумя узлами.

**Параметры:** `from_id`, `to_id`, `relation`, `confidence`, `workspace_id`

### unlink_edge

Удаляет ребро по ID.

**Параметры:** `edge_id`

### list_edges

Поиск рёбер по фильтру (`from_id` и/или `to_id`).

### get_chain

Обход причинной цепочки.

**Параметры:**
- `anchor` — `{ id, type: "node"|"symptom", text }`
- `direction` — `backward` (симптом→корень), `forward_pre` (причина→эффекты), `forward_post` (действие→последствия)
- `max_depth` (по умолчанию 3)
- `scope`

### suggest_related

BFS по рёбрам — возвращает ближайших соседей узла.

**Параметры:** `node_id`, `top_k` (по умолчанию 10), `max_depth` (по умолчанию 2)

## Поиск

### search_nodes

Ключевой поиск по узлам.

**Параметры:** `query`, `level`, `limit` (по умолчанию 10), `workspace_id`

### vector_search

Векторный (семантический) поиск по содержимому.

**Параметры:** `text` или `vector[]`, `top_k` (по умолчанию 10), `min_score` (по умолчанию 0), `workspace_id`

### memory_query

Поиск по памяти (ключевой + семантический).

**Параметры:** `query`, `scope` (`workspace`/`global`), `depth` (0–2)

## Причинно-следственный анализ

### find_contradictions

Находит противоречия среди фактов, причин, следствий и правил.

### predict_risks

Прогноз рисков от узла-причины: возможные эффекты и уровень риска.

**Параметры:** `cause_id`

### propose_causal_link

Предлагает причинную связь между двумя узлами (тип + уверенность + обоснование). Не создаёт ребро — только предложение для подтверждения через `link_nodes`.

**Параметры:** `source_id`, `target_id`

### dream_reflection

Находит повторяющиеся причинные паттерны и выводит правила IF/THEN.

## Сессия и контекст

### record_action

Записывает действие в кратковременную память сессии (S0).

**Параметры:** `summary`, `raw_text`, `related_nodes[]`

### get_s0_context

Получает последние действия S0.

**Параметры:** `limit` (по умолчанию 10, максимум 20)

### flush_session_memory

Завершает сессию: snapshot S0 → re-enqueue → s0.clear → queue.drain_to_l2. При `force=true` запускает causal_reflection.

**Параметры:** `summary`, `related_nodes[]`, `force`

## Workspace'ы

### list_workspaces

Список всех workspace'ов с node_count / edge_count / status.

### create_workspace

Создаёт новый workspace.

**Параметры:** `name`, `path`

### switch_workspace

Переключает активный workspace.

**Параметры:** `workspace_id`

### archive_workspace

Софт-архивация workspace.

**Параметры:** `workspace_id`

### detect_workspace_from_context

Определяет workspace по рабочей директории.

**Параметры:** `cwd`

### fetch_from_workspace

Получает узлы из конкретного workspace с ключевым фильтром.

**Параметры:** `workspace_id`, `query`, `limit` (по умолчанию 20)

### find_workspace_overlaps

Jaccard similarity по node_ids между source и target workspaces.

**Параметры:** `source_workspace`, `min_similarity` (по умолчанию 0.3), `limit` (по умолчанию 10)

### suggest_cross_workspace_links

Находит shared tags / shared nodes между workspace'ами.

**Параметры:** `workspace_id`, `limit` (по умолчанию 10)

## Bootstrap и консолидация

### bootstrap_memory

Автоподстройка памяти: detect workspace + memory_query + recent context.

**Параметры:** `cwd`, `query`, `depth` (0–2)

### consolidate_workspace

Полный pipeline: drain queue → L2 → L1 autogen → L0 autogen.

**Параметры:** `workspace_id`

### route_l1

Прокидывает L2-атом в L1-домен (multi-domain scoring).

**Параметры:** `workspace_id`, `l2_atom_id`

### search_l0_clusters

Поиск по L0-кластерам workspace: keyword-фильтрация по content + tags.

**Параметры:** `workspace_id`, `query`, `limit` (по умолчанию 10)

## Планировщик (P0–P3)

### plan_create_p0

Создаёт P0-план (верхний уровень: описание проблемы).

**Параметры:** `description`, `autonomous_mode` (по умолчанию false)

### plan_propose_p1

Предлагает P1-подплан для P0 (требует review).

**Параметры:** `p0_id`, `description`

### plan_approve_p1 / plan_reject_p1

Одобряет или отклоняет P1-подплан.

**Параметры:** `p1_id` (+ `reason` для reject)

### plan_decompose

Декомпозирует узел плана в P2/P3 children.

**Параметры:** `node_id`

### plan_claim

Sub-agent забирает P3-план в работу.

**Параметры:** `p3_id`, `agent_id`

### plan_complete

Завершает P3-план с результатом.

**Параметры:** `p3_id`, `result`

### plan_set_problem / plan_resolve_problem

Помечает план как проблемный / разрешает проблему.

**Параметры:** `plan_id` (+ `problem_comment` / `resolution`)

### plan_status

Список планов с фильтром по статусу.

**Параметры:** `filter` — `created`, `in_progress`, `pending_review`, `approved`, `rejected`, `problem`, `done`, `archived`

### plan_delete / plan_archive

Удаляет или архивирует план.

**Параметры:** `plan_id` (+ `force` для delete)

## Trust Firewall

### verify_input

Верифицирует вход по источнику, консистентности с памятью, verifiability и tone-аномалиям.

**Параметры:**
- `input` (обязательный)
- `source`
- `source_type` — `user_direct`, `user_document`, `web_search`, `agent_internal`, `external_api`

## Диагностика

### orchestrator_status

Диагностика координатора памяти: per-workspace счётчики новых узлов, что консолидируется сейчас, последние решения.

### get_irritation_report

Отчёт о раздражении/эмоц. состоянии по неопределённостям графа (Cause без Effect, Effect без Cause, противоречия).

### list_curiosity_tasks / close_curiosity_task

Список задач-исследований по текущим неопределённостям графа / завершение задачи по id.
