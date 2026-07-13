#!/usr/bin/env node
/**
 * Инжектирует протокол GraphMind в начало сессии Cursor.
 */
import { readFileSync } from "node:fs";

const input = JSON.parse(readFileSync(0, "utf8"));
const roots = input.workspace_roots ?? [];
const cwd = roots[0] ?? "<PROJECT_ROOT>";

const additional_context = [
  "## GraphMind — автопамять (обязательно)",
  "",
  "MCP-сервер **graphmind** подключён. Перед основной работой:",
  "",
  "1. Вызови `bootstrap_memory({ cwd: \"" + cwd + "\", query: \"<суть запроса пользователя>\", depth: 1 })`.",
  "2. Используй полученный контекст; не дублируй вопросы, на которые память уже ответила.",
  "",
  "Во время работы после значимых шагов: `record_action({ summary })`.",
  "Факт этого репо: `propose_new_memory({ level: \"L2\", node_type: \"atom\", content })`.",
  "Кросс-проектное решение (hooks, MCP, Docker, CI): добавь `scope: \"global\"`.",
  "По hook stop или перед завершением: `flush_session_memory({ summary })`.",
].join("\n");

process.stdout.write(JSON.stringify({ additional_context }));
