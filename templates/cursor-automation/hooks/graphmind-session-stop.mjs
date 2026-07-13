#!/usr/bin/env node
/**
 * Авто-followup для сохранения памяти GraphMind в конце сессии.
 */
import { readFileSync } from "node:fs";

const input = JSON.parse(readFileSync(0, "utf8"));
const status = input.status ?? "completed";
const loopCount = input.loop_count ?? 0;

if (status !== "completed" || loopCount > 0) {
  process.stdout.write("{}");
  process.exit(0);
}

const followup_message = [
  "[GraphMind] Сохрани память этой сессии через MCP graphmind:",
  "",
  "1. `flush_session_memory({ summary: \"...\" })` — итог сессии (workspace).",
  "2. Факты этого репо → `propose_new_memory({ level: \"L2\", node_type: \"atom\", content })`.",
  "3. Кросс-проектные решения (hooks, MCP, Docker, CI, IDE) → тот же вызов с `scope: \"global\"`.",
  "4. Кратко подтверди пользователю, что память сохранена.",
].join("\n");

process.stdout.write(JSON.stringify({ followup_message }));
