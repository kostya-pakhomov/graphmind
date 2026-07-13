#!/usr/bin/env node
/**
 * Напоминание record_action / кросс-проектной памяти после значимых правок файлов.
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { isFrameworkPath } from "./graphmind-constants.mjs";

const HOOK_DIR = dirname(fileURLToPath(import.meta.url));
const STATE_FILE = join(HOOK_DIR, ".graphmind-post-tool-state.json");
const THROTTLE_MS = 45_000;
const EDITS_BEFORE_REMINDER = 2;

const SKIP_PATH_RE =
  /(?:^|[\\/])\.cursor[\\/]hooks[\\/]|(?:^|[\\/])node_modules[\\/]|(?:^|[\\/])dist[\\/]|\.fingerprint$|package-lock\.json$/i;

const EDIT_TOOLS = new Set(["Write", "StrReplace", "EditNotebook"]);

function readInput() {
  try {
    return JSON.parse(readFileSync(0, "utf8"));
  } catch {
    return null;
  }
}

function loadState() {
  try {
    if (existsSync(STATE_FILE)) {
      return JSON.parse(readFileSync(STATE_FILE, "utf8"));
    }
  } catch {
    /* ignore */
  }
  return { edit_count: 0, last_reminder_at: 0, recent_paths: [] };
}

function saveState(state) {
  mkdirSync(dirname(STATE_FILE), { recursive: true });
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), "utf8");
}

function extractPath(toolName, toolInput) {
  if (!toolInput || typeof toolInput !== "object") return null;
  if (toolName === "EditNotebook") {
    return typeof toolInput.target_notebook === "string"
      ? toolInput.target_notebook
      : null;
  }
  return typeof toolInput.path === "string" ? toolInput.path : null;
}

function shouldSkipPath(path) {
  if (!path) return true;
  return SKIP_PATH_RE.test(path.replace(/\\/g, "/"));
}

function basename(path) {
  const parts = path.replace(/\\/g, "/").split("/");
  return parts[parts.length - 1] ?? path;
}

const input = readInput();
if (!input) {
  process.stdout.write("{}");
  process.exit(0);
}

const toolName = input.tool_name ?? "";
if (!EDIT_TOOLS.has(toolName)) {
  process.stdout.write("{}");
  process.exit(0);
}

const path = extractPath(toolName, input.tool_input);
if (shouldSkipPath(path)) {
  process.stdout.write("{}");
  process.exit(0);
}

const state = loadState();
state.recent_paths = [...(state.recent_paths ?? []), path].slice(-5);
state.edit_count = (state.edit_count ?? 0) + 1;

const frameworkTouch = state.recent_paths.some(isFrameworkPath);
const threshold = frameworkTouch ? 1 : EDITS_BEFORE_REMINDER;

const now = Date.now();
const due =
  state.edit_count >= threshold && now - (state.last_reminder_at ?? 0) >= THROTTLE_MS;

if (!due) {
  saveState(state);
  process.stdout.write("{}");
  process.exit(0);
}

state.edit_count = 0;
state.last_reminder_at = now;
saveState(state);

const files = [...new Set(state.recent_paths.map(basename))].join(", ");
const parts = [
  "[GraphMind] Накопились правки (" + files + ").",
  "Если шаг завершён — `record_action({ summary })` (workspace).",
];

if (frameworkTouch) {
  parts.push(
    "Правки инфраструктуры/фреймворка — оцени кросс-проектную запись: `propose_new_memory({ level: \"L2\", node_type: \"atom\", scope: \"global\", content })`.",
  );
}

process.stdout.write(JSON.stringify({ additional_context: parts.join(" ") }));
