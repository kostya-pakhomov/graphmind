# Шаблон Cursor automation для GraphMind

Автоматизация памяти через Cursor Hooks (`bootstrap_memory` при старте, напоминания `record_action`/`flush_session_memory`). 

Скопировать в корень проекта:

```text
.cursor/hooks.json          ← из этой папки
.cursor/hooks/*.mjs         ← из hooks/
.cursor/rules/*.mdc         ← из rules/
```

После копирования замените `<PROJECT_ROOT>` в `rules/graphmind-memory.mdc`. Требуется Node.js ≥18 в PATH и подключённый MCP-сервер graphmind (см. корневой `README.md`).
