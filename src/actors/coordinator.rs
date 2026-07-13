//! MemoryOrchestrator — минимальный координатор памяти (шина событий + CycleTrigger).
//!
//! Зачем: раньше «когда запускать консолидацию» нигде не решалось — она шла только
//! руками (инструмент `consolidate_workspace`). `CycleTrigger` из концепт-дока
//! (≥N новых узлов / долгий idle → консолидация) описан, но не был построен →
//! «автономная эволюция памяти» (TECH-SPEC §5) не работала.
//!
//! Что делает эта версия: единая in-process шина `MemoryEvent` (mpsc), фоновый цикл
//! слушает события, ведёт per-workspace счётчики и сам запускает `ConsolidateRunner`
//! при достижении порога или простое. Плюс диагностика для `orchestrator_status`.
//!
//! Чего здесь НЕТ (сознательно, по согласованию): PolicyEngine/чтение GKL-правил,
//! BudgetTracker сверх in-flight-гарда, глобальный лидер. Имя `OrchestratorActor`
//! занято LLM-разбивщиком планов — этот тип называется `MemoryOrchestrator`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{info, warn};

use super::ConsolidateRunner;

/// Событие домена памяти. Много производителей (handler, очередь) → один потребитель.
#[derive(Debug, Clone)]
pub enum MemoryEvent {
    /// Записан новый L2-узел (propose_new_memory / очередь) — считается CycleTrigger'ом.
    NodeWritten { workspace: String },
    /// Зафиксировано действие (record_action) — обновляет «последнюю активность».
    ActionRecorded { workspace: String },
    /// Фаервол отклонил запись (для журнала решений).
    TrustFirewallBlock { source_id: String, reason: String },
    /// Периодический тик — проверка простоя (idle sweep).
    Tick,
}

/// Настройки CycleTrigger.
#[derive(Debug, Clone)]
pub struct CoordinatorCfg {
    pub min_new_nodes: usize,
    pub max_idle_secs: u64,
    pub tick_secs: u64,
    pub max_decisions: usize,
}

impl Default for CoordinatorCfg {
    fn default() -> Self {
        Self {
            min_new_nodes: 10,
            max_idle_secs: 3600,
            tick_secs: 60,
            max_decisions: 50,
        }
    }
}

struct WsCounters {
    new_since_consolidation: usize,
    last_activity: Instant,
}

/// Запись в журнале решений координатора (для диагностики).
#[derive(Debug, Clone, Serialize)]
pub struct DecisionLog {
    pub at: String,
    pub workspace: String,
    pub action: String,
    pub detail: String,
}

/// Координатор памяти: владеет приёмником событий, счётчиками и запускает консолидацию.
pub struct MemoryOrchestrator {
    event_tx: mpsc::UnboundedSender<MemoryEvent>,
    rx: Mutex<Option<mpsc::UnboundedReceiver<MemoryEvent>>>,
    runner: Arc<ConsolidateRunner>,
    cfg: CoordinatorCfg,
    state: RwLock<HashMap<String, WsCounters>>,
    consolidating: RwLock<HashSet<String>>,
    decisions: RwLock<VecDeque<DecisionLog>>,
}

impl MemoryOrchestrator {
    pub fn new(
        event_tx: mpsc::UnboundedSender<MemoryEvent>,
        rx: mpsc::UnboundedReceiver<MemoryEvent>,
        runner: Arc<ConsolidateRunner>,
        cfg: CoordinatorCfg,
    ) -> Self {
        Self {
            event_tx,
            rx: Mutex::new(Some(rx)),
            runner,
            cfg,
            state: RwLock::new(HashMap::new()),
            consolidating: RwLock::new(HashSet::new()),
            decisions: RwLock::new(VecDeque::new()),
        }
    }

    /// Отправитель событий для эмиттеров (handler, очередь).
    pub fn sender(&self) -> mpsc::UnboundedSender<MemoryEvent> {
        self.event_tx.clone()
    }

    /// Сбросить счётчик новых узлов (напр. после ручной консолидации через инструмент),
    /// чтобы координатор не запускал лишний авто-цикл.
    pub async fn note_consolidated(&self, workspace: &str) {
        if let Some(c) = self.state.write().await.get_mut(workspace) {
            c.new_since_consolidation = 0;
            c.last_activity = Instant::now();
        }
    }

    /// Запустить фоновый цикл: тикер + обработчик событий. Не блокирует.
    pub fn spawn_loop(self: Arc<Self>) {
        // Тикер: периодически шлёт Tick самому себе (idle-детекция).
        let tick_tx = self.event_tx.clone();
        let tick_secs = self.cfg.tick_secs.max(1);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(tick_secs));
            interval.tick().await; // первый тик срабатывает сразу — пропускаем
            loop {
                interval.tick().await;
                if tick_tx.send(MemoryEvent::Tick).is_err() {
                    break; // приёмник закрыт — координатор умер
                }
            }
        });

        // Основной цикл обработки событий.
        tokio::spawn(async move {
            let mut rx = match self.rx.lock().await.take() {
                Some(r) => r,
                None => {
                    warn!("MemoryOrchestrator: приёмник уже забран, цикл не стартует");
                    return;
                }
            };
            info!(
                "MemoryOrchestrator loop started (min_new_nodes={}, max_idle_secs={}, tick_secs={})",
                self.cfg.min_new_nodes, self.cfg.max_idle_secs, self.cfg.tick_secs
            );
            while let Some(ev) = rx.recv().await {
                self.handle_event(ev).await;
            }
        });
    }

    async fn handle_event(&self, ev: MemoryEvent) {
        match ev {
            MemoryEvent::NodeWritten { workspace } => {
                let fire = {
                    let mut st = self.state.write().await;
                    let c = st.entry(workspace.clone()).or_insert_with(WsCounters::fresh);
                    c.new_since_consolidation += 1;
                    c.last_activity = Instant::now();
                    c.new_since_consolidation >= self.cfg.min_new_nodes
                };
                if fire {
                    self.trigger_consolidation(&workspace, "cycle:min_new_nodes").await;
                }
            }
            MemoryEvent::ActionRecorded { workspace } => {
                let mut st = self.state.write().await;
                st.entry(workspace).or_insert_with(WsCounters::fresh).last_activity = Instant::now();
            }
            MemoryEvent::TrustFirewallBlock { source_id, reason } => {
                self.record_decision("(firewall)", "trust_block", &format!("{source_id}: {reason}"))
                    .await;
            }
            MemoryEvent::Tick => {
                let due: Vec<String> = {
                    let st = self.state.read().await;
                    st.iter()
                        .filter(|(_, c)| {
                            c.new_since_consolidation > 0
                                && c.last_activity.elapsed().as_secs() >= self.cfg.max_idle_secs
                        })
                        .map(|(ws, _)| ws.clone())
                        .collect()
                };
                for ws in due {
                    self.trigger_consolidation(&ws, "cycle:idle").await;
                }
            }
        }
    }

    async fn trigger_consolidation(&self, workspace: &str, reason: &str) {
        // In-flight guard: не запускать две консолидации одного workspace разом.
        {
            let mut cs = self.consolidating.write().await;
            if !cs.insert(workspace.to_string()) {
                return; // уже консолидируется
            }
        }
        info!("MemoryOrchestrator: consolidating '{}' (reason={})", workspace, reason);

        let result = self.runner.run(workspace).await;
        match &result {
            Ok(s) => {
                if let Some(c) = self.state.write().await.get_mut(workspace) {
                    c.new_since_consolidation = 0;
                    c.last_activity = Instant::now();
                }
                self.record_decision(
                    workspace,
                    reason,
                    &format!(
                        "l2_atoms={}, new_l1={}, new_l0={}, drained={}",
                        s.l2_atoms, s.new_l1_count, s.new_l0_count, s.drained_from_queue
                    ),
                )
                .await;
            }
            Err(e) => {
                warn!("MemoryOrchestrator: consolidation of '{}' failed: {}", workspace, e);
                self.record_decision(workspace, &format!("{reason}:error"), &e.to_string())
                    .await;
            }
        }

        self.consolidating.write().await.remove(workspace);
    }

    async fn record_decision(&self, workspace: &str, action: &str, detail: &str) {
        let entry = DecisionLog {
            at: chrono::Utc::now().to_rfc3339(),
            workspace: workspace.to_string(),
            action: action.to_string(),
            detail: detail.to_string(),
        };
        let mut d = self.decisions.write().await;
        d.push_front(entry);
        while d.len() > self.cfg.max_decisions {
            d.pop_back();
        }
    }

    /// Снимок состояния для инструмента `orchestrator_status`.
    pub async fn status(&self) -> serde_json::Value {
        let workspaces: Vec<serde_json::Value> = {
            let st = self.state.read().await;
            st.iter()
                .map(|(ws, c)| {
                    serde_json::json!({
                        "workspace": ws,
                        "new_since_consolidation": c.new_since_consolidation,
                        "idle_secs": c.last_activity.elapsed().as_secs(),
                    })
                })
                .collect()
        };
        let consolidating: Vec<String> = self.consolidating.read().await.iter().cloned().collect();
        let recent: Vec<DecisionLog> = self.decisions.read().await.iter().take(20).cloned().collect();

        serde_json::json!({
            "cfg": {
                "min_new_nodes": self.cfg.min_new_nodes,
                "max_idle_secs": self.cfg.max_idle_secs,
                "tick_secs": self.cfg.tick_secs,
            },
            "workspaces": workspaces,
            "consolidating": consolidating,
            "recent_decisions": recent,
        })
    }
}

impl WsCounters {
    fn fresh() -> Self {
        Self {
            new_since_consolidation: 0,
            last_activity: Instant::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::L2Actor;
    use crate::persistence::InMemoryBackend;

    fn runner_no_layers() -> Arc<ConsolidateRunner> {
        let l2 = Arc::new(RwLock::new(L2Actor::new(Arc::new(InMemoryBackend::new()))));
        // Без queue/l1/l0: run() вернёт Ok со счётчиками 0 — достаточно для проверки триггера.
        Arc::new(ConsolidateRunner::new(l2, None, None, None))
    }

    fn orch(cfg: CoordinatorCfg) -> MemoryOrchestrator {
        let (tx, rx) = mpsc::unbounded_channel();
        MemoryOrchestrator::new(tx, rx, runner_no_layers(), cfg)
    }

    #[tokio::test]
    async fn test_cycle_trigger_fires_at_threshold() {
        let o = orch(CoordinatorCfg { min_new_nodes: 3, max_idle_secs: 3600, tick_secs: 60, max_decisions: 10 });

        // Два узла — порог не достигнут.
        o.handle_event(MemoryEvent::NodeWritten { workspace: "w".into() }).await;
        o.handle_event(MemoryEvent::NodeWritten { workspace: "w".into() }).await;
        let s1 = o.status().await;
        assert_eq!(s1["recent_decisions"].as_array().unwrap().len(), 0);

        // Третий — срабатывает CycleTrigger.
        o.handle_event(MemoryEvent::NodeWritten { workspace: "w".into() }).await;
        let s2 = o.status().await;
        let decisions = s2["recent_decisions"].as_array().unwrap();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0]["action"], "cycle:min_new_nodes");

        // Счётчик сброшен после консолидации.
        let ws = s2["workspaces"].as_array().unwrap();
        let w = ws.iter().find(|x| x["workspace"] == "w").unwrap();
        assert_eq!(w["new_since_consolidation"], 0);
    }

    #[tokio::test]
    async fn test_firewall_block_logged() {
        let o = orch(CoordinatorCfg::default());
        o.handle_event(MemoryEvent::TrustFirewallBlock {
            source_id: "project:bad".into(),
            reason: "манипуляция".into(),
        })
        .await;
        let s = o.status().await;
        let d = s["recent_decisions"].as_array().unwrap();
        assert_eq!(d.len(), 1);
        assert_eq!(d[0]["action"], "trust_block");
    }
}
