//! Scheduled methods — the `@Scheduled` / `spring-boot-starter-quartz`
//! analogue, for the single-instance case.
//!
//! ```ignore
//! #[component]
//! pub struct Reports { repo: ReportRepositoryImpl }
//!
//! #[scheduled_tasks]
//! impl Reports {
//!     #[scheduled(cron = "0 0 3 * * *")]
//!     async fn nightly(&self) -> Result<(), String> { ... }
//!
//!     #[scheduled(fixed_delay_seconds = 30)]
//!     async fn poll(&self) -> Result<(), String> { ... }
//! }
//! ```
//!
//! # What this is not
//!
//! It is **not** distributed. Every replica runs every task, exactly as
//! Spring's `@Scheduled` does without Quartz's clustered JobStore. Three
//! replicas means the nightly report runs three times. For work that must
//! happen once across a deployment, take a lock in the task itself — the
//! Redis starter is enough for that — or run it as a Kubernetes CronJob and
//! not here at all. Saying so plainly is more useful than a `distributed:
//! true` flag that only sometimes holds.
//!
//! # Overlap
//!
//! A task never runs concurrently with itself, but the three triggers get
//! there differently.
//!
//! `fixed_delay` measures from the *end* of the previous run, so a job that
//! takes longer than its interval simply runs less often. Nothing can
//! overlap, and nothing is skipped.
//!
//! `fixed_rate` and `cron` keep their cadence: the timer moves on while a
//! run is in flight, which is the whole point of a rate. An occurrence that
//! arrives during a previous run is **skipped and logged**, not queued — a
//! backlog of scheduled runs is almost never what anyone wants, and it turns
//! one slow night into a thundering herd the next morning. `Scheduler::stats`
//! counts the skips, so a task that is chronically too slow for its schedule
//! is visible rather than merely late.

pub use oxide_scheduler_macros::{scheduled_tasks, tasks_of};

use oxide_core::{ComponentDescriptor, Context};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// When a task runs, as written in the attribute.
///
/// Each variant holds the *unresolved* text, because a schedule that can
/// only be changed by recompiling is not much of a schedule. Every one of
/// them accepts a `${key:default}` placeholder, resolved against the
/// application configuration at boot:
///
/// ```ignore
/// #[scheduled(cron = "${reports.nightly-cron:0 0 3 * * *}")]
/// #[scheduled(fixed_delay_seconds = "${poller.delay:30}")]
/// ```
///
/// A literal is still just a literal, so `cron = "0 0 3 * * *"` and
/// `fixed_delay_seconds = 30` keep working unchanged.
#[derive(Debug, Clone)]
pub enum Trigger {
    /// A cron expression with seconds: `"0 0 3 * * *"` is 03:00 daily.
    Cron(String),
    /// Seconds to wait after the previous run *finished*.
    FixedDelay(String),
    /// Seconds between run *starts*. Skips an occurrence rather than
    /// overlapping when a run overruns.
    FixedRate(String),
}

/// A trigger with placeholders resolved and intervals parsed.
#[derive(Debug, Clone)]
pub enum ResolvedTrigger {
    Cron(String),
    FixedDelay(Duration),
    FixedRate(Duration),
}

/// Replaces a `${key}` or `${key:default}` spec with its configured value.
///
/// Anything that is not exactly one placeholder is returned unchanged, so a
/// literal cron expression -- which contains no `$` -- passes straight
/// through.
///
/// A placeholder with no configured value and no default is an error rather
/// than a silent fallback: a schedule that quietly did not take the value an
/// operator set is worse than one that refuses to start.
pub fn resolve_spec(spec: &str, config: &oxide_core::AppConfig) -> Result<String, String> {
    let trimmed = spec.trim();
    let Some(inner) = trimmed.strip_prefix("${").and_then(|r| r.strip_suffix('}')) else {
        return Ok(spec.to_string());
    };
    let (key, default) = match inner.split_once(':') {
        Some((key, default)) => (key, Some(default)),
        None => (inner, None),
    };
    match config.get_opt::<String>(key.trim()) {
        Ok(Some(value)) => Ok(value),
        // A non-string scalar (a number, for an interval) still binds; the
        // config tree keeps YAML types, so ask again untyped.
        Ok(None) | Err(_) => match config.get_opt::<serde_yaml_ng::Value>(key.trim()) {
            Ok(Some(serde_yaml_ng::Value::Number(n))) => Ok(n.to_string()),
            Ok(Some(serde_yaml_ng::Value::String(v))) => Ok(v),
            _ => default
                .map(str::to_string)
                .ok_or_else(|| format!("no value configured for '{key}' and the placeholder '{spec}' has no default")),
        },
    }
}

/// Resolves a trigger's placeholders and parses its interval.
pub fn resolve_trigger(trigger: &Trigger, config: &oxide_core::AppConfig) -> Result<ResolvedTrigger, String> {
    let seconds = |spec: &str| -> Result<Duration, String> {
        let resolved = resolve_spec(spec, config)?;
        let value: u64 = resolved
            .trim()
            .parse()
            .map_err(|_| format!("'{resolved}' is not a whole number of seconds (from '{spec}')"))?;
        if value == 0 {
            // A zero interval is a busy loop that would peg a core; almost
            // certainly a placeholder that resolved to the wrong thing.
            return Err(format!("an interval of 0 seconds would spin (from '{spec}')"));
        }
        Ok(Duration::from_secs(value))
    };
    Ok(match trigger {
        Trigger::Cron(spec) => ResolvedTrigger::Cron(resolve_spec(spec, config)?),
        Trigger::FixedDelay(spec) => ResolvedTrigger::FixedDelay(seconds(spec)?),
        Trigger::FixedRate(spec) => ResolvedTrigger::FixedRate(seconds(spec)?),
    })
}

/// One registered scheduled method.
///
/// Registered by `#[scheduled_tasks]`, which is on the impl block because a
/// method-level attribute cannot emit the module-level registration
/// `inventory` needs — the same constraint `#[kafka_listeners]` lives with.
pub struct ScheduledTask {
    pub name: &'static str,
    pub trigger: fn() -> Trigger,
    /// Resolves the component and runs the method once. `None` when the
    /// component is absent, which the scheduler treats as a hard error at
    /// boot rather than a task that silently never fires.
    pub run: RunFn,
}

pub type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;

/// A task's outcome: `Err` is logged and counted, never propagated.
pub type TaskResult = Result<(), String>;

/// What `#[scheduled_tasks]` generates for each task. A plain `fn` pointer
/// rather than a boxed closure, so `ScheduledTask` stays constructible in the
/// `static` that `inventory::submit!` places it in.
pub type RunFn = fn(&Context) -> Option<BoxFuture<TaskResult>>;

inventory::collect!(ScheduledTask);

/// Configuration bound to `oxide.scheduler`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, oxide_boot::ConfigProperties)]
#[config(prefix = "oxide.scheduler", default)]
#[serde(rename_all = "kebab-case")]
pub struct SchedulerProps {
    /// Turns every task off. Useful for a replica that should serve traffic
    /// but not run jobs, and for tests.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// How long `pre_destroy` waits for a running task to finish before
    /// giving up and letting shutdown continue.
    #[serde(default = "default_shutdown_seconds")]
    pub shutdown_timeout_seconds: u64,
}

fn default_true() -> bool {
    true
}
fn default_shutdown_seconds() -> u64 {
    30
}

impl Default for SchedulerProps {
    fn default() -> Self {
        Self { enabled: default_true(), shutdown_timeout_seconds: default_shutdown_seconds() }
    }
}

/// Runtime counters for one task, readable from tests and useful in a log.
#[derive(Default)]
pub struct TaskStats {
    pub runs: AtomicU64,
    pub failures: AtomicU64,
    pub skipped: AtomicU64,
    running: AtomicBool,
}

/// The scheduler component. Injectable, mostly so tests can read counters.
#[derive(Clone)]
pub struct Scheduler {
    stats: Arc<Vec<(&'static str, Arc<TaskStats>)>>,
    handles: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    /// A flag *and* a notify, not a bare `Notify`.
    ///
    /// `notify_waiters` only wakes waiters registered at the moment it is
    /// called. A task that finishes a run and re-registers afterwards never
    /// sees the signal and loops forever -- which showed up as shutdown
    /// waiting out the full timeout for a task that was merely slow. The
    /// flag is checked at the top of every iteration, so a late arrival
    /// still stops.
    shutdown: Arc<(AtomicBool, tokio::sync::Notify)>,
}

impl Scheduler {
    /// Counters for one task by name, or `None` if no such task exists.
    pub fn stats(&self, name: &str) -> Option<Arc<TaskStats>> {
        self.stats.iter().find(|(n, _)| *n == name).map(|(_, s)| Arc::clone(s))
    }

    /// The names of every registered task, sorted.
    pub fn task_names(&self) -> Vec<&'static str> {
        let mut names: Vec<&'static str> = self.stats.iter().map(|(n, _)| *n).collect();
        names.sort_unstable();
        names
    }
}

/// Computes the wait until a cron expression's next occurrence.
///
/// Separated out so the schedule maths is testable without waiting for wall
/// clock time to pass.
pub fn next_cron_delay(expression: &str, from: chrono::DateTime<chrono::Local>) -> Result<Duration, String> {
    use croner::Cron;
    use std::str::FromStr;
    let cron = Cron::from_str(expression).map_err(|e| format!("invalid cron expression '{expression}': {e}"))?;
    let next = cron
        .find_next_occurrence(&from, false)
        .map_err(|e| format!("cron expression '{expression}' has no next occurrence: {e}"))?;
    (next - from).to_std().map_err(|e| format!("cron expression '{expression}' produced a past time: {e}"))
}

async fn run_once(task: &'static ScheduledTask, ctx: &Context, stats: &TaskStats) {
    // A task never overlaps itself. `swap` rather than a check-then-set so
    // two triggers arriving together cannot both see `false`.
    if stats.running.swap(true, Ordering::SeqCst) {
        stats.skipped.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("scheduled_task_skipped_total", "task" => task.name).increment(1);
        tracing::warn!(task = task.name, "previous run is still going; skipping this occurrence");
        return;
    }

    let started = std::time::Instant::now();
    let outcome = match (task.run)(ctx) {
        Some(future) => future.await,
        None => Err("component is no longer in the context".to_string()),
    };
    stats.running.store(false, Ordering::SeqCst);
    stats.runs.fetch_add(1, Ordering::Relaxed);

    let result = match &outcome {
        Ok(()) => "ok",
        Err(e) => {
            // Logged and counted, never propagated: a failing scheduled task
            // must not stop its own schedule, or one bad night silently ends
            // the job forever.
            stats.failures.fetch_add(1, Ordering::Relaxed);
            tracing::error!(task = task.name, error = %e, "scheduled task failed");
            "error"
        }
    };
    metrics::counter!("scheduled_task_runs_total", "task" => task.name, "result" => result).increment(1);
    metrics::histogram!("scheduled_task_duration_seconds", "task" => task.name).record(started.elapsed().as_secs_f64());
}

inventory::submit! {
    ComponentDescriptor::builder(
        "Scheduler",
        std::any::TypeId::of::<Scheduler>,
        |b| {
            let props = b.resolve::<SchedulerProps>()?;
            let stats: Vec<(&'static str, Arc<TaskStats>)> = inventory::iter::<ScheduledTask>
                .into_iter()
                .map(|t| (t.name, Arc::new(TaskStats::default())))
                .collect();
            let _ = &props;
            b.insert(Scheduler {
                stats: Arc::new(stats),
                handles: Arc::new(std::sync::Mutex::new(Vec::new())),
                shutdown: Arc::new((AtomicBool::new(false), tokio::sync::Notify::new())),
            });
            Ok(())
        },
    )
    .deps(|| vec![(std::any::TypeId::of::<SchedulerProps>(), "SchedulerProps")])
    .post_construct(|ctx: &Context| {
        let scheduler = ctx.get::<Scheduler>().expect("Scheduler exists");
        let props = (*ctx.get::<SchedulerProps>().expect("SchedulerProps exists")).clone();
        // The resolved configuration, for `${...}` placeholders in triggers.
        let config = (*ctx.get::<oxide_core::AppConfig>().expect("AppConfig is always in the context")).clone();
        let ctx = ctx.clone();
        Box::pin(async move {
            if !props.enabled {
                tracing::info!("scheduler disabled by configuration; no tasks will run");
                return Ok(());
            }

            let mut handles = Vec::new();
            for task in inventory::iter::<ScheduledTask> {
                // Placeholders are resolved and cron expressions validated
                // here rather than on first fire: a typo, or a
                // `${schedule.cron}` nobody configured, should fail the
                // deployment instead of going unnoticed until 03:00.
                let resolved = resolve_trigger(&(task.trigger)(), &config).and_then(|t| {
                    if let ResolvedTrigger::Cron(expression) = &t {
                        next_cron_delay(expression, chrono::Local::now())?;
                    }
                    Ok(t)
                });
                let resolved = match resolved {
                    Ok(t) => t,
                    Err(e) => {
                        return Err(oxide_core::ContextError::Construct {
                            type_name: "Scheduler",
                            message: format!("task '{}': {e}", task.name),
                        }
                        .into());
                    }
                };
                tracing::debug!(task = task.name, trigger = ?resolved, "scheduling");

                let stats = scheduler
                    .stats(task.name)
                    .expect("every registered task has a stats entry, built alongside it above");
                let ctx = ctx.clone();
                let shutdown = Arc::clone(&scheduler.shutdown);
                handles.push(tokio::spawn(async move {
                    // For `FixedRate`, the next wake is measured from the
                    // previous *start*, so an occurrence can arrive while a
                    // long run is still going -- and be skipped by
                    // `run_once`. Measuring from the end (which is
                    // `FixedDelay`) makes overlap impossible and the two
                    // triggers indistinguishable.
                    let mut next_rate_wake = tokio::time::Instant::now();

                    loop {
                        if shutdown.0.load(Ordering::SeqCst) {
                            return;
                        }

                        let sleep = match &resolved {
                            ResolvedTrigger::Cron(expression) => {
                                let wait = match next_cron_delay(expression, chrono::Local::now()) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        // Validated at boot, so this is a
                                        // clock jump or a leap-second edge:
                                        // back off rather than spin.
                                        tracing::error!(task = task.name, error = %e, "cron scheduling failed");
                                        Duration::from_secs(60)
                                    }
                                };
                                tokio::time::sleep_until(tokio::time::Instant::now() + wait)
                            }
                            ResolvedTrigger::FixedDelay(d) => {
                                tokio::time::sleep_until(tokio::time::Instant::now() + *d)
                            }
                            ResolvedTrigger::FixedRate(d) => {
                                next_rate_wake += *d;
                                let now = tokio::time::Instant::now();
                                // A run that overran several intervals must
                                // not then fire that many times in a row.
                                if next_rate_wake < now {
                                    next_rate_wake = now;
                                }
                                tokio::time::sleep_until(next_rate_wake)
                            }
                        };

                        tokio::select! {
                            // Shutdown wins over the timer, so a task with a
                            // long interval does not hold the process open.
                            _ = shutdown.1.notified() => return,
                            _ = sleep => {}
                        }

                        // `FixedDelay` awaits the run, by definition: the
                        // next wait starts when this one finishes.
                        //
                        // `FixedRate` and `Cron` must keep their cadence, so
                        // the run is spawned and the timer moves on. That is
                        // what makes an overlapping occurrence possible at
                        // all -- and `run_once`'s guard is what turns it
                        // into a skip rather than a second concurrent run.
                        match &resolved {
                            ResolvedTrigger::FixedDelay(_) => run_once(task, &ctx, &stats).await,
                            ResolvedTrigger::FixedRate(_) | ResolvedTrigger::Cron(_) => {
                                let ctx = ctx.clone();
                                let stats = Arc::clone(&stats);
                                tokio::spawn(async move { run_once(task, &ctx, &stats).await });
                            }
                        }
                    }
                }));
            }

            let count = handles.len();
            *scheduler.handles.lock().unwrap_or_else(|p| p.into_inner()) = handles;
            if count > 0 {
                tracing::info!(tasks = count, "scheduler started");
            }
            Ok(())
        })
    })
    .pre_destroy(|ctx: &Context| {
        let scheduler = ctx.get::<Scheduler>().expect("Scheduler exists");
        let props = (*ctx.get::<SchedulerProps>().expect("SchedulerProps exists")).clone();
        Box::pin(async move {
            // Flag first, then wake: a task between iterations sees the
            // flag, one asleep is woken by the notify.
            scheduler.shutdown.0.store(true, Ordering::SeqCst);
            scheduler.shutdown.1.notify_waiters();
            let handles: Vec<_> = std::mem::take(&mut *scheduler.handles.lock().unwrap_or_else(|p| p.into_inner()));
            let budget = Duration::from_secs(props.shutdown_timeout_seconds);
            for handle in handles {
                // A task in the middle of a run gets the budget to finish;
                // one merely sleeping returns immediately via the select
                // above. Aborting past the deadline rather than waiting
                // forever, so a wedged task cannot block shutdown.
                match tokio::time::timeout(budget, handle).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) if e.is_cancelled() => {}
                    Ok(Err(e)) => tracing::error!(error = %e, "scheduled task panicked"),
                    Err(_) => tracing::warn!(
                        timeout_seconds = props.shutdown_timeout_seconds,
                        "a scheduled task did not finish within the shutdown budget; abandoning it"
                    ),
                }
            }
            tracing::info!("scheduler stopped");
            Ok(())
        })
    })
    .build()
}
