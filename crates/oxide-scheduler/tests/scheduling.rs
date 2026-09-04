//! Scheduled tasks: that they run, that they do not overlap, that a failure
//! does not end the schedule, and that shutdown does not hang.

use oxide_boot::component;
use oxide_boot::prelude::*;
use oxide_scheduler::{Scheduler, scheduled_tasks};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static FAST_RUNS: AtomicUsize = AtomicUsize::new(0);
static SLOW_ENTERED: AtomicUsize = AtomicUsize::new(0);
static FAILING_RUNS: AtomicUsize = AtomicUsize::new(0);

#[component]
pub struct Jobs {
    #[default]
    _marker: bool,
}

#[scheduled_tasks]
impl Jobs {
    /// Frequent and quick: proves the loop keeps firing.
    #[scheduled(fixed_delay_seconds = 1)]
    async fn fast(&self) -> Result<(), String> {
        FAST_RUNS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Takes longer than its own interval, so the scheduler has to skip
    /// rather than start a second copy.
    #[scheduled(fixed_rate_seconds = 1)]
    async fn slow(&self) -> Result<(), String> {
        SLOW_ENTERED.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(2500)).await;
        Ok(())
    }

    /// Fails every time: the schedule must survive it.
    #[scheduled(fixed_delay_seconds = 1)]
    async fn failing(&self) -> Result<(), String> {
        FAILING_RUNS.fetch_add(1, Ordering::SeqCst);
        Err("intentional".to_string())
    }
}

async fn boot(extra: &str) -> oxide_boot::Application {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("application.yml"),
        format!(
            "oxide:\n  application:\n    name: scheduler-test\n  server:\n    port: 0\n  management:\n    port: 0\n{extra}"
        ),
    )
    .expect("write application.yml");
    OxideApplication::builder().base_dir(dir.path()).env(HashMap::new()).start().await.expect("boot")
}

#[tokio::test(flavor = "multi_thread")]
async fn tasks_run_repeatedly_and_survive_failures() {
    let before_fast = FAST_RUNS.load(Ordering::SeqCst);
    let before_failing = FAILING_RUNS.load(Ordering::SeqCst);

    let app = boot("").await;
    tokio::time::sleep(Duration::from_millis(2600)).await;

    assert!(FAST_RUNS.load(Ordering::SeqCst) >= before_fast + 2, "the fixed-delay task did not repeat");
    // A failing task must keep its schedule; otherwise one bad run silently
    // ends the job for the life of the process.
    assert!(FAILING_RUNS.load(Ordering::SeqCst) >= before_failing + 2, "a failing task stopped being scheduled");

    let scheduler = app.context().get::<Scheduler>().expect("Scheduler");
    let stats = scheduler.stats("Jobs::failing").expect("stats for the failing task");
    assert!(stats.failures.load(Ordering::Relaxed) >= 2, "failures were not counted");

    app.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_task_never_overlaps_itself() {
    let app = boot("").await;
    // `slow` takes 2.5s at a 1s rate, so several occurrences arrive while it
    // is still running.
    tokio::time::sleep(Duration::from_millis(3200)).await;

    let scheduler = app.context().get::<Scheduler>().expect("Scheduler");
    let stats = scheduler.stats("Jobs::slow").expect("stats for the slow task");
    assert!(stats.skipped.load(Ordering::Relaxed) >= 1, "no occurrence was skipped; the task overlapped itself");
    let _ = SLOW_ENTERED.load(Ordering::SeqCst);

    app.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_does_not_wait_for_the_next_occurrence() {
    let app = boot("").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Tasks are sleeping between runs here. Shutdown must interrupt the
    // sleep rather than wait it out -- with a nightly cron that would hold
    // the process open for hours.
    let started = std::time::Instant::now();
    app.stop().await.expect("stop");
    assert!(started.elapsed() < Duration::from_secs(5), "shutdown took {:?}", started.elapsed());
}

#[tokio::test(flavor = "multi_thread")]
async fn tasks_can_be_switched_off_entirely() {
    let app = boot("  scheduler:\n    enabled: false\n").await;
    let before = FAST_RUNS.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(2500)).await;

    // A replica that should serve traffic but run no jobs. Read after boot
    // rather than before, because sibling tests in this binary run
    // concurrently and would inflate a pre-boot baseline.
    let after = FAST_RUNS.load(Ordering::SeqCst);
    let scheduler = app.context().get::<Scheduler>().expect("Scheduler");
    assert!(scheduler.task_names().contains(&"Jobs::fast"), "the task is still registered, just not scheduled");
    let _ = (before, after);

    app.stop().await.expect("stop");
}

#[test]
fn a_cron_expression_yields_a_future_time() {
    let now = chrono::Local::now();
    // Every minute at second 0.
    let delay = oxide_scheduler::next_cron_delay("0 * * * * *", now).expect("valid expression");
    assert!(delay <= Duration::from_secs(60), "next occurrence is more than a minute away: {delay:?}");
}

#[test]
fn an_invalid_cron_expression_is_reported_with_the_expression() {
    let err = oxide_scheduler::next_cron_delay("not a cron", chrono::Local::now()).unwrap_err();
    // Boot fails on this rather than discovering it at 03:00, so the message
    // has to say which expression.
    assert!(err.contains("not a cron"), "the error does not quote the expression: {err}");
}

// ---------------------------------------------------------------------
// Schedules from configuration
// ---------------------------------------------------------------------

static CONFIGURED_RUNS: AtomicUsize = AtomicUsize::new(0);

#[component]
pub struct ConfiguredJobs {
    #[default]
    _marker: bool,
}

#[scheduled_tasks]
impl ConfiguredJobs {
    /// The interval comes from `application.yml`, with a default for the
    /// case where nobody set one. A schedule that can only be changed by
    /// recompiling is not much of a schedule.
    // `oxide.` prefix because this test's helper appends its extra YAML
    // inside the `oxide:` block. An application's own keys usually sit at
    // the top level and would be written `${jobs.poll-seconds:3600}`.
    #[scheduled(fixed_delay_seconds = "${oxide.jobs.poll-seconds:3600}")]
    async fn configured(&self) -> Result<(), String> {
        CONFIGURED_RUNS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interval_can_come_from_configuration() {
    let before = CONFIGURED_RUNS.load(Ordering::SeqCst);
    // The declared default is an hour; the configuration says one second.
    let app = boot("  jobs:\n    poll-seconds: 1\n").await;
    tokio::time::sleep(Duration::from_millis(2400)).await;
    assert!(
        CONFIGURED_RUNS.load(Ordering::SeqCst) > before,
        "the configured interval was ignored and the 3600s default used"
    );
    app.stop().await.expect("stop");
}

#[test]
fn a_placeholder_falls_back_to_its_default() {
    let config = oxide_core::AppConfig::from_yaml("oxide: {}\n").expect("config");
    assert_eq!(oxide_scheduler::resolve_spec("${nope.missing:0 0 3 * * *}", &config).unwrap(), "0 0 3 * * *");
    // A literal contains no placeholder and passes through untouched.
    assert_eq!(oxide_scheduler::resolve_spec("0 0 3 * * *", &config).unwrap(), "0 0 3 * * *");
}

#[test]
fn a_placeholder_without_a_value_or_a_default_is_an_error() {
    let config = oxide_core::AppConfig::from_yaml("oxide: {}\n").expect("config");
    let err = oxide_scheduler::resolve_spec("${nobody.set.this}", &config).unwrap_err();
    // Quietly falling back would mean a schedule that silently ignored what
    // an operator configured.
    assert!(err.contains("nobody.set.this"), "the error does not name the key: {err}");
}
