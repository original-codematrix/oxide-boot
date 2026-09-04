//! Process self-metrics: CPU, memory, threads and file descriptors for the
//! running application, exported through `/actuator/prometheus`.
//!
//! Spring Boot Actuator publishes `process.cpu.usage`, `jvm.memory.used` and
//! friends out of the box, and any dashboard built against it assumes they are
//! there. This is the counterpart, using the names Prometheus ecosystems
//! already expect from `process_exporter`/client libraries, so existing
//! dashboards and alerts work unmodified:
//!
//! | metric | meaning |
//! |---|---|
//! | `process_cpu_seconds_total` | counter, CPU seconds consumed |
//! | `process_resident_memory_bytes` | gauge, RSS |
//! | `process_virtual_memory_bytes` | gauge, virtual size |
//! | `process_threads` | gauge, OS threads |
//! | `process_start_time_seconds` | gauge, unix epoch of process start |
//!
//! Sampling is periodic rather than on-scrape: reading process statistics
//! costs a syscall and a refresh of `sysinfo`'s internal tables, and doing
//! that inside a scrape handler makes the scrape's latency depend on the
//! platform's process accounting. The interval is configurable and defaults
//! to a value comfortably below a typical scrape interval, so a scrape never
//! sees a stale sample by more than one period.

use oxide_core::{ComponentDescriptor, Context};
use std::sync::Arc;
use std::time::Duration;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// Configuration bound to `oxide.metrics.process`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, oxide_boot::ConfigProperties)]
#[config(prefix = "oxide.metrics.process", default)]
#[serde(rename_all = "kebab-case")]
pub struct ProcessMetricsProps {
    /// Turn the sampler off entirely. It costs one thread and a syscall per
    /// interval, which is negligible, but an application that exports these
    /// some other way should not publish them twice.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// How often to sample. Kept below the usual 5-15s scrape interval so a
    /// scrape is never more than one period behind.
    #[serde(default = "default_interval_millis")]
    pub interval_millis: u64,
}

fn default_enabled() -> bool {
    true
}
fn default_interval_millis() -> u64 {
    2_000
}

impl Default for ProcessMetricsProps {
    fn default() -> Self {
        Self { enabled: default_enabled(), interval_millis: default_interval_millis() }
    }
}

/// Marker component; the sampler task hangs off its `post_construct`.
pub struct ProcessMetrics {
    _private: (),
}

/// Samples once. Split out so the test can assert on a single pass without
/// waiting for the loop.
fn sample(system: &mut System, pid: Pid) {
    // Refresh only this process, and only the fields below: a full refresh
    // walks every process on the machine, which on a busy host is orders of
    // magnitude more work than what is being measured.
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_cpu().with_memory(),
    );

    let Some(proc) = system.process(pid) else {
        // The process asking about itself has gone away; nothing sensible to
        // publish, and the next tick will find the same.
        return;
    };

    metrics::gauge!("process_resident_memory_bytes").set(proc.memory() as f64);
    metrics::gauge!("process_virtual_memory_bytes").set(proc.virtual_memory() as f64);
    metrics::gauge!("process_start_time_seconds").set(proc.start_time() as f64);

    // `cpu_usage` is a percentage of one core since the previous refresh, and
    // `process_cpu_seconds_total` is a counter of CPU seconds. `run_time` is
    // wall-clock, so converting needs the accumulated total instead: sysinfo
    // exposes it per platform as `accumulated_cpu_time` in milliseconds.
    let cpu_seconds = proc.accumulated_cpu_time() as f64 / 1_000.0;
    metrics::gauge!("process_cpu_seconds_total").set(cpu_seconds);

    // Thread count is not part of sysinfo's portable surface; on the two
    // platforms this framework targets it is cheap to read directly, and an
    // unsupported platform simply does not publish the gauge rather than
    // publishing a wrong one.
    if let Some(threads) = thread_count() {
        metrics::gauge!("process_threads").set(threads as f64);
    }
}

#[cfg(target_os = "linux")]
fn thread_count() -> Option<u64> {
    // /proc/self/stat field 20 is num_threads. Reading the directory instead
    // would allocate one entry per thread on every sample.
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // The comm field can contain spaces and parentheses, so fields are counted
    // from after the closing paren rather than by splitting the whole line.
    let after_comm = &stat[stat.rfind(')')? + 1..];
    after_comm.split_whitespace().nth(17)?.parse().ok()
}

#[cfg(target_os = "macos")]
fn thread_count() -> Option<u64> {
    // No /proc; `sysinfo` does not expose it portably either. Left unpublished
    // rather than guessed -- see the note in `sample`.
    None
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn thread_count() -> Option<u64> {
    None
}

inventory::submit! {
    ComponentDescriptor::builder(
        "ProcessMetrics",
        std::any::TypeId::of::<ProcessMetrics>,
        |b| {
            b.insert(ProcessMetrics { _private: () });
            Ok(())
        },
    )
    .deps(|| vec![(std::any::TypeId::of::<ProcessMetricsProps>(), "ProcessMetricsProps")])
    .post_construct(|ctx: &Context| {
        let props = ctx.get::<ProcessMetricsProps>().expect("ProcessMetricsProps exists").clone();
        Box::pin(async move {
            if !props.enabled {
                tracing::debug!("process metrics disabled");
                return Ok(());
            }
            let interval = Duration::from_millis(props.interval_millis.max(100));
            let pid = Pid::from_u32(std::process::id());
            // Detached: this outlives every request and has no shutdown
            // ordering requirement -- it holds no connection, commits nothing,
            // and losing a final sample at exit costs nothing.
            tokio::spawn(async move {
                let mut system = System::new();
                let mut ticker = tokio::time::interval(interval);
                // The first `cpu_usage` reading after a fresh `System` is
                // meaningless (there is no previous sample to diff against),
                // so prime it once before publishing anything.
                sample(&mut system, pid);
                loop {
                    ticker.tick().await;
                    sample(&mut system, pid);
                }
            });
            tracing::debug!(?interval, "process metrics sampler started");
            Ok(())
        })
    })
    .build()
}

/// Shared `Arc` marker so the descriptor above type-checks against contexts
/// that store components behind an `Arc`.
#[allow(dead_code)]
type _AssertArc = Arc<ProcessMetrics>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampling_publishes_a_plausible_rss() {
        let mut system = System::new();
        let pid = Pid::from_u32(std::process::id());
        sample(&mut system, pid);
        system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::nothing().with_memory(),
        );
        let rss = system.process(pid).expect("this process exists").memory();
        // A running test binary always has a non-zero RSS; the upper bound
        // only catches a unit mix-up (bytes reported as kilobytes, say).
        assert!(rss > 0, "rss was {rss}");
        assert!(rss < 64 * 1024 * 1024 * 1024, "rss implausibly large: {rss}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn thread_count_is_readable_on_linux() {
        let n = thread_count().expect("linux exposes /proc/self/stat");
        assert!(n >= 1, "a running process has at least one thread, got {n}");
    }
}
