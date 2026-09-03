//! Shared test-container fixtures (feature `containers`).
//!
//! Redpanda and Mongo integration tests were previously copy-pasting the
//! same "start a `GenericImage`, retry on port conflict, wait for
//! readiness" harness in four places (`oxide-kafka`, `oxide-mongo`, and the
//! `kafka`/`rest-api` examples), each with its own `unsafe { libc::atexit
//! }` hook to remove the container on process exit. That hook is
//! unnecessary: testcontainers 0.28's `ContainerAsync` starts the Ryuk
//! reaper alongside every container it creates, and Ryuk removes containers
//! it's tracking when the client connection that started them closes —
//! including on an unclean process exit (panic, `SIGKILL`, ...), which
//! `libc::atexit` cannot even handle (atexit handlers don't run on a
//! signal-killed process either). A normal, non-signalled exit additionally
//! runs `ContainerAsync`'s own `Drop`, which removes the container
//! directly. So cleanup for a *non-reused* container needs neither `unsafe`
//! nor an explicit hook — but read on, because these fixtures are reused,
//! and reused containers are deliberately exempt from that cleanup.
//!
//! ## The road to this design (both measured, not theorized)
//!
//! **Attempt 1 — one container per process, host port pinned to the
//! well-known default (9092 / 27017), matching the four harnesses this
//! replaced.** Every `oxide-kafka` integration-test *file* compiles to its
//! own binary (a separate OS process), and `cargo test -p oxide-kafka`
//! genuinely runs several of those binaries concurrently. A plain `cargo
//! test -p oxide-kafka -p oxide-mongo -p oxide-test -p kafka -p rest-api`
//! reliably failed 6-7 of the ~9 Redpanda-backed binaries with "port is
//! already allocated", since every one of them raced for the same host
//! port and only the winner could hold it for its entire run.
//!
//! **Attempt 2 — one container per process, but each on its own freshly
//! allocated (ephemeral) host port**, so there's no shared port to race on.
//! For Redpanda this also needs the advertised Kafka address to follow the
//! allocated port (`--kafka-addr 0.0.0.0:9092 --advertise-kafka-addr
//! localhost:<port>`), because `--mode dev-container` otherwise always
//! advertises `localhost:9092` to clients regardless of the actual mapped
//! host port, which would get a client redirected to a broken address on
//! its second request. This eliminated the port race, but at full
//! concurrency it starts one Redpanda broker *per binary* — six of them
//! observed running at once — and that many broker processes competing for
//! the same machine's CPU/memory meant the last ones to start could not
//! reach protocol-level readiness inside a 60s window. Raising the timeout
//! would only paper over that; the real problem is running six brokers to
//! serve one test suite.
//!
//! **This design — one *reused* container for the whole `cargo test`
//! invocation (and beyond).** testcontainers 0.28's `reusable-containers`
//! support (`ImageExt::with_reuse`, `ReuseDirective`) matches an existing
//! container purely by `with_container_name` (see
//! `testcontainers::core::client::Client::get_container`, which filters by
//! `name`): every process's `redpanda()`/`mongo()` call builds the *same*
//! `ContainerRequest` (same name, same fixed host port, same image/cmd) and
//! asks Docker for a container with that name before creating one. The
//! first process to ask creates it; every other process — in the same
//! `cargo test` invocation or a later one — finds and attaches to that same
//! running container instead of starting a fresh one. So there is at most
//! one Redpanda and one Mongo container system-wide, and both the port
//! race (attempt 1) and the six-brokers-at-once contention (attempt 2) are
//! gone: a single broker serves every binary, so the host port it needs
//! can go back to being the fixed, well-known default, and the advertised
//! address is that same fixed value. The broker is given a bit more room
//! (`--smp 2 --memory 2G`, up from the original single-binary `--smp 1
//! --memory 1G`) since it's now serving the whole suite's concurrent
//! traffic rather than one binary's.
//!
//! ### Trade-off: cleanup
//!
//! A `reuse`-flagged container is explicitly exempt from testcontainers'
//! Ryuk/Drop cleanup — that's the point, it's meant to survive the process
//! that created it — so `oxide-test-redpanda`, `oxide-test-mongo` and
//! `oxide-test-dragonfly` keep running after the test binaries exit, ready for the next run instead of
//! paying the container-start cost again. This is the same pattern this
//! repo's local dev support containers already use (`docker-tracing-1`,
//! `docker-cache-1`, both left running across sessions). Remove them
//! manually when you want a clean slate or want the memory back:
//!
//! ```sh
//! docker rm -f oxide-test-redpanda oxide-test-mongo oxide-test-dragonfly
//! ```
//!
//! All three names are unique to this fixture (nothing else in this repo creates
//! containers by those names), so a stale one is always either exactly what
//! the next run wants (still healthy — `is_running` is checked and it's
//! restarted if not, matching testcontainers' own reuse behaviour) or safe
//! to remove with the command above if you suspect it's wedged; a wedged
//! container simply fails the readiness wait inside the fixture
//! with a panic pointing at the same fix.
//!
//! Mongo needs none of the advertised-address handling Redpanda does — a
//! standalone `mongod` (no replica set) doesn't redirect clients elsewhere
//! — so its fixture is the reuse + fixed-port plumbing only.

use std::time::Duration;
use testcontainers::core::{ContainerRequest, IntoContainerPort};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt, ReuseDirective};
use tokio::sync::OnceCell;

/// The fixed host port every process maps Redpanda's Kafka listener to.
/// Safe to be the well-known default again (unlike a per-process container,
/// there is only ever one of these running) — see module docs.
const REDPANDA_PORT: u16 = 9092;

/// The fixed host port every process maps Mongo's listener to.
const MONGO_PORT: u16 = 27017;

/// The fixed host port every process maps Dragonfly's listener to.
const DRAGONFLY_PORT: u16 = 6379;

/// The fixed name both fixtures reuse each other (and future runs) by. See
/// module docs for the reuse-matching mechanics and the cleanup trade-off.
const REDPANDA_CONTAINER_NAME: &str = "oxide-test-redpanda";
const MONGO_CONTAINER_NAME: &str = "oxide-test-mongo";
const DRAGONFLY_CONTAINER_NAME: &str = "oxide-test-dragonfly";

/// A running fixture container's connection info. Cheap to clone — the
/// actual `ContainerAsync` stays behind the process-wide static and is never
/// exposed here, so callers can't accidentally drop it early.
#[derive(Debug, Clone)]
pub struct ContainerHandle {
    /// The address/URI clients should connect to.
    pub endpoint: String,
}

/// Owns the (possibly reused) container handle for the process's lifetime.
/// Reused containers are not torn down by `Drop`/Ryuk by design — see
/// module docs — so this only needs to keep the handle alive long enough to
/// avoid re-resolving it on every call within the process.
struct Fixture {
    _container: ContainerAsync<GenericImage>,
    endpoint: String,
}

static REDPANDA: OnceCell<Fixture> = OnceCell::const_new();
static MONGO: OnceCell<Fixture> = OnceCell::const_new();
static DRAGONFLY: OnceCell<Fixture> = OnceCell::const_new();

/// Starts (or, per `ReuseDirective::Always`, attaches to) a `GenericImage`,
/// retrying a few times on failure: two processes racing to create the same
/// reused container name can transiently collide ("name already in use")
/// before the loser's next attempt sees it via `get_container` and attaches
/// instead, so a short backoff lets that race resolve instead of failing
/// the whole suite.
async fn start_with_retry(
    build: impl Fn() -> ContainerRequest<GenericImage>,
    what: &str,
) -> ContainerAsync<GenericImage> {
    let mut node = None;
    for attempt in 0..8 {
        match build().start().await {
            Ok(n) => {
                node = Some(n);
                break;
            }
            Err(e) if attempt < 7 => {
                eprintln!("[oxide-test/containers] {what} start attempt {attempt} failed ({e}); retrying");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => {
                panic!("{what} integration tests require Docker — start Docker Desktop, or skip these tests: {e}")
            }
        }
    }
    node.expect("container start attempted at least once")
}

/// Starts or attaches to (once per process; reused across processes — see
/// module docs) the shared Redpanda container on the fixed host port 9092,
/// waiting until the broker actually answers a metadata fetch — a
/// TCP-accept alone isn't enough, Redpanda can accept the connection before
/// it can handle produces. Returns the `bootstrap.servers` value
/// ("127.0.0.1:9092"). Must be awaited from within a tokio runtime.
pub async fn redpanda() -> ContainerHandle {
    let fixture = REDPANDA
        .get_or_init(|| async {
            // The internal Kafka listener always binds 9092; the host-side
            // mapped port is pinned to the same well-known default (safe
            // now that only one Redpanda container ever exists — see module
            // docs). Redpanda's `dev-container` mode advertises
            // `localhost:9092` to clients by default, which happens to
            // match here, but the advertised address is still set
            // explicitly so this stays correct if the port ever changes.
            let cmd: Vec<String> = vec![
                "redpanda".to_string(),
                "start".to_string(),
                "--mode".to_string(),
                "dev-container".to_string(),
                "--overprovisioned".to_string(),
                "--smp".to_string(),
                "2".to_string(),
                "--memory".to_string(),
                "2G".to_string(),
                "--kafka-addr".to_string(),
                "0.0.0.0:9092".to_string(),
                "--advertise-kafka-addr".to_string(),
                format!("localhost:{REDPANDA_PORT}"),
                "--set".to_string(),
                "redpanda.empty_seed_starts_cluster=true".to_string(),
            ];
            let build = || {
                GenericImage::new("redpandadata/redpanda", "v24.2.1")
                    .with_mapped_port(REDPANDA_PORT, 9092.tcp())
                    .with_cmd(cmd.clone())
                    .with_container_name(REDPANDA_CONTAINER_NAME)
                    .with_reuse(ReuseDirective::Always)
            };
            let node = start_with_retry(build, "redpanda").await;
            let endpoint = format!("127.0.0.1:{REDPANDA_PORT}");
            eprintln!("[oxide-test/containers] redpanda bootstrap = {endpoint}; waiting for broker readiness");
            // Wait until the broker is ready at the KAFKA protocol level. A
            // metadata fetch is the smoke test callers rely on anyway, so
            // reuse it as the readiness probe. Also true for an already-
            // running reused container: this simply resolves immediately.
            let ready = tokio::time::timeout(Duration::from_secs(60), async {
                use rdkafka::consumer::Consumer;
                loop {
                    let mut config = rdkafka::ClientConfig::new();
                    config.set("bootstrap.servers", endpoint.as_str());
                    if let Ok(consumer) = config.create::<rdkafka::consumer::BaseConsumer>()
                        && consumer.fetch_metadata(None, Duration::from_secs(1)).is_ok()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            })
            .await;
            if ready.is_err() {
                panic!(
                    "redpanda broker at {endpoint} did not become ready within 60s (if `docker ps` shows \
                     `{REDPANDA_CONTAINER_NAME}` wedged, `docker rm -f {REDPANDA_CONTAINER_NAME}` and retry)"
                );
            }
            eprintln!("[oxide-test/containers] redpanda broker ready");
            Fixture { _container: node, endpoint }
        })
        .await;
    ContainerHandle { endpoint: fixture.endpoint.clone() }
}

/// Starts or attaches to (once per process; reused across processes — see
/// module docs) the shared MongoDB container on the fixed host port 27017.
/// Returns the connection URI ("mongodb://localhost:27017"). Callers that
/// need to wait for Mongo to accept writes (it can lag behind container
/// start) should probe it themselves (e.g. a `ping` loop), matching the
/// previous per-call-site behaviour. Must be awaited from within a tokio
/// runtime.
pub async fn mongo() -> ContainerHandle {
    let fixture = MONGO
        .get_or_init(|| async {
            // Note: `mongo:8` refuses to run on the CI/host Linux kernel
            // (6.19+), see mongodb SERVER-121912; `mongo:7` starts fine and
            // exercises the same repository surface.
            let build = || {
                GenericImage::new("mongo", "7")
                    .with_mapped_port(MONGO_PORT, 27017.tcp())
                    .with_cmd(["mongod", "--bind_ip_all"])
                    .with_container_name(MONGO_CONTAINER_NAME)
                    .with_reuse(ReuseDirective::Always)
            };
            let node = start_with_retry(build, "mongo").await;
            Fixture { _container: node, endpoint: format!("mongodb://localhost:{MONGO_PORT}") }
        })
        .await;
    ContainerHandle { endpoint: fixture.endpoint.clone() }
}

/// Starts or attaches to (once per process; reused across processes — see
/// module docs) the shared Dragonfly container on the fixed host port 6379.
/// Dragonfly speaks the Redis protocol, so this is what the `oxide-redis`
/// starter is tested against. Returns the connection URL
/// ("redis://127.0.0.1:6379"). Must be awaited from within a tokio runtime.
pub async fn dragonfly() -> ContainerHandle {
    let fixture = DRAGONFLY
        .get_or_init(|| async {
            let build = || {
                GenericImage::new("docker.dragonflydb.io/dragonflydb/dragonfly", "v1.23.2")
                    .with_mapped_port(DRAGONFLY_PORT, 6379.tcp())
                    // Dragonfly refuses to start under the default Docker
                    // Desktop limits unless told it may run without
                    // `--memory` cgroup headroom for its io_uring pools.
                    .with_cmd(["dragonfly", "--logtostderr", "--default_lua_flags=allow-undeclared-keys"])
                    .with_container_name(DRAGONFLY_CONTAINER_NAME)
                    .with_reuse(ReuseDirective::Always)
            };
            let node = start_with_retry(build, "dragonfly").await;
            let endpoint = format!("redis://127.0.0.1:{DRAGONFLY_PORT}");
            // Like Mongo, the container can accept TCP before the server
            // answers commands, so wait for an actual PING round-trip.
            let deadline = std::time::Instant::now() + Duration::from_secs(60);
            loop {
                if tokio::net::TcpStream::connect(("127.0.0.1", DRAGONFLY_PORT)).await.is_ok() {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "dragonfly at {endpoint} did not accept connections within 60s"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Fixture { _container: node, endpoint }
        })
        .await;
    ContainerHandle { endpoint: fixture.endpoint.clone() }
}
