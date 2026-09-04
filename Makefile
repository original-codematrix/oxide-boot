# Local CI entry point. Requires: rustup (stable), Docker (container tests),
# `cargo install cargo-audit cargo-deny --locked`.

.PHONY: fmt clippy test check audit ci msrv smoke clean clean-debug loadtest loadtest-showcase benchmark capacity saturation floor stack stack-down

fmt:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

check: fmt clippy test

# The fast check, and the only one that needs no Docker: every crate's unit
# tests plus a full application boot serving real requests. Nine test files in
# the suite need a running daemon, so this is what a contributor without one
# can still run.
smoke:
	cargo test --workspace --lib
	cargo test -p oxide-smoke

audit:
	cargo audit
	cargo deny check

ci: check audit

msrv:
	cargo +1.88 check --workspace --all-targets

# `target/` is the only thing in this repo that grows without bound. A full
# test build plus release binaries sits around 15 GB with the size-bounded
# debug profiles; it was 61 GB without them.
clean:
	cargo clean

# Drops the ~11 GB of debug artefacts but keeps the release binaries the
# benchmark and the load-test scripts run, so cleaning up does not cost a
# ten-minute rebuild before the next run.
clean-debug:
	cargo clean --profile dev

# Release build of the rest-api example plus a k6 run against it.
loadtest:
	./scripts/loadtest.sh

# The showcase example (web + Mongo + Redis + Kafka) under k6, with the local
# Prometheus/Grafana stack. Local docker compose only -- nothing here touches
# Kubernetes.
loadtest-showcase:
	./scripts/loadtest-showcase.sh

# Phased benchmark: seed a Kafka backlog and a Mongo corpus, measure consumer
# drain rate in isolation, then run k6 over the REST surface on top of it.
benchmark:
	./scripts/benchmark.sh

# HTTP alone, Kafka alone, and both at once -- each with its own CPU and memory
# cost, so the isolated and combined figures are comparable.
capacity:
	./scripts/capacity.sh

# Ramps concurrency until throughput stops rising, so the reported numbers are
# a limit rather than whatever the load generator happened to ask for.
saturation:
	./scripts/saturation.sh

# The framework's HTTP ceiling with no backing services in the path. Read every
# other number against this one.
floor:
	./scripts/floor.sh

stack:
	docker compose -f deploy/docker-compose.yml up -d

stack-down:
	docker compose -f deploy/docker-compose.yml down -v
