# Local CI entry point. Requires: rustup (stable), Docker (container tests),
# `cargo install cargo-audit cargo-deny --locked`.

.PHONY: fmt clippy test check audit ci msrv clean loadtest

fmt:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

check: fmt clippy test

audit:
	cargo audit
	cargo deny check

ci: check audit

msrv:
	cargo +1.88 check --workspace --all-targets

# `target/` is the only thing in this repo that grows without bound.
clean:
	cargo clean

# Release build of the rest-api example plus a k6 run against it.
loadtest:
	./scripts/loadtest.sh
