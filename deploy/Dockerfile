# Builds the `showcase` example for the load-test stack.
#
# The point is not distribution, it is measurement: with the service inside the
# compose network, its Redis/Mongo/Kafka traffic stops crossing Docker
# Desktop's host-to-VM boundary, which is what capped the combined benchmark on
# macOS (README has the numbers). Same binary, same config -- only the network
# path changes.

FROM rust:1.98-bookworm AS build

# rdkafka compiles librdkafka from source; these are its build inputs.
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake libssl-dev libsasl2-dev pkg-config zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Manifests first, then a throwaway build of just the dependencies: this layer
# is cached and only invalidated when Cargo.toml/Cargo.lock change, so editing
# the example does not rebuild librdkafka and the whole dependency graph.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates crates
COPY examples examples
# The workspace lists `tests/smoke` as a member, so cargo refuses to read the
# manifest without it -- even though nothing here builds it.
COPY tests tests
RUN cargo build --release -p showcase --bin showcase --bin seed \
    # Strip here rather than in the release profile. The workspace keeps
    # line tables in release so `perf`/`samply` can resolve a flamegraph on
    # the host -- but on Linux that debug info stays *inside* the binary
    # (macOS puts it in a separate .dSYM), which made the showcase binary
    # 130 MB in the image against 23 MB on the host. A container image is
    # not where anyone profiles, so it ships stripped.
    && strip /src/target/release/showcase /src/target/release/seed

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 libsasl2-2 curl \
    && rm -rf /var/lib/apt/lists/*

# Unprivileged: nothing here needs root, and the health check does not either.
RUN useradd --system --uid 10001 --create-home oxide
USER oxide
WORKDIR /app

COPY --from=build /src/target/release/showcase /usr/local/bin/showcase
COPY --from=build /src/target/release/seed /usr/local/bin/seed
# `application.yml` is resolved relative to the working directory.
COPY examples/showcase/application.yml /app/application.yml

EXPOSE 8080 8081
HEALTHCHECK --interval=5s --timeout=3s --retries=20 --start-period=10s \
    CMD curl -fsS http://127.0.0.1:8081/actuator/health || exit 1

CMD ["showcase"]
