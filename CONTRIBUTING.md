# Contributing

Thanks for looking. This is a young project and the surface still moves, so
opening an issue before a large change will save you work.

## Running the checks

`make` is the entry point; CI runs the same targets.

```sh
make check   # fmt + clippy (warnings are errors) + the full test suite
make audit   # cargo-audit and cargo-deny
make msrv    # the declared minimum, currently 1.88
```

`make check` needs a running Docker daemon: nine test files start their own
Kafka, MongoDB and Redis containers. Without one, run

```sh
make smoke   # every crate's unit tests, plus a full application boot
```

which is the largest part of the suite that needs no daemon. Open the pull
request either way and let CI run the rest.

The toolchain is pinned in `rust-toolchain.toml`, so `rustup` will fetch the
right version on its own. `cargo install cargo-audit cargo-deny --locked` is
needed once for `make audit`.

## What a change should look like

**Explain the why in the code, not the what.** The comments in this repo
answer questions a reader would otherwise have to reconstruct — why a
timeout is 30 seconds, why an eviction is used instead of an update, which
alternative was measured and lost. Comments that restate the line above them
get removed in review.

**Say what you measured.** Performance claims belong with numbers.
`make floor` gives the framework's HTTP ceiling with no backing services in
the path; read every other number against it. `make benchmark` runs the
phased benchmark over all four integrations. If a change is meant to be
faster and the measurement says otherwise, that belongs in the commit
message too — the repo already carries several such notes, and they are more
useful than the changes that worked.

**Test the case that would break.** The interesting assertions are usually
about what must *not* happen: that a 4xx is not retried, that a dropped
acknowledgment does not commit, that a disabled circuit breaker changes
nothing.

## Commits

Conventional commits (`feat(scope):`, `fix(scope):`, `docs:`, `test:`,
`refactor:`, `build(deps):`), with a body that explains the reasoning. Look
at `git log` for the register — the subject says what changed, the body says
why it had to.

## Licence

Contributions are dual-licensed under MIT and Apache-2.0, matching the
project. By submitting a pull request you agree to license your work under
those terms.
