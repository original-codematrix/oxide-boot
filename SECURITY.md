# Security policy

## Supported versions

The project is pre-1.0 and only the `main` branch receives fixes. There are
no maintained release branches yet.

## Reporting a vulnerability

Please report security issues privately, through GitHub's
["Report a vulnerability"](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
button on the Security tab, rather than by opening a public issue.

Include what you need to reproduce it: the affected crate, a minimal case,
and what an attacker gains. You will get an acknowledgement within a week.

Because this is a single-maintainer hobby project, please do not expect a
commercial response time. If you need a fix on a schedule, say so in the
report and the answer will be honest about whether that is realistic.

## Scope

The starters wrap widely used crates (axum, rdkafka, the MongoDB and Redis
drivers, rustls). A vulnerability in one of those belongs upstream — report
it there first. What is in scope here is how this framework configures and
exposes them: default settings, credential handling, what ends up in logs
and error responses, and the management endpoints under `/actuator`.

`cargo audit` and `cargo deny check` run in CI weekly against the advisory
database.
