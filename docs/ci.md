# cloudSearch CI

## Goals

The first CI setup for `cloudSearch` is intentionally strict and simple.

It should:

- fail fast on formatting drift
- reject lints before they become habits
- run the full Rust test suite, including restart integration coverage
- protect `main` through pull-request checks

## Current Workflow

The GitHub Actions workflow lives at:

- `.github/workflows/ci.yml`

It runs on:

- pushes to `main`
- pull requests targeting `main`

## Checks

The current CI pipeline is split into separate jobs:

- `fmt`
- `clippy`
- `unit-tests`
- `integration-tests`
- `coverage`

Commands run from `rust/`:

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p cloudsearch-api -p cloudsearch-common -p cloudsearch-index -p cloudsearch-storage`
- `cargo test -p cloudsearch-node --test node_restart`
- `cargo llvm-cov --workspace --all-targets --lcov --output-path lcov.info`

This means formatting issues, clippy warnings, failing tests, and coverage generation failures all block the PR.

## Why This Is Strict By Default

`cloudSearch` is a storage and search engine. Durability, recovery, and query correctness matter more than rapid unchecked iteration.

Strict CI helps us keep:

- predictable style
- idiomatic Rust
- confidence in WAL and recovery behavior
- confidence in the API and restart path

## Local Commands Before Opening A PR

Run these locally before pushing:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p cloudsearch-api -p cloudsearch-common -p cloudsearch-index -p cloudsearch-storage
cargo test -p cloudsearch-node --test node_restart
cargo llvm-cov --workspace --all-targets --lcov --output-path lcov.info
```

If needed, auto-format locally with:

```bash
cargo fmt --all
```

## GitHub CLI Workflow

After opening a PR, use `gh` to inspect and watch CI.

Useful commands:

```bash
gh pr status
gh pr checks <pr-number>
gh pr view <pr-number> --web
gh run list --branch <branch-name>
gh run watch <run-id>
gh run view <run-id> --log
```

Examples:

```bash
gh pr checks 5
gh run list --branch feat/my-branch
gh run watch 123456789
gh run view 123456789 --log
```

## Recommended Release Workflow

For every change:

1. implement the feature, tests, and supporting docs
2. stop and review locally
3. create a branch
4. create small commits
5. push and open a PR
6. watch CI with `gh`
7. wait for human review and merge

The assistant must never merge a PR automatically.

## Coverage

Coverage is generated with `cargo llvm-cov` and uploaded as a workflow artifact.

Current policy:

- coverage runs on every PR to `main`
- coverage is reported, but not yet enforced with a hard percentage threshold
- the artifact can be downloaded from the GitHub Actions run for deeper inspection

## Future CI Improvements

Good next steps after the current pipeline is stable:

- add `cargo audit` as a separate security job
- publish coverage summaries directly in PR comments or step summaries
- split heavier integration suites further if runtime grows
- add release workflows for tagged builds later
