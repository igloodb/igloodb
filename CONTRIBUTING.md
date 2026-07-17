# Contributing to Igloo

We welcome contributions to Igloo! Whether it's bug fixes, feature enhancements, or documentation improvements, your help is appreciated.

## Getting Started

- If you have a larger feature in mind, please open an issue first to discuss the design and approach.
- For smaller changes, feel free to submit a pull request directly.

## Development Workflow

1.  Fork the repository.
2.  Create a new branch for your changes (e.g., `feat/my-new-feature` or `fix/issue-123`).
3.  Make your changes.
4.  Ensure your code adheres to the project's style and quality standards (see below).
5.  Commit your changes with clear and descriptive commit messages.
6.  Push your branch to your fork.
7.  Open a pull request against the main Igloo repository.

## Code Style and Quality

To maintain code quality and consistency, we use Rust's standard tooling:

- **Formatting:** Code is formatted using `rustfmt`. Before submitting, please ensure your code is formatted by running `cargo fmt --all`.
- **Linting:** We use `clippy` for linting. Please ensure your code is free of clippy warnings by running `cargo clippy --all-targets --all-features -- -D warnings`.
- **Dependencies:** `cargo deny` checks licenses and security advisories in CI (config in `deny.toml`). New dependencies should be justified in the PR description; prefer the standard ecosystem crate over rolling our own.

Our Continuous Integration (CI) pipeline automatically checks for formatting and linting issues. Pull requests that do not pass these checks will not be merged.

## Testing Requirements

Tests are required, not optional:

- **Every behavior change needs a test** that fails without the change and passes with it. Test behavior (inputs → outputs), not implementation details.
- **Every bug fix needs a regression test** reproducing the bug. A fix PR without one will be asked to add it before merge.
- **Unit tests** must stay hermetic — no network, no external services, no sleeps. If you need time to pass, inject a clock (see `cache_layer`'s TTL tests for the pattern).
- **Integration tests** live in `tests/` and are gated on `IGLOO_TEST_POSTGRES_URI`; plain `cargo test` skips them. Run the full suite locally with a disposable PostgreSQL:

  ```sh
  IGLOO_TEST_POSTGRES_URI=postgres://postgres:postgres@localhost:5432/igloo_test cargo test
  ```

  CI runs them against a Postgres service container on every PR.
- **Skipping a test is exceptional**: it needs an explanation in the PR and an issue tracking its re-enablement.

## Questions?

Feel free to open an issue if you have any questions or need clarification.
