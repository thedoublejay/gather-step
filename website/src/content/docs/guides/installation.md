---
title: Install Gather Step (Rust CLI)
description: Install Gather Step on macOS with Homebrew, or build it from a repo checkout. Prerequisites, verification, and clean state.
---

Gather Step ships as a Rust CLI binary. Pick the install path that matches how
you want to work:

- **macOS (recommended):** install via Homebrew.
- **Source build:** compile directly from a repo checkout with `cargo`.

Both paths produce the same `gather-step` binary. Once it is on your `PATH`,
every example in the rest of the documentation works identically.

## Install via Homebrew (macOS)

macOS users can install Gather Step directly from the official Homebrew tap:

```bash
brew install thedoublejay/tap/gather-step
```

The release workflow publishes prebuilt macOS artifacts through Homebrew. No
Rust toolchain is required on the host machine.

### Verify the install

```bash
gather-step --version
gather-step --help
```

### Upgrade

```bash
brew update
brew upgrade gather-step
```

### Uninstall

```bash
brew uninstall gather-step
brew untap thedoublejay/tap
```

### Tap availability

The Homebrew tap is published at
[`thedoublejay/homebrew-tap`](https://github.com/thedoublejay/homebrew-tap).
If a release is not visible yet, check the
[Releases page](https://github.com/thedoublejay/gather-step/releases) and use
the source build below until the tag is published.

### Platform scope

The planned tap targets macOS only.

## Build from Source

Source builds are the right path when you want to run unreleased code from the
`main` branch or work directly from a local checkout.

### Prerequisites

- **Rust `1.94.1`** — the exact version pinned in `gather-step/rust-toolchain.toml`.
  `rustup` will install and switch to it automatically when you run any Cargo
  command inside the workspace directory. If you do not have `rustup`, install
  it from [rustup.rs](https://rustup.rs) before continuing.
- **A checked-out copy of the repo** — you need the full source tree.
- **A workspace root** where Gather Step can create `.gather-step/` for
  generated state. This can be any directory; it does not have to be inside
  the source repo.

### Build

Navigate to the `gather-step/` directory inside the repo, then run:

```bash
# debug build — fast to compile, slower to run
cargo build -p gather-step

# release build — optimized binary, suitable for day-to-day use
cargo build -p gather-step --release
```

The compiled binaries land at:

- debug: `target/debug/gather-step`
- release: `target/release/gather-step`

For any workflow that involves a large workspace or a long watch session, use
the release build. Indexing a multi-repo workspace is noticeably faster with
optimizations enabled.

### Put the binary on your PATH

So that every later example can use the bare `gather-step` command:

```bash
export PATH="$PWD/target/release:$PATH"
```

Add this line to your shell profile (`~/.zshrc`, `~/.bashrc`, or equivalent)
to make it permanent, or copy the binary to a directory already on your
`PATH`:

```bash
cp target/release/gather-step /usr/local/bin/
```

## Verify the Build

Regardless of how you installed it, confirm the binary works:

```bash
gather-step --help
gather-step serve --help
```

You should see the top-level command list and the help output for `serve`.

## Run the Full Validation Suite (contributors)

If you are working on Gather Step itself, the `just ready` recipe runs every
quality gate that CI runs:

```bash
just ready
```

In order, it executes:

1. `typos` — spell-checks source and docs
2. `cargo fmt --all --check` — verifies formatting
3. `cargo clippy --all-targets --all-features -- -D warnings` — linting
4. `cargo nextest run --all-features` — full test suite via nextest
5. `cargo deny check` — dependency audit (licenses, advisories, duplicates)
6. `cargo shear` — checks for unused dependencies

All steps must pass cleanly before the build is considered ready. If `just` is
not installed, each step can be run individually with the Cargo commands
above.

## Release Artifacts

The release pipeline publishes pre-built binaries for two Apple Darwin targets:

- `aarch64-apple-darwin` (Apple Silicon)
- `x86_64-apple-darwin` (Intel Mac)

These are the binaries the Homebrew tap serves. If you are working outside that
path, use the source build flow above.

## Uninstall / Clean State

Gather Step stores all generated state inside the workspace directory, under
`.gather-step/`. Source repositories are never modified.

To remove generated index state without deleting the binary:

```bash
gather-step --workspace /path/to/workspace clean --yes
```

This deletes the registry, graph, search index, and metadata database under
`.gather-step/`. It does not remove the binary itself. To rebuild the index
from scratch after cleaning, run `gather-step index` again.

To remove the binary:

- **Homebrew users:** `brew uninstall gather-step && brew untap thedoublejay/tap`
- **Source-build users:** delete the compiled output from `target/` or remove
  the directory from your `PATH`.

## Next Steps

- [Getting started](/guides/getting-started/) — build once and run the
  five-minute quickstart.
- [Workspace setup](/guides/workspace-setup/) — configure your repos and
  indexing scope.
