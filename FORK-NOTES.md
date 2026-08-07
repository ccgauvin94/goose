# Building this fork

Notes for building `ccgauvin94/goose` against `upstream/main`. Kept as a separate file rather
than edits to existing docs so it never conflicts on rebase.

## The upstream Dockerfile cannot build `main`

It pins `rust:1.82-bookworm` while the workspace declares `rust-version = "1.94.1"`, so cargo
refuses before compiling anything. This fork bumps it to `rust:1.94-bookworm`. Upstream does
not hit this because its CI builds tagged releases rather than `main` — which is exactly the
path a fork takes. **Re-check it against `Cargo.toml`'s `rust-version` on every rebase.**

## Fast edit loop

```sh
cargo check -p goose-sdk-types -p goose --no-default-features    # ~51s
```

**Don't use `cargo check -p goose-cli` for iteration.** Its default features include
`local-inference`, which pulls `llama-cpp-sys-2` and a long native build needing cmake and
libclang. Everything this fork changes lives in `crates/goose-sdk-types/src/custom_requests.rs`
and `crates/goose/src/acp/`, neither of which needs it. The failure is loud
(`failed to run custom build command for llama-cpp-sys-2`) and unrelated to any patch.

If bindgen can't find libclang, set `LIBCLANG_PATH` to the directory holding `libclang.so`.

## The real gate is the container image

```sh
podman build --file Dockerfile --tag goose:<tag> .
```

This compiles everything, llama included, because the Dockerfile installs cmake and libclang.
It is what the deployed image runs; the `cargo check` above is only for the edit loop.

## Publishing

`.github/workflows/publish-docker.yml` builds `ghcr.io/${{ github.repository_owner }}/goose`,
so it needs no edits on a fork. It triggers on tag push. Two things bite silently:

- **Actions are disabled by default on a fork** — enable them or nothing runs.
- **The resulting GHCR package defaults to private**, and `podman pull` answers 404 rather
  than "unauthorized". Make it public, or give podman a pull secret.

Pinning a tag means `:latest` stops being meaningful and upgrades become deliberate. That is
the cost of running a fork.

## What this fork carries

| area | what |
|---|---|
| scheduler | start at process startup rather than on first ACP connect; refresh schedules from storage on client connect; honor a recipe's own `settings:` and run scheduled jobs in Auto mode; attach the recipe to the session before the run |
| ACP | `session/conversation/append` for turnless message delivery; `fs/list_directory` for browsing the agent's filesystem (allowlisted roots) |
| roam federation | merge peers' sessions into `session/list` and route session-scoped calls to the owning peer — see `crates/goose/src/acp/server/federation/` and [the roaming guide](documentation/docs/guides/roaming-agents.md) |
| build | Dockerfile rust 1.82 → 1.94 |
