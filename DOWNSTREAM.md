# bigFin downstream

This fork keeps T3 Code close to `pingdotgg/t3code` while adding support for
discovering and resuming existing Codex CLI sessions.

## Codex CLI session bridge

The server scans each enabled Codex provider's shared `CODEX_HOME` immediately
at startup and every 60 seconds. It asks `codex app-server` for non-archived CLI
threads, imports recognizable user and assistant transcript items into T3, and
stores the original Codex thread ID as the provider resume cursor.

This supports the intended split deployment:

- run the desktop client on a local workstation;
- run `t3 serve` beside Codex on a remote or replaceable host;
- reconnect the desktop client after the remote host returns;
- continue a discovered Codex CLI thread from T3 without starting a new Codex
  conversation.

Discovery is receipt-idempotent. Repeated scans reuse project, thread, message,
and command identities, while changed transcript content receives a new command
receipt and refreshes the projection. The importer records Codex's upstream
thread timestamp so unchanged sessions are skipped without replaying their
entire transcript every minute.

## Nix outputs

The flake supports `x86_64-linux` and `aarch64-linux`.

- `packages.<system>.t3code` and `packages.<system>.default`: wrapped desktop
  application plus the `t3` server command;
- `packages.<system>.t3code-unwrapped`: package without optional provider tools
  added to `PATH`;
- `overlays.default`: `t3code-bigfin` and `t3code-bigfin-unwrapped`.

The wrapped package also accepts appearance overrides:

- `customCssPath`: an absolute path, relative path, or `~/...` path to a
  desktop-only stylesheet loaded after the built-in app styles;
- `transparentWindow`: creates the Electron main window with a transparent
  backing surface so custom CSS can expose the compositor background.

The equivalent direct-launch environment variables are `T3CODE_CUSTOM_CSS`
and `T3CODE_DESKTOP_TRANSPARENT_WINDOW=true`. Both are opt-in, so the upstream
appearance remains the default.

Update the fixed pnpm dependency hash after lockfile changes with:

```bash
./scripts/update-nix-pnpm-hash.sh
```

Then run the focused tests and typechecks documented in `AGENTS.md`, followed
by:

```bash
nix flake check
nix build --no-link .#t3code
```

## Upstream maintenance

`.github/workflows/upstream-sync.yml` periodically creates
`automation/upstream-main` from downstream `main`, merges upstream `main` with
`--no-ff`, refreshes the Nix dependency hash, validates the downstream feature,
and opens a pull request.

**Merge upstream synchronization pull requests with a merge commit. Do not
squash or rebase them.** Preserving the upstream merge commit keeps ancestry
explicit and makes subsequent merges predictable.

Conflicts are reported in one reusable GitHub issue rather than creating a new
issue on every scheduled attempt.
