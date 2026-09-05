# Server updates

A [stable launcher](../../apps/server/src/serviceLauncher.ts) owns the runtime
selected by systemd or launchd. It is the only runtime writer of durable service
state. Server children request updates over inherited IPC; they never rewrite
their service definition or select their own replacement. Local service commands
may replace the launcher and state while the service is stopped. Foreground CLI
processes do not self-update.

Exact-version installs keep restarts independent of npm cache eviction or a moving
release tag. Installation and preflight happen in staging before publishing an
immutable runtime. Preflight checks the launcher protocol because a target that
needs new rollback guarantees cannot safely run under an older launcher. Upgrading
that launcher requires a local service update.

The client offers this path only when the connected server is older, targeting the client's exact
version. If the client is older, the user is directed to update the client instead. This
upgrade-only direction is client policy; the server RPC validates an exact package version and
migration compatibility rather than comparing semantic-version ordering.

## Client Selection

The client compares `ExecutionEnvironmentDescriptor.environment.serverVersion` with `APP_VERSION`.

| Condition                                           | Client behavior                                                   |
| --------------------------------------------------- | ----------------------------------------------------------------- |
| Client is older                                     | Update and relaunch this client; do not offer a server downgrade. |
| Versions cannot be ordered as semantic versions     | Show neutral version-sync guidance.                               |
| Older server advertises `boot-service`              | Call the launcher-backed remote update RPC.                       |
| Older server advertises `desktop-managed`           | Update the desktop app on the server machine.                     |
| Older server has no update capability               | Offer the exact manual relaunch command.                          |
| Older server advertises legacy `respawn` capability | Use the RPC path supported by that older server.                  |

The conversation banner and **Settings** → **Connections** use the same environment-keyed operation
state, so navigation does not cancel an update.

## Commit boundary

The launcher durably records the pending update before acknowledging it, then
stops the old child and starts the target as a trial. Service-state writes use
same-directory replacement with file and directory fsync. Invalid state stops
startup rather than guessing which runtime to boot.

The trial must finish migrations, acquire dependencies, bind HTTP, and park every
long-running root at the activation gate before reporting `prepared`. The launcher
then commits the target version durably and replies `committed`. Only then may the
child release its gates, accept commands, and publish ready. Keep fallible startup
acquisitions before this boundary. A listener alone does not prove the runtime is
ready to commit.

A failed or timed-out trial returns to the old version. After commit, the target
is authoritative and the service manager's ordinary restart policy applies.

## Database rollback

After the old child exits, the launcher snapshots SQLite's main file, WAL, and
shared-memory file. This makes trial migrations reversible without down
migrations. The snapshot is made once per update and survives launcher restarts;
replacing it during a retry could capture changes from the failed trial.

Rollback stops the trial before restoring. A durable restore marker makes an
interrupted restore finish before either version boots. Keep the snapshot until
commit, or until both restoration and the terminal rollback state are durable.
Attachments and other files outside SQLite are outside this rollback boundary.

## Client acknowledgement

An accepted update is still pending. Clients correlate the launcher's update ID
with the ready event after reconnecting, then check the outcome and target version.
A reconnect alone cannot distinguish successful replacement from rollback. Older
servers without an update ID retain version-only correlation.

Post-commit trial activation does not call service `start`, `initialize`, `connect`, `load`, or
`acquire` operations. It only opens prepared gates and publishes prepared lifecycle state.

The launcher serializes child exits, IPC messages, and timers. A trial must report prepared within
120 seconds. If the trial exits or times out before prepared, the launcher stops it, restores the
snapshot, records rollback, and starts A. A durable restore marker makes an interrupted restore
resume before either version can boot. After commit, B is active and the service manager's normal
restart policy applies.

## Database Rollback

The launcher snapshots `state.sqlite`, `state.sqlite-wal`, and `state.sqlite-shm` after the old
server stops and before the trial starts. This makes trial migrations and writes reversible without
requiring down migrations. The snapshot is retained across launcher restarts and is removed only
after commit or after both restore and the terminal rollback state are durable.

The protocol version is part of the safety boundary. A target that requires database snapshots is
blocked when the installed launcher is too old. Upgrade the launcher once with:

```sh
npx t3@<version> service update
```

The local command stops the unit, selects the new launcher and exact runtime, then restarts the
service. Later releases, including releases with migrations, can use the remote trial path.

Snapshots briefly require enough free disk for another copy of the SQLite files. Attachments and
other files under the state directory are outside this rollback boundary.

## Client Correlation

The update acknowledgement includes the launcher-generated update ID. After reconnecting, clients
wait for a lifecycle ready event carrying that same ID. `committed` completes the operation only
when the ready server is the target version. `rolled-back` and `failed` end it immediately with the
recorded reason. Older servers without an ID retain version-only reconnect behavior.

## Capability and Compatibility

The existing additive RPC and lifecycle schemas remain compatible with older clients. New servers
advertise remote self-update only when they have valid launcher context and a live IPC channel.
Desktop-managed servers direct the user to update the desktop app. Other process shapes provide a
manual command. The old detached foreground `respawn` path no longer exists in current servers but
remains decodable for compatibility with older servers.

## Release Invariant

Every released client version must have a matching `t3@<version>` package available first. See the
[release invariant](../operations/release.md#server-self-update-release-invariant).

Desktop updates have a separate two-phase handoff because installing the app stops
its bundled backend. Preparation returns a token while the connection is alive;
the client commits that token only after receiving it. Otherwise backend shutdown
could lose the only successful RPC result. The client must then observe the
prepared version after reconnecting. If installation fails, desktop restarts the
stopped backends and replays the failure for the same token.

## Source Map

- Client version policy: `apps/web/src/versionSkew.ts`
- Capability contract: `packages/contracts/src/environment.ts`
- Update and outcome contracts: `packages/contracts/src/server.ts`
- Update preparation: `apps/server/src/cloud/selfUpdate.ts`
- Launcher and state machine: `apps/server/src/serviceLauncher.ts`
- IPC and durable state types: `apps/server/src/cloud/serviceProtocol.ts`
- Child IPC adapter: `apps/server/src/cloud/serviceLauncherClient.ts`
- Staging and preflight: `apps/server/src/cloud/pinnedRuntime.ts` and `servicePreflight.ts`
- Service installation: `apps/server/src/cloud/bootService.ts`
- Activation boundary: `apps/server/src/serverRuntimeStartup.ts` and `serverActivation.ts`
- Client outcome correlation: `packages/client-runtime/src/state/server.ts`
