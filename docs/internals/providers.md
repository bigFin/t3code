# Provider architecture

> For maintainers. Using T3 Code? See [docs/user](../user/).

A provider is the agent runtime that does the actual work. T3 Code supports several, and the
orchestration layer does not know which one is behind a thread.

## Built-in drivers

[`builtInDrivers.ts`][drivers] exports `BUILT_IN_DRIVERS` with five entries:

| Driver kind   | Driver source                           |
| ------------- | --------------------------------------- |
| `codex`       | [`Drivers/CodexDriver.ts`][codex]       |
| `claudeAgent` | [`Drivers/ClaudeDriver.ts`][claude]     |
| `cursor`      | [`Drivers/CursorDriver.ts`][cursor]     |
| `grok`        | [`Drivers/GrokDriver.ts`][grok]         |
| `opencode`    | [`Drivers/OpenCodeDriver.ts`][opencode] |

Each driver declares its `driverKind`, a `configSchema`, and a `create` function that builds an
adapter in a child scope. Adapter implementations live beside them in
`apps/server/src/provider/Layers/` (`CodexAdapter.ts`, `ClaudeAdapter.ts`, and so on) and conform to
[`ProviderAdapter.ts`][adapter]. Read the driver plus its adapter to see how a specific agent's
transport, config, and event shapes are mapped.

## Registry and routing

Two registries separate configuration from live processes:

- [`ProviderInstanceRegistry`][instances] keys configured instances by `ProviderInstanceId`. Creating
  one looks up the driver by `driverKind`, decodes `entry.config` with that driver's schema, opens a
  child scope, and calls `driver.create`.
- [`ProviderAdapterRegistry`][registry] resolves an instance ID to its live adapter via
  `getByInstance`.

[`ProviderService`][service] sits on top. It combines the adapter registry with the provider session
directory to route session and turn operations for a thread, so callers name a thread, not an agent.

Adding a driver means writing the driver plus adapter and adding it to `BUILT_IN_DRIVERS`. No
orchestration, contract, or client change is required for the common case.

## How provider work is requested

Clients never call a provider directly. They dispatch orchestration commands over the RPC method
`orchestration.dispatchCommand`, defined with the rest of the orchestration surface in
[`orchestration.ts`][contracts]. The client-dispatchable provider-facing commands are
`thread.turn.start`, `thread.turn.interrupt`, `thread.approval.respond`,
`thread.user-input.respond`, `thread.checkpoint.revert`, and `thread.session.stop`, plus the mode
setters `thread.runtime-mode.set` and `thread.interaction-mode.set`.

The engine persists an event for the command, and a server-side reactor performs the provider call.
Provider output comes back as internal commands such as `thread.message.assistant.delta` and
`thread.session.set`, which clients observe through `orchestration.subscribeThread`. See
[overview.md](./overview.md) for the command/event loop.

## Server-side workers

Provider work flows through three queue-backed workers. All three are built with
`makeDrainableWorker` from [`DrainableWorker.ts`][worker] and expose `drain` for deterministic test
synchronization.

1. [`ProviderRuntimeIngestion`][ingest] consumes provider runtime streams and emits orchestration
   commands.
2. [`ProviderCommandReactor`][cmd] reacts to orchestration intent events and dispatches provider
   calls.
3. [`CheckpointReactor`][checkpoint] captures workspace checkpoints on turn start and completion, and
   performs reverts.

### Buffered assistant delivery

A thread in `buffered` assistant delivery mode accumulates assistant text instead of streaming each
delta. The buffer is not held until turn completion. In [`ProviderRuntimeIngestion`][ingest],
`MAX_BUFFERED_ASSISTANT_CHARS` is 24,000: the append that would exceed it invalidates the buffer and
spills the whole accumulated text as one delta. The buffer also flushes at interaction boundaries,
when a request opens (approval) or user input is requested, via
`flushBufferedAssistantMessagesForTurn`.

## Detached Codex execution

Codex uses a detached provider host so an agent turn is not owned by the T3 server process. A
provider host can launch one Codex app-server process and multiplex lightweight thread attachments
over a Unix socket, but neither T3 nor any provider host owns the app-server lifecycle after launch.
T3 and provider-host shutdown close their attachments without signaling Codex. An explicit user stop
still sends the provider-host stop command.

The preferred app-server socket is stable for discovery by environment and provider instance, but a
new app-server launch uses a generation-unique sibling socket. The provider-host namespace includes
the protocol and T3 build fingerprint, and every launch also receives generation-unique control and
config paths. The durable manifest, rather than any preferred runtime path, identifies the active
control and app-server endpoints.

The detached provider host itself acquires a cross-process startup lease stored beside the durable
manifest, so launchers with different runtime directories still serialize one environment identity.
While holding it, the host compares the manifest generation it was launched against with the current
manifest, revalidates the app-server socket and process provenance, starts or adopts Codex only if
that ownership remains unambiguous, publishes its new generation, and opens its control socket
before releasing the lease. The startup claim therefore survives the launching T3 process exiting,
and competing launchers cannot both spawn or publish. T3 waits for either host readiness or launcher
exit, cancels the losing readiness probe, then rediscovers whichever manifest generation won.

After a rolling update, the new T3 build can start a compatible host alongside the old host without
signaling either the old host or its Codex child. Generation-unique control endpoints also prevent a
retiring same-build host from unlinking its successor's socket. If an app-server socket remains
occupied but cannot be proven stale, recovery preserves it and starts on a unique sibling endpoint
instead of unlinking a process that may still become responsive. If the durable manifest and process
identity verify the surviving app-server, the replacement host attaches to it and adopts missing
sessions with `thread/resume`. Adoption is resume-only: it never falls back to `thread/start` and
cannot silently create a replacement conversation.

Provider hosts retire after their last T3 attachment remains absent for the idle lease. Retirement
closes only the host's app-server readers and control socket; the detached app-server and its turns
continue independently. This bounds the cost of rolling upgrades instead of accumulating one live
gateway per build.

This lifecycle independence applies to app-servers launched by this model. A legacy v1 host still has
lifecycle authority over the Codex process it launched. When its manifest, hello generation, provider
identity, and process identity agree, current T3 clients negotiate protocol v1 and attach to that
host's existing sessions using the legacy envelopes. They do not adopt or restart its Codex
app-server, and they omit v2-only attach modes and command deadlines.

There is one compatibility repair for a verified legacy session that remains inventoried but closes
every attachment before returning its snapshot. T3 may start a v2 gateway around the same verified
app-server and adopt that Codex thread with resume-only semantics. The repair does not signal,
terminate, unlink, or replace the legacy host or its child, and the legacy host remains the process
lifecycle owner. The v2 gateway replays recovered assistant output from the adopted turn so the new
attachment does not hide a response produced during the handoff. If the legacy thread is not
inventoried, its control endpoint cannot be verified, or its app-server provenance is ambiguous, T3
preserves the processes and reports the detached host as unavailable instead of guessing or creating
a replacement conversation.

Attachments request bounded event replay and then receive a fresh snapshot, so missed transcript
events land before the authoritative state is projected as `session.state.changed`, including an
explicit active turn id (or `null`). Startup events emitted while the first attachment creates its
runtime are synchronized into that replay before the snapshot. When a runtime is replaced in place,
existing attachments receive a new authoritative snapshot after replacement startup output so every
reader adopts the same resume cursor and state. If the requested cursor predates the retained window,
the host reports truncation and T3 projects a runtime warning instead of silently claiming complete
replay. That warning marks the detached binding for passive transcript hydration. The CLI importer
then reconciles app-server history with the durable rollout without changing the provider session's
owner, status, active turn, or resume cursor. Partial app-server turns (`summary` or `notLoaded`) use
the rollout as the historical fallback. The control-protocol handshake has a short deadline, while
the first snapshot has a separate, longer bounded deadline so resuming a large Codex thread is not
mistaken for an unreachable host.

Detached idle bindings are also observed for independent Codex CLI activity. Rollout modification
time and open-file ownership are the direct liveness signals; state-database `active` status is only
used when direct rollout evidence is unavailable. Transcript imports and final session projection
use runtime compare-and-swap guards so a concurrent T3 or CLI writer wins instead of receiving stale
observer output.

Provider event command ids are stable across replay, so reconnecting T3 does not duplicate already
persisted orchestration changes. Transport commands reuse their id while retrying against the same
provider-host generation. If the generation changes before a mutating command returns, T3 does not
repeat the mutation. It attempts a read-only thread check and reports the mutation result as
ambiguous instead. This lets orchestration replace stale projected state without inventing a new
`turn.started` event or prompting the model.

On server startup, [`ProviderSessionReaper`][reaper] reattaches bindings marked with detached session
persistence. A successful reattachment immediately projects the returned `connecting`, `ready`,
`running`, `error`, or `closed` state. Transient transport failures remain visibly reconnecting and
are retried without stopping the provider or aging the binding out through inactivity cleanup. A
typed missing-session result marks the detached execution as no longer present. T3 does not
automatically resume the persisted provider thread, submit a follow-up prompt, or claim that an
interrupted turn continued.

The generated systemd user service uses `KillMode=process`: restarting the T3 service stops its main
server process without killing provider hosts that intentionally continue in the service cgroup.

[drivers]: ../../apps/server/src/provider/builtInDrivers.ts
[codex]: ../../apps/server/src/provider/Drivers/CodexDriver.ts
[claude]: ../../apps/server/src/provider/Drivers/ClaudeDriver.ts
[cursor]: ../../apps/server/src/provider/Drivers/CursorDriver.ts
[grok]: ../../apps/server/src/provider/Drivers/GrokDriver.ts
[opencode]: ../../apps/server/src/provider/Drivers/OpenCodeDriver.ts
[adapter]: ../../apps/server/src/provider/Services/ProviderAdapter.ts
[instances]: ../../apps/server/src/provider/Services/ProviderInstanceRegistry.ts
[registry]: ../../apps/server/src/provider/Services/ProviderAdapterRegistry.ts
[service]: ../../apps/server/src/provider/Layers/ProviderService.ts
[contracts]: ../../packages/contracts/src/orchestration.ts
[worker]: ../../packages/shared/src/DrainableWorker.ts
[ingest]: ../../apps/server/src/orchestration/Layers/ProviderRuntimeIngestion.ts
[cmd]: ../../apps/server/src/orchestration/Layers/ProviderCommandReactor.ts
[checkpoint]: ../../apps/server/src/orchestration/Layers/CheckpointReactor.ts
[reaper]: ../../apps/server/src/provider/Layers/ProviderSessionReaper.ts
