# Provider constraints

Orchestration records intent and state without knowing which provider runs a thread. Provider
protocols, account ownership, permissions, and capabilities belong at the
[adapter boundary](../../apps/server/src/provider/Services/ProviderAdapter.ts). Normalize there
instead of spreading provider checks through reactors and clients.

A driver kind identifies an integration; an instance identifies one configuration and account
lifecycle. Route work by instance, so two accounts using the same driver do not share mutable
session or catalog state.

## Process and account isolation

[`builtInDrivers.ts`][drivers] exports `BUILT_IN_DRIVERS` with eight entries:

| Driver kind    | Driver source                                      |
| -------------- | -------------------------------------------------- |
| `codex`        | [`Drivers/CodexDriver.ts`][codex]                  |
| `claudeAgent`  | [`Drivers/ClaudeDriver.ts`][claude]                |
| `cursor`       | [`Drivers/CursorDriver.ts`][cursor]                |
| `grok`         | [`Drivers/GrokDriver.ts`][grok]                    |
| `omp`          | [`Drivers/OmpDriver.ts`][omp]                      |
| `opencode`     | [`Drivers/OpenCodeDriver.ts`][opencode]            |
| `piAgent`      | [`Drivers/PiDriver.ts`][pi]                        |
| `antigravity`  | [`Drivers/AntigravityDriver.ts`][antigravity]      |

T3-managed OpenCode chat uses one server per thread. Its MCP registrations are directory-scoped, while
T3's MCP connection is thread-scoped. Sharing a chat server between threads in one directory would
let them replace each other's connection. Catalog and text-generation work can share the
[instance-owned helper](../../apps/server/src/provider/OpenCodeServerOwner.ts), which closes
after an idle period. External OpenCode servers remain externally owned and can require an
external restart to pick up configuration changes.

OpenCode also stores persistent approval grants per directory. Automatic full-access replies use
`once` so they cannot widen a supervised thread's permissions on a shared external server.
See the [adapter](../../apps/server/src/provider/Layers/OpenCodeAdapter.ts).

Antigravity separates account profiles per instance while sharing installed executables across the
environment. It forces file-based credential storage because the native macOS keychain entry would
otherwise be shared across instances. The launch environment removes ambient Google credentials,
so an instance cannot silently use another account or billing project.
See [profile isolation](../../apps/server/src/provider/antigravityAuthSupport.ts).

Pi and Oh My Pi run one scoped JSONL RPC process per active session. Their persisted session files
support conversation resume, but execution remains process-bound. T3 records each runtime's native
session identity and ownership. **Release to CLI** stops only T3's adapter process, preserves the
native session, and copies the provider's resume command; a later message may take ownership again.
Sessions discovered from CLI history remain externally owned and are never stopped by T3.

## Registry and routing
The [Antigravity installer](../../apps/server/src/provider/AntigravityInstallation.ts) outlives
client connections and provider-instance rebuilds. Releases are immutable, with an atomic pointer
selecting the version for new processes. Running processes hold leases on their version. Updates
and removal must respect those leases instead of replacing executables under a running agent.

## Setup must not happen as a health-check side effect

Opening a provider session can start MCP servers, run hooks, or launch a login browser.
[Grok probes](../../apps/server/src/provider/Layers/GrokProvider.ts) avoid authentication and
session creation for this reason. Antigravity likewise reserves authenticated catalog sessions for
explicit setup or model refresh; background checks use initialization only.

[Antigravity sign-in](../../apps/server/src/provider/AntigravityAuth.ts) belongs to the initiating
T3 auth session. The client carries the return URL back to the environment because the provider's
loopback listener may be on another machine. Forward only the callback for the owned pending flow;
a successful callback HTTP request is not proof that provider authentication finished. The native
process owns token exchange and storage.

Antigravity sign-out closes admission to new processes and stops existing processes before clearing account
metadata. Otherwise a helper or resumed session could retain the old account. Cached model lists
do not establish current access, and an authoritative empty catalog must clear the old list.

Antigravity text-generation helpers deny tool requests, but native hooks and MCP configuration can
run before the prompt. They reject profiles with such configuration before launch. Prompt
instructions and tool denial do not create a native sandbox.
See [helper constraints](../../apps/server/src/textGeneration/AntigravityTextGeneration.ts).

## Provider updates run only through the owning installer

A one-click update is offered only when the resolved executable's path proves which installer owns
it. Homebrew and npm are proven by the real path (symlinks followed): a versioned keg or cask under
`brew --prefix`, or `<prefix>/lib/node_modules/<pkg>/` (Windows: the shim beside `node_modules`).
Native installer layouts and the global bin directories of pnpm, Bun, and Vite+ may match on either
the resolved path or its real target, since those installers place real files or their own symlinks
there. Anything unproven stays manual-only but still reports the version gap. npm updates pin
`--prefix` because the `npm` on `PATH` can belong to a different Node than the one that owns the
provider. Homebrew
compares against `brew info` since casks trail npm by hours; native installs share npm's version
train, so the registry stays authoritative for them.
See the [resolver](../../apps/server/src/provider/providerMaintenance.ts).

Ownership is cached per instance and re-read immediately before an update runs. The
[runner](../../apps/server/src/provider/providerMaintenanceRunner.ts) refuses when the lock key
changed since the advisory, and reports success only when the refreshed provider is still installed
with a readable, current version.

## Protocol traps

Codex async questions arrive as notifications and are answered with a new user message. There is
no pending RPC response to send. Blocking questions still use the request/response path. The
[adapter](../../apps/server/src/provider/Layers/CodexAdapter.ts) distinguishes them; the
[decider](../../apps/server/src/orchestration/decider.ts) records an async answer and its user
message together.

An async question can outlive the turn or a server restart. The engine reads that request's
durable activity before resolving it because the in-memory command snapshot omits old activities.
Do not infer that a request has disappeared merely because it is outside the recent window.

Capabilities must describe what the provider can actually do. Antigravity can capture workspace
checkpoints but cannot roll back its conversation. The [checkpoint boundary](./overview.md#turn-completion-and-checkpoints)
therefore rejects revert before touching files. Native permission and question option IDs must
also survive normalization; a display label is not necessarily a valid reply.

## Attachments and stored history

Attachments live outside the project workspace. [ProviderService](../../apps/server/src/provider/Layers/ProviderService.ts)
puts their environment-local paths in turn input and lets adapters choose native input formats.
A path in the prompt does not grant filesystem access. Keep provider sandbox and approval rules
in force; copying uploads into the project to bypass them changes that boundary.

File attachments introduced a replay compatibility limit. Image-only clients cannot decode
file-bearing messages, and an image-only server can fail the entire environment's startup when
replaying one such event. Rollouts and downgrades must account for persisted history as well as
current client support.

The server stores uploaded attachments in its attachment directory, outside the project workspace.
`ProviderService` adds the absolute path of each attachment to the turn text, then passes every
attachment to the provider adapter. Each adapter decides what its provider ingests natively:

- Codex, Claude, Cursor, and Grok send images as native image inputs and skip generic files. For
  these providers, generic files reach the agent only as file paths in the turn text.
- OpenCode sends PNG/JPEG/GIF/WebP images, text files, and PDFs up to 20 MB as native file parts
  with their real mime type. Everything else (ZIP and other binaries, image formats model APIs
  reject, oversized files) falls back to the file path in the turn text, like the other providers.

Claude receives the attachment directory as an allowed additional directory. Codex keeps its
configured sandbox policy, so access depends on that policy and the selected runtime mode. OpenCode
allows all paths in full-access mode and requests approval for directories outside the workspace in
restricted modes. Cursor and Grok use their own provider permission rules.

The server does not copy attachments into a project or bypass provider approval rules. If an agent
cannot read an attachment, the user must approve the access or select a runtime mode that permits it.

Updated attachment schemas tolerate unknown attachment members, but old image-only clients still
cannot decode messages that contain file attachments. Client file-picking rollouts must account for
this limit.

Do not run an old image-only server against state that contains file attachments. Replay decodes
each persisted event before projection. A file-bearing event can make `ProjectionPipeline` bootstrap
and `OrchestrationEngine` startup fail for the entire environment, not only the affected thread.

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
mistaken for an unreachable host. The client replay queue remains count- and byte-bounded; if its
consumer falls behind, T3 pauses socket reads and applies transport backpressure instead of closing
the attachment.

Detached idle bindings are also observed for independent Codex CLI activity. Rollout modification
time and open-file ownership are the direct liveness signals; state-database `active` status is only
used when direct rollout evidence is unavailable. Transcript imports and final session projection
use runtime compare-and-swap guards so a concurrent T3 or CLI writer wins instead of receiving stale
observer output. Existing projections seed monotonic rollout cursors, so periodic observation reads
only records newer than the last persisted import instead of reparsing the complete transcript.
Linux ownership probes inspect file descriptors only for Codex executables.

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
interrupted turn continued. When a detached execution stays unreachable, the reaper eventually gives
up: it records the attempt window in the runtime payload and, once that window passes, projects the
same missing-runtime state (`interrupted` while a turn was active, otherwise `stopped`) so a
permanently-hostless binding does not spin in the reconnecting state forever.

The generated systemd user service uses `KillMode=process`: restarting the T3 service stops its main
server process without killing provider hosts that intentionally continue in the service cgroup.
Model classification has its own [manifest constraints](./model-manifest.md). Assistant-reference
handling is documented under [citations](./assistant-citations.md).

[drivers]: ../../apps/server/src/provider/builtInDrivers.ts
[codex]: ../../apps/server/src/provider/Drivers/CodexDriver.ts
[claude]: ../../apps/server/src/provider/Drivers/ClaudeDriver.ts
[cursor]: ../../apps/server/src/provider/Drivers/CursorDriver.ts
[grok]: ../../apps/server/src/provider/Drivers/GrokDriver.ts
[opencode]: ../../apps/server/src/provider/Drivers/OpenCodeDriver.ts
[omp]: ../../apps/server/src/provider/Drivers/OmpDriver.ts
[pi]: ../../apps/server/src/provider/Drivers/PiDriver.ts
[antigravity]: ../../apps/server/src/provider/Drivers/AntigravityDriver.ts
[opencode-server-owner]: ../../apps/server/src/provider/OpenCodeServerOwner.ts
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
