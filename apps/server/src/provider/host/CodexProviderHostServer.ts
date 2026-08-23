// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalDate:off - Process identity is captured at startup.
// @effect-diagnostics globalTimers:off - Child and socket readiness use bounded timers.
// @effect-diagnostics preferSchemaOverJson:off - NDJSON framing encodes validated envelopes.
import {
  ProviderEvent,
  ProviderInstanceId,
  ThreadId,
  type ProviderSession,
  type ResourceTelemetryProcessIdentity,
} from "@t3tools/contracts";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";
import type * as EffectCodexSchema from "effect-codex-app-server/schema";

import { expandHomePath } from "../../pathExpansion.ts";
import { acquireSqliteTransactionLock } from "../../sqliteTransactionLock.ts";
import { codexAppServerArgs } from "../Layers/codexLaunchArgs.ts";
import {
  CodexResumeCursorSchema,
  CodexSessionRuntimeThreadIdMissingError,
  makeCodexSessionRuntime,
  type CodexSessionRuntimeError,
  type CodexSessionRuntimeOptions,
  type CodexSessionRuntimeShape,
} from "../Layers/CodexSessionRuntime.ts";
import { probeCodexAppServerWebSocket } from "../Layers/CodexAppServerWebSocket.ts";
import type { CodexProviderHostConfig } from "./CodexProviderHostConfig.ts";
import {
  CODEX_PROVIDER_HOST_OPERATIONS,
  CodexProviderHostApprovalPayload,
  CodexProviderHostFeedbackPayload,
  CodexProviderHostInterruptPayload,
  CodexProviderHostRollbackPayload,
  CodexProviderHostSendTurnPayload,
  CodexProviderHostSessionOptions,
  CodexProviderHostUserInputPayload,
} from "./CodexProviderHostSession.ts";
import {
  makeProviderHostCommandResultCache,
  type ProviderHostCommandResultCache,
} from "./ProviderHostCommandResultCache.ts";
import {
  PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
  persistProviderHostManifest,
  readProviderHostManifest,
} from "./ProviderHostManifest.ts";
import {
  PROVIDER_HOST_PROTOCOL_VERSION,
  ProviderHostAttachErrorEnvelope,
  ProviderHostAttachmentId,
  ProviderHostBuildFingerprint,
  type ProviderHostClientId,
  ProviderHostClientEnvelope,
  ProviderHostCommandResultEnvelope,
  ProviderHostEventEnvelope,
  ProviderHostEventSequence,
  ProviderHostGenerationFingerprint,
  ProviderHostHealthEnvelope,
  ProviderHostHelloEnvelope,
  ProviderHostInventoryEnvelope,
  ProviderHostReplayCursor,
  ProviderHostSnapshotEnvelope,
  providerHostReplayCursorForSequence,
  type ProviderHostCommandEnvelope,
  type ProviderHostClientEnvelope as ProviderHostClientEnvelopeType,
  type ProviderHostServerEnvelope,
} from "./ProviderHostProtocol.ts";
import {
  makeProviderHostLineFramer,
  type ProviderHostLineFramer,
} from "./ProviderHostLineFramer.ts";

const START_TIMEOUT_MS = 10_000;
const START_POLL_MS = 50;
const MAX_INBOUND_LINE_BYTES = 32 * 1024 * 1024;
const MAX_REPLAY_EVENTS = 4_096;
const MAX_REPLAY_BYTES = 16 * 1024 * 1024;
const MAX_OUTBOUND_FRAME_BYTES = MAX_REPLAY_BYTES;
const MAX_OUTBOUND_BUFFER_BYTES = MAX_OUTBOUND_FRAME_BYTES * 2;
const MAX_COMMAND_RESULT_CACHE_BYTES = 64 * 1024 * 1024;
const MAX_COMMAND_FIBERS = 256;
const PRIORITY_COMMAND_FIBER_RESERVE = 16;
const COMMAND_TIMEOUT_MS = 120_000;
const MAX_PENDING_ENVELOPES_PER_CONNECTION = 256;
const MAX_PENDING_ENVELOPE_BYTES_PER_CONNECTION = MAX_INBOUND_LINE_BYTES * 2;
const ZERO_ATTACHMENT_IDLE_TIMEOUT_MS = 60_000;
const APP_SERVER_MONITOR_INITIAL_DELAY_MS = 5_000;
const APP_SERVER_MONITOR_MAX_DELAY_MS = 60_000;
const APP_SERVER_MONITOR_FAILURE_DELAY_MS = 1_000;
const APP_SERVER_MONITOR_FAILURE_THRESHOLD = 3;
const CHILD_TERMINATE_GRACE_MS = 2_000;
const STARTUP_LOCK_TIMEOUT_MS = 30_000;

const decodeClientEnvelope = Schema.decodeUnknownResult(
  Schema.fromJsonString(ProviderHostClientEnvelope),
);
const decodeSessionOptions = Schema.decodeUnknownEffect(CodexProviderHostSessionOptions);
const decodeSendTurn = Schema.decodeUnknownEffect(CodexProviderHostSendTurnPayload);
const decodeInterrupt = Schema.decodeUnknownEffect(CodexProviderHostInterruptPayload);
const decodeRollback = Schema.decodeUnknownEffect(CodexProviderHostRollbackPayload);
const decodeApproval = Schema.decodeUnknownEffect(CodexProviderHostApprovalPayload);
const decodeUserInput = Schema.decodeUnknownEffect(CodexProviderHostUserInputPayload);
const decodeFeedback = Schema.decodeUnknownEffect(CodexProviderHostFeedbackPayload);
const isCodexResumeCursor = Schema.is(CodexResumeCursorSchema);
const isCodexSessionRuntimeThreadIdMissingError = Schema.is(
  CodexSessionRuntimeThreadIdMissingError,
);
const encodeUnknownJsonString = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const decodeJsonString = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Json));

function providerSessionSnapshotState(snapshot: ProviderSession): Schema.Json {
  return decodeJsonString(encodeUnknownJsonString(snapshot));
}

interface ReplayEntry {
  readonly envelope: ProviderHostEventEnvelope;
  readonly encoded: string;
  readonly bytes: number;
}

interface HostConnection {
  readonly socket: NodeNet.Socket;
  readonly attachments: Map<
    ProviderHostAttachmentId,
    {
      readonly clientId: ProviderHostClientId;
      readonly threadId: ThreadId;
    }
  >;
  processing: Promise<void>;
  pendingEnvelopes: number;
  pendingEnvelopeBytes: number;
  closed: boolean;
  readonly lineFramer: ProviderHostLineFramer;
}

interface HostSession {
  readonly options: CodexProviderHostSessionOptions;
  readonly creationMode: "normal" | "resume-only";
  readonly scope: Scope.Closeable;
  readonly runtime: CodexSessionRuntimeShape;
  readonly eventFiber: Fiber.Fiber<void, never>;
  readonly commandLock: Semaphore.Semaphore;
  readonly attachments: Set<HostConnection>;
}

function sessionOptionsRequireReplacement(
  current: CodexProviderHostSessionOptions,
  requested: CodexProviderHostSessionOptions,
  preserveUnspecifiedThreadConfig = false,
): boolean {
  return (
    current.providerInstanceId !== requested.providerInstanceId ||
    current.cwd !== requested.cwd ||
    current.runtimeMode !== requested.runtimeMode ||
    current.model !== requested.model ||
    current.serviceTier !== requested.serviceTier ||
    ((!preserveUnspecifiedThreadConfig || requested.threadConfig !== undefined) &&
      !NodeUtil.isDeepStrictEqual(current.threadConfig, requested.threadConfig))
  );
}

function preserveAdoptedThreadConfig(
  current: CodexProviderHostSessionOptions,
  requested: CodexProviderHostSessionOptions,
  adoptionMode: "normal" | "resume-only",
): CodexProviderHostSessionOptions {
  return adoptionMode === "resume-only" &&
    requested.threadConfig === undefined &&
    current.threadConfig !== undefined
    ? { ...requested, threadConfig: current.threadConfig }
    : requested;
}

function sessionMatchesResumeCursor(
  snapshot: ProviderSession,
  requested: CodexProviderHostSessionOptions,
): boolean {
  return (
    isCodexResumeCursor(snapshot.resumeCursor) &&
    isCodexResumeCursor(requested.resumeCursor) &&
    snapshot.resumeCursor.threadId === requested.resumeCursor.threadId
  );
}

export interface CodexProviderHostServerOptions {
  readonly makeRuntime?: (
    options: CodexSessionRuntimeOptions,
  ) => Effect.Effect<
    CodexSessionRuntimeShape,
    CodexSessionRuntimeError,
    ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto | Scope.Scope
  >;
  readonly spawnCodex?: (config: CodexProviderHostConfig) => Promise<NodeChildProcess.ChildProcess>;
  readonly maxReplayEvents?: number;
  readonly maxReplayBytes?: number;
  readonly idleTimeoutMs?: number;
  readonly maxCommandFibers?: number;
  readonly commandTimeoutMs?: number;
  readonly priorityCommandFiberReserve?: number;
  readonly maxPendingEnvelopesPerConnection?: number;
  readonly maxPendingEnvelopeBytesPerConnection?: number;
  readonly probeAppServer?: (socketPath: string) => Promise<boolean>;
  readonly waitForAppServerReady?: (socketPath: string) => Promise<boolean>;
  readonly inspectAppServerProcess?: (
    process: ResourceTelemetryProcessIdentity,
  ) => Promise<ProcessIdentityStatus>;
}

export class CodexProviderHostError extends Schema.TaggedErrorClass<CodexProviderHostError>()(
  "CodexProviderHostError",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Codex provider-host operation '${this.operation}' failed.`;
  }
}

function inheritedEnvironment(): NodeJS.ProcessEnv {
  return Object.fromEntries(
    Object.entries(process.env).filter(
      (entry): entry is [string, string] => typeof entry[1] === "string",
    ),
  );
}

function currentProcessIdentity(): ResourceTelemetryProcessIdentity {
  return {
    pid: process.pid,
    startTimeMs: Math.max(0, Math.floor(Date.now() - process.uptime() * 1_000)),
  };
}

export type ProcessIdentityStatus = "current" | "stale" | "unknown";

function readProcessStartTimeMs(pid: number): Promise<number | undefined> {
  return new Promise((resolve) => {
    NodeChildProcess.execFile(
      "ps",
      ["-o", "lstart=", "-p", String(pid)],
      {
        env: {
          ...process.env,
          LANG: "C",
          LC_ALL: "C",
        },
      },
      (cause, stdout) => {
        if (cause) {
          resolve(undefined);
          return;
        }
        const startTimeMs = Date.parse(stdout.trim());
        resolve(Number.isFinite(startTimeMs) ? startTimeMs : undefined);
      },
    );
  });
}

async function inspectProcessIdentity(
  identity: ResourceTelemetryProcessIdentity,
): Promise<ProcessIdentityStatus> {
  try {
    process.kill(identity.pid, 0);
  } catch (cause) {
    return (cause as NodeJS.ErrnoException).code === "EPERM" ? "unknown" : "stale";
  }
  const startTimeMs = await readProcessStartTimeMs(identity.pid);
  if (startTimeMs === undefined) return "unknown";
  return Math.abs(startTimeMs - identity.startTimeMs) <= 2_000 ? "current" : "stale";
}

async function childProcessIdentity(
  child: NodeChildProcess.ChildProcess,
  observedStartTimeMs: number,
): Promise<ResourceTelemetryProcessIdentity> {
  const pid = child.pid ?? 0;
  return {
    pid,
    startTimeMs: (pid > 0 ? await readProcessStartTimeMs(pid) : undefined) ?? observedStartTimeMs,
  };
}

function errorMessage(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}

function encodeEnvelope(envelope: ProviderHostServerEnvelope): string {
  return `${JSON.stringify(envelope)}\n`;
}

function writeEncodedEnvelope(connection: HostConnection, encoded: string): boolean {
  if (connection.closed || connection.socket.destroyed) {
    return false;
  }
  const encodedBytes = Buffer.byteLength(encoded);
  if (encodedBytes > MAX_OUTBOUND_FRAME_BYTES) {
    connection.socket.destroy(new Error("Provider-host frame exceeded the maximum frame size."));
    return false;
  }
  if (connection.socket.writableLength + encodedBytes > MAX_OUTBOUND_BUFFER_BYTES) {
    connection.socket.destroy(
      new Error("Provider-host reader exceeded the bounded outbound buffer."),
    );
    return false;
  }
  connection.socket.write(encoded);
  return true;
}

function writeEnvelope(connection: HostConnection, envelope: ProviderHostServerEnvelope): boolean {
  return writeEncodedEnvelope(connection, encodeEnvelope(envelope));
}

function boundedCommandResultEnvelope(
  envelope: ProviderHostCommandResultEnvelope,
): ProviderHostCommandResultEnvelope {
  if (Buffer.byteLength(encodeEnvelope(envelope)) <= MAX_OUTBOUND_FRAME_BYTES) {
    return envelope;
  }
  return ProviderHostCommandResultEnvelope.make({
    version: PROVIDER_HOST_PROTOCOL_VERSION,
    type: "commandResult",
    commandId: envelope.commandId,
    threadId: envelope.threadId,
    ok: false,
    error: `Provider-host command result exceeded the ${MAX_OUTBOUND_FRAME_BYTES}-byte frame limit.`,
  });
}

async function waitForCodexSocket(socketPath: string): Promise<boolean> {
  const deadline = Date.now() + START_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (await probeCodexAppServerWebSocket(socketPath)) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, START_POLL_MS));
  }
  return false;
}

function terminateChild(child: NodeChildProcess.ChildProcess): Promise<void> {
  return new Promise((resolve) => {
    let settled = false;
    let forceTimer: ReturnType<typeof setTimeout> | undefined;
    const hasExited = () => child.exitCode !== null || child.signalCode !== null;
    const finish = () => {
      if (settled) return;
      settled = true;
      if (forceTimer !== undefined) {
        clearTimeout(forceTimer);
        forceTimer = undefined;
      }
      child.off("exit", finish);
      resolve();
    };
    child.once("exit", finish);

    if (hasExited()) {
      finish();
      return;
    }

    forceTimer = setTimeout(() => {
      forceTimer = undefined;
      if (!hasExited()) {
        try {
          child.kill("SIGKILL");
        } catch {
          // The captured child already exited.
        }
      }
      finish();
    }, CHILD_TERMINATE_GRACE_MS);

    try {
      if (!child.kill("SIGTERM") && hasExited()) {
        finish();
      }
    } catch {
      finish();
    }
  });
}

type ProviderHostExit = { readonly _tag: "app-server-unavailable" } | { readonly _tag: "idle" };

interface AppServerMonitorState {
  readonly delayMs: number;
  readonly consecutiveFailures: number;
}

interface SocketPathIdentity {
  readonly device: number;
  readonly inode: number;
}

async function readSocketPathIdentity(socketPath: string): Promise<SocketPathIdentity | undefined> {
  try {
    const stat = await NodeFSP.lstat(socketPath);
    return {
      device: stat.dev,
      inode: stat.ino,
    };
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw cause;
  }
}

async function removeSocketPathIfOwned(
  socketPath: string,
  expected: SocketPathIdentity,
): Promise<boolean> {
  const current = await readSocketPathIdentity(socketPath);
  if (!current || current.device !== expected.device || current.inode !== expected.inode) {
    return false;
  }
  await NodeFSP.rm(socketPath, { force: true });
  return true;
}

function nextAppServerMonitorState(
  state: AppServerMonitorState,
  available: boolean,
): AppServerMonitorState {
  return available
    ? {
        delayMs: Math.min(state.delayMs * 2, APP_SERVER_MONITOR_MAX_DELAY_MS),
        consecutiveFailures: 0,
      }
    : {
        delayMs: APP_SERVER_MONITOR_FAILURE_DELAY_MS,
        consecutiveFailures: state.consecutiveFailures + 1,
      };
}

async function defaultSpawnCodex(
  config: CodexProviderHostConfig,
): Promise<NodeChildProcess.ChildProcess> {
  const env = inheritedEnvironment();
  const resolvedHomePath = config.codex.homePath
    ? expandHomePath(config.codex.homePath)
    : undefined;
  if (resolvedHomePath) {
    env.CODEX_HOME = resolvedHomePath;
  }
  const spawnCommand = await Effect.runPromise(
    resolveSpawnCommand(
      config.codex.binaryPath,
      [
        ...codexAppServerArgs(config.codex.launchArgs),
        "--listen",
        `unix://${config.appServerSocketPath}`,
      ],
      {
        env,
        extendEnv: false,
      },
    ),
  );
  const child = NodeChildProcess.spawn(spawnCommand.command, [...spawnCommand.args], {
    cwd: config.codex.cwd,
    detached: true,
    env,
    shell: spawnCommand.shell,
    stdio: ["ignore", "inherit", "inherit"],
  });
  child.unref();
  return child;
}

function readCodexVersion(config: CodexProviderHostConfig): Promise<string> {
  return new Promise((resolve) => {
    const child = NodeChildProcess.spawn(config.codex.binaryPath, ["--version"], {
      cwd: config.codex.cwd,
      env: inheritedEnvironment(),
      stdio: ["ignore", "pipe", "ignore"],
    });
    let output = "";
    const timer = setTimeout(() => {
      child.kill();
      resolve("unknown");
    }, 2_000);
    child.stdout?.setEncoding("utf8");
    child.stdout?.on("data", (chunk: string) => {
      output += chunk;
    });
    child.once("error", () => {
      clearTimeout(timer);
      resolve("unknown");
    });
    child.once("exit", () => {
      clearTimeout(timer);
      resolve(output.trim() || "unknown");
    });
  });
}

export const runCodexProviderHost = Effect.fn("runCodexProviderHost")(function* (
  config: CodexProviderHostConfig,
  options?: CodexProviderHostServerOptions,
) {
  const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const crypto = yield* Crypto.Crypto;
  const hostScope = yield* Scope.Scope;
  const runtimeContext = yield* Effect.context<never>();
  const runPromise = Effect.runPromiseWith(runtimeContext);
  const eventLock = yield* Semaphore.make(1);
  const sessionCreationLock = yield* Semaphore.make(1);
  const sessions = new Map<ThreadId, HostSession>();
  const sessionCreations = new Map<
    ThreadId,
    {
      readonly deferred: Deferred.Deferred<
        HostSession,
        CodexSessionRuntimeError | CodexProviderHostError
      >;
      readonly settled: Deferred.Deferred<void>;
    }
  >();
  const sessionStops = new Map<ThreadId, Deferred.Deferred<boolean>>();
  const commandResults: ProviderHostCommandResultCache<ProviderHostCommandResultEnvelope, never> =
    yield* makeProviderHostCommandResultCache<ProviderHostCommandResultEnvelope, never>(4_096, {
      maxCompletedBytes: MAX_COMMAND_RESULT_CACHE_BYTES,
      completedEntryBytes: (result) => Buffer.byteLength(encodeEnvelope(result)),
    });
  const commandFiberCount = yield* Ref.make(0);
  const connections = new Set<HostConnection>();
  let replay: ReplayEntry[] = [];
  let replayHead = 0;
  let replayBytes = 0;
  const droppedReplayCursorByThread = new Map<ThreadId, ProviderHostReplayCursor>();
  const maxReplayEvents = options?.maxReplayEvents ?? MAX_REPLAY_EVENTS;
  const maxReplayBytes = options?.maxReplayBytes ?? MAX_REPLAY_BYTES;
  const idleTimeoutMs = options?.idleTimeoutMs ?? ZERO_ATTACHMENT_IDLE_TIMEOUT_MS;
  const maxCommandFibers = options?.maxCommandFibers ?? MAX_COMMAND_FIBERS;
  const commandTimeoutMs = Math.max(1, options?.commandTimeoutMs ?? COMMAND_TIMEOUT_MS);
  const priorityCommandFiberReserve =
    options?.priorityCommandFiberReserve ?? PRIORITY_COMMAND_FIBER_RESERVE;
  const maxPendingEnvelopesPerConnection =
    options?.maxPendingEnvelopesPerConnection ?? MAX_PENDING_ENVELOPES_PER_CONNECTION;
  const maxPendingEnvelopeBytesPerConnection =
    options?.maxPendingEnvelopeBytesPerConnection ?? MAX_PENDING_ENVELOPE_BYTES_PER_CONNECTION;
  let latestCursor = ProviderHostReplayCursor.make(0);
  const hostProcess = currentProcessIdentity();
  const generationFingerprint = ProviderHostGenerationFingerprint.make(
    NodeCrypto.createHash("sha256")
      .update(
        `${config.buildFingerprint}:${config.configurationFingerprint}:` +
          `${hostProcess.pid}:${hostProcess.startTimeMs}`,
      )
      .digest("hex"),
  );
  const buildFingerprint = ProviderHostBuildFingerprint.make(config.buildFingerprint);
  const canAdoptSessions = true;
  const startedAt = yield* DateTime.now;
  const hostExit = yield* Deferred.make<ProviderHostExit>();
  const startupLock = yield* Effect.tryPromise({
    try: () =>
      acquireSqliteTransactionLock(config.startupLockPath, {
        timeoutMs: STARTUP_LOCK_TIMEOUT_MS,
      }),
    catch: (cause) =>
      new CodexProviderHostError({
        operation: "acquire-startup-lock",
        cause,
      }),
  });
  let startupLockReleased = false;
  const releaseStartupLock = Effect.suspend(() => {
    if (startupLockReleased) return Effect.void;
    startupLockReleased = true;
    return Effect.tryPromise({
      try: startupLock.release,
      catch: (cause) =>
        new CodexProviderHostError({
          operation: "release-startup-lock",
          cause,
        }),
    });
  });
  yield* Effect.addFinalizer(() => releaseStartupLock.pipe(Effect.ignore));
  const startupManifest = Option.getOrUndefined(
    yield* readProviderHostManifest(config.manifestPath),
  );
  const currentManifestGeneration = startupManifest?.generationFingerprint;
  if (currentManifestGeneration !== config.expectedManifestGenerationFingerprint) {
    return yield* new CodexProviderHostError({
      operation: "claim-startup-generation",
      cause: new Error(
        "Another provider host published a newer manifest before this startup acquired the detached-host lease.",
      ),
    });
  }
  const spawnCodex = options?.spawnCodex ?? defaultSpawnCodex;
  const probeAppServer = options?.probeAppServer ?? probeCodexAppServerWebSocket;
  const waitForAppServerReady = options?.waitForAppServerReady ?? waitForCodexSocket;
  const inspectAppServerProcess = options?.inspectAppServerProcess ?? inspectProcessIdentity;
  let effectiveAppServerMode = config.appServerMode;
  let adoptedAppServer = config.adoptedAppServer;
  if (effectiveAppServerMode === "spawn") {
    const appServerAlreadyAvailable = yield* Effect.tryPromise({
      try: () => probeAppServer(config.appServerSocketPath),
      catch: (cause) =>
        new CodexProviderHostError({
          operation: "revalidate-app-server-socket",
          cause,
        }),
    });
    if (appServerAlreadyAvailable) {
      const manifestAppServer =
        startupManifest?.schemaVersion === 2 &&
        startupManifest.codex.appServer.socketPath === config.appServerSocketPath
          ? {
              owner: startupManifest.codex.owner,
              appServer: startupManifest.codex.appServer,
            }
          : undefined;
      if (!manifestAppServer) {
        return yield* new CodexProviderHostError({
          operation: "claim-app-server-socket",
          cause: new Error(
            "The app-server socket became available under the startup lease without verified process provenance.",
          ),
        });
      }
      const processStatus = yield* Effect.tryPromise({
        try: () => inspectAppServerProcess(manifestAppServer.appServer.process),
        catch: (cause) =>
          new CodexProviderHostError({
            operation: "verify-adopted-codex",
            cause,
          }),
      });
      if (processStatus !== "current") {
        return yield* new CodexProviderHostError({
          operation: "verify-adopted-codex",
          cause: new Error(
            `Codex process identity became ${processStatus} before provider-host startup.`,
          ),
        });
      }
      effectiveAppServerMode = "attach";
      adoptedAppServer = manifestAppServer;
    }
  }
  if (effectiveAppServerMode === "attach") {
    if (!adoptedAppServer || adoptedAppServer.appServer.socketPath !== config.appServerSocketPath) {
      return yield* new CodexProviderHostError({
        operation: "adopt-app-server",
        cause: new Error("Attach-mode provider host requires matching app-server provenance."),
      });
    }
    const processStatus = yield* Effect.tryPromise({
      try: () => inspectAppServerProcess(adoptedAppServer.appServer.process),
      catch: (cause) =>
        new CodexProviderHostError({
          operation: "verify-adopted-codex",
          cause,
        }),
    });
    if (processStatus !== "current") {
      return yield* new CodexProviderHostError({
        operation: "verify-adopted-codex",
        cause: new Error(
          `Adopted Codex process identity became ${processStatus} before provider-host startup.`,
        ),
      });
    }
  }
  const codexSpawnObservedAtMs = yield* Clock.currentTimeMillis;
  const appServerProvenance = yield* Effect.acquireUseRelease(
    effectiveAppServerMode === "spawn"
      ? Effect.tryPromise({
          try: () => spawnCodex(config),
          catch: (cause) =>
            new CodexProviderHostError({
              operation: "spawn-codex",
              cause,
            }),
        })
      : Effect.sync((): NodeChildProcess.ChildProcess | undefined => undefined),
    (codexChild) =>
      Effect.gen(function* () {
        const provenance = codexChild
          ? {
              owner: {
                generationFingerprint,
                process: hostProcess,
              },
              appServer: {
                process: yield* Effect.promise(() =>
                  childProcessIdentity(codexChild, codexSpawnObservedAtMs),
                ),
                socketPath: config.appServerSocketPath,
                resolvedBinary: config.codex.binaryPath,
                version: yield* Effect.promise(() => readCodexVersion(config)),
                launchConfig: {
                  arguments: [
                    ...codexAppServerArgs(config.codex.launchArgs),
                    "--listen",
                    `unix://${config.appServerSocketPath}`,
                  ],
                  workingDirectory: config.codex.cwd,
                  environmentKeys: Object.keys(inheritedEnvironment()).sort(),
                },
              },
            }
          : adoptedAppServer;
        if (!provenance) {
          return yield* new CodexProviderHostError({
            operation: "adopt-app-server",
            cause: new Error("Attach-mode provider host requires verified app-server provenance."),
          });
        }
        const ready = yield* Effect.tryPromise({
          try: () => waitForAppServerReady(config.appServerSocketPath),
          catch: (cause) =>
            new CodexProviderHostError({
              operation: "wait-for-codex",
              cause,
            }),
        });
        if (!ready) {
          return yield* new CodexProviderHostError({
            operation: "wait-for-codex",
            cause: new Error(
              `Codex app-server did not become ready at ${config.appServerSocketPath}.`,
            ),
          });
        }
        const appServerProcessStatus = yield* Effect.tryPromise({
          try: () => inspectAppServerProcess(provenance.appServer.process),
          catch: (cause) =>
            new CodexProviderHostError({
              operation: codexChild ? "verify-spawned-codex" : "verify-adopted-codex",
              cause,
            }),
        });
        if (appServerProcessStatus !== "current") {
          return yield* new CodexProviderHostError({
            operation: codexChild ? "verify-spawned-codex" : "verify-adopted-codex",
            cause: new Error(
              `${codexChild ? "Spawned" : "Adopted"} Codex process identity became ${appServerProcessStatus} before manifest publication.`,
            ),
          });
        }
        if (codexChild) {
          const socketStillAvailable = yield* Effect.tryPromise({
            try: () => probeAppServer(config.appServerSocketPath),
            catch: (cause) =>
              new CodexProviderHostError({
                operation: "verify-spawned-codex-socket",
                cause,
              }),
          });
          if (!socketStillAvailable) {
            return yield* new CodexProviderHostError({
              operation: "verify-spawned-codex-socket",
              cause: new Error(
                `Spawned Codex app-server socket disappeared before manifest publication.`,
              ),
            });
          }
        }
        yield* persistProviderHostManifest({
          path: config.manifestPath,
          manifest: {
            schemaVersion: PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
            protocolVersion: PROVIDER_HOST_PROTOCOL_VERSION,
            buildFingerprint,
            generationFingerprint,
            hostProcess,
            controlSocketPath: config.controlSocketPath,
            codex: {
              appServerMode: effectiveAppServerMode,
              ...provenance,
            },
            startedAt,
          },
        });
        return provenance;
      }),
    (codexChild, exit) =>
      codexChild && Exit.isFailure(exit)
        ? Effect.promise(() => terminateChild(codexChild))
        : Effect.void,
  );
  let idleTimer: ReturnType<typeof setTimeout> | undefined;
  let idleShutdownStarted = false;
  const hasAttachedClients = () =>
    Array.from(sessions.values()).some((session) => session.attachments.size > 0);
  const cancelIdleShutdown = () => {
    if (idleTimer === undefined) return;
    clearTimeout(idleTimer);
    idleTimer = undefined;
  };
  const scheduleIdleShutdown = () => {
    if (
      idleTimeoutMs <= 0 ||
      idleShutdownStarted ||
      idleTimer !== undefined ||
      hasAttachedClients() ||
      sessionCreations.size > 0
    ) {
      return;
    }
    idleTimer = setTimeout(() => {
      idleTimer = undefined;
      if (hasAttachedClients() || sessionCreations.size > 0 || idleShutdownStarted) {
        return;
      }
      idleShutdownStarted = true;
      void runPromise(Deferred.succeed(hostExit, { _tag: "idle" as const }));
    }, idleTimeoutMs);
    idleTimer.unref?.();
  };
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      cancelIdleShutdown();
    }),
  );
  yield* Effect.gen(function* () {
    let monitorState: AppServerMonitorState = {
      delayMs: APP_SERVER_MONITOR_INITIAL_DELAY_MS,
      consecutiveFailures: 0,
    };
    while (monitorState.consecutiveFailures < APP_SERVER_MONITOR_FAILURE_THRESHOLD) {
      yield* Effect.sleep(`${monitorState.delayMs} millis`);
      const [socketAvailable, processStatus] = yield* Effect.all(
        [
          Effect.tryPromise(() => probeAppServer(config.appServerSocketPath)).pipe(
            Effect.orElseSucceed(() => false),
          ),
          Effect.tryPromise(() =>
            inspectAppServerProcess(appServerProvenance.appServer.process),
          ).pipe(Effect.orElseSucceed(() => "unknown" as const)),
        ],
        { concurrency: 2 },
      );
      monitorState = nextAppServerMonitorState(
        monitorState,
        socketAvailable && processStatus !== "stale",
      );
    }
    yield* Deferred.succeed(hostExit, { _tag: "app-server-unavailable" as const });
  }).pipe(Effect.forkIn(hostScope));

  const inventoryEnvelope = (threadId: ThreadId): ProviderHostInventoryEnvelope => {
    const session = sessions.get(threadId);
    return ProviderHostInventoryEnvelope.make({
      version: PROVIDER_HOST_PROTOCOL_VERSION,
      type: "inventory",
      threads: session
        ? [
            {
              threadId,
              status: "unknown",
              attachmentCount: session.attachments.size,
              cursor: latestCursor,
            },
          ]
        : [],
    });
  };

  const publishEvent = (event: ProviderEvent) =>
    eventLock.withPermit(
      Effect.sync(() => {
        const sequence = ProviderHostEventSequence.make(Number(latestCursor) + 1);
        latestCursor = providerHostReplayCursorForSequence(sequence);
        const envelope = ProviderHostEventEnvelope.make({
          version: PROVIDER_HOST_PROTOCOL_VERSION,
          type: "event",
          threadId: event.threadId,
          sequence,
          event,
        });
        const encoded = `${JSON.stringify(envelope)}\n`;
        const entry = {
          envelope,
          encoded,
          bytes: Buffer.byteLength(encoded),
        } satisfies ReplayEntry;
        replay.push(entry);
        replayBytes += entry.bytes;
        while (replay.length - replayHead > maxReplayEvents || replayBytes > maxReplayBytes) {
          const removed = replay[replayHead];
          if (removed) {
            replayHead += 1;
            replayBytes -= removed.bytes;
            droppedReplayCursorByThread.set(
              removed.envelope.threadId,
              providerHostReplayCursorForSequence(removed.envelope.sequence),
            );
          }
        }
        if (replayHead >= 1_024 && replayHead * 2 >= replay.length) {
          replay = replay.slice(replayHead);
          replayHead = 0;
        }
        const session = sessions.get(event.threadId);
        if (!session) return;
        for (const connection of session.attachments) {
          writeEncodedEnvelope(connection, encoded);
        }
      }),
    );

  const makeRuntime = options?.makeRuntime ?? makeCodexSessionRuntime;
  const clearReplayForThread = (threadId: ThreadId) => {
    for (let index = replay.length - 1; index >= replayHead; index -= 1) {
      const entry = replay[index];
      if (entry?.envelope.threadId !== threadId) continue;
      replay.splice(index, 1);
      replayBytes -= entry.bytes;
    }
    if (replayHead > 0) {
      replay = replay.slice(replayHead);
      replayHead = 0;
    }
    droppedReplayCursorByThread.delete(threadId);
  };
  const createSession: (
    requestedSessionOptions: CodexProviderHostSessionOptions,
    adoptionMode?: "normal" | "resume-only",
  ) => Effect.Effect<HostSession, CodexSessionRuntimeError | CodexProviderHostError> = Effect.fn(
    "CodexProviderHost.createSession",
  )(function* (requestedSessionOptions: CodexProviderHostSessionOptions, adoptionMode = "normal") {
    const admission = yield* sessionCreationLock.withPermit(
      Effect.gen(function* () {
        const stopping = sessionStops.get(requestedSessionOptions.threadId);
        if (stopping) {
          return { _tag: "stopping" as const, deferred: stopping };
        }
        const pending = sessionCreations.get(requestedSessionOptions.threadId);
        if (pending) {
          return { _tag: "pending" as const, ...pending };
        }
        const existing = sessions.get(requestedSessionOptions.threadId);
        if (existing) {
          const snapshot = yield* existing.runtime.getSession;
          const eventFiberExit = existing.eventFiber.pollUnsafe();
          const effectiveRequestedSessionOptions = preserveAdoptedThreadConfig(
            existing.options,
            requestedSessionOptions,
            adoptionMode,
          );
          if (
            snapshot.status !== "error" &&
            snapshot.status !== "closed" &&
            eventFiberExit === undefined &&
            (adoptionMode !== "resume-only" ||
              sessionMatchesResumeCursor(snapshot, effectiveRequestedSessionOptions)) &&
            !sessionOptionsRequireReplacement(
              existing.options,
              effectiveRequestedSessionOptions,
              adoptionMode === "resume-only",
            )
          ) {
            return { _tag: "existing" as const, session: existing };
          }
          const deferred = yield* Deferred.make<
            HostSession,
            CodexSessionRuntimeError | CodexProviderHostError
          >();
          const settled = yield* Deferred.make<void>();
          sessionCreations.set(requestedSessionOptions.threadId, {
            deferred,
            settled,
          });
          return {
            _tag: "create" as const,
            deferred,
            settled,
            previous: existing,
            sessionOptions: {
              ...effectiveRequestedSessionOptions,
              ...(effectiveRequestedSessionOptions.resumeCursor === undefined &&
              isCodexResumeCursor(snapshot.resumeCursor)
                ? { resumeCursor: snapshot.resumeCursor }
                : {}),
            } satisfies CodexProviderHostSessionOptions,
          };
        }
        const deferred = yield* Deferred.make<
          HostSession,
          CodexSessionRuntimeError | CodexProviderHostError
        >();
        const settled = yield* Deferred.make<void>();
        sessionCreations.set(requestedSessionOptions.threadId, {
          deferred,
          settled,
        });
        return {
          _tag: "create" as const,
          deferred,
          settled,
          previous: undefined,
          sessionOptions: requestedSessionOptions,
        };
      }),
    );
    if (admission._tag === "existing") return admission.session;
    if (admission._tag === "pending") {
      const pendingExit = yield* Deferred.await(admission.deferred).pipe(Effect.exit);
      yield* Deferred.await(admission.settled);
      if (Exit.isFailure(pendingExit)) {
        return yield* createSession(requestedSessionOptions, adoptionMode);
      }
      const session = pendingExit.value;
      const snapshot = yield* session.runtime.getSession;
      const effectiveRequestedSessionOptions = preserveAdoptedThreadConfig(
        session.options,
        requestedSessionOptions,
        adoptionMode,
      );
      if (
        snapshot.status !== "error" &&
        snapshot.status !== "closed" &&
        (adoptionMode !== "resume-only" ||
          sessionMatchesResumeCursor(snapshot, effectiveRequestedSessionOptions)) &&
        !sessionOptionsRequireReplacement(
          session.options,
          effectiveRequestedSessionOptions,
          adoptionMode === "resume-only",
        )
      ) {
        return session;
      }
      return yield* createSession(effectiveRequestedSessionOptions, adoptionMode);
    }
    if (admission._tag === "stopping") {
      const stopped = yield* Deferred.await(admission.deferred);
      if (stopped) {
        return yield* new CodexProviderHostError({
          operation: "attach-stopped-session",
          cause: new Error(
            `Provider-host session ${requestedSessionOptions.threadId} was stopped.`,
          ),
        });
      }
      return yield* createSession(requestedSessionOptions, adoptionMode);
    }

    cancelIdleShutdown();
    const sessionOptions = admission.sessionOptions;
    let runtime: CodexSessionRuntimeShape | undefined;
    let eventFiber: Fiber.Fiber<void, never> | undefined;
    let sessionScope: Scope.Closeable | undefined;
    let provisionalSession: HostSession | undefined;
    let publishedEventCount = 0;
    let publishedEventWaiter:
      | {
          readonly target: number;
          readonly completed: Deferred.Deferred<void>;
        }
      | undefined;
    const recordPublishedEvent = Effect.gen(function* () {
      publishedEventCount += 1;
      if (publishedEventWaiter && publishedEventCount >= publishedEventWaiter.target) {
        const waiter = publishedEventWaiter;
        publishedEventWaiter = undefined;
        yield* Deferred.succeed(waiter.completed, undefined);
      }
    });
    const waitForPublishedEvents = Effect.fn("CodexProviderHost.waitForPublishedEvents")(function* (
      target: number,
    ) {
      if (publishedEventCount >= target) return;
      const completed = yield* Deferred.make<void>();
      publishedEventWaiter = { target, completed };
      if (publishedEventCount >= target) {
        publishedEventWaiter = undefined;
        yield* Deferred.succeed(completed, undefined);
      }
      yield* Deferred.await(completed);
    });
    return yield* Effect.gen(function* () {
      if (admission.previous) {
        yield* admission.previous.runtime.detach;
        yield* Fiber.join(admission.previous.eventFiber);
        yield* Scope.close(admission.previous.scope, Exit.void).pipe(Effect.ignore);
        if (sessions.get(sessionOptions.threadId) === admission.previous) {
          sessions.delete(sessionOptions.threadId);
        }
      }
      const scope = yield* Scope.make("sequential");
      sessionScope = scope;
      const serviceTier =
        typeof sessionOptions.serviceTier === "string"
          ? (sessionOptions.serviceTier as CodexSessionRuntimeOptions["serviceTier"])
          : undefined;
      const runtimeOptions: CodexSessionRuntimeOptions = {
        threadId: sessionOptions.threadId,
        providerInstanceId: sessionOptions.providerInstanceId,
        cwd: sessionOptions.cwd,
        runtimeMode: sessionOptions.runtimeMode,
        ...(sessionOptions.model ? { model: sessionOptions.model } : {}),
        ...(sessionOptions.resumeCursor ? { resumeCursor: sessionOptions.resumeCursor } : {}),
        ...(sessionOptions.threadConfig ? { threadConfig: sessionOptions.threadConfig } : {}),
        binaryPath: config.codex.binaryPath,
        ...(config.codex.launchArgs ? { launchArgs: config.codex.launchArgs } : {}),
        ...(config.codex.homePath ? { homePath: config.codex.homePath } : {}),
        environment: inheritedEnvironment(),
        appServerSocketPath: config.appServerSocketPath,
        ...(adoptionMode === "resume-only" ? { resumePolicy: "resume-only" as const } : {}),
        ...(serviceTier ? { serviceTier } : {}),
      };
      runtime = yield* makeRuntime(runtimeOptions).pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
        Effect.provideService(Crypto.Crypto, crypto),
      );
      const commandLock = yield* Semaphore.make(1);
      eventFiber = yield* Stream.runForEach(runtime.events, (event) =>
        publishEvent(event).pipe(Effect.tap(() => recordPublishedEvent)),
      ).pipe(Effect.forkIn(hostScope));
      const hostSession = {
        options: sessionOptions,
        creationMode: adoptionMode,
        scope,
        runtime,
        eventFiber,
        commandLock,
        attachments: admission.previous?.attachments ?? new Set<HostConnection>(),
      } satisfies HostSession;
      provisionalSession = hostSession;
      sessions.set(sessionOptions.threadId, hostSession);
      yield* runtime.start();
      yield* waitForPublishedEvents(yield* runtime.emittedEventCount);
      if (admission.previous && hostSession.attachments.size > 0) {
        yield* eventLock.withPermit(
          Effect.gen(function* () {
            if (sessions.get(sessionOptions.threadId) !== hostSession) return;
            const snapshot = yield* hostSession.runtime.getSession;
            for (const connection of hostSession.attachments) {
              if (connection.closed || connection.socket.destroyed) continue;
              writeEnvelope(connection, inventoryEnvelope(sessionOptions.threadId));
              writeEnvelope(
                connection,
                ProviderHostSnapshotEnvelope.make({
                  version: PROVIDER_HOST_PROTOCOL_VERSION,
                  type: "snapshot",
                  threadId: sessionOptions.threadId,
                  cursor: latestCursor,
                  state: providerSessionSnapshotState(snapshot),
                }),
              );
            }
          }),
        );
      }
      yield* Deferred.succeed(admission.deferred, hostSession);
      return hostSession;
    }).pipe(
      Effect.onError(() =>
        Effect.gen(function* () {
          if (provisionalSession && sessions.get(sessionOptions.threadId) === provisionalSession) {
            sessions.delete(sessionOptions.threadId);
          }
          yield* runtime?.close.pipe(Effect.ignore) ?? Effect.void;
          if (eventFiber) {
            yield* Fiber.join(eventFiber).pipe(Effect.ignore);
          }
          if (sessionScope) {
            yield* Scope.close(sessionScope, Exit.void).pipe(Effect.ignore);
          }
        }),
      ),
      Effect.onExit((outcome) =>
        Exit.isFailure(outcome)
          ? Deferred.failCause(admission.deferred, outcome.cause)
          : Effect.void,
      ),
      Effect.ensuring(
        Effect.gen(function* () {
          if (sessionCreations.get(sessionOptions.threadId)?.deferred === admission.deferred) {
            sessionCreations.delete(sessionOptions.threadId);
          }
          yield* Deferred.succeed(admission.settled, undefined);
          scheduleIdleShutdown();
        }),
      ),
    );
  });

  const stopSession: (threadId: ThreadId) => Effect.Effect<void, CodexSessionRuntimeError> =
    Effect.fn("CodexProviderHost.stopSession")(function* (threadId: ThreadId) {
      const completed = yield* Deferred.make<boolean>();
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const admission = yield* restore(
            sessionCreationLock.withPermit(
              Effect.sync(() => {
                const pendingCreation = sessionCreations.get(threadId);
                if (pendingCreation) {
                  return {
                    _tag: "creating" as const,
                    deferred: pendingCreation.deferred,
                    settled: pendingCreation.settled,
                  };
                }
                const pendingStop = sessionStops.get(threadId);
                if (pendingStop) {
                  return { _tag: "stopping" as const, deferred: pendingStop };
                }
                const session = sessions.get(threadId);
                if (!session) {
                  return { _tag: "missing" as const };
                }
                sessionStops.set(threadId, completed);
                return { _tag: "stop" as const, session, completed };
              }),
            ),
          );

          if (admission._tag === "creating") {
            yield* restore(Deferred.await(admission.deferred).pipe(Effect.exit));
            yield* restore(Deferred.await(admission.settled));
            return yield* restore(Effect.suspend(() => stopSession(threadId)));
          }
          if (admission._tag === "stopping") {
            const stopped = yield* restore(Deferred.await(admission.deferred));
            if (stopped) return;
            return yield* restore(Effect.suspend(() => stopSession(threadId)));
          }
          if (admission._tag === "missing") return;

          return yield* restore(
            Effect.gen(function* () {
              yield* admission.session.runtime.interruptTurn();
              yield* admission.session.runtime.close;
              yield* Fiber.join(admission.session.eventFiber);
              sessions.delete(threadId);
              yield* eventLock.withPermit(Effect.sync(() => clearReplayForThread(threadId)));
              yield* Scope.close(admission.session.scope, Exit.void).pipe(Effect.ignore);
              for (const connection of admission.session.attachments) {
                for (const [attachmentId, attachment] of connection.attachments) {
                  if (attachment.threadId === threadId) {
                    connection.attachments.delete(attachmentId);
                  }
                }
              }
              scheduleIdleShutdown();
            }),
          ).pipe(
            Effect.onExit((outcome) =>
              Effect.gen(function* () {
                if (sessionStops.get(threadId) === admission.completed) {
                  sessionStops.delete(threadId);
                }
                yield* Deferred.succeed(admission.completed, Exit.isSuccess(outcome));
              }),
            ),
          );
        }),
      );
    });

  const runCommand = Effect.fn("CodexProviderHost.runCommand")(function* (
    connection: HostConnection,
    command: ProviderHostCommandEnvelope,
  ) {
    return yield* commandResults.execute(
      command.commandId,
      Effect.gen(function* () {
        const operation = command.operation;
        const now = yield* Clock.currentTimeMillis;
        const deadlineAtMs = command.deadlineAtMs ?? now + commandTimeoutMs;
        const remainingMs = Number(deadlineAtMs) - now;
        if (remainingMs <= 0) {
          return ProviderHostCommandResultEnvelope.make({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "commandResult",
            commandId: command.commandId,
            threadId: command.threadId,
            ok: false,
            error: `Provider-host command '${operation}' exceeded its deadline.`,
            errorCode: "deadline-exceeded",
          });
        }
        const attachment = connection.attachments.get(command.attachmentId);
        if (attachment?.threadId !== command.threadId || attachment.clientId !== command.clientId) {
          return ProviderHostCommandResultEnvelope.make({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "commandResult",
            commandId: command.commandId,
            threadId: command.threadId,
            ok: false,
            error: "Provider-host command requires an active matching attachment.",
          });
        }
        let session = sessions.get(command.threadId);
        if (
          operation !== CODEX_PROVIDER_HOST_OPERATIONS.stopSession &&
          sessionStops.has(command.threadId)
        ) {
          return ProviderHostCommandResultEnvelope.make({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "commandResult",
            commandId: command.commandId,
            threadId: command.threadId,
            ok: false,
            error: `Provider-host session ${command.threadId} is being stopped.`,
          });
        }
        if (
          operation !== CODEX_PROVIDER_HOST_OPERATIONS.stopSession &&
          sessionCreations.has(command.threadId)
        ) {
          return ProviderHostCommandResultEnvelope.make({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "commandResult",
            commandId: command.commandId,
            threadId: command.threadId,
            ok: false,
            error: `Provider-host session ${command.threadId} is being replaced.`,
          });
        }
        if (!session && operation !== CODEX_PROVIDER_HOST_OPERATIONS.stopSession) {
          return ProviderHostCommandResultEnvelope.make({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "commandResult",
            commandId: command.commandId,
            threadId: command.threadId,
            ok: false,
            error: `No provider-host session exists for ${command.threadId}.`,
          });
        }
        if (
          session &&
          operation !== CODEX_PROVIDER_HOST_OPERATIONS.stopSession &&
          session.eventFiber.pollUnsafe() !== undefined
        ) {
          const recovery = yield* Effect.exit(createSession(session.options, "resume-only"));
          if (Exit.isFailure(recovery)) {
            return ProviderHostCommandResultEnvelope.make({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "commandResult",
              commandId: command.commandId,
              threadId: command.threadId,
              ok: false,
              error: errorMessage(Cause.squash(recovery.cause)),
            });
          }
          session = recovery.value;
        }
        const activeSession = session as HostSession;
        const execute = Effect.gen(function* () {
          const value = yield* (() => {
            switch (operation) {
              case CODEX_PROVIDER_HOST_OPERATIONS.sendTurn:
                return decodeSendTurn(command.payload).pipe(
                  Effect.flatMap((decoded) =>
                    activeSession.runtime.sendTurn({
                      ...(decoded.input ? { input: decoded.input } : {}),
                      ...(decoded.attachments ? { attachments: decoded.attachments } : {}),
                      ...(decoded.model ? { model: decoded.model } : {}),
                      ...(decoded.interactionMode
                        ? { interactionMode: decoded.interactionMode }
                        : {}),
                      ...(decoded.effort
                        ? {
                            effort:
                              decoded.effort as EffectCodexSchema.V2TurnStartParams__ReasoningEffort,
                          }
                        : {}),
                      ...(typeof decoded.serviceTier === "string"
                        ? {
                            serviceTier: decoded.serviceTier as NonNullable<
                              EffectCodexSchema.V2ThreadStartParams["serviceTier"]
                            >,
                          }
                        : {}),
                    }),
                  ),
                );
              case CODEX_PROVIDER_HOST_OPERATIONS.interruptTurn:
                return decodeInterrupt(command.payload).pipe(
                  Effect.flatMap((decoded) => activeSession.runtime.interruptTurn(decoded.turnId)),
                  Effect.as(null),
                );
              case CODEX_PROVIDER_HOST_OPERATIONS.readThread:
                return activeSession.runtime.readThread;
              case CODEX_PROVIDER_HOST_OPERATIONS.rollbackThread:
                return decodeRollback(command.payload).pipe(
                  Effect.flatMap((decoded) =>
                    activeSession.runtime.rollbackThread(decoded.numTurns),
                  ),
                );
              case CODEX_PROVIDER_HOST_OPERATIONS.respondToRequest:
                return decodeApproval(command.payload).pipe(
                  Effect.flatMap((decoded) =>
                    activeSession.runtime.respondToRequest(decoded.requestId, decoded.decision),
                  ),
                  Effect.as(null),
                );
              case CODEX_PROVIDER_HOST_OPERATIONS.respondToUserInput:
                return decodeUserInput(command.payload).pipe(
                  Effect.flatMap((decoded) =>
                    activeSession.runtime.respondToUserInput(decoded.requestId, decoded.answers),
                  ),
                  Effect.as(null),
                );
              case CODEX_PROVIDER_HOST_OPERATIONS.uploadFeedback:
                return decodeFeedback(command.payload).pipe(
                  Effect.flatMap((decoded) =>
                    activeSession.runtime.uploadFeedback(decoded.reason),
                  ),
                );
              case CODEX_PROVIDER_HOST_OPERATIONS.stopSession:
                return stopSession(command.threadId).pipe(Effect.as(null));
              default:
                return new CodexProviderHostError({
                  operation: "dispatch-command",
                  cause: new Error(`Unknown provider-host operation '${operation}'.`),
                });
            }
          })();
          return value;
        });
        const outcomeOption = yield* Effect.exit(
          operation === CODEX_PROVIDER_HOST_OPERATIONS.interruptTurn ||
            operation === CODEX_PROVIDER_HOST_OPERATIONS.stopSession
            ? execute
            : activeSession.commandLock.withPermit(execute),
        ).pipe(Effect.timeoutOption(Duration.millis(remainingMs)));
        if (Option.isNone(outcomeOption)) {
          return ProviderHostCommandResultEnvelope.make({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "commandResult",
            commandId: command.commandId,
            threadId: command.threadId,
            ok: false,
            error: `Provider-host command '${operation}' exceeded its deadline.`,
            errorCode: "deadline-exceeded",
          });
        }
        const outcome = outcomeOption.value;
        if (Exit.isFailure(outcome)) {
          return ProviderHostCommandResultEnvelope.make({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "commandResult",
            commandId: command.commandId,
            threadId: command.threadId,
            ok: false,
            error: errorMessage(Cause.squash(outcome.cause)),
          });
        }
        return ProviderHostCommandResultEnvelope.make({
          version: PROVIDER_HOST_PROTOCOL_VERSION,
          type: "commandResult",
          commandId: command.commandId,
          threadId: command.threadId,
          ok: true,
          result: outcome.value as Schema.Json,
        });
      }).pipe(Effect.map(boundedCommandResultEnvelope)),
    );
  });

  const isPriorityCommand = (command: ProviderHostCommandEnvelope): boolean =>
    command.operation === CODEX_PROVIDER_HOST_OPERATIONS.interruptTurn ||
    command.operation === CODEX_PROVIDER_HOST_OPERATIONS.stopSession;

  const acquireCommandFiber = (command: ProviderHostCommandEnvelope) =>
    Ref.modify(commandFiberCount, (active) => {
      const limit =
        maxCommandFibers + (isPriorityCommand(command) ? priorityCommandFiberReserve : 0);
      return active < limit ? [true, active + 1] : [false, active];
    });

  const releaseCommandFiber = Ref.update(commandFiberCount, (active) => Math.max(0, active - 1));

  const detachConnection = (connection: HostConnection) => {
    if (connection.closed) return;
    connection.closed = true;
    connections.delete(connection);
    for (const attachment of connection.attachments.values()) {
      sessions.get(attachment.threadId)?.attachments.delete(connection);
    }
    connection.attachments.clear();
    scheduleIdleShutdown();
  };

  const handleEnvelope = Effect.fn("CodexProviderHost.handleEnvelope")(function* (
    connection: HostConnection,
    envelope: ProviderHostClientEnvelopeType,
  ) {
    switch (envelope.type) {
      case "attach": {
        return yield* Effect.gen(function* () {
          if (idleShutdownStarted) {
            return yield* new CodexProviderHostError({
              operation: "attach-idle-provider-host",
              cause: new Error("Provider host idle shutdown has already started."),
            });
          }
          const replayFrom = envelope.replayFrom ?? latestCursor;
          let session: HostSession;
          const existing = sessions.get(envelope.threadId);
          if (envelope.mode === "reuse") {
            if (
              !existing ||
              sessionCreations.has(envelope.threadId) ||
              sessionStops.has(envelope.threadId)
            ) {
              writeEnvelope(connection, inventoryEnvelope(envelope.threadId));
              return yield* new CodexProviderHostError({
                operation: "reuse-session",
                cause: new Error(
                  `No running provider-host session exists for ${envelope.threadId}.`,
                ),
              });
            }
            session = existing;
          } else {
            if (envelope.mode === "adopt" && !canAdoptSessions) {
              return yield* new CodexProviderHostError({
                operation: "adopt-session",
                cause: new Error("This provider host cannot adopt Codex sessions."),
              });
            }
            if (envelope.session === undefined) {
              return yield* new CodexProviderHostError({
                operation: envelope.mode === "adopt" ? "adopt-session" : "create-session",
                cause: new Error(
                  `Provider-host session options are required for ${envelope.mode}.`,
                ),
              });
            }
            const decoded = yield* decodeSessionOptions(envelope.session).pipe(Effect.result);
            if (Result.isFailure(decoded)) {
              connection.socket.destroy(new Error("Invalid provider-host session options."));
              return;
            }
            if (decoded.success.threadId !== envelope.threadId) {
              connection.socket.destroy(new Error("Provider-host attachment thread mismatch."));
              return;
            }
            if (envelope.mode === "adopt" && decoded.success.resumeCursor === undefined) {
              return yield* new CodexProviderHostError({
                operation: "adopt-session",
                cause: new Error(
                  `Cannot adopt provider-host session ${envelope.threadId} without a Codex resume cursor.`,
                ),
              });
            }
            session = yield* createSession(
              decoded.success,
              envelope.mode === "adopt" ? "resume-only" : "normal",
            );
          }
          yield* eventLock.withPermit(
            Effect.gen(function* () {
              if (
                sessions.get(envelope.threadId) !== session ||
                sessionStops.has(envelope.threadId)
              ) {
                return yield* new CodexProviderHostError({
                  operation: "attach-session-transition",
                  cause: new Error(
                    `Provider-host session ${envelope.threadId} changed while attaching.`,
                  ),
                });
              }
              const snapshot = yield* session.runtime.getSession;
              if (
                sessions.get(envelope.threadId) !== session ||
                sessionCreations.has(envelope.threadId) ||
                sessionStops.has(envelope.threadId) ||
                connection.closed ||
                connection.socket.destroyed
              ) {
                if (connection.closed || connection.socket.destroyed) {
                  return;
                }
                return yield* new CodexProviderHostError({
                  operation: "attach-session-transition",
                  cause: new Error(
                    `Provider-host session ${envelope.threadId} changed while attaching.`,
                  ),
                });
              }
              cancelIdleShutdown();
              writeEnvelope(connection, inventoryEnvelope(envelope.threadId));
              session.attachments.add(connection);
              connection.attachments.set(envelope.attachmentId, {
                clientId: envelope.clientId,
                threadId: envelope.threadId,
              });
              const replayTruncated =
                Number(replayFrom) <
                Number(
                  droppedReplayCursorByThread.get(envelope.threadId) ??
                    ProviderHostReplayCursor.make(0),
                );
              for (let index = replayHead; index < replay.length; index += 1) {
                const entry = replay[index];
                if (!entry) continue;
                if (
                  entry.envelope.threadId === envelope.threadId &&
                  Number(entry.envelope.sequence) > Number(replayFrom) &&
                  Number(entry.envelope.sequence) <= Number(latestCursor)
                ) {
                  writeEncodedEnvelope(connection, entry.encoded);
                }
              }
              writeEnvelope(
                connection,
                ProviderHostSnapshotEnvelope.make({
                  version: PROVIDER_HOST_PROTOCOL_VERSION,
                  type: "snapshot",
                  threadId: envelope.threadId,
                  cursor: latestCursor,
                  ...(replayTruncated ? { replayTruncated: true } : {}),
                  state: providerSessionSnapshotState(snapshot),
                }),
              );
            }),
          );
        }).pipe(
          Effect.onError((cause) =>
            Effect.sync(() => {
              const attachmentError = Cause.squash(cause);
              if (isCodexSessionRuntimeThreadIdMissingError(attachmentError)) {
                writeEnvelope(
                  connection,
                  ProviderHostAttachErrorEnvelope.make({
                    version: PROVIDER_HOST_PROTOCOL_VERSION,
                    type: "attachError",
                    threadId: envelope.threadId,
                    errorCode: "thread-id-missing",
                    error: attachmentError.message,
                  }),
                );
                connection.socket.end();
                return;
              }
              connection.socket.destroy(new Error("Provider-host attachment failed."));
            }),
          ),
        );
      }
      case "detach": {
        const attachment = connection.attachments.get(envelope.attachmentId);
        if (
          attachment?.threadId !== envelope.threadId ||
          attachment.clientId !== envelope.clientId
        ) {
          return;
        }
        connection.attachments.delete(envelope.attachmentId);
        const session = sessions.get(envelope.threadId);
        if (
          session &&
          !Array.from(connection.attachments.values()).some(
            (candidate) => candidate.threadId === envelope.threadId,
          )
        ) {
          session.attachments.delete(connection);
        }
        scheduleIdleShutdown();
        return;
      }
      case "command": {
        if (!(yield* acquireCommandFiber(envelope))) {
          const overloaded = yield* commandResults.execute(
            envelope.commandId,
            Effect.succeed(
              ProviderHostCommandResultEnvelope.make({
                version: PROVIDER_HOST_PROTOCOL_VERSION,
                type: "commandResult",
                commandId: envelope.commandId,
                threadId: envelope.threadId,
                ok: false,
                error: "Provider-host command admission is at capacity.",
              }),
            ),
          );
          writeEnvelope(connection, overloaded);
          return;
        }
        yield* runCommand(connection, envelope).pipe(
          Effect.tap((result) =>
            Effect.sync(() => {
              writeEnvelope(connection, result);
            }),
          ),
          Effect.ensuring(releaseCommandFiber),
          Effect.forkIn(hostScope),
        );
      }
    }
  });

  const server = NodeNet.createServer((socket) => {
    socket.setNoDelay(true);
    const connection: HostConnection = {
      socket,
      attachments: new Map(),
      processing: Promise.resolve(),
      pendingEnvelopes: 0,
      pendingEnvelopeBytes: 0,
      closed: false,
      lineFramer: makeProviderHostLineFramer(MAX_INBOUND_LINE_BYTES),
    };
    connections.add(connection);
    writeEnvelope(
      connection,
      ProviderHostHelloEnvelope.make({
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        type: "hello",
        providerInstanceId: ProviderInstanceId.make(config.providerInstanceId),
        buildFingerprint,
        generationFingerprint,
        appServerMode: config.appServerMode,
        canAdoptSessions,
        hostProcess,
        startedAt,
        latestCursor,
      }),
    );
    writeEnvelope(
      connection,
      ProviderHostHealthEnvelope.make({
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        type: "health",
        status: "healthy",
        buildFingerprint,
        generationFingerprint,
        appServerMode: config.appServerMode,
        canAdoptSessions,
        hostProcess,
        codexChildProcess: appServerProvenance.appServer.process,
        latestCursor,
      }),
    );
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => {
      const framed = connection.lineFramer.push(chunk);
      if (framed.overflowed) {
        socket.destroy(new Error("Provider-host request exceeded the maximum frame size."));
        return;
      }
      for (const framedLine of framed.lines) {
        const lineBytes = Buffer.byteLength(framedLine);
        const line = framedLine.trim();
        if (!line) continue;
        if (
          connection.pendingEnvelopes >= maxPendingEnvelopesPerConnection ||
          lineBytes > maxPendingEnvelopeBytesPerConnection - connection.pendingEnvelopeBytes
        ) {
          socket.destroy(new Error("Provider-host request queue exceeded its bounded capacity."));
          return;
        }
        const decoded = decodeClientEnvelope(line);
        if (Result.isFailure(decoded)) {
          socket.destroy(new Error("Invalid provider-host protocol envelope."));
          return;
        }
        connection.pendingEnvelopes += 1;
        connection.pendingEnvelopeBytes += lineBytes;
        connection.processing = connection.processing
          .then(() =>
            runPromise(
              handleEnvelope(connection, decoded.success).pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning("codex.provider-host.request-failed", {
                    cause: Cause.pretty(cause),
                  }),
                ),
              ),
            ),
          )
          .finally(() => {
            connection.pendingEnvelopes = Math.max(0, connection.pendingEnvelopes - 1);
            connection.pendingEnvelopeBytes = Math.max(
              0,
              connection.pendingEnvelopeBytes - lineBytes,
            );
          });
      }
    });
    socket.on("close", () => detachConnection(connection));
    socket.on("error", () => detachConnection(connection));
  });

  let controlSocketIdentity: SocketPathIdentity | undefined;
  let transportShutdown: Promise<void> | undefined;
  const shutdownTransport = () => {
    if (!transportShutdown) {
      transportShutdown = (async () => {
        for (const connection of connections) {
          connection.socket.destroy();
        }
        if (server.listening) {
          await new Promise<void>((resolve) => server.close(() => resolve()));
        }
        if (controlSocketIdentity) {
          await removeSocketPathIfOwned(config.controlSocketPath, controlSocketIdentity);
        }
      })();
    }
    return transportShutdown;
  };

  yield* Effect.addFinalizer(() => Effect.promise(shutdownTransport));
  yield* Effect.tryPromise({
    try: async () => {
      await NodeFSP.mkdir(NodePath.dirname(config.controlSocketPath), {
        recursive: true,
        mode: 0o700,
      });
      await NodeFSP.chmod(NodePath.dirname(config.controlSocketPath), 0o700);
      await NodeFSP.rm(config.controlSocketPath, { force: true });
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(config.controlSocketPath, () => {
          server.removeListener("error", reject);
          resolve();
        });
      });
      await NodeFSP.chmod(config.controlSocketPath, 0o600);
      controlSocketIdentity = await readSocketPathIdentity(config.controlSocketPath);
      if (!controlSocketIdentity) {
        throw new Error("Provider-host control socket disappeared after startup.");
      }
      scheduleIdleShutdown();
    },
    catch: (cause) =>
      new CodexProviderHostError({
        operation: "listen",
        cause,
      }),
  });
  yield* releaseStartupLock;

  yield* Effect.logInfo("codex.provider-host.ready", {
    providerInstanceId: config.providerInstanceId,
    controlSocketPath: config.controlSocketPath,
    appServerSocketPath: config.appServerSocketPath,
    pid: process.pid,
  });

  const exit = yield* Deferred.await(hostExit);
  yield* Effect.logInfo("codex.provider-host.stopped", {
    providerInstanceId: config.providerInstanceId,
    reason: exit._tag,
  });
  yield* Effect.promise(shutdownTransport);
  yield* Effect.forEach(
    Array.from(sessions.values()),
    (session) =>
      session.runtime.detach.pipe(
        Effect.andThen(Fiber.join(session.eventFiber)),
        Effect.ensuring(Scope.close(session.scope, Exit.void).pipe(Effect.ignore)),
        Effect.ignore,
      ),
    { concurrency: "unbounded", discard: true },
  );
});

export const __testing = {
  appServerMonitorFailureThreshold: APP_SERVER_MONITOR_FAILURE_THRESHOLD,
  appServerMonitorInitialDelayMs: APP_SERVER_MONITOR_INITIAL_DELAY_MS,
  appServerMonitorMaxDelayMs: APP_SERVER_MONITOR_MAX_DELAY_MS,
  nextAppServerMonitorState,
  readSocketPathIdentity,
  removeSocketPathIfOwned,
  maxOutboundFrameBytes: MAX_OUTBOUND_FRAME_BYTES,
  zeroAttachmentIdleTimeoutMs: ZERO_ATTACHMENT_IDLE_TIMEOUT_MS,
};
