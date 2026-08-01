// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalDate:off - Process identity is captured at startup.
// @effect-diagnostics globalTimers:off - Child and socket readiness use bounded timers.
// @effect-diagnostics preferSchemaOverJson:off - NDJSON framing encodes validated envelopes.
import {
  ProviderEvent,
  ProviderInstanceId,
  ThreadId,
  type ResourceTelemetryProcessIdentity,
} from "@t3tools/contracts";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";
import type * as EffectCodexSchema from "effect-codex-app-server/schema";

import { expandHomePath } from "../../pathExpansion.ts";
import { codexAppServerArgs } from "../Layers/codexLaunchArgs.ts";
import {
  CodexResumeCursorSchema,
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
} from "./ProviderHostManifest.ts";
import {
  PROVIDER_HOST_PROTOCOL_VERSION,
  ProviderHostAttachmentId,
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
const MAX_PENDING_ENVELOPES_PER_CONNECTION = 256;
const MAX_PENDING_ENVELOPE_BYTES_PER_CONNECTION = MAX_INBOUND_LINE_BYTES * 2;
const ZERO_SESSION_IDLE_TIMEOUT_MS = 60_000;

const decodeClientEnvelope = Schema.decodeUnknownResult(
  Schema.fromJsonString(ProviderHostClientEnvelope),
);
const decodeSessionOptions = Schema.decodeUnknownEffect(CodexProviderHostSessionOptions);
const decodeSendTurn = Schema.decodeUnknownEffect(CodexProviderHostSendTurnPayload);
const decodeInterrupt = Schema.decodeUnknownEffect(CodexProviderHostInterruptPayload);
const decodeRollback = Schema.decodeUnknownEffect(CodexProviderHostRollbackPayload);
const decodeApproval = Schema.decodeUnknownEffect(CodexProviderHostApprovalPayload);
const decodeUserInput = Schema.decodeUnknownEffect(CodexProviderHostUserInputPayload);
const isCodexResumeCursor = Schema.is(CodexResumeCursorSchema);

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
  readonly scope: Scope.Closeable;
  readonly runtime: CodexSessionRuntimeShape;
  readonly eventFiber: Fiber.Fiber<void, never>;
  readonly commandLock: Semaphore.Semaphore;
  readonly attachments: Set<HostConnection>;
}

function sessionOptionsRequireReplacement(
  current: CodexProviderHostSessionOptions,
  requested: CodexProviderHostSessionOptions,
): boolean {
  return (
    current.providerInstanceId !== requested.providerInstanceId ||
    current.cwd !== requested.cwd ||
    current.runtimeMode !== requested.runtimeMode ||
    current.model !== requested.model ||
    current.serviceTier !== requested.serviceTier ||
    !NodeUtil.isDeepStrictEqual(current.threadConfig, requested.threadConfig)
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
  readonly priorityCommandFiberReserve?: number;
  readonly maxPendingEnvelopesPerConnection?: number;
  readonly maxPendingEnvelopeBytesPerConnection?: number;
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

function childProcessIdentity(
  child: NodeChildProcess.ChildProcess,
): ResourceTelemetryProcessIdentity {
  return {
    pid: child.pid ?? 0,
    startTimeMs: Date.now(),
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
    const hasExited = () =>
      child.exitCode !== null || child.signalCode !== null || child.pid === undefined;
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
    }, 2_000);

    try {
      if (!child.kill("SIGTERM") && hasExited()) {
        finish();
      }
    } catch {
      finish();
    }
  });
}

interface CodexChildExit {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly cause?: Error;
}

function waitForCodexChildExit(
  child: NodeChildProcess.ChildProcess,
): Effect.Effect<CodexChildExit> {
  return Effect.callback((resume) => {
    if (child.exitCode !== null || child.signalCode !== null) {
      resume(
        Effect.succeed({
          code: child.exitCode,
          signal: child.signalCode,
        }),
      );
      return;
    }

    const cleanup = () => {
      child.removeListener("exit", onExit);
      child.removeListener("error", onError);
    };
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();
      resume(Effect.succeed({ code, signal }));
    };
    const onError = (cause: Error) => {
      cleanup();
      resume(
        Effect.succeed({
          code: child.exitCode,
          signal: child.signalCode,
          cause,
        }),
      );
    };

    child.once("exit", onExit);
    child.once("error", onError);
    return Effect.sync(cleanup);
  });
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
  await NodeFSP.rm(config.appServerSocketPath, { force: true });
  return NodeChildProcess.spawn(spawnCommand.command, [...spawnCommand.args], {
    cwd: config.codex.cwd,
    env,
    shell: spawnCommand.shell,
    stdio: ["ignore", "inherit", "inherit"],
  });
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
    Deferred.Deferred<HostSession, CodexSessionRuntimeError>
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
  const idleTimeoutMs = options?.idleTimeoutMs ?? ZERO_SESSION_IDLE_TIMEOUT_MS;
  const maxCommandFibers = options?.maxCommandFibers ?? MAX_COMMAND_FIBERS;
  const priorityCommandFiberReserve =
    options?.priorityCommandFiberReserve ?? PRIORITY_COMMAND_FIBER_RESERVE;
  const maxPendingEnvelopesPerConnection =
    options?.maxPendingEnvelopesPerConnection ?? MAX_PENDING_ENVELOPES_PER_CONNECTION;
  const maxPendingEnvelopeBytesPerConnection =
    options?.maxPendingEnvelopeBytesPerConnection ?? MAX_PENDING_ENVELOPE_BYTES_PER_CONNECTION;
  let latestCursor = ProviderHostReplayCursor.make(0);
  const hostProcess = currentProcessIdentity();
  const generationFingerprint = ProviderHostGenerationFingerprint.make(
    `${config.generationFingerprint}:${hostProcess.pid}:${hostProcess.startTimeMs}`,
  );
  const startedAt = yield* DateTime.now;
  const spawnCodex = options?.spawnCodex ?? defaultSpawnCodex;
  const codexChild = yield* Effect.tryPromise({
    try: () => spawnCodex(config),
    catch: (cause) =>
      new CodexProviderHostError({
        operation: "spawn-codex",
        cause,
      }),
  });
  yield* Effect.addFinalizer(() => Effect.promise(() => terminateChild(codexChild)));
  let idleTimer: ReturnType<typeof setTimeout> | undefined;
  let idleShutdownStarted = false;
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
      sessions.size > 0 ||
      sessionCreations.size > 0
    ) {
      return;
    }
    idleTimer = setTimeout(() => {
      idleTimer = undefined;
      if (sessions.size > 0 || sessionCreations.size > 0 || idleShutdownStarted) {
        return;
      }
      idleShutdownStarted = true;
      void terminateChild(codexChild);
    }, idleTimeoutMs);
    idleTimer.unref?.();
  };
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      cancelIdleShutdown();
    }),
  );
  const childExitFiber = yield* waitForCodexChildExit(codexChild).pipe(Effect.forkIn(hostScope));
  const ready = yield* Effect.promise(() => waitForCodexSocket(config.appServerSocketPath));
  if (!ready) {
    yield* Effect.promise(() => terminateChild(codexChild));
    return yield* new CodexProviderHostError({
      operation: "wait-for-codex",
      cause: new Error(`Codex app-server did not become ready at ${config.appServerSocketPath}.`),
    });
  }

  const codexVersion = yield* Effect.promise(() => readCodexVersion(config));
  yield* persistProviderHostManifest({
    path: config.manifestPath,
    manifest: {
      schemaVersion: PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
      protocolVersion: PROVIDER_HOST_PROTOCOL_VERSION,
      generationFingerprint,
      hostProcess,
      socketPath: config.controlSocketPath,
      codex: {
        childProcess: childProcessIdentity(codexChild),
        resolvedBinary: config.codex.binaryPath,
        version: codexVersion,
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
      startedAt,
    },
  });

  const inventoryEnvelope = (): ProviderHostInventoryEnvelope =>
    ProviderHostInventoryEnvelope.make({
      version: PROVIDER_HOST_PROTOCOL_VERSION,
      type: "inventory",
      threads: Array.from(sessions.entries()).map(([threadId, session]) => ({
        threadId,
        status: "unknown",
        attachmentCount: session.attachments.size,
        cursor: latestCursor,
      })),
    });

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
  ) => Effect.Effect<HostSession, CodexSessionRuntimeError | CodexProviderHostError> = Effect.fn(
    "CodexProviderHost.createSession",
  )(function* (requestedSessionOptions: CodexProviderHostSessionOptions) {
    const admission = yield* sessionCreationLock.withPermit(
      Effect.gen(function* () {
        const stopping = sessionStops.get(requestedSessionOptions.threadId);
        if (stopping) {
          return { _tag: "stopping" as const, deferred: stopping };
        }
        const pending = sessionCreations.get(requestedSessionOptions.threadId);
        if (pending) {
          return { _tag: "pending" as const, deferred: pending };
        }
        const existing = sessions.get(requestedSessionOptions.threadId);
        if (existing) {
          const snapshot = yield* existing.runtime.getSession;
          if (
            snapshot.status !== "error" &&
            snapshot.status !== "closed" &&
            !sessionOptionsRequireReplacement(existing.options, requestedSessionOptions)
          ) {
            return { _tag: "existing" as const, session: existing };
          }
          const deferred = yield* Deferred.make<HostSession, CodexSessionRuntimeError>();
          sessionCreations.set(requestedSessionOptions.threadId, deferred);
          return {
            _tag: "create" as const,
            deferred,
            previous: existing,
            sessionOptions: {
              ...requestedSessionOptions,
              ...(requestedSessionOptions.resumeCursor === undefined &&
              isCodexResumeCursor(snapshot.resumeCursor)
                ? { resumeCursor: snapshot.resumeCursor }
                : {}),
            } satisfies CodexProviderHostSessionOptions,
          };
        }
        const deferred = yield* Deferred.make<HostSession, CodexSessionRuntimeError>();
        sessionCreations.set(requestedSessionOptions.threadId, deferred);
        return {
          _tag: "create" as const,
          deferred,
          previous: undefined,
          sessionOptions: requestedSessionOptions,
        };
      }),
    );
    if (admission._tag === "existing") return admission.session;
    if (admission._tag === "pending") return yield* Deferred.await(admission.deferred);
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
      return yield* createSession(requestedSessionOptions);
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
      yield* Deferred.succeed(admission.deferred, hostSession);
      return hostSession;
    }).pipe(
      Effect.tapError((cause) => Deferred.fail(admission.deferred, cause)),
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
      Effect.ensuring(
        Effect.sync(() => {
          if (sessionCreations.get(sessionOptions.threadId) === admission.deferred) {
            sessionCreations.delete(sessionOptions.threadId);
          }
          scheduleIdleShutdown();
        }),
      ),
    );
  });

  const stopSession: (threadId: ThreadId) => Effect.Effect<void, CodexSessionRuntimeError> =
    Effect.fn("CodexProviderHost.stopSession")(function* (threadId: ThreadId) {
      const admission = yield* sessionCreationLock.withPermit(
        Effect.gen(function* () {
          const pendingCreation = sessionCreations.get(threadId);
          if (pendingCreation) {
            return { _tag: "creating" as const, deferred: pendingCreation };
          }
          const pendingStop = sessionStops.get(threadId);
          if (pendingStop) {
            return { _tag: "stopping" as const, deferred: pendingStop };
          }
          const session = sessions.get(threadId);
          if (!session) {
            return { _tag: "missing" as const };
          }
          const completed = yield* Deferred.make<boolean>();
          sessionStops.set(threadId, completed);
          return { _tag: "stop" as const, session, completed };
        }),
      );

      if (admission._tag === "creating") {
        yield* Deferred.await(admission.deferred);
        return yield* Effect.suspend(() => stopSession(threadId));
      }
      if (admission._tag === "stopping") {
        const stopped = yield* Deferred.await(admission.deferred);
        if (stopped) return;
        return yield* Effect.suspend(() => stopSession(threadId));
      }
      if (admission._tag === "missing") return;

      const outcome = yield* Effect.exit(
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
      );

      yield* sessionCreationLock.withPermit(
        Effect.gen(function* () {
          if (sessionStops.get(threadId) === admission.completed) {
            sessionStops.delete(threadId);
          }
          yield* Deferred.succeed(admission.completed, Exit.isSuccess(outcome));
        }),
      );
      if (Exit.isFailure(outcome)) {
        return yield* Effect.failCause(outcome.cause);
      }
    });

  const runCommand = Effect.fn("CodexProviderHost.runCommand")(function* (
    connection: HostConnection,
    command: ProviderHostCommandEnvelope,
  ) {
    return yield* commandResults.execute(
      command.commandId,
      Effect.gen(function* () {
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
        const operation = command.operation;
        const session = sessions.get(command.threadId);
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
        const outcome = yield* Effect.exit(
          operation === CODEX_PROVIDER_HOST_OPERATIONS.interruptTurn ||
            operation === CODEX_PROVIDER_HOST_OPERATIONS.stopSession
            ? execute
            : activeSession.commandLock.withPermit(execute),
        );
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
  };

  const handleEnvelope = Effect.fn("CodexProviderHost.handleEnvelope")(function* (
    connection: HostConnection,
    envelope: ProviderHostClientEnvelopeType,
  ) {
    switch (envelope.type) {
      case "attach": {
        return yield* Effect.gen(function* () {
          const replayFrom = envelope.replayFrom ?? latestCursor;
          let session: HostSession;
          if (envelope.session === undefined) {
            const existing = sessions.get(envelope.threadId);
            if (
              !existing ||
              sessionCreations.has(envelope.threadId) ||
              sessionStops.has(envelope.threadId)
            ) {
              return yield* new CodexProviderHostError({
                operation: "attach-existing-session",
                cause: new Error(
                  `No running provider-host session exists for ${envelope.threadId}.`,
                ),
              });
            }
            session = existing;
          } else {
            const decoded = yield* decodeSessionOptions(envelope.session).pipe(Effect.result);
            if (Result.isFailure(decoded)) {
              connection.socket.destroy(new Error("Invalid provider-host session options."));
              return;
            }
            if (decoded.success.threadId !== envelope.threadId) {
              connection.socket.destroy(new Error("Provider-host attachment thread mismatch."));
              return;
            }
            session = yield* createSession(decoded.success);
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
                  state: snapshot,
                }),
              );
            }),
          );
        }).pipe(
          Effect.onError(() =>
            Effect.sync(() => {
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
        generationFingerprint,
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
        generationFingerprint,
        hostProcess,
        codexChildProcess: childProcessIdentity(codexChild),
        latestCursor,
      }),
    );
    writeEnvelope(connection, inventoryEnvelope());

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
                    cause,
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
        await NodeFSP.rm(config.controlSocketPath, { force: true });
        await NodeFSP.rm(config.appServerSocketPath, { force: true });
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
      scheduleIdleShutdown();
    },
    catch: (cause) =>
      new CodexProviderHostError({
        operation: "listen",
        cause,
      }),
  });

  yield* Effect.logInfo("codex.provider-host.ready", {
    providerInstanceId: config.providerInstanceId,
    controlSocketPath: config.controlSocketPath,
    appServerSocketPath: config.appServerSocketPath,
    pid: process.pid,
  });

  const childExit = yield* Fiber.join(childExitFiber);
  yield* Effect.logWarning("codex.provider-host.codex-exited", {
    providerInstanceId: config.providerInstanceId,
    code: childExit.code,
    signal: childExit.signal,
    ...(childExit.cause ? { cause: childExit.cause } : {}),
  });
  yield* Effect.promise(shutdownTransport);
});

export const __testing = {
  maxOutboundFrameBytes: MAX_OUTBOUND_FRAME_BYTES,
  terminateChild,
  zeroSessionIdleTimeoutMs: ZERO_SESSION_IDLE_TIMEOUT_MS,
};
