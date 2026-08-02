// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalDate:off - Command deadlines are wall-clock timestamps shared with the detached host.
// @effect-diagnostics globalTimers:off - Socket timeouts use bounded timers.
// @effect-diagnostics preferSchemaOverJson:off - NDJSON framing encodes already validated envelopes.
import {
  CommandId,
  EventId,
  ProviderDriverKind,
  ProviderEvent,
  ProviderInstanceId,
  ProviderSession,
  ProviderTurnStartResult,
  TurnId,
} from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";
import * as NodeNet from "node:net";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as CodexErrors from "effect-codex-app-server/errors";

import {
  CodexSessionRuntimeMutationAmbiguousError,
  CodexSessionRuntimeThreadIdMissingError,
  type CodexSessionRuntimeError,
  type CodexSessionRuntimeOptions,
  type CodexSessionRuntimeShape,
  type CodexThreadSnapshot,
} from "../Layers/CodexSessionRuntime.ts";
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
  PROVIDER_HOST_PROTOCOL_VERSION,
  ProviderHostAttachmentId,
  type ProviderHostAttachMode,
  ProviderHostClientId,
  ProviderHostCommandDeadlineMs,
  ProviderHostCommandEnvelope,
  ProviderHostDetachEnvelope,
  ProviderHostReplayCursor,
  ProviderHostServerEnvelope,
  type ProviderHostCommandResultEnvelope,
  type ProviderHostGenerationFingerprint,
} from "./ProviderHostProtocol.ts";
import {
  makeProviderHostLineFramer,
  type ProviderHostLineFramer,
} from "./ProviderHostLineFramer.ts";

const CONNECT_TIMEOUT_MS = 5_000;
const RECONNECT_WINDOW_MS = 10_000;
const COMMAND_TIMEOUT_MS = 120_000;
const COMMAND_CLIENT_GRACE_MS = 5_000;
const DETACH_CLOSE_TIMEOUT_MS = 1_000;
const MAX_INBOUND_LINE_BYTES = 32 * 1024 * 1024;
const MAX_PENDING_PROVIDER_EVENTS = 4_096;
const MAX_PENDING_PROVIDER_EVENT_BYTES = 16 * 1024 * 1024;

function commandDeadlineAtMs(timeoutMs: number, nowMs = Date.now()): ProviderHostCommandDeadlineMs {
  return ProviderHostCommandDeadlineMs.make(nowMs + timeoutMs);
}

function commandResponseWaitMs(
  deadlineAtMs: ProviderHostCommandDeadlineMs,
  graceMs: number,
  nowMs = Date.now(),
): number {
  return Math.max(0, Number(deadlineAtMs) - nowMs + graceMs);
}

const decodeServerEnvelope = Schema.decodeUnknownSync(
  Schema.fromJsonString(ProviderHostServerEnvelope),
);
const decodeCommandPayload = Schema.decodeUnknownEffect(Schema.Json);
const isProviderSession = Schema.is(ProviderSession);
const isProviderEvent = Schema.is(ProviderEvent);
const isProviderTurnStartResult = Schema.is(ProviderTurnStartResult);
const isCodexSessionRuntimeThreadIdMissingError = Schema.is(
  CodexSessionRuntimeThreadIdMissingError,
);
const CodexThreadSnapshotSchema = Schema.Struct({
  threadId: Schema.String,
  turns: Schema.Array(
    Schema.Struct({
      id: TurnId,
      items: Schema.Array(Schema.Unknown),
    }),
  ),
});
const isCodexThreadSnapshot = Schema.is(CodexThreadSnapshotSchema);
type CodexProviderHostOperation =
  (typeof CODEX_PROVIDER_HOST_OPERATIONS)[keyof typeof CODEX_PROVIDER_HOST_OPERATIONS];

interface PendingCommand {
  readonly resolve: (result: ProviderHostCommandResultEnvelope) => void;
  readonly reject: (cause: Error) => void;
  readonly timer: ReturnType<typeof setTimeout>;
}

interface HostConnection {
  readonly socket: NodeNet.Socket;
  readonly snapshot: Promise<ProviderSession>;
  readonly pending: Map<CommandId, PendingCommand>;
  readonly replayEvents: Array<{
    readonly sequence: number;
    readonly event: ProviderEvent;
    readonly retainedBytes: number;
  }>;
  readonly transportClosed: Promise<void>;
  generationFingerprint?: ProviderHostGenerationFingerprint;
  canAdoptSessions: boolean;
  replayTruncated: boolean;
  snapshotReceived: boolean;
  closed: boolean;
  readonly lineFramer: ProviderHostLineFramer;
}

interface BufferedProviderEvent {
  readonly event: ProviderEvent;
  readonly retainedBytes: number;
}

function transportError(
  operation: string,
  cause: unknown,
): CodexErrors.CodexAppServerTransportError {
  const detail = cause instanceof Error ? cause.message : String(cause);
  return new CodexErrors.CodexAppServerTransportError({
    operation: "read-input-stream",
    cause: new Error(`${operation}: ${detail}`, cause instanceof Error ? { cause } : undefined),
  });
}

function toSessionOptions(options: CodexSessionRuntimeOptions): CodexProviderHostSessionOptions {
  return CodexProviderHostSessionOptions.make({
    threadId: options.threadId,
    providerInstanceId: options.providerInstanceId ?? ProviderInstanceId.make("codex"),
    cwd: options.cwd,
    runtimeMode: options.runtimeMode,
    ...(options.model ? { model: options.model } : {}),
    ...(options.serviceTier ? { serviceTier: options.serviceTier } : {}),
    ...(options.resumeCursor ? { resumeCursor: options.resumeCursor } : {}),
    ...(options.threadConfig ? { threadConfig: options.threadConfig } : {}),
  });
}

export const makeCodexProviderHostRuntime = Effect.fn("makeCodexProviderHostRuntime")(
  function* (input: {
    readonly controlSocketPath: string;
    readonly options: CodexSessionRuntimeOptions;
    readonly sessionMode?: "create" | "reuse" | "adopt";
    readonly commandTimeoutMs?: number;
    readonly commandClientGraceMs?: number;
    readonly detachCloseTimeoutMs?: number;
    readonly maxPendingProviderEvents?: number;
    readonly maxPendingProviderEventBytes?: number;
  }): Effect.fn.Return<CodexSessionRuntimeShape, never, Scope.Scope> {
    const events = yield* Queue.unbounded<BufferedProviderEvent, Cause.Done<void>>();
    const initialNow = DateTime.formatIso(yield* DateTime.now);
    const sessionRef = yield* Ref.make<ProviderSession>({
      provider: ProviderDriverKind.make("codex"),
      ...(input.options.providerInstanceId
        ? { providerInstanceId: input.options.providerInstanceId }
        : {}),
      threadId: input.options.threadId,
      status: "connecting",
      runtimeMode: input.options.runtimeMode,
      cwd: input.options.cwd,
      ...(input.options.model ? { model: input.options.model } : {}),
      ...(input.options.resumeCursor ? { resumeCursor: input.options.resumeCursor } : {}),
      createdAt: initialNow,
      updatedAt: initialNow,
    });
    const clientId = ProviderHostClientId.make(`t3-${process.pid}-${NodeCrypto.randomUUID()}`);
    const attachmentId = ProviderHostAttachmentId.make(NodeCrypto.randomUUID());
    const sessionOptions = toSessionOptions(input.options);
    const requestedSessionMode = input.sessionMode ?? "create";
    const runtimeContext = yield* Effect.context<never>();
    const runFork = Effect.runForkWith(runtimeContext);
    const runPromise = Effect.runPromiseWith(runtimeContext);
    const sockets = new Set<NodeNet.Socket>();
    let connection: HostConnection | undefined;
    let connecting: Promise<HostConnection> | undefined;
    let closing = false;
    let started = false;
    let attachmentAttempted = false;
    let reconnecting = false;
    let replayCursor = ProviderHostReplayCursor.make(0);
    let generationFingerprint: ProviderHostGenerationFingerprint | undefined;
    let latestResumeCursor = sessionOptions.resumeCursor;
    let runtimeRecoveryRequired = false;
    let processing = Promise.resolve();
    let retainedProviderEvents = 0;
    let retainedProviderEventBytes = 0;
    const capacityWaiters = new Set<() => void>();
    const detachCloseTimeoutMs = Math.max(1, input.detachCloseTimeoutMs ?? DETACH_CLOSE_TIMEOUT_MS);
    const commandTimeoutMs = Math.max(1, input.commandTimeoutMs ?? COMMAND_TIMEOUT_MS);
    const commandClientGraceMs = Math.max(0, input.commandClientGraceMs ?? COMMAND_CLIENT_GRACE_MS);
    const maxPendingProviderEvents = Math.max(
      1,
      input.maxPendingProviderEvents ?? MAX_PENDING_PROVIDER_EVENTS,
    );
    const maxPendingProviderEventBytes = Math.max(
      1,
      input.maxPendingProviderEventBytes ?? MAX_PENDING_PROVIDER_EVENT_BYTES,
    );

    const providerEventsDrained = () => retainedProviderEvents === 0;

    const wakeCapacityWaiters = () => {
      if (!closing && !providerEventsDrained()) return;
      for (const resolve of capacityWaiters) resolve();
      capacityWaiters.clear();
    };

    const reserveProviderEvent = (retainedBytes: number): boolean => {
      if (
        retainedProviderEvents >= maxPendingProviderEvents ||
        retainedBytes > maxPendingProviderEventBytes - retainedProviderEventBytes
      ) {
        return false;
      }
      retainedProviderEvents += 1;
      retainedProviderEventBytes += retainedBytes;
      return true;
    };

    const releaseProviderEvent = (retainedBytes: number) => {
      retainedProviderEvents = Math.max(0, retainedProviderEvents - 1);
      retainedProviderEventBytes = Math.max(0, retainedProviderEventBytes - retainedBytes);
      wakeCapacityWaiters();
    };

    const waitForProviderEventDrain = (): Promise<void> =>
      new Promise((resolve) => {
        if (closing || providerEventsDrained()) {
          resolve();
          return;
        }
        capacityWaiters.add(resolve);
      });

    const offerEvent = (event: ProviderEvent, retainedBytes = 0) =>
      Queue.offer(events, { event, retainedBytes });

    const appendProcessing = (current: HostConnection, evaluate: () => Promise<void>) => {
      processing = processing.then(evaluate).catch((cause) => {
        if (!current.socket.destroyed) {
          current.socket.destroy(
            cause instanceof Error
              ? cause
              : new Error("Failed to process a provider-host event.", { cause }),
          );
        }
      });
    };

    const updateSession = Effect.fn("CodexProviderHostClient.updateSession")(function* (
      updates: Partial<ProviderSession>,
    ) {
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      yield* Ref.update(sessionRef, (current) => ({
        ...current,
        ...updates,
        updatedAt,
      }));
    });

    const emitConnectionEvent = Effect.fn("CodexProviderHostClient.emitConnectionEvent")(function* (
      method: string,
      message: string,
    ) {
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      return yield* offerEvent({
        id: EventId.make(NodeCrypto.randomUUID()),
        provider: ProviderDriverKind.make("codex"),
        ...(input.options.providerInstanceId
          ? { providerInstanceId: input.options.providerInstanceId }
          : {}),
        threadId: input.options.threadId,
        kind: "session",
        method,
        message,
        createdAt: updatedAt,
      });
    }, Effect.asVoid);

    const emitReattachedEvent = Effect.fn("CodexProviderHostClient.emitReattachedEvent")(function* (
      snapshot: ProviderSession,
    ) {
      const createdAt = DateTime.formatIso(yield* DateTime.now);
      return yield* offerEvent({
        id: EventId.make(NodeCrypto.randomUUID()),
        provider: ProviderDriverKind.make("codex"),
        ...(input.options.providerInstanceId
          ? { providerInstanceId: input.options.providerInstanceId }
          : {}),
        threadId: input.options.threadId,
        kind: "session",
        method: "session/reattached",
        message: "T3 reattached to the independent Codex execution.",
        ...(snapshot.activeTurnId ? { turnId: snapshot.activeTurnId } : {}),
        payload: {
          status: snapshot.status,
          ...(snapshot.activeTurnId ? { activeTurnId: snapshot.activeTurnId } : {}),
          ...(snapshot.lastError ? { lastError: snapshot.lastError } : {}),
        },
        createdAt,
      });
    }, Effect.asVoid);

    const emitReplayTruncatedEvent = emitConnectionEvent(
      "session/replay-truncated",
      "T3 reattached after the provider-host replay window was truncated. Codex execution continued, but some intermediate output may need transcript reconciliation.",
    );

    const applyProviderEvent = (event: ProviderEvent) => {
      switch (event.method) {
        case "turn/started":
          return updateSession({
            status: "running",
            ...(event.turnId ? { activeTurnId: event.turnId } : {}),
          });
        case "turn/completed":
        case "turn/aborted":
          return updateSession({ status: "ready", activeTurnId: undefined });
        case "session/closed":
          return updateSession({ status: "closed", activeTurnId: undefined });
        case "session/exited":
        case "session/disconnected":
          runtimeRecoveryRequired = true;
          return updateSession({
            status: "error",
            activeTurnId: undefined,
            ...(event.message ? { lastError: event.message } : {}),
          });
        default:
          return Effect.void;
      }
    };

    const rejectPending = (current: HostConnection, cause: Error) => {
      for (const pending of current.pending.values()) {
        clearTimeout(pending.timer);
        pending.reject(cause);
      }
      current.pending.clear();
    };

    function ensureConnection(): Effect.Effect<HostConnection, CodexSessionRuntimeError> {
      if (closing) {
        return Effect.fail(
          transportError("connect-provider-host", "Provider-host runtime is closing."),
        );
      }
      if (connection && !connection.closed && !connection.socket.destroyed) {
        return Effect.succeed(connection);
      }
      if (!connecting) {
        connecting = openConnection().finally(() => {
          connecting = undefined;
        });
      }
      return Effect.tryPromise({
        try: () => connecting!,
        catch: (cause) =>
          isCodexSessionRuntimeThreadIdMissingError(cause)
            ? cause
            : transportError("connect-provider-host", cause),
      }).pipe(
        Effect.flatMap((connected) => {
          if (closing) {
            connected.socket.destroy();
            return Effect.fail(
              transportError("connect-provider-host", "Provider-host runtime is closing."),
            );
          }
          connection = connected;
          return Effect.succeed(connected);
        }),
      );
    }

    const scheduleReconnect = () => {
      if (closing || !started || reconnecting) return;
      reconnecting = true;
      runFork(
        Effect.gen(function* () {
          yield* updateSession({ status: "connecting" });
          yield* emitConnectionEvent(
            "session/connecting",
            "Reattaching T3 to the independent Codex provider host.",
          );
          const deadline = (yield* Clock.currentTimeMillis) + RECONNECT_WINDOW_MS;
          let delayMs = 100;
          while (true) {
            if (closing || (yield* Clock.currentTimeMillis) >= deadline) break;
            yield* Effect.promise(waitForProviderEventDrain);
            if (closing) return;
            const exit = yield* Effect.exit(ensureConnection());
            if (Exit.isSuccess(exit)) {
              const snapshotExit = yield* Effect.exit(
                Effect.tryPromise({
                  try: () => exit.value.snapshot,
                  catch: (cause) => transportError("reattach-provider-host", cause),
                }),
              );
              if (Exit.isSuccess(snapshotExit)) {
                if (closing) {
                  exit.value.socket.destroy();
                  return;
                }
                return;
              }
              if (connection === exit.value) {
                connection = undefined;
              }
              exit.value.socket.destroy();
            }
            yield* Effect.sleep(`${delayMs} millis`);
            delayMs = Math.min(delayMs * 2, 2_000);
          }
          if (!closing) {
            const message =
              "T3 lost its provider-host attachment. Codex execution was not interrupted.";
            yield* updateSession({ status: "error", lastError: message });
            yield* emitConnectionEvent("session/disconnected", message);
          }
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              reconnecting = false;
            }),
          ),
        ),
      );
    };

    const openConnection = (): Promise<HostConnection> =>
      new Promise((resolve, reject) => {
        const socket = NodeNet.createConnection(input.controlSocketPath);
        sockets.add(socket);
        socket.setNoDelay(true);
        let settleSnapshot: (session: ProviderSession) => void = () => undefined;
        const snapshot = new Promise<ProviderSession>((snapshotResolve) => {
          settleSnapshot = snapshotResolve;
        });
        let resolveTransportClosed: () => void = () => undefined;
        const transportClosed = new Promise<void>((resolve) => {
          resolveTransportClosed = resolve;
        });
        const current: HostConnection = {
          socket,
          snapshot,
          pending: new Map(),
          replayEvents: [],
          transportClosed,
          canAdoptSessions: false,
          replayTruncated: false,
          snapshotReceived: false,
          closed: false,
          lineFramer: makeProviderHostLineFramer(MAX_INBOUND_LINE_BYTES),
        };
        let settled = false;
        let attached = false;
        let attachMode: ProviderHostAttachMode | undefined;
        let helloReceived = false;
        let generationChanged = false;
        const timeout = setTimeout(() => {
          socket.destroy(new Error("Timed out connecting to the Codex provider host."));
        }, CONNECT_TIMEOUT_MS);

        const fail = (cause: Error) => {
          clearTimeout(timeout);
          if (!settled) {
            settled = true;
            reject(cause);
          }
          rejectPending(current, cause);
        };

        const attach = () => {
          const needsExistingSession =
            requestedSessionMode !== "create" || started || attachmentAttempted;
          if (attached || !helloReceived) {
            return;
          }
          let mode: ProviderHostAttachMode;
          if (!needsExistingSession) {
            mode = "create";
          } else if (
            current.canAdoptSessions &&
            latestResumeCursor !== undefined &&
            (requestedSessionMode === "adopt" || runtimeRecoveryRequired || generationChanged)
          ) {
            mode = "adopt";
          } else {
            mode = "reuse";
          }
          attachMode = mode;
          attached = true;
          attachmentAttempted = true;
          const effectiveSessionOptions =
            latestResumeCursor === undefined
              ? sessionOptions
              : { ...sessionOptions, resumeCursor: latestResumeCursor };
          socket.write(
            `${JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "attach",
              clientId,
              attachmentId,
              threadId: input.options.threadId,
              replayFrom: replayCursor,
              mode,
              ...(mode === "reuse" ? {} : { session: effectiveSessionOptions }),
            })}\n`,
            (cause) => {
              if (cause) {
                fail(cause);
              }
            },
          );
        };

        const enqueueProviderEvent = (
          sequence: number,
          providerEvent: ProviderEvent,
          retainedBytes: number,
          replayTruncated?: boolean,
        ) => {
          appendProcessing(current, async () => {
            if (sequence <= Number(replayCursor)) {
              releaseProviderEvent(retainedBytes);
              return;
            }
            const event =
              replayTruncated === undefined
                ? providerEvent
                : {
                    ...providerEvent,
                    replay: {
                      truncated: replayTruncated,
                    },
                  };
            let buffered = false;
            try {
              await runPromise(
                applyProviderEvent(event).pipe(
                  Effect.andThen(offerEvent(event, retainedBytes)),
                  Effect.tap(() =>
                    Effect.sync(() => {
                      replayCursor = ProviderHostReplayCursor.make(sequence);
                    }),
                  ),
                  Effect.asVoid,
                ),
              );
              buffered = true;
            } finally {
              if (!buffered) {
                releaseProviderEvent(retainedBytes);
              }
            }
          });
        };

        socket.setEncoding("utf8");
        socket.on("data", (chunk: string) => {
          const framed = current.lineFramer.push(chunk);
          if (framed.overflowed) {
            socket.destroy(new Error("Provider-host response exceeded the maximum frame size."));
            return;
          }
          for (const framedLine of framed.lines) {
            const line = framedLine.trim();
            if (!line) continue;
            let envelope: ProviderHostServerEnvelope;
            try {
              envelope = decodeServerEnvelope(line);
            } catch {
              socket.destroy(new Error("Invalid provider-host response."));
              return;
            }
            switch (envelope.type) {
              case "hello": {
                if (helloReceived) break;
                generationChanged =
                  generationFingerprint === undefined ||
                  generationFingerprint !== envelope.generationFingerprint;
                if (
                  generationFingerprint !== undefined &&
                  generationFingerprint !== envelope.generationFingerprint
                ) {
                  replayCursor = ProviderHostReplayCursor.make(0);
                }
                current.generationFingerprint = envelope.generationFingerprint;
                current.canAdoptSessions = envelope.canAdoptSessions;
                helloReceived = true;
                attach();
                break;
              }
              case "inventory": {
                if (
                  attached &&
                  attachMode === "reuse" &&
                  !current.snapshotReceived &&
                  !envelope.threads.some((thread) => thread.threadId === input.options.threadId)
                ) {
                  fail(
                    new CodexSessionRuntimeThreadIdMissingError({
                      threadId: input.options.threadId,
                    }),
                  );
                  socket.destroy();
                }
                break;
              }
              case "snapshot":
                if (
                  envelope.threadId === input.options.threadId &&
                  isProviderSession(envelope.state)
                ) {
                  const snapshotState = envelope.state;
                  runtimeRecoveryRequired =
                    snapshotState.status === "error" || snapshotState.status === "closed";
                  if (
                    snapshotState.resumeCursor !== undefined &&
                    typeof snapshotState.resumeCursor === "object" &&
                    snapshotState.resumeCursor !== null &&
                    "threadId" in snapshotState.resumeCursor &&
                    typeof snapshotState.resumeCursor.threadId === "string"
                  ) {
                    latestResumeCursor = {
                      threadId: snapshotState.resumeCursor.threadId,
                    };
                  }
                  const shouldEmitAuthoritativeBarrier =
                    started || requestedSessionMode === "reuse";
                  current.replayTruncated = envelope.replayTruncated === true;
                  current.snapshotReceived = true;
                  for (const replayEvent of current.replayEvents.splice(0)) {
                    enqueueProviderEvent(
                      replayEvent.sequence,
                      replayEvent.event,
                      replayEvent.retainedBytes,
                      current.replayTruncated,
                    );
                  }
                  appendProcessing(current, () =>
                    runPromise(
                      Ref.set(sessionRef, snapshotState).pipe(
                        Effect.tap(() =>
                          current.replayTruncated ? emitReplayTruncatedEvent : Effect.void,
                        ),
                        Effect.tap(() =>
                          shouldEmitAuthoritativeBarrier
                            ? emitReattachedEvent(snapshotState)
                            : Effect.void,
                        ),
                        Effect.tap(() =>
                          Effect.sync(() => {
                            replayCursor = envelope.cursor;
                            generationFingerprint = current.generationFingerprint;
                            settleSnapshot(snapshotState);
                            clearTimeout(timeout);
                            if (!settled) {
                              settled = true;
                              resolve(current);
                            }
                          }),
                        ),
                      ),
                    ),
                  );
                }
                break;
              case "event":
                if (
                  envelope.threadId !== input.options.threadId ||
                  !isProviderEvent(envelope.event)
                ) {
                  break;
                }
                const providerEvent = envelope.event;
                const sequence = Number(envelope.sequence);
                const retainedBytes = Buffer.byteLength(framedLine);
                if (!reserveProviderEvent(retainedBytes)) {
                  socket.destroy(
                    new Error(
                      "Provider-host event queue exceeded its bounded count or byte capacity.",
                    ),
                  );
                  return;
                }
                if (!current.snapshotReceived) {
                  current.replayEvents.push({
                    sequence,
                    event: providerEvent,
                    retainedBytes,
                  });
                  break;
                }
                enqueueProviderEvent(sequence, providerEvent, retainedBytes);
                break;
              case "commandResult": {
                const pending = current.pending.get(envelope.commandId);
                if (!pending) break;
                current.pending.delete(envelope.commandId);
                clearTimeout(pending.timer);
                pending.resolve(envelope);
                break;
              }
            }
          }
        });
        socket.once("error", fail);
        socket.once("close", () => {
          sockets.delete(socket);
          current.closed = true;
          current.lineFramer.clear();
          for (const replayEvent of current.replayEvents.splice(0)) {
            releaseProviderEvent(replayEvent.retainedBytes);
          }
          resolveTransportClosed();
          fail(new Error("Codex provider-host connection closed."));
          if (connection === current) connection = undefined;
          scheduleReconnect();
        });
      });

    const sendCommandOnConnection = (
      current: HostConnection,
      commandInput: {
        readonly commandId: CommandId;
        readonly operation: CodexProviderHostOperation;
        readonly payload: Schema.Json;
        readonly deadlineAtMs: ProviderHostCommandDeadlineMs;
      },
    ): Promise<ProviderHostCommandResultEnvelope> =>
      new Promise((resolve, reject) => {
        const responseWaitMs = commandResponseWaitMs(
          commandInput.deadlineAtMs,
          commandClientGraceMs,
        );
        const timer = setTimeout(() => {
          current.pending.delete(commandInput.commandId);
          const cause = new Error(
            `Provider-host command '${commandInput.operation}' exceeded its response deadline.`,
          );
          current.socket.destroy(cause);
          reject(cause);
        }, responseWaitMs);
        current.pending.set(commandInput.commandId, { resolve, reject, timer });
        const command = ProviderHostCommandEnvelope.make({
          version: PROVIDER_HOST_PROTOCOL_VERSION,
          type: "command",
          clientId,
          attachmentId,
          commandId: commandInput.commandId,
          threadId: input.options.threadId,
          operation: commandInput.operation,
          payload: commandInput.payload,
          deadlineAtMs: commandInput.deadlineAtMs,
        });
        current.socket.write(`${JSON.stringify(command)}\n`, (cause) => {
          if (!cause) return;
          current.pending.delete(commandInput.commandId);
          clearTimeout(timer);
          reject(cause);
        });
      });

    const readThreadAfterAmbiguousMutation = Effect.fn(
      "CodexProviderHostClient.readThreadAfterAmbiguousMutation",
    )(function* (current: HostConnection) {
      const deadlineAtMs = commandDeadlineAtMs(commandTimeoutMs);
      return yield* Effect.tryPromise({
        try: () =>
          sendCommandOnConnection(current, {
            commandId: CommandId.make(NodeCrypto.randomUUID()),
            operation: CODEX_PROVIDER_HOST_OPERATIONS.readThread,
            payload: {},
            deadlineAtMs,
          }),
        catch: () => undefined,
      }).pipe(
        Effect.flatMap((result) => {
          if (!result.ok || !isCodexThreadSnapshot(result.result)) {
            return Effect.succeed(false);
          }
          latestResumeCursor = { threadId: result.result.threadId };
          return updateSession({ resumeCursor: latestResumeCursor }).pipe(Effect.as(true));
        }),
        Effect.orElseSucceed(() => false),
      );
    });

    const ambiguousMutationError = (
      operation: CodexProviderHostOperation,
      threadReadSucceeded: boolean,
    ) =>
      new CodexSessionRuntimeMutationAmbiguousError({
        threadId: input.options.threadId,
        operation,
        threadReadSucceeded,
      });

    const sendCommand = Effect.fn("CodexProviderHostClient.sendCommand")(function* (
      operation: CodexProviderHostOperation,
      payload: unknown,
    ): Effect.fn.Return<Schema.Json | undefined, CodexSessionRuntimeError> {
      const jsonPayload = yield* decodeCommandPayload(payload).pipe(
        Effect.mapError((cause) => transportError(`${operation}.payload`, cause)),
      );
      const commandId = CommandId.make(NodeCrypto.randomUUID());
      const deadlineAtMs = commandDeadlineAtMs(commandTimeoutMs);
      let lastError: CodexSessionRuntimeError | undefined;
      let commandGeneration: ProviderHostGenerationFingerprint | undefined;
      for (let attempt = 0; attempt < 2; attempt += 1) {
        const connectionResult = yield* ensureConnection().pipe(Effect.result);
        if (Result.isFailure(connectionResult)) {
          if (
            attempt > 0 &&
            operation !== CODEX_PROVIDER_HOST_OPERATIONS.readThread &&
            commandGeneration !== undefined
          ) {
            return yield* ambiguousMutationError(operation, false);
          }
          return yield* connectionResult.failure;
        }
        const current = connectionResult.success;
        if (
          attempt > 0 &&
          operation !== CODEX_PROVIDER_HOST_OPERATIONS.readThread &&
          commandGeneration !== undefined &&
          current.generationFingerprint !== commandGeneration
        ) {
          const threadReadSucceeded = yield* readThreadAfterAmbiguousMutation(current);
          return yield* ambiguousMutationError(operation, threadReadSucceeded);
        }
        commandGeneration ??= current.generationFingerprint;
        const outcome = yield* Effect.tryPromise({
          try: () =>
            sendCommandOnConnection(current, {
              commandId,
              operation,
              payload: jsonPayload,
              deadlineAtMs,
            }),
          catch: (cause) => transportError(operation, cause),
        }).pipe(Effect.result);
        if (Result.isSuccess(outcome)) {
          if (!outcome.success.ok) {
            if (
              outcome.success.errorCode === "deadline-exceeded" &&
              operation !== CODEX_PROVIDER_HOST_OPERATIONS.readThread
            ) {
              const threadReadSucceeded = yield* readThreadAfterAmbiguousMutation(current);
              return yield* ambiguousMutationError(operation, threadReadSucceeded);
            }
            return yield* transportError(
              operation,
              new Error(outcome.success.error ?? "Provider-host command failed."),
            );
          }
          return outcome.success.result;
        }
        lastError = outcome.failure;
        connection = undefined;
      }
      if (
        operation !== CODEX_PROVIDER_HOST_OPERATIONS.readThread &&
        commandGeneration !== undefined
      ) {
        return yield* ambiguousMutationError(operation, false);
      }
      return yield* lastError ?? transportError(operation, "Provider-host request failed.");
    });

    const start = Effect.fn("CodexProviderHostClient.start")(function* () {
      const current = yield* ensureConnection();
      const snapshot = yield* Effect.tryPromise({
        try: () => current.snapshot,
        catch: (cause) => transportError("attach-provider-host", cause),
      });
      started = true;
      return snapshot;
    });

    const release = Effect.fn("CodexProviderHostClient.release")(function* (stop: boolean) {
      if (closing) return;
      if (stop) {
        yield* sendCommand(CODEX_PROVIDER_HOST_OPERATIONS.stopSession, {});
      }
      closing = true;
      wakeCapacityWaiters();
      const current = connection;
      connection = undefined;
      for (const socket of sockets) {
        if (socket !== current?.socket) {
          socket.destroy();
        }
      }
      if (current && !current.closed && !current.socket.destroyed) {
        const detach = ProviderHostDetachEnvelope.make({
          version: PROVIDER_HOST_PROTOCOL_VERSION,
          type: "detach",
          clientId,
          attachmentId,
          threadId: input.options.threadId,
        });
        current.socket.end(`${JSON.stringify(detach)}\n`);
      }
      if (current) {
        yield* Effect.tryPromise({
          try: async () => {
            await new Promise<void>((resolve) => {
              let settled = false;
              const finish = () => {
                if (settled) return;
                settled = true;
                clearTimeout(timer);
                resolve();
              };
              const timer = setTimeout(() => {
                current.socket.destroy();
                finish();
              }, detachCloseTimeoutMs);
              void current.transportClosed.then(finish);
            });
          },
          catch: (cause) => transportError("drain-provider-host-events", cause),
        });
      }
      yield* Effect.tryPromise({
        try: () => processing,
        catch: (cause) => transportError("drain-provider-host-events", cause),
      });
      if (stop) {
        yield* updateSession({ status: "closed", activeTurnId: undefined });
      }
      yield* Queue.end(events);
    });

    yield* Effect.addFinalizer(() => release(false).pipe(Effect.ignore));

    return {
      start,
      getSession: Ref.get(sessionRef),
      sendTurn: (turnInput) =>
        sendCommand(
          CODEX_PROVIDER_HOST_OPERATIONS.sendTurn,
          CodexProviderHostSendTurnPayload.make({
            ...(turnInput.input !== undefined ? { input: turnInput.input } : {}),
            ...(turnInput.attachments !== undefined ? { attachments: turnInput.attachments } : {}),
            ...(turnInput.model !== undefined ? { model: turnInput.model } : {}),
            ...(turnInput.serviceTier !== undefined ? { serviceTier: turnInput.serviceTier } : {}),
            ...(turnInput.effort !== undefined ? { effort: turnInput.effort } : {}),
            ...(turnInput.interactionMode !== undefined
              ? { interactionMode: turnInput.interactionMode }
              : {}),
          }),
        ).pipe(
          Effect.flatMap((result) =>
            isProviderTurnStartResult(result)
              ? Effect.succeed(result)
              : Effect.fail(transportError("turn.start", "Invalid provider-host turn result.")),
          ),
          Effect.tap((result) =>
            updateSession({
              status: "running",
              activeTurnId: result.turnId,
              ...(result.resumeCursor ? { resumeCursor: result.resumeCursor } : {}),
            }),
          ),
        ),
      interruptTurn: (turnId) =>
        sendCommand(
          CODEX_PROVIDER_HOST_OPERATIONS.interruptTurn,
          CodexProviderHostInterruptPayload.make(turnId ? { turnId } : {}),
        ).pipe(Effect.asVoid),
      readThread: sendCommand(CODEX_PROVIDER_HOST_OPERATIONS.readThread, {}).pipe(
        Effect.flatMap((result) =>
          isCodexThreadSnapshot(result)
            ? Effect.succeed(result as CodexThreadSnapshot)
            : Effect.fail(transportError("thread.read", "Invalid provider-host thread snapshot.")),
        ),
      ),
      rollbackThread: (numTurns) =>
        sendCommand(
          CODEX_PROVIDER_HOST_OPERATIONS.rollbackThread,
          CodexProviderHostRollbackPayload.make({ numTurns }),
        ).pipe(
          Effect.flatMap((result) =>
            isCodexThreadSnapshot(result)
              ? Effect.succeed(result as CodexThreadSnapshot)
              : Effect.fail(
                  transportError("thread.rollback", "Invalid provider-host rollback result."),
                ),
          ),
        ),
      respondToRequest: (requestId, decision) =>
        sendCommand(
          CODEX_PROVIDER_HOST_OPERATIONS.respondToRequest,
          CodexProviderHostApprovalPayload.make({
            requestId,
            decision,
          }),
        ).pipe(Effect.asVoid),
      respondToUserInput: (requestId, answers) =>
        sendCommand(
          CODEX_PROVIDER_HOST_OPERATIONS.respondToUserInput,
          CodexProviderHostUserInputPayload.make({
            requestId,
            answers,
          }),
        ).pipe(Effect.asVoid),
      emittedEventCount: Effect.succeed(0),
      events: Stream.fromQueue(events).pipe(
        Stream.mapEffect(({ event, retainedBytes }) =>
          Effect.sync(() => {
            if (retainedBytes > 0) {
              releaseProviderEvent(retainedBytes);
            }
            return event;
          }),
        ),
      ),
      detach: release(false).pipe(Effect.orDie),
      close: release(true).pipe(Effect.orDie),
    } satisfies CodexSessionRuntimeShape;
  },
);

export const __testing = {
  commandResponseWaitMs,
  detachCloseTimeoutMs: DETACH_CLOSE_TIMEOUT_MS,
  maxPendingProviderEventBytes: MAX_PENDING_PROVIDER_EVENT_BYTES,
  maxPendingProviderEvents: MAX_PENDING_PROVIDER_EVENTS,
};
