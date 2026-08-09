import {
  EventId,
  type PiSettings,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
  type ThreadId,
  TrimmedNonEmptyString,
  TurnId,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import { expandHomePath } from "../../pathExpansion.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import {
  PiRuntime,
  type PiRpcClient,
  type PiRpcEvent,
  type PiRpcResponse,
  type PiRpcApprovalFlag,
  type PiRuntimeError,
} from "../piRuntime.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";
import type { EventNdjsonLogger } from "./EventNdjsonLogger.ts";

const DEFAULT_PROVIDER = ProviderDriverKind.make("piAgent");
const PI_RESUME_VERSION = 1 as const;

const PiModel = Schema.Struct({
  provider: TrimmedNonEmptyString,
  id: TrimmedNonEmptyString,
});
const PiState = Schema.Struct({
  model: Schema.optionalKey(Schema.NullOr(PiModel)),
  thinkingLevel: Schema.String,
  isStreaming: Schema.Boolean,
  sessionFile: Schema.optionalKey(Schema.String),
  sessionId: TrimmedNonEmptyString,
});
type PiState = typeof PiState.Type;

const PiMessagesData = Schema.Struct({
  messages: Schema.Array(Schema.Unknown),
});
const PiResumeCursor = Schema.Struct({
  schemaVersion: Schema.Literal(PI_RESUME_VERSION),
  sessionId: TrimmedNonEmptyString,
  sessionFile: Schema.optionalKey(TrimmedNonEmptyString),
});
type PiResumeCursor = typeof PiResumeCursor.Type;

const PiMessageUpdateEvent = Schema.Struct({
  type: Schema.Literal("message_update"),
  message: Schema.Unknown,
  assistantMessageEvent: Schema.Struct({
    type: Schema.String,
    contentIndex: Schema.optionalKey(Schema.Int),
    delta: Schema.optionalKey(Schema.String),
    content: Schema.optionalKey(Schema.String),
    reason: Schema.optionalKey(Schema.String),
    partial: Schema.optionalKey(Schema.Unknown),
    toolCall: Schema.optionalKey(Schema.Unknown),
  }),
});
const PiMessageEvent = Schema.Struct({
  type: Schema.Literals(["message_start", "message_end", "turn_end"]),
  message: Schema.Unknown,
});
const PiToolExecutionEvent = Schema.Struct({
  type: Schema.Literals(["tool_execution_start", "tool_execution_update", "tool_execution_end"]),
  toolCallId: TrimmedNonEmptyString,
  toolName: TrimmedNonEmptyString,
  args: Schema.optionalKey(Schema.Unknown),
  partialResult: Schema.optionalKey(Schema.Unknown),
  result: Schema.optionalKey(Schema.Unknown),
  isError: Schema.optionalKey(Schema.Boolean),
});
const PiRetryEvent = Schema.Struct({
  type: Schema.Literals(["auto_retry_start", "auto_retry_end"]),
  errorMessage: Schema.optionalKey(Schema.String),
  finalError: Schema.optionalKey(Schema.String),
  success: Schema.optionalKey(Schema.Boolean),
});
const PiCompactionEvent = Schema.Struct({
  type: Schema.Literals(["compaction_start", "compaction_end"]),
  reason: Schema.optionalKey(Schema.String),
  result: Schema.optionalKey(Schema.Unknown),
  aborted: Schema.optionalKey(Schema.Boolean),
  errorMessage: Schema.optionalKey(Schema.String),
});
const PiExtensionErrorEvent = Schema.Struct({
  type: Schema.Literal("extension_error"),
  error: Schema.optionalKey(Schema.Unknown),
  message: Schema.optionalKey(Schema.String),
});

const decodePiStateExit = Schema.decodeUnknownExit(PiState);
const decodePiMessagesDataExit = Schema.decodeUnknownExit(PiMessagesData);
const decodePiResumeCursorExit = Schema.decodeUnknownExit(PiResumeCursor);
const decodePiMessageUpdateEventExit = Schema.decodeUnknownExit(PiMessageUpdateEvent);
const decodePiMessageEventExit = Schema.decodeUnknownExit(PiMessageEvent);
const decodePiToolExecutionEventExit = Schema.decodeUnknownExit(PiToolExecutionEvent);
const decodePiRetryEventExit = Schema.decodeUnknownExit(PiRetryEvent);
const decodePiCompactionEventExit = Schema.decodeUnknownExit(PiCompactionEvent);
const decodePiExtensionErrorEventExit = Schema.decodeUnknownExit(PiExtensionErrorEvent);

interface PiAssistantSettlement {
  readonly stopReason?: string | undefined;
  readonly errorMessage?: string | undefined;
  readonly usage?: unknown;
}

interface PiSessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly rpc: PiRpcClient;
  sessionId: string;
  sessionFile: string | undefined;
  activeTurnId: TurnId | undefined;
  lastAssistantSettlement: PiAssistantSettlement | undefined;
  readonly startedItemIds: Set<string>;
  stopped: boolean;
}

export interface PiAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId;
  readonly nativeEventLogger?: EventNdjsonLogger;
  /** Override Pi's legacy approval flag for compatible RPC providers. */
  readonly rpcApprovalFlag?: PiRpcApprovalFlag;
  /** Runtime provider identity; defaults to the Pi Agent driver. */
  readonly provider?: ProviderDriverKind;
}

function parseModelSlug(
  slug: string | undefined,
): { readonly provider: string; readonly modelId: string } | undefined {
  const trimmed = slug?.trim();
  if (!trimmed) {
    return undefined;
  }
  const separator = trimmed.indexOf("/");
  if (separator <= 0 || separator === trimmed.length - 1) {
    return undefined;
  }
  return {
    provider: trimmed.slice(0, separator),
    modelId: trimmed.slice(separator + 1),
  };
}

function modelSlugFromState(state: PiState): string | undefined {
  return state.model ? `${state.model.provider}/${state.model.id}` : undefined;
}

function parseResumeCursor(raw: unknown): PiResumeCursor | undefined {
  const decoded = decodePiResumeCursorExit(raw);
  return Exit.isSuccess(decoded) ? decoded.value : undefined;
}

function resumeCursorFromState(state: PiState): PiResumeCursor {
  const sessionFile = state.sessionFile?.trim();
  return {
    schemaVersion: PI_RESUME_VERSION,
    sessionId: state.sessionId,
    ...(sessionFile ? { sessionFile } : {}),
  };
}

function assistantSettlement(message: unknown): PiAssistantSettlement | undefined {
  if (!Predicate.isObject(message) || message.role !== "assistant") {
    return undefined;
  }
  return {
    ...(typeof message.stopReason === "string" ? { stopReason: message.stopReason } : {}),
    ...(typeof message.errorMessage === "string" ? { errorMessage: message.errorMessage } : {}),
    ...("usage" in message ? { usage: message.usage } : {}),
  };
}

function toolItemType(toolName: string) {
  if (toolName === "bash") {
    return "command_execution" as const;
  }
  if (toolName === "edit" || toolName === "write") {
    return "file_change" as const;
  }
  return "dynamic_tool_call" as const;
}

function textItemId(turnId: TurnId, streamKind: "assistant" | "thinking", index: number) {
  return RuntimeItemId.make(`pi:${turnId}:${streamKind}:${index}`);
}

function compactionItemId(turnId: TurnId | undefined) {
  return RuntimeItemId.make(`pi:${turnId ?? "session"}:compaction`);
}

function errorDetail(error: PiRuntimeError): string {
  return error.detail.trim() || error.message;
}

export const isPiSessionFileOpen = Effect.fn("PiAdapter.isPiSessionFileOpen")(function* (
  fileSystem: FileSystem.FileSystem,
  path: Path.Path,
  sessionFile: string,
  options?: { readonly procRoot?: string; readonly currentProcessId?: string },
): Effect.fn.Return<boolean> {
  const expected = path.resolve(sessionFile);
  const procRoot = options?.procRoot ?? "/proc";
  const currentProcessId = options?.currentProcessId ?? String(process.pid);
  const processes = yield* fileSystem.readDirectory(procRoot).pipe(Effect.orElseSucceed(() => []));
  for (const processId of processes) {
    if (!/^\d+$/u.test(processId) || processId === currentProcessId) continue;
    const descriptors = yield* fileSystem
      .readDirectory(path.join(procRoot, processId, "fd"))
      .pipe(Effect.orElseSucceed(() => []));
    for (const descriptor of descriptors) {
      const target = yield* fileSystem
        .readLink(path.join(procRoot, processId, "fd", descriptor))
        .pipe(Effect.option);
      if (Option.isSome(target) && path.resolve(target.value) === expected) return true;
    }
  }
  return false;
});

export function makePiAdapter(piSettings: PiSettings, options?: PiAdapterLiveOptions) {
  return Effect.gen(function* () {
    const PROVIDER = options?.provider ?? DEFAULT_PROVIDER;
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make(PROVIDER);
    const runtime = yield* PiRuntime;
    const crypto = yield* Crypto.Crypto;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const serverConfig = yield* ServerConfig;
    const sessions = new Map<ThreadId, PiSessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const runtimeEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate a Pi Agent runtime identifier.",
            cause,
          }),
      ),
    );
    const makeEventStamp = () =>
      Effect.all({
        eventId: randomUUIDv4.pipe(Effect.map(EventId.make)),
        createdAt: nowIso,
      });
    const emit = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEvents, event).pipe(Effect.asVoid);
    const logNative = (threadId: ThreadId, event: PiRpcEvent) =>
      options?.nativeEventLogger
        ? Effect.gen(function* () {
            const observedAt = yield* nowIso;
            yield* options.nativeEventLogger!.write({ observedAt, event }, threadId);
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("Failed to write native Pi Agent event log.", {
                cause,
                threadId,
                method: event.type,
              }),
            ),
          )
        : Effect.void;

    const eventBase = Effect.fn("PiAdapter.eventBase")(function* (
      ctx: PiSessionContext,
      event: PiRpcEvent,
      input?: {
        readonly turnId?: TurnId | undefined;
        readonly itemId?: RuntimeItemId | undefined;
      },
    ) {
      return {
        ...(yield* makeEventStamp()),
        provider: PROVIDER,
        providerInstanceId: boundInstanceId,
        threadId: ctx.threadId,
        ...(input?.turnId ? { turnId: input.turnId } : {}),
        ...(input?.itemId ? { itemId: input.itemId } : {}),
        raw: {
          source: "pi.rpc.event" as const,
          method: event.type,
          payload: event,
        },
      };
    });

    const getThreadSemaphore = (threadId: string) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing = Option.fromNullishOr(current.get(threadId));
        return Option.match(existing, {
          onNone: () =>
            Semaphore.make(1).pipe(
              Effect.map((semaphore) => {
                const next = new Map(current);
                next.set(threadId, semaphore);
                return [semaphore, next] as const;
              }),
            ),
          onSome: (semaphore) => Effect.succeed([semaphore, current] as const),
        });
      });
    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadSemaphore(threadId), (semaphore) => semaphore.withPermit(effect));

    const requestRpc = Effect.fn("PiAdapter.requestRpc")(function* (
      threadId: ThreadId,
      rpc: PiRpcClient,
      command: Readonly<Record<string, unknown>> & { readonly type: string },
    ) {
      const response: PiRpcResponse = yield* rpc.request(command).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: command.type,
              detail: errorDetail(cause),
              cause,
            }),
        ),
      );
      if (!response.success) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: command.type,
          detail: response.error?.trim() || `Pi Agent rejected '${command.type}'.`,
        });
      }
      return response.data;
    });

    const getState = Effect.fn("PiAdapter.getState")(function* (
      threadId: ThreadId,
      rpc: PiRpcClient,
    ) {
      const data = yield* requestRpc(threadId, rpc, { type: "get_state" });
      const decoded = decodePiStateExit(data);
      if (Exit.isFailure(decoded)) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "get_state",
          detail: "Pi Agent returned an invalid session state.",
          cause: decoded.cause,
        });
      }
      return decoded.value;
    });

    const applyModelSelection = Effect.fn("PiAdapter.applyModelSelection")(function* (
      threadId: ThreadId,
      rpc: PiRpcClient,
      selection: Parameters<PiAdapterShape["startSession"]>[0]["modelSelection"],
    ) {
      if (!selection) {
        return yield* getState(threadId, rpc);
      }
      if (selection.instanceId !== boundInstanceId) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "modelSelection",
          issue: `Pi Agent model selection is bound to instance '${selection.instanceId}', expected '${boundInstanceId}'.`,
        });
      }
      const parsed = parseModelSlug(selection.model);
      if (!parsed) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "modelSelection",
          issue: "Pi Agent model selection must use the 'provider/model' format.",
        });
      }

      yield* requestRpc(threadId, rpc, {
        type: "set_model",
        provider: parsed.provider,
        modelId: parsed.modelId,
      });
      const thinkingLevel = getModelSelectionStringOptionValue(selection, "thinkingLevel");
      if (thinkingLevel) {
        yield* requestRpc(threadId, rpc, {
          type: "set_thinking_level",
          level: thinkingLevel,
        });
      }
      return yield* getState(threadId, rpc);
    });

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<PiSessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId,
          }),
        );
      }
      return Effect.succeed(ctx);
    };

    const refreshSessionFromState = Effect.fn("PiAdapter.refreshSessionFromState")(function* (
      ctx: PiSessionContext,
      state: PiState,
    ) {
      const updatedAt = yield* nowIso;
      const model = modelSlugFromState(state);
      const resumeCursor = resumeCursorFromState(state);
      ctx.sessionId = state.sessionId;
      ctx.sessionFile = resumeCursor.sessionFile;
      ctx.session = {
        ...ctx.session,
        ...(model ? { model } : {}),
        resumeCursor,
        updatedAt,
      };
    });

    const ensureContentItemStarted = Effect.fn("PiAdapter.ensureContentItemStarted")(function* (
      ctx: PiSessionContext,
      event: PiRpcEvent,
      turnId: TurnId,
      itemId: RuntimeItemId,
      itemType: "assistant_message" | "reasoning",
    ) {
      if (ctx.startedItemIds.has(itemId)) {
        return;
      }
      ctx.startedItemIds.add(itemId);
      yield* emit({
        ...(yield* eventBase(ctx, event, { turnId, itemId })),
        type: "item.started",
        payload: {
          itemType,
          status: "inProgress",
        },
      });
    });

    const settleActiveTurn = Effect.fn("PiAdapter.settleActiveTurn")(function* (
      ctx: PiSessionContext,
      event: PiRpcEvent,
    ) {
      const turnId = ctx.activeTurnId;
      if (!turnId) {
        return;
      }

      const stateExit = yield* Effect.exit(getState(ctx.threadId, ctx.rpc));
      if (Exit.isSuccess(stateExit)) {
        yield* refreshSessionFromState(ctx, stateExit.value);
      }

      const settlement = ctx.lastAssistantSettlement;
      const stopReason = settlement?.stopReason;
      const state =
        stopReason === "aborted"
          ? ("cancelled" as const)
          : stopReason === "error" || settlement?.errorMessage
            ? ("failed" as const)
            : ("completed" as const);
      ctx.activeTurnId = undefined;
      ctx.lastAssistantSettlement = undefined;
      const updatedAt = yield* nowIso;
      const { activeTurnId: _activeTurnId, ...readySession } = ctx.session;
      ctx.session = {
        ...readySession,
        status: "ready",
        updatedAt,
        ...(state === "failed" && settlement?.errorMessage
          ? { lastError: settlement.errorMessage }
          : {}),
      };
      yield* emit({
        ...(yield* eventBase(ctx, event, { turnId })),
        type: "turn.completed",
        payload: {
          state,
          ...(stopReason ? { stopReason } : {}),
          ...(settlement?.usage !== undefined ? { usage: settlement.usage } : {}),
          ...(settlement?.errorMessage ? { errorMessage: settlement.errorMessage } : {}),
        },
      });
    });

    const handleRpcEvent = Effect.fn("PiAdapter.handleRpcEvent")(function* (
      ctx: PiSessionContext,
      event: PiRpcEvent,
    ) {
      yield* logNative(ctx.threadId, event);
      if (ctx.stopped) {
        return;
      }

      if (event.type === "agent_settled") {
        yield* settleActiveTurn(ctx, event);
        return;
      }

      const messageUpdate = decodePiMessageUpdateEventExit(event);
      if (Exit.isSuccess(messageUpdate)) {
        const turnId = ctx.activeTurnId;
        if (!turnId) {
          return;
        }
        const update = messageUpdate.value.assistantMessageEvent;
        const index = update.contentIndex ?? 0;
        if (update.type.startsWith("text_")) {
          const itemId = textItemId(turnId, "assistant", index);
          yield* ensureContentItemStarted(ctx, event, turnId, itemId, "assistant_message");
          if (update.type === "text_delta" && update.delta !== undefined) {
            yield* emit({
              ...(yield* eventBase(ctx, event, { turnId, itemId })),
              type: "content.delta",
              payload: {
                streamKind: "assistant_text",
                delta: update.delta,
                contentIndex: index,
              },
            });
          }
          if (update.type === "text_end") {
            yield* emit({
              ...(yield* eventBase(ctx, event, { turnId, itemId })),
              type: "item.completed",
              payload: {
                itemType: "assistant_message",
                status: "completed",
                ...(update.content ? { detail: update.content } : {}),
              },
            });
          }
          return;
        }
        if (update.type.startsWith("thinking_")) {
          const itemId = textItemId(turnId, "thinking", index);
          yield* ensureContentItemStarted(ctx, event, turnId, itemId, "reasoning");
          if (update.type === "thinking_delta" && update.delta !== undefined) {
            yield* emit({
              ...(yield* eventBase(ctx, event, { turnId, itemId })),
              type: "content.delta",
              payload: {
                streamKind: "reasoning_text",
                delta: update.delta,
                contentIndex: index,
              },
            });
          }
          if (update.type === "thinking_end") {
            yield* emit({
              ...(yield* eventBase(ctx, event, { turnId, itemId })),
              type: "item.completed",
              payload: {
                itemType: "reasoning",
                status: "completed",
                ...(update.content ? { detail: update.content } : {}),
              },
            });
          }
        }
        return;
      }

      const messageEvent = decodePiMessageEventExit(event);
      if (Exit.isSuccess(messageEvent)) {
        const settlement = assistantSettlement(messageEvent.value.message);
        if (settlement) {
          ctx.lastAssistantSettlement = settlement;
        }
        return;
      }

      const toolEvent = decodePiToolExecutionEventExit(event);
      if (Exit.isSuccess(toolEvent)) {
        const turnId = ctx.activeTurnId;
        if (!turnId) {
          return;
        }
        const itemId = RuntimeItemId.make(toolEvent.value.toolCallId);
        const itemType = toolItemType(toolEvent.value.toolName);
        if (toolEvent.value.type === "tool_execution_start") {
          yield* emit({
            ...(yield* eventBase(ctx, event, { turnId, itemId })),
            type: "item.started",
            payload: {
              itemType,
              status: "inProgress",
              title: toolEvent.value.toolName,
              ...(toolEvent.value.args !== undefined ? { data: toolEvent.value.args } : {}),
            },
          });
        } else if (toolEvent.value.type === "tool_execution_update") {
          yield* emit({
            ...(yield* eventBase(ctx, event, { turnId, itemId })),
            type: "item.updated",
            payload: {
              itemType,
              status: "inProgress",
              title: toolEvent.value.toolName,
              ...(toolEvent.value.partialResult !== undefined
                ? { data: toolEvent.value.partialResult }
                : {}),
            },
          });
        } else {
          yield* emit({
            ...(yield* eventBase(ctx, event, { turnId, itemId })),
            type: "item.completed",
            payload: {
              itemType,
              status: toolEvent.value.isError ? "failed" : "completed",
              title: toolEvent.value.toolName,
              ...(toolEvent.value.result !== undefined ? { data: toolEvent.value.result } : {}),
            },
          });
        }
        return;
      }

      const retryEvent = decodePiRetryEventExit(event);
      if (Exit.isSuccess(retryEvent)) {
        if (retryEvent.value.type === "auto_retry_start") {
          yield* emit({
            ...(yield* eventBase(ctx, event, { turnId: ctx.activeTurnId })),
            type: "runtime.warning",
            payload: {
              message: retryEvent.value.errorMessage || "Pi Agent is retrying the active turn.",
              retrying: true,
              detail: event,
            },
          });
        } else if (retryEvent.value.success === false) {
          yield* emit({
            ...(yield* eventBase(ctx, event, { turnId: ctx.activeTurnId })),
            type: "runtime.error",
            payload: {
              message: retryEvent.value.finalError || "Pi Agent retry failed.",
              class: "provider_error",
              detail: event,
            },
          });
        }
        return;
      }

      const compactionEvent = decodePiCompactionEventExit(event);
      if (Exit.isSuccess(compactionEvent)) {
        const itemId = compactionItemId(ctx.activeTurnId);
        if (compactionEvent.value.type === "compaction_start") {
          yield* emit({
            ...(yield* eventBase(ctx, event, {
              turnId: ctx.activeTurnId,
              itemId,
            })),
            type: "item.started",
            payload: {
              itemType: "context_compaction",
              status: "inProgress",
              title: "Context compaction",
              ...(compactionEvent.value.reason ? { detail: compactionEvent.value.reason } : {}),
            },
          });
        } else {
          yield* emit({
            ...(yield* eventBase(ctx, event, {
              turnId: ctx.activeTurnId,
              itemId,
            })),
            type: "item.completed",
            payload: {
              itemType: "context_compaction",
              status:
                compactionEvent.value.aborted || compactionEvent.value.errorMessage
                  ? "failed"
                  : "completed",
              title: "Context compaction",
              ...(compactionEvent.value.result !== undefined
                ? { data: compactionEvent.value.result }
                : {}),
              ...(compactionEvent.value.errorMessage
                ? { detail: compactionEvent.value.errorMessage }
                : {}),
            },
          });
        }
        return;
      }

      const extensionError = decodePiExtensionErrorEventExit(event);
      if (Exit.isSuccess(extensionError)) {
        yield* emit({
          ...(yield* eventBase(ctx, event, { turnId: ctx.activeTurnId })),
          type: "runtime.warning",
          payload: {
            message: extensionError.value.message || "A Pi Agent extension reported an error.",
            detail: extensionError.value.error ?? event,
          },
        });
      }
    });

    const stopSessionInternal = Effect.fn("PiAdapter.stopSessionInternal")(function* (
      ctx: PiSessionContext,
      emitExit: boolean,
    ) {
      if (ctx.stopped) {
        return;
      }
      ctx.stopped = true;
      sessions.delete(ctx.threadId);
      yield* ctx.rpc.stop;
      yield* Scope.close(ctx.scope, Exit.void).pipe(Effect.ignore);
      if (emitExit) {
        const syntheticEvent: PiRpcEvent = { type: "session_stopped" };
        yield* emit({
          ...(yield* eventBase(ctx, syntheticEvent)),
          type: "session.exited",
          payload: {
            reason: "Pi Agent session stopped.",
            recoverable: false,
            exitKind: "graceful",
          },
        });
      }
    });

    const startEventPumps = Effect.fn("PiAdapter.startEventPumps")(function* (
      ctx: PiSessionContext,
    ) {
      yield* ctx.rpc.events.pipe(
        Stream.runForEach((event) =>
          handleRpcEvent(ctx, event).pipe(
            Effect.catchCause((cause) =>
              Effect.logError("Failed to process Pi Agent RPC event.", {
                cause,
                threadId: ctx.threadId,
                method: event.type,
              }),
            ),
          ),
        ),
        Effect.forkIn(ctx.scope),
      );

      yield* ctx.rpc.exit.pipe(
        Effect.exit,
        Effect.flatMap((exit) =>
          Effect.gen(function* () {
            if (ctx.stopped) {
              return;
            }
            ctx.stopped = true;
            sessions.delete(ctx.threadId);
            const syntheticEvent: PiRpcEvent = {
              type: "process_exit",
              exit,
            };
            const detail = Exit.isSuccess(exit)
              ? `Pi Agent RPC exited with code ${exit.value.code}.`
              : (() => {
                  const cause = Cause.squash(exit.cause);
                  return cause instanceof Error ? cause.message : String(cause);
                })();
            yield* emit({
              ...(yield* eventBase(ctx, syntheticEvent, {
                turnId: ctx.activeTurnId,
              })),
              type: "runtime.error",
              payload: {
                message: detail,
                class: "transport_error",
                detail: exit,
              },
            });
            yield* emit({
              ...(yield* eventBase(ctx, syntheticEvent)),
              type: "session.exited",
              payload: {
                reason: detail,
                recoverable: true,
                exitKind: "error",
              },
            });
          }),
        ),
        Effect.forkIn(ctx.scope),
      );
    });

    const startSession: PiAdapterShape["startSession"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          if (!input.cwd?.trim()) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and must be non-empty.",
            });
          }

          const existing = sessions.get(input.threadId);
          if (existing && !existing.stopped) {
            yield* stopSessionInternal(existing, false);
          }

          const cwd = path.resolve(input.cwd.trim());
          const resume = parseResumeCursor(input.resumeCursor);
          if (
            resume?.sessionFile &&
            (yield* isPiSessionFileOpen(fileSystem, path, resume.sessionFile))
          ) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "This session is still open in another Pi-compatible process.",
            });
          }
          const sessionScope = yield* Scope.make("sequential");
          let transferred = false;
          yield* Effect.addFinalizer(() =>
            transferred ? Effect.void : Scope.close(sessionScope, Exit.void),
          );

          const rpc = yield* runtime
            .startRpc({
              binaryPath: piSettings.binaryPath,
              cwd,
              ...(options?.environment ? { environment: options.environment } : {}),
              ...(piSettings.sessionDir.trim()
                ? { sessionDir: expandHomePath(piSettings.sessionDir.trim()) }
                : {}),
              ...(resume?.sessionFile ? { resumeSessionFile: resume.sessionFile } : {}),
              ...(options?.rpcApprovalFlag ? { approvalFlag: options.rpcApprovalFlag } : {}),
            })
            .pipe(
              Effect.provideService(Scope.Scope, sessionScope),
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterProcessError({
                    provider: PROVIDER,
                    threadId: input.threadId,
                    detail: errorDetail(cause),
                    cause,
                  }),
              ),
            );
          const state = yield* applyModelSelection(input.threadId, rpc, input.modelSelection).pipe(
            Effect.tapError(() => rpc.stop),
            Effect.tapError(() => Scope.close(sessionScope, Exit.void).pipe(Effect.ignore)),
          );
          const createdAt = yield* nowIso;
          const model = modelSlugFromState(state);
          const resumeCursor = resumeCursorFromState(state);
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd,
            ...(model ? { model } : {}),
            threadId: input.threadId,
            resumeCursor,
            createdAt,
            updatedAt: createdAt,
          };
          const ctx: PiSessionContext = {
            threadId: input.threadId,
            session,
            scope: sessionScope,
            rpc,
            sessionId: state.sessionId,
            sessionFile: resumeCursor.sessionFile,
            activeTurnId: undefined,
            lastAssistantSettlement: undefined,
            startedItemIds: new Set(),
            stopped: false,
          };
          sessions.set(input.threadId, ctx);
          transferred = true;
          yield* startEventPumps(ctx);

          const syntheticEvent: PiRpcEvent = { type: "session_started" };
          yield* emit({
            ...(yield* eventBase(ctx, syntheticEvent)),
            type: "session.started",
            payload: {
              message: "Pi Agent RPC session started.",
              resume: resumeCursor,
            },
          });
          yield* emit({
            ...(yield* eventBase(ctx, syntheticEvent)),
            type: "session.state.changed",
            payload: {
              state: "ready",
              reason: "Pi Agent RPC session ready.",
            },
          });
          yield* emit({
            ...(yield* eventBase(ctx, syntheticEvent)),
            type: "thread.started",
            payload: {
              providerThreadId: state.sessionId,
            },
          });
          return session;
        }).pipe(Effect.scoped),
      );

    const sendTurn: PiAdapterShape["sendTurn"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(input.threadId);
          const text = input.input?.trim();
          const images = yield* Effect.forEach(input.attachments ?? [], (attachment) =>
            Effect.gen(function* () {
              const attachmentPath = resolveAttachmentPath({
                attachmentsDir: serverConfig.attachmentsDir,
                attachment,
              });
              if (!attachmentPath) {
                return yield* new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "prompt",
                  detail: `Invalid attachment id '${attachment.id}'.`,
                });
              }
              const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterRequestError({
                      provider: PROVIDER,
                      method: "prompt",
                      detail: cause.message,
                      cause,
                    }),
                ),
              );
              return {
                type: "image" as const,
                data: Buffer.from(bytes).toString("base64"),
                mimeType: attachment.mimeType,
              };
            }),
          );
          if (!text && images.length === 0) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "Pi Agent turns require text input or at least one attachment.",
            });
          }

          const steeringTurnId = ctx.activeTurnId;
          const turnId = steeringTurnId ?? TurnId.make(yield* randomUUIDv4);
          if (steeringTurnId === undefined && input.modelSelection) {
            const state = yield* applyModelSelection(input.threadId, ctx.rpc, input.modelSelection);
            yield* refreshSessionFromState(ctx, state);
          } else if (
            input.modelSelection !== undefined &&
            input.modelSelection.instanceId !== boundInstanceId
          ) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: `Pi Agent model selection is bound to instance '${input.modelSelection.instanceId}', expected '${boundInstanceId}'.`,
            });
          }

          ctx.activeTurnId = turnId;
          ctx.lastAssistantSettlement = undefined;
          ctx.startedItemIds.clear();
          const updatedAt = yield* nowIso;
          ctx.session = {
            ...ctx.session,
            status: "running",
            activeTurnId: turnId,
            updatedAt,
          };

          const syntheticEvent: PiRpcEvent = {
            type: steeringTurnId ? "steer_requested" : "prompt_requested",
          };
          if (steeringTurnId === undefined) {
            yield* emit({
              ...(yield* eventBase(ctx, syntheticEvent, { turnId })),
              type: "turn.started",
              payload: {
                ...(ctx.session.model ? { model: ctx.session.model } : {}),
                ...(input.modelSelection
                  ? {
                      effort: getModelSelectionStringOptionValue(
                        input.modelSelection,
                        "thinkingLevel",
                      ),
                    }
                  : {}),
              },
            });
          }

          const command = {
            type: steeringTurnId ? "steer" : "prompt",
            message: text ?? "",
            ...(images.length > 0 ? { images } : {}),
          };
          yield* requestRpc(input.threadId, ctx.rpc, command).pipe(
            Effect.tapError((error) =>
              steeringTurnId
                ? Effect.void
                : Effect.gen(function* () {
                    ctx.activeTurnId = undefined;
                    const failedAt = yield* nowIso;
                    const { activeTurnId: _activeTurnId, ...readySession } = ctx.session;
                    ctx.session = {
                      ...readySession,
                      status: "ready",
                      updatedAt: failedAt,
                      lastError: error.detail,
                    };
                    yield* emit({
                      ...(yield* eventBase(ctx, syntheticEvent, { turnId })),
                      type: "turn.aborted",
                      payload: {
                        reason: error.detail,
                      },
                    });
                  }),
            ),
          );

          const stateExit = yield* Effect.exit(getState(input.threadId, ctx.rpc));
          if (Exit.isSuccess(stateExit)) {
            yield* refreshSessionFromState(ctx, stateExit.value);
          }
          return {
            threadId: input.threadId,
            turnId,
            ...(ctx.session.resumeCursor !== undefined
              ? { resumeCursor: ctx.session.resumeCursor }
              : {}),
          };
        }),
      );

    const interruptTurn: PiAdapterShape["interruptTurn"] = (threadId, requestedTurnId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          const turnId = requestedTurnId ?? ctx.activeTurnId;
          yield* requestRpc(threadId, ctx.rpc, { type: "abort" });
          if (!turnId) {
            return;
          }
          ctx.activeTurnId = undefined;
          const updatedAt = yield* nowIso;
          const { activeTurnId: _activeTurnId, ...readySession } = ctx.session;
          ctx.session = {
            ...readySession,
            status: "ready",
            updatedAt,
          };
          const syntheticEvent: PiRpcEvent = { type: "abort_requested" };
          yield* emit({
            ...(yield* eventBase(ctx, syntheticEvent, { turnId })),
            type: "turn.aborted",
            payload: {
              reason: "Interrupted by user.",
            },
          });
        }),
      );

    const respondToRequest: PiAdapterShape["respondToRequest"] = (threadId, requestId, _decision) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "extension_ui_response",
          detail: `Pi Agent approval response '${requestId}' is not supported yet.`,
        });
      });

    const respondToUserInput: PiAdapterShape["respondToUserInput"] = (
      threadId,
      requestId,
      _answers,
    ) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "extension_ui_response",
          detail: `Pi Agent user-input response '${requestId}' is not supported yet.`,
        });
      });

    const stopSession: PiAdapterShape["stopSession"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          yield* stopSessionInternal(ctx, true);
        }),
      );

    const listSessions: PiAdapterShape["listSessions"] = () =>
      Effect.sync(() => [...sessions.values()].map((ctx) => ({ ...ctx.session })));

    const hasSession: PiAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => {
        const ctx = sessions.get(threadId);
        return ctx !== undefined && !ctx.stopped;
      });

    const readThread: PiAdapterShape["readThread"] = Effect.fn("PiAdapter.readThread")(
      function* (threadId) {
        const ctx = yield* requireSession(threadId);
        const data = yield* requestRpc(threadId, ctx.rpc, { type: "get_messages" });
        const decoded = decodePiMessagesDataExit(data);
        if (Exit.isFailure(decoded)) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "get_messages",
            detail: "Pi Agent returned an invalid message transcript.",
            cause: decoded.cause,
          });
        }
        const turns = decoded.value.messages.flatMap((message, index) => {
          if (!Predicate.isObject(message) || message.role !== "assistant") {
            return [];
          }
          const rawId =
            typeof message.id === "string"
              ? message.id
              : typeof message.timestamp === "number" || typeof message.timestamp === "string"
                ? String(message.timestamp)
                : String(index);
          return [
            {
              id: TurnId.make(`pi-message-${rawId}`),
              items: [message],
            },
          ];
        });
        return {
          threadId,
          turns,
        };
      },
    );

    const rollbackThread: PiAdapterShape["rollbackThread"] = (threadId, numTurns) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        if (!Number.isInteger(numTurns) || numTurns < 1) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          });
        }
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "thread/rollback",
          detail: "Pi Agent RPC does not expose provider-side rollback.",
        });
      });

    const stopAll: PiAdapterShape["stopAll"] = () =>
      Effect.forEach(
        [...sessions.values()],
        (ctx) => stopSessionInternal(ctx, false).pipe(Effect.ignore),
        { concurrency: "unbounded", discard: true },
      );

    yield* Effect.addFinalizer(() =>
      stopAll().pipe(Effect.andThen(PubSub.shutdown(runtimeEvents)), Effect.ignore),
    );

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "in-session",
        sessionPersistence: "process-bound",
      },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      streamEvents: Stream.fromPubSub(runtimeEvents),
    } satisfies PiAdapterShape;
  });
}
