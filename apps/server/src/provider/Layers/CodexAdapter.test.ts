// @effect-diagnostics nodeBuiltinImport:off
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  ApprovalRequestId,
  CodexSettings,
  EnvironmentId,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderItemId,
  type ProviderApprovalDecision,
  type ProviderEvent,
  type ProviderSession,
  type ProviderTurnStartResult,
  type ProviderUserInputAnswers,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it, vi } from "@effect/vitest";

import * as Context from "effect/Context";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as CodexErrors from "effect-codex-app-server/errors";

import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { ProviderAdapterValidationError } from "../Errors.ts";
import type { CodexAdapterShape } from "../Services/CodexAdapter.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import {
  type CodexSessionRuntimeError,
  CodexSessionRuntimeMutationAmbiguousError,
  CodexSessionRuntimeThreadIdMissingError,
  type CodexSessionRuntimeOptions,
  type CodexSessionRuntimeSendTurnInput,
  type CodexSessionRuntimeShape,
  type CodexThreadSnapshot,
} from "./CodexSessionRuntime.ts";
import { makeCodexAdapter } from "./CodexAdapter.ts";
const decodeCodexSettings = Schema.decodeSync(CodexSettings);

// Test-local service tag so the rest of the file can keep using `yield* CodexAdapter`.
class CodexAdapter extends Context.Service<CodexAdapter, CodexAdapterShape>()(
  "t3/provider/Layers/CodexAdapter.test/CodexAdapter",
) {}

const asThreadId = (value: string): ThreadId => ThreadId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);
const asEventId = (value: string): EventId => EventId.make(value);
const asItemId = (value: string): ProviderItemId => ProviderItemId.make(value);

class FakeCodexRuntime implements CodexSessionRuntimeShape {
  private readonly eventQueue = Effect.runSync(Queue.unbounded<ProviderEvent, Cause.Done<void>>());
  private eventDeliveryBlock:
    | {
        readonly entered: () => void;
        readonly wait: Promise<void>;
      }
    | undefined;
  private readonly now = "2026-01-01T00:00:00.000Z";

  public readonly startImpl = vi.fn<() => Promise<ProviderSession>>(() =>
    Promise.resolve({
      provider: ProviderDriverKind.make("codex"),
      status: "ready" as const,
      runtimeMode: this.options.runtimeMode,
      threadId: this.options.threadId,
      cwd: this.options.cwd,
      ...(this.options.model ? { model: this.options.model } : {}),
      createdAt: this.now,
      updatedAt: this.now,
    } satisfies ProviderSession),
  );

  public readonly sendTurnImpl = vi.fn(
    (_input: CodexSessionRuntimeSendTurnInput): Promise<ProviderTurnStartResult> =>
      Promise.resolve({
        threadId: this.options.threadId,
        turnId: asTurnId("turn-1"),
      }),
  );
  public sendTurnError: CodexSessionRuntimeError | undefined;

  public readonly compactThread = Effect.void;

  public readonly interruptTurnImpl = vi.fn((_turnId?: TurnId): Promise<void> =>
    Promise.resolve(undefined),
  );

  public readonly readThreadImpl = vi.fn((): Promise<CodexThreadSnapshot> =>
    Promise.resolve({
      threadId: "provider-thread-1",
      turns: [],
    }),
  );

  public readonly rollbackThreadImpl = vi.fn((_numTurns: number): Promise<CodexThreadSnapshot> =>
    Promise.resolve({
      threadId: "provider-thread-1",
      turns: [],
    }),
  );

  public readonly uploadFeedbackImpl = vi.fn((_reason?: string) =>
    Promise.resolve({ threadId: "provider-thread-1" }),
  );

  public readonly respondToRequestImpl = vi.fn(
    (_requestId: ApprovalRequestId, _decision: ProviderApprovalDecision): Promise<void> =>
      Promise.resolve(undefined),
  );

  public readonly respondToUserInputImpl = vi.fn(
    (_requestId: ApprovalRequestId, _answers: ProviderUserInputAnswers): Promise<void> =>
      Promise.resolve(undefined),
  );

  public readonly closeImpl = vi.fn<() => Promise<void>>(() => Promise.resolve());
  public readonly detachImpl = vi.fn<() => Promise<void>>(() => Promise.resolve());
  public readonly getSessionImpl = vi.fn((): Promise<ProviderSession> => this.startImpl());
  public startError: CodexSessionRuntimeError | undefined;
  public readonly started: Promise<ProviderSession>;
  private resolveStarted: (session: ProviderSession) => void = () => undefined;

  readonly options: CodexSessionRuntimeOptions;

  constructor(options: CodexSessionRuntimeOptions) {
    this.options = options;
    const { promise, resolve } = Promise.withResolvers<ProviderSession>();
    this.started = promise;
    this.resolveStarted = resolve;
  }

  start(): Effect.Effect<ProviderSession, CodexSessionRuntimeError> {
    if (this.startError) {
      return Effect.fail(this.startError);
    }
    return Effect.promise(() => this.startImpl()).pipe(
      Effect.tap((session) =>
        Effect.sync(() => {
          this.resolveStarted(session);
        }),
      ),
    );
  }

  getSession = Effect.promise(() => this.getSessionImpl());
  emittedEventCount = Effect.succeed(0);

  sendTurn(input: CodexSessionRuntimeSendTurnInput) {
    if (this.sendTurnError) {
      return Effect.fail(this.sendTurnError);
    }
    return Effect.promise(() => this.sendTurnImpl(input));
  }

  interruptTurn(turnId?: TurnId) {
    return Effect.promise(() => this.interruptTurnImpl(turnId));
  }

  readThread = Effect.promise(() => this.readThreadImpl());

  rollbackThread(numTurns: number) {
    return Effect.promise(() => this.rollbackThreadImpl(numTurns));
  }

  uploadFeedback(reason?: string) {
    return Effect.promise(() => this.uploadFeedbackImpl(reason));
  }

  respondToRequest(requestId: ApprovalRequestId, decision: ProviderApprovalDecision) {
    return Effect.promise(() => this.respondToRequestImpl(requestId, decision));
  }

  respondToUserInput(requestId: ApprovalRequestId, answers: ProviderUserInputAnswers) {
    return Effect.promise(() => this.respondToUserInputImpl(requestId, answers));
  }

  get events() {
    return Stream.fromQueue(this.eventQueue).pipe(
      Stream.mapEffect((event) => {
        const block = this.eventDeliveryBlock;
        if (!block) {
          return Effect.succeed(event);
        }
        block.entered();
        return Effect.promise(() => block.wait).pipe(Effect.as(event));
      }),
    );
  }

  close = Effect.promise(() => this.closeImpl()).pipe(
    Effect.andThen(Queue.end(this.eventQueue)),
    Effect.asVoid,
  );
  detach = Effect.promise(() => this.detachImpl()).pipe(
    Effect.andThen(Queue.end(this.eventQueue)),
    Effect.asVoid,
  );

  emit(event: ProviderEvent) {
    return Queue.offer(this.eventQueue, event).pipe(Effect.asVoid);
  }

  emitUnsafe(event: ProviderEvent): void {
    Queue.offerUnsafe(this.eventQueue, event);
  }

  blockEventDelivery(): {
    readonly entered: Promise<void>;
    readonly release: () => void;
  } {
    const { promise: entered, resolve: resolveEntered } = Promise.withResolvers<void>();
    const { promise: wait, resolve: resolveWait } = Promise.withResolvers<void>();
    const block = {
      entered: resolveEntered,
      wait,
    };
    this.eventDeliveryBlock = block;
    return {
      entered,
      release: () => {
        if (this.eventDeliveryBlock === block) {
          this.eventDeliveryBlock = undefined;
        }
        resolveWait();
      },
    };
  }
}

function makeRuntimeFactory() {
  const runtimes: Array<FakeCodexRuntime> = [];
  const factory = vi.fn((options: CodexSessionRuntimeOptions) => {
    const runtime = new FakeCodexRuntime(options);
    runtimes.push(runtime);
    return Effect.succeed(runtime);
  });

  return {
    factory,
    get lastRuntime(): FakeCodexRuntime | undefined {
      return runtimes.at(-1);
    },
  };
}

function makeScopedRuntimeFactory(options?: { readonly failConstruction?: boolean }) {
  const runtimes: Array<FakeCodexRuntime> = [];
  const releasedThreadIds: Array<ThreadId> = [];

  const factory = vi.fn((runtimeOptions: CodexSessionRuntimeOptions) =>
    Effect.gen(function* () {
      yield* Scope.Scope;
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          releasedThreadIds.push(runtimeOptions.threadId);
        }),
      );

      if (options?.failConstruction) {
        return yield* new CodexErrors.CodexAppServerSpawnError({
          command: `${runtimeOptions.binaryPath} app-server`,
          cause: new Error("runtime construction failed"),
        });
      }

      const runtime = new FakeCodexRuntime(runtimeOptions);
      runtimes.push(runtime);
      return runtime;
    }),
  );

  return {
    factory,
    releasedThreadIds,
    get lastRuntime(): FakeCodexRuntime | undefined {
      return runtimes.at(-1);
    },
  };
}

const providerSessionDirectoryTestLayer = Layer.succeed(ProviderSessionDirectory, {
  upsert: () => Effect.void,
  insertIfAbsent: () => Effect.succeed(false),
  mergeRuntimePayload: () => Effect.void,
  mergeRuntimePayloadIfCurrent: () => Effect.succeed(false),
  recordImportedTranscript: () => Effect.die("unused"),
  getProvider: () =>
    Effect.die(new Error("ProviderSessionDirectory.getProvider is not used in test")),
  getBinding: () => Effect.succeed(Option.none()),
  listThreadIds: () => Effect.succeed([]),
  listBindings: () => Effect.succeed([]),
});

const validationRuntimeFactory = makeRuntimeFactory();
const validationLayer = it.layer(
  Layer.effect(
    CodexAdapter,
    Effect.gen(function* () {
      const codexConfig = decodeCodexSettings({});
      return yield* makeCodexAdapter(codexConfig, {
        makeRuntime: validationRuntimeFactory.factory,
      });
    }),
  ).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(ServerSettingsService.layerTest()),
    Layer.provideMerge(providerSessionDirectoryTestLayer),
    Layer.provideMerge(NodeServices.layer),
  ),
);

validationLayer("CodexAdapterLive validation", (it) => {
  it.effect("returns validation error for non-codex provider on startSession", () =>
    Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const result = yield* adapter
        .startSession({
          provider: ProviderDriverKind.make("claudeAgent"),
          threadId: asThreadId("thread-1"),
          runtimeMode: "full-access",
        })
        .pipe(Effect.result);

      NodeAssert.equal(result._tag, "Failure");
      NodeAssert.deepStrictEqual(
        result.failure,
        new ProviderAdapterValidationError({
          provider: ProviderDriverKind.make("codex"),
          operation: "startSession",
          issue: "Expected provider 'codex' but received 'claudeAgent'.",
        }),
      );
      NodeAssert.equal(validationRuntimeFactory.factory.mock.calls.length, 0);
    }),
  );
  it.effect("maps codex model options before starting a session", () =>
    Effect.gen(function* () {
      validationRuntimeFactory.factory.mockClear();
      const adapter = yield* CodexAdapter;

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.3-codex", [
          { id: "serviceTier", value: "priority" },
        ]),
        runtimeMode: "full-access",
      });

      NodeAssert.deepStrictEqual(validationRuntimeFactory.factory.mock.calls[0]?.[0], {
        binaryPath: "codex",
        cwd: process.cwd(),
        launchArgs: "",
        model: "gpt-5.3-codex",
        providerInstanceId: ProviderInstanceId.make("codex"),
        serviceTier: "priority",
        threadId: asThreadId("thread-1"),
        runtimeMode: "full-access",
      });
    }),
  );
});

const sessionRuntimeFactory = makeRuntimeFactory();
const sessionErrorLayer = it.layer(
  Layer.effect(
    CodexAdapter,
    Effect.gen(function* () {
      const codexConfig = decodeCodexSettings({});
      return yield* makeCodexAdapter(codexConfig, {
        makeRuntime: sessionRuntimeFactory.factory,
      });
    }),
  ).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(ServerSettingsService.layerTest()),
    Layer.provideMerge(providerSessionDirectoryTestLayer),
    Layer.provideMerge(NodeServices.layer),
  ),
);

sessionErrorLayer("CodexAdapterLive session errors", (it) => {
  it.effect("maps missing adapter sessions to ProviderAdapterSessionNotFoundError", () =>
    Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const result = yield* adapter
        .sendTurn({
          threadId: asThreadId("sess-missing"),
          input: "hello",
          attachments: [],
        })
        .pipe(Effect.result);

      NodeAssert.equal(result._tag, "Failure");
      NodeAssert.equal(result.failure._tag, "ProviderAdapterSessionNotFoundError");
      NodeAssert.equal(result.failure.provider, "codex");
      NodeAssert.equal(result.failure.threadId, "sess-missing");
    }),
  );

  it.effect("compacts the active Codex thread and emits compacted state", () =>
    Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const threadId = asThreadId("thread-compact");
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        runtimeMode: "full-access",
      });
      const runtime = sessionRuntimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      const compactedEventFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "thread.state.changed"),
        Stream.runHead,
        Effect.forkChild,
      );
      yield* adapter.compactThread!(threadId);
      yield* runtime.emit({
        id: asEventId("evt-compaction-item-completed"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "item/completed",
        threadId,
        payload: {
          completedAtMs: 1_778_000_000_000,
          threadId: "provider-thread-1",
          turnId: "provider-compact-turn",
          item: {
            id: "provider-compact-item",
            type: "contextCompaction",
          },
        },
      });
      const event = Option.getOrThrow(yield* Fiber.join(compactedEventFiber));
      NodeAssert.ok(event.type === "thread.state.changed");
      NodeAssert.equal(event.payload.state, "compacted");
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("uploads feedback for the active Codex thread", () =>
    Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const threadId = asThreadId("thread-feedback");
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        runtimeMode: "full-access",
      });
      const runtime = sessionRuntimeFactory.lastRuntime;
      NodeAssert.ok(runtime);

      const result = yield* adapter.uploadFeedback({
        threadId,
        reason: "The agent stopped early.",
      });

      NodeAssert.deepStrictEqual(result, { feedbackId: "provider-thread-1" });
      NodeAssert.deepStrictEqual(runtime.uploadFeedbackImpl.mock.calls, [
        ["The agent stopped early."],
      ]);
    }),
  );

  it.effect("rejects feedback for an unknown Codex thread", () =>
    Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const result = yield* adapter
        .uploadFeedback({ threadId: asThreadId("thread-feedback-missing") })
        .pipe(Effect.result);

      NodeAssert.equal(result._tag, "Failure");
      NodeAssert.equal(result.failure._tag, "ProviderAdapterSessionNotFoundError");
    }),
  );

  it.effect("maps codex model options before sending a turn", () =>
    Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("sess-missing"),
        runtimeMode: "full-access",
      });
      const runtime = sessionRuntimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      runtime.sendTurnImpl.mockClear();

      yield* Effect.ignore(
        adapter.sendTurn({
          threadId: asThreadId("sess-missing"),
          input: "hello",
          modelSelection: createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.3-codex", [
            { id: "reasoningEffort", value: "high" },
            { id: "serviceTier", value: "priority" },
          ]),
          attachments: [],
        }),
      );

      NodeAssert.deepStrictEqual(runtime.sendTurnImpl.mock.calls[0]?.[0], {
        input: "hello",
        model: "gpt-5.3-codex",
        effort: "high",
        serviceTier: "priority",
      });
    }),
  );

  it.effect("passes configured launch args into the session runtime", () => {
    const runtimeFactory = makeRuntimeFactory();
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({ launchArgs: "--strict-config --enable foo" });
        return yield* makeCodexAdapter(codexConfig, {
          makeRuntime: runtimeFactory.factory,
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("sess-launch-args"),
        runtimeMode: "full-access",
      });

      const runtime = runtimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      NodeAssert.equal(runtime.options.launchArgs, "--strict-config --enable foo");
    }).pipe(Effect.provide(layer));
  });

  it.effect("uses T3CODE_CODEX_LAUNCH_ARGS for the session runtime", () => {
    const runtimeFactory = makeRuntimeFactory();
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({ launchArgs: "--enable settings-feature" });
        return yield* makeCodexAdapter(codexConfig, {
          environment: { T3CODE_CODEX_LAUNCH_ARGS: " --strict-config --enable env-feature " },
          makeRuntime: runtimeFactory.factory,
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("sess-launch-args-env"),
        runtimeMode: "full-access",
      });

      const runtime = runtimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      NodeAssert.equal(runtime.options.launchArgs, "--strict-config --enable env-feature");
    }).pipe(Effect.provide(layer));
  });

  it.effect("maps codex model options for the adapter's bound custom instance id", () => {
    const customInstanceId = ProviderInstanceId.make("codex_personal");
    const customRuntimeFactory = makeRuntimeFactory();
    const customLayer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          instanceId: customInstanceId,
          makeRuntime: customRuntimeFactory.factory,
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("sess-custom-instance"),
        runtimeMode: "full-access",
      });
      const runtime = customRuntimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      runtime.sendTurnImpl.mockClear();

      yield* Effect.ignore(
        adapter.sendTurn({
          threadId: asThreadId("sess-custom-instance"),
          input: "hello",
          modelSelection: createModelSelection(
            ProviderInstanceId.make("codex_personal"),
            "gpt-5.3-codex",
            [
              { id: "reasoningEffort", value: "high" },
              { id: "serviceTier", value: "flex" },
            ],
          ),
          attachments: [],
        }),
      );

      NodeAssert.deepStrictEqual(runtime.sendTurnImpl.mock.calls[0]?.[0], {
        input: "hello",
        model: "gpt-5.3-codex",
        effort: "high",
        serviceTier: "flex",
      });
    }).pipe(Effect.provide(customLayer));
  });
  it.effect("uses a detached proxy with thread-scoped MCP credentials", () => {
    const runtimeFactory = makeRuntimeFactory();
    const threadId = asThreadId("sess-detached-mcp");
    const controlSocketPaths: Array<string> = [];
    const sessionModes: Array<"create" | "reuse" | "adopt" | undefined> = [];
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          appServerHost: {
            socketPath: "/tmp/t3-codex.sock",
            ensure: Effect.succeed("/tmp/t3-codex.sock"),
          },
          makeProviderHostRuntime: ({ controlSocketPath, options, sessionMode }) => {
            controlSocketPaths.push(controlSocketPath);
            sessionModes.push(sessionMode);
            return runtimeFactory.factory(options);
          },
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      McpProviderSession.setMcpProviderSession({
        environmentId: EnvironmentId.make("environment-test"),
        threadId,
        providerSessionId: "provider-session-test",
        providerInstanceId: ProviderInstanceId.make("codex"),
        endpoint: "http://127.0.0.1:3773/mcp",
        authorizationHeader: "Bearer thread-secret",
      });
      const adapter = yield* CodexAdapter;
      NodeAssert.equal(adapter.capabilities.sessionPersistence, "detached");
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        runtimeMode: "full-access",
      });

      const runtime = runtimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      NodeAssert.deepStrictEqual(controlSocketPaths, ["/tmp/t3-codex.sock"]);
      NodeAssert.deepStrictEqual(sessionModes, ["create"]);
      NodeAssert.equal(runtime.options.appServerSocketPath, undefined);
      NodeAssert.equal(runtime.options.environment?.T3_MCP_BEARER_TOKEN, undefined);
      NodeAssert.deepStrictEqual(runtime.options.threadConfig, {
        mcp_servers: {
          "t3-code": {
            url: "http://127.0.0.1:3773/mcp",
            http_headers: {
              Authorization: "Bearer thread-secret",
            },
          },
        },
      });
    }).pipe(
      Effect.provide(layer),
      Effect.ensuring(
        Effect.sync(() => {
          McpProviderSession.clearMcpProviderSession(threadId);
        }),
      ),
    );
  });

  it.effect("fails visibly when the detached host cannot provide a control endpoint", () => {
    const processRuntimeFactory = makeRuntimeFactory();
    const providerHostRuntimeFactory = makeRuntimeFactory();
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          appServerHost: {
            socketPath: "/tmp/t3-codex-unavailable.sock",
            ensure: Effect.sync((): string | undefined => undefined),
          },
          makeRuntime: processRuntimeFactory.factory,
          makeProviderHostRuntime: ({ options }) => providerHostRuntimeFactory.factory(options),
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const result = yield* adapter
        .startSession({
          provider: ProviderDriverKind.make("codex"),
          threadId: asThreadId("sess-detached-host-unavailable"),
          runtimeMode: "full-access",
        })
        .pipe(Effect.result);

      NodeAssert.equal(result._tag, "Failure");
      if (result._tag !== "Failure") return;
      NodeAssert.equal(result.failure._tag, "ProviderAdapterProcessError");
      NodeAssert.match(result.failure.detail, /did not fall back to a process-bound session/);
      NodeAssert.equal(processRuntimeFactory.factory.mock.calls.length, 0);
      NodeAssert.equal(providerHostRuntimeFactory.factory.mock.calls.length, 0);
    }).pipe(Effect.provide(layer));
  });

  it.effect("reattaches an existing detached runtime without MCP session configuration", () => {
    const runtimeFactory = makeRuntimeFactory();
    const threadId = asThreadId("sess-detached-reattach");
    const providerHostInputs: Array<{
      readonly controlSocketPath: string;
      readonly options: CodexSessionRuntimeOptions;
      readonly sessionMode?: "create" | "reuse" | "adopt";
    }> = [];
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          appServerHost: {
            socketPath: "/tmp/t3-codex-reattach.sock",
            ensure: Effect.succeed("/tmp/t3-codex-reattach.sock"),
          },
          makeProviderHostRuntime: (input) => {
            providerHostInputs.push(input);
            return runtimeFactory.factory(input.options);
          },
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      McpProviderSession.setMcpProviderSession({
        environmentId: EnvironmentId.make("environment-test"),
        threadId,
        providerSessionId: "provider-session-test",
        providerInstanceId: ProviderInstanceId.make("codex"),
        endpoint: "http://127.0.0.1:3773/mcp",
        authorizationHeader: "Bearer thread-secret",
      });
      const adapter = yield* CodexAdapter;
      const reattachSession = adapter.reattachSession;
      NodeAssert.ok(reattachSession);

      const session = yield* reattachSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        resumeCursor: { threadId: "provider-thread-existing" },
        runtimeMode: "full-access",
      });

      NodeAssert.equal(session.status, "ready");
      NodeAssert.equal(providerHostInputs.length, 1);
      NodeAssert.equal(providerHostInputs[0]?.sessionMode, "adopt");
      NodeAssert.equal(providerHostInputs[0]?.options.threadConfig, undefined);
      NodeAssert.deepStrictEqual(providerHostInputs[0]?.options.resumeCursor, {
        threadId: "provider-thread-existing",
      });
      const runtime = runtimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 0);
    }).pipe(
      Effect.provide(layer),
      Effect.ensuring(
        Effect.sync(() => {
          McpProviderSession.clearMcpProviderSession(threadId);
        }),
      ),
    );
  });

  it.effect("maps authoritative attach-existing absence without stopping a host runtime", () => {
    const threadId = asThreadId("sess-detached-reattach-missing");
    let runtime: FakeCodexRuntime | undefined;
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          appServerHost: {
            socketPath: "/tmp/t3-codex-reattach-missing.sock",
            ensure: Effect.succeed("/tmp/t3-codex-reattach-missing.sock"),
          },
          makeProviderHostRuntime: ({ options, sessionMode }) => {
            NodeAssert.equal(sessionMode, "adopt");
            runtime = new FakeCodexRuntime(options);
            runtime.startError = new CodexSessionRuntimeThreadIdMissingError({ threadId });
            return Effect.succeed(runtime);
          },
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const reattachSession = adapter.reattachSession;
      NodeAssert.ok(reattachSession);

      const result = yield* reattachSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        runtimeMode: "full-access",
      }).pipe(Effect.result);

      NodeAssert.equal(result._tag, "Failure");
      if (result._tag !== "Failure") return;
      NodeAssert.equal(result.failure._tag, "ProviderAdapterSessionNotFoundError");
      NodeAssert.equal(result.failure.threadId, threadId);
      NodeAssert.ok(runtime);
      NodeAssert.equal(runtime.detachImpl.mock.calls.length, 1);
      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 0);
    }).pipe(Effect.provide(layer));
  });

  it.effect("keeps attach-existing transport failures transient without resume fallback", () => {
    const threadId = asThreadId("sess-detached-reattach-transport");
    const processRuntimeFactory = makeRuntimeFactory();
    let runtime: FakeCodexRuntime | undefined;
    let providerHostRuntimeCalls = 0;
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          appServerHost: {
            socketPath: "/tmp/t3-codex-reattach-transport.sock",
            ensure: Effect.succeed("/tmp/t3-codex-reattach-transport.sock"),
          },
          makeRuntime: processRuntimeFactory.factory,
          makeProviderHostRuntime: ({ options, sessionMode }) => {
            providerHostRuntimeCalls += 1;
            NodeAssert.equal(sessionMode, "adopt");
            runtime = new FakeCodexRuntime(options);
            runtime.startError = new CodexErrors.CodexAppServerTransportError({
              operation: "read-input-stream",
              cause: new Error("provider host unavailable"),
            });
            return Effect.succeed(runtime);
          },
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const reattachSession = adapter.reattachSession;
      NodeAssert.ok(reattachSession);

      const result = yield* reattachSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        resumeCursor: { threadId: "provider-thread-existing" },
        runtimeMode: "full-access",
      }).pipe(Effect.result);

      NodeAssert.equal(result._tag, "Failure");
      if (result._tag !== "Failure") return;
      NodeAssert.equal(result.failure._tag, "ProviderAdapterSessionClosedError");
      NodeAssert.equal(providerHostRuntimeCalls, 1);
      NodeAssert.equal(processRuntimeFactory.factory.mock.calls.length, 0);
      NodeAssert.ok(runtime);
      NodeAssert.equal(runtime.detachImpl.mock.calls.length, 1);
      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 0);
    }).pipe(Effect.provide(layer));
  });

  it.effect("detaches instead of stopping when a create attachment fails ambiguously", () => {
    const threadId = asThreadId("sess-detached-create-transport");
    let runtime: FakeCodexRuntime | undefined;
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          appServerHost: {
            socketPath: "/tmp/t3-codex-create-transport.sock",
            ensure: Effect.succeed("/tmp/t3-codex-create-transport.sock"),
          },
          makeProviderHostRuntime: ({ options, sessionMode }) => {
            NodeAssert.equal(sessionMode, "create");
            runtime = new FakeCodexRuntime(options);
            runtime.startError = new CodexErrors.CodexAppServerTransportError({
              operation: "read-input-stream",
              cause: new Error("snapshot response was lost"),
            });
            return Effect.succeed(runtime);
          },
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const result = yield* adapter
        .startSession({
          provider: ProviderDriverKind.make("codex"),
          threadId,
          runtimeMode: "full-access",
        })
        .pipe(Effect.result);

      NodeAssert.equal(result._tag, "Failure");
      NodeAssert.ok(runtime);
      NodeAssert.equal(runtime.detachImpl.mock.calls.length, 1);
      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 0);
    }).pipe(Effect.provide(layer));
  });

  it.effect("reports cross-generation mutations as ambiguous without closing the session", () => {
    const threadId = asThreadId("sess-detached-ambiguous-mutation");
    let runtime: FakeCodexRuntime | undefined;
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          appServerHost: {
            socketPath: "/tmp/t3-codex-ambiguous-mutation.sock",
            ensure: Effect.succeed("/tmp/t3-codex-ambiguous-mutation.sock"),
          },
          makeProviderHostRuntime: ({ options }) => {
            runtime = new FakeCodexRuntime(options);
            runtime.sendTurnError = new CodexSessionRuntimeMutationAmbiguousError({
              threadId,
              operation: "turn.start",
              threadReadSucceeded: true,
            });
            return Effect.succeed(runtime);
          },
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        runtimeMode: "full-access",
      });

      const result = yield* adapter
        .sendTurn({
          threadId,
          input: "Proceed",
          attachments: [],
        })
        .pipe(Effect.result);

      NodeAssert.equal(result._tag, "Failure");
      if (result._tag !== "Failure") return;
      NodeAssert.equal(result.failure._tag, "ProviderAdapterRequestError");
      if (result.failure._tag === "ProviderAdapterRequestError") {
        NodeAssert.equal(result.failure.reconciled, true);
      }
      NodeAssert.equal(yield* adapter.hasSession(threadId), true);
      NodeAssert.ok(runtime);
    }).pipe(Effect.provide(layer));
  });

  it.effect("reattaches a stale provider-host session before retrying a turn", () => {
    const threadId = asThreadId("sess-detached-stale-host");
    const runtimes: Array<FakeCodexRuntime> = [];
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          appServerHost: {
            socketPath: "/tmp/t3-codex-stale-host.sock",
            ensure: Effect.succeed("/tmp/t3-codex-stale-host.sock"),
          },
          makeProviderHostRuntime: ({ options }) => {
            const runtime = new FakeCodexRuntime(options);
            if (runtimes.length === 0) {
              runtime.sendTurnError = new CodexErrors.CodexAppServerTransportError({
                operation: "read-input-stream",
                cause: new Error("connect ENOENT /tmp/t3-codex-stale-host.sock"),
              });
            }
            runtimes.push(runtime);
            return Effect.succeed(runtime);
          },
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        runtimeMode: "full-access",
      });

      const result = yield* adapter.sendTurn({
        threadId,
        input: "try again",
        attachments: [],
      });

      NodeAssert.equal(result.turnId, asTurnId("turn-1"));
      NodeAssert.equal(runtimes.length, 2);
      NodeAssert.equal(runtimes[0]?.sendTurnImpl.mock.calls.length, 0);
      NodeAssert.equal(runtimes[0]?.detachImpl.mock.calls.length, 1);
      NodeAssert.equal(runtimes[1]?.sendTurnImpl.mock.calls.length, 1);
      NodeAssert.equal(yield* adapter.hasSession(threadId), true);
    }).pipe(Effect.provide(layer));
  });
});

const lifecycleRuntimeFactory = makeRuntimeFactory();
const lifecycleLayer = it.layer(
  Layer.effect(
    CodexAdapter,
    Effect.gen(function* () {
      const codexConfig = decodeCodexSettings({});
      return yield* makeCodexAdapter(codexConfig, {
        makeRuntime: lifecycleRuntimeFactory.factory,
      });
    }),
  ).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(ServerSettingsService.layerTest()),
    Layer.provideMerge(providerSessionDirectoryTestLayer),
    Layer.provideMerge(NodeServices.layer),
  ),
);

function startLifecycleRuntime() {
  return Effect.gen(function* () {
    const adapter = yield* CodexAdapter;
    yield* adapter.startSession({
      provider: ProviderDriverKind.make("codex"),
      threadId: asThreadId("thread-1"),
      runtimeMode: "full-access",
    });
    const runtime = lifecycleRuntimeFactory.lastRuntime;
    NodeAssert.ok(runtime);
    return { adapter, runtime };
  });
}

function codexTokenUsageEvent(input: {
  readonly id: string;
  readonly turnId: string;
  readonly inputTokens: number;
  readonly cachedInputTokens: number;
  readonly cacheCreationTokens: number;
  readonly outputTokens: number;
  readonly reasoningTokens: number;
  readonly last?: {
    readonly inputTokens: number;
    readonly cachedInputTokens: number;
    readonly cacheCreationTokens: number;
    readonly outputTokens: number;
    readonly reasoningTokens: number;
  };
}): ProviderEvent {
  const totalTokens = input.inputTokens + input.outputTokens;
  const last = input.last ?? input;
  return {
    id: asEventId(input.id),
    kind: "notification",
    provider: ProviderDriverKind.make("codex"),
    threadId: asThreadId("thread-1"),
    turnId: asTurnId(input.turnId),
    createdAt: "2026-01-01T00:00:00.000Z",
    method: "thread/tokenUsage/updated",
    payload: {
      threadId: "thread-1",
      turnId: input.turnId,
      tokenUsage: {
        total: {
          inputTokens: input.inputTokens,
          cachedInputTokens: input.cachedInputTokens,
          cacheWriteInputTokens: input.cacheCreationTokens,
          outputTokens: input.outputTokens,
          reasoningOutputTokens: input.reasoningTokens,
          totalTokens,
        },
        last: {
          inputTokens: last.inputTokens,
          cachedInputTokens: last.cachedInputTokens,
          cacheWriteInputTokens: last.cacheCreationTokens,
          outputTokens: last.outputTokens,
          reasoningOutputTokens: last.reasoningTokens,
          totalTokens: last.inputTokens + last.outputTokens,
        },
      },
    },
  };
}

function codexTurnEvent(method: "turn/started" | "turn/completed", turnId: string): ProviderEvent {
  return {
    id: asEventId(`evt-${method}-${turnId}`),
    kind: "notification",
    provider: ProviderDriverKind.make("codex"),
    threadId: asThreadId("thread-1"),
    turnId: asTurnId(turnId),
    createdAt: "2026-01-01T00:00:00.000Z",
    method,
    payload:
      method === "turn/started"
        ? {}
        : {
            threadId: "thread-1",
            turn: { id: turnId, items: [], status: "completed" },
          },
  };
}

lifecycleLayer("CodexAdapterLive lifecycle", (it) => {
  it.effect("calculates one Codex turn total from cumulative counters", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const completedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "turn.completed"),
        Stream.runHead,
        Effect.forkChild,
      );

      yield* runtime.emit(codexTurnEvent("turn/started", "turn-usage"));
      yield* runtime.emit(
        codexTokenUsageEvent({
          id: "evt-usage-1",
          turnId: "turn-usage",
          inputTokens: 100,
          cachedInputTokens: 40,
          cacheCreationTokens: 10,
          outputTokens: 20,
          reasoningTokens: 8,
        }),
      );
      // Codex can repeat both notifications without new work.
      yield* runtime.emit(codexTurnEvent("turn/started", "turn-usage"));
      yield* runtime.emit(
        codexTokenUsageEvent({
          id: "evt-usage-duplicate",
          turnId: "turn-usage",
          inputTokens: 100,
          cachedInputTokens: 40,
          cacheCreationTokens: 10,
          outputTokens: 20,
          reasoningTokens: 8,
        }),
      );
      yield* runtime.emit({
        id: asEventId("evt-collab-activity"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-usage"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "collabAgent/activity",
        payload: {
          agentThreadId: "child-1",
          agentPath: "/root/child-1",
          activityKind: "started",
        },
      });
      yield* runtime.emit(
        codexTokenUsageEvent({
          id: "evt-usage-2",
          turnId: "turn-usage",
          inputTokens: 150,
          cachedInputTokens: 60,
          cacheCreationTokens: 15,
          outputTokens: 30,
          reasoningTokens: 12,
        }),
      );
      yield* runtime.emit(codexTurnEvent("turn/completed", "turn-usage"));

      const completed = yield* Fiber.join(completedFiber);
      NodeAssert.equal(completed._tag, "Some");
      if (completed._tag === "Some" && completed.value.type === "turn.completed") {
        NodeAssert.deepStrictEqual(completed.value.payload.tokenUsage, {
          usageStatus: "complete",
          usageScope: "main_agent",
          inputTokens: 150,
          cachedInputTokens: 60,
          cacheCreationTokens: 15,
          outputTokens: 30,
          reasoningTokens: 12,
          hasSubagents: true,
        });
      }
    }),
  );

  it.effect("does not charge a late prior-turn update to the next Codex turn", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const completedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "turn.completed"),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* runtime.emit(codexTurnEvent("turn/started", "turn-first"));
      yield* runtime.emit(
        codexTokenUsageEvent({
          id: "evt-late-1",
          turnId: "turn-first",
          inputTokens: 100,
          cachedInputTokens: 40,
          cacheCreationTokens: 10,
          outputTokens: 20,
          reasoningTokens: 8,
        }),
      );
      yield* runtime.emit(codexTurnEvent("turn/completed", "turn-first"));
      yield* runtime.emit(codexTurnEvent("turn/started", "turn-second"));
      // A late update for the finished turn lands after the next turn starts.
      yield* runtime.emit(
        codexTokenUsageEvent({
          id: "evt-late-2",
          turnId: "turn-first",
          inputTokens: 150,
          cachedInputTokens: 60,
          cacheCreationTokens: 15,
          outputTokens: 30,
          reasoningTokens: 12,
        }),
      );
      yield* runtime.emit(
        codexTokenUsageEvent({
          id: "evt-late-3",
          turnId: "turn-second",
          inputTokens: 170,
          cachedInputTokens: 65,
          cacheCreationTokens: 16,
          outputTokens: 35,
          reasoningTokens: 14,
        }),
      );
      yield* runtime.emit(codexTurnEvent("turn/completed", "turn-second"));

      const completed = Array.from(yield* Fiber.join(completedFiber));
      const second = completed[1];
      NodeAssert.equal(second?.type, "turn.completed");
      if (second?.type === "turn.completed") {
        NodeAssert.deepStrictEqual(second.payload.tokenUsage, {
          usageStatus: "complete",
          usageScope: "main_agent",
          inputTokens: 20,
          cachedInputTokens: 5,
          cacheCreationTokens: 1,
          outputTokens: 5,
          reasoningTokens: 2,
          hasSubagents: false,
        });
      }
    }),
  );

  it.effect("clamps Codex cache and reasoning subsets to their totals", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const completedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "turn.completed"),
        Stream.runHead,
        Effect.forkChild,
      );

      yield* runtime.emit(codexTurnEvent("turn/started", "turn-clamp"));
      yield* runtime.emit(
        codexTokenUsageEvent({
          id: "evt-clamp-1",
          turnId: "turn-clamp",
          inputTokens: 100,
          cachedInputTokens: 140,
          cacheCreationTokens: 120,
          outputTokens: 20,
          reasoningTokens: 30,
        }),
      );
      yield* runtime.emit(codexTurnEvent("turn/completed", "turn-clamp"));

      const completed = yield* Fiber.join(completedFiber);
      NodeAssert.equal(completed._tag, "Some");
      if (completed._tag === "Some" && completed.value.type === "turn.completed") {
        NodeAssert.deepStrictEqual(completed.value.payload.tokenUsage, {
          usageStatus: "complete",
          usageScope: "main_agent",
          inputTokens: 100,
          cachedInputTokens: 100,
          cacheCreationTokens: 100,
          outputTokens: 20,
          reasoningTokens: 20,
          hasSubagents: false,
        });
      }
    }),
  );

  it.effect("counts the last response when Codex resets its running total mid-turn", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const completedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "turn.completed"),
        Stream.runHead,
        Effect.forkChild,
      );

      yield* runtime.emit(codexTurnEvent("turn/started", "turn-reset"));
      yield* runtime.emit(
        codexTokenUsageEvent({
          id: "evt-reset-1",
          turnId: "turn-reset",
          inputTokens: 5_000,
          cachedInputTokens: 4_000,
          cacheCreationTokens: 100,
          outputTokens: 500,
          reasoningTokens: 200,
          last: {
            inputTokens: 100,
            cachedInputTokens: 80,
            cacheCreationTokens: 10,
            outputTokens: 20,
            reasoningTokens: 8,
          },
        }),
      );
      // Codex restarted its cumulative total. The new total is smaller than
      // the previous one, so only `last` is counted for this update.
      yield* runtime.emit(
        codexTokenUsageEvent({
          id: "evt-reset-2",
          turnId: "turn-reset",
          inputTokens: 150,
          cachedInputTokens: 90,
          cacheCreationTokens: 5,
          outputTokens: 30,
          reasoningTokens: 12,
        }),
      );
      yield* runtime.emit(codexTurnEvent("turn/completed", "turn-reset"));

      const completed = yield* Fiber.join(completedFiber);
      NodeAssert.equal(completed._tag, "Some");
      if (completed._tag === "Some" && completed.value.type === "turn.completed") {
        NodeAssert.deepStrictEqual(completed.value.payload.tokenUsage, {
          usageStatus: "complete",
          usageScope: "main_agent",
          inputTokens: 250,
          cachedInputTokens: 170,
          cacheCreationTokens: 15,
          outputTokens: 50,
          reasoningTokens: 20,
          hasSubagents: false,
        });
      }
    }),
  );

  it.effect("uses the last response usage when no prior Codex total exists", () =>
    Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        resumeCursor: { threadId: "provider-thread-1" },
        runtimeMode: "full-access",
      });
      const runtime = lifecycleRuntimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      const firstCompletionsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "turn.completed"),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );

      // Resumed thread: the cumulative total already holds old history, so the
      // first update must count only `last`.
      yield* runtime.emit(codexTurnEvent("turn/started", "turn-resumed"));
      yield* runtime.emit(
        codexTokenUsageEvent({
          id: "evt-resume-baseline",
          turnId: "turn-resumed",
          inputTokens: 1_000,
          cachedInputTokens: 400,
          cacheCreationTokens: 100,
          outputTokens: 200,
          reasoningTokens: 80,
          last: {
            inputTokens: 300,
            cachedInputTokens: 120,
            cacheCreationTokens: 30,
            outputTokens: 60,
            reasoningTokens: 24,
          },
        }),
      );
      yield* runtime.emit(codexTurnEvent("turn/completed", "turn-resumed"));

      yield* runtime.emit(codexTurnEvent("turn/started", "turn-after-resume"));
      yield* runtime.emit(
        codexTokenUsageEvent({
          id: "evt-after-resume",
          turnId: "turn-after-resume",
          inputTokens: 1_100,
          cachedInputTokens: 440,
          cacheCreationTokens: 110,
          outputTokens: 220,
          reasoningTokens: 88,
        }),
      );
      yield* runtime.emit(codexTurnEvent("turn/completed", "turn-after-resume"));

      const firstCompletions = Array.from(yield* Fiber.join(firstCompletionsFiber));

      yield* adapter.rollbackThread(asThreadId("thread-1"), 1);
      const rollbackCompletionFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "turn.completed"),
        Stream.runHead,
        Effect.forkChild,
      );
      // Rollback drops the baseline and Codex shrinks its total, so the first
      // update after it counts only `last` again.
      yield* runtime.emit(codexTurnEvent("turn/started", "turn-after-rollback"));
      yield* runtime.emit(
        codexTokenUsageEvent({
          id: "evt-after-rollback",
          turnId: "turn-after-rollback",
          inputTokens: 1_050,
          cachedInputTokens: 420,
          cacheCreationTokens: 105,
          outputTokens: 210,
          reasoningTokens: 84,
          last: {
            inputTokens: 50,
            cachedInputTokens: 20,
            cacheCreationTokens: 5,
            outputTokens: 10,
            reasoningTokens: 4,
          },
        }),
      );
      yield* runtime.emit(codexTurnEvent("turn/completed", "turn-after-rollback"));

      const rollbackCompletion = yield* Fiber.join(rollbackCompletionFiber);
      const completions = [
        ...firstCompletions,
        ...(rollbackCompletion._tag === "Some" ? [rollbackCompletion.value] : []),
      ];
      NodeAssert.deepStrictEqual(
        completions.map((event) =>
          event.type === "turn.completed" ? event.payload.tokenUsage : undefined,
        ),
        [
          {
            usageStatus: "complete",
            usageScope: "main_agent",
            inputTokens: 300,
            cachedInputTokens: 120,
            cacheCreationTokens: 30,
            outputTokens: 60,
            reasoningTokens: 24,
            hasSubagents: false,
          },
          {
            usageStatus: "complete",
            usageScope: "main_agent",
            inputTokens: 100,
            cachedInputTokens: 40,
            cacheCreationTokens: 10,
            outputTokens: 20,
            reasoningTokens: 8,
            hasSubagents: false,
          },
          {
            usageStatus: "complete",
            usageScope: "main_agent",
            inputTokens: 50,
            cachedInputTokens: 20,
            cacheCreationTokens: 5,
            outputTokens: 10,
            reasoningTokens: 4,
            hasSubagents: false,
          },
        ],
      );
    }),
  );

  it.effect("carries child model metadata through every task event", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 10)).pipe(
        Effect.forkChild,
      );

      const cases = [
        ["collabAgent/started", {}],
        ["collabAgent/activity", { activityKind: "started" }],
        ["collabAgent/turnStarted", {}],
        ["collabAgent/turnCompleted", { turn: { status: "completed" } }],
        ["collabAgent/statusChanged", { status: { type: "active", activeFlags: [] } }],
        ["collabAgent/tokenUsage", { tokenUsage: { total: { totalTokens: 42 } } }],
        ["collabAgent/item", { item: { type: "commandExecution", command: "pwd" } }],
        ["collabAgent/closed", {}],
        ["collabAgent/metadataUpdated", {}],
      ] as const;

      for (const [index, [method, extra]] of cases.entries()) {
        yield* runtime.emit({
          id: asEventId(`evt-child-model-${index}`),
          kind: "notification",
          provider: ProviderDriverKind.make("codex"),
          createdAt: "2026-01-01T00:00:00.000Z",
          method,
          threadId: asThreadId("thread-1"),
          turnId: asTurnId("turn-1"),
          payload: {
            agentThreadId: "child-model",
            agentPath: "/root/model-check",
            model: " gpt-5.6-sol ",
            effort: " high ",
            ...extra,
          },
        });
      }
      yield* runtime.emit({
        id: asEventId("evt-child-model-blank"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "collabAgent/metadataUpdated",
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-1"),
        payload: {
          agentThreadId: "child-model",
          model: "  ",
          effort: "",
        },
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.deepStrictEqual(
        events.map((event) => event.type),
        [
          "task.started",
          "task.started",
          "task.updated",
          "task.updated",
          "task.updated",
          "task.progress",
          "task.progress",
          "task.updated",
          "task.updated",
          "task.updated",
        ],
      );
      for (const event of events.slice(0, -1)) {
        const payload = event.payload as Record<string, unknown>;
        NodeAssert.equal(payload.model, "gpt-5.6-sol");
        NodeAssert.equal(payload.effort, "high");
      }

      const metadataPayload = events[8]?.payload as Record<string, unknown>;
      NodeAssert.equal("status" in metadataPayload, false);
      const blankMetadataPayload = events[9]?.payload as Record<string, unknown>;
      NodeAssert.equal("status" in blankMetadataPayload, false);
      NodeAssert.equal("model" in blankMetadataPayload, false);
      NodeAssert.equal("effort" in blankMetadataPayload, false);
    }),
  );

  it.effect("does not reactivate an idle child after a parent interaction", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 3)).pipe(
        Effect.forkChild,
      );

      const childEvent = (id: string, method: string, payload: Record<string, unknown>) => ({
        id: asEventId(id),
        kind: "notification" as const,
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method,
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-1"),
        payload,
      });

      yield* runtime.emit(
        childEvent("evt-child-running", "collabAgent/turnStarted", {
          agentThreadId: "child-1",
          agentPath: "/root/audit",
        }),
      );
      yield* runtime.emit(
        childEvent("evt-child-idle", "collabAgent/turnCompleted", {
          agentThreadId: "child-1",
          agentPath: "/root/audit",
          turn: { status: "completed" },
        }),
      );
      yield* runtime.emit(
        childEvent("evt-child-interacted", "collabAgent/activity", {
          agentThreadId: "child-1",
          agentPath: "/root/audit",
          activityKind: "interacted",
        }),
      );
      yield* runtime.emit(
        childEvent("evt-other-child-running", "collabAgent/turnStarted", {
          agentThreadId: "child-2",
          agentPath: "/root/other",
        }),
      );

      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.deepStrictEqual(
        events.map((event) =>
          event.type === "task.updated"
            ? { taskId: event.payload.taskId, status: event.payload.status }
            : { type: event.type },
        ),
        [
          { taskId: "child-1", status: "running" },
          { taskId: "child-1", status: "idle" },
          { taskId: "child-2", status: "running" },
        ],
      );
    }),
  );

  it.effect("maps completed agent message items to canonical item.completed events", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      const event: ProviderEvent = {
        id: asEventId("evt-msg-complete"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "item/completed",
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-1"),
        itemId: asItemId("msg_1"),
        payload: {
          completedAtMs: 1_778_000_000_000,
          threadId: "thread-1",
          turnId: "turn-1",
          item: {
            type: "agentMessage",
            id: "msg_1",
            text: "done",
          },
        },
      };

      yield* runtime.emit(event);
      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "item.completed");
      if (firstEvent.value.type !== "item.completed") {
        return;
      }
      NodeAssert.equal(firstEvent.value.itemId, "msg_1");
      NodeAssert.equal(firstEvent.value.turnId, "turn-1");
      NodeAssert.equal(firstEvent.value.payload.itemType, "assistant_message");
    }),
  );

  it.effect("labels MCP lifecycle entries with server and tool names", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-mcp-complete"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "item/completed",
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-1"),
        itemId: asItemId("mcp_1"),
        payload: {
          completedAtMs: 1_778_000_000_000,
          threadId: "thread-1",
          turnId: "turn-1",
          item: {
            type: "mcpToolCall",
            id: "mcp_1",
            server: "t3-code",
            tool: "preview_status",
            arguments: {},
            durationMs: 12,
            error: null,
            result: { content: [{ type: "text", text: "attached" }] },
            status: "completed",
          },
        },
      });
      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some" || firstEvent.value.type !== "item.completed") {
        return;
      }
      NodeAssert.equal(firstEvent.value.payload.itemType, "mcp_tool_call");
      NodeAssert.equal(firstEvent.value.payload.title, "t3-code · preview_status");
      NodeAssert.deepStrictEqual(firstEvent.value.payload.data, {
        completedAtMs: 1_778_000_000_000,
        threadId: "thread-1",
        turnId: "turn-1",
        item: {
          type: "mcpToolCall",
          id: "mcp_1",
          server: "t3-code",
          tool: "preview_status",
          arguments: {},
          durationMs: 12,
          error: null,
          result: { content: [{ type: "text", text: "attached" }] },
          status: "completed",
        },
      });
    }),
  );

  it.effect("presents browser and computer-use calls with Codex-style titles and sources", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 3)).pipe(
        Effect.forkChild,
      );
      const longIntentTitle = `  ${"a".repeat(39)}   ${"a".repeat(38)}😀bc  `;
      const serializedOverContractUrl = `https://example.com/?query=${"😀".repeat(400)}`;

      yield* runtime.emit({
        id: asEventId("evt-computer-start"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "item/started",
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-1"),
        itemId: asItemId("computer_1"),
        payload: {
          startedAtMs: 1_778_000_000_000,
          threadId: "thread-1",
          turnId: "turn-1",
          item: {
            type: "mcpToolCall",
            id: "computer_1",
            server: "node_repl",
            tool: "js",
            arguments: {
              code: 'await sky.click({ app: "Finder", x: 10, y: 20 })',
              title: longIntentTitle,
            },
            durationMs: null,
            error: null,
            result: {
              _meta: {
                "codex/toolSurface": {
                  kind: "computerUse",
                  app: { kind: "appId", appId: "com.apple.finder" },
                },
              },
              content: [],
            },
            status: "inProgress",
          },
        },
      });
      yield* runtime.emit({
        id: asEventId("evt-browser-complete"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:01.000Z",
        method: "item/completed",
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-1"),
        itemId: asItemId("browser_1"),
        payload: {
          completedAtMs: 1_778_000_001_000,
          threadId: "thread-1",
          turnId: "turn-1",
          item: {
            type: "mcpToolCall",
            id: "browser_1",
            server: "node_repl",
            tool: "js",
            arguments: { code: "await tab.playwright.domSnapshot()", title: "Inspect checkout" },
            durationMs: 12,
            error: null,
            result: {
              _meta: {
                "codex/toolSurface": {
                  kind: "browserUse",
                  backend: "chrome",
                  openTabs: [
                    {
                      pageUrl: "https://www.mathworks.com/help/matlab/",
                      faviconUrl: "https://www.mathworks.com/favicon.ico",
                      faviconUrlDark: "https://www.mathworks.com/favicon-dark.ico",
                      url: "https://www.mathworks.com/help/matlab/",
                    },
                  ],
                },
                browser_use: { url: serializedOverContractUrl },
              },
              content: [],
            },
            status: "completed",
          },
        },
      });
      yield* runtime.emit({
        id: asEventId("evt-computer-use-complete"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:02.000Z",
        method: "item/completed",
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-1"),
        itemId: asItemId("computer_2"),
        payload: {
          completedAtMs: 1_778_000_002_000,
          threadId: "thread-1",
          turnId: "turn-1",
          item: {
            type: "mcpToolCall",
            id: "computer_2",
            server: "computer-use",
            tool: "type_text",
            arguments: { text: "Hello world", app: "TextEdit" },
            durationMs: 12,
            error: null,
            result: {
              _meta: {
                "codex/toolSurface": {
                  kind: "computerUse",
                  app: { kind: "displayName", displayName: "TextEdit" },
                },
              },
              content: [],
            },
            status: "completed",
          },
        },
      });

      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.deepStrictEqual(
        events.map((event) => ({
          type: event.type,
          title: "title" in event.payload ? event.payload.title : undefined,
          toolSurface: "toolSurface" in event.payload ? event.payload.toolSurface : undefined,
          toolIcon: "toolIcon" in event.payload ? event.payload.toolIcon : undefined,
          toolSource: "toolSource" in event.payload ? event.payload.toolSource : undefined,
        })),
        [
          {
            type: "item.started",
            title: `${"a".repeat(39)} ${"a".repeat(38)}😀…`,
            toolSurface: "computer",
            toolIcon: {
              _tag: "native-app",
              app: { _tag: "app-id", appId: "com.apple.finder" },
            },
            toolSource: {
              key: "native-app:com.apple.finder",
              name: "Finder",
              kind: "computer",
              icon: {
                _tag: "native-app",
                app: { _tag: "app-id", appId: "com.apple.finder" },
              },
            },
          },
          {
            type: "item.completed",
            title: "Inspect checkout",
            toolSurface: "browser",
            toolIcon: {
              _tag: "website",
              pageUrl: "https://www.mathworks.com/help/matlab/",
              faviconUrl: "https://www.mathworks.com/favicon.ico",
              faviconUrlDark: "https://www.mathworks.com/favicon-dark.ico",
            },
            toolSource: {
              key: "browser-use:chrome",
              name: "Chrome",
              kind: "integration",
              icon: {
                _tag: "native-app",
                app: { _tag: "display-name", displayName: "Google Chrome" },
              },
            },
          },
          {
            type: "item.completed",
            title: "Typed text in TextEdit",
            toolSurface: "computer",
            toolIcon: {
              _tag: "native-app",
              app: { _tag: "display-name", displayName: "TextEdit" },
            },
            toolSource: {
              key: "native-app-name:textedit",
              name: "TextEdit",
              kind: "computer",
              icon: {
                _tag: "native-app",
                app: { _tag: "display-name", displayName: "TextEdit" },
              },
            },
          },
        ],
      );
    }),
  );

  it.effect("preserves failed and declined outcomes on completed tool items", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const maxLengthAppId = `com.${"a".repeat(508)}`;
      const collidingMaxLengthAppId = `com.${"a".repeat(507)}b`;
      const longAppSourceKeys: string[] = [];
      const items = [
        {
          type: "commandExecution",
          id: "failed-command",
          command: "vp test run",
          commandActions: [],
          cwd: "/tmp",
          exitCode: 1,
          status: "failed",
        },
        {
          type: "mcpToolCall",
          id: "failed-mcp",
          server: "simulator",
          tool: "build",
          arguments: {},
          error: { message: "Build failed" },
          status: "failed",
        },
        {
          type: "mcpToolCall",
          id: "failed-computer",
          server: "computer-use",
          tool: "click",
          arguments: { app: "Finder" },
          error: { message: "Click failed" },
          result: {
            _meta: {
              "codex/toolSurface": {
                kind: "computerUse",
                app: { kind: "appId", appId: maxLengthAppId },
              },
            },
            content: [],
          },
          status: "failed",
        },
        {
          type: "mcpToolCall",
          id: "failed-computer-collision",
          server: "computer-use",
          tool: "click",
          arguments: { app: "Other" },
          error: { message: "Click failed" },
          result: {
            _meta: {
              "codex/toolSurface": {
                kind: "computerUse",
                app: { kind: "appId", appId: collidingMaxLengthAppId },
              },
            },
            content: [],
          },
          status: "failed",
        },
        {
          type: "fileChange",
          id: "declined-change",
          changes: [],
          status: "declined",
        },
      ] as const;

      for (const item of items) {
        const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

        yield* runtime.emit({
          id: asEventId(`evt-${item.id}`),
          kind: "notification",
          provider: ProviderDriverKind.make("codex"),
          createdAt: "2026-01-01T00:00:00.000Z",
          method: "item/completed",
          threadId: asThreadId("thread-1"),
          turnId: asTurnId("turn-1"),
          itemId: asItemId(item.id),
          payload: {
            completedAtMs: 1_778_000_000_000,
            threadId: "thread-1",
            turnId: "turn-1",
            item,
          },
        });

        const firstEvent = yield* Fiber.join(firstEventFiber);
        NodeAssert.equal(firstEvent._tag, "Some");
        if (firstEvent._tag !== "Some" || firstEvent.value.type !== "item.completed") {
          return;
        }
        NodeAssert.equal(firstEvent.value.payload.status, item.status);
        if (item.id.startsWith("failed-computer")) {
          NodeAssert.equal(firstEvent.value.payload.title, "computer-use · click");
          const sourceKey = firstEvent.value.payload.toolSource?.key;
          NodeAssert.equal(sourceKey?.length, 512);
          if (sourceKey) longAppSourceKeys.push(sourceKey);
        }
      }
      NodeAssert.equal(new Set(longAppSourceKeys).size, 2);
    }),
  );

  it.effect("maps completed plan items to canonical proposed-plan completion events", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      const event: ProviderEvent = {
        id: asEventId("evt-plan-complete"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "item/completed",
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-1"),
        itemId: asItemId("plan_1"),
        payload: {
          completedAtMs: 1_778_000_000_000,
          threadId: "thread-1",
          turnId: "turn-1",
          item: {
            type: "plan",
            id: "plan_1",
            text: "## Final plan\n\n- one\n- two",
          },
        },
      };

      yield* runtime.emit(event);
      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "turn.proposed.completed");
      if (firstEvent.value.type !== "turn.proposed.completed") {
        return;
      }
      NodeAssert.equal(firstEvent.value.turnId, "turn-1");
      NodeAssert.equal(firstEvent.value.payload.planMarkdown, "## Final plan\n\n- one\n- two");
    }),
  );

  it.effect("maps plan deltas to canonical proposed-plan delta events", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-plan-delta"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "item/plan/delta",
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-1"),
        itemId: asItemId("plan_1"),
        payload: {
          threadId: "thread-1",
          turnId: "turn-1",
          itemId: "plan_1",
          delta: "## Final plan",
        },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "turn.proposed.delta");
      if (firstEvent.value.type !== "turn.proposed.delta") {
        return;
      }
      NodeAssert.equal(firstEvent.value.turnId, "turn-1");
      NodeAssert.equal(firstEvent.value.payload.delta, "## Final plan");
    }),
  );

  it.effect("maps session/closed lifecycle events to canonical session.exited runtime events", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      const event: ProviderEvent = {
        id: asEventId("evt-session-closed"),
        kind: "session",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "session/closed",
        message: "Session stopped",
      };

      yield* runtime.emit(event);
      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "session.exited");
      if (firstEvent.value.type !== "session.exited") {
        return;
      }
      NodeAssert.equal(firstEvent.value.threadId, "thread-1");
      NodeAssert.equal(firstEvent.value.payload.reason, "Session stopped");
    }),
  );

  it.effect("maps retryable Codex error notifications to runtime.warning", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-retryable-error"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "error",
        turnId: asTurnId("turn-1"),
        payload: {
          threadId: "thread-1",
          turnId: "turn-1",
          error: {
            message: "Reconnecting... 2/5",
          },
          willRetry: true,
        },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "runtime.warning");
      if (firstEvent.value.type !== "runtime.warning") {
        return;
      }
      NodeAssert.equal(firstEvent.value.turnId, "turn-1");
      NodeAssert.equal(firstEvent.value.payload.message, "Reconnecting... 2/5");
    }),
  );

  it.effect("maps process stderr notifications to runtime.warning", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-process-stderr"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "process/stderr",
        turnId: asTurnId("turn-1"),
        message: "The filename or extension is too long. (os error 206)",
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "runtime.warning");
      if (firstEvent.value.type !== "runtime.warning") {
        return;
      }
      NodeAssert.equal(firstEvent.value.turnId, "turn-1");
      NodeAssert.equal(
        firstEvent.value.payload.message,
        "The filename or extension is too long. (os error 206)",
      );
    }),
  );

  it.effect("maps realtime started notifications with upstream realtime session ids", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-realtime-started"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "thread/realtime/started",
        payload: {
          threadId: "thread-1",
          realtimeSessionId: "realtime-session-1",
          version: "v2",
        },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "thread.realtime.started");
      if (firstEvent.value.type !== "thread.realtime.started") {
        return;
      }
      NodeAssert.equal(firstEvent.value.threadId, "thread-1");
      NodeAssert.equal(firstEvent.value.payload.realtimeSessionId, "realtime-session-1");
    }),
  );

  it.effect("maps fatal websocket stderr notifications to runtime.error", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-process-stderr-websocket"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "process/stderr",
        turnId: asTurnId("turn-1"),
        message:
          "2026-03-31T18:14:06.833399Z ERROR codex_api::endpoint::responses_websocket: failed to connect to websocket: HTTP error: 503 Service Unavailable, url: wss://chatgpt.com/backend-api/codex/responses",
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "runtime.error");
      if (firstEvent.value.type !== "runtime.error") {
        return;
      }
      NodeAssert.equal(firstEvent.value.turnId, "turn-1");
      NodeAssert.equal(firstEvent.value.payload.class, "provider_error");
      NodeAssert.equal(
        firstEvent.value.payload.message,
        "2026-03-31T18:14:06.833399Z ERROR codex_api::endpoint::responses_websocket: failed to connect to websocket: HTTP error: 503 Service Unavailable, url: wss://chatgpt.com/backend-api/codex/responses",
      );
    }),
  );

  it.effect("preserves request type when mapping serverRequest/resolved", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      const event: ProviderEvent = {
        id: asEventId("evt-request-resolved"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "serverRequest/resolved",
        requestKind: "command",
        requestId: ApprovalRequestId.make("req-1"),
        payload: {
          threadId: "thread-1",
          requestId: "req-1",
        },
      };

      yield* runtime.emit(event);
      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "request.resolved");
      if (firstEvent.value.type !== "request.resolved") {
        return;
      }
      NodeAssert.equal(firstEvent.value.payload.requestType, "command_execution_approval");
    }),
  );

  it.effect("names the edited files in an apply-patch approval without a reason", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-apply-patch"),
        kind: "request",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-08-24T00:00:00.000Z",
        method: "applyPatchApproval",
        requestKind: "file-change",
        requestId: ApprovalRequestId.make("req-patch"),
        turnId: asTurnId("turn-1"),
        payload: {
          callId: "call-1",
          conversationId: "provider-thread-1",
          fileChanges: {
            "/tmp/removed.md": { type: "delete", content: "gone" },
            "/tmp/added.ts": { type: "add", content: "export {};" },
          },
        },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some" || firstEvent.value.type !== "request.opened") {
        return;
      }
      NodeAssert.equal(firstEvent.value.payload.requestType, "apply_patch_approval");
      NodeAssert.equal(
        firstEvent.value.payload.detail,
        "add /tmp/added.ts\ndelete /tmp/removed.md",
      );
    }),
  );

  it.effect("keeps the reason when an apply-patch approval carries one", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-apply-patch-reason"),
        kind: "request",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-08-24T00:00:00.000Z",
        method: "applyPatchApproval",
        requestKind: "file-change",
        requestId: ApprovalRequestId.make("req-patch-reason"),
        turnId: asTurnId("turn-1"),
        payload: {
          callId: "call-2",
          conversationId: "provider-thread-1",
          reason: "Needs to rewrite the changelog",
          fileChanges: { "/tmp/CHANGELOG.md": { type: "add", content: "x" } },
        },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some" || firstEvent.value.type !== "request.opened") {
        return;
      }
      NodeAssert.equal(firstEvent.value.payload.detail, "Needs to rewrite the changelog");
    }),
  );

  it.effect("falls back to the grant root for a file-change approval without a reason", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-file-change"),
        kind: "request",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-08-24T00:00:00.000Z",
        method: "item/fileChange/requestApproval",
        requestKind: "file-change",
        requestId: ApprovalRequestId.make("req-file-change"),
        turnId: asTurnId("turn-1"),
        payload: {
          itemId: "item-1",
          grantRoot: "/tmp/workspace",
          startedAtMs: 0,
          threadId: "provider-thread-1",
          turnId: "turn-1",
        },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some" || firstEvent.value.type !== "request.opened") {
        return;
      }
      NodeAssert.equal(firstEvent.value.payload.requestType, "file_change_approval");
      NodeAssert.equal(firstEvent.value.payload.detail, "/tmp/workspace");
    }),
  );

  it.effect("prefers the edited files over a blank apply-patch reason", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-apply-patch-blank"),
        kind: "request",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-08-24T00:00:00.000Z",
        method: "applyPatchApproval",
        requestKind: "file-change",
        requestId: ApprovalRequestId.make("req-patch-blank"),
        turnId: asTurnId("turn-1"),
        payload: {
          callId: "call-3",
          conversationId: "provider-thread-1",
          reason: "   ",
          fileChanges: {
            "/tmp/moved.ts": { type: "update", unified_diff: "@@", move_path: "/tmp/renamed.ts" },
          },
        },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some" || firstEvent.value.type !== "request.opened") {
        return;
      }
      NodeAssert.equal(firstEvent.value.payload.detail, "update /tmp/moved.ts -> /tmp/renamed.ts");
    }),
  );

  it.effect("caps the described files in an oversized apply-patch approval", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      const fileChanges = Object.fromEntries(
        Array.from({ length: 25 }, (_unused, index) => [
          `/tmp/file-${String(index).padStart(2, "0")}.ts`,
          { type: "add", content: "x" },
        ]),
      );

      yield* runtime.emit({
        id: asEventId("evt-apply-patch-many"),
        kind: "request",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-08-24T00:00:00.000Z",
        method: "applyPatchApproval",
        requestKind: "file-change",
        requestId: ApprovalRequestId.make("req-patch-many"),
        turnId: asTurnId("turn-1"),
        payload: { callId: "call-4", conversationId: "provider-thread-1", fileChanges },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some" || firstEvent.value.type !== "request.opened") {
        return;
      }
      const detail = firstEvent.value.payload.detail ?? "";
      NodeAssert.equal(detail.split("\n").length, 21);
      NodeAssert.ok(detail.endsWith("+5 more"));
    }),
  );

  it.effect("leaves an apply-patch approval without changes or a reason undetailed", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-apply-patch-empty"),
        kind: "request",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-08-24T00:00:00.000Z",
        method: "applyPatchApproval",
        requestKind: "file-change",
        requestId: ApprovalRequestId.make("req-patch-empty"),
        turnId: asTurnId("turn-1"),
        payload: { callId: "call-5", conversationId: "provider-thread-1", fileChanges: {} },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some" || firstEvent.value.type !== "request.opened") {
        return;
      }
      NodeAssert.equal(firstEvent.value.payload.detail, undefined);
    }),
  );

  it.effect("maps MCP elicitation requests into app access approvals", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-mcp-elicitation"),
        kind: "request",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-08-24T00:00:00.000Z",
        method: "mcpServer/elicitation/request",
        requestKind: "mcp-elicitation",
        requestId: ApprovalRequestId.make("req-safari"),
        turnId: asTurnId("turn-1"),
        payload: {
          mode: "form",
          message: "Allow ChatGPT to use Safari?",
          serverName: "computer-use",
          threadId: "provider-thread-1",
          turnId: "turn-1",
          _meta: { app_name: "Safari", persist: ["session", "always"] },
          requestedSchema: { type: "object", properties: {} },
        },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some" || firstEvent.value.type !== "request.opened") {
        return;
      }
      NodeAssert.equal(firstEvent.value.payload.requestType, "mcp_elicitation_approval");
      NodeAssert.equal(firstEvent.value.payload.appName, "Safari");
      NodeAssert.equal(firstEvent.value.payload.detail, "Allow ChatGPT to use Safari?");
      NodeAssert.deepStrictEqual(firstEvent.value.payload.options, [
        { decision: "cancel", label: "Cancel" },
        { decision: "decline", label: "Decline" },
        { decision: "acceptForSession", label: "Always allow this session" },
        { decision: "acceptAlways", label: "Always allow" },
        { decision: "accept", label: "Approve" },
      ]);
    }),
  );

  it.effect("preserves MCP elicitation type when an app access request resolves", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-mcp-elicitation-resolved"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-08-24T00:00:00.000Z",
        method: "item/requestApproval/decision",
        requestKind: "mcp-elicitation",
        requestId: ApprovalRequestId.make("req-safari"),
        payload: { decision: "acceptAlways" },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some" || firstEvent.value.type !== "request.resolved") {
        return;
      }
      NodeAssert.equal(firstEvent.value.payload.requestType, "mcp_elicitation_approval");
      NodeAssert.equal(firstEvent.value.payload.decision, "acceptAlways");
    }),
  );

  it.effect("preserves file-read request type when mapping serverRequest/resolved", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      const event: ProviderEvent = {
        id: asEventId("evt-file-read-request-resolved"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "serverRequest/resolved",
        requestKind: "file-read",
        requestId: ApprovalRequestId.make("req-file-read-1"),
        payload: {
          threadId: "thread-1",
          requestId: "req-file-read-1",
        },
      };

      yield* runtime.emit(event);
      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "request.resolved");
      if (firstEvent.value.type !== "request.resolved") {
        return;
      }
      NodeAssert.equal(firstEvent.value.payload.requestType, "file_read_approval");
    }),
  );

  it.effect("preserves explicit empty multi-select user-input answers", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      const event: ProviderEvent = {
        id: asEventId("evt-user-input-empty"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "item/tool/requestUserInput/answered",
        payload: {
          answers: {
            scope: {
              answers: [],
            },
          },
        },
      };

      yield* runtime.emit(event);
      const firstEvent = yield* Fiber.join(firstEventFiber);

      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "user-input.resolved");
      if (firstEvent.value.type !== "user-input.resolved") {
        return;
      }
      NodeAssert.deepEqual(firstEvent.value.payload.answers, {
        scope: [],
      });
    }),
  );

  it.effect("maps windowsSandbox/setupCompleted to session state and warning on failure", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 2)).pipe(
        Effect.forkChild,
      );

      const event: ProviderEvent = {
        id: asEventId("evt-windows-sandbox-failed"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "windowsSandbox/setupCompleted",
        message: "Sandbox setup failed",
        payload: {
          mode: "unelevated",
          success: false,
          error: "unsupported environment",
        },
      };

      yield* runtime.emit(event);
      const events = Array.from(yield* Fiber.join(eventsFiber));

      NodeAssert.equal(events.length, 2);

      const firstEvent = events[0];
      const secondEvent = events[1];

      NodeAssert.equal(firstEvent?.type, "session.state.changed");
      if (firstEvent?.type === "session.state.changed") {
        NodeAssert.equal(firstEvent.payload.state, "error");
        NodeAssert.equal(firstEvent.payload.reason, "Sandbox setup failed");
      }

      NodeAssert.equal(secondEvent?.type, "runtime.warning");
      if (secondEvent?.type === "runtime.warning") {
        NodeAssert.equal(secondEvent.payload.message, "Sandbox setup failed");
      }
    }),
  );

  it.effect(
    "maps requestUserInput requests and answered notifications to canonical user-input events",
    () =>
      Effect.gen(function* () {
        const { adapter, runtime } = yield* startLifecycleRuntime();
        const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 2)).pipe(
          Effect.forkChild,
        );

        yield* runtime.emit({
          id: asEventId("evt-user-input-requested"),
          kind: "request",
          provider: ProviderDriverKind.make("codex"),
          threadId: asThreadId("thread-1"),
          createdAt: "2026-01-01T00:00:00.000Z",
          method: "item/tool/requestUserInput",
          requestId: ApprovalRequestId.make("req-user-input-1"),
          payload: {
            itemId: "item-user-input-1",
            threadId: "thread-1",
            turnId: "turn-1",
            questions: [
              {
                id: "sandbox_mode",
                header: "Sandbox",
                question: "Which mode should be used?",
                options: [
                  {
                    label: "workspace-write",
                    description: "Allow workspace writes only",
                  },
                ],
              },
            ],
          },
        } satisfies ProviderEvent);
        yield* runtime.emit({
          id: asEventId("evt-user-input-resolved"),
          kind: "notification",
          provider: ProviderDriverKind.make("codex"),
          threadId: asThreadId("thread-1"),
          createdAt: "2026-01-01T00:00:00.000Z",
          method: "item/tool/requestUserInput/answered",
          requestId: ApprovalRequestId.make("req-user-input-1"),
          payload: {
            answers: {
              sandbox_mode: {
                answers: ["workspace-write"],
              },
            },
          },
        } satisfies ProviderEvent);

        const events = Array.from(yield* Fiber.join(eventsFiber));
        NodeAssert.equal(events[0]?.type, "user-input.requested");
        if (events[0]?.type === "user-input.requested") {
          NodeAssert.equal(events[0].requestId, "req-user-input-1");
          NodeAssert.equal(events[0].payload.questions[0]?.id, "sandbox_mode");
          NodeAssert.equal(events[0].payload.questions[0]?.multiSelect, false);
        }

        NodeAssert.equal(events[1]?.type, "user-input.resolved");
        if (events[1]?.type === "user-input.resolved") {
          NodeAssert.equal(events[1].requestId, "req-user-input-1");
          NodeAssert.deepEqual(events[1].payload.answers, {
            sandbox_mode: "workspace-write",
          });
        }
      }),
  );

  it.effect("maps async agent questions without ending the turn", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 2)).pipe(
        Effect.forkChild,
      );
      yield* runtime.emit({
        id: asEventId("evt-async-question"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "item/completed",
        payload: {
          completedAtMs: 0,
          threadId: "thread-1",
          turnId: "turn-1",
          item: {
            type: "agentMessage",
            id: "async-question-1",
            text: "Which package manager?\n- pnpm\n- npm\n\nWhat should it be named?",
            phase: "final_answer",
            delivery: "async",
            questions: [
              { title: "Which package manager?", options: ["pnpm", "npm"] },
              { title: "What should it be named?" },
            ],
          },
        },
      });
      yield* runtime.emit({
        id: asEventId("evt-async-continued"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:01.000Z",
        method: "item/agentMessage/delta",
        payload: {
          threadId: "thread-1",
          turnId: "turn-1",
          itemId: "message-2",
          delta: "I will keep working.",
        },
      });
      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.equal(events[0]?.type, "user-input.requested");
      NodeAssert.equal(events[0]?.requestId, "codex-async:thread-1:async-question-1");
      NodeAssert.deepEqual(events[0]?.payload, {
        responseMode: "message",
        questions: [
          {
            id: "0",
            header: "Question",
            question: "Which package manager?",
            options: [
              { label: "pnpm", description: "" },
              { label: "npm", description: "" },
            ],
            allowCustomAnswer: true,
            multiSelect: false,
          },
          {
            id: "1",
            header: "Question",
            question: "What should it be named?",
            options: [],
            allowCustomAnswer: true,
            multiSelect: false,
          },
        ],
      });
      NodeAssert.equal(events[1]?.type, "content.delta");
    }),
  );

  it.effect("unwraps Codex token usage payloads for context window events", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-codex-thread-token-usage-updated"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "thread/tokenUsage/updated",
        payload: {
          threadId: "thread-1",
          turnId: "turn-1",
          tokenUsage: {
            total: {
              inputTokens: 11_833,
              cachedInputTokens: 3456,
              outputTokens: 6,
              reasoningOutputTokens: 0,
              totalTokens: 11_839,
            },
            last: {
              inputTokens: 120,
              cachedInputTokens: 0,
              outputTokens: 6,
              reasoningOutputTokens: 0,
              totalTokens: 126,
            },
            modelContextWindow: 258_400,
          },
        },
      } satisfies ProviderEvent);

      const firstEvent = yield* Fiber.join(firstEventFiber);
      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "thread.token-usage.updated");
      if (firstEvent.value.type !== "thread.token-usage.updated") {
        return;
      }

      NodeAssert.deepEqual(firstEvent.value.payload.usage, {
        usedTokens: 126,
        totalProcessedTokens: 11_839,
        maxTokens: 258_400,
        inputTokens: 120,
        cachedInputTokens: 0,
        outputTokens: 6,
        reasoningOutputTokens: 0,
        lastUsedTokens: 126,
        lastInputTokens: 120,
        lastCachedInputTokens: 0,
        lastOutputTokens: 6,
        lastReasoningOutputTokens: 0,
        compactsAutomatically: true,
      });
    }),
  );

  // Production calls startSession from a request fiber that finishes as soon as
  // the session exists. `Effect.forkChild` made the runtime event consumer a
  // child of that fiber, and Effect interrupts a fiber's children when it
  // completes, so the consumer died on return and every event the session
  // emitted afterwards was dropped. The other tests here start the session from
  // the test fiber, which never completes, so the consumer survived and the bug
  // stayed invisible. Starting it in a fiber that finishes reproduces
  // production.
  it.effect("keeps consuming runtime events after the startSession fiber completes", () =>
    Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const startSessionFiber = yield* adapter
        .startSession({
          provider: ProviderDriverKind.make("codex"),
          threadId: asThreadId("thread-outlives-start"),
          runtimeMode: "full-access",
        })
        .pipe(Effect.forkChild);
      yield* Fiber.join(startSessionFiber);

      const runtime = lifecycleRuntimeFactory.lastRuntime;
      NodeAssert.ok(runtime);

      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);
      yield* runtime.emit({
        id: asEventId("evt-after-start-session"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "item/completed",
        threadId: asThreadId("thread-outlives-start"),
        turnId: asTurnId("turn-1"),
        itemId: asItemId("msg_after_start"),
        payload: {
          completedAtMs: 1_778_000_000_000,
          threadId: "thread-outlives-start",
          turnId: "turn-1",
          item: {
            type: "agentMessage",
            id: "msg_after_start",
            text: "emitted after startSession returned",
          },
        },
      });

      const firstEvent = yield* Fiber.join(firstEventFiber).pipe(Effect.timeout("10 seconds"));
      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") {
        return;
      }
      NodeAssert.equal(firstEvent.value.type, "item.completed");
      // Live clock so the timeout above is real: under the default test clock it
      // waits on virtual time that never advances, and a regression would hang
      // until the suite timeout instead of failing here.
    }).pipe(TestClock.withLive),
  );
});

it.effect("reattaches exactly once after transport loss without starting a model turn", () =>
  Effect.gen(function* () {
    const runtimeQueue = yield* Queue.unbounded<FakeCodexRuntime>();
    const sessionModes: Array<"create" | "reuse" | "adopt" | undefined> = [];
    const runtimeFactory = vi.fn((options: CodexSessionRuntimeOptions) => {
      const runtime = new FakeCodexRuntime(options);
      return Queue.offer(runtimeQueue, runtime).pipe(Effect.as(runtime));
    });
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          appServerHost: {
            socketPath: "/tmp/t3-codex-auto-reattach.sock",
            ensure: Effect.succeed("/tmp/t3-codex-auto-reattach.sock"),
          },
          makeProviderHostRuntime: ({ options, sessionMode }) => {
            sessionModes.push(sessionMode);
            return runtimeFactory(options);
          },
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    yield* Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const threadId = asThreadId("thread-auto-reattach");
      const started = yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        runtimeMode: "full-access",
      });
      const original = yield* Queue.take(runtimeQueue);
      original.getSessionImpl.mockResolvedValue({
        ...started,
        status: "error",
        resumeCursor: { threadId: "provider-thread-auto-reattach" },
        updatedAt: "2026-01-01T00:00:01.000Z",
      });

      yield* original.emit({
        id: asEventId("evt-auto-reattach"),
        kind: "session",
        provider: ProviderDriverKind.make("codex"),
        threadId,
        createdAt: "2026-01-01T00:00:01.000Z",
        method: "session/disconnected",
        message: "Connection dropped.",
      });

      const replacement = yield* Queue.take(runtimeQueue);
      yield* Effect.promise(() => replacement.started);

      NodeAssert.equal(runtimeFactory.mock.calls.length, 2);
      NodeAssert.deepStrictEqual(sessionModes, ["create", "adopt"]);
      NodeAssert.equal(original.detachImpl.mock.calls.length, 1);
      NodeAssert.equal(original.closeImpl.mock.calls.length, 0);
      NodeAssert.equal(original.sendTurnImpl.mock.calls.length, 0);
      NodeAssert.equal(original.interruptTurnImpl.mock.calls.length, 0);
      NodeAssert.equal(replacement.sendTurnImpl.mock.calls.length, 0);
      NodeAssert.equal(replacement.interruptTurnImpl.mock.calls.length, 0);
      NodeAssert.deepStrictEqual(replacement.options.resumeCursor, {
        threadId: "provider-thread-auto-reattach",
      });
      NodeAssert.equal(yield* adapter.hasSession(threadId), true);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("keeps recovered sessions in attach-existing mode across transport reconnects", () =>
  Effect.gen(function* () {
    const runtimeQueue = yield* Queue.unbounded<FakeCodexRuntime>();
    const sessionModes: Array<"create" | "reuse" | "adopt" | undefined> = [];
    const runtimeFactory = vi.fn((options: CodexSessionRuntimeOptions) => {
      const runtime = new FakeCodexRuntime(options);
      return Queue.offer(runtimeQueue, runtime).pipe(Effect.as(runtime));
    });
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          appServerHost: {
            socketPath: "/tmp/t3-codex-existing-auto-reattach.sock",
            ensure: Effect.succeed("/tmp/t3-codex-existing-auto-reattach.sock"),
          },
          makeProviderHostRuntime: ({ options, sessionMode }) => {
            sessionModes.push(sessionMode);
            return runtimeFactory(options);
          },
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    yield* Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const reattachSession = adapter.reattachSession;
      NodeAssert.ok(reattachSession);
      const threadId = asThreadId("thread-existing-auto-reattach");
      const started = yield* reattachSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        resumeCursor: { threadId: "provider-thread-existing-auto-reattach" },
        runtimeMode: "full-access",
      });
      const original = yield* Queue.take(runtimeQueue);
      original.getSessionImpl.mockResolvedValue({
        ...started,
        status: "error",
        updatedAt: "2026-01-01T00:00:01.000Z",
      });

      yield* original.emit({
        id: asEventId("evt-existing-auto-reattach"),
        kind: "session",
        provider: ProviderDriverKind.make("codex"),
        threadId,
        createdAt: "2026-01-01T00:00:01.000Z",
        method: "session/disconnected",
        message: "Connection dropped.",
      });

      const replacement = yield* Queue.take(runtimeQueue);
      yield* Effect.promise(() => replacement.started);

      NodeAssert.deepStrictEqual(sessionModes, ["adopt", "adopt"]);
      NodeAssert.equal(original.detachImpl.mock.calls.length, 1);
      NodeAssert.equal(original.closeImpl.mock.calls.length, 0);
      NodeAssert.equal(replacement.options.threadConfig, undefined);
      NodeAssert.equal(replacement.sendTurnImpl.mock.calls.length, 0);
      NodeAssert.equal(yield* adapter.hasSession(threadId), true);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("accepts a provider-error snapshot after transport reconnection", () =>
  Effect.gen(function* () {
    const runtimeQueue = yield* Queue.unbounded<FakeCodexRuntime>();
    let runtimeCount = 0;
    const runtimeFactory = vi.fn((options: CodexSessionRuntimeOptions) => {
      runtimeCount += 1;
      const runtime = new FakeCodexRuntime(options);
      if (runtimeCount === 2) {
        const providerError: ProviderSession = {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: options.providerInstanceId,
          threadId: options.threadId,
          status: "error",
          runtimeMode: options.runtimeMode,
          cwd: options.cwd,
          ...(options.resumeCursor ? { resumeCursor: options.resumeCursor } : {}),
          lastError: "The provider turn failed.",
          createdAt: "2026-01-01T00:00:00.000Z",
          updatedAt: "2026-01-01T00:00:01.000Z",
        };
        runtime.startImpl.mockResolvedValue(providerError);
        runtime.getSessionImpl.mockResolvedValue(providerError);
      }
      return Queue.offer(runtimeQueue, runtime).pipe(Effect.as(runtime));
    });
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          appServerHost: {
            socketPath: "/tmp/t3-codex-auto-reattach-provider-error.sock",
            ensure: Effect.succeed("/tmp/t3-codex-auto-reattach-provider-error.sock"),
          },
          makeProviderHostRuntime: ({ options }) => runtimeFactory(options),
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    yield* Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const threadId = asThreadId("thread-auto-reattach-provider-error");
      const started = yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        resumeCursor: { threadId: "provider-thread-auto-reattach-provider-error" },
        runtimeMode: "full-access",
      });
      const original = yield* Queue.take(runtimeQueue);
      original.getSessionImpl.mockResolvedValue({
        ...started,
        status: "error",
        updatedAt: "2026-01-01T00:00:01.000Z",
      });

      yield* original.emit({
        id: asEventId("evt-auto-reattach-provider-error"),
        kind: "session",
        provider: ProviderDriverKind.make("codex"),
        threadId,
        createdAt: "2026-01-01T00:00:01.000Z",
        method: "session/disconnected",
        message: "Connection dropped.",
      });

      const replacement = yield* Queue.take(runtimeQueue);
      yield* Effect.promise(() => replacement.started);
      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;

      NodeAssert.equal(runtimeFactory.mock.calls.length, 2);
      NodeAssert.equal(original.detachImpl.mock.calls.length, 1);
      NodeAssert.equal(replacement.detachImpl.mock.calls.length, 0);
      NodeAssert.equal(yield* adapter.hasSession(threadId), true);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("stops reconnecting when the detached host reports the session missing", () =>
  Effect.gen(function* () {
    const runtimeQueue = yield* Queue.unbounded<FakeCodexRuntime>();
    let runtimeCount = 0;
    const runtimeFactory = vi.fn((options: CodexSessionRuntimeOptions) => {
      runtimeCount += 1;
      const runtime = new FakeCodexRuntime(options);
      if (runtimeCount === 2) {
        runtime.startError = new CodexSessionRuntimeThreadIdMissingError({
          threadId: options.threadId,
        });
      }
      return Queue.offer(runtimeQueue, runtime).pipe(Effect.as(runtime));
    });
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          appServerHost: {
            socketPath: "/tmp/t3-codex-auto-reattach-missing.sock",
            ensure: Effect.succeed("/tmp/t3-codex-auto-reattach-missing.sock"),
          },
          makeProviderHostRuntime: ({ options }) => runtimeFactory(options),
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    yield* Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const threadId = asThreadId("thread-auto-reattach-missing");
      const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 2)).pipe(
        Effect.forkChild,
      );
      const started = yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        resumeCursor: { threadId: "provider-thread-auto-reattach-missing" },
        runtimeMode: "full-access",
      });
      const original = yield* Queue.take(runtimeQueue);
      original.getSessionImpl.mockResolvedValue({
        ...started,
        status: "error",
        updatedAt: "2026-01-01T00:00:01.000Z",
      });

      yield* original.emit({
        id: asEventId("evt-auto-reattach-missing"),
        kind: "session",
        provider: ProviderDriverKind.make("codex"),
        threadId,
        createdAt: "2026-01-01T00:00:01.000Z",
        method: "session/disconnected",
        message: "Connection dropped.",
      });

      const missingReplacement = yield* Queue.take(runtimeQueue);
      yield* Effect.yieldNow;
      yield* TestClock.adjust("10 seconds");
      yield* Effect.yieldNow;

      NodeAssert.equal(runtimeFactory.mock.calls.length, 2);
      NodeAssert.equal(original.detachImpl.mock.calls.length, 1);
      NodeAssert.equal(missingReplacement.detachImpl.mock.calls.length, 1);
      NodeAssert.equal(yield* adapter.hasSession(threadId), false);
      const events = Array.from(yield* Fiber.join(eventsFiber));
      NodeAssert.equal(events.length, 2);
      NodeAssert.equal(events[0]?.type, "session.state.changed");
      if (events[0]?.type === "session.state.changed") {
        NodeAssert.equal(events[0].payload.state, "error");
        NodeAssert.equal(events[0].payload.activeTurnId, null);
      }
      NodeAssert.equal(events[1]?.type, "session.state.changed");
      if (events[1]?.type === "session.state.changed") {
        NodeAssert.equal(events[1].payload.state, "error");
        NodeAssert.equal(events[1].payload.activeTurnId, null);
      }
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("retries when a replacement attachment cannot reach the provider host", () =>
  Effect.gen(function* () {
    const runtimeQueue = yield* Queue.unbounded<FakeCodexRuntime>();
    let runtimeCount = 0;
    const runtimeFactory = vi.fn((options: CodexSessionRuntimeOptions) => {
      runtimeCount += 1;
      const runtime = new FakeCodexRuntime(options);
      if (runtimeCount === 2) {
        runtime.startError = new CodexErrors.CodexAppServerTransportError({
          operation: "read-input-stream",
          cause: new Error("Detached Codex host is still unavailable."),
        });
      }
      return Queue.offer(runtimeQueue, runtime).pipe(Effect.as(runtime));
    });
    const layer = Layer.effect(
      CodexAdapter,
      Effect.gen(function* () {
        const codexConfig = decodeCodexSettings({});
        return yield* makeCodexAdapter(codexConfig, {
          appServerHost: {
            socketPath: "/tmp/t3-codex-auto-reattach-retry.sock",
            ensure: Effect.succeed("/tmp/t3-codex-auto-reattach-retry.sock"),
          },
          makeProviderHostRuntime: ({ options }) => runtimeFactory(options),
        });
      }),
    ).pipe(
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(ServerSettingsService.layerTest()),
      Layer.provideMerge(providerSessionDirectoryTestLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    yield* Effect.gen(function* () {
      const adapter = yield* CodexAdapter;
      const threadId = asThreadId("thread-auto-reattach-retry");
      const started = yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        resumeCursor: { threadId: "provider-thread-auto-reattach-retry" },
        runtimeMode: "full-access",
      });
      const original = yield* Queue.take(runtimeQueue);
      original.getSessionImpl.mockResolvedValue({
        ...started,
        status: "error",
        updatedAt: "2026-01-01T00:00:01.000Z",
      });

      yield* original.emit({
        id: asEventId("evt-auto-reattach-retry"),
        kind: "session",
        provider: ProviderDriverKind.make("codex"),
        threadId,
        createdAt: "2026-01-01T00:00:01.000Z",
        method: "session/disconnected",
        message: "Connection dropped.",
      });

      const disconnectedReplacement = yield* Queue.take(runtimeQueue);
      const recoveredReplacementFiber = yield* Queue.take(runtimeQueue).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* TestClock.adjust("250 millis");
      const recoveredReplacement = yield* Fiber.join(recoveredReplacementFiber);
      yield* Effect.promise(() => recoveredReplacement.started);

      NodeAssert.equal(runtimeFactory.mock.calls.length, 3);
      NodeAssert.equal(disconnectedReplacement.detachImpl.mock.calls.length, 1);
      NodeAssert.equal(disconnectedReplacement.closeImpl.mock.calls.length, 0);
      NodeAssert.equal(recoveredReplacement.sendTurnImpl.mock.calls.length, 0);
      NodeAssert.equal(recoveredReplacement.interruptTurnImpl.mock.calls.length, 0);
      NodeAssert.equal(yield* adapter.hasSession(threadId), true);
    }).pipe(Effect.provide(layer));
  }),
);
const scopedLifecycleRuntimeFactory = makeScopedRuntimeFactory();
const scopedLifecycleLayer = it.layer(
  Layer.effect(
    CodexAdapter,
    Effect.gen(function* () {
      const codexConfig = decodeCodexSettings({});
      return yield* makeCodexAdapter(codexConfig, {
        makeRuntime: scopedLifecycleRuntimeFactory.factory,
      });
    }),
  ).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(ServerSettingsService.layerTest()),
    Layer.provideMerge(providerSessionDirectoryTestLayer),
    Layer.provideMerge(NodeServices.layer),
  ),
);

scopedLifecycleLayer("CodexAdapterLive scoped lifecycle", (it) => {
  it.effect("closes the externally owned session scope on stopSession", () =>
    Effect.gen(function* () {
      scopedLifecycleRuntimeFactory.releasedThreadIds.length = 0;
      const adapter = yield* CodexAdapter;

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-stop"),
        runtimeMode: "full-access",
      });

      const runtime = scopedLifecycleRuntimeFactory.lastRuntime;
      NodeAssert.ok(runtime);

      yield* adapter.stopSession(asThreadId("thread-stop"));

      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 1);
      NodeAssert.deepStrictEqual(scopedLifecycleRuntimeFactory.releasedThreadIds, [
        asThreadId("thread-stop"),
      ]);
      NodeAssert.equal(yield* adapter.hasSession(asThreadId("thread-stop")), false);
    }),
  );
  it.effect("drains terminal runtime events before stopSession completes", () =>
    Effect.gen(function* () {
      scopedLifecycleRuntimeFactory.releasedThreadIds.length = 0;
      const adapter = yield* CodexAdapter;
      const threadId = asThreadId("thread-stop-drain");

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        runtimeMode: "full-access",
      });
      const runtime = scopedLifecycleRuntimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      const delivery = runtime.blockEventDelivery();
      const eventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);
      runtime.closeImpl.mockImplementation(() => {
        runtime.emitUnsafe({
          id: asEventId("evt-session-closed-during-stop"),
          kind: "session",
          provider: ProviderDriverKind.make("codex"),
          threadId,
          createdAt: "2026-01-01T00:00:00.000Z",
          method: "session/closed",
          message: "Session stopped",
        });
        return Promise.resolve();
      });

      yield* Effect.gen(function* () {
        const stopFiber = yield* adapter.stopSession(threadId).pipe(Effect.forkChild);
        yield* Effect.promise(() => delivery.entered);
        yield* Effect.yieldNow;

        NodeAssert.equal(stopFiber.pollUnsafe(), undefined);

        delivery.release();
        yield* Fiber.join(stopFiber);
        const event = yield* Fiber.join(eventFiber);
        NodeAssert.equal(event._tag, "Some");
        if (event._tag !== "Some") return;
        NodeAssert.equal(event.value.type, "session.exited");
        if (event.value.type !== "session.exited") return;
        NodeAssert.equal(event.value.payload.exitKind, "graceful");
        NodeAssert.equal(event.value.payload.reason, "Session stopped");
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            delivery.release();
          }),
        ),
      );
    }),
  );

  it.effect("keeps a session attached when explicit stop fails", () =>
    Effect.gen(function* () {
      scopedLifecycleRuntimeFactory.releasedThreadIds.length = 0;
      const adapter = yield* CodexAdapter;
      const threadId = asThreadId("thread-stop-failure");

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        runtimeMode: "full-access",
      });
      const runtime = scopedLifecycleRuntimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      runtime.closeImpl.mockRejectedValueOnce(new Error("stop failed"));

      const stopped = yield* Effect.result(adapter.stopSession(threadId));

      NodeAssert.equal(stopped._tag, "Failure");
      NodeAssert.equal(yield* adapter.hasSession(threadId), true);
      NodeAssert.deepStrictEqual(scopedLifecycleRuntimeFactory.releasedThreadIds, []);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("keeps an errored provider session routable for a follow-up turn", () =>
    Effect.gen(function* () {
      scopedLifecycleRuntimeFactory.releasedThreadIds.length = 0;
      const adapter = yield* CodexAdapter;
      const threadId = asThreadId("thread-provider-error");

      const started = yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        runtimeMode: "full-access",
      });
      const runtime = scopedLifecycleRuntimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      runtime.getSessionImpl.mockResolvedValue({
        ...started,
        status: "error",
        lastError: "The previous turn failed.",
        updatedAt: "2026-01-01T00:00:01.000Z",
      });

      NodeAssert.equal(yield* adapter.hasSession(threadId), true);
      yield* adapter.sendTurn({
        threadId,
        input: "try again",
        attachments: [],
      });
      NodeAssert.equal(runtime.sendTurnImpl.mock.calls.length, 1);
      NodeAssert.deepStrictEqual(scopedLifecycleRuntimeFactory.releasedThreadIds, []);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("discards a closed attachment so the provider service can reattach", () =>
    Effect.gen(function* () {
      scopedLifecycleRuntimeFactory.releasedThreadIds.length = 0;
      const adapter = yield* CodexAdapter;
      NodeAssert.equal(adapter.capabilities.sessionPersistence, "process-bound");
      const threadId = asThreadId("thread-disconnected");

      const started = yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId,
        runtimeMode: "full-access",
      });
      const runtime = scopedLifecycleRuntimeFactory.lastRuntime;
      NodeAssert.ok(runtime);
      runtime.getSessionImpl.mockResolvedValue({
        ...started,
        status: "closed",
        updatedAt: "2026-01-01T00:00:01.000Z",
      });

      NodeAssert.equal(yield* adapter.hasSession(threadId), false);
      NodeAssert.equal(runtime.detachImpl.mock.calls.length, 1);
      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 0);
      NodeAssert.deepStrictEqual(scopedLifecycleRuntimeFactory.releasedThreadIds, [threadId]);
    }),
  );

  it.effect("closes process-bound sessions on stopAll", () =>
    Effect.gen(function* () {
      scopedLifecycleRuntimeFactory.releasedThreadIds.length = 0;
      const adapter = yield* CodexAdapter;

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-stop-all-1"),
        runtimeMode: "full-access",
      });
      const firstRuntime = scopedLifecycleRuntimeFactory.lastRuntime;
      NodeAssert.ok(firstRuntime);
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-stop-all-2"),
        runtimeMode: "full-access",
      });
      const secondRuntime = scopedLifecycleRuntimeFactory.lastRuntime;
      NodeAssert.ok(secondRuntime);

      yield* adapter.stopAll();

      NodeAssert.equal(firstRuntime.detachImpl.mock.calls.length, 0);
      NodeAssert.equal(firstRuntime.closeImpl.mock.calls.length, 1);
      NodeAssert.equal(secondRuntime.detachImpl.mock.calls.length, 0);
      NodeAssert.equal(secondRuntime.closeImpl.mock.calls.length, 1);
      NodeAssert.deepStrictEqual(
        new Set(scopedLifecycleRuntimeFactory.releasedThreadIds),
        new Set([asThreadId("thread-stop-all-1"), asThreadId("thread-stop-all-2")]),
      );
    }),
  );
});

const scopedFailureRuntimeFactory = makeScopedRuntimeFactory({ failConstruction: true });
const scopedFailureLayer = it.layer(
  Layer.effect(
    CodexAdapter,
    Effect.gen(function* () {
      const codexConfig = decodeCodexSettings({});
      return yield* makeCodexAdapter(codexConfig, {
        makeRuntime: scopedFailureRuntimeFactory.factory,
      });
    }),
  ).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(ServerSettingsService.layerTest()),
    Layer.provideMerge(providerSessionDirectoryTestLayer),
    Layer.provideMerge(NodeServices.layer),
  ),
);

scopedFailureLayer("CodexAdapterLive scoped startup failure", (it) => {
  it.effect("closes the externally owned session scope when startSession fails", () =>
    Effect.gen(function* () {
      scopedFailureRuntimeFactory.releasedThreadIds.length = 0;
      const adapter = yield* CodexAdapter;

      const result = yield* adapter
        .startSession({
          provider: ProviderDriverKind.make("codex"),
          threadId: asThreadId("thread-fail"),
          runtimeMode: "full-access",
        })
        .pipe(Effect.result);

      NodeAssert.equal(result._tag, "Failure");
      NodeAssert.equal(result.failure._tag, "ProviderAdapterProcessError");
      NodeAssert.deepStrictEqual(scopedFailureRuntimeFactory.releasedThreadIds, [
        asThreadId("thread-fail"),
      ]);
      NodeAssert.equal(yield* adapter.hasSession(asThreadId("thread-fail")), false);
    }),
  );
});

it.effect("flushes managed native logs when the adapter layer shuts down", () =>
  Effect.gen(function* () {
    const tempDir = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "t3-codex-adapter-native-log-"),
    );
    const basePath = NodePath.join(tempDir, "provider-native.ndjson");
    const runtimeFactory = makeRuntimeFactory();
    const scope = yield* Scope.make("sequential");
    let scopeClosed = false;

    try {
      const layer = Layer.effect(
        CodexAdapter,
        Effect.gen(function* () {
          const codexConfig = decodeCodexSettings({});
          return yield* makeCodexAdapter(codexConfig, {
            makeRuntime: runtimeFactory.factory,
            nativeEventLogPath: basePath,
          });
        }),
      ).pipe(
        Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
        Layer.provideMerge(ServerSettingsService.layerTest()),
        Layer.provideMerge(providerSessionDirectoryTestLayer),
        Layer.provideMerge(NodeServices.layer),
      );
      const context = yield* Layer.buildWithScope(layer, scope);
      const adapter = yield* Effect.service(CodexAdapter).pipe(Effect.provide(context));

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-logger"),
        runtimeMode: "full-access",
      });

      const runtime = runtimeFactory.lastRuntime;
      NodeAssert.ok(runtime);

      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);
      yield* runtime.emit({
        id: asEventId("evt-native-log"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-logger"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "process/stderr",
        message: "native flush test",
      } satisfies ProviderEvent);
      yield* Fiber.join(firstEventFiber);

      yield* Scope.close(scope, Exit.void);
      scopeClosed = true;

      const threadLogPath = NodePath.join(tempDir, "provider-native.thread-logger.log");
      NodeAssert.equal(NodeFS.existsSync(threadLogPath), true);
      const contents = NodeFS.readFileSync(threadLogPath, "utf8");
      NodeAssert.match(contents, /NTIVE: .*"message":"native flush test"/);
    } finally {
      if (!scopeClosed) {
        yield* Scope.close(scope, Exit.void);
      }
      NodeFS.rmSync(tempDir, { recursive: true, force: true });
    }
  }),
);
