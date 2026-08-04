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

  public readonly interruptTurnImpl = vi.fn(
    (_turnId?: TurnId): Promise<void> => Promise.resolve(undefined),
  );

  public readonly readThreadImpl = vi.fn(
    (): Promise<CodexThreadSnapshot> =>
      Promise.resolve({
        threadId: "provider-thread-1",
        turns: [],
      }),
  );

  public readonly rollbackThreadImpl = vi.fn(
    (_numTurns: number): Promise<CodexThreadSnapshot> =>
      Promise.resolve({
        threadId: "provider-thread-1",
        turns: [],
      }),
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
    this.started = new Promise((resolve) => {
      this.resolveStarted = resolve;
    });
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
    let resolveEntered: () => void = () => undefined;
    let resolveWait: () => void = () => undefined;
    const entered = new Promise<void>((resolve) => {
      resolveEntered = resolve;
    });
    const wait = new Promise<void>((resolve) => {
      resolveWait = resolve;
    });
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

lifecycleLayer("CodexAdapterLive lifecycle", (it) => {
  it.effect("treats transport loss as a reconnectable attachment state", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-session-disconnected"),
        kind: "session",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "session/disconnected",
        message: "T3 lost its Codex connection.",
      });

      const firstEvent = yield* Fiber.join(firstEventFiber);
      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") return;
      NodeAssert.equal(firstEvent.value.type, "session.state.changed");
      if (firstEvent.value.type !== "session.state.changed") return;
      NodeAssert.equal(firstEvent.value.payload.state, "error");
      NodeAssert.equal(firstEvent.value.payload.activeTurnId, null);
      NodeAssert.equal(firstEvent.value.payload.reason, "T3 lost its Codex connection.");
      NodeAssert.deepStrictEqual(firstEvent.value.payload.detail, {
        source: "provider-host-disconnect",
      });
    }),
  );

  it.effect("marks provider-host reconnect attempts as non-working", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-session-reconnecting"),
        kind: "session",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "session/reconnecting",
        message: "T3 is reconnecting without interrupting Codex execution.",
      });

      const firstEvent = yield* Fiber.join(firstEventFiber);
      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") return;
      NodeAssert.equal(firstEvent.value.type, "session.state.changed");
      if (firstEvent.value.type !== "session.state.changed") return;
      NodeAssert.equal(firstEvent.value.payload.state, "error");
      NodeAssert.equal(firstEvent.value.payload.activeTurnId, null);
      NodeAssert.equal(
        firstEvent.value.payload.reason,
        "T3 is reconnecting without interrupting Codex execution.",
      );
      NodeAssert.deepStrictEqual(firstEvent.value.payload.detail, {
        source: "provider-host-reconnect",
      });
    }),
  );

  it.effect("maps provider-host reattachment snapshots without synthesizing a turn start", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const activeTurnId = asTurnId("turn-reattached-running");
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-session-reattached"),
        kind: "session",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        turnId: activeTurnId,
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "session/reattached",
        message: "T3 reattached to the independent Codex execution.",
        payload: {
          status: "running",
          activeTurnId,
        },
      });

      const firstEvent = yield* Fiber.join(firstEventFiber);
      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") return;
      NodeAssert.equal(firstEvent.value.type, "session.state.changed");
      if (firstEvent.value.type !== "session.state.changed") return;
      NodeAssert.equal(firstEvent.value.turnId, activeTurnId);
      NodeAssert.equal(firstEvent.value.payload.state, "running");
      NodeAssert.equal(firstEvent.value.payload.activeTurnId, activeTurnId);
      NodeAssert.deepStrictEqual(firstEvent.value.payload.detail, {
        source: "provider-host-reattach",
        providerStatus: "running",
      });
    }),
  );

  it.effect("treats a detached running snapshot without an active turn as ready", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-session-reattached-without-turn"),
        kind: "session",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "session/reattached",
        message: "T3 reattached to the independent Codex execution.",
        payload: {
          status: "running",
        },
      });

      const firstEvent = yield* Fiber.join(firstEventFiber);
      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") return;
      NodeAssert.equal(firstEvent.value.type, "session.state.changed");
      if (firstEvent.value.type !== "session.state.changed") return;
      NodeAssert.equal(firstEvent.value.payload.state, "ready");
      NodeAssert.equal(firstEvent.value.payload.activeTurnId, null);
      NodeAssert.deepStrictEqual(firstEvent.value.payload.detail, {
        source: "provider-host-reattach",
        providerStatus: "running",
      });
    }),
  );

  it.effect("preserves truncated replay metadata on canonical runtime events", () =>
    Effect.gen(function* () {
      const { adapter, runtime } = yield* startLifecycleRuntime();
      const firstEventFiber = yield* Stream.runHead(adapter.streamEvents).pipe(Effect.forkChild);

      yield* runtime.emit({
        id: asEventId("evt-truncated-replay"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-1"),
        turnId: asTurnId("turn-replayed"),
        itemId: asItemId("item-replayed"),
        createdAt: "2026-01-01T00:00:00.000Z",
        method: "item/agentMessage/delta",
        textDelta: "replayed",
        replay: {
          truncated: true,
        },
        payload: {
          threadId: "thread-1",
          turnId: "turn-replayed",
          itemId: "item-replayed",
          delta: "replayed",
        },
      });

      const firstEvent = yield* Fiber.join(firstEventFiber);
      NodeAssert.equal(firstEvent._tag, "Some");
      if (firstEvent._tag !== "Some") return;
      NodeAssert.equal(firstEvent.value.type, "content.delta");
      NodeAssert.deepStrictEqual(firstEvent.value.replay, {
        truncated: true,
      });
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
      NodeAssert.equal(firstEvent.value.payload.retrying, true);
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
