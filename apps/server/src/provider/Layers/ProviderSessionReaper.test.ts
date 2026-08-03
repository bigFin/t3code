import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodePerfHooks from "node:perf_hooks";
import {
  type OrchestrationCommand,
  ProjectId,
  ThreadId,
  TurnId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderSession,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import { ServerActivation } from "../../serverActivation.ts";
import { ProviderAdapterSessionNotFoundError, ProviderValidationError } from "../Errors.ts";
import { ProviderSessionReaper } from "../Services/ProviderSessionReaper.ts";
import { ProviderService, type ProviderServiceShape } from "../Services/ProviderService.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";
import { makeProviderSessionReaperLive } from "./ProviderSessionReaper.ts";

const defaultModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5-codex",
} as const;

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 2_000,
): Promise<void> {
  const deadline = NodePerfHooks.performance.now() + timeoutMs;
  const poll = async (): Promise<void> => {
    if (await predicate()) {
      return;
    }
    if (NodePerfHooks.performance.now() >= deadline) {
      throw new Error("Timed out waiting for expectation.");
    }
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
    return poll();
  };

  return poll();
}

const drainFibers = Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
  discard: true,
});

const unsupported = () => Effect.die(new Error("Unsupported provider call in test")) as never;

function makeLiveSession(threadId: ThreadId, provider: "codex" | "claudeAgent"): ProviderSession {
  const now = "2026-01-01T00:00:00.000Z";
  return {
    provider: ProviderDriverKind.make(provider),
    providerInstanceId: ProviderInstanceId.make(provider),
    status: "running",
    runtimeMode: "full-access",
    threadId,
    createdAt: now,
    updatedAt: now,
  };
}

function makeReadModel(
  threads: ReadonlyArray<{
    readonly id: ThreadId;
    readonly session: {
      readonly threadId: ThreadId;
      readonly status: "starting" | "running" | "ready" | "interrupted" | "stopped" | "error";
      readonly providerName: "codex" | "claudeAgent";
      readonly runtimeMode: "approval-required" | "full-access" | "auto-accept-edits";
      readonly activeTurnId: TurnId | null;
      readonly lastError: string | null;
      readonly updatedAt: string;
    } | null;
  }>,
) {
  const now = "2026-01-01T00:00:00.000Z";
  const projectId = ProjectId.make("project-provider-session-reaper");

  return {
    snapshotSequence: 0,
    updatedAt: now,
    projects: [
      {
        id: projectId,
        title: "Provider Reaper Project",
        workspaceRoot: "/tmp/provider-reaper-project",
        defaultModelSelection,
        scripts: [],
        createdAt: now,
        updatedAt: now,
        deletedAt: null,
      },
    ],
    threads: threads.map((thread) => ({
      id: thread.id,
      projectId,
      title: `Thread ${thread.id}`,
      modelSelection: defaultModelSelection,
      interactionMode: "default" as const,
      runtimeMode: "full-access" as const,
      branch: null,
      worktreePath: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      latestUserMessageAt: null,
      hasPendingApprovals: false,
      hasPendingUserInput: false,
      hasActionableProposedPlan: false,
      latestTurn: null,
      messages: [],
      session: thread.session,
      activities: [],
      proposedPlans: [],
      checkpoints: [],
      deletedAt: null,
    })),
  };
}

describe("ProviderSessionReaper", () => {
  let runtime: ManagedRuntime.ManagedRuntime<
    ProviderSessionReaper | ProviderSessionRuntime.ProviderSessionRuntimeRepository,
    unknown
  > | null = null;
  let scope: Scope.Closeable | null = null;

  afterEach(async () => {
    if (scope && runtime) {
      await runtime.runPromise(Scope.close(scope, Exit.void));
    }
    scope = null;
    if (runtime) {
      await runtime.dispose();
    }
    runtime = null;
  });

  async function createHarness(input: {
    readonly readModel: ReturnType<typeof makeReadModel>;
    readonly liveSessions?: ReadonlyArray<ProviderSession>;
    readonly listSessionsImplementation?: () => ReturnType<ProviderServiceShape["listSessions"]>;
    readonly sessionPersistence?: "process-bound" | "detached";
    readonly getCapabilitiesImplementation?: ProviderServiceShape["getCapabilities"];
    readonly reattachSessionImplementation?: (input: {
      readonly threadId: ThreadId;
    }) => ReturnType<ProviderServiceShape["reattachSession"]>;
    readonly stopSessionImplementation?: (input: {
      readonly threadId: ThreadId;
    }) => ReturnType<ProviderServiceShape["stopSession"]>;
    readonly dispatchImplementation?: (
      command: OrchestrationCommand,
    ) => ReturnType<OrchestrationEngineShape["dispatch"]>;
    readonly inactivityThresholdMs?: number;
    readonly reattachRetryIntervalMs?: number;
    readonly sweepIntervalMs?: number;
  }) {
    const stoppedThreadIds = new Set<ThreadId>();
    const dispatchedCommands: OrchestrationCommand[] = [];
    const stopSession = vi.fn<ProviderServiceShape["stopSession"]>(
      (request) =>
        (input.stopSessionImplementation
          ? input.stopSessionImplementation(request)
          : Effect.sync(() => {
              stoppedThreadIds.add(request.threadId);
            })) as ReturnType<ProviderServiceShape["stopSession"]>,
    );
    const dispatch = vi.fn<OrchestrationEngineShape["dispatch"]>(
      (command) =>
        (input.dispatchImplementation
          ? input.dispatchImplementation(command)
          : Effect.sync(() => {
              dispatchedCommands.push(command);
              return { sequence: dispatchedCommands.length };
            })) as ReturnType<OrchestrationEngineShape["dispatch"]>,
    );
    const reattachSession = vi.fn<ProviderServiceShape["reattachSession"]>(
      (request) =>
        (input.reattachSessionImplementation
          ? input.reattachSessionImplementation(request)
          : unsupported()) as ReturnType<ProviderServiceShape["reattachSession"]>,
    );

    const providerService: ProviderServiceShape = {
      startSession: () => unsupported(),
      reattachSession,
      sendTurn: () => unsupported(),
      interruptTurn: () => unsupported(),
      respondToRequest: () => unsupported(),
      respondToUserInput: () => unsupported(),
      stopSession,
      listSessions: () =>
        input.listSessionsImplementation
          ? input.listSessionsImplementation()
          : Effect.succeed(input.liveSessions ?? []),
      getCapabilities: input.getCapabilitiesImplementation
        ? input.getCapabilitiesImplementation
        : () =>
            Effect.succeed({
              sessionModelSwitch: "in-session",
              ...(input.sessionPersistence ? { sessionPersistence: input.sessionPersistence } : {}),
            }),
      getInstanceInfo: (instanceId) => {
        const driverKind = ProviderDriverKind.make(String(instanceId));
        return Effect.succeed({
          instanceId,
          driverKind,
          displayName: undefined,
          enabled: true,
          continuationIdentity: {
            driverKind,
            continuationKey: `${driverKind}:instance:${instanceId}`,
          },
        });
      },
      rollbackConversation: () => unsupported(),
      streamEvents: Stream.empty,
    };
    const orchestrationEngine: OrchestrationEngineShape = {
      readEvents: () => Stream.empty,
      readAggregateEvents: () => Stream.empty,
      dispatch,
      streamDomainEvents: Stream.empty,
      latestSequence: Effect.succeed(0),
    };

    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const providerSessionDirectoryLayer = ProviderSessionDirectoryLive.pipe(
      Layer.provide(runtimeRepositoryLayer),
    );
    const layer = makeProviderSessionReaperLive({
      inactivityThresholdMs: input.inactivityThresholdMs ?? 1_000,
      ...(input.reattachRetryIntervalMs !== undefined
        ? { reattachRetryIntervalMs: input.reattachRetryIntervalMs }
        : {}),
      sweepIntervalMs: input.sweepIntervalMs ?? 60_000,
    }).pipe(
      Layer.provideMerge(providerSessionDirectoryLayer),
      Layer.provideMerge(runtimeRepositoryLayer),
      Layer.provideMerge(Layer.succeed(ProviderService, providerService)),
      Layer.provideMerge(Layer.succeed(OrchestrationEngineService, orchestrationEngine)),
      Layer.provideMerge(
        Layer.succeed(ProjectionSnapshotQuery, {
          getCommandReadModel: () => Effect.die("unused"),
          getSnapshot: () => Effect.die("unused"),
          getShellSnapshot: () => Effect.die("unused"),
          getArchivedShellSnapshot: () => Effect.die("unused"),
          getSnapshotSequence: () =>
            Effect.succeed({ snapshotSequence: input.readModel.snapshotSequence }),
          getCounts: () => Effect.die("unused"),
          getActiveProjectByWorkspaceRoot: () => Effect.die("unused"),
          getProjectShellById: () => Effect.die("unused"),
          getFirstActiveThreadIdByProjectId: () => Effect.die("unused"),
          getThreadCheckpointContext: () => Effect.die("unused"),
          getFullThreadDiffContext: () => Effect.die("unused"),
          getThreadShellsByIds: (threadIds) =>
            Effect.succeed(
              new Map(
                input.readModel.threads
                  .filter((thread) => threadIds.includes(thread.id))
                  .map((thread) => [thread.id, thread]),
              ),
            ),
          getThreadShellById: (threadId) =>
            Effect.succeed(
              input.readModel.threads.find((thread) => thread.id === threadId)
                ? Option.some(input.readModel.threads.find((thread) => thread.id === threadId)!)
                : Option.none(),
            ),
          getThreadTranscriptById: () => Effect.die("unused"),
          getExistingThreadActivityIds: () => Effect.die("unused"),
          getThreadDetailById: () => Effect.die("unused"),
          getThreadDetailSnapshot: () => Effect.die("unused"),
          searchThreads: () => Effect.succeed({ matches: [] }),
        }),
      ),
      Layer.provideMerge(NodeServices.layer),
    );

    runtime = ManagedRuntime.make(layer);
    return { dispatch, dispatchedCommands, reattachSession, stopSession, stoppedThreadIds };
  }

  it("parks startup reconciliation until a trial server is activated", async () => {
    const activation = await Effect.runPromise(Deferred.make<void>());
    const listed = await Effect.runPromise(Deferred.make<void>());
    const listSessions = vi.fn(() =>
      Deferred.succeed(listed, undefined).pipe(Effect.as<ReadonlyArray<ProviderSession>>([])),
    );
    const harness = await createHarness({
      readModel: makeReadModel([]),
      listSessionsImplementation: listSessions,
    });
    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));

    await runtime!.runPromise(
      reaper
        .start()
        .pipe(
          Scope.provide(scope),
          Effect.provideService(ServerActivation, Deferred.await(activation)),
        ),
    );

    expect(listSessions).not.toHaveBeenCalled();
    expect(harness.reattachSession).not.toHaveBeenCalled();
    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.stopSession).not.toHaveBeenCalled();

    await runtime!.runPromise(Deferred.succeed(activation, undefined));
    await runtime!.runPromise(Deferred.await(listed));

    expect(listSessions).toHaveBeenCalledTimes(1);
  });

  it("does not reconcile a parked trial when its scope closes before activation", async () => {
    const activation = await Effect.runPromise(Deferred.make<void>());
    const listSessions = vi.fn(() => Effect.succeed<ReadonlyArray<ProviderSession>>([]));
    const harness = await createHarness({
      readModel: makeReadModel([]),
      listSessionsImplementation: listSessions,
    });
    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));

    await runtime!.runPromise(
      reaper
        .start()
        .pipe(
          Scope.provide(scope),
          Effect.provideService(ServerActivation, Deferred.await(activation)),
        ),
    );
    await runtime!.runPromise(Scope.close(scope, Exit.void));
    scope = null;

    expect(listSessions).not.toHaveBeenCalled();
    expect(harness.reattachSession).not.toHaveBeenCalled();
    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.stopSession).not.toHaveBeenCalled();
  });

  it("reaps stale persisted sessions without active turns", async () => {
    const threadId = ThreadId.make("thread-reaper-stale");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "ready",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
      liveSessions: [makeLiveSession(threadId, "claudeAgent")],
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-stale",
        },
        runtimePayload: null,
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));

    await waitFor(() => harness.stopSession.mock.calls.length === 1);

    expect(harness.stopSession.mock.calls[0]?.[0]).toEqual({ threadId });
    expect(harness.stoppedThreadIds.has(threadId)).toBe(true);
  });

  it("interrupts an active turn whose persisted runtime disappeared on restart", async () => {
    const threadId = ThreadId.make("thread-reaper-active-turn");
    const turnId = TurnId.make("turn-reaper-active");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-active-turn",
        },
        runtimePayload: null,
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));

    expect(harness.stopSession).not.toHaveBeenCalled();
    expect(harness.dispatchedCommands).toEqual([
      expect.objectContaining({
        type: "thread.session.set",
        threadId,
        session: expect.objectContaining({
          status: "interrupted",
          activeTurnId: null,
          retrying: false,
          lastError:
            "The T3 server restarted while this turn was running. T3 recovered the available transcript. Send a message to continue.",
        }),
      }),
    ]);
    const remaining = Option.getOrThrow(
      await runtime!.runPromise(repository.getByThreadId({ threadId })),
    );
    expect(remaining.status).toBe("stopped");
    expect(remaining.resumeCursor).toEqual({ opaque: "resume-active-turn" });
    expect(remaining.runtimePayload).toMatchObject({
      activeTurnId: null,
      lastRuntimeEvent: "provider.session.orphaned-on-startup",
    });
  });

  it("interrupts an active turn whose runtime was stopped during server shutdown", async () => {
    const threadId = ThreadId.make("thread-reaper-shutdown-active-turn");
    const turnId = TurnId.make("turn-reaper-shutdown-active");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "running",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "codex",
        providerInstanceId,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "stopped",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-shutdown-active-turn",
        },
        runtimePayload: {
          activeTurnId: null,
          lastRuntimeEvent: "provider.stopAll",
          lastRuntimeEventAt: "2026-04-14T00:00:01.000Z",
        },
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));

    expect(harness.stopSession).not.toHaveBeenCalled();
    expect(harness.dispatchedCommands).toEqual([
      expect.objectContaining({
        type: "thread.session.set",
        threadId,
        commandId: expect.stringContaining("provider-session-reaper:startup-interrupted"),
        session: expect.objectContaining({
          status: "interrupted",
          activeTurnId: null,
          retrying: false,
          lastError:
            "The T3 server restarted while this turn was running. T3 recovered the available transcript. Send a message to continue.",
        }),
      }),
    ]);
    const remaining = Option.getOrThrow(
      await runtime!.runPromise(repository.getByThreadId({ threadId })),
    );
    expect(remaining.status).toBe("stopped");
    expect(remaining.resumeCursor).toEqual({ opaque: "resume-shutdown-active-turn" });
    expect(remaining.runtimePayload).toMatchObject({
      activeTurnId: null,
      lastRuntimeEvent: "provider.session.orphaned-on-startup",
    });
  });

  it("marks an unreachable detached Codex turn non-working while retrying", async () => {
    const threadId = ThreadId.make("thread-reaper-detached-active-turn");
    const turnId = TurnId.make("turn-reaper-detached-active");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "running",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "codex",
        providerInstanceId,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-detached-active-turn",
        },
        runtimePayload: {
          activeTurnId: turnId,
          lastRuntimeEvent: "provider.detachAll",
          lastRuntimeEventAt: "2026-04-14T00:00:01.000Z",
        },
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));

    expect(harness.stopSession).not.toHaveBeenCalled();
    expect(harness.reattachSession).toHaveBeenCalledWith({ threadId });
    expect(harness.dispatchedCommands).toEqual([
      expect.objectContaining({
        type: "thread.session.set",
        threadId,
        commandId: expect.stringContaining("provider-session-reaper:startup-reattach-retrying"),
        session: expect.objectContaining({
          status: "error",
          activeTurnId: null,
          retrying: true,
          lastError:
            "T3 could not reattach to the detached provider execution yet. The provider was not interrupted; T3 will keep retrying.",
        }),
      }),
    ]);
    const remaining = Option.getOrThrow(
      await runtime!.runPromise(repository.getByThreadId({ threadId })),
    );
    expect(remaining.status).toBe("error");
    expect(remaining.resumeCursor).toEqual({ opaque: "resume-detached-active-turn" });
    expect(remaining.runtimePayload).toMatchObject({
      activeTurnId: null,
      lastRuntimeEvent: "provider.session.detached-reattach-pending",
      sessionPersistence: "detached",
    });
  });

  it.each(["starting", "running", "error"] as const)(
    "leaves %s detached Codex CLI observers for transcript reconciliation",
    async (status) => {
      const threadId = ThreadId.make(`thread-reaper-codex-cli-observer-${status}`);
      const turnId = TurnId.make(`turn-reaper-codex-cli-observer-${status}`);
      const providerInstanceId = ProviderInstanceId.make("codex");
      const now = "2026-08-03T00:00:00.000Z";
      const harness = await createHarness({
        readModel: makeReadModel([
          {
            id: threadId,
            session: {
              threadId,
              status,
              providerName: "codex",
              runtimeMode: "full-access",
              activeTurnId: turnId,
              lastError: status === "error" ? "Waiting for transcript reconciliation." : null,
              updatedAt: now,
            },
          },
        ]),
        sessionPersistence: "detached",
      });
      const repository = await runtime!.runPromise(
        Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
      );

      await runtime!.runPromise(
        repository.upsert({
          threadId,
          providerName: "codex",
          providerInstanceId,
          adapterKey: "codex",
          runtimeMode: "full-access",
          status,
          lastSeenAt: now,
          resumeCursor: {
            threadId: "provider-thread-codex-cli-observer",
          },
          runtimePayload: {
            activeTurnId: null,
            importedFrom: "codex-cli",
            codexCliImportVersion: 2,
            sessionPersistence: "detached",
          },
        }),
      );

      const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
      scope = await runtime!.runPromise(Scope.make("sequential"));
      await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));
      await runtime!.runPromise(drainFibers);

      expect(harness.reattachSession).not.toHaveBeenCalled();
      expect(harness.dispatch).not.toHaveBeenCalled();
      expect(harness.stopSession).not.toHaveBeenCalled();
      const remaining = Option.getOrThrow(
        await runtime!.runPromise(repository.getByThreadId({ threadId })),
      );
      expect(remaining.status).toBe(status);
      expect(remaining.runtimePayload).toMatchObject({
        activeTurnId: null,
        importedFrom: "codex-cli",
        codexCliImportVersion: 2,
        sessionPersistence: "detached",
      });
    },
  );

  it("repairs a legacy pending detached reattachment once without repeated state churn", async () => {
    const threadId = ThreadId.make("thread-reaper-detached-pending-retry");
    const turnId = TurnId.make("turn-reaper-detached-pending-retry");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const lastSeenAt = "2026-04-14T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "starting",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: "Reconnecting",
            updatedAt: lastSeenAt,
          },
        },
      ]),
      inactivityThresholdMs: 1,
      reattachRetryIntervalMs: 1,
      sessionPersistence: "detached",
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "codex",
        providerInstanceId,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "starting",
        lastSeenAt,
        resumeCursor: {
          opaque: "resume-detached-pending-retry",
        },
        runtimePayload: {
          activeTurnId: turnId,
          lastRuntimeEvent: "provider.session.detached-reattach-pending",
          lastRuntimeEventAt: lastSeenAt,
        },
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));
    const corrected = Option.getOrThrow(
      await runtime!.runPromise(repository.getByThreadId({ threadId })),
    );
    await waitFor(() => harness.reattachSession.mock.calls.length >= 3);

    expect(harness.dispatchedCommands).toEqual([
      expect.objectContaining({
        type: "thread.session.set",
        threadId,
        session: expect.objectContaining({
          status: "error",
          activeTurnId: null,
          retrying: true,
          lastError:
            "T3 could not reattach to the detached provider execution yet. The provider was not interrupted; T3 will keep retrying.",
        }),
      }),
    ]);
    expect(harness.stopSession).not.toHaveBeenCalled();
    const remaining = Option.getOrThrow(
      await runtime!.runPromise(repository.getByThreadId({ threadId })),
    );
    expect(remaining.status).toBe("error");
    expect(remaining).toEqual(corrected);
    expect(remaining.runtimePayload).toMatchObject({
      activeTurnId: null,
      lastRuntimeEvent: "provider.session.detached-reattach-pending",
      sessionPersistence: "detached",
    });
  });

  it("marks an authoritatively missing detached execution interrupted without resuming it", async () => {
    const threadId = ThreadId.make("thread-reaper-detached-missing");
    const turnId = TurnId.make("turn-reaper-detached-missing");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const lastSeenAt = "2026-04-14T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "running",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: lastSeenAt,
          },
        },
      ]),
      sessionPersistence: "detached",
      reattachSessionImplementation: () =>
        Effect.fail(
          new ProviderAdapterSessionNotFoundError({
            provider: "codex",
            threadId,
          }),
        ),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "codex",
        providerInstanceId,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt,
        resumeCursor: {
          opaque: "resume-detached-missing",
        },
        runtimePayload: {
          activeTurnId: turnId,
          lastRuntimeEvent: "provider.detachAll",
          lastRuntimeEventAt: lastSeenAt,
        },
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));

    expect(harness.reattachSession).toHaveBeenCalledTimes(1);
    expect(harness.stopSession).not.toHaveBeenCalled();
    expect(harness.dispatchedCommands).toEqual([
      expect.objectContaining({
        type: "thread.session.set",
        threadId,
        session: expect.objectContaining({
          status: "interrupted",
          activeTurnId: null,
          retrying: false,
          lastError:
            "The detached provider execution is no longer present. T3 did not resume it automatically. Send a message to continue.",
        }),
      }),
    ]);
    const remaining = Option.getOrThrow(
      await runtime!.runPromise(repository.getByThreadId({ threadId })),
    );
    expect(remaining.status).toBe("stopped");
    expect(remaining.resumeCursor).toEqual({ opaque: "resume-detached-missing" });
    expect(remaining.runtimePayload).toMatchObject({
      activeTurnId: null,
      lastRuntimeEvent: "provider.session.detached-runtime-missing-on-startup",
      sessionPersistence: "detached",
    });
  });

  it("emits a new missing-runtime status command after a later detached occurrence", async () => {
    const threadId = ThreadId.make("thread-reaper-detached-missing-recurrence");
    const turnId = TurnId.make("turn-reaper-detached-missing-recurrence");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const firstSeenAt = "2026-04-14T00:00:00.000Z";
    const secondSeenAt = "2026-04-14T01:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "running",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: firstSeenAt,
          },
        },
      ]),
      sessionPersistence: "detached",
      reattachSessionImplementation: () =>
        Effect.fail(
          new ProviderAdapterSessionNotFoundError({
            provider: "codex",
            threadId,
          }),
        ),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );
    const upsertDetachedOccurrence = (lastSeenAt: string) =>
      repository.upsert({
        threadId,
        providerName: "codex",
        providerInstanceId,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt,
        resumeCursor: {
          opaque: `resume-detached-missing-${lastSeenAt}`,
        },
        runtimePayload: {
          activeTurnId: turnId,
          lastRuntimeEvent: "provider.detachAll",
          lastRuntimeEventAt: lastSeenAt,
          sessionPersistence: "detached",
        },
      });

    await runtime!.runPromise(upsertDetachedOccurrence(firstSeenAt));

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));
    await runtime!.runPromise(Scope.close(scope, Exit.void));

    await runtime!.runPromise(upsertDetachedOccurrence(secondSeenAt));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));

    const commandIds = harness.dispatchedCommands.map((command) => command.commandId);
    expect(commandIds).toHaveLength(2);
    expect(new Set(commandIds).size).toBe(2);
  });

  it("emits a new reconnecting status command after a later detached occurrence", async () => {
    const threadId = ThreadId.make("thread-reaper-detached-pending-recurrence");
    const turnId = TurnId.make("turn-reaper-detached-pending-recurrence");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const firstSeenAt = "2026-04-14T00:00:00.000Z";
    const secondSeenAt = "2026-04-14T01:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "running",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: firstSeenAt,
          },
        },
      ]),
      sessionPersistence: "detached",
      reattachSessionImplementation: () =>
        Effect.fail(
          new ProviderValidationError({
            operation: "ProviderSessionReaper.test",
            issue: "provider host unavailable",
          }),
        ),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );
    const upsertDetachedOccurrence = (lastSeenAt: string) =>
      repository.upsert({
        threadId,
        providerName: "codex",
        providerInstanceId,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt,
        resumeCursor: {
          opaque: `resume-detached-pending-${lastSeenAt}`,
        },
        runtimePayload: {
          activeTurnId: turnId,
          lastRuntimeEvent: "provider.detachAll",
          lastRuntimeEventAt: lastSeenAt,
          sessionPersistence: "detached",
        },
      });

    await runtime!.runPromise(upsertDetachedOccurrence(firstSeenAt));

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));
    await runtime!.runPromise(Scope.close(scope, Exit.void));

    await runtime!.runPromise(upsertDetachedOccurrence(secondSeenAt));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));

    const commandIds = harness.dispatchedCommands.map((command) => command.commandId);
    expect(commandIds).toHaveLength(2);
    expect(new Set(commandIds).size).toBe(2);
  });

  it("defers detached startup projection to the provider runtime event stream", async () => {
    const threadId = ThreadId.make("thread-reaper-reattached-running");
    const turnId = TurnId.make("turn-reaper-reattached-running");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "starting",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: "Reconnecting",
            updatedAt: now,
          },
        },
      ]),
      reattachSessionImplementation: () =>
        Effect.succeed({
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId,
          status: "running",
          runtimeMode: "full-access",
          threadId,
          activeTurnId: turnId,
          createdAt: now,
          updatedAt: "2026-01-01T00:00:02.000Z",
        }),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "codex",
        providerInstanceId,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          threadId: "provider-thread-reaper-reattached-running",
        },
        runtimePayload: {
          activeTurnId: null,
          lastRuntimeEvent: "provider.detachAll",
          lastRuntimeEventAt: "2026-04-14T00:00:01.000Z",
        },
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));

    expect(harness.reattachSession).toHaveBeenCalledWith({ threadId });
    expect(harness.dispatchedCommands).toEqual([]);
  });

  it("reattaches a detached error binding without projecting a parallel snapshot", async () => {
    const threadId = ThreadId.make("thread-reaper-reattached-error");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "error",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: "Codex reported a system error for this thread.",
            updatedAt: now,
          },
        },
      ]),
      reattachSessionImplementation: () =>
        Effect.succeed({
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId,
          status: "error",
          runtimeMode: "full-access",
          threadId,
          lastError: "Codex reported a system error for this thread.",
          createdAt: now,
          updatedAt: "2026-01-01T00:00:02.000Z",
        }),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "codex",
        providerInstanceId,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "error",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          threadId: "provider-thread-reaper-reattached-error",
        },
        runtimePayload: {
          activeTurnId: null,
          lastRuntimeEvent: "provider.session.detached-reattached",
          lastRuntimeEventAt: "2026-04-14T00:00:01.000Z",
          sessionPersistence: "detached",
        },
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));

    expect(harness.reattachSession).toHaveBeenCalledWith({ threadId });
    expect(harness.dispatchedCommands).toEqual([]);
  });

  it("defers stale active-turn clearing to the authoritative reattach event", async () => {
    const threadId = ThreadId.make("thread-reaper-reattached-ready");
    const staleTurnId = TurnId.make("turn-reaper-stale-before-ready");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "running",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: staleTurnId,
            lastError: "Stale projected error",
            updatedAt: now,
          },
        },
      ]),
      reattachSessionImplementation: () =>
        Effect.succeed({
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId,
          status: "ready",
          runtimeMode: "full-access",
          threadId,
          createdAt: now,
          updatedAt: "2026-01-01T00:00:03.000Z",
        }),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "codex",
        providerInstanceId,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          threadId: "provider-thread-reaper-reattached-ready",
        },
        runtimePayload: {
          activeTurnId: staleTurnId,
          lastRuntimeEvent: "provider.detachAll",
          lastRuntimeEventAt: "2026-04-14T00:00:01.000Z",
        },
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));

    expect(harness.reattachSession).toHaveBeenCalledWith({ threadId });
    expect(harness.dispatchedCommands).toEqual([]);
  });

  it("keeps an active projected turn when the matching provider runtime is live", async () => {
    const threadId = ThreadId.make("thread-reaper-live-active-turn");
    const turnId = TurnId.make("turn-reaper-live-active");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
      liveSessions: [makeLiveSession(threadId, "claudeAgent")],
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-live-active-turn",
        },
        runtimePayload: null,
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));
    await runtime!.runPromise(drainFibers);

    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.stopSession).not.toHaveBeenCalled();
    const remaining = Option.getOrThrow(
      await runtime!.runPromise(repository.getByThreadId({ threadId })),
    );
    expect(remaining.status).toBe("running");
  });

  it("does not reap sessions that are still within the inactivity threshold", async () => {
    const threadId = ThreadId.make("thread-reaper-fresh");
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "ready",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: "2026-01-01T00:00:00.000Z",
          },
        },
      ]),
      liveSessions: [makeLiveSession(threadId, "claudeAgent")],
    });
    const now = DateTime.formatIso(await runtime!.runPromise(DateTime.now));
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: now,
        resumeCursor: {
          opaque: "resume-fresh",
        },
        runtimePayload: null,
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));
    await runtime!.runPromise(drainFibers);

    expect(harness.stopSession).not.toHaveBeenCalled();
    const remaining = await runtime!.runPromise(repository.getByThreadId({ threadId }));
    expect(Option.isSome(remaining)).toBe(true);
  });

  it("never reaps detached sessions for inactivity", async () => {
    const threadId = ThreadId.make("thread-reaper-detached-idle");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "ready",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
      liveSessions: [
        {
          ...makeLiveSession(threadId, "codex"),
          providerInstanceId,
        },
      ],
      sessionPersistence: "detached",
      inactivityThresholdMs: 1,
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "codex",
        providerInstanceId,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-01-01T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-detached-idle",
        },
        runtimePayload: {
          sessionPersistence: "detached",
        },
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));
    await runtime!.runPromise(drainFibers);

    expect(harness.stopSession).not.toHaveBeenCalled();
    const remaining = Option.getOrThrow(
      await runtime!.runPromise(repository.getByThreadId({ threadId })),
    );
    expect(remaining.status).toBe("running");
  });

  it("uses persisted detached ownership when capability lookup is unavailable", async () => {
    const threadId = ThreadId.make("thread-reaper-detached-persisted-capability-failure");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const now = "2026-01-01T00:00:00.000Z";
    const reattachedAt = "2026-01-01T00:00:01.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "running",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: TurnId.make("turn-detached-persisted-capability-failure"),
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
      getCapabilitiesImplementation: () => Effect.die(new Error("capabilities unavailable")),
      reattachSessionImplementation: () =>
        Effect.succeed({
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId,
          threadId,
          status: "ready",
          runtimeMode: "full-access",
          cwd: process.cwd(),
          createdAt: now,
          updatedAt: reattachedAt,
        }),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "codex",
        providerInstanceId,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: now,
        resumeCursor: {
          opaque: "resume-detached-persisted-capability-failure",
        },
        runtimePayload: {
          sessionPersistence: "detached",
        },
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));
    await runtime!.runPromise(drainFibers);

    expect(harness.reattachSession).toHaveBeenCalledWith({ threadId });
    expect(harness.stopSession).not.toHaveBeenCalled();
    expect(harness.dispatchedCommands).toEqual([]);
  });

  it("skips persisted sessions that are already marked stopped", async () => {
    const threadId = ThreadId.make("thread-reaper-stopped");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "stopped",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "stopped",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-stopped",
        },
        runtimePayload: {
          importedFrom: "codex-cli",
        },
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));
    await runtime!.runPromise(drainFibers);

    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.stopSession).not.toHaveBeenCalled();
    const remaining = Option.getOrThrow(
      await runtime!.runPromise(repository.getByThreadId({ threadId })),
    );
    expect(remaining.status).toBe("stopped");
    expect(remaining.runtimePayload).toEqual({ importedFrom: "codex-cli" });
  });

  it("makes no changes when live provider inventory cannot be acquired", async () => {
    const threadId = ThreadId.make("thread-reaper-inventory-failure");
    const turnId = TurnId.make("turn-reaper-inventory-failure");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "running",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
      listSessionsImplementation: () => Effect.die(new Error("inventory unavailable")),
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-inventory-failure",
        },
        runtimePayload: null,
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));
    await runtime!.runPromise(drainFibers);

    expect(harness.dispatch).not.toHaveBeenCalled();
    expect(harness.stopSession).not.toHaveBeenCalled();
    const remaining = Option.getOrThrow(
      await runtime!.runPromise(repository.getByThreadId({ threadId })),
    );
    expect(remaining.status).toBe("running");
  });

  it("continues reaping other sessions when one stop attempt fails", async () => {
    const failedThreadId = ThreadId.make("thread-reaper-stop-failure");
    const reapedThreadId = ThreadId.make("thread-reaper-stop-success");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: failedThreadId,
          session: {
            threadId: failedThreadId,
            status: "ready",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
        {
          id: reapedThreadId,
          session: {
            threadId: reapedThreadId,
            status: "ready",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
      stopSessionImplementation: (request) =>
        request.threadId === failedThreadId
          ? Effect.fail(
              new ProviderValidationError({
                operation: "ProviderSessionReaper.test",
                issue: "simulated stop failure",
              }),
            )
          : Effect.void,
      liveSessions: [
        makeLiveSession(failedThreadId, "claudeAgent"),
        makeLiveSession(reapedThreadId, "codex"),
      ],
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId: failedThreadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-failure",
        },
        runtimePayload: null,
      }),
    );
    await runtime!.runPromise(
      repository.upsert({
        threadId: reapedThreadId,
        providerName: "codex",
        providerInstanceId: null,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:01:00.000Z",
        resumeCursor: {
          opaque: "resume-success",
        },
        runtimePayload: null,
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));

    await waitFor(() => harness.stopSession.mock.calls.length === 2);

    expect(harness.stopSession.mock.calls.map(([request]) => request.threadId)).toEqual([
      failedThreadId,
      reapedThreadId,
    ]);
  });

  it("continues reaping other sessions when one stop attempt defects", async () => {
    const defectThreadId = ThreadId.make("thread-reaper-stop-defect");
    const reapedThreadId = ThreadId.make("thread-reaper-stop-after-defect");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = await createHarness({
      readModel: makeReadModel([
        {
          id: defectThreadId,
          session: {
            threadId: defectThreadId,
            status: "ready",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
        {
          id: reapedThreadId,
          session: {
            threadId: reapedThreadId,
            status: "ready",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
      stopSessionImplementation: (request) =>
        request.threadId === defectThreadId
          ? Effect.die(new Error("simulated stop defect"))
          : Effect.void,
      liveSessions: [
        makeLiveSession(defectThreadId, "claudeAgent"),
        makeLiveSession(reapedThreadId, "codex"),
      ],
    });
    const repository = await runtime!.runPromise(
      Effect.service(ProviderSessionRuntime.ProviderSessionRuntimeRepository),
    );

    await runtime!.runPromise(
      repository.upsert({
        threadId: defectThreadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-defect",
        },
        runtimePayload: null,
      }),
    );
    await runtime!.runPromise(
      repository.upsert({
        threadId: reapedThreadId,
        providerName: "codex",
        providerInstanceId: null,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-04-14T00:01:00.000Z",
        resumeCursor: {
          opaque: "resume-after-defect",
        },
        runtimePayload: null,
      }),
    );

    const reaper = await runtime!.runPromise(Effect.service(ProviderSessionReaper));
    scope = await runtime!.runPromise(Scope.make("sequential"));
    await runtime!.runPromise(reaper.start().pipe(Scope.provide(scope)));

    await waitFor(() => harness.stopSession.mock.calls.length === 2);

    expect(harness.stopSession.mock.calls.map(([request]) => request.threadId)).toEqual([
      defectThreadId,
      reapedThreadId,
    ]);
  });
});
