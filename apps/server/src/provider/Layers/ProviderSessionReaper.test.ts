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
import { ProviderValidationError } from "../Errors.ts";
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
    readonly stopSessionImplementation?: (input: {
      readonly threadId: ThreadId;
    }) => ReturnType<ProviderServiceShape["stopSession"]>;
    readonly dispatchImplementation?: (
      command: OrchestrationCommand,
    ) => ReturnType<OrchestrationEngineShape["dispatch"]>;
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

    const providerService: ProviderServiceShape = {
      startSession: () => unsupported(),
      sendTurn: () => unsupported(),
      interruptTurn: () => unsupported(),
      respondToRequest: () => unsupported(),
      respondToUserInput: () => unsupported(),
      stopSession,
      listSessions: () =>
        input.listSessionsImplementation
          ? input.listSessionsImplementation()
          : Effect.succeed(input.liveSessions ?? []),
      getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
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
      inactivityThresholdMs: 1_000,
      sweepIntervalMs: 60_000,
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
          getThreadDetailById: () => Effect.die("unused"),
          getThreadDetailSnapshot: () => Effect.die("unused"),
          searchThreads: () => Effect.succeed({ matches: [] }),
        }),
      ),
      Layer.provideMerge(NodeServices.layer),
    );

    runtime = ManagedRuntime.make(layer);
    return { dispatch, dispatchedCommands, stopSession, stoppedThreadIds };
  }

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
