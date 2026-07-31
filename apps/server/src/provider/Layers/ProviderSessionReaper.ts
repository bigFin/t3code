import { CommandId, type ProviderInstanceId, type ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";

import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import {
  ProviderSessionReaper,
  type ProviderSessionReaperShape,
} from "../Services/ProviderSessionReaper.ts";
import { ProviderService } from "../Services/ProviderService.ts";

const DEFAULT_INACTIVITY_THRESHOLD_MS = 30 * 60 * 1000;
const DEFAULT_SWEEP_INTERVAL_MS = 5 * 60 * 1000;
const RESTART_INTERRUPTION_MESSAGE =
  "The T3 server restarted while this turn was running. T3 recovered the available transcript. Send a message to continue.";

export interface ProviderSessionReaperLiveOptions {
  readonly inactivityThresholdMs?: number;
  readonly sweepIntervalMs?: number;
}

const makeProviderSessionReaper = (options?: ProviderSessionReaperLiveOptions) =>
  Effect.gen(function* () {
    const providerService = yield* ProviderService;
    const directory = yield* ProviderSessionDirectory;
    const orchestrationEngine = yield* OrchestrationEngineService;
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;

    const inactivityThresholdMs = Math.max(
      1,
      options?.inactivityThresholdMs ?? DEFAULT_INACTIVITY_THRESHOLD_MS,
    );
    const sweepIntervalMs = Math.max(1, options?.sweepIntervalMs ?? DEFAULT_SWEEP_INTERVAL_MS);

    const reconcileOrphanedSessions = Effect.fn("ProviderSessionReaper.reconcileOrphanedSessions")(
      function* () {
        const liveSessionsExit = yield* Effect.exit(providerService.listSessions());
        if (Exit.isFailure(liveSessionsExit)) {
          yield* Effect.logWarning("provider.session.reaper.startup-inventory-failed", {
            cause: liveSessionsExit.cause,
          });
          return;
        }

        const liveThreadIdsByInstance = new Map<ProviderInstanceId, Set<ThreadId>>();
        for (const session of liveSessionsExit.value) {
          if (session.providerInstanceId === undefined) {
            yield* Effect.logWarning("provider.session.reaper.live-session-missing-instance", {
              threadId: session.threadId,
              provider: session.provider,
            });
            continue;
          }
          const threadIds = liveThreadIdsByInstance.get(session.providerInstanceId);
          if (threadIds) {
            threadIds.add(session.threadId);
          } else {
            liveThreadIdsByInstance.set(session.providerInstanceId, new Set([session.threadId]));
          }
        }

        const bindings = yield* directory.listBindings();
        const orphanedBindings = bindings.filter((binding) => {
          if (binding.status !== "starting" && binding.status !== "running") {
            return false;
          }
          if (binding.providerInstanceId === undefined) {
            return false;
          }
          return !liveThreadIdsByInstance.get(binding.providerInstanceId)?.has(binding.threadId);
        });
        if (orphanedBindings.length === 0) {
          return;
        }

        const threadShells = yield* projectionSnapshotQuery.getThreadShellsByIds(
          orphanedBindings.map((binding) => binding.threadId),
        );
        const reconciledAt = DateTime.formatIso(yield* DateTime.now);
        const reconcileOrphanedSession = Effect.fn(
          "ProviderSessionReaper.reconcileOrphanedSession",
        )(function* (binding: (typeof orphanedBindings)[number]) {
          if (binding.providerInstanceId === undefined) {
            return { reconciled: false, interrupted: false };
          }

          const session = threadShells.get(binding.threadId)?.session;
          const interrupted = session?.status === "starting" || session?.status === "running";
          if (interrupted) {
            yield* orchestrationEngine.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make(
                [
                  "provider-session-reaper",
                  "startup-interrupted",
                  binding.threadId,
                  session.activeTurnId ?? session.updatedAt,
                ].join(":"),
              ),
              threadId: binding.threadId,
              session: {
                ...session,
                status: "interrupted",
                activeTurnId: null,
                lastError: RESTART_INTERRUPTION_MESSAGE,
                retrying: false,
                updatedAt: reconciledAt,
              },
              createdAt: reconciledAt,
            });
          }

          yield* directory.upsert({
            threadId: binding.threadId,
            provider: binding.provider,
            providerInstanceId: binding.providerInstanceId,
            status: "stopped",
            runtimePayload: {
              activeTurnId: null,
              lastRuntimeEvent: "provider.session.orphaned-on-startup",
              lastRuntimeEventAt: reconciledAt,
            },
          });
          return { reconciled: true, interrupted };
        });
        const reconciliationResults = yield* Effect.forEach(
          orphanedBindings,
          (binding) =>
            reconcileOrphanedSession(binding).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("provider.session.reaper.startup-binding-reconcile-failed", {
                  threadId: binding.threadId,
                  provider: binding.provider,
                  cause,
                }).pipe(Effect.as({ reconciled: false, interrupted: false })),
              ),
            ),
          { concurrency: 1 },
        );
        const reconciledCount = reconciliationResults.filter((result) => result.reconciled).length;
        const interruptedCount = reconciliationResults.filter(
          (result) => result.interrupted,
        ).length;

        yield* Effect.logInfo("provider.session.reaper.startup-reconciled", {
          orphanedCount: orphanedBindings.length,
          reconciledCount,
          interruptedCount,
        });
      },
    );

    const sweep = Effect.gen(function* () {
      const bindings = yield* directory.listBindings();
      const now = yield* Clock.currentTimeMillis;
      let reapedCount = 0;

      for (const binding of bindings) {
        if (binding.status === "stopped") {
          continue;
        }

        const lastSeenMs = Date.parse(binding.lastSeenAt);
        if (Number.isNaN(lastSeenMs)) {
          yield* Effect.logWarning("provider.session.reaper.invalid-last-seen", {
            threadId: binding.threadId,
            provider: binding.provider,
            lastSeenAt: binding.lastSeenAt,
          });
          continue;
        }

        const idleDurationMs = now - lastSeenMs;
        if (idleDurationMs < inactivityThresholdMs) {
          continue;
        }

        const thread = yield* projectionSnapshotQuery
          .getThreadShellById(binding.threadId)
          .pipe(Effect.map(Option.getOrUndefined));
        if (thread?.session?.activeTurnId != null) {
          yield* Effect.logDebug("provider.session.reaper.skipped-active-turn", {
            threadId: binding.threadId,
            activeTurnId: thread.session.activeTurnId,
            idleDurationMs,
          });
          continue;
        }

        const reaped = yield* providerService.stopSession({ threadId: binding.threadId }).pipe(
          Effect.tap(() =>
            Effect.logInfo("provider.session.reaped", {
              threadId: binding.threadId,
              provider: binding.provider,
              idleDurationMs,
              reason: "inactivity_threshold",
            }),
          ),
          Effect.as(true),
          Effect.catchCause((cause) =>
            Effect.logWarning("provider.session.reaper.stop-failed", {
              threadId: binding.threadId,
              provider: binding.provider,
              idleDurationMs,
              cause,
            }).pipe(Effect.as(false)),
          ),
        );

        if (reaped) {
          reapedCount += 1;
        }
      }

      if (reapedCount > 0) {
        yield* Effect.logInfo("provider.session.reaper.sweep-complete", {
          reapedCount,
          totalBindings: bindings.length,
        });
      }
    });

    const start: ProviderSessionReaperShape["start"] = () =>
      Effect.gen(function* () {
        yield* reconcileOrphanedSessions().pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("provider.session.reaper.startup-reconcile-failed", {
              cause,
            }),
          ),
        );

        yield* Effect.forkScoped(
          sweep.pipe(
            Effect.catch((error: unknown) =>
              Effect.logWarning("provider.session.reaper.sweep-failed", {
                error,
              }),
            ),
            Effect.catchDefect((defect: unknown) =>
              Effect.logWarning("provider.session.reaper.sweep-defect", {
                defect,
              }),
            ),
            Effect.repeat(Schedule.spaced(Duration.millis(sweepIntervalMs))),
          ),
        );

        yield* Effect.logInfo("provider.session.reaper.started", {
          inactivityThresholdMs,
          sweepIntervalMs,
        });
      });

    return {
      start,
    } satisfies ProviderSessionReaperShape;
  });

export const makeProviderSessionReaperLive = (options?: ProviderSessionReaperLiveOptions) =>
  Layer.effect(ProviderSessionReaper, makeProviderSessionReaper(options));

export const ProviderSessionReaperLive = makeProviderSessionReaperLive();
