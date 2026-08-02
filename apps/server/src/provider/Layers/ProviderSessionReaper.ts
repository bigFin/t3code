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
import {
  ProviderSessionDirectory,
  type ProviderRuntimeBindingWithMetadata,
} from "../Services/ProviderSessionDirectory.ts";
import type { ProviderSessionPersistence } from "../Services/ProviderAdapter.ts";
import {
  ProviderSessionReaper,
  type ProviderSessionReaperShape,
} from "../Services/ProviderSessionReaper.ts";
import { forkParked, ServerActivation } from "../../serverActivation.ts";
import { ProviderService } from "../Services/ProviderService.ts";

const DEFAULT_INACTIVITY_THRESHOLD_MS = 30 * 60 * 1000;
const DEFAULT_SWEEP_INTERVAL_MS = 5 * 60 * 1000;
const DEFAULT_REATTACH_RETRY_INTERVAL_MS = 15 * 1000;
const RESTART_INTERRUPTION_MESSAGE =
  "The T3 server restarted while this turn was running. T3 recovered the available transcript. Send a message to continue.";
const DETACHED_RUNTIME_RECONNECTING_MESSAGE =
  "T3 could not reattach to the detached provider execution yet. The provider was not interrupted; T3 will keep retrying.";
const DETACHED_RUNTIME_MISSING_MESSAGE =
  "The detached provider execution is no longer present. T3 did not resume it automatically. Send a message to continue.";

export interface ProviderSessionReaperLiveOptions {
  readonly inactivityThresholdMs?: number;
  readonly sweepIntervalMs?: number;
  readonly reattachRetryIntervalMs?: number;
}

function wasStoppedByServerShutdown(binding: ProviderRuntimeBindingWithMetadata): boolean {
  const runtimePayload = binding.runtimePayload;
  return (
    binding.status === "stopped" &&
    runtimePayload !== null &&
    typeof runtimePayload === "object" &&
    !Array.isArray(runtimePayload) &&
    "lastRuntimeEvent" in runtimePayload &&
    runtimePayload.lastRuntimeEvent === "provider.stopAll"
  );
}

function wasDetachedByServerShutdown(binding: ProviderRuntimeBindingWithMetadata): boolean {
  const runtimePayload = binding.runtimePayload;
  return (
    runtimePayload !== null &&
    typeof runtimePayload === "object" &&
    !Array.isArray(runtimePayload) &&
    "lastRuntimeEvent" in runtimePayload &&
    runtimePayload.lastRuntimeEvent === "provider.detachAll"
  );
}

function hasDetachedReattachPending(binding: ProviderRuntimeBindingWithMetadata): boolean {
  const runtimePayload = binding.runtimePayload;
  return (
    runtimePayload !== null &&
    typeof runtimePayload === "object" &&
    !Array.isArray(runtimePayload) &&
    "lastRuntimeEvent" in runtimePayload &&
    runtimePayload.lastRuntimeEvent === "provider.session.detached-reattach-pending"
  );
}

function hasHonestDetachedReattachPending(binding: ProviderRuntimeBindingWithMetadata): boolean {
  const runtimePayload = binding.runtimePayload;
  return (
    hasDetachedReattachPending(binding) &&
    binding.status === "error" &&
    runtimePayload !== null &&
    typeof runtimePayload === "object" &&
    !Array.isArray(runtimePayload) &&
    "activeTurnId" in runtimePayload &&
    runtimePayload.activeTurnId === null
  );
}

function readPersistedSessionPersistence(
  binding: ProviderRuntimeBindingWithMetadata,
): ProviderSessionPersistence | undefined {
  const runtimePayload = binding.runtimePayload;
  if (
    runtimePayload === null ||
    typeof runtimePayload !== "object" ||
    Array.isArray(runtimePayload) ||
    !("sessionPersistence" in runtimePayload)
  ) {
    return undefined;
  }
  return runtimePayload.sessionPersistence === "detached" ||
    runtimePayload.sessionPersistence === "process-bound"
    ? runtimePayload.sessionPersistence
    : undefined;
}

function startupStatusCommandId(
  transition: "startup-detached-missing" | "startup-reattach-retrying",
  binding: ProviderRuntimeBindingWithMetadata,
): CommandId {
  return CommandId.make(
    ["provider-session-reaper", transition, binding.threadId, binding.lastSeenAt].join(":"),
  );
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
    const reattachRetryIntervalMs = Math.max(
      1,
      options?.reattachRetryIntervalMs ?? DEFAULT_REATTACH_RETRY_INTERVAL_MS,
    );

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
        const orphanedCandidates = bindings.filter((binding) => {
          if (binding.providerInstanceId === undefined) {
            return false;
          }
          if (liveThreadIdsByInstance.get(binding.providerInstanceId)?.has(binding.threadId)) {
            return false;
          }
          return (
            binding.status === "starting" ||
            binding.status === "running" ||
            (binding.status === "error" &&
              readPersistedSessionPersistence(binding) === "detached") ||
            wasStoppedByServerShutdown(binding) ||
            wasDetachedByServerShutdown(binding)
          );
        });
        if (orphanedCandidates.length === 0) {
          return;
        }

        const threadShells = yield* projectionSnapshotQuery.getThreadShellsByIds(
          orphanedCandidates.map((binding) => binding.threadId),
        );
        const orphanedBindings = orphanedCandidates.filter((binding) => {
          if (!wasStoppedByServerShutdown(binding)) {
            return true;
          }
          const session = threadShells.get(binding.threadId)?.session;
          return session?.status === "starting" || session?.status === "running";
        });
        if (orphanedBindings.length === 0) {
          return;
        }

        const reconciledAt = DateTime.formatIso(yield* DateTime.now);
        const reconcileOrphanedSession = Effect.fn(
          "ProviderSessionReaper.reconcileOrphanedSession",
        )(function* (binding: (typeof orphanedBindings)[number]) {
          if (binding.providerInstanceId === undefined) {
            return { reconciled: false, interrupted: false, reattached: false };
          }

          const session = threadShells.get(binding.threadId)?.session;
          const capabilitiesExit = yield* Effect.exit(
            providerService.getCapabilities(binding.providerInstanceId),
          );
          const persistedSessionPersistence = readPersistedSessionPersistence(binding);
          if (
            !wasDetachedByServerShutdown(binding) &&
            persistedSessionPersistence === undefined &&
            Exit.isFailure(capabilitiesExit)
          ) {
            yield* Effect.logWarning("provider.session.reaper.startup-capabilities-unavailable", {
              threadId: binding.threadId,
              provider: binding.provider,
              providerInstanceId: binding.providerInstanceId,
              cause: capabilitiesExit.cause,
            });
            return { reconciled: false, interrupted: false, reattached: false };
          }
          const detached =
            wasDetachedByServerShutdown(binding) ||
            persistedSessionPersistence === "detached" ||
            (Exit.isSuccess(capabilitiesExit) &&
              capabilitiesExit.value.sessionPersistence === "detached");
          if (detached) {
            const reattachResult = yield* providerService
              .reattachSession({ threadId: binding.threadId })
              .pipe(
                Effect.map((reattached) => ({ _tag: "reattached" as const, reattached })),
                Effect.catchTag("ProviderAdapterSessionNotFoundError", (error) =>
                  Effect.succeed({ _tag: "missing" as const, error }),
                ),
                Effect.catchCause((cause) => Effect.succeed({ _tag: "failed" as const, cause })),
              );
            if (reattachResult._tag === "reattached") {
              const reattached = reattachResult.reattached;
              yield* Effect.logInfo("provider.session.reaper.startup-reattached", {
                threadId: binding.threadId,
                provider: binding.provider,
                providerInstanceId: binding.providerInstanceId,
                status: reattached.status,
                activeTurnId: reattached.activeTurnId,
              });
              return { reconciled: true, interrupted: false, reattached: true };
            }
            if (reattachResult._tag === "missing") {
              const interrupted = session?.status === "starting" || session?.status === "running";
              yield* orchestrationEngine.dispatch({
                type: "thread.session.set",
                commandId: startupStatusCommandId("startup-detached-missing", binding),
                threadId: binding.threadId,
                session: {
                  threadId: binding.threadId,
                  status: interrupted ? "interrupted" : "stopped",
                  providerName: session?.providerName ?? binding.provider,
                  providerInstanceId: binding.providerInstanceId,
                  runtimeMode: session?.runtimeMode ?? binding.runtimeMode ?? "full-access",
                  activeTurnId: null,
                  lastError: DETACHED_RUNTIME_MISSING_MESSAGE,
                  retrying: false,
                  updatedAt: reconciledAt,
                },
                createdAt: reconciledAt,
              });
              yield* directory.upsert({
                threadId: binding.threadId,
                provider: binding.provider,
                providerInstanceId: binding.providerInstanceId,
                status: "stopped",
                runtimePayload: {
                  activeTurnId: null,
                  lastRuntimeEvent: "provider.session.detached-runtime-missing-on-startup",
                  lastRuntimeEventAt: reconciledAt,
                  sessionPersistence: "detached",
                },
              });
              yield* Effect.logInfo("provider.session.reaper.startup-detached-runtime-missing", {
                threadId: binding.threadId,
                provider: binding.provider,
                providerInstanceId: binding.providerInstanceId,
                interrupted,
                cause: reattachResult.error,
              });
              return { reconciled: true, interrupted, reattached: false };
            }
            if (hasHonestDetachedReattachPending(binding)) {
              yield* Effect.logDebug("provider.session.reaper.startup-reattach-still-pending", {
                threadId: binding.threadId,
                provider: binding.provider,
                providerInstanceId: binding.providerInstanceId,
              });
              return { reconciled: false, interrupted: false, reattached: false };
            }
            yield* Effect.logWarning("provider.session.reaper.startup-reattach-failed", {
              threadId: binding.threadId,
              provider: binding.provider,
              providerInstanceId: binding.providerInstanceId,
              cause: reattachResult.cause,
            });
            yield* orchestrationEngine.dispatch({
              type: "thread.session.set",
              commandId: startupStatusCommandId("startup-reattach-retrying", binding),
              threadId: binding.threadId,
              session: {
                threadId: binding.threadId,
                status: "error",
                providerName: session?.providerName ?? binding.provider,
                providerInstanceId: binding.providerInstanceId,
                runtimeMode: session?.runtimeMode ?? binding.runtimeMode ?? "full-access",
                activeTurnId: null,
                lastError: DETACHED_RUNTIME_RECONNECTING_MESSAGE,
                retrying: true,
                updatedAt: reconciledAt,
              },
              createdAt: reconciledAt,
            });
            yield* directory.upsert({
              threadId: binding.threadId,
              provider: binding.provider,
              providerInstanceId: binding.providerInstanceId,
              status: "error",
              runtimePayload: {
                activeTurnId: null,
                lastRuntimeEvent: "provider.session.detached-reattach-pending",
                lastRuntimeEventAt: reconciledAt,
                sessionPersistence: "detached",
              },
            });
            return { reconciled: true, interrupted: false, reattached: false };
          }

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
              lastRuntimeEvent: detached
                ? "provider.session.detached-runtime-missing-on-startup"
                : "provider.session.orphaned-on-startup",
              lastRuntimeEventAt: reconciledAt,
            },
          });
          return { reconciled: true, interrupted, reattached: false };
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
                }).pipe(Effect.as({ reconciled: false, interrupted: false, reattached: false })),
              ),
            ),
          { concurrency: 4 },
        );
        const reconciledCount = reconciliationResults.filter((result) => result.reconciled).length;
        const interruptedCount = reconciliationResults.filter(
          (result) => result.interrupted,
        ).length;
        const reattachedCount = reconciliationResults.filter((result) => result.reattached).length;

        if (reconciledCount > 0) {
          yield* Effect.logInfo("provider.session.reaper.startup-reconciled", {
            orphanedCount: orphanedBindings.length,
            reconciledCount,
            interruptedCount,
            reattachedCount,
            shutdownStoppedCount: orphanedBindings.filter(wasStoppedByServerShutdown).length,
          });
        }
      },
    );

    const sweep = Effect.gen(function* () {
      const bindings = yield* directory.listBindings();
      const now = yield* Clock.currentTimeMillis;
      let reapedCount = 0;
      const persistenceByInstance = new Map<
        ProviderInstanceId,
        ProviderSessionPersistence | "unknown"
      >();

      for (const binding of bindings) {
        if (binding.status === "stopped") {
          continue;
        }
        if (hasDetachedReattachPending(binding)) {
          continue;
        }
        if (binding.providerInstanceId === undefined) {
          continue;
        }
        const persistedSessionPersistence = readPersistedSessionPersistence(binding);
        let sessionPersistence = persistedSessionPersistence;
        if (sessionPersistence === undefined) {
          const cached = persistenceByInstance.get(binding.providerInstanceId);
          if (cached !== undefined) {
            sessionPersistence = cached === "unknown" ? undefined : cached;
          } else {
            const capabilitiesExit = yield* Effect.exit(
              providerService.getCapabilities(binding.providerInstanceId),
            );
            if (Exit.isFailure(capabilitiesExit)) {
              persistenceByInstance.set(binding.providerInstanceId, "unknown");
              yield* Effect.logWarning("provider.session.reaper.sweep-capabilities-unavailable", {
                threadId: binding.threadId,
                provider: binding.provider,
                providerInstanceId: binding.providerInstanceId,
                cause: capabilitiesExit.cause,
              });
              continue;
            }
            sessionPersistence = capabilitiesExit.value.sessionPersistence ?? "process-bound";
            persistenceByInstance.set(binding.providerInstanceId, sessionPersistence);
          }
        }
        if (sessionPersistence === "detached") {
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

    const startActivated = Effect.gen(function* () {
      yield* reconcileOrphanedSessions().pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("provider.session.reaper.startup-reconcile-failed", {
            cause,
          }),
        ),
      );

      yield* Effect.forkScoped(
        Effect.forever(
          Effect.sleep(Duration.millis(reattachRetryIntervalMs)).pipe(
            Effect.andThen(
              reconcileOrphanedSessions().pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning("provider.session.reaper.retry-reconcile-failed", {
                    cause,
                  }),
                ),
              ),
            ),
          ),
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
    });

    const start: ProviderSessionReaperShape["start"] = () =>
      Effect.gen(function* () {
        const activation = yield* ServerActivation;
        if (activation === undefined) {
          yield* startActivated;
        } else {
          yield* forkParked(startActivated);
        }

        yield* Effect.logInfo("provider.session.reaper.started", {
          inactivityThresholdMs,
          sweepIntervalMs,
          reattachRetryIntervalMs,
        });
      });

    return {
      start,
    } satisfies ProviderSessionReaperShape;
  });

export const makeProviderSessionReaperLive = (options?: ProviderSessionReaperLiveOptions) =>
  Layer.effect(ProviderSessionReaper, makeProviderSessionReaper(options));

export const ProviderSessionReaperLive = makeProviderSessionReaperLive();
