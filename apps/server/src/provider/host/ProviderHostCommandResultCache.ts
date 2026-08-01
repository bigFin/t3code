import type { CommandId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Ref from "effect/Ref";

type EntryStatus = "pending" | "settling" | "completed";

interface CacheEntry<A, E> {
  readonly outcome: Deferred.Deferred<A, E>;
  readonly settled: Deferred.Deferred<void>;
  readonly status: EntryStatus;
  readonly completedBytes: number;
}

interface CacheState<A, E> {
  readonly entries: ReadonlyMap<CommandId, CacheEntry<A, E>>;
  readonly completedBytes: number;
}

export interface ProviderHostCommandResultCacheOptions<A> {
  readonly maxCompletedBytes?: number;
  readonly completedEntryBytes?: (value: A) => number;
}

type Admission<A, E> =
  | {
      readonly _tag: "Execute";
      readonly entry: CacheEntry<A, E>;
    }
  | {
      readonly _tag: "Await";
      readonly entry: CacheEntry<A, E>;
    }
  | {
      readonly _tag: "WaitForSpace";
      readonly entry: CacheEntry<A, E>;
    };

export interface ProviderHostCommandResultCache<A, E> {
  readonly capacity: number;
  readonly size: Effect.Effect<number>;
  readonly completedBytes: Effect.Effect<number>;
  /**
   * Executes a command once within the bounded idempotency window. Concurrent
   * and completed duplicates await the same stored outcome, including failures.
   */
  readonly execute: <R>(
    commandId: CommandId,
    operation: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>;
}

const validateCapacity = (capacity: number): void => {
  if (!Number.isSafeInteger(capacity) || capacity <= 0) {
    throw new RangeError("Provider-host command cache capacity must be a positive safe integer.");
  }
};

const validateMaxCompletedBytes = (maxCompletedBytes: number): void => {
  if (
    maxCompletedBytes !== Number.POSITIVE_INFINITY &&
    (!Number.isSafeInteger(maxCompletedBytes) || maxCompletedBytes <= 0)
  ) {
    throw new RangeError(
      "Provider-host command cache byte capacity must be a positive safe integer.",
    );
  }
};

const measuredCompletedBytes = <A>(
  value: A,
  measure: ((value: A) => number) | undefined,
): number => {
  if (!measure) return 0;
  const bytes = measure(value);
  if (!Number.isSafeInteger(bytes) || bytes < 0) {
    throw new RangeError(
      "Provider-host command cache completed entry size must be a non-negative safe integer.",
    );
  }
  return bytes;
};

const updateEntryStatus = <A, E>(
  state: CacheState<A, E>,
  commandId: CommandId,
  outcome: Deferred.Deferred<A, E>,
  status: EntryStatus,
): CacheState<A, E> => {
  const current = state.entries.get(commandId);
  if (current?.outcome !== outcome) {
    return state;
  }

  const next = new Map(state.entries);
  next.set(commandId, { ...current, status });
  return {
    entries: next,
    completedBytes: state.completedBytes,
  };
};

export const makeProviderHostCommandResultCache = <A, E>(
  capacity: number,
  options?: ProviderHostCommandResultCacheOptions<A>,
): Effect.Effect<ProviderHostCommandResultCache<A, E>> => {
  validateCapacity(capacity);
  const maxCompletedBytes = options?.maxCompletedBytes ?? Number.POSITIVE_INFINITY;
  validateMaxCompletedBytes(maxCompletedBytes);
  if (maxCompletedBytes !== Number.POSITIVE_INFINITY && !options?.completedEntryBytes) {
    throw new RangeError(
      "Provider-host command cache requires a completed entry size function when byte-bounded.",
    );
  }

  return Effect.map(
    Ref.make<CacheState<A, E>>({
      entries: new Map(),
      completedBytes: 0,
    }),
    (stateRef) => {
      const execute = <R>(
        commandId: CommandId,
        operation: Effect.Effect<A, E, R>,
      ): Effect.Effect<A, E, R> =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const candidateOutcome = yield* Deferred.make<A, E>();
            const candidateSettled = yield* Deferred.make<void>();
            const admission = yield* Ref.modify(
              stateRef,
              (state): [Admission<A, E>, CacheState<A, E>] => {
                const existing = state.entries.get(commandId);
                if (existing !== undefined) {
                  if (existing.status !== "completed") {
                    return [{ _tag: "Await", entry: existing }, state];
                  }

                  const touched = new Map(state.entries);
                  touched.delete(commandId);
                  touched.set(commandId, existing);
                  return [
                    { _tag: "Await", entry: existing },
                    {
                      entries: touched,
                      completedBytes: state.completedBytes,
                    },
                  ];
                }

                const nextEntry: CacheEntry<A, E> = {
                  outcome: candidateOutcome,
                  settled: candidateSettled,
                  status: "pending",
                  completedBytes: 0,
                };
                if (state.entries.size < capacity) {
                  const next = new Map(state.entries);
                  next.set(commandId, nextEntry);
                  return [
                    { _tag: "Execute", entry: nextEntry },
                    {
                      entries: next,
                      completedBytes: state.completedBytes,
                    },
                  ];
                }

                const completed = [...state.entries].find(
                  ([, entry]) => entry.status === "completed",
                );
                if (completed !== undefined) {
                  const next = new Map(state.entries);
                  next.delete(completed[0]);
                  next.set(commandId, nextEntry);
                  return [
                    { _tag: "Execute", entry: nextEntry },
                    {
                      entries: next,
                      completedBytes: state.completedBytes - completed[1].completedBytes,
                    },
                  ];
                }

                return [
                  {
                    _tag: "WaitForSpace",
                    entry: state.entries.values().next().value!,
                  },
                  state,
                ];
              },
            );

            if (admission._tag === "Await") {
              return yield* restore(Deferred.await(admission.entry.outcome));
            }
            if (admission._tag === "WaitForSpace") {
              yield* restore(Deferred.await(admission.entry.settled));
              return yield* restore(execute(commandId, operation));
            }

            const exit = yield* restore(operation).pipe(Effect.exit);
            yield* Ref.update(stateRef, (state) =>
              updateEntryStatus(state, commandId, admission.entry.outcome, "settling"),
            );
            yield* Deferred.done(admission.entry.outcome, exit);
            const completedBytes = Exit.isSuccess(exit)
              ? measuredCompletedBytes(exit.value, options?.completedEntryBytes)
              : 0;
            yield* Ref.update(stateRef, (state) => {
              const current = state.entries.get(commandId);
              if (current?.outcome !== admission.entry.outcome) {
                return state;
              }

              const next = new Map(state.entries);
              next.set(commandId, {
                ...current,
                status: "completed",
                completedBytes,
              });
              let nextCompletedBytes = state.completedBytes + completedBytes;
              for (const [candidateId, candidate] of next) {
                if (nextCompletedBytes <= maxCompletedBytes) break;
                if (candidateId === commandId || candidate.status !== "completed") continue;
                next.delete(candidateId);
                nextCompletedBytes -= candidate.completedBytes;
              }
              return {
                entries: next,
                completedBytes: nextCompletedBytes,
              };
            });
            yield* Deferred.succeed(admission.entry.settled, undefined);
            return yield* Deferred.await(admission.entry.outcome);
          }),
        );

      return {
        capacity,
        size: Effect.map(Ref.get(stateRef), (state) => state.entries.size),
        completedBytes: Effect.map(Ref.get(stateRef), (state) => state.completedBytes),
        execute,
      };
    },
  );
};
