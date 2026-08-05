import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";

export interface CodexClientLeasePoolTarget {
  readonly leaseKey: string;
  readonly configKey: string;
}

export interface CodexClientLease<Resource> {
  readonly configKey: string;
  readonly resource: Resource;
  readonly scope: Scope.Closeable;
}

export interface CodexClientLeaseAcquisition<Resource> {
  readonly lease: CodexClientLease<Resource>;
  readonly restarted: boolean;
  readonly reused: boolean;
}

export interface CodexClientLeasePool<Target, Resource, Error, Requirements> {
  readonly acquire: (
    target: Target,
  ) => Effect.Effect<CodexClientLeaseAcquisition<Resource>, Error, Requirements>;
  readonly invalidate: (target: Target, lease: CodexClientLease<Resource>) => Effect.Effect<void>;
  readonly reconcile: (targets: ReadonlyArray<Target>) => Effect.Effect<void>;
}

export const makeCodexClientLeasePool = <
  Target extends CodexClientLeasePoolTarget,
  Resource,
  Error,
  Requirements,
>(options: {
  readonly open: (
    target: Target,
    scope: Scope.Closeable,
  ) => Effect.Effect<Resource, Error, Requirements>;
  readonly isRunning: (resource: Resource) => Effect.Effect<boolean>;
}): Effect.Effect<
  CodexClientLeasePool<Target, Resource, Error, Requirements>,
  never,
  Scope.Scope
> =>
  Effect.gen(function* () {
    const leases = new Map<string, CodexClientLease<Resource>>();
    const restartPendingKeys = new Set<string>();

    const closeLease = (lease: CodexClientLease<Resource>) =>
      Scope.close(lease.scope, Exit.void).pipe(Effect.ignore);

    const closeAll = Effect.fn("CodexClientLeasePool.closeAll")(function* () {
      const current = [...leases.values()];
      leases.clear();
      restartPendingKeys.clear();
      yield* Effect.forEach(current, closeLease, { discard: true });
    });

    const acquire = Effect.fn("CodexClientLeasePool.acquire")(function* (target: Target) {
      const existing = leases.get(target.leaseKey);
      if (existing !== undefined && existing.configKey === target.configKey) {
        const isRunning = yield* options.isRunning(existing.resource);
        if (isRunning) {
          return {
            lease: existing,
            restarted: false,
            reused: true,
          } satisfies CodexClientLeaseAcquisition<Resource>;
        }
      }

      const restarted = existing !== undefined || restartPendingKeys.has(target.leaseKey);
      if (existing !== undefined) {
        leases.delete(target.leaseKey);
        yield* closeLease(existing);
      }
      if (restarted) {
        restartPendingKeys.add(target.leaseKey);
      }

      const scope = yield* Scope.make("sequential");
      const resource = yield* options
        .open(target, scope)
        .pipe(Effect.onError(() => Scope.close(scope, Exit.void).pipe(Effect.ignore)));
      const lease = {
        configKey: target.configKey,
        resource,
        scope,
      } satisfies CodexClientLease<Resource>;
      leases.set(target.leaseKey, lease);
      restartPendingKeys.add(target.leaseKey);
      return {
        lease,
        restarted,
        reused: false,
      } satisfies CodexClientLeaseAcquisition<Resource>;
    });

    const invalidate = Effect.fn("CodexClientLeasePool.invalidate")(function* (
      target: Target,
      lease: CodexClientLease<Resource>,
    ) {
      if (leases.get(target.leaseKey) !== lease) {
        return;
      }
      leases.delete(target.leaseKey);
      restartPendingKeys.add(target.leaseKey);
      yield* closeLease(lease);
    });

    const reconcile = Effect.fn("CodexClientLeasePool.reconcile")(function* (
      targets: ReadonlyArray<Target>,
    ) {
      const retainedKeys = new Set(targets.map((target) => target.leaseKey));
      for (const leaseKey of restartPendingKeys) {
        if (!retainedKeys.has(leaseKey)) {
          restartPendingKeys.delete(leaseKey);
        }
      }
      for (const [leaseKey, lease] of leases) {
        if (retainedKeys.has(leaseKey)) {
          continue;
        }
        leases.delete(leaseKey);
        restartPendingKeys.delete(leaseKey);
        yield* closeLease(lease);
      }
    });

    yield* Effect.addFinalizer(closeAll);

    return {
      acquire,
      invalidate,
      reconcile,
    } satisfies CodexClientLeasePool<Target, Resource, Error, Requirements>;
  });
