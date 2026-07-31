import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";

import {
  makeCodexClientLeasePool,
  type CodexClientLeasePoolTarget,
} from "./CodexClientLeasePool.ts";

interface TestResource {
  readonly id: number;
  running: boolean;
}

function target(leaseKey: string, configKey: string): CodexClientLeasePoolTarget {
  return { leaseKey, configKey };
}

function makeTestPool(openFailure?: Error) {
  const opened: TestResource[] = [];
  const closed: number[] = [];
  const make = makeCodexClientLeasePool<CodexClientLeasePoolTarget, TestResource, Error, never>({
    open: (_target, scope) =>
      Effect.gen(function* () {
        const resource = {
          id: opened.length + 1,
          running: true,
        };
        opened.push(resource);
        yield* Scope.addFinalizer(
          scope,
          Effect.sync(() => {
            closed.push(resource.id);
          }),
        );
        if (openFailure !== undefined) {
          return yield* Effect.fail(openFailure);
        }
        return resource;
      }),
    isRunning: (resource) => Effect.succeed(resource.running),
  });
  return { closed, make, opened };
}

describe("CodexClientLeasePool", () => {
  it.effect("reuses the same running client for an unchanged target", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const testPool = makeTestPool();
        const pool = yield* testPool.make;

        const first = yield* pool.acquire(target("/shared", "config-a"));
        const second = yield* pool.acquire(target("/shared", "config-a"));

        expect(first.reused).toBe(false);
        expect(second.reused).toBe(true);
        expect(second.lease.resource).toBe(first.lease.resource);
        expect(testPool.opened).toHaveLength(1);
        expect(testPool.closed).toEqual([]);
      }),
    ),
  );

  it.effect("closes and replaces a client when its config changes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const testPool = makeTestPool();
        const pool = yield* testPool.make;

        yield* pool.acquire(target("/shared", "config-a"));
        const replacement = yield* pool.acquire(target("/shared", "config-b"));

        expect(replacement.restarted).toBe(true);
        expect(replacement.reused).toBe(false);
        expect(testPool.opened).toHaveLength(2);
        expect(testPool.closed).toEqual([1]);
      }),
    ),
  );

  it.effect("closes and replaces a client after its process exits", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const testPool = makeTestPool();
        const pool = yield* testPool.make;

        const first = yield* pool.acquire(target("/shared", "config-a"));
        first.lease.resource.running = false;
        const replacement = yield* pool.acquire(target("/shared", "config-a"));

        expect(replacement.restarted).toBe(true);
        expect(replacement.reused).toBe(false);
        expect(testPool.opened).toHaveLength(2);
        expect(testPool.closed).toEqual([1]);
      }),
    ),
  );

  it.effect("closes clients for removed targets", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const testPool = makeTestPool();
        const pool = yield* testPool.make;

        yield* pool.acquire(target("/first", "config-a"));
        yield* pool.acquire(target("/second", "config-b"));
        yield* pool.reconcile([target("/second", "config-b")]);

        expect(testPool.closed).toEqual([1]);
        const retained = yield* pool.acquire(target("/second", "config-b"));
        expect(retained.reused).toBe(true);
      }),
    ),
  );

  it.effect("marks the next client as restarted after invalidation", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const testPool = makeTestPool();
        const pool = yield* testPool.make;
        const sharedTarget = target("/shared", "config-a");

        const first = yield* pool.acquire(sharedTarget);
        yield* pool.invalidate(sharedTarget, first.lease);
        const replacement = yield* pool.acquire(sharedTarget);

        expect(replacement.restarted).toBe(true);
        expect(replacement.reused).toBe(false);
        expect(testPool.closed).toEqual([1]);
      }),
    ),
  );

  it.effect("closes every client when its parent scope shuts down", () =>
    Effect.gen(function* () {
      const testPool = makeTestPool();
      const scope = yield* Scope.make("sequential");
      const pool = yield* testPool.make.pipe(Effect.provideService(Scope.Scope, scope));

      yield* pool.acquire(target("/first", "config-a"));
      yield* pool.acquire(target("/second", "config-b"));
      expect(testPool.closed).toEqual([]);

      yield* Scope.close(scope, Exit.void);

      expect(testPool.closed).toEqual([1, 2]);
    }),
  );

  it.effect("closes a partial client scope when acquisition fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const testPool = makeTestPool(new Error("open failed"));
        const pool = yield* testPool.make;

        const result = yield* pool.acquire(target("/shared", "config-a")).pipe(Effect.result);

        expect(result._tag).toBe("Failure");
        expect(testPool.opened).toHaveLength(1);
        expect(testPool.closed).toEqual([1]);
      }),
    ),
  );
});
