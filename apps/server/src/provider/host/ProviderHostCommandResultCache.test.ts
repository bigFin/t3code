import { CommandId } from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";

import { makeProviderHostCommandResultCache } from "./ProviderHostCommandResultCache.ts";

describe("ProviderHostCommandResultCache", () => {
  it.effect("coalesces concurrent duplicate execution onto one outcome", () =>
    Effect.gen(function* () {
      const cache = yield* makeProviderHostCommandResultCache<{ readonly value: number }, Error>(4);
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const executions = yield* Ref.make(0);
      const outcome = { value: 42 };
      const commandId = CommandId.make("command-1");

      const first = yield* cache
        .execute(
          commandId,
          Effect.gen(function* () {
            yield* Ref.update(executions, (count) => count + 1);
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
            return outcome;
          }),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(started);

      const second = yield* cache
        .execute(
          commandId,
          Effect.sync(() => {
            throw new Error("duplicate operation executed");
          }),
        )
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;

      assert.equal(yield* Ref.get(executions), 1);
      yield* Deferred.succeed(release, undefined);

      assert.strictEqual(yield* Fiber.join(first), outcome);
      assert.strictEqual(yield* Fiber.join(second), outcome);
      assert.equal(yield* cache.size, 1);
    }),
  );

  it.effect("returns the original typed failure without re-executing", () =>
    Effect.gen(function* () {
      const cache = yield* makeProviderHostCommandResultCache<number, Error>(2);
      const failure = new Error("original failure");
      const duplicateExecutions = yield* Ref.make(0);
      const commandId = CommandId.make("command-failure");

      const firstFailure = yield* cache.execute(commandId, Effect.fail(failure)).pipe(Effect.flip);
      const duplicateFailure = yield* cache
        .execute(
          commandId,
          Ref.update(duplicateExecutions, (count) => count + 1).pipe(Effect.as(2)),
        )
        .pipe(Effect.flip);

      assert.strictEqual(firstFailure, failure);
      assert.strictEqual(duplicateFailure, failure);
      assert.equal(yield* Ref.get(duplicateExecutions), 0);
    }),
  );

  it.effect("bounds completed outcomes and retains recently reused command ids", () =>
    Effect.gen(function* () {
      const cache = yield* makeProviderHostCommandResultCache<number, Error>(2);
      const reruns = yield* Ref.make(0);
      const firstId = CommandId.make("command-first");
      const secondId = CommandId.make("command-second");
      const thirdId = CommandId.make("command-third");

      assert.equal(yield* cache.execute(firstId, Effect.succeed(1)), 1);
      assert.equal(yield* cache.execute(secondId, Effect.succeed(2)), 2);
      assert.equal(yield* cache.execute(firstId, Effect.succeed(99)), 1);
      assert.equal(yield* cache.execute(thirdId, Effect.succeed(3)), 3);
      assert.equal(yield* cache.size, 2);

      const rerunSecond = yield* cache.execute(
        secondId,
        Ref.update(reruns, (count) => count + 1).pipe(Effect.as(20)),
      );
      const cachedThird = yield* cache.execute(thirdId, Effect.succeed(30));

      assert.equal(rerunSecond, 20);
      assert.equal(cachedThird, 3);
      assert.equal(yield* Ref.get(reruns), 1);
      assert.equal(yield* cache.size, 2);
    }),
  );

  it.effect("evicts completed outcomes to stay within the byte budget", () =>
    Effect.gen(function* () {
      const cache = yield* makeProviderHostCommandResultCache<string, Error>(8, {
        maxCompletedBytes: 5,
        completedEntryBytes: (value) => value.length,
      });
      const reruns = yield* Ref.make(0);
      const firstId = CommandId.make("command-byte-first");
      const secondId = CommandId.make("command-byte-second");

      assert.equal(yield* cache.execute(firstId, Effect.succeed("aaa")), "aaa");
      assert.equal(yield* cache.execute(secondId, Effect.succeed("bbb")), "bbb");
      assert.equal(yield* cache.completedBytes, 3);
      assert.equal(yield* cache.size, 1);
      assert.equal(yield* cache.execute(secondId, Effect.succeed("unused")), "bbb");

      assert.equal(
        yield* cache.execute(
          firstId,
          Ref.update(reruns, (count) => count + 1).pipe(Effect.as("aa")),
        ),
        "aa",
      );
      assert.equal(yield* Ref.get(reruns), 1);
      assert.equal(yield* cache.completedBytes, 5);
      assert.equal(yield* cache.size, 2);
    }),
  );

  it.effect("applies backpressure when every bounded entry is still running", () =>
    Effect.gen(function* () {
      const cache = yield* makeProviderHostCommandResultCache<number, Error>(1);
      const firstStarted = yield* Deferred.make<void>();
      const releaseFirst = yield* Deferred.make<void>();
      const secondStarted = yield* Deferred.make<void>();

      const first = yield* cache
        .execute(
          CommandId.make("command-first"),
          Effect.gen(function* () {
            yield* Deferred.succeed(firstStarted, undefined);
            yield* Deferred.await(releaseFirst);
            return 1;
          }),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(firstStarted);

      const second = yield* cache
        .execute(
          CommandId.make("command-second"),
          Deferred.succeed(secondStarted, undefined).pipe(Effect.as(2)),
        )
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;

      assert.isFalse(yield* Deferred.isDone(secondStarted));
      assert.equal(yield* cache.size, 1);

      yield* Deferred.succeed(releaseFirst, undefined);
      assert.equal(yield* Fiber.join(first), 1);
      assert.equal(yield* Fiber.join(second), 2);
      assert.isTrue(yield* Deferred.isDone(secondStarted));
      assert.equal(yield* cache.size, 1);
    }),
  );

  it("rejects invalid capacities", () => {
    assert.throws(() => makeProviderHostCommandResultCache(0), RangeError);
    assert.throws(() => makeProviderHostCommandResultCache(Number.POSITIVE_INFINITY), RangeError);
    assert.throws(
      () =>
        makeProviderHostCommandResultCache(1, {
          maxCompletedBytes: 1,
        }),
      RangeError,
    );
  });
});
