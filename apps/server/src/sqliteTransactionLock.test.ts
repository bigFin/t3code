// @effect-diagnostics nodeBuiltinImport:off
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { afterEach, describe, it } from "vite-plus/test";

import { acquireSqliteTransactionLock } from "./sqliteTransactionLock.ts";

describe("acquireSqliteTransactionLock", () => {
  const tempDirs: string[] = [];

  afterEach(() => {
    for (const dir of tempDirs.splice(0)) {
      NodeFS.rmSync(dir, { recursive: true, force: true });
    }
  });

  const makeLockPath = () => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-sqlite-lock-"));
    tempDirs.push(dir);
    return NodePath.join(dir, "lock.sqlite");
  };

  it("serializes contenders and lets the next owner proceed after release", async () => {
    const lockPath = makeLockPath();
    const first = await acquireSqliteTransactionLock(lockPath, {
      retryDelayMs: 1,
      timeoutMs: 1_000,
    });
    let secondAcquired = false;
    const secondPromise = acquireSqliteTransactionLock(lockPath, {
      retryDelayMs: 1,
      timeoutMs: 1_000,
    }).then((lock) => {
      secondAcquired = true;
      return lock;
    });

    await Promise.resolve();
    NodeAssert.equal(secondAcquired, false);

    await first.release();
    const second = await secondPromise;
    NodeAssert.equal(secondAcquired, true);
    await second.release();
  });

  it("reuses a persistent lock database without stale-owner cleanup", async () => {
    const lockPath = makeLockPath();
    const first = await acquireSqliteTransactionLock(lockPath);
    await first.release();
    const second = await acquireSqliteTransactionLock(lockPath);
    await second.release();

    NodeAssert.equal(NodeFS.statSync(lockPath).mode & 0o777, 0o600);
  });
});
