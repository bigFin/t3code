// @effect-diagnostics nodeBuiltinImport:off
import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeChildProcess from "node:child_process";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as ServerInstanceLock from "./serverInstanceLock.ts";
import * as ServerRuntimeState from "./serverRuntimeState.ts";

const isServerInstanceLockError = Schema.is(ServerInstanceLock.ServerInstanceLockError);

const releaseLock = (lock: ServerInstanceLock.ServerInstanceLock) =>
  Effect.promise(() => lock.release()).pipe(Effect.orDie);

const waitForOutput = (child: NodeChildProcess.ChildProcess, expected: string): Promise<string> =>
  new Promise((resolve, reject) => {
    let output = "";
    const onData = (chunk: Buffer | string) => {
      output += chunk.toString();
      if (!output.includes(expected)) return;
      cleanup();
      resolve(output);
    };
    const onError = (cause: Error) => {
      cleanup();
      reject(cause);
    };
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();
      reject(
        new Error(
          `Lock-holder process exited before reporting readiness (${String(code ?? signal)}).`,
        ),
      );
    };
    const cleanup = () => {
      child.stdout?.off("data", onData);
      child.off("error", onError);
      child.off("exit", onExit);
    };
    child.stdout?.on("data", onData);
    child.once("error", onError);
    child.once("exit", onExit);
  });

const waitForExit = (child: NodeChildProcess.ChildProcess): Promise<void> => {
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve();
  return new Promise((resolve) => child.once("exit", () => resolve()));
};

describe("serverInstanceLock", () => {
  it.effect("rejects a second server and reports the active runtime", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const stateDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-server-instance-lock-test-",
      });
      const serverRuntimeStatePath = path.join(stateDir, "server-runtime.json");
      const config = { stateDir, serverRuntimeStatePath };
      const runtime: ServerRuntimeState.PersistedServerRuntimeState = {
        version: 1,
        pid: 123,
        port: 4_971,
        origin: "http://127.0.0.1:4971",
        startedAt: "2026-08-04T00:00:00.000Z",
      };
      yield* ServerRuntimeState.persistServerRuntimeState({
        path: serverRuntimeStatePath,
        state: runtime,
      });

      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* Effect.acquireRelease(
            ServerInstanceLock.acquireServerInstanceLock(config),
            releaseLock,
          );

          const error = yield* ServerInstanceLock.acquireServerInstanceLock(config).pipe(
            Effect.flip,
          );
          assert.isTrue(isServerInstanceLockError(error));
          if (isServerInstanceLockError(error)) {
            assert.equal(error.reason, "already-running");
            assert.equal(error.runtimePid, runtime.pid);
            assert.equal(error.runtimeOrigin, runtime.origin);
            assert.include(error.message, stateDir);
            assert.include(error.message, `pid ${runtime.pid}`);
          }
        }),
      );

      const replacement = yield* ServerInstanceLock.acquireServerInstanceLock(config);
      yield* releaseLock(replacement);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("releases the lock after a crash without affecting detached provider work", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const stateDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-server-instance-lock-crash-test-",
      });
      const serverRuntimeStatePath = path.join(stateDir, "server-runtime.json");
      const lockPath = ServerInstanceLock.serverInstanceLockPath(stateDir);
      const child = yield* Effect.acquireRelease(
        Effect.sync(() =>
          NodeChildProcess.spawn(
            process.execPath,
            [
              "-e",
              `
const { DatabaseSync } = require("node:sqlite");
const { spawn } = require("node:child_process");
const database = new DatabaseSync(process.env.T3_TEST_LOCK_PATH, { timeout: 0 });
database.exec("BEGIN IMMEDIATE");
const provider = spawn(process.execPath, ["-e", "setInterval(() => {}, 2 ** 30)"], {
  detached: true,
  stdio: "ignore",
});
provider.unref();
process.stdout.write(\`locked:\${provider.pid}\\n\`);
setInterval(() => {}, 2 ** 30);
`,
            ],
            {
              env: { ...process.env, T3_TEST_LOCK_PATH: lockPath },
              stdio: ["ignore", "pipe", "inherit"],
            },
          ),
        ),
        (child) =>
          Effect.promise(async () => {
            if (child.exitCode === null && child.signalCode === null) {
              child.kill("SIGKILL");
              await waitForExit(child);
            }
          }).pipe(Effect.orDie),
      );
      const receipt = yield* Effect.promise(() => waitForOutput(child, "locked:"));
      const providerPid = Number.parseInt(receipt.trim().slice("locked:".length), 10);
      assert.isTrue(Number.isInteger(providerPid) && providerPid > 0);
      yield* Effect.acquireRelease(Effect.succeed(providerPid), (pid) =>
        Effect.sync(() => {
          try {
            process.kill(pid, "SIGKILL");
          } catch {}
        }),
      );

      const blocked = yield* ServerInstanceLock.acquireServerInstanceLock({
        stateDir,
        serverRuntimeStatePath,
      }).pipe(Effect.flip);
      assert.isTrue(isServerInstanceLockError(blocked));
      if (isServerInstanceLockError(blocked)) {
        assert.equal(blocked.reason, "already-running");
      }

      child.kill("SIGKILL");
      yield* Effect.promise(() => waitForExit(child));
      assert.doesNotThrow(() => process.kill(providerPid, 0));

      const recovered = yield* ServerInstanceLock.acquireServerInstanceLock({
        stateDir,
        serverRuntimeStatePath,
      });
      yield* releaseLock(recovered);
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
