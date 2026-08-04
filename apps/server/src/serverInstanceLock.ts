// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";

import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type * as ServerConfig from "./config.ts";
import {
  acquireSqliteTransactionLock,
  isSqliteTransactionLockBusy,
  type SqliteTransactionLock,
} from "./sqliteTransactionLock.ts";
import { readPersistedServerRuntimeState } from "./serverRuntimeState.ts";

const SERVER_INSTANCE_LOCK_FILE = "server-instance.lock.sqlite";

type ServerInstanceLockConfig = Pick<
  ServerConfig.ServerConfig["Service"],
  "stateDir" | "serverRuntimeStatePath"
>;

export interface ServerInstanceLock {
  readonly path: string;
  readonly release: () => Promise<void>;
}

class ServerInstanceLockAcquireError extends Schema.TaggedErrorClass<ServerInstanceLockAcquireError>()(
  "ServerInstanceLockAcquireError",
  {
    cause: Schema.Defect(),
  },
) {}

export class ServerInstanceLockError extends Schema.TaggedErrorClass<ServerInstanceLockError>()(
  "ServerInstanceLockError",
  {
    reason: Schema.Literals(["already-running", "acquire"]),
    stateDir: Schema.String,
    lockPath: Schema.String,
    runtimePid: Schema.optional(Schema.Int),
    runtimeOrigin: Schema.optional(Schema.String),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    if (this.reason === "already-running") {
      const runtime =
        this.runtimePid === undefined
          ? ""
          : ` (pid ${this.runtimePid}${this.runtimeOrigin ? ` at ${this.runtimeOrigin}` : ""})`;
      return `Another T3 server is already using ${this.stateDir}${runtime}. Connect to that server or stop it before starting another one.`;
    }
    return `Failed to acquire the T3 server instance lock at ${this.lockPath}.`;
  }
}

export const serverInstanceLockPath = (stateDir: string): string =>
  NodePath.join(stateDir, SERVER_INSTANCE_LOCK_FILE);

export const acquireServerInstanceLock = Effect.fn("acquireServerInstanceLock")(function* (
  config: ServerInstanceLockConfig,
) {
  const lockPath = serverInstanceLockPath(config.stateDir);
  const lock: SqliteTransactionLock = yield* Effect.tryPromise({
    try: (signal) =>
      acquireSqliteTransactionLock(lockPath, {
        signal,
        timeoutMs: 0,
      }),
    catch: (cause) => new ServerInstanceLockAcquireError({ cause }),
  }).pipe(
    Effect.catchTag("ServerInstanceLockAcquireError", (error) =>
      Effect.gen(function* () {
        const reason = isSqliteTransactionLockBusy(error.cause) ? "already-running" : "acquire";
        const runtimeState =
          reason === "already-running"
            ? yield* readPersistedServerRuntimeState(config.serverRuntimeStatePath)
            : Option.none();
        const runtime = Option.getOrUndefined(runtimeState);

        return yield* new ServerInstanceLockError({
          reason,
          stateDir: config.stateDir,
          lockPath,
          ...(runtime ? { runtimePid: runtime.pid, runtimeOrigin: runtime.origin } : {}),
          cause: error.cause,
        });
      }),
    ),
  );

  return {
    path: lockPath,
    release: lock.release,
  } satisfies ServerInstanceLock;
});
