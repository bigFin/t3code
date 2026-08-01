// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalDate:off - Lock acquisition uses a bounded wall-clock deadline.
// @effect-diagnostics globalTimers:off - Busy SQLite writers retry without blocking the event loop.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

export interface SqliteTransactionLockOptions {
  readonly retryDelayMs?: number;
  readonly timeoutMs?: number;
  readonly signal?: AbortSignal;
}

export interface SqliteTransactionLock {
  readonly release: () => Promise<void>;
}

const isSqliteBusy = (cause: unknown): boolean => {
  const error = cause as {
    readonly code?: unknown;
    readonly errcode?: unknown;
  };
  return error.code === "ERR_SQLITE_ERROR" && (error.errcode === 5 || error.errcode === 6);
};

const waitForRetry = (delayMs: number, signal: AbortSignal | undefined): Promise<void> =>
  new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    const finish = () => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    };
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason);
    };
    const timer = setTimeout(finish, delayMs);
    signal?.addEventListener("abort", onAbort, { once: true });
  });

/**
 * Acquires a process-crash-safe cross-process mutex backed by a SQLite write
 * transaction. The kernel releases the database lock when a process exits, so
 * no stale-owner unlink or PID-reuse recovery protocol is required.
 */
export async function acquireSqliteTransactionLock(
  lockPath: string,
  options: SqliteTransactionLockOptions = {},
): Promise<SqliteTransactionLock> {
  const retryDelayMs = options.retryDelayMs ?? 25;
  const timeoutMs = options.timeoutMs ?? 30_000;
  const deadline = Date.now() + timeoutMs;
  await NodeFSP.mkdir(NodePath.dirname(lockPath), { recursive: true, mode: 0o700 });
  const database = new NodeSqlite.DatabaseSync(lockPath, { timeout: 0 });
  await NodeFSP.chmod(lockPath, 0o600);

  try {
    while (true) {
      if (options.signal?.aborted) {
        throw options.signal.reason;
      }
      try {
        database.exec("BEGIN IMMEDIATE");
        break;
      } catch (cause) {
        if (!isSqliteBusy(cause) || Date.now() >= deadline) {
          throw cause;
        }
      }
      await waitForRetry(retryDelayMs, options.signal);
    }
  } catch (cause) {
    database.close();
    throw cause;
  }

  let released = false;
  return {
    release: async () => {
      if (released) return;
      released = true;
      try {
        database.exec("ROLLBACK");
      } finally {
        database.close();
      }
    },
  };
}
