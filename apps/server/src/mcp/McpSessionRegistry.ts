import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { HttpServer } from "effect/unstable/http";

import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { acquireSqliteTransactionLock } from "../sqliteTransactionLock.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import * as McpProviderSession from "./McpProviderSession.ts";

export interface McpCredentialRequest {
  readonly threadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId;
}

export interface McpIssuedCredential {
  readonly config: McpProviderSession.McpProviderSessionConfig;
}

export interface McpSessionRegistryShape {
  readonly issue: (request: McpCredentialRequest) => Effect.Effect<McpIssuedCredential>;
  /**
   * Confirms that a newly issued credential reached its provider session.
   * Superseded credentials are retired only after this confirmation.
   */
  readonly activateProviderSession: (providerSessionId: string) => Effect.Effect<void>;
  readonly resolve: (
    rawToken: string,
  ) => Effect.Effect<McpInvocationContext.McpInvocationScope | undefined>;
  /**
   * Records a sign of life for the newest credential bound to `threadId` and
   * retires superseded credentials after a replacement session is active.
   */
  readonly touch: (threadId: ThreadId) => Effect.Effect<void>;
  readonly revokeProviderSession: (providerSessionId: string) => Effect.Effect<void>;
  readonly revokeThread: (threadId: ThreadId) => Effect.Effect<void>;
  readonly revokeAll: Effect.Effect<void>;
}

export class McpSessionRegistry extends Context.Service<
  McpSessionRegistry,
  McpSessionRegistryShape
>()("t3/mcp/McpSessionRegistry") {}

interface CredentialRecord {
  readonly tokenHash: string;
  readonly scope: McpInvocationContext.McpInvocationScope;
  readonly lastAliveAt: number;
  readonly active: boolean;
}

interface RegistryState {
  readonly records: ReadonlyMap<string, CredentialRecord>;
}

const PERSISTED_REGISTRY_FILE_NAME = "mcp-session-registry.json";

const PersistedCredentialRecord = Schema.Struct({
  tokenHash: Schema.String,
  scope: Schema.Struct({
    environmentId: EnvironmentId,
    threadId: ThreadId,
    providerSessionId: Schema.String,
    providerInstanceId: ProviderInstanceId,
    capabilities: Schema.Array(Schema.Literal("preview")),
    issuedAt: Schema.Number,
  }),
  lastAliveAt: Schema.Number,
  active: Schema.optional(Schema.Boolean),
});

const PersistedRegistryState = Schema.Struct({
  version: Schema.Literal(1),
  records: Schema.Array(PersistedCredentialRecord),
});

type PersistedRegistryState = typeof PersistedRegistryState.Type;

const decodePersistedRegistryState = Schema.decodeUnknownEffect(
  Schema.fromJsonString(PersistedRegistryState),
);
const encodePersistedRegistryState = Schema.encodeEffect(
  Schema.fromJsonString(PersistedRegistryState),
);
export interface McpSessionRegistryOptions {
  readonly livenessWindowMs?: number;
  readonly persistenceHeartbeatMs?: number;
  readonly persistenceLockRetryDelayMs?: number;
  readonly now?: () => number;
}

/**
 * How long a credential outlives the last sign of life from its provider
 * session.
 *
 * Liveness is refreshed both by MCP traffic and by `touch` on every provider
 * turn, so a session that is still doing work never expires no matter how long
 * it goes between browser tool calls. This window therefore only bounds
 * credentials whose session died without a clean stop — explicit thread and
 * provider-session stop paths revoke eagerly and do not wait for it.
 *
 * The bound matters because `/mcp` is mounted outside the environment auth
 * stack and is reachable on whatever host the server binds to, so this token is
 * the only thing guarding the preview toolkit on a remote-reachable server.
 */
const DEFAULT_LIVENESS_WINDOW_MS = 24 * 60 * 60 * 1_000;
const DEFAULT_PERSISTENCE_HEARTBEAT_MS = 5 * 60 * 1_000;
const PERSISTENCE_LOCK_RETRY_DELAY_MS = 25;

const bytesToHex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

const tokenFromBytes = (bytes: Uint8Array): string => Buffer.from(bytes).toString("base64url");

const getHttpMcpEndpointHost = (hostname: string): string => {
  const normalized = hostname.toLowerCase();
  const endpointHostname =
    normalized === "0.0.0.0" || normalized === "::" || normalized === "[::]"
      ? "127.0.0.1"
      : hostname;
  return endpointHostname.includes(":") && !endpointHostname.startsWith("[")
    ? `[${endpointHostname}]`
    : endpointHostname;
};

const makeWithOptions = Effect.fn("McpSessionRegistry.make")(function* (
  options: McpSessionRegistryOptions = {},
) {
  const crypto = yield* Crypto.Crypto;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const environmentId = yield* environment.getEnvironmentId;
  const httpServer = yield* HttpServer.HttpServer;
  const currentTimeMillis = options.now ? Effect.sync(options.now) : Clock.currentTimeMillis;
  const livenessWindowMs = options.livenessWindowMs ?? DEFAULT_LIVENESS_WINDOW_MS;
  const persistenceHeartbeatMs =
    options.persistenceHeartbeatMs ??
    Math.min(DEFAULT_PERSISTENCE_HEARTBEAT_MS, Math.max(1, Math.floor(livenessWindowMs / 4)));
  const persistenceLockRetryDelayMs =
    options.persistenceLockRetryDelayMs ?? PERSISTENCE_LOCK_RETRY_DELAY_MS;
  const persistedStatePath = path.join(serverConfig.stateDir, PERSISTED_REGISTRY_FILE_NAME);
  const persistenceLockPath = `${persistedStatePath}.lock.sqlite`;
  const endpoint =
    httpServer.address._tag === "TcpAddress"
      ? `http://${getHttpMcpEndpointHost(httpServer.address.hostname)}:${httpServer.address.port}/mcp`
      : "http://127.0.0.1/mcp";

  const hashToken = (token: string) =>
    crypto
      .digest("SHA-256", new TextEncoder().encode(token))
      .pipe(Effect.map(bytesToHex), Effect.orDie);

  const pruneDead = (records: ReadonlyMap<string, CredentialRecord>, timestamp: number) => {
    const next = new Map(
      Array.from(records).filter(
        ([, record]) => timestamp - record.lastAliveAt <= livenessWindowMs,
      ),
    );
    return next.size === records.size ? records : next;
  };

  const writePersistedRecords = Effect.fn("McpSessionRegistry.writePersistedRecords")(function* (
    records: ReadonlyMap<string, CredentialRecord>,
  ) {
    const persisted: PersistedRegistryState = {
      version: 1,
      // Preserve issuance order as the durable tie-breaker when two
      // credentials are minted in the same millisecond. Sorting by token hash
      // lets an older registry generation mistake its stale token for newest.
      records: Array.from(records.values()).map((record) => ({
        tokenHash: record.tokenHash,
        scope: {
          environmentId: record.scope.environmentId,
          threadId: record.scope.threadId,
          providerSessionId: record.scope.providerSessionId,
          providerInstanceId: record.scope.providerInstanceId,
          capabilities: Array.from(record.scope.capabilities).toSorted(),
          issuedAt: record.scope.issuedAt,
        },
        lastAliveAt: record.lastAliveAt,
        active: record.active,
      })),
    };
    const encoded = yield* encodePersistedRegistryState(persisted);
    const temporarySuffix = yield* crypto.randomUUIDv4;
    const temporaryPath = `${persistedStatePath}.${temporarySuffix}.tmp`;
    const contents = new TextEncoder().encode(`${encoded}\n`);

    yield* Effect.scoped(
      Effect.gen(function* () {
        const file = yield* fileSystem.open(temporaryPath, {
          flag: "wx",
          mode: 0o600,
        });
        yield* file.writeAll(contents);
        yield* file.sync;
      }),
    ).pipe(
      Effect.andThen(fileSystem.chmod(temporaryPath, 0o600)),
      Effect.andThen(fileSystem.rename(temporaryPath, persistedStatePath)),
      Effect.andThen(fileSystem.chmod(persistedStatePath, 0o600)),
      Effect.catch((cause) =>
        fileSystem
          .remove(temporaryPath, { force: true })
          .pipe(Effect.ignore, Effect.andThen(Effect.fail(cause))),
      ),
    );
  }, Effect.orDie);

  const acquirePersistenceLock = () =>
    Effect.promise((signal) =>
      acquireSqliteTransactionLock(persistenceLockPath, {
        retryDelayMs: persistenceLockRetryDelayMs,
        signal,
      }),
    ).pipe(Effect.orDie);

  const withPersistenceLock = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.acquireUseRelease(
      acquirePersistenceLock(),
      () => effect,
      (lock) => Effect.promise(() => lock.release()).pipe(Effect.orDie),
    );

  const readPersistedRecords = Effect.fn("McpSessionRegistry.readPersistedRecords")(function* () {
    const raw = yield* fileSystem
      .readFileString(persistedStatePath)
      .pipe(
        Effect.catch((cause) =>
          cause.reason._tag === "NotFound" ? Effect.void : Effect.fail(cause),
        ),
      );
    if (raw === undefined || raw.trim().length === 0) {
      return new Map<string, CredentialRecord>();
    }

    const persisted = yield* decodePersistedRegistryState(raw);

    return new Map(
      persisted.records
        .filter((record) => record.scope.environmentId === environmentId)
        .map((record) => [
          record.tokenHash,
          {
            tokenHash: record.tokenHash,
            scope: {
              environmentId: record.scope.environmentId,
              threadId: record.scope.threadId,
              providerSessionId: record.scope.providerSessionId,
              providerInstanceId: record.scope.providerInstanceId,
              capabilities: new Set(record.scope.capabilities),
              issuedAt: record.scope.issuedAt,
            },
            lastAliveAt: record.lastAliveAt,
            // Older version 1 records did not persist activation state. Treat
            // them as active so upgrades preserve credentials that may already
            // be installed in detached provider sessions.
            active: record.active ?? true,
          },
        ]),
    );
  });

  const updatePersistedRecords = Effect.fn("McpSessionRegistry.updatePersistedRecords")(function* (
    timestamp: number,
    update: (
      records: ReadonlyMap<string, CredentialRecord>,
    ) => ReadonlyMap<string, CredentialRecord>,
    forceWrite = false,
  ) {
    return yield* withPersistenceLock(
      Effect.gen(function* () {
        const persisted = yield* readPersistedRecords();
        const current = pruneDead(persisted, timestamp);
        const next = update(current);
        if (forceWrite || next !== persisted) {
          yield* writePersistedRecords(next);
        }
        return next;
      }),
    );
  });

  yield* fileSystem.makeDirectory(serverConfig.stateDir, { recursive: true }).pipe(Effect.orDie);
  const loadedAt = yield* currentTimeMillis;
  // Rewriting on startup prunes expired and foreign-environment records and
  // repairs permissions from older versions before accepting requests.
  const initialRecords = yield* updatePersistedRecords(loadedAt, (records) => records, true);
  const state = yield* SynchronizedRef.make<RegistryState>({ records: initialRecords });

  const issue: McpSessionRegistryShape["issue"] = Effect.fn("McpSessionRegistry.issue")(
    function* (request) {
      const issuedAt = yield* currentTimeMillis;
      const providerSessionId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
      const rawToken = yield* crypto.randomBytes(32).pipe(Effect.map(tokenFromBytes), Effect.orDie);
      const tokenHash = yield* hashToken(rawToken);
      const scope: McpInvocationContext.McpInvocationScope = {
        environmentId,
        threadId: ThreadId.make(request.threadId),
        providerSessionId,
        providerInstanceId: ProviderInstanceId.make(request.providerInstanceId),
        capabilities: new Set(["preview"]),
        issuedAt,
      };
      yield* SynchronizedRef.updateEffect(state, () =>
        updatePersistedRecords(issuedAt, (records) => {
          const next = new Map(records);
          next.set(tokenHash, { tokenHash, scope, lastAliveAt: issuedAt, active: false });
          return next;
        }).pipe(Effect.map((records) => ({ records }))),
      ).pipe(Effect.orDie);
      return {
        config: {
          environmentId,
          threadId: scope.threadId,
          providerSessionId,
          providerInstanceId: scope.providerInstanceId,
          endpoint,
          authorizationHeader: `Bearer ${rawToken}`,
        },
      };
    },
  );

  const activateProviderSession: McpSessionRegistryShape["activateProviderSession"] = Effect.fn(
    "McpSessionRegistry.activateProviderSession",
  )(function* (providerSessionId) {
    const timestamp = yield* currentTimeMillis;
    yield* SynchronizedRef.updateEffect(state, () =>
      updatePersistedRecords(timestamp, (records) => {
        const target = Array.from(records.values()).find(
          (record) => record.scope.providerSessionId === providerSessionId,
        );
        if (!target) return records;

        const next = new Map(
          Array.from(records).filter(
            ([, record]) =>
              record.scope.threadId !== target.scope.threadId ||
              record.tokenHash === target.tokenHash,
          ),
        );
        next.set(target.tokenHash, {
          ...target,
          active: true,
          lastAliveAt: timestamp,
        });
        return next;
      }).pipe(Effect.map((records) => ({ records }))),
    ).pipe(Effect.orDie);
  });

  const resolve: McpSessionRegistryShape["resolve"] = Effect.fn("McpSessionRegistry.resolve")(
    function* (rawToken) {
      if (rawToken.length === 0) return undefined;
      const tokenHash = yield* hashToken(rawToken);
      const timestamp = yield* currentTimeMillis;
      return yield* SynchronizedRef.modifyEffect(state, ({ records }) => {
        const current = pruneDead(records, timestamp);
        const record = current.get(tokenHash);
        if (!record || (record.active && timestamp - record.lastAliveAt < persistenceHeartbeatMs)) {
          return Effect.succeed([record?.scope, { records: current }] as const);
        }
        return updatePersistedRecords(timestamp, (persisted) => {
          const persistedRecord = persisted.get(tokenHash);
          if (!persistedRecord) return persisted;
          const heartbeatDue = timestamp - persistedRecord.lastAliveAt >= persistenceHeartbeatMs;
          if (persistedRecord.active && !heartbeatDue) return persisted;

          const next = persistedRecord.active
            ? new Map(persisted)
            : new Map(
                Array.from(persisted).filter(
                  ([, candidate]) =>
                    candidate.scope.threadId !== persistedRecord.scope.threadId ||
                    candidate.tokenHash === persistedRecord.tokenHash,
                ),
              );
          next.set(tokenHash, {
            ...persistedRecord,
            active: true,
            ...(heartbeatDue || !persistedRecord.active ? { lastAliveAt: timestamp } : {}),
          });
          return next;
        }).pipe(Effect.map((next) => [next.get(tokenHash)?.scope, { records: next }] as const));
      }).pipe(Effect.orDie);
    },
  );

  const touch: McpSessionRegistryShape["touch"] = Effect.fn("McpSessionRegistry.touch")(
    function* (threadId) {
      const timestamp = yield* currentTimeMillis;
      yield* SynchronizedRef.updateEffect(state, ({ records }) => {
        const current = pruneDead(records, timestamp);
        const threadRecords = Array.from(current.values()).filter(
          (record) => record.scope.threadId === threadId,
        );
        const activeThreadRecords = threadRecords.filter((record) => record.active);
        const latest = activeThreadRecords.reduce<CredentialRecord | undefined>(
          (selected, record) =>
            !selected || record.scope.issuedAt >= selected.scope.issuedAt ? record : selected,
          undefined,
        );
        const heartbeatDue =
          latest !== undefined && timestamp - latest.lastAliveAt >= persistenceHeartbeatMs;
        if (!heartbeatDue && activeThreadRecords.length <= 1) {
          return Effect.succeed({ records: current });
        }

        return updatePersistedRecords(timestamp, (persisted) => {
          const persistedLatest = Array.from(persisted.values())
            .filter((record) => record.scope.threadId === threadId && record.active)
            .reduce<CredentialRecord | undefined>(
              (selected, record) =>
                !selected || record.scope.issuedAt >= selected.scope.issuedAt ? record : selected,
              undefined,
            );
          if (!persistedLatest) return persisted;

          const updated = new Map(
            Array.from(persisted).filter(
              ([, record]) =>
                record.scope.threadId !== threadId ||
                !record.active ||
                record.tokenHash === persistedLatest.tokenHash,
            ),
          );
          if (timestamp - persistedLatest.lastAliveAt >= persistenceHeartbeatMs) {
            updated.set(persistedLatest.tokenHash, {
              ...persistedLatest,
              lastAliveAt: timestamp,
            });
          }
          return updated;
        }).pipe(Effect.map((records) => ({ records })));
      }).pipe(Effect.orDie);
    },
  );

  const revokeWhere = Effect.fn("McpSessionRegistry.revokeWhere")(function* (
    predicate: (record: CredentialRecord) => boolean,
  ) {
    const timestamp = yield* currentTimeMillis;
    yield* SynchronizedRef.updateEffect(state, () =>
      updatePersistedRecords(timestamp, (records) => {
        const next = new Map(Array.from(records).filter(([, record]) => !predicate(record)));
        return next.size === records.size ? records : next;
      }).pipe(Effect.map((records) => ({ records }))),
    ).pipe(Effect.orDie);
  });

  const revokeAll = Effect.gen(function* () {
    const timestamp = yield* currentTimeMillis;
    yield* SynchronizedRef.updateEffect(state, () =>
      updatePersistedRecords(timestamp, (records) =>
        records.size === 0 ? records : new Map<string, CredentialRecord>(),
      ).pipe(Effect.map((records) => ({ records }))),
    ).pipe(Effect.orDie);
  }).pipe(Effect.withSpan("McpSessionRegistry.revokeAll"));

  return McpSessionRegistry.of({
    issue,
    activateProviderSession,
    resolve,
    touch,
    revokeProviderSession: Effect.fn("McpSessionRegistry.revokeProviderSession")(
      function* (providerSessionId) {
        yield* revokeWhere((record) => record.scope.providerSessionId === providerSessionId);
      },
    ),
    revokeThread: Effect.fn("McpSessionRegistry.revokeThread")(function* (threadId) {
      yield* revokeWhere((record) => record.scope.threadId === threadId);
    }),
    revokeAll,
  });
});

let activeMcpSessionRegistry: McpSessionRegistryShape | undefined;

const make = Effect.acquireRelease(
  makeWithOptions().pipe(
    Effect.tap((registry) =>
      Effect.sync(() => {
        activeMcpSessionRegistry = registry;
      }),
    ),
  ),
  (registry) =>
    Effect.sync(() => {
      if (activeMcpSessionRegistry === registry) {
        activeMcpSessionRegistry = undefined;
      }
    }),
);

export const layer = Layer.effect(McpSessionRegistry, make);

export const issueActiveMcpCredential = (
  request: McpCredentialRequest,
): Effect.Effect<McpIssuedCredential | undefined> =>
  // A detached provider process may still use an older credential minted by a
  // previous T3 generation. The replacement is retired only after its provider
  // session shows life, or all credentials are revoked by explicit stop.
  activeMcpSessionRegistry
    ? activeMcpSessionRegistry.issue(request)
    : Effect.sync((): McpIssuedCredential | undefined => undefined);

/**
 * Refreshes the liveness of a thread's MCP credential. Called on every provider
 * turn so an active session is never mistaken for an abandoned one.
 */
export const touchActiveMcpThread = (threadId: ThreadId): Effect.Effect<void> =>
  activeMcpSessionRegistry ? activeMcpSessionRegistry.touch(threadId) : Effect.void;

export const activateActiveMcpProviderSession = (providerSessionId: string): Effect.Effect<void> =>
  activeMcpSessionRegistry
    ? activeMcpSessionRegistry.activateProviderSession(providerSessionId)
    : Effect.void;

export const revokeActiveMcpThread = (threadId: ThreadId): Effect.Effect<void> =>
  activeMcpSessionRegistry ? activeMcpSessionRegistry.revokeThread(threadId) : Effect.void;

export const revokeActiveMcpProviderSession = (providerSessionId: string): Effect.Effect<void> =>
  activeMcpSessionRegistry
    ? activeMcpSessionRegistry.revokeProviderSession(providerSessionId)
    : Effect.void;

/** Exposed for tests. */
export const __testing = {
  make: makeWithOptions,
  persistedRegistryFileName: PERSISTED_REGISTRY_FILE_NAME,
};
