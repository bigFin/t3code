import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { writeFileStringAtomically } from "./atomicWrite.ts";
import type * as ServerConfig from "./config.ts";
import { acquireSqliteTransactionLock } from "./sqliteTransactionLock.ts";
import { formatHostForUrl, isWildcardHost } from "./startupAccess.ts";
import packageJson from "../package.json" with { type: "json" };

export const PersistedServerRuntimeState = Schema.Struct({
  version: Schema.Literal(1),
  pid: Schema.Int,
  host: Schema.optional(Schema.String),
  port: Schema.Int,
  origin: Schema.String,
  // Present when the server fronts a dev web server (VITE_DEV_SERVER_URL).
  // Dev is single-origin: browsers must pair through this URL, not `origin`.
  devUrl: Schema.optional(Schema.String),
  startedAt: Schema.String,
  serverVersion: Schema.optional(Schema.String),
  sshLaunch: Schema.optional(
    Schema.Struct({
      stateKey: Schema.String,
      runnerId: Schema.String,
    }),
  ),
});
export type PersistedServerRuntimeState = typeof PersistedServerRuntimeState.Type;

export class ServerRuntimeStateError extends Schema.TaggedErrorClass<ServerRuntimeStateError>()(
  "ServerRuntimeStateError",
  {
    operation: Schema.Literals(["persist", "read", "decode", "clear"]),
    statePath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to ${this.operation} server runtime state at ${this.statePath}.`;
  }
}

const PersistedServerRuntimeStateJson = Schema.fromJsonString(PersistedServerRuntimeState);
const decodePersistedServerRuntimeState = Schema.decodeUnknownEffect(
  PersistedServerRuntimeStateJson,
);
const encodePersistedServerRuntimeState = Schema.encodeEffect(PersistedServerRuntimeStateJson);

const runtimeOriginForConfig = (
  config: Pick<ServerConfig.ServerConfig["Service"], "host">,
  port: number,
): PersistedServerRuntimeState["origin"] => {
  const hostname =
    config.host && !isWildcardHost(config.host) ? formatHostForUrl(config.host) : "127.0.0.1";
  return `http://${hostname}:${port}`;
};

export const sshLaunchIdentityFromEnvironment = (
  environment: Readonly<Record<string, string | undefined>>,
): PersistedServerRuntimeState["sshLaunch"] => {
  const stateKey = environment.T3CODE_SSH_STATE_KEY?.trim() ?? "";
  const runnerId = environment.T3CODE_SSH_RUNNER_ID?.trim() ?? "";
  return stateKey && runnerId ? { stateKey, runnerId } : undefined;
};

export const makePersistedServerRuntimeState = (input: {
  readonly config: Pick<ServerConfig.ServerConfig["Service"], "host" | "devUrl">;
  readonly port: number;
}): Effect.Effect<PersistedServerRuntimeState> =>
  Effect.map(DateTime.now, (now) => {
    const sshLaunch = sshLaunchIdentityFromEnvironment(process.env);

    return {
      version: 1,
      pid: process.pid,
      ...(input.config.host ? { host: input.config.host } : {}),
      port: input.port,
      origin: runtimeOriginForConfig(input.config, input.port),
      ...(input.config.devUrl ? { devUrl: input.config.devUrl.toString() } : {}),
      startedAt: DateTime.formatIso(now),
      serverVersion: packageJson.version,
      ...(sshLaunch ? { sshLaunch } : {}),
    };
  });

const withServerRuntimeStateLock = <A, E, R>(input: {
  readonly path: string;
  readonly operation: ServerRuntimeStateError["operation"];
  readonly effect: Effect.Effect<A, E, R>;
}): Effect.Effect<A, E | ServerRuntimeStateError, R> =>
  Effect.acquireUseRelease(
    Effect.tryPromise({
      try: (signal) =>
        acquireSqliteTransactionLock(`${input.path}.lock.sqlite`, {
          signal,
        }),
      catch: (cause) =>
        new ServerRuntimeStateError({
          operation: input.operation,
          statePath: input.path,
          cause,
        }),
    }),
    () => input.effect,
    (lock) => Effect.promise(() => lock.release()).pipe(Effect.orDie),
  );

const removePersistedServerRuntimeState = (path: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    yield* fs.remove(path, { force: true }).pipe(
      Effect.mapError(
        (cause) =>
          new ServerRuntimeStateError({
            operation: "clear",
            statePath: path,
            cause,
          }),
      ),
    );
  });

export const persistServerRuntimeState = Effect.fn("persistServerRuntimeState")(function* (input: {
  readonly path: string;
  readonly state: PersistedServerRuntimeState;
}) {
  const encoded = yield* encodePersistedServerRuntimeState(input.state).pipe(
    Effect.mapError(
      (cause) =>
        new ServerRuntimeStateError({
          operation: "persist",
          statePath: input.path,
          cause,
        }),
    ),
  );
  yield* withServerRuntimeStateLock({
    path: input.path,
    operation: "persist",
    effect: writeFileStringAtomically({
      filePath: input.path,
      contents: `${encoded}\n`,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ServerRuntimeStateError({
            operation: "persist",
            statePath: input.path,
            cause,
          }),
      ),
    ),
  });
});

export const clearPersistedServerRuntimeState = (path: string) =>
  withServerRuntimeStateLock({
    path,
    operation: "clear",
    effect: removePersistedServerRuntimeState(path),
  }).pipe(
    Effect.catchTags({
      ServerRuntimeStateError: (error) =>
        Effect.logWarning(error.message).pipe(
          Effect.annotateLogs({
            operation: error.operation,
            statePath: error.statePath,
            cause: error,
          }),
        ),
    }),
  );

export const identifiesPersistedServerRuntimeOwner = (
  current: PersistedServerRuntimeState,
  expected: PersistedServerRuntimeState,
): boolean =>
  current.pid === expected.pid &&
  current.port === expected.port &&
  current.startedAt === expected.startedAt &&
  current.sshLaunch?.stateKey === expected.sshLaunch?.stateKey &&
  current.sshLaunch?.runnerId === expected.sshLaunch?.runnerId;

export const clearPersistedServerRuntimeStateIfOwned = Effect.fn(
  "clearPersistedServerRuntimeStateIfOwned",
)(function* (input: { readonly path: string; readonly state: PersistedServerRuntimeState }) {
  yield* withServerRuntimeStateLock({
    path: input.path,
    operation: "clear",
    effect: Effect.gen(function* () {
      const current = yield* readPersistedServerRuntimeState(input.path);
      if (
        Option.isSome(current) &&
        identifiesPersistedServerRuntimeOwner(current.value, input.state)
      ) {
        yield* removePersistedServerRuntimeState(input.path);
      }
    }),
  }).pipe(
    Effect.catchTags({
      ServerRuntimeStateError: (error) =>
        Effect.logWarning(error.message).pipe(
          Effect.annotateLogs({
            operation: error.operation,
            statePath: error.statePath,
            cause: error,
          }),
        ),
    }),
  );
});

export const readPersistedServerRuntimeState = (path: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const raw = yield* fs.readFileString(path).pipe(
      Effect.matchEffect({
        onFailure: (cause) =>
          cause.reason._tag === "NotFound"
            ? Effect.succeed(Option.none<string>())
            : Effect.fail(
                new ServerRuntimeStateError({
                  operation: "read",
                  statePath: path,
                  cause,
                }),
              ),
        onSuccess: (contents) => Effect.succeed(Option.some(contents)),
      }),
    );
    if (Option.isNone(raw)) {
      return Option.none<PersistedServerRuntimeState>();
    }

    const trimmed = raw.value.trim();
    if (trimmed.length === 0) {
      return Option.none<PersistedServerRuntimeState>();
    }

    return yield* decodePersistedServerRuntimeState(trimmed).pipe(
      Effect.map(Option.some),
      Effect.mapError(
        (cause) =>
          new ServerRuntimeStateError({
            operation: "decode",
            statePath: path,
            cause,
          }),
      ),
    );
  }).pipe(
    Effect.catchTags({
      ServerRuntimeStateError: (error) =>
        Effect.logWarning(error.message).pipe(
          Effect.annotateLogs({
            operation: error.operation,
            statePath: error.statePath,
            cause: error,
          }),
          Effect.as(Option.none<PersistedServerRuntimeState>()),
        ),
    }),
  );
