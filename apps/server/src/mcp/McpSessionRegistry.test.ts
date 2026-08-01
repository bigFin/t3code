import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import { HttpServer } from "effect/unstable/http";

import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";

const environmentId = EnvironmentId.make("environment-1");
const makeFakeEnvironment = (id: EnvironmentId) =>
  ServerEnvironment.ServerEnvironment.of({
    getEnvironmentId: Effect.succeed(id),
    getDescriptor: Effect.die("unused"),
  });
const makeFakeHttpServer = (hostname: string, port = 43123) =>
  HttpServer.HttpServer.of({
    address: { _tag: "TcpAddress", hostname, port },
    serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
  });
const fakeHttpServer = makeFakeHttpServer("127.0.0.1");
const fakeEnvironment = makeFakeEnvironment(environmentId);
const PersistedRegistryStateForTest = Schema.Struct({
  version: Schema.Literal(1),
  records: Schema.Array(
    Schema.Struct({
      tokenHash: Schema.String,
      scope: Schema.Struct({
        environmentId: EnvironmentId,
        threadId: ThreadId,
      }),
      lastAliveAt: Schema.Number,
      active: Schema.optional(Schema.Boolean),
    }),
  ),
});
const decodePersistedRegistryStateForTest = Schema.decodeUnknownEffect(
  Schema.fromJsonString(PersistedRegistryStateForTest),
);

const makeRegistry = (
  now: () => number,
  baseDir: string,
  httpServer = fakeHttpServer,
  livenessWindowMs = 100,
  serverEnvironment = fakeEnvironment,
  persistenceHeartbeatMs?: number,
) =>
  McpSessionRegistry.__testing
    .make({
      now,
      livenessWindowMs,
      ...(persistenceHeartbeatMs !== undefined ? { persistenceHeartbeatMs } : {}),
      persistenceLockRetryDelayMs: 0,
    })
    .pipe(
      Effect.provideService(HttpServer.HttpServer, httpServer),
      Effect.provideService(ServerEnvironment.ServerEnvironment, serverEnvironment),
      Effect.provide(ServerConfig.layerTest(process.cwd(), baseDir)),
    );

const makeActiveRegistryLayer = (baseDir: string) =>
  McpSessionRegistry.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        NodeServices.layer,
        ServerConfig.layerTest(process.cwd(), baseDir).pipe(Layer.provide(NodeServices.layer)),
        Layer.succeed(HttpServer.HttpServer, fakeHttpServer),
        Layer.succeed(ServerEnvironment.ServerEnvironment, fakeEnvironment),
      ),
    ),
  );

it.effect("stores only a token hash, resolves the bearer token, and revokes by thread", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-test-",
    });
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp, baseDir);
    const threadId = ThreadId.make("thread-1");
    const issued = yield* registry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    expect(issued.config.endpoint).toBe("http://127.0.0.1:43123/mcp");
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");
    expect(token.length).toBeGreaterThan(20);
    const persistedPath = path.join(
      baseDir,
      "userdata",
      McpSessionRegistry.__testing.persistedRegistryFileName,
    );
    const persisted = yield* fileSystem.readFileString(persistedPath);
    const persistedInfo = yield* fileSystem.stat(persistedPath);
    expect(persisted).not.toContain(token);
    expect(persistedInfo.mode & 0o777).toBe(0o600);
    expect(yield* decodePersistedRegistryStateForTest(persisted)).toMatchObject({
      version: 1,
      records: [{ tokenHash: expect.stringMatching(/^[a-f0-9]{64}$/) }],
    });

    const resolved = yield* registry.resolve(token);
    expect(resolved?.threadId).toBe(threadId);

    yield* registry.revokeThread(threadId);
    expect(yield* registry.resolve(token)).toBeUndefined();

    timestamp += 2_000;
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("builds MCP endpoints from the bound server host", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-endpoint-test-",
    });
    const cases = [
      ["100.64.0.40", "http://100.64.0.40:43123/mcp"],
      ["0.0.0.0", "http://127.0.0.1:43123/mcp"],
      ["localhost", "http://localhost:43123/mcp"],
      ["127.0.0.1", "http://127.0.0.1:43123/mcp"],
    ] as const;

    for (const [hostname, expectedEndpoint] of cases) {
      const registry = yield* makeRegistry(() => 1_000, baseDir, makeFakeHttpServer(hostname));
      const issued = yield* registry.issue({
        threadId: ThreadId.make(`thread-${hostname}`),
        providerInstanceId: ProviderInstanceId.make("codex"),
      });
      expect(issued.config.endpoint).toBe(expectedEndpoint);
    }
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("expires credentials once their session stops showing signs of life", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-expiry-test-",
    });
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp, baseDir);
    const issued = yield* registry.issue({
      threadId: ThreadId.make("thread-2"),
      providerInstanceId: ProviderInstanceId.make("claude"),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");
    timestamp += 101;
    expect(yield* registry.resolve(token)).toBeUndefined();
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("keeps a credential alive across turns that never touch an MCP tool", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-liveness-test-",
    });
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp, baseDir);
    const threadId = ThreadId.make("thread-3");
    const issued = yield* registry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("claude"),
    });
    yield* registry.activateProviderSession(issued.config.providerSessionId);
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");

    // Well past the liveness window in total, but each turn reports in before
    // it lapses — this is the long-session case that used to lose the toolkit.
    for (let turn = 0; turn < 10; turn += 1) {
      timestamp += 99;
      yield* registry.touch(threadId);
    }

    expect((yield* registry.resolve(token))?.threadId).toBe(threadId);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("does not keep credentials of other threads alive", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-isolation-test-",
    });
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp, baseDir);
    const issued = yield* registry.issue({
      threadId: ThreadId.make("thread-4"),
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");

    timestamp += 99;
    yield* registry.touch(ThreadId.make("thread-unrelated"));
    timestamp += 2;

    expect(yield* registry.resolve(token)).toBeUndefined();
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("resolves a credential after registry teardown and recreation", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-restart-test-",
    });
    let timestamp = 1_000;
    const firstRegistry = yield* makeRegistry(() => timestamp, baseDir);
    const threadId = ThreadId.make("thread-restart");
    const issued = yield* firstRegistry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");

    timestamp += 50;
    const restartedRegistry = yield* makeRegistry(() => timestamp, baseDir);

    expect((yield* restartedRegistry.resolve(token))?.threadId).toBe(threadId);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("retires superseded credentials after the replacement session shows life", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-active-restart-test-",
    });
    const threadId = ThreadId.make("thread-active-restart");
    const issued = yield* Effect.scoped(
      Effect.gen(function* () {
        const first = yield* McpSessionRegistry.issueActiveMcpCredential({
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
        });
        const afterFirst = yield* fileSystem.readFileString(
          `${baseDir}/userdata/${McpSessionRegistry.__testing.persistedRegistryFileName}`,
        );
        const second = yield* McpSessionRegistry.issueActiveMcpCredential({
          threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
        });
        const afterSecond = yield* fileSystem.readFileString(
          `${baseDir}/userdata/${McpSessionRegistry.__testing.persistedRegistryFileName}`,
        );
        if (second) {
          yield* McpSessionRegistry.activateActiveMcpProviderSession(
            second.config.providerSessionId,
          );
        }
        const afterTouch = yield* fileSystem.readFileString(
          `${baseDir}/userdata/${McpSessionRegistry.__testing.persistedRegistryFileName}`,
        );
        return { first, second, afterFirst, afterSecond, afterTouch };
      }).pipe(Effect.provide(makeActiveRegistryLayer(baseDir))),
    );
    const afterTeardown = yield* fileSystem.readFileString(
      `${baseDir}/userdata/${McpSessionRegistry.__testing.persistedRegistryFileName}`,
    );
    const firstToken = issued.first?.config.authorizationHeader.replace(/^Bearer\s+/, "");
    const secondToken = issued.second?.config.authorizationHeader.replace(/^Bearer\s+/, "");
    expect(firstToken).toBeDefined();
    expect(secondToken).toBeDefined();
    expect((yield* decodePersistedRegistryStateForTest(issued.afterFirst)).records).toHaveLength(1);
    expect((yield* decodePersistedRegistryStateForTest(issued.afterSecond)).records).toHaveLength(
      2,
    );
    expect((yield* decodePersistedRegistryStateForTest(issued.afterTouch)).records).toHaveLength(1);
    expect((yield* decodePersistedRegistryStateForTest(afterTeardown)).records).toHaveLength(1);
    const persistedBeforeRestart = yield* decodePersistedRegistryStateForTest(
      yield* fileSystem.readFileString(
        `${baseDir}/userdata/${McpSessionRegistry.__testing.persistedRegistryFileName}`,
      ),
    );
    expect(persistedBeforeRestart.records).toHaveLength(1);
    expect(persistedBeforeRestart.records[0]?.scope).toMatchObject({
      environmentId,
      threadId,
    });
    const restartedAt =
      Math.max(...persistedBeforeRestart.records.map((record) => record.lastAliveAt)) + 50;

    const restartedRegistry = yield* makeRegistry(
      () => restartedAt,
      baseDir,
      fakeHttpServer,
      24 * 60 * 60 * 1_000,
    );
    expect(yield* restartedRegistry.resolve(firstToken ?? "")).toBeUndefined();
    expect((yield* restartedRegistry.resolve(secondToken ?? ""))?.threadId).toBe(threadId);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("does not let a stale registry generation retire a newer credential", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-stale-generation-test-",
    });
    const threadId = ThreadId.make("thread-stale-generation");
    let timestamp = 1_000;
    const staleRegistry = yield* makeRegistry(
      () => timestamp,
      baseDir,
      fakeHttpServer,
      1_000,
      fakeEnvironment,
      1,
    );
    const staleIssued = yield* staleRegistry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    const replacementRegistry = yield* makeRegistry(
      () => timestamp,
      baseDir,
      fakeHttpServer,
      1_000,
      fakeEnvironment,
      1,
    );
    const replacementIssued = yield* replacementRegistry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    const staleToken = staleIssued.config.authorizationHeader.replace(/^Bearer\s+/, "");
    const replacementToken = replacementIssued.config.authorizationHeader.replace(/^Bearer\s+/, "");

    yield* replacementRegistry.activateProviderSession(replacementIssued.config.providerSessionId);
    timestamp += 2;
    yield* staleRegistry.touch(threadId);

    const restartedRegistry = yield* makeRegistry(() => timestamp, baseDir);
    expect(yield* restartedRegistry.resolve(staleToken)).toBeUndefined();
    expect((yield* restartedRegistry.resolve(replacementToken))?.threadId).toBe(threadId);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("keeps the installed credential when a replacement was issued but never activated", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-unactivated-replacement-test-",
    });
    const threadId = ThreadId.make("thread-unactivated-replacement");
    let timestamp = 1_000;
    const registry = yield* makeRegistry(
      () => timestamp,
      baseDir,
      fakeHttpServer,
      1_000,
      fakeEnvironment,
      1,
    );
    const installed = yield* registry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    yield* registry.activateProviderSession(installed.config.providerSessionId);
    const interruptedReplacement = yield* registry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    const installedToken = installed.config.authorizationHeader.replace(/^Bearer\s+/, "");
    const replacementToken = interruptedReplacement.config.authorizationHeader.replace(
      /^Bearer\s+/,
      "",
    );

    timestamp += 2;
    yield* registry.touch(threadId);

    const persistedPath = path.join(
      baseDir,
      "userdata",
      McpSessionRegistry.__testing.persistedRegistryFileName,
    );
    const beforeActivation = yield* decodePersistedRegistryStateForTest(
      yield* fileSystem.readFileString(persistedPath),
    );
    expect(beforeActivation.records).toHaveLength(2);
    expect(beforeActivation.records.map((record) => record.active).toSorted()).toEqual([
      false,
      true,
    ]);
    expect(
      beforeActivation.records.find(
        (record) => record.scope.threadId === interruptedReplacement.config.threadId,
      ),
    ).toBeDefined();

    const restartedRegistry = yield* makeRegistry(
      () => timestamp,
      baseDir,
      fakeHttpServer,
      1_000,
      fakeEnvironment,
      1,
    );
    expect((yield* restartedRegistry.resolve(installedToken))?.threadId).toBe(threadId);
    expect((yield* restartedRegistry.resolve(replacementToken))?.threadId).toBe(threadId);
    expect(yield* restartedRegistry.resolve(installedToken)).toBeUndefined();
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("skips persistent I/O for an inactive replacement before the heartbeat is due", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-inactive-replacement-heartbeat-test-",
    });
    const persistedPath = path.join(
      baseDir,
      "userdata",
      McpSessionRegistry.__testing.persistedRegistryFileName,
    );
    let timestamp = 1_000;
    let rejectPersistentReads = false;
    const readFailure = PlatformError.systemError({
      _tag: "PermissionDenied",
      module: "FileSystem",
      method: "readFileString",
      pathOrDescriptor: persistedPath,
      description: "Persistent registry reads are disabled for this assertion.",
    });
    const guardedFileSystem = FileSystem.FileSystem.of({
      ...fileSystem,
      readFileString: (target, options) =>
        rejectPersistentReads && String(target) === persistedPath
          ? Effect.fail(readFailure)
          : fileSystem.readFileString(target, options),
    });
    const registry = yield* makeRegistry(
      () => timestamp,
      baseDir,
      fakeHttpServer,
      1_000,
      fakeEnvironment,
      100,
    ).pipe(Effect.provideService(FileSystem.FileSystem, guardedFileSystem));
    const threadId = ThreadId.make("thread-inactive-replacement-heartbeat");
    const installed = yield* registry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    yield* registry.activateProviderSession(installed.config.providerSessionId);
    yield* registry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("codex"),
    });

    timestamp = 1_050;
    rejectPersistentReads = true;
    yield* registry.touch(threadId);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("filters persisted credentials to the current environment", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-environment-test-",
    });
    const foreignRegistry = yield* makeRegistry(
      () => 1_000,
      baseDir,
      fakeHttpServer,
      100,
      makeFakeEnvironment(EnvironmentId.make("environment-2")),
    );
    const issued = yield* foreignRegistry.issue({
      threadId: ThreadId.make("thread-foreign-environment"),
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");

    const currentRegistry = yield* makeRegistry(() => 1_050, baseDir);
    expect(yield* currentRegistry.resolve(token)).toBeUndefined();

    const persisted = yield* decodePersistedRegistryStateForTest(
      yield* fileSystem.readFileString(
        path.join(baseDir, "userdata", McpSessionRegistry.__testing.persistedRegistryFileName),
      ),
    );
    expect(persisted.records).toEqual([]);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("fails closed without overwriting invalid persistent registry state", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const invalidStates = ["not-json\n", '{"version":2,"records":[]}\n'];

    for (const [index, original] of invalidStates.entries()) {
      const baseDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: `t3-mcp-session-registry-invalid-${index}-`,
      });
      const persistedPath = path.join(
        baseDir,
        "userdata",
        McpSessionRegistry.__testing.persistedRegistryFileName,
      );
      yield* fileSystem.makeDirectory(path.dirname(persistedPath), { recursive: true });
      yield* fileSystem.writeFileString(persistedPath, original);

      const exit = yield* Effect.exit(makeRegistry(() => 1_000, baseDir));

      expect(Exit.isFailure(exit)).toBe(true);
      expect(yield* fileSystem.readFileString(persistedPath)).toBe(original);
    }
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("fails closed without overwriting unreadable persistent registry state", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-unreadable-",
    });
    const persistedPath = path.join(
      baseDir,
      "userdata",
      McpSessionRegistry.__testing.persistedRegistryFileName,
    );
    const original = '{"version":1,"records":[]}\n';
    yield* fileSystem.makeDirectory(path.dirname(persistedPath), { recursive: true });
    yield* fileSystem.writeFileString(persistedPath, original);

    const readFailure = PlatformError.systemError({
      _tag: "PermissionDenied",
      module: "FileSystem",
      method: "readFileString",
      pathOrDescriptor: persistedPath,
      description: "Persistent registry is unreadable.",
    });
    const failingFileSystem = FileSystem.FileSystem.of({
      ...fileSystem,
      readFileString: (target, options) =>
        String(target) === persistedPath
          ? Effect.fail(readFailure)
          : fileSystem.readFileString(target, options),
    });

    const exit = yield* Effect.exit(
      makeRegistry(() => 1_000, baseDir).pipe(
        Effect.provideService(FileSystem.FileSystem, failingFileSystem),
      ),
    );

    expect(Exit.isFailure(exit)).toBe(true);
    expect(yield* fileSystem.readFileString(persistedPath)).toBe(original);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("serializes concurrent credential persistence updates", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-concurrency-test-",
    });
    const registry = yield* makeRegistry(() => 1_000, baseDir);

    yield* Effect.forEach(
      Array.from({ length: 12 }, (_, index) => index),
      (index) =>
        registry.issue({
          threadId: ThreadId.make(`thread-concurrent-${index}`),
          providerInstanceId: ProviderInstanceId.make("codex"),
        }),
      { concurrency: "unbounded" },
    );

    const persisted = yield* decodePersistedRegistryStateForTest(
      yield* fileSystem.readFileString(
        path.join(baseDir, "userdata", McpSessionRegistry.__testing.persistedRegistryFileName),
      ),
    );
    expect(persisted.records).toHaveLength(12);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("merges concurrent credential issuance from independent registries", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-process-concurrency-test-",
    });
    const [firstRegistry, secondRegistry] = yield* Effect.all(
      [makeRegistry(() => 1_000, baseDir), makeRegistry(() => 1_000, baseDir)],
      { concurrency: "unbounded" },
    );

    yield* Effect.all(
      [
        firstRegistry.issue({
          threadId: ThreadId.make("thread-process-concurrent-1"),
          providerInstanceId: ProviderInstanceId.make("codex"),
        }),
        secondRegistry.issue({
          threadId: ThreadId.make("thread-process-concurrent-2"),
          providerInstanceId: ProviderInstanceId.make("codex"),
        }),
      ],
      { concurrency: "unbounded" },
    );

    const persisted = yield* decodePersistedRegistryStateForTest(
      yield* fileSystem.readFileString(
        path.join(baseDir, "userdata", McpSessionRegistry.__testing.persistedRegistryFileName),
      ),
    );
    expect(persisted.records.map((record) => record.scope.threadId).toSorted()).toEqual([
      "thread-process-concurrent-1",
      "thread-process-concurrent-2",
    ]);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("coalesces durable liveness heartbeats instead of writing on every resolve", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-heartbeat-test-",
    });
    let timestamp = 1_000;
    const registry = yield* makeRegistry(
      () => timestamp,
      baseDir,
      fakeHttpServer,
      1_000,
      fakeEnvironment,
      100,
    );
    const issued = yield* registry.issue({
      threadId: ThreadId.make("thread-heartbeat"),
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    yield* registry.activateProviderSession(issued.config.providerSessionId);
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");
    const persistedPath = path.join(
      baseDir,
      "userdata",
      McpSessionRegistry.__testing.persistedRegistryFileName,
    );

    timestamp = 1_050;
    expect((yield* registry.resolve(token))?.threadId).toBe(issued.config.threadId);
    const beforeHeartbeat = yield* decodePersistedRegistryStateForTest(
      yield* fileSystem.readFileString(persistedPath),
    );
    expect(beforeHeartbeat.records[0]?.lastAliveAt).toBe(1_000);

    timestamp = 1_100;
    expect((yield* registry.resolve(token))?.threadId).toBe(issued.config.threadId);
    const afterHeartbeat = yield* decodePersistedRegistryStateForTest(
      yield* fileSystem.readFileString(persistedPath),
    );
    expect(afterHeartbeat.records[0]?.lastAliveAt).toBe(1_100);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("persists thread and provider-session revocations across recreation", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-revocation-test-",
    });
    const registry = yield* makeRegistry(() => 1_000, baseDir);
    const threadCredential = yield* registry.issue({
      threadId: ThreadId.make("thread-revoked"),
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    const providerCredential = yield* registry.issue({
      threadId: ThreadId.make("provider-session-revoked"),
      providerInstanceId: ProviderInstanceId.make("claude"),
    });
    const threadToken = threadCredential.config.authorizationHeader.replace(/^Bearer\s+/, "");
    const providerToken = providerCredential.config.authorizationHeader.replace(/^Bearer\s+/, "");

    yield* registry.revokeThread(threadCredential.config.threadId);
    yield* registry.revokeProviderSession(providerCredential.config.providerSessionId);

    const restartedRegistry = yield* makeRegistry(() => 1_050, baseDir);
    expect(yield* restartedRegistry.resolve(threadToken)).toBeUndefined();
    expect(yield* restartedRegistry.resolve(providerToken)).toBeUndefined();
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("prunes expired credentials from persistent state during recreation", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-mcp-session-registry-prune-test-",
    });
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp, baseDir);
    const issued = yield* registry.issue({
      threadId: ThreadId.make("thread-expired-on-restart"),
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");

    timestamp += 101;
    const restartedRegistry = yield* makeRegistry(() => timestamp, baseDir);
    expect(yield* restartedRegistry.resolve(token)).toBeUndefined();

    const persisted = yield* decodePersistedRegistryStateForTest(
      yield* fileSystem.readFileString(
        path.join(baseDir, "userdata", McpSessionRegistry.__testing.persistedRegistryFileName),
      ),
    );
    expect(persisted.records).toEqual([]);
  }).pipe(Effect.provide(NodeServices.layer)),
);
