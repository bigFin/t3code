// @effect-diagnostics nodeBuiltinImport:off
import * as NodeAssert from "node:assert/strict";
import type * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodePerfHooks from "node:perf_hooks";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProviderInstanceId } from "@t3tools/contracts";
import {
  HostProcessArguments,
  HostProcessExecutablePath,
  HostProcessPlatform,
} from "@t3tools/shared/hostProcess";
import { it, vi } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { describe } from "vite-plus/test";

import {
  __testing,
  codexAppServerHostPaths,
  codexProviderHostConfigurationFingerprint,
  makeCodexAppServerHost,
  providerHostBuildFingerprint,
  type CodexAppServerHostOperations,
  type CodexAppServerHostOptions,
  type ProcessIdentityStatus,
} from "./CodexAppServerHost.ts";
import {
  CODEX_PROVIDER_HOST_CONFIG_VERSION,
  readCodexProviderHostConfig,
} from "../host/CodexProviderHostConfig.ts";
import {
  PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
  persistProviderHostManifest,
} from "../host/ProviderHostManifest.ts";
import {
  PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
  PROVIDER_HOST_PROTOCOL_VERSION,
  ProviderHostBuildFingerprint,
  ProviderHostGenerationFingerprint,
  ProviderHostHelloEnvelope,
  ProviderHostReplayCursor,
  ProviderHostV1HelloEnvelope,
} from "../host/ProviderHostProtocol.ts";

const TEST_EXECUTABLE_PATH = "/opt/t3/bin/node";
const TEST_ENTRY_PATH = "/opt/t3/dist/bin.mjs";

function makeDetachedChild(input?: {
  readonly exitCode?: number | null;
  readonly signalCode?: NodeJS.Signals | null;
  readonly kill?: (
    signal: NodeJS.Signals | number | undefined,
    child: NodeChildProcess.ChildProcess,
    emitter: NodeEvents.EventEmitter,
  ) => boolean;
}) {
  const emitter = new NodeEvents.EventEmitter();
  const child = Object.assign(emitter, {
    pid: 4242,
    exitCode: input?.exitCode ?? null,
    signalCode: input?.signalCode ?? null,
    kill: (_signal?: NodeJS.Signals | number) => true,
  }) as NodeChildProcess.ChildProcess;
  const kill = vi.fn(
    (signal?: NodeJS.Signals | number) => input?.kill?.(signal, child, emitter) ?? true,
  );
  child.kill = kill;
  return { child, emitter, kill };
}

function makeOptions(root: string, homePath = NodePath.join(root, "codex-home")) {
  return {
    binaryPath: process.execPath,
    launchArgs: "--strict-config --enable foo",
    homePath,
    environment: {
      HOME: NodeOS.homedir(),
      PATH: process.env.PATH,
      XDG_RUNTIME_DIR: NodePath.join(root, "runtime"),
    },
    cwd: root,
    stateDir: NodePath.join(root, "state"),
    providerLogsDir: NodePath.join(root, "logs"),
    providerInstanceId: ProviderInstanceId.make("codex"),
  } satisfies CodexAppServerHostOptions;
}

function makeOperations(input?: {
  readonly appServerProbeResults?: ReadonlyArray<boolean>;
  readonly probeResults?: ReadonlyArray<boolean>;
  readonly processIdentityMatches?: boolean;
  readonly processIdentityResults?: ReadonlyArray<boolean>;
  readonly processIdentityStatuses?: ReadonlyArray<"current" | "stale" | "unknown">;
  readonly socketPathExists?: boolean;
  readonly waitResult?: boolean;
}) {
  const appServerProbeResults = [...(input?.appServerProbeResults ?? [false])];
  const probeResults = [...(input?.probeResults ?? [false])];
  const processIdentityResults = [...(input?.processIdentityResults ?? [])];
  const processIdentityStatuses = [...(input?.processIdentityStatuses ?? [])];
  const terminatedPids: Array<number> = [];
  const terminate = vi.fn(() => Promise.resolve());
  const operations = {
    inspectProcessIdentity: vi.fn((): Promise<ProcessIdentityStatus> => {
      const status = processIdentityStatuses.shift();
      if (status) return Promise.resolve(status);
      return Promise.resolve(
        (processIdentityResults.shift() ?? input?.processIdentityMatches ?? true)
          ? "current"
          : "stale",
      );
    }),
    probeAppServer: vi.fn((_socketPath: string) =>
      Promise.resolve(appServerProbeResults.shift() ?? false),
    ),
    probeHost: vi.fn((_socketPath: string) => Promise.resolve(probeResults.shift() ?? false)),
    readBuildFingerprint: vi.fn(() =>
      Promise.resolve(ProviderHostBuildFingerprint.make("development")),
    ),
    removeSocket: vi.fn((_socketPath: string) => Promise.resolve()),
    sleep: vi.fn((_durationMs: number) => Promise.resolve()),
    socketPathExists: vi.fn((_socketPath: string) =>
      Promise.resolve(input?.socketPathExists ?? false),
    ),
    spawnDetached: vi.fn(
      (_command: Parameters<CodexAppServerHostOperations["spawnDetached"]>[0], _logPath: string) =>
        Promise.resolve({
          pid: 4242,
          exited: new Promise<void>(() => undefined),
          terminate: () => {
            terminatedPids.push(4242);
            return terminate();
          },
        }),
    ),
    waitForHost: vi.fn((_socketPath: string) => Promise.resolve(input?.waitResult ?? true)),
  } satisfies CodexAppServerHostOperations;
  return { operations, terminate, terminatedPids };
}

function spawnedConfigPath(operations: CodexAppServerHostOperations, spawnIndex = 0): string {
  const spawn = (operations.spawnDetached as ReturnType<typeof vi.fn>).mock.calls[spawnIndex] as
    | [Parameters<CodexAppServerHostOperations["spawnDetached"]>[0], string]
    | undefined;
  const configFlagIndex = spawn?.[0].args.indexOf("--config") ?? -1;
  const configPath = configFlagIndex >= 0 ? spawn?.[0].args[configFlagIndex + 1] : undefined;
  NodeAssert.equal(typeof configPath, "string");
  return configPath as string;
}

function writeLegacyManifest(input: {
  readonly path: string;
  readonly socketPath: string;
  readonly appServerSocketPath: string;
  readonly options: CodexAppServerHostOptions;
  readonly hostProcess: { readonly pid: number; readonly startTimeMs: number };
  readonly childProcess: { readonly pid: number; readonly startTimeMs: number };
}) {
  NodeFS.mkdirSync(NodePath.dirname(input.path), { recursive: true });
  NodeFS.writeFileSync(
    input.path,
    `${JSON.stringify({
      schemaVersion: 1,
      protocolVersion: 1,
      generationFingerprint: "legacy-generation",
      hostProcess: input.hostProcess,
      socketPath: input.socketPath,
      codex: {
        childProcess: input.childProcess,
        resolvedBinary: input.options.binaryPath,
        version: "codex-cli legacy",
        launchConfig: {
          arguments: ["app-server", "--listen", `unix://${input.appServerSocketPath}`],
          workingDirectory: input.options.cwd,
          environmentKeys: [],
        },
      },
      startedAt: "2026-08-01T00:00:00.000Z",
    })}\n`,
    { mode: 0o600 },
  );
}

const makeHost = (
  root: string,
  operations: CodexAppServerHostOperations,
  input?: {
    readonly options?: CodexAppServerHostOptions;
    readonly platform?: NodeJS.Platform;
    readonly executablePath?: string;
    readonly processArguments?: ReadonlyArray<string>;
  },
) =>
  makeCodexAppServerHost(input?.options ?? makeOptions(root), operations).pipe(
    Effect.provideService(HostProcessPlatform, input?.platform ?? "linux"),
    Effect.provideService(HostProcessExecutablePath, input?.executablePath ?? TEST_EXECUTABLE_PATH),
    Effect.provideService(
      HostProcessArguments,
      input?.processArguments ?? [TEST_EXECUTABLE_PATH, TEST_ENTRY_PATH, "serve"],
    ),
  );

describe("codexAppServerHostPaths", () => {
  it("keeps host addresses stable by identity and isolated by state and Codex home", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-path-"));
    try {
      const options = makeOptions(root);
      const first = codexAppServerHostPaths(options);
      const same = codexAppServerHostPaths(options);
      const changedGeneration = codexAppServerHostPaths({
        ...options,
        binaryPath: "/opt/codex-new/bin/codex",
        launchArgs: "--strict-config --enable new-feature",
      });
      const changedBuild = codexAppServerHostPaths(
        options,
        ProviderHostBuildFingerprint.make("different-build"),
      );
      const otherHome = codexAppServerHostPaths(
        makeOptions(root, NodePath.join(root, "other-codex-home")),
      );
      const otherState = codexAppServerHostPaths({
        ...options,
        stateDir: NodePath.join(root, "other-state"),
      });

      NodeAssert.deepStrictEqual(first, same);
      NodeAssert.equal(first.socketPath, changedGeneration.socketPath);
      NodeAssert.notEqual(first.socketPath, changedBuild.socketPath);
      NodeAssert.equal(first.appServerSocketPath, changedBuild.appServerSocketPath);
      NodeAssert.notEqual(first.socketPath, otherHome.socketPath);
      NodeAssert.notEqual(first.socketPath, otherState.socketPath);
      NodeAssert.notEqual(first.socketPath, first.appServerSocketPath);
      NodeAssert.equal(NodePath.dirname(first.configPath), NodePath.dirname(first.manifestPath));
      NodeAssert.equal(NodePath.dirname(first.lockPath), NodePath.dirname(first.manifestPath));
      NodeAssert.ok(first.configPath.startsWith(NodePath.join(options.stateDir, "provider-hosts")));
      NodeAssert.ok(first.socketPath.startsWith(NodePath.join(root, "runtime", "t3-code")));
      NodeAssert.ok(Buffer.byteLength(first.socketPath) < 100);
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("falls back to a short per-user runtime directory for long Unix socket roots", () => {
    const root = NodePath.join("/tmp", "x".repeat(180));
    const paths = codexAppServerHostPaths(makeOptions(root));

    NodeAssert.ok(
      paths.socketPath.startsWith(NodePath.join("/tmp", `t3-code-${process.getuid?.() ?? "user"}`)),
    );
    NodeAssert.ok(Buffer.byteLength(paths.socketPath) <= 96);
    NodeAssert.ok(Buffer.byteLength(paths.appServerSocketPath) <= 96);
  });

  it("keeps the app-server socket portable and the startup lock beside durable state", () => {
    const root = NodePath.join("/tmp", "x".repeat(41));
    const options = makeOptions(root);
    const paths = codexAppServerHostPaths(options);
    const preferredRuntimeDir = NodePath.join(options.environment.XDG_RUNTIME_DIR!, "t3-code");

    NodeAssert.ok(paths.socketPath.startsWith("/tmp/t3-code-"));
    NodeAssert.ok(paths.appServerSocketPath.startsWith(preferredRuntimeDir));
    NodeAssert.equal(NodePath.dirname(paths.lockPath), NodePath.dirname(paths.manifestPath));
    NodeAssert.ok(Buffer.byteLength(paths.appServerSocketPath) <= 96);
  });

  it("serializes one durable host identity across different runtime directories", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-lock-"));
    try {
      const options = makeOptions(root);
      const first = codexAppServerHostPaths(options);
      const second = codexAppServerHostPaths({
        ...options,
        environment: {
          ...options.environment,
          XDG_RUNTIME_DIR: NodePath.join(root, "other-runtime"),
        },
      });

      NodeAssert.notEqual(first.socketPath, second.socketPath);
      NodeAssert.notEqual(first.appServerSocketPath, second.appServerSocketPath);
      NodeAssert.equal(first.manifestPath, second.manifestPath);
      NodeAssert.equal(first.lockPath, second.lockPath);
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("providerHostBuildFingerprint", () => {
  it("changes source-entry fingerprints per dev process while keeping bundles content-based", async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-build-"));
    try {
      const sourceEntryPath = NodePath.join(root, "src", "bin.ts");
      const importedSourcePath = NodePath.join(root, "src", "provider-host.ts");
      const firstBundlePath = NodePath.join(root, "dist", "first.mjs");
      const sameBundlePath = NodePath.join(root, "dist", "same.mjs");
      const changedBundlePath = NodePath.join(root, "dist", "changed.mjs");
      NodeFS.mkdirSync(NodePath.dirname(sourceEntryPath), { recursive: true });
      NodeFS.mkdirSync(NodePath.dirname(firstBundlePath), { recursive: true });
      NodeFS.writeFileSync(sourceEntryPath, "import './provider-host.ts';\n");
      NodeFS.writeFileSync(importedSourcePath, "export const generation = 1;\n");
      NodeFS.writeFileSync(firstBundlePath, "console.log('bundle');\n");
      NodeFS.writeFileSync(sameBundlePath, "console.log('bundle');\n");
      NodeFS.writeFileSync(changedBundlePath, "console.log('changed bundle');\n");

      const firstDevelopment = await providerHostBuildFingerprint(
        sourceEntryPath,
        "development-process-a",
      );
      const sameDevelopment = await providerHostBuildFingerprint(
        sourceEntryPath,
        "development-process-a",
      );
      NodeFS.writeFileSync(importedSourcePath, "export const generation = 2;\n");
      const restartedDevelopment = await providerHostBuildFingerprint(
        sourceEntryPath,
        "development-process-b",
      );
      const firstBundle = await providerHostBuildFingerprint(
        firstBundlePath,
        "development-process-a",
      );
      const sameBundle = await providerHostBuildFingerprint(
        sameBundlePath,
        "development-process-b",
      );
      const changedBundle = await providerHostBuildFingerprint(
        changedBundlePath,
        "development-process-a",
      );

      NodeAssert.equal(firstDevelopment, sameDevelopment);
      NodeAssert.notEqual(firstDevelopment, restartedDevelopment);
      NodeAssert.equal(firstBundle, sameBundle);
      NodeAssert.notEqual(firstBundle, changedBundle);
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("makeCodexAppServerHost", () => {
  it("allows the child startup budget to include socket and version readiness", () => {
    NodeAssert.ok(__testing.hostStartTimeoutMs >= 15_000);
  });

  it("matches a process by pid and start generation rather than pid alone", async () => {
    const currentIdentity = {
      pid: process.pid,
      startTimeMs: Math.max(0, Math.floor(NodePerfHooks.performance.timeOrigin)),
    };

    NodeAssert.equal(await __testing.inspectProcessIdentity(currentIdentity), "current");
    NodeAssert.equal(
      await __testing.inspectProcessIdentity({
        ...currentIdentity,
        startTimeMs: 1_000,
      }),
      "stale",
    );
  });

  it("matches the complete provider-host build and provider identity", () => {
    const hello = ProviderHostHelloEnvelope.make({
      version: PROVIDER_HOST_PROTOCOL_VERSION,
      type: "hello",
      providerInstanceId: ProviderInstanceId.make("codex"),
      buildFingerprint: ProviderHostBuildFingerprint.make("build-a"),
      generationFingerprint: ProviderHostGenerationFingerprint.make("generation-a"),
      appServerMode: "spawn",
      canAdoptSessions: false,
      hostProcess: { pid: 123, startTimeMs: 1_000 },
      startedAt: DateTime.makeUnsafe("2026-08-01T00:00:00.000Z"),
      latestCursor: ProviderHostReplayCursor.make(0),
    });

    NodeAssert.equal(
      __testing.matchesProviderHostIdentity(hello, {
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        providerInstanceId: ProviderInstanceId.make("codex"),
        buildFingerprint: ProviderHostBuildFingerprint.make("build-a"),
      }),
      true,
    );
    NodeAssert.equal(
      __testing.matchesProviderHostIdentity(hello, {
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        providerInstanceId: ProviderInstanceId.make("codex-work"),
        buildFingerprint: ProviderHostBuildFingerprint.make("build-a"),
      }),
      false,
    );
    NodeAssert.equal(
      __testing.matchesProviderHostIdentity(hello, {
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        providerInstanceId: ProviderInstanceId.make("codex"),
        buildFingerprint: ProviderHostBuildFingerprint.make("build-b"),
      }),
      false,
    );
  });

  it("matches a legacy provider host by provider, generation, and process identity", () => {
    const hello = ProviderHostV1HelloEnvelope.make({
      version: PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
      type: "hello",
      providerInstanceId: ProviderInstanceId.make("codex"),
      generationFingerprint: ProviderHostGenerationFingerprint.make("generation-legacy"),
      hostProcess: { pid: 123, startTimeMs: 1_000 },
      startedAt: DateTime.makeUnsafe("2026-08-01T00:00:00.000Z"),
      latestCursor: ProviderHostReplayCursor.make(0),
    });
    const expected = {
      version: PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
      providerInstanceId: ProviderInstanceId.make("codex"),
      generationFingerprint: ProviderHostGenerationFingerprint.make("generation-legacy"),
      hostProcess: { pid: 123, startTimeMs: 1_000 },
    } as const;

    NodeAssert.equal(__testing.matchesProviderHostIdentity(hello, expected), true);
    NodeAssert.equal(
      __testing.matchesProviderHostIdentity(hello, {
        ...expected,
        generationFingerprint: ProviderHostGenerationFingerprint.make("generation-other"),
      }),
      false,
    );
    NodeAssert.equal(
      __testing.matchesProviderHostIdentity(hello, {
        ...expected,
        hostProcess: { pid: 123, startTimeMs: 2_000 },
      }),
      false,
    );
  });

  it("terminates through the captured child and clears a synchronous exit timer", async () => {
    vi.useFakeTimers();
    try {
      const detached = makeDetachedChild({
        kill: (signal, child, emitter) => {
          if (signal === "SIGTERM") {
            Object.assign(child, { signalCode: "SIGTERM" });
            emitter.emit("exit", null, "SIGTERM");
          }
          return true;
        },
      });

      await __testing.terminateDetachedProcess(detached.child);

      NodeAssert.deepStrictEqual(detached.kill.mock.calls, [["SIGTERM"]]);
      NodeAssert.equal(vi.getTimerCount(), 0);
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not signal a captured child that already exited", async () => {
    const { child, kill } = makeDetachedChild({ exitCode: 0 });

    await __testing.terminateDetachedProcess(child);

    NodeAssert.equal(kill.mock.calls.length, 0);
  });

  it("resolves safely when signaling the captured child fails", async () => {
    vi.useFakeTimers();
    try {
      const { child, kill } = makeDetachedChild({
        kill: () => {
          throw new Error("already exited");
        },
      });

      await __testing.terminateDetachedProcess(child);

      NodeAssert.equal(kill.mock.calls.length, 1);
      NodeAssert.equal(vi.getTimerCount(), 0);
    } finally {
      vi.useRealTimers();
    }
  });

  it.effect("starts one detached provider host through the hidden command and reuses it", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const { operations, terminate } = makeOperations({
        probeResults: [false, false, true],
      });
      try {
        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host);
        const first = yield* host.ensure;
        const second = yield* host.ensure;

        NodeAssert.equal(typeof first, "string");
        NodeAssert.notEqual(first, paths.socketPath);
        NodeAssert.equal(second, first);
        NodeAssert.equal(host.socketPath, paths.socketPath);
        NodeAssert.equal(host.appServerSocketPath, paths.appServerSocketPath);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
        NodeAssert.equal(terminate.mock.calls.length, 0);

        const spawn = operations.spawnDetached.mock.calls[0];
        const configPath = spawnedConfigPath(operations);
        NodeAssert.equal(spawn?.[0].command, TEST_EXECUTABLE_PATH);
        NodeAssert.deepStrictEqual(spawn?.[0].args, [
          TEST_ENTRY_PATH,
          "__provider-host",
          "--config",
          configPath,
        ]);
        NodeAssert.equal(spawn?.[0].cwd, root);
        NodeAssert.equal(spawn?.[0].env.CODEX_HOME, options.homePath);
        NodeAssert.equal(spawn?.[1], paths.logPath);

        const config = yield* readCodexProviderHostConfig(configPath);
        const configInfo = NodeFS.statSync(configPath);
        NodeAssert.equal(configInfo.mode & 0o777, 0o600);
        NodeAssert.notEqual(config.appServerSocketPath, paths.appServerSocketPath);
        NodeAssert.equal(
          NodePath.dirname(config.appServerSocketPath),
          NodePath.dirname(paths.appServerSocketPath),
        );
        NodeAssert.deepStrictEqual(config, {
          version: CODEX_PROVIDER_HOST_CONFIG_VERSION,
          providerInstanceId: options.providerInstanceId,
          buildFingerprint: ProviderHostBuildFingerprint.make("development"),
          configurationFingerprint: codexProviderHostConfigurationFingerprint(options),
          controlSocketPath: first,
          appServerSocketPath: config.appServerSocketPath,
          startupLockPath: paths.lockPath,
          appServerMode: "spawn",
          manifestPath: paths.manifestPath,
          codex: {
            binaryPath: options.binaryPath,
            launchArgs: options.launchArgs,
            homePath: options.homePath,
            cwd: root,
          },
        });
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("attaches to an existing healthy host without changing its lifecycle", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const { operations, terminate } = makeOperations({ probeResults: [true] });
      try {
        const host = yield* makeHost(root, operations);
        NodeAssert.ok(host);

        NodeAssert.equal(yield* host.ensure, host.socketPath);
        NodeAssert.equal(operations.probeHost.mock.calls.length, 1);
        NodeAssert.equal(operations.waitForHost.mock.calls.length, 0);
        NodeAssert.equal(operations.removeSocket.mock.calls.length, 0);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 0);
        NodeAssert.equal(terminate.mock.calls.length, 0);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("preserves an unprovenanced app-server after bounded transient probe failures", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const { operations } = makeOperations({
        appServerProbeResults: [false, false, true],
        probeResults: [false, false],
        socketPathExists: true,
      });
      try {
        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host);

        NodeAssert.equal(yield* host.ensure, undefined);
        NodeAssert.deepStrictEqual(operations.probeAppServer.mock.calls, [
          [paths.appServerSocketPath],
          [paths.appServerSocketPath],
          [paths.appServerSocketPath],
        ]);
        NodeAssert.deepStrictEqual(operations.sleep.mock.calls, [
          [__testing.appServerStaleProbeRetryMs],
          [__testing.appServerStaleProbeRetryMs],
        ]);
        NodeAssert.equal(
          operations.probeAppServer.mock.calls.length,
          __testing.appServerStaleProbeAttempts,
        );
        NodeAssert.equal(operations.removeSocket.mock.calls.length, 0);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 0);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("starts recovery beside an unprovenanced socket after every bounded probe fails", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const { operations } = makeOperations({
        appServerProbeResults: [false, false, false],
        probeResults: [false, false],
        socketPathExists: true,
      });
      try {
        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host);

        const controlSocketPath = yield* host.ensure;
        NodeAssert.equal(typeof controlSocketPath, "string");
        NodeAssert.notEqual(controlSocketPath, paths.socketPath);
        NodeAssert.equal(
          operations.probeAppServer.mock.calls.length,
          __testing.appServerStaleProbeAttempts + 1,
        );
        NodeAssert.deepStrictEqual(operations.removeSocket.mock.calls, [[controlSocketPath]]);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
        const config = yield* readCodexProviderHostConfig(spawnedConfigPath(operations));
        NodeAssert.equal(config.controlSocketPath, controlSocketPath);
        NodeAssert.equal(config.appServerMode, "spawn");
        NodeAssert.notEqual(config.appServerSocketPath, paths.appServerSocketPath);
        NodeAssert.equal(
          NodePath.dirname(config.appServerSocketPath),
          NodePath.dirname(paths.appServerSocketPath),
        );
        NodeAssert.equal(
          Buffer.byteLength(config.appServerSocketPath),
          Buffer.byteLength(paths.appServerSocketPath),
        );
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("preserves an app-server that becomes ready at the final takeover check", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const { operations } = makeOperations({
        appServerProbeResults: [false, false, false, true],
        probeResults: [false, false],
        socketPathExists: true,
      });
      try {
        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host);

        NodeAssert.equal(yield* host.ensure, undefined);
        NodeAssert.equal(
          operations.probeAppServer.mock.calls.length,
          __testing.appServerStaleProbeAttempts + 1,
        );
        NodeAssert.equal(operations.removeSocket.mock.calls.length, 0);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 0);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("does not unlink a preferred app-server that becomes live after the final probe", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const { operations } = makeOperations({
        appServerProbeResults: [false, false, false, false],
        probeResults: [false, false],
        socketPathExists: true,
      });
      let preferredAppServerBecameLive = false;
      operations.removeSocket.mockImplementation((socketPath) => {
        if (socketPath === paths.appServerSocketPath) {
          return Promise.reject(new Error("healthy app-server socket was unlinked"));
        }
        preferredAppServerBecameLive = true;
        return Promise.resolve();
      });
      try {
        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host);

        const controlSocketPath = yield* host.ensure;
        NodeAssert.equal(typeof controlSocketPath, "string");
        NodeAssert.notEqual(controlSocketPath, paths.socketPath);
        NodeAssert.equal(preferredAppServerBecameLive, true);
        NodeAssert.deepStrictEqual(operations.removeSocket.mock.calls, [[controlSocketPath]]);
        const config = yield* readCodexProviderHostConfig(spawnedConfigPath(operations));
        NodeAssert.equal(config.controlSocketPath, controlSocketPath);
        NodeAssert.notEqual(config.appServerSocketPath, paths.appServerSocketPath);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("starts a replacement host around a surviving Codex app-server", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const legacyAppServerSocketPath = NodePath.join(root, "legacy-runtime", "codex.sock");
      const { operations } = makeOperations({
        probeResults: [false, false],
        processIdentityResults: [false, true],
      });
      operations.probeAppServer.mockResolvedValue(true);
      try {
        writeLegacyManifest({
          path: paths.manifestPath,
          socketPath: paths.socketPath,
          appServerSocketPath: legacyAppServerSocketPath,
          options,
          hostProcess: { pid: 4_241, startTimeMs: 1_000 },
          childProcess: { pid: 4_242, startTimeMs: 2_000 },
        });
        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host);

        const controlSocketPath = yield* host.ensure;
        NodeAssert.equal(typeof controlSocketPath, "string");
        NodeAssert.notEqual(controlSocketPath, paths.socketPath);
        NodeAssert.deepStrictEqual(operations.probeAppServer.mock.calls, [
          [legacyAppServerSocketPath],
        ]);
        NodeAssert.deepStrictEqual(operations.removeSocket.mock.calls, [[controlSocketPath]]);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
        const config = yield* readCodexProviderHostConfig(spawnedConfigPath(operations));
        NodeAssert.equal(config.controlSocketPath, controlSocketPath);
        NodeAssert.equal(
          config.expectedManifestGenerationFingerprint,
          ProviderHostGenerationFingerprint.make("legacy-generation"),
        );
        NodeAssert.equal(config.appServerMode, "attach");
        NodeAssert.equal(config.appServerSocketPath, legacyAppServerSocketPath);
        NodeAssert.deepStrictEqual(config.adoptedAppServer?.appServer.process, {
          pid: 4_242,
          startTimeMs: 2_000,
        });
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reuses a verified live legacy host without adopting its app-server", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const legacyControlSocketPath = NodePath.join(root, "legacy-runtime", "host.sock");
      const { operations } = makeOperations({
        probeResults: [false, false],
        processIdentityStatuses: ["current", "current"],
      });
      operations.probeAppServer.mockResolvedValue(true);
      try {
        writeLegacyManifest({
          path: paths.manifestPath,
          socketPath: legacyControlSocketPath,
          appServerSocketPath: paths.appServerSocketPath,
          options,
          hostProcess: { pid: 4_241, startTimeMs: 1_000 },
          childProcess: { pid: 4_242, startTimeMs: 2_000 },
        });
        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host);

        NodeAssert.equal(yield* host.ensure, legacyControlSocketPath);
        NodeAssert.deepStrictEqual(operations.inspectProcessIdentity.mock.calls, [
          [{ pid: 4_241, startTimeMs: 1_000 }],
        ]);
        NodeAssert.deepStrictEqual(operations.waitForHost.mock.calls, [
          [
            legacyControlSocketPath,
            {
              version: PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
              providerInstanceId: options.providerInstanceId,
              generationFingerprint: ProviderHostGenerationFingerprint.make("legacy-generation"),
              hostProcess: { pid: 4_241, startTimeMs: 1_000 },
            },
          ],
        ]);
        NodeAssert.equal(operations.probeAppServer.mock.calls.length, 0);
        NodeAssert.equal(operations.removeSocket.mock.calls.length, 0);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 0);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("promotes a failed legacy attachment without changing legacy process ownership", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const legacyControlSocketPath = NodePath.join(root, "legacy-runtime", "host.sock");
      const legacyAppServerSocketPath = NodePath.join(root, "legacy-runtime", "codex.sock");
      const { operations, terminate } = makeOperations({
        probeResults: [true, true],
        processIdentityStatuses: ["current"],
      });
      operations.probeAppServer.mockResolvedValue(true);
      try {
        writeLegacyManifest({
          path: paths.manifestPath,
          socketPath: legacyControlSocketPath,
          appServerSocketPath: legacyAppServerSocketPath,
          options,
          hostProcess: { pid: 4_241, startTimeMs: 1_000 },
          childProcess: { pid: 4_242, startTimeMs: 2_000 },
        });
        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host);
        NodeAssert.equal(yield* host.ensure, legacyControlSocketPath);
        NodeAssert.ok(host.promoteLegacyHost);

        const promotedControlSocketPath = yield* host.promoteLegacyHost({
          controlSocketPath: legacyControlSocketPath,
          generationFingerprint: "legacy-generation",
        });

        NodeAssert.equal(typeof promotedControlSocketPath, "string");
        NodeAssert.notEqual(promotedControlSocketPath, legacyControlSocketPath);
        NodeAssert.deepStrictEqual(operations.probeHost.mock.calls.slice(0, 2), [
          [
            legacyControlSocketPath,
            {
              version: PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
              providerInstanceId: options.providerInstanceId,
              generationFingerprint: ProviderHostGenerationFingerprint.make("legacy-generation"),
              hostProcess: { pid: 4_241, startTimeMs: 1_000 },
            },
          ],
          [
            legacyControlSocketPath,
            {
              version: PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
              providerInstanceId: options.providerInstanceId,
              generationFingerprint: ProviderHostGenerationFingerprint.make("legacy-generation"),
              hostProcess: { pid: 4_241, startTimeMs: 1_000 },
            },
          ],
        ]);
        NodeAssert.deepStrictEqual(operations.removeSocket.mock.calls, [
          [promotedControlSocketPath],
        ]);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
        NodeAssert.equal(terminate.mock.calls.length, 0);
        const config = yield* readCodexProviderHostConfig(spawnedConfigPath(operations));
        NodeAssert.equal(config.controlSocketPath, promotedControlSocketPath);
        NodeAssert.equal(config.appServerMode, "attach");
        NodeAssert.equal(config.appServerSocketPath, legacyAppServerSocketPath);
        NodeAssert.equal(
          config.expectedManifestGenerationFingerprint,
          ProviderHostGenerationFingerprint.make("legacy-generation"),
        );
        NodeAssert.deepStrictEqual(config.adoptedAppServer, {
          owner: {
            generationFingerprint: ProviderHostGenerationFingerprint.make("legacy-generation"),
            process: { pid: 4_241, startTimeMs: 1_000 },
          },
          appServer: {
            process: { pid: 4_242, startTimeMs: 2_000 },
            socketPath: legacyAppServerSocketPath,
            resolvedBinary: options.binaryPath,
            version: "codex-cli legacy",
            launchConfig: {
              arguments: ["app-server", "--listen", `unix://${legacyAppServerSocketPath}`],
              workingDirectory: root,
              environmentKeys: [],
            },
          },
        });
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("waits for a concurrently promoted same-build host to become ready", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const promotedControlSocketPath = NodePath.join(root, "runtime", "promoted.sock");
      const { operations } = makeOperations({
        waitResult: true,
      });
      try {
        yield* persistProviderHostManifest({
          path: paths.manifestPath,
          manifest: {
            schemaVersion: PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
            protocolVersion: PROVIDER_HOST_PROTOCOL_VERSION,
            buildFingerprint: ProviderHostBuildFingerprint.make("development"),
            generationFingerprint: ProviderHostGenerationFingerprint.make(
              "generation-concurrent-promotion",
            ),
            hostProcess: {
              pid: 4_241,
              startTimeMs: 1_000,
            },
            controlSocketPath: promotedControlSocketPath,
            codex: {
              appServerMode: "attach",
              owner: {
                generationFingerprint: ProviderHostGenerationFingerprint.make("generation-legacy"),
                process: {
                  pid: 4_240,
                  startTimeMs: 500,
                },
              },
              appServer: {
                process: {
                  pid: 4_242,
                  startTimeMs: 2_000,
                },
                socketPath: paths.appServerSocketPath,
                resolvedBinary: options.binaryPath,
                version: "codex-cli test",
                launchConfig: {
                  arguments: [],
                  workingDirectory: options.cwd,
                  environmentKeys: [],
                },
              },
            },
            startedAt: DateTime.makeUnsafe("2026-08-02T00:00:00.000Z"),
          },
        });
        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host?.promoteLegacyHost);

        NodeAssert.equal(
          yield* host.promoteLegacyHost({
            controlSocketPath: NodePath.join(root, "legacy-runtime", "host.sock"),
            generationFingerprint: "legacy-generation",
          }),
          promotedControlSocketPath,
        );
        NodeAssert.deepStrictEqual(operations.waitForHost.mock.calls, [
          [
            promotedControlSocketPath,
            {
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              providerInstanceId: options.providerInstanceId,
              buildFingerprint: ProviderHostBuildFingerprint.make("development"),
            },
          ],
        ]);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 0);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("adopts the winning concurrent promotion after its socket becomes ready", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const legacyControlSocketPath = NodePath.join(root, "legacy-runtime", "host.sock");
      const legacyAppServerSocketPath = NodePath.join(root, "legacy-runtime", "codex.sock");
      const winningControlSocketPath = NodePath.join(root, "runtime", "winner.sock");
      const { operations, terminate } = makeOperations({
        probeResults: [true, true, false],
        processIdentityStatuses: ["current"],
      });
      operations.probeAppServer.mockResolvedValue(true);
      let waitCount = 0;
      operations.waitForHost.mockImplementation(async () => {
        waitCount += 1;
        if (waitCount === 1) {
          NodeFS.writeFileSync(
            paths.manifestPath,
            `${JSON.stringify({
              schemaVersion: PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
              protocolVersion: PROVIDER_HOST_PROTOCOL_VERSION,
              buildFingerprint: "development",
              generationFingerprint: "generation-concurrent-winner",
              hostProcess: {
                pid: 5_241,
                startTimeMs: 3_000,
              },
              controlSocketPath: winningControlSocketPath,
              codex: {
                appServerMode: "attach",
                owner: {
                  generationFingerprint: "legacy-generation",
                  process: {
                    pid: 4_241,
                    startTimeMs: 1_000,
                  },
                },
                appServer: {
                  process: {
                    pid: 4_242,
                    startTimeMs: 2_000,
                  },
                  socketPath: legacyAppServerSocketPath,
                  resolvedBinary: options.binaryPath,
                  version: "codex-cli legacy",
                  launchConfig: {
                    arguments: ["app-server", "--listen", `unix://${legacyAppServerSocketPath}`],
                    workingDirectory: options.cwd,
                    environmentKeys: [],
                  },
                },
              },
              startedAt: "2026-08-02T00:00:00.000Z",
            })}\n`,
            { mode: 0o600 },
          );
          return false;
        }
        return true;
      });
      try {
        writeLegacyManifest({
          path: paths.manifestPath,
          socketPath: legacyControlSocketPath,
          appServerSocketPath: legacyAppServerSocketPath,
          options,
          hostProcess: { pid: 4_241, startTimeMs: 1_000 },
          childProcess: { pid: 4_242, startTimeMs: 2_000 },
        });
        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host);
        NodeAssert.equal(yield* host.ensure, legacyControlSocketPath);
        NodeAssert.ok(host.promoteLegacyHost);

        NodeAssert.equal(
          yield* host.promoteLegacyHost({
            controlSocketPath: legacyControlSocketPath,
            generationFingerprint: "legacy-generation",
          }),
          winningControlSocketPath,
        );
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
        NodeAssert.equal(terminate.mock.calls.length, 1);
        NodeAssert.equal(waitCount, 2);
        NodeAssert.equal(
          (operations.waitForHost.mock.calls[1] as [string] | undefined)?.[0],
          winningControlSocketPath,
        );
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("preserves a legacy host whose process identity cannot be verified", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const legacyControlSocketPath = NodePath.join(root, "legacy-runtime", "host.sock");
      const { operations } = makeOperations({
        probeResults: [false, false],
        processIdentityStatuses: ["unknown"],
        waitResult: false,
      });
      try {
        writeLegacyManifest({
          path: paths.manifestPath,
          socketPath: legacyControlSocketPath,
          appServerSocketPath: paths.appServerSocketPath,
          options,
          hostProcess: { pid: 4_241, startTimeMs: 1_000 },
          childProcess: { pid: 4_242, startTimeMs: 2_000 },
        });
        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host);

        NodeAssert.equal(yield* host.ensure, undefined);
        NodeAssert.equal(operations.waitForHost.mock.calls.length, 1);
        NodeAssert.equal(operations.probeAppServer.mock.calls.length, 0);
        NodeAssert.equal(operations.removeSocket.mock.calls.length, 0);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 0);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "takes over an unavailable same-build control socket without replacing its app-server",
    () =>
      Effect.gen(function* () {
        const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
        const options = makeOptions(root);
        const paths = codexAppServerHostPaths(options);
        const { operations } = makeOperations({
          probeResults: [false, false, true],
          processIdentityStatuses: ["current"],
          waitResult: false,
        });
        operations.probeAppServer.mockResolvedValue(true);
        operations.waitForHost.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
        try {
          yield* persistProviderHostManifest({
            path: paths.manifestPath,
            manifest: {
              schemaVersion: PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
              protocolVersion: PROVIDER_HOST_PROTOCOL_VERSION,
              buildFingerprint: ProviderHostBuildFingerprint.make("development"),
              generationFingerprint: ProviderHostGenerationFingerprint.make("generation-current"),
              hostProcess: {
                pid: 4_241,
                startTimeMs: 1_000,
              },
              controlSocketPath: paths.socketPath,
              codex: {
                appServerMode: "spawn",
                owner: {
                  generationFingerprint:
                    ProviderHostGenerationFingerprint.make("generation-current"),
                  process: {
                    pid: 4_241,
                    startTimeMs: 1_000,
                  },
                },
                appServer: {
                  process: {
                    pid: 4_242,
                    startTimeMs: 2_000,
                  },
                  socketPath: paths.appServerSocketPath,
                  resolvedBinary: options.binaryPath,
                  version: "codex-cli test",
                  launchConfig: {
                    arguments: [],
                    workingDirectory: options.cwd,
                    environmentKeys: [],
                  },
                },
              },
              startedAt: DateTime.makeUnsafe("2026-08-01T00:00:00.000Z"),
            },
          });
          const host = yield* makeHost(root, operations, { options });
          NodeAssert.ok(host);

          const replacementSocketPath = yield* host.ensure;
          NodeAssert.ok(replacementSocketPath);
          NodeAssert.notEqual(replacementSocketPath, paths.socketPath);
          NodeAssert.equal(
            NodePath.dirname(replacementSocketPath),
            NodePath.dirname(paths.socketPath),
          );
          NodeAssert.ok(
            NodePath.basename(replacementSocketPath).startsWith(
              `h${PROVIDER_HOST_PROTOCOL_VERSION}-`,
            ),
          );
          NodeAssert.equal(operations.waitForHost.mock.calls.length, 2);
          NodeAssert.deepStrictEqual(
            operations.waitForHost.mock.calls.map(([socketPath]) => socketPath),
            [paths.socketPath, replacementSocketPath],
          );
          NodeAssert.deepStrictEqual(operations.probeAppServer.mock.calls, [
            [paths.appServerSocketPath],
          ]);
          NodeAssert.deepStrictEqual(operations.removeSocket.mock.calls, [[replacementSocketPath]]);
          NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
          const config = yield* readCodexProviderHostConfig(spawnedConfigPath(operations));
          NodeAssert.equal(config.controlSocketPath, replacementSocketPath);
          NodeAssert.equal(
            config.expectedManifestGenerationFingerprint,
            ProviderHostGenerationFingerprint.make("generation-current"),
          );
          NodeAssert.equal(config.appServerMode, "attach");
          NodeAssert.equal(config.appServerSocketPath, paths.appServerSocketPath);

          yield* persistProviderHostManifest({
            path: paths.manifestPath,
            manifest: {
              schemaVersion: PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
              protocolVersion: PROVIDER_HOST_PROTOCOL_VERSION,
              buildFingerprint: ProviderHostBuildFingerprint.make("development"),
              generationFingerprint:
                ProviderHostGenerationFingerprint.make("generation-replacement"),
              hostProcess: {
                pid: 4_243,
                startTimeMs: 3_000,
              },
              controlSocketPath: replacementSocketPath,
              codex: {
                appServerMode: "attach",
                owner: {
                  generationFingerprint:
                    ProviderHostGenerationFingerprint.make("generation-current"),
                  process: {
                    pid: 4_241,
                    startTimeMs: 1_000,
                  },
                },
                appServer: {
                  process: {
                    pid: 4_242,
                    startTimeMs: 2_000,
                  },
                  socketPath: paths.appServerSocketPath,
                  resolvedBinary: options.binaryPath,
                  version: "codex-cli test",
                  launchConfig: {
                    arguments: [],
                    workingDirectory: options.cwd,
                    environmentKeys: [],
                  },
                },
              },
              startedAt: DateTime.makeUnsafe("2026-08-01T00:01:00.000Z"),
            },
          });

          const restartedHost = yield* makeHost(root, operations, { options });
          NodeAssert.ok(restartedHost);
          NodeAssert.equal(yield* restartedHost.ensure, replacementSocketPath);
          NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
          NodeAssert.equal(operations.probeHost.mock.calls.at(-1)?.[0], replacementSocketPath);
        } finally {
          NodeFS.rmSync(root, { recursive: true, force: true });
        }
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("does not wait on the control socket from a previous T3 build", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const previousPaths = codexAppServerHostPaths(
        options,
        ProviderHostBuildFingerprint.make("previous-build"),
      );
      const previousAppServerSocketPath = NodePath.join(root, "previous-runtime", "codex.sock");
      const { operations } = makeOperations({
        probeResults: [false, false],
        processIdentityResults: [true, true],
      });
      operations.probeAppServer.mockResolvedValue(true);
      try {
        yield* persistProviderHostManifest({
          path: paths.manifestPath,
          manifest: {
            schemaVersion: PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
            protocolVersion: PROVIDER_HOST_PROTOCOL_VERSION,
            buildFingerprint: ProviderHostBuildFingerprint.make("previous-build"),
            generationFingerprint: ProviderHostGenerationFingerprint.make("previous-generation"),
            hostProcess: {
              pid: 4_241,
              startTimeMs: 1_000,
            },
            controlSocketPath: previousPaths.socketPath,
            codex: {
              appServerMode: "spawn",
              owner: {
                generationFingerprint:
                  ProviderHostGenerationFingerprint.make("previous-generation"),
                process: {
                  pid: 4_241,
                  startTimeMs: 1_000,
                },
              },
              appServer: {
                process: {
                  pid: 4_242,
                  startTimeMs: 2_000,
                },
                socketPath: previousAppServerSocketPath,
                resolvedBinary: options.binaryPath,
                version: "codex-cli test",
                launchConfig: {
                  arguments: [],
                  workingDirectory: options.cwd,
                  environmentKeys: [],
                },
              },
            },
            startedAt: DateTime.makeUnsafe("2026-08-01T00:00:00.000Z"),
          },
        });

        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host);

        const controlSocketPath = yield* host.ensure;
        NodeAssert.equal(typeof controlSocketPath, "string");
        NodeAssert.notEqual(controlSocketPath, paths.socketPath);
        NodeAssert.notEqual(previousPaths.socketPath, paths.socketPath);
        NodeAssert.deepStrictEqual(
          operations.waitForHost.mock.calls.map(([socketPath]) => socketPath),
          [controlSocketPath],
        );
        NodeAssert.deepStrictEqual(operations.probeAppServer.mock.calls, [
          [previousAppServerSocketPath],
        ]);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
        NodeAssert.deepStrictEqual(operations.removeSocket.mock.calls, [[controlSocketPath]]);
        const config = yield* readCodexProviderHostConfig(spawnedConfigPath(operations));
        NodeAssert.equal(config.controlSocketPath, controlSocketPath);
        NodeAssert.equal(
          config.expectedManifestGenerationFingerprint,
          ProviderHostGenerationFingerprint.make("previous-generation"),
        );
        NodeAssert.equal(config.appServerSocketPath, previousAppServerSocketPath);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("fails closed when the durable provider-host manifest is corrupt", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const { operations } = makeOperations({
        probeResults: [false, false],
      });
      try {
        NodeFS.mkdirSync(NodePath.dirname(paths.manifestPath), { recursive: true });
        NodeFS.writeFileSync(paths.manifestPath, "{not-json", { mode: 0o600 });

        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host);

        NodeAssert.equal(yield* host.ensure, undefined);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 0);
        NodeAssert.equal(operations.removeSocket.mock.calls.length, 0);
        NodeAssert.equal(operations.waitForHost.mock.calls.length, 0);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("preserves a live app-server process while its socket is unavailable", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const { operations } = makeOperations({
        probeResults: [false, false],
        processIdentityResults: [false, true],
      });
      operations.probeAppServer.mockResolvedValue(false);
      try {
        writeLegacyManifest({
          path: paths.manifestPath,
          socketPath: paths.socketPath,
          appServerSocketPath: paths.appServerSocketPath,
          options,
          hostProcess: { pid: 4_241, startTimeMs: 1_000 },
          childProcess: { pid: 4_242, startTimeMs: 2_000 },
        });
        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host);

        NodeAssert.equal(yield* host.ensure, undefined);
        NodeAssert.deepStrictEqual(operations.inspectProcessIdentity.mock.calls, [
          [{ pid: 4_241, startTimeMs: 1_000 }],
          [{ pid: 4_242, startTimeMs: 2_000 }],
        ]);
        NodeAssert.equal(operations.removeSocket.mock.calls.length, 0);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 0);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("preserves an app-server when process liveness cannot be determined", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const { operations } = makeOperations({
        probeResults: [false, false],
        processIdentityStatuses: ["stale", "unknown"],
      });
      operations.probeAppServer.mockResolvedValue(false);
      try {
        writeLegacyManifest({
          path: paths.manifestPath,
          socketPath: paths.socketPath,
          appServerSocketPath: paths.appServerSocketPath,
          options,
          hostProcess: { pid: 4_241, startTimeMs: 1_000 },
          childProcess: { pid: 4_242, startTimeMs: 2_000 },
        });
        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host);

        NodeAssert.equal(yield* host.ensure, undefined);
        NodeAssert.equal(operations.removeSocket.mock.calls.length, 0);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 0);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "preserves a live preferred app-server when a stale manifest names another socket",
    () =>
      Effect.gen(function* () {
        const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
        const options = makeOptions(root);
        const paths = codexAppServerHostPaths(options);
        const staleAppServerSocketPath = NodePath.join(root, "stale-runtime", "codex.sock");
        const { operations } = makeOperations({
          probeResults: [false, false],
          processIdentityResults: [false, false],
        });
        operations.probeAppServer.mockImplementation((socketPath) =>
          Promise.resolve(socketPath === paths.appServerSocketPath),
        );
        try {
          writeLegacyManifest({
            path: paths.manifestPath,
            socketPath: paths.socketPath,
            appServerSocketPath: staleAppServerSocketPath,
            options,
            hostProcess: { pid: 4_241, startTimeMs: 1_000 },
            childProcess: { pid: 4_242, startTimeMs: 2_000 },
          });
          const host = yield* makeHost(root, operations, { options });
          NodeAssert.ok(host);

          NodeAssert.equal(yield* host.ensure, undefined);
          NodeAssert.deepStrictEqual(operations.probeAppServer.mock.calls, [
            [staleAppServerSocketPath],
            [paths.appServerSocketPath],
          ]);
          NodeAssert.equal(operations.removeSocket.mock.calls.length, 0);
          NodeAssert.equal(operations.spawnDetached.mock.calls.length, 0);
        } finally {
          NodeFS.rmSync(root, { recursive: true, force: true });
        }
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "replaces a stale manifest when its pid belongs to a different process generation",
    () =>
      Effect.gen(function* () {
        const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
        const options = makeOptions(root);
        const paths = codexAppServerHostPaths(options);
        const { operations } = makeOperations({
          probeResults: [false, false],
          processIdentityResults: [false, false],
        });
        try {
          yield* persistProviderHostManifest({
            path: paths.manifestPath,
            manifest: {
              schemaVersion: PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
              protocolVersion: PROVIDER_HOST_PROTOCOL_VERSION,
              buildFingerprint: ProviderHostBuildFingerprint.make("development"),
              generationFingerprint: ProviderHostGenerationFingerprint.make("generation-stale"),
              hostProcess: {
                pid: process.pid,
                startTimeMs: 1_000,
              },
              controlSocketPath: paths.socketPath,
              codex: {
                appServerMode: "spawn",
                owner: {
                  generationFingerprint: ProviderHostGenerationFingerprint.make("generation-stale"),
                  process: {
                    pid: process.pid,
                    startTimeMs: 1_000,
                  },
                },
                appServer: {
                  process: {
                    pid: process.pid + 1,
                    startTimeMs: 1_001,
                  },
                  socketPath: paths.appServerSocketPath,
                  resolvedBinary: options.binaryPath,
                  version: "codex-cli test",
                  launchConfig: {
                    arguments: [],
                    environmentKeys: [],
                  },
                },
              },
              startedAt: DateTime.makeUnsafe("2026-08-01T00:00:00.000Z"),
            },
          });

          const host = yield* makeHost(root, operations, { options });
          NodeAssert.ok(host);

          const controlSocketPath = yield* host.ensure;
          NodeAssert.equal(typeof controlSocketPath, "string");
          NodeAssert.notEqual(controlSocketPath, paths.socketPath);
          NodeAssert.deepStrictEqual(operations.inspectProcessIdentity.mock.calls, [
            [
              {
                pid: process.pid,
                startTimeMs: 1_000,
              },
            ],
            [
              {
                pid: process.pid + 1,
                startTimeMs: 1_001,
              },
            ],
          ]);
          NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
          NodeAssert.deepStrictEqual(operations.removeSocket.mock.calls, [[controlSocketPath]]);
        } finally {
          NodeFS.rmSync(root, { recursive: true, force: true });
        }
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("does not start a provider host without the T3 entry path", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const { operations } = makeOperations();
      try {
        const host = yield* makeHost(root, operations, {
          processArguments: [TEST_EXECUTABLE_PATH],
        });

        NodeAssert.equal(host, undefined);
        NodeAssert.equal(operations.probeHost.mock.calls.length, 0);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 0);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("uses the scoped fallback on platforms without Unix sockets", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const { operations } = makeOperations();
      try {
        const host = yield* makeHost(root, operations, { platform: "win32" });

        NodeAssert.equal(host, undefined);
        NodeAssert.equal(operations.probeHost.mock.calls.length, 0);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 0);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("terminates only the captured provider host when startup times out", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const paths = codexAppServerHostPaths(makeOptions(root));
      const { operations, terminate, terminatedPids } = makeOperations({
        probeResults: [false, false],
        waitResult: false,
      });
      try {
        const host = yield* makeHost(root, operations);
        NodeAssert.ok(host);

        NodeAssert.equal(yield* host.ensure, undefined);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
        NodeAssert.equal(terminate.mock.calls.length, 1);
        NodeAssert.deepStrictEqual(terminatedPids, [4242]);
        const controlSocketPath = (
          operations.removeSocket.mock.calls[0] as [string] | undefined
        )?.[0];
        NodeAssert.equal(typeof controlSocketPath, "string");
        NodeAssert.notEqual(controlSocketPath, paths.socketPath);
        NodeAssert.deepStrictEqual(
          operations.removeSocket.mock.calls.map(([socketPath]) => socketPath),
          [controlSocketPath, controlSocketPath],
        );
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps a healthy host attached across Codex config generation changes", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const firstOptions = makeOptions(root);
      const nextOptions = {
        ...makeOptions(root),
        binaryPath: "/opt/codex-next/bin/codex",
      };
      const { operations, terminate } = makeOperations({
        probeResults: [false, false, true],
      });
      try {
        const firstHost = yield* makeHost(root, operations, { options: firstOptions });
        const nextHost = yield* makeHost(root, operations, { options: nextOptions });
        NodeAssert.ok(firstHost);
        NodeAssert.ok(nextHost);

        const firstControlSocketPath = yield* firstHost.ensure;
        NodeAssert.equal(typeof firstControlSocketPath, "string");
        const firstConfig = yield* readCodexProviderHostConfig(spawnedConfigPath(operations));
        yield* persistProviderHostManifest({
          path: codexAppServerHostPaths(firstOptions).manifestPath,
          manifest: {
            schemaVersion: PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
            protocolVersion: PROVIDER_HOST_PROTOCOL_VERSION,
            buildFingerprint: ProviderHostBuildFingerprint.make("development"),
            generationFingerprint: ProviderHostGenerationFingerprint.make("generation-current"),
            hostProcess: {
              pid: 4_241,
              startTimeMs: 1_000,
            },
            controlSocketPath: firstControlSocketPath as string,
            codex: {
              appServerMode: firstConfig.appServerMode,
              owner: {
                generationFingerprint: ProviderHostGenerationFingerprint.make("generation-current"),
                process: {
                  pid: 4_241,
                  startTimeMs: 1_000,
                },
              },
              appServer: {
                process: {
                  pid: 4_242,
                  startTimeMs: 2_000,
                },
                socketPath: firstConfig.appServerSocketPath,
                resolvedBinary: firstOptions.binaryPath,
                version: "codex-cli test",
                launchConfig: {
                  arguments: [],
                  workingDirectory: firstOptions.cwd,
                  environmentKeys: [],
                },
              },
            },
            startedAt: DateTime.makeUnsafe("2026-08-01T00:00:00.000Z"),
          },
        });

        NodeAssert.equal(yield* nextHost.ensure, firstControlSocketPath);
        NodeAssert.equal(firstHost.socketPath, nextHost.socketPath);
        NodeAssert.notEqual(
          codexProviderHostConfigurationFingerprint(firstOptions),
          codexProviderHostConfigurationFingerprint(nextOptions),
        );
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
        NodeAssert.equal(operations.removeSocket.mock.calls.length, 1);
        NodeAssert.equal(terminate.mock.calls.length, 0);

        NodeAssert.equal(firstConfig.codex.binaryPath, firstOptions.binaryPath);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("gives concurrent detached-host launches unique control and config paths", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const { operations } = makeOperations({
        probeResults: [false, false, false, false],
      });
      try {
        const firstHost = yield* makeHost(root, operations);
        const secondHost = yield* makeHost(root, operations);
        NodeAssert.ok(firstHost);
        NodeAssert.ok(secondHost);
        const [firstControlSocketPath, secondControlSocketPath] = yield* Effect.all(
          [firstHost.ensure, secondHost.ensure],
          { concurrency: 2 },
        );

        NodeAssert.equal(typeof firstControlSocketPath, "string");
        NodeAssert.equal(typeof secondControlSocketPath, "string");
        NodeAssert.notEqual(firstControlSocketPath, secondControlSocketPath);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 2);
        NodeAssert.equal(operations.removeSocket.mock.calls.length, 2);
        const firstConfigPath = spawnedConfigPath(operations, 0);
        const secondConfigPath = spawnedConfigPath(operations, 1);
        NodeAssert.notEqual(firstConfigPath, secondConfigPath);
        const firstConfig = yield* readCodexProviderHostConfig(firstConfigPath);
        const secondConfig = yield* readCodexProviderHostConfig(secondConfigPath);
        NodeAssert.deepStrictEqual(
          new Set([firstConfig.controlSocketPath, secondConfig.controlSocketPath]),
          new Set([firstControlSocketPath, secondControlSocketPath]),
        );
        NodeAssert.notEqual(firstConfig.appServerSocketPath, secondConfig.appServerSocketPath);
        NodeAssert.notEqual(firstConfig.appServerSocketPath, firstHost.appServerSocketPath);
        NodeAssert.notEqual(secondConfig.appServerSocketPath, secondHost.appServerSocketPath);
        NodeAssert.equal(firstConfig.startupLockPath, secondConfig.startupLockPath);
        NodeAssert.equal(firstConfig.expectedManifestGenerationFingerprint, undefined);
        NodeAssert.equal(secondConfig.expectedManifestGenerationFingerprint, undefined);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
