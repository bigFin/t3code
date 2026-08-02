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
import * as Fiber from "effect/Fiber";
import { describe } from "vite-plus/test";

import {
  __testing,
  codexAppServerHostPaths,
  codexProviderHostConfigurationFingerprint,
  makeCodexAppServerHost,
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
  PROVIDER_HOST_PROTOCOL_VERSION,
  ProviderHostBuildFingerprint,
  ProviderHostGenerationFingerprint,
  ProviderHostHelloEnvelope,
  ProviderHostReplayCursor,
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

  it("keeps the app-server socket and startup lock stable when only the control socket is too long", () => {
    const root = NodePath.join("/tmp", "x".repeat(41));
    const options = makeOptions(root);
    const paths = codexAppServerHostPaths(options);
    const preferredRuntimeDir = NodePath.join(options.environment.XDG_RUNTIME_DIR!, "t3-code");

    NodeAssert.ok(paths.socketPath.startsWith("/tmp/t3-code-"));
    NodeAssert.ok(paths.appServerSocketPath.startsWith(preferredRuntimeDir));
    NodeAssert.equal(NodePath.dirname(paths.lockPath), preferredRuntimeDir);
    NodeAssert.ok(Buffer.byteLength(paths.appServerSocketPath) <= 96);
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
        providerInstanceId: ProviderInstanceId.make("codex"),
        buildFingerprint: ProviderHostBuildFingerprint.make("build-a"),
      }),
      true,
    );
    NodeAssert.equal(
      __testing.matchesProviderHostIdentity(hello, {
        providerInstanceId: ProviderInstanceId.make("codex-work"),
        buildFingerprint: ProviderHostBuildFingerprint.make("build-a"),
      }),
      false,
    );
    NodeAssert.equal(
      __testing.matchesProviderHostIdentity(hello, {
        providerInstanceId: ProviderInstanceId.make("codex"),
        buildFingerprint: ProviderHostBuildFingerprint.make("build-b"),
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

        NodeAssert.equal(first, paths.socketPath);
        NodeAssert.equal(second, paths.socketPath);
        NodeAssert.equal(host.socketPath, paths.socketPath);
        NodeAssert.equal(host.appServerSocketPath, paths.appServerSocketPath);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
        NodeAssert.equal(terminate.mock.calls.length, 0);

        const spawn = operations.spawnDetached.mock.calls[0];
        NodeAssert.equal(spawn?.[0].command, TEST_EXECUTABLE_PATH);
        NodeAssert.deepStrictEqual(spawn?.[0].args, [
          TEST_ENTRY_PATH,
          "__provider-host",
          "--config",
          paths.configPath,
        ]);
        NodeAssert.equal(spawn?.[0].cwd, root);
        NodeAssert.equal(spawn?.[0].env.CODEX_HOME, options.homePath);
        NodeAssert.equal(spawn?.[1], paths.logPath);

        const config = yield* readCodexProviderHostConfig(paths.configPath);
        const configInfo = NodeFS.statSync(paths.configPath);
        NodeAssert.equal(configInfo.mode & 0o777, 0o600);
        NodeAssert.deepStrictEqual(config, {
          version: CODEX_PROVIDER_HOST_CONFIG_VERSION,
          providerInstanceId: options.providerInstanceId,
          buildFingerprint: ProviderHostBuildFingerprint.make("development"),
          configurationFingerprint: codexProviderHostConfigurationFingerprint(options),
          controlSocketPath: paths.socketPath,
          appServerSocketPath: paths.appServerSocketPath,
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

        NodeAssert.equal(yield* host.ensure, paths.socketPath);
        NodeAssert.equal(
          operations.probeAppServer.mock.calls.length,
          __testing.appServerStaleProbeAttempts + 1,
        );
        NodeAssert.deepStrictEqual(operations.removeSocket.mock.calls, [[paths.socketPath]]);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
        const config = yield* readCodexProviderHostConfig(paths.configPath);
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

        NodeAssert.equal(yield* host.ensure, paths.socketPath);
        NodeAssert.equal(preferredAppServerBecameLive, true);
        NodeAssert.deepStrictEqual(operations.removeSocket.mock.calls, [[paths.socketPath]]);
        const config = yield* readCodexProviderHostConfig(paths.configPath);
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

        NodeAssert.equal(yield* host.ensure, paths.socketPath);
        NodeAssert.deepStrictEqual(operations.probeAppServer.mock.calls, [
          [legacyAppServerSocketPath],
        ]);
        NodeAssert.deepStrictEqual(operations.removeSocket.mock.calls, [[paths.socketPath]]);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
        const config = yield* readCodexProviderHostConfig(paths.configPath);
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

  it.effect("does not adopt from a live legacy host that may still own its app-server", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const { operations } = makeOperations({
        probeResults: [false, false],
        processIdentityStatuses: ["current"],
      });
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
          probeResults: [false, false],
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

          NodeAssert.equal(yield* host.ensure, paths.socketPath);
          NodeAssert.equal(operations.waitForHost.mock.calls.length, 2);
          NodeAssert.deepStrictEqual(operations.probeAppServer.mock.calls, [
            [paths.appServerSocketPath],
          ]);
          NodeAssert.deepStrictEqual(operations.removeSocket.mock.calls, [[paths.socketPath]]);
          NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
          const config = yield* readCodexProviderHostConfig(paths.configPath);
          NodeAssert.equal(config.appServerMode, "attach");
          NodeAssert.equal(config.appServerSocketPath, paths.appServerSocketPath);
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

        NodeAssert.equal(yield* host.ensure, paths.socketPath);
        NodeAssert.notEqual(previousPaths.socketPath, paths.socketPath);
        NodeAssert.deepStrictEqual(
          operations.waitForHost.mock.calls.map(([socketPath]) => socketPath),
          [paths.socketPath],
        );
        NodeAssert.deepStrictEqual(operations.probeAppServer.mock.calls, [
          [previousAppServerSocketPath],
        ]);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
        NodeAssert.deepStrictEqual(operations.removeSocket.mock.calls, [[paths.socketPath]]);
        const config = yield* readCodexProviderHostConfig(paths.configPath);
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

          NodeAssert.equal(yield* host.ensure, paths.socketPath);
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
          NodeAssert.deepStrictEqual(operations.removeSocket.mock.calls, [[paths.socketPath]]);
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
        NodeAssert.deepStrictEqual(
          operations.removeSocket.mock.calls.map(([socketPath]) => socketPath),
          [paths.socketPath, paths.socketPath],
        );
        NodeAssert.equal(NodeFS.statSync(paths.lockPath).mode & 0o777, 0o600);
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

        NodeAssert.equal(yield* firstHost.ensure, firstHost.socketPath);
        NodeAssert.equal(yield* nextHost.ensure, nextHost.socketPath);
        NodeAssert.equal(firstHost.socketPath, nextHost.socketPath);
        NodeAssert.notEqual(
          codexProviderHostConfigurationFingerprint(firstOptions),
          codexProviderHostConfigurationFingerprint(nextOptions),
        );
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
        NodeAssert.equal(operations.removeSocket.mock.calls.length, 1);
        NodeAssert.equal(terminate.mock.calls.length, 0);

        const config = yield* readCodexProviderHostConfig(
          codexAppServerHostPaths(firstOptions).configPath,
        );
        NodeAssert.equal(config.codex.binaryPath, firstOptions.binaryPath);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("serializes concurrent host creators without replacing the winner", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      let ready = false;
      let markSpawnStarted: () => void = () => undefined;
      const spawnStarted = new Promise<void>((resolve) => {
        markSpawnStarted = resolve;
      });
      let releaseSpawn: () => void = () => undefined;
      const spawnReleased = new Promise<void>((resolve) => {
        releaseSpawn = resolve;
      });
      let markReady: () => void = () => undefined;
      const becameReady = new Promise<void>((resolve) => {
        markReady = resolve;
      });
      const operations = {
        inspectProcessIdentity: vi.fn(() => Promise.resolve("current" as const)),
        probeAppServer: vi.fn(() => Promise.resolve(false)),
        probeHost: vi.fn(() => Promise.resolve(ready)),
        readBuildFingerprint: vi.fn(() =>
          Promise.resolve(ProviderHostBuildFingerprint.make("development")),
        ),
        removeSocket: vi.fn(() => Promise.resolve()),
        sleep: vi.fn(() => Promise.resolve()),
        socketPathExists: vi.fn(() => Promise.resolve(false)),
        spawnDetached: vi.fn(async () => {
          markSpawnStarted();
          await spawnReleased;
          ready = true;
          markReady();
          return { pid: 4242, terminate: () => Promise.resolve() };
        }),
        waitForHost: vi.fn(async () => {
          if (!ready) await becameReady;
          return true;
        }),
      } satisfies CodexAppServerHostOperations;

      try {
        const firstHost = yield* makeHost(root, operations);
        const secondHost = yield* makeHost(root, operations);
        NodeAssert.ok(firstHost);
        NodeAssert.ok(secondHost);
        const first = yield* firstHost.ensure.pipe(Effect.forkChild);
        yield* Effect.promise(() => spawnStarted);
        const second = yield* secondHost.ensure.pipe(Effect.forkChild);
        releaseSpawn();

        NodeAssert.equal(yield* Fiber.join(first), firstHost.socketPath);
        NodeAssert.equal(yield* Fiber.join(second), secondHost.socketPath);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
        NodeAssert.equal(operations.removeSocket.mock.calls.length, 1);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
