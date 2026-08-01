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
  codexProviderHostGenerationFingerprint,
  makeCodexAppServerHost,
  type CodexAppServerHostOperations,
  type CodexAppServerHostOptions,
} from "./CodexAppServerHost.ts";
import {
  CODEX_PROVIDER_HOST_CONFIG_VERSION,
  readCodexProviderHostConfig,
} from "../host/CodexProviderHostConfig.ts";
import {
  PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
  persistProviderHostManifest,
} from "../host/ProviderHostManifest.ts";
import { PROVIDER_HOST_PROTOCOL_VERSION } from "../host/ProviderHostProtocol.ts";

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
  readonly probeResults?: ReadonlyArray<boolean>;
  readonly processIdentityMatches?: boolean;
  readonly waitResult?: boolean;
}) {
  const probeResults = [...(input?.probeResults ?? [false])];
  const terminatedPids: Array<number> = [];
  const terminate = vi.fn(() => Promise.resolve());
  const operations = {
    isProcessIdentityCurrent: vi.fn(() => Promise.resolve(input?.processIdentityMatches ?? true)),
    probeAppServer: vi.fn((_socketPath: string) => Promise.resolve(false)),
    probeHost: vi.fn((_socketPath: string) => Promise.resolve(probeResults.shift() ?? false)),
    removeSocket: vi.fn((_socketPath: string) => Promise.resolve()),
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
      const otherHome = codexAppServerHostPaths(
        makeOptions(root, NodePath.join(root, "other-codex-home")),
      );
      const otherState = codexAppServerHostPaths({
        ...options,
        stateDir: NodePath.join(root, "other-state"),
      });

      NodeAssert.deepStrictEqual(first, same);
      NodeAssert.equal(first.socketPath, changedGeneration.socketPath);
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

    NodeAssert.equal(await __testing.isProcessIdentityCurrent(currentIdentity), true);
    NodeAssert.equal(
      await __testing.isProcessIdentityCurrent({
        ...currentIdentity,
        startTimeMs: 1_000,
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
          generationFingerprint: codexProviderHostGenerationFingerprint(options),
          controlSocketPath: paths.socketPath,
          appServerSocketPath: paths.appServerSocketPath,
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

  it.effect("preserves a surviving Codex app-server when its provider host is unavailable", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
      const options = makeOptions(root);
      const paths = codexAppServerHostPaths(options);
      const { operations } = makeOperations({
        probeResults: [false, false],
      });
      operations.probeAppServer.mockResolvedValue(true);
      try {
        const host = yield* makeHost(root, operations, { options });
        NodeAssert.ok(host);

        NodeAssert.equal(yield* host.ensure, undefined);
        NodeAssert.deepStrictEqual(operations.probeAppServer.mock.calls, [
          [paths.appServerSocketPath],
        ]);
        NodeAssert.equal(operations.removeSocket.mock.calls.length, 0);
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 0);
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

  it.effect(
    "replaces a stale manifest when its pid belongs to a different process generation",
    () =>
      Effect.gen(function* () {
        const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-host-"));
        const options = makeOptions(root);
        const paths = codexAppServerHostPaths(options);
        const { operations } = makeOperations({
          probeResults: [false, false],
          processIdentityMatches: false,
        });
        try {
          yield* persistProviderHostManifest({
            path: paths.manifestPath,
            manifest: {
              schemaVersion: PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
              protocolVersion: PROVIDER_HOST_PROTOCOL_VERSION,
              generationFingerprint: codexProviderHostGenerationFingerprint(options),
              hostProcess: {
                pid: process.pid,
                startTimeMs: 1_000,
              },
              socketPath: paths.socketPath,
              codex: {
                resolvedBinary: options.binaryPath,
                version: "codex-cli test",
                launchConfig: {
                  arguments: [],
                  environmentKeys: [],
                },
              },
              startedAt: DateTime.makeUnsafe("2026-08-01T00:00:00.000Z"),
            },
          });

          const host = yield* makeHost(root, operations, { options });
          NodeAssert.ok(host);

          NodeAssert.equal(yield* host.ensure, paths.socketPath);
          NodeAssert.deepStrictEqual(operations.isProcessIdentityCurrent.mock.calls, [
            [
              {
                pid: process.pid,
                startTimeMs: 1_000,
              },
            ],
          ]);
          NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
          NodeAssert.deepStrictEqual(
            operations.removeSocket.mock.calls.map(([socketPath]) => socketPath),
            [paths.socketPath, paths.appServerSocketPath],
          );
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

  it.effect("terminates only the captured child when provider-host startup times out", () =>
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
          [
            paths.socketPath,
            paths.appServerSocketPath,
            paths.socketPath,
            paths.appServerSocketPath,
          ],
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
          codexProviderHostGenerationFingerprint(firstOptions),
          codexProviderHostGenerationFingerprint(nextOptions),
        );
        NodeAssert.equal(operations.spawnDetached.mock.calls.length, 1);
        NodeAssert.equal(operations.removeSocket.mock.calls.length, 2);
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
        isProcessIdentityCurrent: vi.fn(() => Promise.resolve(true)),
        probeAppServer: vi.fn(() => Promise.resolve(false)),
        probeHost: vi.fn(() => Promise.resolve(ready)),
        removeSocket: vi.fn(() => Promise.resolve()),
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
        NodeAssert.equal(operations.removeSocket.mock.calls.length, 2);
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
