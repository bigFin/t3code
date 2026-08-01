// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalDate:off - Startup ownership records use wall-clock time.
// @effect-diagnostics globalTimers:off - Detached host readiness uses bounded timers.
import type { ProviderInstanceId, ResourceTelemetryProcessIdentity } from "@t3tools/contracts";
import {
  HostProcessArguments,
  HostProcessExecutablePath,
  HostProcessPlatform,
} from "@t3tools/shared/hostProcess";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import { expandHomePath } from "../../pathExpansion.ts";
import { acquireSqliteTransactionLock } from "../../sqliteTransactionLock.ts";
import { probeCodexAppServerWebSocket } from "./CodexAppServerWebSocket.ts";
import {
  CODEX_PROVIDER_HOST_CONFIG_VERSION,
  CodexProviderHostConfig,
  persistCodexProviderHostConfig,
} from "../host/CodexProviderHostConfig.ts";
import {
  PROVIDER_HOST_PROTOCOL_VERSION,
  ProviderHostGenerationFingerprint,
  ProviderHostHelloEnvelope,
} from "../host/ProviderHostProtocol.ts";
import { readProviderHostManifest } from "../host/ProviderHostManifest.ts";

const HOST_START_TIMEOUT_MS = 15_000;
const HOST_START_POLL_MS = 50;
const HOST_TERMINATE_TIMEOUT_MS = 2_000;
const HOST_LOCK_TIMEOUT_MS = 30_000;
const MAX_PORTABLE_UNIX_SOCKET_PATH_BYTES = 96;
const PROCESS_START_TIME_TOLERANCE_MS = 2_000;

export interface CodexAppServerHostShape {
  readonly socketPath: string;
  readonly appServerSocketPath?: string;
  readonly ensure: Effect.Effect<string | undefined>;
}

export interface CodexAppServerHostOptions {
  readonly binaryPath: string;
  readonly launchArgs?: string;
  readonly homePath?: string;
  readonly environment?: NodeJS.ProcessEnv;
  readonly cwd: string;
  readonly stateDir: string;
  readonly providerLogsDir: string;
  readonly providerInstanceId: ProviderInstanceId;
}

interface HostCommand {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly env: NodeJS.ProcessEnv;
}

export interface CodexAppServerDetachedProcess {
  readonly pid: number;
  readonly terminate: () => Promise<void>;
}

export interface CodexAppServerHostOperations {
  readonly isProcessIdentityCurrent: (
    identity: ResourceTelemetryProcessIdentity,
  ) => Promise<boolean>;
  readonly probeAppServer: (socketPath: string) => Promise<boolean>;
  readonly probeHost: (socketPath: string) => Promise<boolean>;
  readonly removeSocket: (socketPath: string) => Promise<void>;
  readonly spawnDetached: (
    command: HostCommand,
    logPath: string,
  ) => Promise<CodexAppServerDetachedProcess>;
  readonly waitForHost: (socketPath: string) => Promise<boolean>;
}

export interface CodexAppServerHostPaths {
  readonly socketPath: string;
  readonly appServerSocketPath: string;
  readonly logPath: string;
  readonly lockPath: string;
  readonly configPath: string;
  readonly manifestPath: string;
}

const decodeHello = Schema.decodeUnknownSync(Schema.fromJsonString(ProviderHostHelloEnvelope));

function mergeEnvironment(
  environment: NodeJS.ProcessEnv | undefined,
  homePath: string | undefined,
): NodeJS.ProcessEnv {
  const resolvedHomePath = homePath ? expandHomePath(homePath) : undefined;
  return {
    ...(environment === undefined ? process.env : environment),
    ...(resolvedHomePath ? { CODEX_HOME: resolvedHomePath } : {}),
  };
}

function effectiveCodexHome(options: CodexAppServerHostOptions): string {
  const configured =
    options.homePath ??
    options.environment?.CODEX_HOME ??
    process.env.CODEX_HOME ??
    NodePath.join(options.environment?.HOME ?? process.env.HOME ?? NodeOS.homedir(), ".codex");
  return expandHomePath(configured);
}

export function codexAppServerHostPaths(
  options: CodexAppServerHostOptions,
): CodexAppServerHostPaths {
  const configuredRuntimeRoot = options.environment?.XDG_RUNTIME_DIR ?? process.env.XDG_RUNTIME_DIR;
  const runtimeRoot = configuredRuntimeRoot ?? NodeOS.tmpdir();
  const identity = JSON.stringify({
    stateDir: NodePath.resolve(options.stateDir),
    providerInstanceId: options.providerInstanceId,
    codexHome: effectiveCodexHome(options),
  });
  const suffix = NodeCrypto.createHash("sha256").update(identity).digest("hex").slice(0, 20);
  const runtimeDirName = configuredRuntimeRoot
    ? "t3-code"
    : `t3-code-${process.getuid?.() ?? "user"}`;
  const preferredRuntimeDir = NodePath.join(runtimeRoot, runtimeDirName);
  const controlSocketName = `h-${suffix}.sock`;
  const appServerSocketName = `a-${suffix}.sock`;
  const runtimeDir =
    Buffer.byteLength(NodePath.join(preferredRuntimeDir, controlSocketName)) <=
    MAX_PORTABLE_UNIX_SOCKET_PATH_BYTES
      ? preferredRuntimeDir
      : NodePath.join("/tmp", `t3-code-${process.getuid?.() ?? "user"}`);
  const durableDir = NodePath.join(options.stateDir, "provider-hosts", `codex-${suffix}`);
  return {
    // Keep Unix-domain socket paths short enough for macOS's smaller sockaddr_un limit.
    socketPath: NodePath.join(runtimeDir, controlSocketName),
    appServerSocketPath: NodePath.join(runtimeDir, appServerSocketName),
    logPath: NodePath.join(
      options.providerLogsDir,
      `codex-provider-host-${options.providerInstanceId}-${suffix}.log`,
    ),
    lockPath: NodePath.join(runtimeDir, `h-${suffix}.startup.sqlite`),
    configPath: NodePath.join(durableDir, "config.json"),
    manifestPath: NodePath.join(durableDir, "manifest.json"),
  };
}

export function codexProviderHostGenerationFingerprint(
  options: CodexAppServerHostOptions,
): ProviderHostGenerationFingerprint {
  const identity = JSON.stringify({
    binaryPath: options.binaryPath,
    launchArgs: options.launchArgs ?? "",
    cwd: NodePath.resolve(options.cwd),
    environment: Object.entries(mergeEnvironment(options.environment, options.homePath)).sort(
      ([left], [right]) => left.localeCompare(right),
    ),
  });
  return ProviderHostGenerationFingerprint.make(
    NodeCrypto.createHash("sha256").update(identity).digest("hex"),
  );
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (cause) {
    return (cause as NodeJS.ErrnoException).code === "EPERM";
  }
}

function readProcessStartTimeMs(pid: number): Promise<number | undefined> {
  return new Promise((resolve) => {
    NodeChildProcess.execFile(
      "ps",
      ["-o", "lstart=", "-p", String(pid)],
      {
        env: {
          ...process.env,
          LANG: "C",
          LC_ALL: "C",
        },
      },
      (cause, stdout) => {
        if (cause) {
          resolve(undefined);
          return;
        }
        const startTimeMs = Date.parse(stdout.trim());
        resolve(Number.isFinite(startTimeMs) ? startTimeMs : undefined);
      },
    );
  });
}

async function isProcessIdentityCurrent(
  identity: ResourceTelemetryProcessIdentity,
): Promise<boolean> {
  if (!isProcessAlive(identity.pid)) {
    return false;
  }
  const startTimeMs = await readProcessStartTimeMs(identity.pid);
  return (
    startTimeMs !== undefined &&
    Math.abs(startTimeMs - identity.startTimeMs) <= PROCESS_START_TIME_TOLERANCE_MS
  );
}

function probeHost(socketPath: string): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = NodeNet.createConnection(socketPath);
    socket.setEncoding("utf8");
    let settled = false;
    let input = "";
    const finish = (available: boolean) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      resolve(available);
    };
    const timer = setTimeout(() => finish(false), 500);
    socket.on("data", (chunk: string) => {
      input += chunk;
      const newline = input.indexOf("\n");
      if (newline < 0) return;
      try {
        const decoded = decodeHello(input.slice(0, newline).trim());
        finish(decoded.version === PROVIDER_HOST_PROTOCOL_VERSION);
      } catch {
        finish(false);
      }
    });
    socket.once("error", () => finish(false));
    socket.once("close", () => finish(false));
  });
}

function terminateDetachedProcess(child: NodeChildProcess.ChildProcess): Promise<void> {
  return new Promise((resolve) => {
    let settled = false;
    let forceTimer: ReturnType<typeof setTimeout> | undefined;
    const hasExited = () => child.exitCode !== null || child.signalCode !== null;
    const finish = () => {
      if (settled) return;
      settled = true;
      if (forceTimer !== undefined) {
        clearTimeout(forceTimer);
        forceTimer = undefined;
      }
      child.off("exit", finish);
      resolve();
    };
    child.once("exit", finish);

    if (hasExited()) {
      finish();
      return;
    }

    forceTimer = setTimeout(() => {
      forceTimer = undefined;
      if (!hasExited()) {
        try {
          child.kill("SIGKILL");
        } catch {
          // The captured host already exited.
        }
      }
      finish();
    }, HOST_TERMINATE_TIMEOUT_MS);

    try {
      if (!child.kill("SIGTERM") && hasExited()) {
        finish();
      }
    } catch {
      finish();
    }
  });
}

function spawnDetached(
  command: HostCommand,
  logPath: string,
): Promise<CodexAppServerDetachedProcess> {
  return new Promise((resolve, reject) => {
    NodeFS.mkdirSync(NodePath.dirname(logPath), { recursive: true });
    const logFd = NodeFS.openSync(logPath, "a", 0o600);
    let child: NodeChildProcess.ChildProcess;
    try {
      child = NodeChildProcess.spawn(command.command, [...command.args], {
        cwd: command.cwd,
        detached: true,
        env: command.env,
        stdio: ["ignore", logFd, logFd],
      });
    } catch (cause) {
      NodeFS.closeSync(logFd);
      reject(cause);
      return;
    }
    const closeLog = () => {
      try {
        NodeFS.closeSync(logFd);
      } catch {
        // Spawn and error callbacks may race.
      }
    };
    child.once("error", (cause) => {
      closeLog();
      reject(cause);
    });
    child.once("spawn", () => {
      const pid = child.pid;
      closeLog();
      if (pid === undefined) {
        reject(new Error("Detached provider host did not report a process id."));
        return;
      }
      child.unref();
      resolve({
        pid,
        terminate: () => terminateDetachedProcess(child),
      });
    });
  });
}

async function waitForHost(socketPath: string): Promise<boolean> {
  const deadline = Date.now() + HOST_START_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (await probeHost(socketPath)) return true;
    await new Promise((resolve) => setTimeout(resolve, HOST_START_POLL_MS));
  }
  return false;
}

const liveOperations: CodexAppServerHostOperations = {
  isProcessIdentityCurrent,
  probeAppServer: probeCodexAppServerWebSocket,
  probeHost,
  removeSocket: (socketPath) => NodeFSP.rm(socketPath, { force: true }),
  spawnDetached,
  waitForHost,
};

class CodexAppServerHostOperationError extends Schema.TaggedErrorClass<CodexAppServerHostOperationError>()(
  "CodexAppServerHostOperationError",
  {
    cause: Schema.Defect(),
  },
) {}

const fromPromise = <A>(evaluate: () => Promise<A>) =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) => new CodexAppServerHostOperationError({ cause }),
  });

export const makeCodexAppServerHost = Effect.fn("makeCodexAppServerHost")(function* (
  options: CodexAppServerHostOptions,
  operations: CodexAppServerHostOperations = liveOperations,
) {
  const platform = yield* HostProcessPlatform;
  const executablePath = yield* HostProcessExecutablePath;
  const processArguments = yield* HostProcessArguments;
  const entryPath = processArguments[1];
  if (platform !== "linux" && platform !== "darwin") {
    return undefined;
  }
  if (!entryPath) {
    yield* Effect.logWarning("Cannot start Codex provider host without the T3 entry path.");
    return undefined;
  }

  const paths = codexAppServerHostPaths(options);
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const mutex = yield* Semaphore.make(1);
  const environment = mergeEnvironment(options.environment, options.homePath);
  const config = CodexProviderHostConfig.make({
    version: CODEX_PROVIDER_HOST_CONFIG_VERSION,
    providerInstanceId: options.providerInstanceId,
    generationFingerprint: codexProviderHostGenerationFingerprint(options),
    controlSocketPath: paths.socketPath,
    appServerSocketPath: paths.appServerSocketPath,
    manifestPath: paths.manifestPath,
    codex: {
      binaryPath: options.binaryPath,
      ...(options.launchArgs ? { launchArgs: options.launchArgs } : {}),
      ...(options.homePath ? { homePath: expandHomePath(options.homePath) } : {}),
      cwd: options.cwd,
    },
  });

  const ensure = mutex
    .withPermit(
      Effect.gen(function* () {
        if (
          yield* fromPromise(() => operations.probeHost(paths.socketPath)).pipe(
            Effect.orElseSucceed(() => false),
          )
        ) {
          return paths.socketPath;
        }

        yield* fromPromise(() =>
          NodeFSP.mkdir(NodePath.dirname(paths.socketPath), {
            recursive: true,
            mode: 0o700,
          }),
        );
        yield* fromPromise(() => NodeFSP.chmod(NodePath.dirname(paths.socketPath), 0o700));
        const startupLock = yield* fromPromise(() =>
          acquireSqliteTransactionLock(paths.lockPath, {
            timeoutMs: HOST_LOCK_TIMEOUT_MS,
          }),
        );

        return yield* Effect.gen(function* () {
          if (yield* fromPromise(() => operations.probeHost(paths.socketPath))) {
            return paths.socketPath;
          }
          if (
            yield* fromPromise(() => operations.probeAppServer(paths.appServerSocketPath)).pipe(
              Effect.orElseSucceed(() => false),
            )
          ) {
            yield* Effect.logWarning(
              "Codex app-server is still live without its provider host; preserving the independent execution.",
              {
                socketPath: paths.socketPath,
                appServerSocketPath: paths.appServerSocketPath,
                manifestPath: paths.manifestPath,
              },
            );
            return undefined;
          }
          const manifest = yield* readProviderHostManifest(paths.manifestPath);
          const manifestValue = Option.getOrUndefined(manifest);
          if (
            manifestValue &&
            (yield* fromPromise(() =>
              operations.isProcessIdentityCurrent(manifestValue.hostProcess),
            ))
          ) {
            const ready = yield* fromPromise(() => operations.waitForHost(paths.socketPath));
            return ready ? paths.socketPath : undefined;
          }

          yield* fromPromise(() => operations.removeSocket(paths.socketPath));
          yield* fromPromise(() => operations.removeSocket(paths.appServerSocketPath));
          yield* persistCodexProviderHostConfig({
            path: paths.configPath,
            config,
          });
          const detached = yield* fromPromise(() =>
            operations.spawnDetached(
              {
                command: executablePath,
                args: [entryPath, "__provider-host", "--config", paths.configPath],
                cwd: options.cwd,
                env: environment,
              },
              paths.logPath,
            ),
          );
          const ready = yield* fromPromise(() => operations.waitForHost(paths.socketPath));
          if (!ready) {
            yield* fromPromise(detached.terminate);
            yield* fromPromise(() => operations.removeSocket(paths.socketPath));
            yield* fromPromise(() => operations.removeSocket(paths.appServerSocketPath));
            yield* Effect.logWarning("Codex provider host did not become ready.", {
              socketPath: paths.socketPath,
              logPath: paths.logPath,
              pid: detached.pid,
            });
            return undefined;
          }
          return paths.socketPath;
        }).pipe(
          Effect.ensuring(fromPromise(startupLock.release).pipe(Effect.ignore)),
          Effect.catch((cause) =>
            Effect.logWarning("Failed to prepare independent Codex provider host.", {
              socketPath: paths.socketPath,
              logPath: paths.logPath,
              cause,
            }).pipe(Effect.as(undefined)),
          ),
        );
      }),
    )
    .pipe(
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(Path.Path, path),
      Effect.catch((cause) =>
        Effect.logWarning("Failed to prepare independent Codex provider host.", {
          socketPath: paths.socketPath,
          logPath: paths.logPath,
          cause,
        }).pipe(Effect.as(undefined)),
      ),
    );

  return {
    socketPath: paths.socketPath,
    appServerSocketPath: paths.appServerSocketPath,
    ensure,
  } satisfies CodexAppServerHostShape;
});

export const __testing = {
  hostStartTimeoutMs: HOST_START_TIMEOUT_MS,
  isProcessIdentityCurrent,
  liveOperations,
  processStartTimeToleranceMs: PROCESS_START_TIME_TOLERANCE_MS,
  terminateDetachedProcess,
};
