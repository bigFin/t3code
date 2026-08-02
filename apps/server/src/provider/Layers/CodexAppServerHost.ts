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
  ProviderHostBuildFingerprint,
  ProviderHostConfigurationFingerprint,
  ProviderHostHelloEnvelope,
} from "../host/ProviderHostProtocol.ts";
import {
  readProviderHostManifest,
  type DecodedProviderHostManifest,
  type ProviderHostAppServerProvenance,
} from "../host/ProviderHostManifest.ts";

const HOST_START_TIMEOUT_MS = 15_000;
const HOST_START_POLL_MS = 50;
const HOST_TERMINATE_TIMEOUT_MS = 2_000;
const HOST_LOCK_TIMEOUT_MS = 30_000;
const APP_SERVER_STALE_PROBE_ATTEMPTS = 3;
const APP_SERVER_STALE_PROBE_RETRY_MS = 100;
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

interface ProviderHostExpectedIdentity {
  readonly providerInstanceId: ProviderInstanceId;
  readonly buildFingerprint: ProviderHostBuildFingerprint;
}

export type ProcessIdentityStatus = "current" | "stale" | "unknown";

export interface CodexAppServerDetachedProcess {
  readonly pid: number;
  readonly terminate: () => Promise<void>;
}

export interface CodexAppServerHostOperations {
  readonly inspectProcessIdentity: (
    identity: ResourceTelemetryProcessIdentity,
  ) => Promise<ProcessIdentityStatus>;
  readonly probeAppServer: (socketPath: string) => Promise<boolean>;
  readonly probeHost: (
    socketPath: string,
    expectedIdentity: ProviderHostExpectedIdentity,
  ) => Promise<boolean>;
  readonly readBuildFingerprint: (entryPath: string) => Promise<ProviderHostBuildFingerprint>;
  readonly removeSocket: (socketPath: string) => Promise<void>;
  readonly sleep: (durationMs: number) => Promise<void>;
  readonly socketPathExists: (socketPath: string) => Promise<boolean>;
  readonly spawnDetached: (
    command: HostCommand,
    logPath: string,
  ) => Promise<CodexAppServerDetachedProcess>;
  readonly waitForHost: (
    socketPath: string,
    expectedIdentity: ProviderHostExpectedIdentity,
  ) => Promise<boolean>;
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
const buildFingerprintByEntryPath = new Map<string, Promise<ProviderHostBuildFingerprint>>();

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

function readAdoptableAppServerProvenance(
  manifest: DecodedProviderHostManifest | undefined,
  fallbackAppServerSocketPath: string,
): ProviderHostAppServerProvenance | undefined {
  if (!manifest) return undefined;
  if (manifest.schemaVersion === 2) {
    return {
      owner: manifest.codex.owner,
      appServer: manifest.codex.appServer,
    };
  }
  if (!manifest.codex.childProcess) return undefined;
  const listenArgumentIndex = manifest.codex.launchConfig.arguments.indexOf("--listen");
  const configuredListenAddress =
    listenArgumentIndex >= 0
      ? manifest.codex.launchConfig.arguments[listenArgumentIndex + 1]
      : manifest.codex.launchConfig.arguments
          .find((argument) => argument.startsWith("--listen="))
          ?.slice("--listen=".length);
  const appServerSocketPath = configuredListenAddress?.startsWith("unix://")
    ? configuredListenAddress.slice("unix://".length)
    : fallbackAppServerSocketPath;
  return {
    owner: {
      generationFingerprint: manifest.generationFingerprint,
      process: manifest.hostProcess,
    },
    appServer: {
      process: manifest.codex.childProcess,
      socketPath: appServerSocketPath,
      resolvedBinary: manifest.codex.resolvedBinary,
      version: manifest.codex.version,
      launchConfig: manifest.codex.launchConfig,
    },
  };
}

function readManifestControlSocketPath(manifest: DecodedProviderHostManifest): string {
  return manifest.schemaVersion === 2 ? manifest.controlSocketPath : manifest.socketPath;
}

function replacementAppServerSocketPath(preferredSocketPath: string): string {
  let candidate: string;
  do {
    candidate = NodePath.join(
      NodePath.dirname(preferredSocketPath),
      `a-${NodeCrypto.randomBytes(10).toString("hex")}.sock`,
    );
  } while (candidate === preferredSocketPath);
  return candidate;
}

export function codexAppServerHostPaths(
  options: CodexAppServerHostOptions,
  buildFingerprint: ProviderHostBuildFingerprint = ProviderHostBuildFingerprint.make("development"),
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
  const buildSuffix = buildFingerprint.slice(0, 12);
  const controlSocketName = `h${PROVIDER_HOST_PROTOCOL_VERSION}-${buildSuffix}-${suffix}.sock`;
  const appServerSocketName = `a-${suffix}.sock`;
  const fallbackRuntimeDir = NodePath.join("/tmp", `t3-code-${process.getuid?.() ?? "user"}`);
  const controlRuntimeDir =
    Buffer.byteLength(NodePath.join(preferredRuntimeDir, controlSocketName)) <=
    MAX_PORTABLE_UNIX_SOCKET_PATH_BYTES
      ? preferredRuntimeDir
      : fallbackRuntimeDir;
  const appServerRuntimeDir =
    Buffer.byteLength(NodePath.join(preferredRuntimeDir, appServerSocketName)) <=
    MAX_PORTABLE_UNIX_SOCKET_PATH_BYTES
      ? preferredRuntimeDir
      : fallbackRuntimeDir;
  const durableDir = NodePath.join(options.stateDir, "provider-hosts", `codex-${suffix}`);
  return {
    // Keep Unix-domain socket paths short enough for macOS's smaller sockaddr_un limit.
    socketPath: NodePath.join(controlRuntimeDir, controlSocketName),
    appServerSocketPath: NodePath.join(appServerRuntimeDir, appServerSocketName),
    logPath: NodePath.join(
      options.providerLogsDir,
      `codex-provider-host-${options.providerInstanceId}-${buildSuffix}-${suffix}.log`,
    ),
    lockPath: NodePath.join(appServerRuntimeDir, `h-${suffix}.startup.sqlite`),
    configPath: NodePath.join(
      durableDir,
      `config-v${PROVIDER_HOST_PROTOCOL_VERSION}-${buildSuffix}.json`,
    ),
    manifestPath: NodePath.join(durableDir, "manifest.json"),
  };
}

export function codexProviderHostConfigurationFingerprint(
  options: CodexAppServerHostOptions,
): ProviderHostConfigurationFingerprint {
  const identity = JSON.stringify({
    binaryPath: options.binaryPath,
    launchArgs: options.launchArgs ?? "",
    cwd: NodePath.resolve(options.cwd),
    environment: Object.entries(mergeEnvironment(options.environment, options.homePath)).sort(
      ([left], [right]) => left.localeCompare(right),
    ),
  });
  return ProviderHostConfigurationFingerprint.make(
    NodeCrypto.createHash("sha256").update(identity).digest("hex"),
  );
}

export function providerHostBuildFingerprint(
  entryPath: string,
): Promise<ProviderHostBuildFingerprint> {
  const resolvedEntryPath = NodePath.resolve(entryPath);
  const existing = buildFingerprintByEntryPath.get(resolvedEntryPath);
  if (existing) return existing;
  const pending = NodeFSP.readFile(resolvedEntryPath)
    .then((contents) =>
      ProviderHostBuildFingerprint.make(
        NodeCrypto.createHash("sha256").update(contents).digest("hex"),
      ),
    )
    .catch((cause) => {
      buildFingerprintByEntryPath.delete(resolvedEntryPath);
      throw cause;
    });
  buildFingerprintByEntryPath.set(resolvedEntryPath, pending);
  return pending;
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

async function inspectProcessIdentity(
  identity: ResourceTelemetryProcessIdentity,
): Promise<ProcessIdentityStatus> {
  if (!isProcessAlive(identity.pid)) {
    return "stale";
  }
  const startTimeMs = await readProcessStartTimeMs(identity.pid);
  if (startTimeMs === undefined) return "unknown";
  return Math.abs(startTimeMs - identity.startTimeMs) <= PROCESS_START_TIME_TOLERANCE_MS
    ? "current"
    : "stale";
}

function matchesProviderHostIdentity(
  hello: ProviderHostHelloEnvelope,
  expectedIdentity: ProviderHostExpectedIdentity,
): boolean {
  return (
    hello.providerInstanceId === expectedIdentity.providerInstanceId &&
    hello.buildFingerprint === expectedIdentity.buildFingerprint
  );
}

function probeHost(
  socketPath: string,
  expectedIdentity: ProviderHostExpectedIdentity,
): Promise<boolean> {
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
        finish(matchesProviderHostIdentity(decoded, expectedIdentity));
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

async function waitForHost(
  socketPath: string,
  expectedIdentity: ProviderHostExpectedIdentity,
): Promise<boolean> {
  const deadline = Date.now() + HOST_START_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (await probeHost(socketPath, expectedIdentity)) return true;
    await new Promise((resolve) => setTimeout(resolve, HOST_START_POLL_MS));
  }
  return false;
}

async function probeAppServerForStartup(
  socketPath: string,
  operations: CodexAppServerHostOperations,
): Promise<boolean> {
  if (await operations.probeAppServer(socketPath).catch(() => false)) {
    return true;
  }
  if (!(await operations.socketPathExists(socketPath))) {
    return false;
  }
  for (let attempt = 1; attempt < APP_SERVER_STALE_PROBE_ATTEMPTS; attempt += 1) {
    await operations.sleep(APP_SERVER_STALE_PROBE_RETRY_MS);
    if (await operations.probeAppServer(socketPath).catch(() => false)) {
      return true;
    }
  }
  return false;
}

const liveOperations: CodexAppServerHostOperations = {
  inspectProcessIdentity,
  probeAppServer: probeCodexAppServerWebSocket,
  probeHost,
  readBuildFingerprint: providerHostBuildFingerprint,
  removeSocket: (socketPath) => NodeFSP.rm(socketPath, { force: true }),
  sleep: (durationMs) => new Promise((resolve) => setTimeout(resolve, durationMs)),
  socketPathExists: (socketPath) =>
    NodeFSP.lstat(socketPath).then(
      () => true,
      (cause: NodeJS.ErrnoException) => {
        if (cause.code === "ENOENT") return false;
        throw cause;
      },
    ),
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

  const buildFingerprint = yield* fromPromise(() =>
    operations.readBuildFingerprint(entryPath),
  ).pipe(
    Effect.catch((cause) =>
      Effect.logWarning("Cannot fingerprint the T3 provider-host entry bundle.", {
        entryPath,
        cause,
      }).pipe(Effect.as(undefined)),
    ),
  );
  if (!buildFingerprint) {
    return undefined;
  }
  const paths = codexAppServerHostPaths(options, buildFingerprint);
  const expectedHostIdentity = {
    providerInstanceId: options.providerInstanceId,
    buildFingerprint,
  } satisfies ProviderHostExpectedIdentity;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const mutex = yield* Semaphore.make(1);
  const environment = mergeEnvironment(options.environment, options.homePath);
  const makeConfig = (
    appServerMode: CodexProviderHostConfig["appServerMode"],
    appServerSocketPath: string,
    adoptedAppServer?: ProviderHostAppServerProvenance,
  ) =>
    CodexProviderHostConfig.make({
      version: CODEX_PROVIDER_HOST_CONFIG_VERSION,
      providerInstanceId: options.providerInstanceId,
      buildFingerprint,
      configurationFingerprint: codexProviderHostConfigurationFingerprint(options),
      controlSocketPath: paths.socketPath,
      appServerSocketPath,
      appServerMode,
      ...(adoptedAppServer ? { adoptedAppServer } : {}),
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
          yield* fromPromise(() =>
            operations.probeHost(paths.socketPath, expectedHostIdentity),
          ).pipe(Effect.orElseSucceed(() => false))
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
          if (
            yield* fromPromise(() => operations.probeHost(paths.socketPath, expectedHostIdentity))
          ) {
            return paths.socketPath;
          }
          const manifest = yield* readProviderHostManifest(paths.manifestPath);
          const manifestValue = Option.getOrUndefined(manifest);
          const manifestHostProcessStatus = manifestValue
            ? yield* fromPromise(() => operations.inspectProcessIdentity(manifestValue.hostProcess))
            : "stale";
          if (manifestValue && manifestHostProcessStatus !== "stale") {
            const manifestControlSocketPath = readManifestControlSocketPath(manifestValue);
            if (manifestValue.schemaVersion === 1) {
              yield* Effect.logWarning(
                "A legacy Codex provider host may still own its app-server; preserving the legacy generation until it exits.",
                {
                  previousSocketPath: manifestControlSocketPath,
                  appServerSocketPath: paths.appServerSocketPath,
                  hostProcess: manifestValue.hostProcess,
                  manifestHostProcessStatus,
                },
              );
              return undefined;
            }
            const manifestBelongsToCurrentBuild =
              manifestValue.buildFingerprint === buildFingerprint;
            if (manifestBelongsToCurrentBuild && manifestControlSocketPath === paths.socketPath) {
              const ready = yield* fromPromise(() =>
                operations.waitForHost(paths.socketPath, expectedHostIdentity),
              );
              if (ready) {
                return paths.socketPath;
              }
              yield* Effect.logWarning(
                "Codex provider host process may still be live but remained unavailable after the startup lease; replacing only its control-socket endpoint while preserving Codex execution.",
                {
                  socketPath: paths.socketPath,
                  appServerSocketPath: paths.appServerSocketPath,
                  hostProcess: manifestValue.hostProcess,
                  manifestHostProcessStatus,
                },
              );
            } else {
              yield* Effect.logInfo(
                "Codex provider host belongs to another T3 build; starting the current build alongside it.",
                {
                  previousSocketPath: manifestControlSocketPath,
                  socketPath: paths.socketPath,
                  appServerSocketPath: paths.appServerSocketPath,
                  hostProcess: manifestValue.hostProcess,
                },
              );
            }
          }

          const appServerProvenance = readAdoptableAppServerProvenance(
            manifestValue,
            paths.appServerSocketPath,
          );
          const appServerSocketPath =
            appServerProvenance?.appServer.socketPath ?? paths.appServerSocketPath;
          const appServerAvailable = yield* fromPromise(() =>
            probeAppServerForStartup(appServerSocketPath, operations),
          );
          const preferredAppServerAvailable =
            appServerAvailable || appServerSocketPath === paths.appServerSocketPath
              ? appServerAvailable
              : yield* fromPromise(() =>
                  probeAppServerForStartup(paths.appServerSocketPath, operations),
                );
          const appServerProcessStatus = appServerProvenance
            ? yield* fromPromise(() =>
                operations.inspectProcessIdentity(appServerProvenance.appServer.process),
              )
            : "stale";
          if (
            appServerAvailable &&
            (!appServerProvenance || appServerProcessStatus !== "current")
          ) {
            yield* Effect.logWarning(
              "Codex app-server is live but its process provenance cannot be verified; preserving it without adopting.",
              {
                appServerSocketPath,
                appServerProcessStatus,
                manifestPath: paths.manifestPath,
              },
            );
            return undefined;
          }
          if (!appServerAvailable && appServerProcessStatus !== "stale") {
            yield* Effect.logWarning(
              "Codex app-server process is still live but its socket is unavailable; preserving it without starting a replacement.",
              {
                appServerSocketPath,
                appServerProcess: appServerProvenance?.appServer.process,
                appServerProcessStatus,
                manifestPath: paths.manifestPath,
              },
            );
            return undefined;
          }
          if (!appServerAvailable && preferredAppServerAvailable) {
            yield* Effect.logWarning(
              "Codex app-server is live at the preferred socket without matching process provenance; preserving it without adopting.",
              {
                appServerSocketPath: paths.appServerSocketPath,
                manifestPath: paths.manifestPath,
              },
            );
            return undefined;
          }
          if (
            !appServerAvailable &&
            (yield* fromPromise(() =>
              operations.probeAppServer(paths.appServerSocketPath).catch(() => false),
            ))
          ) {
            yield* Effect.logWarning(
              "Codex app-server became available during stale-socket confirmation; preserving it without starting a replacement.",
              {
                appServerSocketPath: paths.appServerSocketPath,
                manifestPath: paths.manifestPath,
              },
            );
            return undefined;
          }
          const appServerMode = appServerAvailable ? "attach" : "spawn";
          const preferredAppServerSocketOccupied =
            !appServerAvailable &&
            (yield* fromPromise(() => operations.socketPathExists(paths.appServerSocketPath)));
          const spawnedAppServerSocketPath = preferredAppServerSocketOccupied
            ? replacementAppServerSocketPath(paths.appServerSocketPath)
            : paths.appServerSocketPath;
          if (preferredAppServerSocketOccupied) {
            yield* Effect.logWarning(
              "Preserving an unresponsive Codex app-server socket and starting recovery on a distinct endpoint.",
              {
                preservedAppServerSocketPath: paths.appServerSocketPath,
                replacementAppServerSocketPath: spawnedAppServerSocketPath,
                manifestPath: paths.manifestPath,
              },
            );
          }
          yield* fromPromise(() => operations.removeSocket(paths.socketPath));
          yield* persistCodexProviderHostConfig({
            path: paths.configPath,
            config: makeConfig(
              appServerMode,
              appServerMode === "attach" ? appServerSocketPath : spawnedAppServerSocketPath,
              appServerAvailable ? appServerProvenance : undefined,
            ),
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
          const ready = yield* fromPromise(() =>
            operations.waitForHost(paths.socketPath, expectedHostIdentity),
          );
          if (!ready) {
            yield* fromPromise(detached.terminate);
            yield* fromPromise(() => operations.removeSocket(paths.socketPath));
            yield* Effect.logWarning("Codex provider host did not become ready.", {
              socketPath: paths.socketPath,
              appServerMode,
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
  appServerStaleProbeAttempts: APP_SERVER_STALE_PROBE_ATTEMPTS,
  appServerStaleProbeRetryMs: APP_SERVER_STALE_PROBE_RETRY_MS,
  hostStartTimeoutMs: HOST_START_TIMEOUT_MS,
  inspectProcessIdentity,
  liveOperations,
  matchesProviderHostIdentity,
  processStartTimeToleranceMs: PROCESS_START_TIME_TOLERANCE_MS,
  terminateDetachedProcess,
};
