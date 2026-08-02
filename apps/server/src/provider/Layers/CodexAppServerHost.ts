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
import { probeCodexAppServerWebSocket } from "./CodexAppServerWebSocket.ts";
import {
  CODEX_PROVIDER_HOST_CONFIG_VERSION,
  CodexProviderHostConfig,
  persistCodexProviderHostConfig,
} from "../host/CodexProviderHostConfig.ts";
import {
  PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
  PROVIDER_HOST_PROTOCOL_VERSION,
  ProviderHostBuildFingerprint,
  ProviderHostCompatibleHelloEnvelope,
  ProviderHostConfigurationFingerprint,
  type ProviderHostCompatibleHelloEnvelope as ProviderHostCompatibleHelloEnvelopeType,
  type ProviderHostGenerationFingerprint,
} from "../host/ProviderHostProtocol.ts";
import {
  readProviderHostManifest,
  type DecodedProviderHostManifest,
  type ProviderHostAppServerProvenance,
} from "../host/ProviderHostManifest.ts";

const HOST_START_TIMEOUT_MS = 15_000;
const HOST_START_POLL_MS = 50;
const HOST_TERMINATE_TIMEOUT_MS = 5_000;
const APP_SERVER_STALE_PROBE_ATTEMPTS = 3;
const APP_SERVER_STALE_PROBE_RETRY_MS = 100;
const MAX_PORTABLE_UNIX_SOCKET_PATH_BYTES = 96;
const PROCESS_START_TIME_TOLERANCE_MS = 2_000;
const DEVELOPMENT_SOURCE_ENTRY_EXTENSIONS = new Set([".cts", ".mts", ".ts", ".tsx"]);
const DEVELOPMENT_PROCESS_FINGERPRINT = NodeCrypto.randomBytes(16).toString("hex");

export interface CodexAppServerHostShape {
  readonly socketPath: string;
  readonly appServerSocketPath?: string;
  readonly ensure: Effect.Effect<string | undefined>;
  readonly promoteLegacyHost?: (
    input: CodexLegacyHostPromotionInput,
  ) => Effect.Effect<string | undefined>;
}

export interface CodexLegacyHostPromotionInput {
  readonly controlSocketPath: string;
  readonly generationFingerprint: string;
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

interface ProviderHostV2ExpectedIdentity {
  readonly version: typeof PROVIDER_HOST_PROTOCOL_VERSION;
  readonly providerInstanceId: ProviderInstanceId;
  readonly buildFingerprint: ProviderHostBuildFingerprint;
}

interface ProviderHostV1ExpectedIdentity {
  readonly version: typeof PROVIDER_HOST_LEGACY_PROTOCOL_VERSION;
  readonly providerInstanceId: ProviderInstanceId;
  readonly generationFingerprint: ProviderHostGenerationFingerprint;
  readonly hostProcess: ResourceTelemetryProcessIdentity;
}

type ProviderHostExpectedIdentity = ProviderHostV2ExpectedIdentity | ProviderHostV1ExpectedIdentity;

export type ProcessIdentityStatus = "current" | "stale" | "unknown";

export interface CodexAppServerDetachedProcess {
  readonly pid: number;
  readonly exited: Promise<void>;
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
    signal?: AbortSignal,
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

const decodeHello = Schema.decodeUnknownSync(
  Schema.fromJsonString(ProviderHostCompatibleHelloEnvelope),
);
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

function replacementControlSocketPath(preferredSocketPath: string): string {
  let candidate: string;
  do {
    candidate = NodePath.join(
      NodePath.dirname(preferredSocketPath),
      `h${PROVIDER_HOST_PROTOCOL_VERSION}-${NodeCrypto.randomBytes(10).toString("hex")}.sock`,
    );
  } while (candidate === preferredSocketPath);
  return candidate;
}

function replacementConfigPath(preferredConfigPath: string): string {
  return NodePath.join(
    NodePath.dirname(preferredConfigPath),
    `config-v${CODEX_PROVIDER_HOST_CONFIG_VERSION}-${NodeCrypto.randomBytes(10).toString("hex")}.json`,
  );
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
    lockPath: NodePath.join(durableDir, "startup.sqlite"),
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
  developmentProcessFingerprint = DEVELOPMENT_PROCESS_FINGERPRINT,
): Promise<ProviderHostBuildFingerprint> {
  const resolvedEntryPath = NodePath.resolve(entryPath);
  const isDevelopmentSourceEntry = DEVELOPMENT_SOURCE_ENTRY_EXTENSIONS.has(
    NodePath.extname(resolvedEntryPath),
  );
  const cacheKey = isDevelopmentSourceEntry
    ? `${resolvedEntryPath}\0${developmentProcessFingerprint}`
    : resolvedEntryPath;
  const existing = buildFingerprintByEntryPath.get(cacheKey);
  if (existing) return existing;
  const pending = NodeFSP.readFile(resolvedEntryPath)
    .then((contents) =>
      ProviderHostBuildFingerprint.make(
        NodeCrypto.createHash("sha256")
          .update(contents)
          .update(isDevelopmentSourceEntry ? `\0${developmentProcessFingerprint}` : "")
          .digest("hex"),
      ),
    )
    .catch((cause) => {
      buildFingerprintByEntryPath.delete(cacheKey);
      throw cause;
    });
  buildFingerprintByEntryPath.set(cacheKey, pending);
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
  hello: ProviderHostCompatibleHelloEnvelopeType,
  expectedIdentity: ProviderHostExpectedIdentity,
): boolean {
  if (
    hello.version !== expectedIdentity.version ||
    hello.providerInstanceId !== expectedIdentity.providerInstanceId
  ) {
    return false;
  }
  if (hello.version === PROVIDER_HOST_PROTOCOL_VERSION) {
    return (
      hello.buildFingerprint ===
      (expectedIdentity as ProviderHostV2ExpectedIdentity).buildFingerprint
    );
  }
  const legacyIdentity = expectedIdentity as ProviderHostV1ExpectedIdentity;
  return (
    hello.generationFingerprint === legacyIdentity.generationFingerprint &&
    hello.hostProcess.pid === legacyIdentity.hostProcess.pid &&
    hello.hostProcess.startTimeMs === legacyIdentity.hostProcess.startTimeMs
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
      const exited = new Promise<void>((exitResolve) => {
        if (child.exitCode !== null || child.signalCode !== null) {
          exitResolve();
          return;
        }
        child.once("exit", () => exitResolve());
      });
      child.unref();
      resolve({
        pid,
        exited,
        terminate: () => terminateDetachedProcess(child),
      });
    });
  });
}

async function waitForHost(
  socketPath: string,
  expectedIdentity: ProviderHostExpectedIdentity,
  signal?: AbortSignal,
): Promise<boolean> {
  const deadline = Date.now() + HOST_START_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (signal?.aborted) return false;
    if (await probeHost(socketPath, expectedIdentity)) return true;
    await new Promise<void>((resolve) => {
      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        signal?.removeEventListener("abort", finish);
        resolve();
      };
      const timer = setTimeout(finish, HOST_START_POLL_MS);
      signal?.addEventListener("abort", finish, { once: true });
    });
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
    version: PROVIDER_HOST_PROTOCOL_VERSION,
    providerInstanceId: options.providerInstanceId,
    buildFingerprint,
  } satisfies ProviderHostExpectedIdentity;
  const expectedLegacyHostIdentity = (
    manifest: Extract<DecodedProviderHostManifest, { readonly schemaVersion: 1 }>,
  ) =>
    ({
      version: PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
      providerInstanceId: options.providerInstanceId,
      generationFingerprint: manifest.generationFingerprint,
      hostProcess: manifest.hostProcess,
    }) satisfies ProviderHostExpectedIdentity;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const mutex = yield* Semaphore.make(1);
  const environment = mergeEnvironment(options.environment, options.homePath);
  const makeConfig = (
    controlSocketPath: string,
    appServerMode: CodexProviderHostConfig["appServerMode"],
    appServerSocketPath: string,
    expectedManifestGenerationFingerprint: ProviderHostGenerationFingerprint | undefined,
    adoptedAppServer?: ProviderHostAppServerProvenance,
  ) =>
    CodexProviderHostConfig.make({
      version: CODEX_PROVIDER_HOST_CONFIG_VERSION,
      providerInstanceId: options.providerInstanceId,
      buildFingerprint,
      configurationFingerprint: codexProviderHostConfigurationFingerprint(options),
      controlSocketPath,
      appServerSocketPath,
      startupLockPath: paths.lockPath,
      ...(expectedManifestGenerationFingerprint ? { expectedManifestGenerationFingerprint } : {}),
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

  let knownControlSocketPath: string | undefined;
  const discoverCurrentHost = Effect.fn("CodexAppServerHost.discoverCurrentHost")(function* () {
    const manifest = yield* readProviderHostManifest(paths.manifestPath);
    const manifestValue = Option.getOrUndefined(manifest);
    const legacyManifest =
      manifestValue?.schemaVersion === 1 &&
      manifestValue.protocolVersion === PROVIDER_HOST_LEGACY_PROTOCOL_VERSION
        ? manifestValue
        : undefined;
    const controlSocketPath = legacyManifest
      ? legacyManifest.socketPath
      : manifestValue?.schemaVersion === 2 && manifestValue.buildFingerprint === buildFingerprint
        ? manifestValue.controlSocketPath
        : (knownControlSocketPath ?? paths.socketPath);
    const expectedIdentity = legacyManifest
      ? expectedLegacyHostIdentity(legacyManifest)
      : expectedHostIdentity;
    const available = yield* fromPromise(() =>
      operations.probeHost(controlSocketPath, expectedIdentity),
    ).pipe(Effect.orElseSucceed(() => false));
    return {
      manifestValue,
      socketPath: available ? controlSocketPath : undefined,
    };
  });

  const launchHost = Effect.fn("CodexAppServerHost.launchHost")(function* (input: {
    readonly manifestValue: DecodedProviderHostManifest | undefined;
    readonly controlSocketPath: string;
    readonly appServerMode: CodexProviderHostConfig["appServerMode"];
    readonly appServerSocketPath: string;
    readonly adoptedAppServer: ProviderHostAppServerProvenance | undefined;
    readonly acceptLegacyRecovery: boolean;
  }) {
    const launchConfigPath = replacementConfigPath(paths.configPath);
    yield* fromPromise(() => operations.removeSocket(input.controlSocketPath));
    yield* persistCodexProviderHostConfig({
      path: launchConfigPath,
      config: makeConfig(
        input.controlSocketPath,
        input.appServerMode,
        input.appServerSocketPath,
        input.manifestValue?.generationFingerprint,
        input.adoptedAppServer,
      ),
    });
    const detached = yield* fromPromise(() =>
      operations.spawnDetached(
        {
          command: executablePath,
          args: [entryPath, "__provider-host", "--config", launchConfigPath],
          cwd: options.cwd,
          env: environment,
        },
        paths.logPath,
      ),
    ).pipe(
      Effect.tapError(() =>
        fromPromise(() => NodeFSP.rm(launchConfigPath, { force: true })).pipe(Effect.ignore),
      ),
    );
    const startupAbort = new AbortController();
    const startup = yield* fromPromise(() =>
      Promise.race([
        operations
          .waitForHost(input.controlSocketPath, expectedHostIdentity, startupAbort.signal)
          .then((ready) => ({ _tag: "ready" as const, ready })),
        detached.exited.then(() => ({ _tag: "exited" as const })),
      ]).finally(() => startupAbort.abort()),
    );
    const ready = startup._tag === "ready" && startup.ready;
    if (!ready) {
      const recovered = yield* discoverCurrentHost();
      const recoveredManifest = recovered.manifestValue;
      let recoveredSocketPath =
        recovered.socketPath &&
        (input.acceptLegacyRecovery ||
          (recoveredManifest?.schemaVersion === 2 &&
            recoveredManifest.buildFingerprint === buildFingerprint))
          ? recovered.socketPath
          : undefined;
      if (
        recoveredSocketPath === undefined &&
        recoveredManifest?.schemaVersion === 2 &&
        recoveredManifest.buildFingerprint === buildFingerprint
      ) {
        const recoveredReady = yield* fromPromise(() =>
          operations.waitForHost(recoveredManifest.controlSocketPath, expectedHostIdentity),
        );
        if (recoveredReady) {
          recoveredSocketPath = recoveredManifest.controlSocketPath;
        }
      }
      if (recoveredSocketPath) {
        if (recoveredSocketPath !== input.controlSocketPath) {
          yield* fromPromise(detached.terminate);
          yield* fromPromise(() => operations.removeSocket(input.controlSocketPath));
          yield* fromPromise(() => NodeFSP.rm(launchConfigPath, { force: true })).pipe(
            Effect.ignore,
          );
        }
        knownControlSocketPath = recoveredSocketPath;
        return recoveredSocketPath;
      }
      yield* fromPromise(detached.terminate);
      yield* fromPromise(() => operations.removeSocket(input.controlSocketPath));
      yield* fromPromise(() => NodeFSP.rm(launchConfigPath, { force: true })).pipe(Effect.ignore);
      yield* Effect.logWarning("Codex provider host did not become ready.", {
        socketPath: input.controlSocketPath,
        appServerMode: input.appServerMode,
        logPath: paths.logPath,
        pid: detached.pid,
      });
      return undefined;
    }
    knownControlSocketPath = input.controlSocketPath;
    return input.controlSocketPath;
  });

  const ensure = mutex
    .withPermit(
      Effect.gen(function* () {
        const discovered = yield* discoverCurrentHost();
        if (discovered.socketPath) {
          knownControlSocketPath = discovered.socketPath;
          return discovered.socketPath;
        }

        yield* fromPromise(() =>
          NodeFSP.mkdir(NodePath.dirname(paths.socketPath), {
            recursive: true,
            mode: 0o700,
          }),
        );
        yield* fromPromise(() => NodeFSP.chmod(NodePath.dirname(paths.socketPath), 0o700));
        return yield* Effect.gen(function* () {
          const discovered = yield* discoverCurrentHost();
          if (discovered.socketPath) {
            knownControlSocketPath = discovered.socketPath;
            return discovered.socketPath;
          }
          const manifestValue = discovered.manifestValue;
          const manifestHostProcessStatus = manifestValue
            ? yield* fromPromise(() => operations.inspectProcessIdentity(manifestValue.hostProcess))
            : "stale";
          const controlSocketPath = replacementControlSocketPath(paths.socketPath);
          if (manifestValue && manifestHostProcessStatus !== "stale") {
            const manifestControlSocketPath = readManifestControlSocketPath(manifestValue);
            if (manifestValue.schemaVersion === 1) {
              const ready =
                manifestValue.protocolVersion === PROVIDER_HOST_LEGACY_PROTOCOL_VERSION
                  ? yield* fromPromise(() =>
                      operations.waitForHost(
                        manifestControlSocketPath,
                        expectedLegacyHostIdentity(manifestValue),
                      ),
                    )
                  : false;
              if (ready) {
                yield* Effect.logInfo(
                  "Reusing a verified legacy Codex provider host without changing its app-server lifecycle.",
                  {
                    socketPath: manifestControlSocketPath,
                    hostProcess: manifestValue.hostProcess,
                    generationFingerprint: manifestValue.generationFingerprint,
                  },
                );
                return manifestControlSocketPath;
              }
              yield* Effect.logWarning(
                "A legacy Codex provider host may still terminate its app-server but could not be verified through its control protocol; preserving that generation without adopting it.",
                {
                  previousSocketPath: manifestControlSocketPath,
                  appServerSocketPath: paths.appServerSocketPath,
                  hostProcess: manifestValue.hostProcess,
                  manifestHostProcessStatus,
                },
              );
              return undefined;
            } else {
              const manifestBelongsToCurrentBuild =
                manifestValue.buildFingerprint === buildFingerprint;
              if (manifestBelongsToCurrentBuild) {
                const ready = yield* fromPromise(() =>
                  operations.waitForHost(manifestControlSocketPath, expectedHostIdentity),
                );
                if (ready) {
                  knownControlSocketPath = manifestControlSocketPath;
                  return manifestControlSocketPath;
                }
                yield* Effect.logWarning(
                  "Codex provider host remained unavailable after the startup lease; starting a replacement on a generation-unique control endpoint while preserving Codex execution.",
                  {
                    previousSocketPath: manifestControlSocketPath,
                    socketPath: controlSocketPath,
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
          const spawnedAppServerSocketPath = replacementAppServerSocketPath(
            paths.appServerSocketPath,
          );
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
          return yield* launchHost({
            manifestValue,
            controlSocketPath,
            appServerMode,
            appServerSocketPath:
              appServerMode === "attach" ? appServerSocketPath : spawnedAppServerSocketPath,
            adoptedAppServer: appServerAvailable ? appServerProvenance : undefined,
            acceptLegacyRecovery: true,
          });
        }).pipe(
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

  const promoteLegacyHost = (input: CodexLegacyHostPromotionInput) =>
    mutex
      .withPermit(
        Effect.gen(function* () {
          yield* fromPromise(() =>
            NodeFSP.mkdir(NodePath.dirname(paths.socketPath), {
              recursive: true,
              mode: 0o700,
            }),
          );
          yield* fromPromise(() => NodeFSP.chmod(NodePath.dirname(paths.socketPath), 0o700));
          const manifest = yield* readProviderHostManifest(paths.manifestPath);
          const manifestValue = Option.getOrUndefined(manifest);
          if (
            manifestValue?.schemaVersion === 2 &&
            manifestValue.buildFingerprint === buildFingerprint
          ) {
            const available = yield* fromPromise(() =>
              operations.waitForHost(manifestValue.controlSocketPath, expectedHostIdentity),
            ).pipe(Effect.orElseSucceed(() => false));
            if (available) {
              knownControlSocketPath = manifestValue.controlSocketPath;
              return manifestValue.controlSocketPath;
            }
            return undefined;
          }
          if (
            manifestValue?.schemaVersion !== 1 ||
            manifestValue.protocolVersion !== PROVIDER_HOST_LEGACY_PROTOCOL_VERSION ||
            manifestValue.socketPath !== input.controlSocketPath ||
            manifestValue.generationFingerprint !== input.generationFingerprint
          ) {
            return undefined;
          }
          const legacyAvailable = yield* fromPromise(() =>
            operations.probeHost(
              manifestValue.socketPath,
              expectedLegacyHostIdentity(manifestValue),
            ),
          ).pipe(Effect.orElseSucceed(() => false));
          if (!legacyAvailable) {
            return undefined;
          }
          const appServerProvenance = readAdoptableAppServerProvenance(
            manifestValue,
            paths.appServerSocketPath,
          );
          if (!appServerProvenance) {
            return undefined;
          }
          const appServerAvailable = yield* fromPromise(() =>
            probeAppServerForStartup(appServerProvenance.appServer.socketPath, operations),
          );
          const appServerProcessStatus = yield* fromPromise(() =>
            operations.inspectProcessIdentity(appServerProvenance.appServer.process),
          );
          if (!appServerAvailable || appServerProcessStatus !== "current") {
            yield* Effect.logWarning(
              "Cannot promote a legacy Codex provider host without a live, verified app-server.",
              {
                legacyControlSocketPath: manifestValue.socketPath,
                appServerSocketPath: appServerProvenance.appServer.socketPath,
                appServerProcessStatus,
              },
            );
            return undefined;
          }
          const controlSocketPath = replacementControlSocketPath(paths.socketPath);
          yield* Effect.logWarning(
            "Promoting a failed legacy provider-host attachment through a v2 gateway without changing Codex execution.",
            {
              legacyControlSocketPath: manifestValue.socketPath,
              controlSocketPath,
              appServerSocketPath: appServerProvenance.appServer.socketPath,
              generationFingerprint: manifestValue.generationFingerprint,
            },
          );
          return yield* launchHost({
            manifestValue,
            controlSocketPath,
            appServerMode: "attach",
            appServerSocketPath: appServerProvenance.appServer.socketPath,
            adoptedAppServer: appServerProvenance,
            acceptLegacyRecovery: false,
          });
        }),
      )
      .pipe(
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.provideService(Path.Path, path),
        Effect.catch((cause) =>
          Effect.logWarning("Failed to promote legacy Codex provider host.", {
            legacyControlSocketPath: input.controlSocketPath,
            generationFingerprint: input.generationFingerprint,
            cause,
          }).pipe(Effect.as(undefined)),
        ),
      );

  return {
    socketPath: paths.socketPath,
    appServerSocketPath: paths.appServerSocketPath,
    ensure,
    promoteLegacyHost,
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
