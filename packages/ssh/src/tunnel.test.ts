import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NetService from "@t3tools/shared/Net";
import * as NodeCrypto from "node:crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as Result from "effect/Result";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { SshPasswordPrompt } from "./auth.ts";
import {
  buildRemoteLaunchScript,
  buildRemotePairingScript,
  buildRemoteStopScript,
  buildRemoteT3RunnerScript,
  compareRemoteT3Versions,
  decideRemoteT3Version,
  describeReadinessCause,
  issueRemotePairingToken,
  launchOrReuseRemoteServer,
  managedRuntimeComponentPids,
  REMOTE_PICK_PORT_SCRIPT,
  REMOTE_WAIT_READY_SCRIPT,
  staleManagedRuntimePids,
  SshEnvironmentManager,
  T3_SERVER_READINESS_PATH,
  waitForHttpReady,
} from "./tunnel.ts";

const TEST_NODE_ENGINE_RANGE = "^22.16 || ^23.11 || >=24.10";

const makeSuccessfulProcess = (stdout: string) => {
  const stdoutStream = Stream.make(new TextEncoder().encode(stdout));
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(123),
    stdout: stdoutStream,
    stderr: Stream.empty,
    all: stdoutStream,
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    stdin: Sink.drain,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
    unref: Effect.succeed(Effect.void),
  });
};

const makeDelayedSuccessfulProcess = (stdout: string, delayMs: number) => {
  const process = makeSuccessfulProcess(stdout);
  return {
    ...process,
    exitCode: Effect.sleep(Duration.millis(delayMs)).pipe(
      Effect.as(ChildProcessSpawner.ExitCode(0)),
    ),
  };
};

const makeRunningProcess = (onKill: () => void) => {
  let finish: ((exitCode: ChildProcessSpawner.ExitCode) => void) | null = null;
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(123),
    stdout: Stream.empty,
    stderr: Stream.empty,
    all: Stream.empty,
    exitCode: Effect.callback<ChildProcessSpawner.ExitCode>((resume) => {
      finish = (exitCode) => resume(Effect.succeed(exitCode));
      return Effect.sync(() => {
        finish = null;
      });
    }),
    isRunning: Effect.succeed(true),
    kill: () =>
      Effect.sync(() => {
        onKill();
        finish?.(ChildProcessSpawner.ExitCode(143));
      }),
    stdin: Sink.drain,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
    unref: Effect.succeed(Effect.void),
  });
};

const hangingHttpClient = HttpClient.make(() => Effect.never);

const testNetService = NetService.NetService.of({
  canListenOnHost: () => Effect.succeed(true),
  isPortAvailableOnLoopback: () => Effect.succeed(true),
  hasListenerOnHost: () => Effect.succeed(false),
  reserveLoopbackPort: () => Effect.succeed(41_773),
  findAvailablePort: (preferred) => Effect.succeed(preferred),
});

function commandArgs(command: ChildProcess.Command): ReadonlyArray<string> {
  return command._tag === "StandardCommand" ? command.args : [];
}

const isByteStream = (
  input: ChildProcess.CommandInput,
): input is Stream.Stream<Uint8Array, PlatformError.PlatformError> => Stream.isStream(input);

const readCommandStdin = (command: ChildProcess.Command) =>
  Effect.gen(function* () {
    if (command._tag !== "StandardCommand") {
      return new Uint8Array();
    }
    const stdin = command.options.stdin;
    if (
      typeof stdin !== "object" ||
      stdin === null ||
      !("stream" in stdin) ||
      !isByteStream(stdin.stream)
    ) {
      return new Uint8Array();
    }

    const bytes: number[] = [];
    yield* Stream.runForEach(stdin.stream, (chunk) =>
      Effect.sync(() => {
        bytes.push(...chunk);
      }),
    );
    return Uint8Array.from(bytes);
  });

describe("ssh tunnel scripts", () => {
  it("builds the remote t3 runner with npx and npm fallbacks", () => {
    const script = buildRemoteT3RunnerScript({ nodeEngineRange: TEST_NODE_ENGINE_RANGE });

    assert.include(script, "T3_NODE_SCRIPT_PATH=''");
    assert.include(script, "T3_PACKAGE_SPEC='t3@latest'");
    assert.include(script, "T3_PACKAGE_CACHE_DIR=''");
    assert.include(script, 'exec t3 "$@"');
    assert.include(script, 'exec "$T3_CLI_PATH" "$@"');
    assert.include(script, "could not install 't3@latest'");
    assert.include(script, "require_installed_t3_cli npx --yes --package 't3@latest'");
    assert.include(script, "require_installed_t3_cli npm exec --yes --package 't3@latest'");
    assert.include(script, "npm produced no t3 executable");
    assert.include(script, 'prepend_path_if_dir "$HOME/.local/bin"');
    assert.isBelow(
      script.indexOf('prepend_path_if_dir "$HOME/.local/bin"'),
      script.indexOf("if command -v node >/dev/null 2>&1"),
    );
    assert.include(script, `T3_NODE_ENGINE_RANGE='${TEST_NODE_ENGINE_RANGE}'`);
    assert.include(script, "remote_node_satisfies_engine()");
    assert.include(script, "function satisfiesSemverRange");
    assert.include(script, "satisfiesSemverRange(rawVersion, range)");
    assert.include(script, 'prepend_path_if_dir "$VOLTA_HOME/bin"');
    assert.include(script, 'prepend_path_if_dir "$HOME/.asdf/shims"');
    assert.include(script, 'prepend_path_if_dir "$HOME/.local/share/mise/shims"');
    assert.include(script, 'eval "$(fnm env --shell bash)"');
    assert.include(script, "fnm use --silent-if-unchanged");
    assert.include(script, "fnm use default");
    assert.include(script, 'prepend_path_if_dir "$HOME/.nodenv/shims"');
    assert.include(script, 'NVM_DIR="$HOME/.nvm"');
    assert.include(script, "nvm use --silent default");
    assert.include(script, 'for T3_NODE_BIN in "$NVM_DIR"/versions/node/*/bin');
    assert.notInclude(script, "ensure $NVM_DIR/nvm.sh is available");
  });

  it("does not hard-code a remote node engine range", () => {
    const script = buildRemoteT3RunnerScript();

    assert.include(script, "T3_NODE_ENGINE_RANGE=''");
    assert.notInclude(script, TEST_NODE_ENGINE_RANGE);
  });

  it("does not fall back to a public or ambient CLI for an exact-build runner", () => {
    const script = buildRemoteT3RunnerScript({
      nodeEngineRange: TEST_NODE_ENGINE_RANGE,
      requireExactBuild: true,
    });

    assert.include(script, "T3_PACKAGE_SPEC=''");
    assert.include(script, "T3_REQUIRE_EXACT_BUILD=1");
    assert.include(script, 'if [ "$T3_REQUIRE_EXACT_BUILD" != "1" ] && command -v t3');
    assert.include(script, "requires its matching server build");
    assert.notInclude(script, "T3_PACKAGE_SPEC='t3@latest'");
  });

  it("shell-quotes package specs in the remote t3 runner", () => {
    const script = buildRemoteT3RunnerScript({
      packageSpec: "t3@nightly; touch /tmp/t3-owned",
    });

    assert.include(
      script,
      "require_installed_t3_cli npx --yes --package 't3@nightly; touch /tmp/t3-owned'",
    );
    assert.notInclude(script, "exec npx --yes t3@nightly; touch /tmp/t3-owned");
  });

  it("invokes an uploaded package archive through its t3 binary", () => {
    const archivePath = "/home/julius/.t3/ssh-runtime/packages/t3-test.tgz";
    const cacheDir = "/home/julius/.t3/ssh-runtime/npm-cache/t3-test";
    const script = buildRemoteT3RunnerScript({
      packageCacheDir: cacheDir,
      packageSpec: archivePath,
    });

    assert.include(script, `T3_PACKAGE_SPEC='${archivePath}'`);
    assert.include(script, `T3_PACKAGE_CACHE_DIR='${cacheDir}'`);
    assert.include(script, 'export npm_config_cache="$T3_PACKAGE_CACHE_DIR"');
    assert.include(script, 'exec npx --yes --package="$T3_PACKAGE_SPEC" -- t3 "$@"');
    assert.include(script, 'exec npm exec --yes --package="$T3_PACKAGE_SPEC" -- t3 "$@"');
    assert.notInclude(script, 'exec npx --yes "$T3_PACKAGE_SPEC" "$@"');
    assert.notInclude(script, 'exec npm exec --yes "$T3_PACKAGE_SPEC" -- "$@"');
  });

  it("isolates same-version package upgrades in digest-specific npm caches", () => {
    const oldScript = buildRemoteT3RunnerScript({
      packageCacheDir: "/home/julius/.t3/ssh-runtime/npm-cache/t3-old",
      packageSpec: "/home/julius/.t3/ssh-runtime/packages/t3-old.tgz",
      version: "0.0.31",
    });
    const newScript = buildRemoteT3RunnerScript({
      packageCacheDir: "/home/julius/.t3/ssh-runtime/npm-cache/t3-new",
      packageSpec: "/home/julius/.t3/ssh-runtime/packages/t3-new.tgz",
      version: "0.0.31",
    });

    assert.notEqual(oldScript, newScript);
    assert.include(
      oldScript,
      "T3_PACKAGE_CACHE_DIR='/home/julius/.t3/ssh-runtime/npm-cache/t3-old'",
    );
    assert.include(
      newScript,
      "T3_PACKAGE_CACHE_DIR='/home/julius/.t3/ssh-runtime/npm-cache/t3-new'",
    );
    assert.notInclude(oldScript, 'npm_config_cache="$HOME/.npm"');
    assert.notInclude(newScript, 'npm_config_cache="$HOME/.npm"');
  });

  it("builds the remote t3 runner with a node script override", () => {
    const script = buildRemoteT3RunnerScript({
      nodeScriptPath: "/Users/julius/Development/Work/codething-mvp/apps/server/dist/bin.mjs",
    });

    assert.include(
      script,
      "T3_NODE_SCRIPT_PATH='/Users/julius/Development/Work/codething-mvp/apps/server/dist/bin.mjs'",
    );
    assert.include(script, 'exec node "$T3_NODE_SCRIPT_PATH" "$@"');
  });

  it("uses the active remote t3 runner for launch and pairing scripts", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;

    assert.include(
      buildRemoteLaunchScript({ nodeEngineRange: TEST_NODE_ENGINE_RANGE }),
      '[ -n "$REMOTE_PID" ] && [ -n "$REMOTE_PORT" ] && kill -0 "$REMOTE_PID" 2>/dev/null',
    );
    assert.include(buildRemoteLaunchScript(), "RUNNER_CHANGED=1");
    assert.match(buildRemoteLaunchScript(), /RUNNER_ID='[0-9a-f]{64}'/u);
    assert.include(
      buildRemoteLaunchScript({ version: "0.0.30" }),
      "DESIRED_SERVER_VERSION='0.0.30'",
    );
    assert.include(buildRemoteLaunchScript(), "runtime.serverVersion");
    assert.include(buildRemoteLaunchScript(), 'if [ "$VERSION_DECISION" != "reuse" ]; then');
    assert.include(buildRemoteLaunchScript(), 'comparison > 0 ? "upgrade" : "reuse"');
    assert.include(buildRemoteLaunchScript(), "RUNNER_CHANGED=0");
    assert.include(buildRemoteLaunchScript(), 'rm -f "$RUNNER_NEXT"');
    const archivePath = "/home/julius/.t3/ssh-runtime/packages/t3-test.tgz";
    const cacheDir = "/home/julius/.t3/ssh-runtime/npm-cache/t3-test";
    const archiveLaunchScript = buildRemoteLaunchScript({
      localPackageArchivePath: "/tmp/t3-server.tgz",
      packageCacheDir: cacheDir,
      packageSpec: archivePath,
    });
    assert.include(archiveLaunchScript, `PACKAGE_ARCHIVE_PATH='${archivePath}'`);
    assert.include(archiveLaunchScript, `PACKAGE_CACHE_DIR='${cacheDir}'`);
    assert.include(archiveLaunchScript, "prune_inactive_package_archives()");
    assert.include(archiveLaunchScript, 'if [ "$ACTIVE_RUNTIME_RUNNER_ID" != "$RUNNER_ID" ]; then');
    assert.include(
      archiveLaunchScript,
      'for PACKAGE_CANDIDATE in "$PACKAGE_ARCHIVE_DIR"/t3-*.tgz; do',
    );
    assert.include(archiveLaunchScript, '[ "$PACKAGE_CANDIDATE" != "$PACKAGE_ARCHIVE_PATH" ]');
    assert.include(
      archiveLaunchScript,
      'for PACKAGE_CACHE_CANDIDATE in "$PACKAGE_CACHE_ROOT"/t3-*; do',
    );
    assert.include(archiveLaunchScript, '[ "$PACKAGE_CACHE_CANDIDATE" != "$PACKAGE_CACHE_DIR" ]');
    assert.isBelow(
      archiveLaunchScript.lastIndexOf("prune_inactive_package_archives"),
      archiveLaunchScript.lastIndexOf('printf \'{"remotePort"'),
    );
    assert.isBelow(
      buildRemoteLaunchScript().indexOf('if [ "$VERSION_DECISION" != "reuse" ]; then'),
      buildRemoteLaunchScript().indexOf(
        'elif [ -n "$REMOTE_PID" ] && [ -n "$REMOTE_PORT" ] && kill -0 "$REMOTE_PID"',
      ),
    );
    assert.include(
      buildRemoteLaunchScript(),
      'LAUNCH_LOCK_DIR="$HOME/.t3/ssh-launch/server-launch.lock"',
    );
    assert.include(buildRemoteLaunchScript(), "acquire_launch_lock()");
    assert.include(buildRemoteLaunchScript(), 'while ! mkdir "$LAUNCH_LOCK_DIR"');
    assert.include(buildRemoteLaunchScript(), 'kill -0 "$LOCK_OWNER"');
    assert.include(
      buildRemoteLaunchScript(),
      'if [ -z "$LOCK_OWNER" ] && [ "$WAIT_COUNT" -ge 50 ]; then',
    );
    assert.include(
      buildRemoteLaunchScript(),
      "Timed out waiting for another T3 remote server launch",
    );
    assert.isBelow(
      buildRemoteLaunchScript().indexOf("acquire_launch_lock"),
      buildRemoteLaunchScript().indexOf('DEFAULT_RUNTIME_INFO="$(resolve_default_runtime_port'),
    );
    assert.include(buildRemoteLaunchScript(), "ensure_remote_node_path()");
    assert.include(buildRemoteLaunchScript(), "if ! ensure_remote_node_path; then");
    assert.include(
      buildRemoteLaunchScript({ nodeEngineRange: TEST_NODE_ENGINE_RANGE }),
      `T3_NODE_ENGINE_RANGE='${TEST_NODE_ENGINE_RANGE}'`,
    );
    assert.include(
      buildRemoteLaunchScript({ nodeEngineRange: TEST_NODE_ENGINE_RANGE }),
      "does not satisfy required range ",
    );
    assert.include(buildRemoteLaunchScript(), 'stop_pid "$REMOTE_PID"');
    assert.include(buildRemoteLaunchScript(), 'execFileSync("ps", ["-eo", "pid=,ppid="]');
    assert.include(buildRemoteLaunchScript(), "stop_stale_managed_runtimes");
    assert.include(buildRemoteLaunchScript(), "managedRuntimeComponentPids");
    assert.include(buildRemoteLaunchScript(), "staleManagedRuntimePids");
    assert.include(buildRemoteLaunchScript(), "fs.realpathSync(`/proc/${pid}/fd/${fd}`)");
    assert.notInclude(buildRemoteLaunchScript(), "descendant_pids");
    assert.include(buildRemoteLaunchScript(), 'kill -KILL "$PID_TO_SIGNAL"');
    assert.include(buildRemoteLaunchScript(), "wait_ready");
    assert.include(buildRemoteLaunchScript(), '"$RUNNER_FILE" serve --host 127.0.0.1');
    assert.include(
      buildRemoteLaunchScript(),
      'T3CODE_SSH_STATE_KEY="$STATE_KEY" T3CODE_SSH_RUNNER_ID="$RUNNER_ID"',
    );
    assert.include(buildRemoteLaunchScript(), '--base-dir "$DEFAULT_SERVER_HOME"');
    assert.notInclude(buildRemoteLaunchScript(), "server-home");
    assert.include(buildRemoteLaunchScript(), "Remote T3 server did not become ready");
    assert.include(buildRemoteLaunchScript(), 'wait_ready "60000"');
    assert.include(buildRemoteLaunchScript(), 'if [ -s "$LOG_FILE" ]; then');
    assert.include(buildRemoteLaunchScript(), "It wrote nothing to %s");
    assert.include(buildRemoteLaunchScript({ packageSpec: "t3@nightly" }), "t3@nightly");
    assert.include(
      buildRemotePairingScript(target),
      '"$RUNNER_FILE" auth pairing create --base-dir "$PAIRING_BASE_DIR" --json',
    );
    assert.include(buildRemotePairingScript(target), 'PAIRING_BASE_DIR="$DEFAULT_SERVER_HOME"');
    assert.notInclude(buildRemotePairingScript(target), "server-home");
    assert.include(buildRemotePairingScript(target), 'if [ ! -x "$RUNNER_FILE" ]; then');
    assert.notInclude(buildRemotePairingScript(target), "cat >");
    assert.include(
      buildRemoteStopScript(target),
      'if [ "$REMOTE_MANAGED" != "external" ] && [ -n "$REMOTE_PID" ]',
    );
    assert.include(buildRemoteStopScript(target), 'kill "$PID_TO_SIGNAL" 2>/dev/null || true');
    assert.include(buildRemoteStopScript(target), 'execFileSync("ps", ["-eo", "pid=,ppid="]');
    assert.include(buildRemoteStopScript(target), "managedRuntimeComponentPids");
    assert.notInclude(buildRemoteStopScript(target), "descendant_pids");
    assert.include(buildRemoteStopScript(target), 'kill -KILL "$PID_TO_SIGNAL"');
    assert.include(buildRemoteStopScript(target), 'rm -f "$PID_FILE" "$PORT_FILE" "$MANAGED_FILE"');
    assert.include(
      buildRemoteLaunchScript(),
      'DEFAULT_RUNTIME_FILE="$DEFAULT_SERVER_HOME/userdata/server-runtime.json"',
    );
    assert.include(buildRemoteLaunchScript(), "resolve_default_runtime_port()");
    assert.include(
      buildRemoteLaunchScript(),
      'DEFAULT_RUNTIME_INFO="$(resolve_default_runtime_port',
    );
    assert.include(
      buildRemoteLaunchScript(),
      "if (!Number.isInteger(pid) || pid <= 0 || !Number.isInteger(port))",
    );
    assert.include(buildRemoteLaunchScript(), "runtime.sshLaunch.stateKey");
    assert.include(buildRemoteLaunchScript(), "runtime.sshLaunch.runnerId");
    assert.include(buildRemoteLaunchScript(), "is_legacy_managed_runtime()");
    assert.include(buildRemoteLaunchScript(), "fs.realpathSync(`/proc/${pid}/fd/${fd}`)");
    assert.include(buildRemoteLaunchScript(), 'elif [ "$LEGACY_MANAGED" -eq 1 ]; then');
    assert.include(buildRemoteLaunchScript(), 'if [ -n "$DEFAULT_RUNTIME_STATE_KEY" ]; then');
    assert.include(
      buildRemoteLaunchScript(),
      'if [ "$DEFAULT_RUNTIME_RUNNER_ID" != "$RUNNER_ID" ]; then',
    );
    assert.include(buildRemoteLaunchScript(), 'stop_pid "$DEFAULT_RUNTIME_PID"');
    assert.include(buildRemoteLaunchScript(), 'REMOTE_PORT="$DEFAULT_REMOTE_PORT"');
    assert.include(buildRemoteLaunchScript(), "printf 'external\\n' >\"$MANAGED_FILE\"");
    assert.include(buildRemoteLaunchScript(), 'if [ -z "$REMOTE_PORT" ]; then');
    assert.isBelow(
      buildRemoteLaunchScript().indexOf('if [ -n "$DEFAULT_RUNTIME_STATE_KEY" ]; then'),
      buildRemoteLaunchScript().indexOf("printf 'external\\n' >\"$MANAGED_FILE\""),
    );
    assert.isBelow(
      buildRemoteLaunchScript().indexOf('DEFAULT_RUNTIME_INFO="$(resolve_default_runtime_port'),
      buildRemoteLaunchScript().indexOf('elif [ -n "$REMOTE_PID" ]'),
    );
  });

  it("orders remote T3 versions monotonically", () => {
    assert.equal(compareRemoteT3Versions("0.0.30", "0.0.29"), 1);
    assert.equal(compareRemoteT3Versions("0.0.29", "0.0.30"), -1);
    assert.equal(compareRemoteT3Versions("1.2.3", "1.2.3"), 0);
    assert.equal(compareRemoteT3Versions("1.2.3", "1.2.3-nightly.20260728.1"), 1);
    assert.equal(
      compareRemoteT3Versions("1.2.3-nightly.20260728.2", "1.2.3-nightly.20260728.1"),
      1,
    );
    assert.equal(compareRemoteT3Versions("", "1.2.3"), null);
    assert.equal(compareRemoteT3Versions("nightly", "1.2.3"), null);
  });

  it("isolates managed server components without selecting provider descendants", () => {
    const processes = [
      { pid: 100, parentPid: 1, ownsLog: true },
      { pid: 101, parentPid: 100, ownsLog: true },
      { pid: 102, parentPid: 101, ownsLog: false },
      { pid: 200, parentPid: 1, ownsLog: true },
      { pid: 201, parentPid: 200, ownsLog: true },
    ];

    assert.deepEqual(managedRuntimeComponentPids(processes, 101), [100, 101]);
    assert.deepEqual(staleManagedRuntimePids(processes, 101), [200, 201]);
    assert.notInclude(managedRuntimeComponentPids(processes, 101), 102);
  });

  it("selects every managed component when no runtime is active", () => {
    const processes = [
      { pid: 100, parentPid: 1, ownsLog: true },
      { pid: 101, parentPid: 100, ownsLog: true },
      { pid: 102, parentPid: 101, ownsLog: false },
      { pid: 200, parentPid: 1, ownsLog: true },
    ];

    assert.deepEqual(staleManagedRuntimePids(processes), [100, 101, 200]);
    assert.deepEqual(managedRuntimeComponentPids(processes, 102), []);
  });

  it("never replaces a healthy runtime with an equal or older packaged build", () => {
    assert.equal(decideRemoteT3Version("0.0.32", "0.0.31"), "upgrade");
    assert.equal(decideRemoteT3Version("0.0.32", "0.0.32"), "reuse");
    assert.equal(decideRemoteT3Version("0.0.31", "0.0.32"), "reuse");
    assert.equal(
      decideRemoteT3Version("0.0.32-bigfin.1785860000", "0.0.32-bigfin.1785850000"),
      "upgrade",
    );
    assert.equal(
      decideRemoteT3Version("0.0.32-bigfin.1785850000", "0.0.32-bigfin.1785860000"),
      "reuse",
    );
    assert.equal(decideRemoteT3Version("", "0.0.32"), "unknown");
  });

  it.effect("accepts launch JSON after remote shell startup noise", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const spawnedCommands: Array<ReadonlyArray<string>> = [];
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.sync(() => {
        spawnedCommands.push(commandArgs(command));
        return makeSuccessfulProcess('loaded nvm default\n{"remotePort":3774}\n');
      }),
    );
    const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const processLayer = Layer.merge(NodeServices.layer, spawnerLayer);

    return Effect.gen(function* () {
      const result = yield* launchOrReuseRemoteServer(target);
      assert.equal(result.remotePort, 3774);
      assert.deepEqual(spawnedCommands[0]?.slice(-5, -1), ["sh", "-l", "-s", "--"]);
    }).pipe(Effect.provide(processLayer));
  });

  it.effect("uploads the matching remote package once and pairs with the active runner", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const archiveBytes = Uint8Array.from([0, 255, 1, 128, 10, 13]);
    const digest = NodeCrypto.createHash("sha256").update(archiveBytes).digest("hex");
    const remoteArchivePath = `/home/julius/.t3/ssh-runtime/packages/t3-${digest}.tgz`;
    const remoteCacheDir = `/home/julius/.t3/ssh-runtime/npm-cache/t3-${digest}`;
    const uploadedBytes: number[] = [];
    const runnerScripts: string[] = [];
    let packageAvailable = false;
    let uploadCount = 0;

    const spawner = ChildProcessSpawner.make((command) =>
      Effect.gen(function* () {
        const args = commandArgs(command);
        const stdin = yield* readCommandStdin(command);
        const remoteScriptArg = args.at(-1) ?? "";

        if (args.includes("-c")) {
          if (remoteScriptArg.includes('cat >"$PACKAGE_NEXT"')) {
            uploadCount += 1;
            uploadedBytes.push(...stdin);
            packageAvailable = true;
            return makeSuccessfulProcess(`${remoteArchivePath}\n`);
          }
          return makeSuccessfulProcess(packageAvailable ? `${remoteArchivePath}\n` : "\n");
        }

        const runnerScript = new TextDecoder().decode(stdin);
        runnerScripts.push(runnerScript);
        if (args.includes("--")) {
          return makeSuccessfulProcess('{"remotePort":3774,"remoteServerKind":"managed"}\n');
        }
        return makeSuccessfulProcess(`{
  "id": "88941235-6ed5-4184-a2ff-5339e2075958",
  "credential": "LCL4R2TPHDKQ",
  "scopes": ["orchestration:read"],
  "expiresAt": "2026-04-29T01:01:20.994Z"
}
`);
      }),
    );
    const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const processLayer = Layer.merge(NodeServices.layer, spawnerLayer);

    return Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ssh-package-test-" });
      const archivePath = `${directory}/t3-server.tgz`;
      yield* fs.writeFile(archivePath, archiveBytes);
      const runner = {
        localPackageArchivePath: archivePath,
        packageSpec: "t3@stale",
        nodeEngineRange: TEST_NODE_ENGINE_RANGE,
        requireExactBuild: true,
      } as const;

      const first = yield* launchOrReuseRemoteServer(target, undefined, runner);
      const second = yield* launchOrReuseRemoteServer(target, undefined, runner);
      const pairing = yield* issueRemotePairingToken(target);

      assert.equal(first.remotePort, 3774);
      assert.equal(second.remotePort, 3774);
      assert.equal(pairing.credential, "LCL4R2TPHDKQ");
      assert.equal(uploadCount, 1);
      assert.deepEqual(uploadedBytes, Array.from(archiveBytes));
      assert.lengthOf(runnerScripts, 3);
      for (const script of runnerScripts.slice(0, 2)) {
        assert.include(script, `T3_PACKAGE_SPEC='${remoteArchivePath}'`);
        assert.include(script, `T3_PACKAGE_CACHE_DIR='${remoteCacheDir}'`);
        assert.include(script, "T3_REQUIRE_EXACT_BUILD=1");
        assert.notInclude(script, "T3_PACKAGE_SPEC='t3@stale'");
      }
      assert.include(runnerScripts[0] ?? "", 'wait_ready "120000"');
      assert.include(
        runnerScripts[2] ?? "",
        '"$RUNNER_FILE" auth pairing create --base-dir "$PAIRING_BASE_DIR" --json',
      );
      assert.notInclude(runnerScripts[2] ?? "", "T3_PACKAGE_SPEC=");
    }).pipe(Effect.provide(processLayer), Effect.scoped);
  });

  it.effect("allows cold remote launches to exceed the default SSH command timeout", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(makeDelayedSuccessfulProcess('{"remotePort":3774}\n', 75_000)),
    );
    const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const processLayer = Layer.mergeAll(NodeServices.layer, spawnerLayer, TestClock.layer());

    return Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(launchOrReuseRemoteServer(target));
      yield* Effect.yieldNow;
      yield* TestClock.adjust(Duration.seconds(75));

      const result = yield* Fiber.join(fiber);
      assert.equal(result.remotePort, 3774);
    }).pipe(Effect.provide(processLayer));
  });

  it("allows the remote port picker to run without a state file path", () => {
    assert.include(REMOTE_PICK_PORT_SCRIPT, 'const filePath = process.argv[2] ?? "";');
  });

  it("probes the API descriptor instead of the optional static frontend", () => {
    assert.include(REMOTE_WAIT_READY_SCRIPT, `path: ${JSON.stringify(T3_SERVER_READINESS_PATH)}`);
    assert.notInclude(REMOTE_WAIT_READY_SCRIPT, 'path: "/"');
  });

  it.effect("bounds each HTTP readiness probe so retries cannot hang on one request", () =>
    Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(
        Effect.result(
          waitForHttpReady({
            baseUrl: "http://127.0.0.1:41773/",
            timeoutMs: 1_000,
            intervalMs: 100,
            probeTimeoutMs: 250,
          }),
        ),
      );
      yield* Effect.yieldNow;
      yield* TestClock.adjust(Duration.millis(1_000));

      const result = yield* Fiber.join(fiber);

      assert.isTrue(Result.isFailure(result));
      if (Result.isFailure(result)) {
        assert.include(result.failure.message, "Timed out waiting 1000ms");
      }
    }).pipe(
      Effect.provide(
        Layer.merge(TestClock.layer(), Layer.succeed(HttpClient.HttpClient, hangingHttpClient)),
      ),
    ),
  );

  it("preserves primitive readiness reason values in diagnostic output", () => {
    assert.deepEqual(
      describeReadinessCause({
        _tag: "HttpClientError",
        message: "Backend readiness probe failed.",
        reason: "authentication failed",
        cause: "upstream closed",
      }),
      {
        _tag: "HttpClientError",
        message: "Backend readiness probe failed.",
        reason: "authentication failed",
        cause: "upstream closed",
      },
    );
  });

  it("steals stale remote launch locks atomically", () => {
    const script = buildRemoteLaunchScript();
    assert.include(script, 'mv "$LAUNCH_LOCK_DIR" "$LAUNCH_LOCK_DIR.stale.$$"');
    // The old two-step rm/rmdir steals could delete a freshly acquired
    // launch lock; the only remaining rmdir is the holder's own cleanup.
    assert.equal(script.split('rmdir "$LAUNCH_LOCK_DIR"').length - 1, 1);
  });

  it.effect("accepts pretty-printed pairing JSON from the remote CLI", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(
        makeSuccessfulProcess(`{
  "id": "88941235-6ed5-4184-a2ff-5339e2075958",
  "credential": "LCL4R2TPHDKQ",
  "scopes": ["orchestration:read"],
  "expiresAt": "2026-04-29T01:01:20.994Z"
}

`),
      ),
    );
    const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const processLayer = Layer.merge(NodeServices.layer, spawnerLayer);
    return Effect.gen(function* () {
      const result = yield* issueRemotePairingToken(target);
      assert.equal(result.credential, "LCL4R2TPHDKQ");
    }).pipe(Effect.provide(processLayer));
  });

  it.effect("accepts pretty-printed pairing JSON after remote shell startup noise", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(
        makeSuccessfulProcess(`loaded nvm default
{
  "id": "88941235-6ed5-4184-a2ff-5339e2075958",
  "credential": "LCL4R2TPHDKQ",
  "scopes": ["orchestration:read"],
  "expiresAt": "2026-04-29T01:01:20.994Z"
}

`),
      ),
    );
    const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const processLayer = Layer.merge(NodeServices.layer, spawnerLayer);
    return Effect.gen(function* () {
      const result = yield* issueRemotePairingToken(target);
      assert.equal(result.credential, "LCL4R2TPHDKQ");
    }).pipe(Effect.provide(processLayer));
  });

  it.effect("closes only the local tunnel and starts fresh after disconnect", () => {
    const spawnedCommands: Array<ReadonlyArray<string>> = [];
    const readinessPaths: string[] = [];
    let tunnelKillCount = 0;
    let stopCommandCount = 0;
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.sync(() => {
        const args = commandArgs(command);
        spawnedCommands.push(args);
        if (args.includes("-N")) {
          return makeRunningProcess(() => {
            tunnelKillCount += 1;
          });
        }
        if (args.includes("sh") && args.includes("--")) {
          return makeSuccessfulProcess('{"remotePort":3773}\n');
        }
        if (args.includes("sh")) {
          stopCommandCount += 1;
          return makeSuccessfulProcess('{"stopped":true}\n');
        }
        return makeSuccessfulProcess("\n");
      }),
    );
    const descriptorOnlyHttpClient = HttpClient.make((request) =>
      Effect.sync(() => {
        const path = new URL(request.url).pathname;
        readinessPaths.push(path);
        return HttpClientResponse.fromWeb(
          request,
          new Response("", {
            status: path === T3_SERVER_READINESS_PATH ? 200 : 404,
          }),
        );
      }),
    );
    const layer = Layer.mergeAll(
      NodeServices.layer,
      Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Layer.succeed(HttpClient.HttpClient, descriptorOnlyHttpClient),
      Layer.succeed(NetService.NetService, testNetService),
      SshPasswordPrompt.disabledLayer,
      SshEnvironmentManager.layer(),
    );
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;

    return Effect.gen(function* () {
      const manager = yield* SshEnvironmentManager;

      const first = yield* manager.ensureEnvironment(target);
      assert.equal(first.httpBaseUrl, "http://127.0.0.1:41773/");
      const firstTunnelCommand = spawnedCommands.find((args) => args.includes("-N"));
      assert.isDefined(firstTunnelCommand);
      assert.include(firstTunnelCommand, "ControlMaster=no");
      assert.include(firstTunnelCommand, "ControlPath=none");
      assert.include(firstTunnelCommand, "ControlPersist=no");

      const reused = yield* manager.ensureEnvironment(target);
      assert.equal(reused.httpBaseUrl, first.httpBaseUrl);
      assert.deepEqual(readinessPaths, [T3_SERVER_READINESS_PATH, T3_SERVER_READINESS_PATH]);

      yield* manager.disconnectEnvironment(target);
      assert.equal(tunnelKillCount, 1);
      assert.equal(stopCommandCount, 0);

      yield* manager.ensureEnvironment(target);

      assert.equal(spawnedCommands.filter((args) => args.includes("-N")).length, 2);
      assert.equal(tunnelKillCount, 1);
      assert.equal(stopCommandCount, 0);
    }).pipe(Effect.provide(layer), Effect.scoped);
  });

  it.effect("keeps the shared remote server alive when the manager scope closes", () => {
    let tunnelKillCount = 0;
    let stopCommandCount = 0;
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.sync(() => {
        const args = commandArgs(command);
        if (args.includes("-N")) {
          return makeRunningProcess(() => {
            tunnelKillCount += 1;
          });
        }
        if (args.includes("sh") && args.includes("--")) {
          return makeSuccessfulProcess('{"remotePort":3773,"serverKind":"managed"}\n');
        }
        if (args.includes("sh")) {
          stopCommandCount += 1;
          return makeSuccessfulProcess('{"stopped":true}\n');
        }
        return makeSuccessfulProcess("\n");
      }),
    );
    const descriptorOnlyHttpClient = HttpClient.make((request) =>
      Effect.succeed(HttpClientResponse.fromWeb(request, new Response("", { status: 200 }))),
    );
    const layer = Layer.mergeAll(
      NodeServices.layer,
      Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Layer.succeed(HttpClient.HttpClient, descriptorOnlyHttpClient),
      Layer.succeed(NetService.NetService, testNetService),
      SshPasswordPrompt.disabledLayer,
      SshEnvironmentManager.layer(),
    );
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;

    return Effect.gen(function* () {
      yield* Effect.gen(function* () {
        const manager = yield* SshEnvironmentManager;
        yield* manager.ensureEnvironment(target);
      }).pipe(Effect.provide(layer), Effect.scoped);

      assert.equal(tunnelKillCount, 1);
      assert.equal(stopCommandCount, 0);
    });
  });

  it.effect("keeps an existing tunnel through a brief backend stall", () => {
    const spawnedCommands: Array<ReadonlyArray<string>> = [];
    let tunnelKillCount = 0;
    let readinessRequestCount = 0;
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.sync(() => {
        const args = commandArgs(command);
        spawnedCommands.push(args);
        if (args.includes("-N")) {
          return makeRunningProcess(() => {
            tunnelKillCount += 1;
          });
        }
        if (args.includes("sh") && args.includes("--")) {
          return makeSuccessfulProcess('{"remotePort":3773}\n');
        }
        return makeSuccessfulProcess('{"stopped":true}\n');
      }),
    );
    const temporarilyUnavailableHttpClient = HttpClient.make((request) =>
      Effect.sync(() => {
        readinessRequestCount += 1;
        const status = readinessRequestCount === 1 || readinessRequestCount >= 26 ? 200 : 503;
        return HttpClientResponse.fromWeb(request, new Response("", { status }));
      }),
    );
    const layer = Layer.mergeAll(
      NodeServices.layer,
      TestClock.layer(),
      Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Layer.succeed(HttpClient.HttpClient, temporarilyUnavailableHttpClient),
      Layer.succeed(NetService.NetService, testNetService),
      SshPasswordPrompt.disabledLayer,
      SshEnvironmentManager.layer(),
    );
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;

    return Effect.gen(function* () {
      const manager = yield* SshEnvironmentManager;
      const first = yield* manager.ensureEnvironment(target);
      const secondFiber = yield* manager.ensureEnvironment(target).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* TestClock.adjust(Duration.seconds(3));
      const second = yield* Fiber.join(secondFiber);

      assert.equal(second.httpBaseUrl, first.httpBaseUrl);
      assert.equal(spawnedCommands.filter((args) => args.includes("-N")).length, 1);
      assert.equal(tunnelKillCount, 0);
    }).pipe(Effect.provide(layer), Effect.scoped);
  });

  it.effect("keeps an existing tunnel when replay traffic delays its readiness probe", () => {
    let tunnelSpawnCount = 0;
    let tunnelKillCount = 0;
    let readinessRequestCount = 0;
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.sync(() => {
        const args = commandArgs(command);
        if (args.includes("-N")) {
          tunnelSpawnCount += 1;
          return makeRunningProcess(() => {
            tunnelKillCount += 1;
          });
        }
        if (args.includes("sh") && args.includes("--")) {
          return makeSuccessfulProcess('{"remotePort":3773}\n');
        }
        return makeSuccessfulProcess('{"stopped":true}\n');
      }),
    );
    const replayCongestedHttpClient = HttpClient.make((request) =>
      Effect.gen(function* () {
        readinessRequestCount += 1;
        if (readinessRequestCount > 1 && tunnelSpawnCount === 1) {
          yield* Effect.sleep(Duration.seconds(2));
        }
        return HttpClientResponse.fromWeb(request, new Response("", { status: 200 }));
      }),
    );
    const layer = Layer.mergeAll(
      NodeServices.layer,
      TestClock.layer(),
      Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Layer.succeed(HttpClient.HttpClient, replayCongestedHttpClient),
      Layer.succeed(NetService.NetService, testNetService),
      SshPasswordPrompt.disabledLayer,
      SshEnvironmentManager.layer(),
    );
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;

    return Effect.gen(function* () {
      const manager = yield* SshEnvironmentManager;
      const first = yield* manager.ensureEnvironment(target);
      const secondFiber = yield* manager.ensureEnvironment(target).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* TestClock.adjust(Duration.seconds(2));
      const second = yield* Fiber.join(secondFiber);

      assert.equal(second.httpBaseUrl, first.httpBaseUrl);
      assert.equal(tunnelSpawnCount, 1);
      assert.equal(tunnelKillCount, 0);
    }).pipe(Effect.provide(layer), Effect.scoped);
  });

  it.effect("rotates a stale local tunnel without stopping the reusable remote server", () => {
    const spawnedCommands: Array<ReadonlyArray<string>> = [];
    let tunnelSpawnCount = 0;
    let tunnelKillCount = 0;
    let stopCommandCount = 0;
    let readinessRequestCount = 0;
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.sync(() => {
        const args = commandArgs(command);
        spawnedCommands.push(args);
        if (args.includes("-N")) {
          tunnelSpawnCount += 1;
          return makeRunningProcess(() => {
            tunnelKillCount += 1;
          });
        }
        if (args.includes("sh") && args.includes("--")) {
          return makeSuccessfulProcess('{"remotePort":3773,"serverKind":"managed"}\n');
        }
        if (args.includes("sh")) {
          stopCommandCount += 1;
          return makeSuccessfulProcess('{"stopped":true}\n');
        }
        return makeSuccessfulProcess("\n");
      }),
    );
    const staleTunnelHttpClient = HttpClient.make((request) =>
      Effect.sync(() => {
        readinessRequestCount += 1;
        const status = readinessRequestCount === 1 || tunnelSpawnCount >= 2 ? 200 : 503;
        return HttpClientResponse.fromWeb(request, new Response("", { status }));
      }),
    );
    const layer = Layer.mergeAll(
      NodeServices.layer,
      TestClock.layer(),
      Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Layer.succeed(HttpClient.HttpClient, staleTunnelHttpClient),
      Layer.succeed(NetService.NetService, testNetService),
      SshPasswordPrompt.disabledLayer,
      SshEnvironmentManager.layer(),
    );
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;

    return Effect.gen(function* () {
      const manager = yield* SshEnvironmentManager;
      const first = yield* manager.ensureEnvironment(target);
      const secondFiber = yield* manager.ensureEnvironment(target).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* TestClock.adjust(Duration.seconds(16));
      const second = yield* Fiber.join(secondFiber);

      assert.equal(second.httpBaseUrl, first.httpBaseUrl);
      assert.equal(spawnedCommands.filter((args) => args.includes("-N")).length, 2);
      assert.equal(tunnelKillCount, 1);
      assert.equal(stopCommandCount, 0);
    }).pipe(Effect.provide(layer), Effect.scoped);
  });
});

describe("ssh tunnel lifecycle", () => {
  it.effect("coalesces concurrent stale reconnects into one replacement tunnel", () => {
    let tunnelSpawnCount = 0;
    let launchCount = 0;
    let readinessRequestCount = 0;
    let staleProbesArmed = false;
    let arrivedStaleChecks = 0;
    let replacementReleased = false;

    const spawner = ChildProcessSpawner.make((command) =>
      Effect.sync(() => {
        const args = commandArgs(command);
        if (args.includes("-N")) {
          tunnelSpawnCount += 1;
          return makeRunningProcess(() => {});
        }
        if (args.includes("sh") && args.includes("--")) {
          launchCount += 1;
          return makeSuccessfulProcess('{"remotePort":3773,"serverKind":"managed"}\n');
        }
        return makeSuccessfulProcess("\n");
      }),
    );
    const staleDuringReconnectHttpClient = HttpClient.make((request) =>
      Effect.gen(function* () {
        readinessRequestCount += 1;
        const respond = (status: number) =>
          Effect.succeed(HttpClientResponse.fromWeb(request, new Response("", { status })));
        if (readinessRequestCount === 1 || tunnelSpawnCount >= 2) {
          return yield* respond(200);
        }
        if (!staleProbesArmed) {
          return yield* respond(503);
        }
        arrivedStaleChecks += 1;
        if (arrivedStaleChecks === 2) {
          // Both reconnecting fibers are now inside their stale-entry
          // checks; release them into the close/create race together.
          replacementReleased = true;
          return yield* respond(503);
        }
        // Hold the first fiber until the second one arrives.
        while (!replacementReleased) {
          yield* Effect.sleep(Duration.millis(100));
        }
        return yield* respond(503);
      }),
    );
    const layer = Layer.mergeAll(
      NodeServices.layer,
      TestClock.layer(),
      Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Layer.succeed(HttpClient.HttpClient, staleDuringReconnectHttpClient),
      Layer.succeed(NetService.NetService, testNetService),
      SshPasswordPrompt.disabledLayer,
      SshEnvironmentManager.layer(),
    );
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;

    return Effect.gen(function* () {
      const manager = yield* SshEnvironmentManager;
      yield* manager.ensureEnvironment(target);

      staleProbesArmed = true;
      const fiberA = yield* Effect.forkChild(manager.ensureEnvironment(target));
      const fiberB = yield* Effect.forkChild(manager.ensureEnvironment(target));

      let joined = false;
      // Keep the shared clock moving so readiness retries and held probes
      // make progress while this fiber waits on the reconnects.
      yield* Effect.forkChild(
        Effect.gen(function* () {
          while (!joined) {
            yield* TestClock.adjust(Duration.millis(100));
          }
        }),
      );

      const resultA = yield* Fiber.join(fiberA);
      const resultB = yield* Fiber.join(fiberB);
      joined = true;

      assert.equal(launchCount, 2);
      assert.equal(tunnelSpawnCount, 2);
      assert.equal(resultB.httpBaseUrl, resultA.httpBaseUrl);
    }).pipe(Effect.provide(layer), Effect.scoped);
  });
});
