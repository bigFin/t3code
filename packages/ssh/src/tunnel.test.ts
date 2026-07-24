import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NetService from "@t3tools/shared/Net";
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
  describeReadinessCause,
  issueRemotePairingToken,
  launchOrReuseRemoteServer,
  REMOTE_PICK_PORT_SCRIPT,
  SshEnvironmentManager,
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

const testHttpClient = HttpClient.make((request) =>
  Effect.succeed(HttpClientResponse.fromWeb(request, new Response("", { status: 200 }))),
);

const hangingHttpClient = HttpClient.make(() => Effect.never);

const testNetService = NetService.NetService.of({
  canListenOnHost: () => Effect.succeed(true),
  isPortAvailableOnLoopback: () => Effect.succeed(true),
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
    assert.include(script, 'exec t3 "$@"');
    assert.include(script, 'exec npx --yes --package="$T3_PACKAGE_SPEC" -- t3 "$@"');
    assert.include(script, 'exec npm exec --yes --package="$T3_PACKAGE_SPEC" -- t3 "$@"');
    assert.include(script, "could not install %s because node/npm/npx are unavailable");
    assert.isBelow(
      script.indexOf('if [ -n "$T3_PACKAGE_SPEC" ] && command -v npx'),
      script.indexOf("if command -v t3 >/dev/null"),
    );
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

  it("shell-quotes package specs in the remote t3 runner", () => {
    const script = buildRemoteT3RunnerScript({
      packageSpec: "t3@nightly; touch /tmp/t3-owned",
    });

    assert.include(script, "T3_PACKAGE_SPEC='t3@nightly; touch /tmp/t3-owned'");
    assert.include(script, 'exec npx --yes --package="$T3_PACKAGE_SPEC" -- t3 "$@"');
    assert.include(script, 'exec npm exec --yes --package="$T3_PACKAGE_SPEC" -- t3 "$@"');
    assert.notInclude(script, "exec npx --yes t3@nightly; touch /tmp/t3-owned");
  });

  it("invokes an uploaded package archive through its t3 binary", () => {
    const archivePath = "/home/julius/.t3/ssh-runtime/packages/t3-test.tgz";
    const script = buildRemoteT3RunnerScript({ packageSpec: archivePath });

    assert.include(script, `T3_PACKAGE_SPEC='${archivePath}'`);
    assert.include(script, 'exec npx --yes --package="$T3_PACKAGE_SPEC" -- t3 "$@"');
    assert.include(script, 'exec npm exec --yes --package="$T3_PACKAGE_SPEC" -- t3 "$@"');
    assert.notInclude(script, 'exec npx --yes "$T3_PACKAGE_SPEC" "$@"');
    assert.notInclude(script, 'exec npm exec --yes "$T3_PACKAGE_SPEC" -- "$@"');
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

  it("uses the remote t3 runner for launch and pairing scripts", () => {
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
    assert.include(buildRemoteLaunchScript(), "wait_ready");
    assert.include(buildRemoteLaunchScript(), '"$RUNNER_FILE" serve --host 127.0.0.1');
    assert.include(
      buildRemoteLaunchScript(),
      'T3CODE_SSH_STATE_KEY="$STATE_KEY" T3CODE_SSH_RUNNER_ID="$RUNNER_ID"',
    );
    assert.include(buildRemoteLaunchScript(), '--base-dir "$DEFAULT_SERVER_HOME"');
    assert.notInclude(buildRemoteLaunchScript(), "server-home");
    assert.include(buildRemoteLaunchScript(), "Remote T3 server did not become ready");
    assert.include(buildRemoteLaunchScript({ packageSpec: "t3@nightly" }), "t3@nightly");
    assert.include(
      buildRemotePairingScript(target),
      '"$RUNNER_FILE" auth pairing create --base-dir "$PAIRING_BASE_DIR" --json',
    );
    assert.include(buildRemotePairingScript(target), 'PAIRING_BASE_DIR="$DEFAULT_SERVER_HOME"');
    assert.notInclude(buildRemotePairingScript(target), "server-home");
    assert.include(buildRemotePairingScript(target, { packageSpec: "t3@nightly" }), "t3@nightly");
    assert.include(
      buildRemoteStopScript(target),
      'if [ "$REMOTE_MANAGED" != "external" ] && [ -n "$REMOTE_PID" ]',
    );
    assert.include(buildRemoteStopScript(target), 'kill "$REMOTE_PID" 2>/dev/null || true');
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
    assert.include(
      buildRemoteLaunchScript(),
      'if [ "$DEFAULT_RUNTIME_STATE_KEY" = "$STATE_KEY" ]; then',
    );
    assert.include(
      buildRemoteLaunchScript(),
      'if [ "$DEFAULT_RUNTIME_RUNNER_ID" != "$RUNNER_ID" ]; then',
    );
    assert.include(buildRemoteLaunchScript(), 'stop_pid "$DEFAULT_RUNTIME_PID"');
    assert.include(buildRemoteLaunchScript(), 'REMOTE_PORT="$DEFAULT_REMOTE_PORT"');
    assert.include(buildRemoteLaunchScript(), "printf 'external\\n' >\"$MANAGED_FILE\"");
    assert.include(buildRemoteLaunchScript(), 'if [ -z "$REMOTE_PORT" ]; then');
    assert.isBelow(
      buildRemoteLaunchScript().indexOf('if [ "$DEFAULT_RUNTIME_STATE_KEY" = "$STATE_KEY" ]; then'),
      buildRemoteLaunchScript().indexOf("printf 'external\\n' >\"$MANAGED_FILE\""),
    );
    assert.isBelow(
      buildRemoteLaunchScript().indexOf('DEFAULT_RUNTIME_INFO="$(resolve_default_runtime_port'),
      buildRemoteLaunchScript().indexOf('elif [ -n "$REMOTE_PID" ]'),
    );
  });

  it.effect("accepts launch JSON after remote shell startup noise", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(makeSuccessfulProcess('loaded nvm default\n{"remotePort":3774}\n')),
    );
    const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const processLayer = Layer.merge(NodeServices.layer, spawnerLayer);

    return Effect.gen(function* () {
      const result = yield* launchOrReuseRemoteServer(target);
      assert.equal(result.remotePort, 3774);
    }).pipe(Effect.provide(processLayer));
  });

  it.effect("uploads and reuses the matching remote package archive for launch and pairing", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const remoteArchivePath = "/home/julius/.t3/ssh-runtime/packages/t3-test.tgz";
    const archiveBytes = Uint8Array.from([0, 255, 1, 128, 10, 13]);
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
      } as const;

      const first = yield* launchOrReuseRemoteServer(target, undefined, runner);
      const second = yield* launchOrReuseRemoteServer(target, undefined, runner);
      const pairing = yield* issueRemotePairingToken(target, undefined, runner);

      assert.equal(first.remotePort, 3774);
      assert.equal(second.remotePort, 3774);
      assert.equal(pairing.credential, "LCL4R2TPHDKQ");
      assert.equal(uploadCount, 1);
      assert.deepEqual(uploadedBytes, Array.from(archiveBytes));
      assert.lengthOf(runnerScripts, 3);
      for (const script of runnerScripts) {
        assert.include(script, `T3_PACKAGE_SPEC='${remoteArchivePath}'`);
        assert.notInclude(script, "T3_PACKAGE_SPEC='t3@stale'");
      }
      assert.include(runnerScripts[0] ?? "", 'wait_ready "120000"');
    }).pipe(Effect.provide(processLayer), Effect.scoped);
  });

  it("allows the remote port picker to run without a state file path", () => {
    assert.include(REMOTE_PICK_PORT_SCRIPT, 'const filePath = process.argv[2] ?? "";');
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

  it.effect("closes the tunnel scope and starts fresh after disconnect", () => {
    const spawnedCommands: Array<ReadonlyArray<string>> = [];
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
    const layer = Layer.mergeAll(
      NodeServices.layer,
      Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Layer.succeed(HttpClient.HttpClient, testHttpClient),
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

      yield* manager.disconnectEnvironment(target);
      assert.equal(tunnelKillCount, 1);
      assert.equal(stopCommandCount, 1);

      yield* manager.ensureEnvironment(target);

      assert.equal(spawnedCommands.filter((args) => args.includes("-N")).length, 2);
      assert.equal(tunnelKillCount, 1);
    }).pipe(Effect.provide(layer), Effect.scoped);
  });
});
