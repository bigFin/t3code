import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import { collectStreamAsString } from "./providerSnapshot.ts";

const PI_RPC_REQUEST_TIMEOUT = "30 seconds";
const PI_FORCE_KILL_AFTER = "1 second";
const MAX_STDERR_CHARS = 64 * 1024;

const PiRpcJsonRecord = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown));
const PiRpcEventHeader = Schema.Struct({
  type: Schema.String,
});
const PiRpcResponseSchema = Schema.Struct({
  id: Schema.optionalKey(Schema.String),
  type: Schema.Literal("response"),
  command: Schema.String,
  success: Schema.Boolean,
  data: Schema.optionalKey(Schema.Unknown),
  error: Schema.optionalKey(Schema.String),
});

export type PiRpcResponse = typeof PiRpcResponseSchema.Type;
export type PiRpcEvent = Readonly<Record<string, unknown>> & {
  readonly type: string;
};
const isPiRpcResponse = Schema.is(PiRpcResponseSchema);

export interface PiCommandResult {
  readonly stdout: string;
  readonly stderr: string;
  readonly code: number;
}

export interface PiRpcExit {
  readonly code: number;
  readonly stderr: string;
}

export interface PiRpcClient {
  readonly request: (
    command: Readonly<Record<string, unknown>> & { readonly type: string },
  ) => Effect.Effect<PiRpcResponse, PiRuntimeError>;
  readonly notify: (
    message: Readonly<Record<string, unknown>> & { readonly type: string },
  ) => Effect.Effect<void, PiRuntimeError>;
  readonly events: Stream.Stream<PiRpcEvent>;
  readonly exit: Effect.Effect<PiRpcExit, PiRuntimeError>;
  readonly stop: Effect.Effect<void>;
}

export interface PiRuntimeShape {
  readonly runCommand: (input: {
    readonly binaryPath: string;
    readonly args: ReadonlyArray<string>;
    readonly cwd?: string | undefined;
    readonly environment?: NodeJS.ProcessEnv | undefined;
  }) => Effect.Effect<PiCommandResult, PiRuntimeError>;
  readonly startRpc: (input: {
    readonly binaryPath: string;
    readonly cwd: string;
    readonly environment?: NodeJS.ProcessEnv | undefined;
    readonly sessionDir?: string | undefined;
    readonly resumeSessionFile?: string | undefined;
    readonly noSession?: boolean | undefined;
  }) => Effect.Effect<PiRpcClient, PiRuntimeError, Scope.Scope>;
}

export class PiRuntimeError extends Schema.TaggedErrorClass<PiRuntimeError>()("PiRuntimeError", {
  operation: Schema.String,
  detail: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Pi runtime ${this.operation} failed: ${this.detail}`;
  }
}

function runtimeError(operation: string, detail: string, cause?: unknown): PiRuntimeError {
  return new PiRuntimeError({
    operation,
    detail,
    ...(cause === undefined ? {} : { cause }),
  });
}

function withPiStartupEnvironment(environment: NodeJS.ProcessEnv | undefined): NodeJS.ProcessEnv {
  return {
    ...environment,
    PI_SKIP_VERSION_CHECK: environment?.PI_SKIP_VERSION_CHECK ?? "1",
  };
}

export const parsePiRpcLine = Effect.fn("parsePiRpcLine")(function* (
  line: string,
): Effect.fn.Return<PiRpcResponse | PiRpcEvent, PiRuntimeError> {
  const record = yield* Schema.decodeUnknownEffect(PiRpcJsonRecord)(line).pipe(
    Effect.mapError((cause) =>
      runtimeError("decode-response", "Pi emitted invalid JSONL output.", cause),
    ),
  );

  if (record.type === "response") {
    return yield* Schema.decodeUnknownEffect(PiRpcResponseSchema)(record).pipe(
      Effect.mapError((cause) =>
        runtimeError("decode-response", "Pi emitted an invalid RPC response.", cause),
      ),
    );
  }

  const header = yield* Schema.decodeUnknownEffect(PiRpcEventHeader)(record).pipe(
    Effect.mapError((cause) =>
      runtimeError("decode-event", "Pi emitted an invalid RPC event.", cause),
    ),
  );
  return { ...record, type: header.type };
});

export function splitPiRpcLines(
  previousRemainder: string,
  chunk: string,
): {
  readonly lines: ReadonlyArray<string>;
  readonly remainder: string;
} {
  const fragments = `${previousRemainder}${chunk}`.split("\n");
  const remainder = fragments.pop() ?? "";
  return {
    lines: fragments.map((line) => line.replace(/\r$/u, "")).filter((line) => line.length > 0),
    remainder,
  };
}

export const makePiRuntime = Effect.fn("makePiRuntime")(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const encodeJson = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

  const runCommand: PiRuntimeShape["runCommand"] = Effect.fn("PiRuntime.runCommand")(function* (
    input,
  ) {
    const environment = withPiStartupEnvironment(input.environment);
    const spawnCommand = yield* resolveSpawnCommand(input.binaryPath, input.args, {
      env: environment,
      extendEnv: input.environment === undefined,
    }).pipe(
      Effect.mapError((cause) =>
        runtimeError("resolve-command", `Unable to resolve '${input.binaryPath}'.`, cause),
      ),
    );
    const child = yield* spawner
      .spawn(
        ChildProcess.make(spawnCommand.command, spawnCommand.args, {
          ...(input.cwd ? { cwd: input.cwd } : {}),
          env: environment,
          extendEnv: input.environment === undefined,
          shell: spawnCommand.shell,
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          runtimeError("spawn-command", `Unable to start '${input.binaryPath}'.`, cause),
        ),
      );
    const [stdout, stderr, code] = yield* Effect.all(
      [
        collectStreamAsString(child.stdout),
        collectStreamAsString(child.stderr),
        child.exitCode.pipe(Effect.map(Number)),
      ],
      { concurrency: "unbounded" },
    ).pipe(
      Effect.mapError((cause) =>
        runtimeError("collect-command", `Failed while running '${input.binaryPath}'.`, cause),
      ),
    );
    return { stdout, stderr, code };
  }, Effect.scoped);

  const startRpc: PiRuntimeShape["startRpc"] = Effect.fn("PiRuntime.startRpc")(function* (input) {
    const scope = yield* Scope.Scope;
    // RPC mode has no TUI where Pi can ask whether to trust project-local resources.
    const args = ["--mode", "rpc", "--approve"];
    if (input.sessionDir) {
      args.push("--session-dir", input.sessionDir);
    }
    if (input.resumeSessionFile) {
      args.push("--session", input.resumeSessionFile);
    }
    if (input.noSession) {
      args.push("--no-session");
    }

    const environment = withPiStartupEnvironment(input.environment);
    const spawnCommand = yield* resolveSpawnCommand(input.binaryPath, args, {
      env: environment,
      extendEnv: input.environment === undefined,
    }).pipe(
      Effect.mapError((cause) =>
        runtimeError("resolve-rpc-command", `Unable to resolve '${input.binaryPath}'.`, cause),
      ),
    );
    const child = yield* spawner
      .spawn(
        ChildProcess.make(spawnCommand.command, spawnCommand.args, {
          cwd: input.cwd,
          env: environment,
          extendEnv: input.environment === undefined,
          forceKillAfter: PI_FORCE_KILL_AFTER,
          shell: spawnCommand.shell,
          stdin: { stream: "pipe", endOnDone: false },
        }),
      )
      .pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.mapError((cause) =>
          runtimeError("spawn-rpc", `Unable to start '${input.binaryPath} --mode rpc'.`, cause),
        ),
      );

    const events = yield* Queue.unbounded<PiRpcEvent>();
    const pending = yield* Ref.make(
      new Map<string, Deferred.Deferred<PiRpcResponse, PiRuntimeError>>(),
    );
    const requestCounter = yield* Ref.make(0);
    const writeSemaphore = yield* Semaphore.make(1);
    const stdoutRemainder = yield* Ref.make("");
    const stderrText = yield* Ref.make("");
    const exitDeferred = yield* Deferred.make<PiRpcExit, PiRuntimeError>();

    const failPending = (error: PiRuntimeError) =>
      Effect.gen(function* () {
        const requests = yield* Ref.getAndSet(pending, new Map());
        yield* Effect.forEach(requests.values(), (deferred) => Deferred.fail(deferred, error), {
          discard: true,
        });
      });

    const writeMessage = Effect.fn("PiRuntime.writeMessage")(function* (
      message: Readonly<Record<string, unknown>>,
    ) {
      const encoded = yield* encodeJson(message).pipe(
        Effect.mapError((cause) =>
          runtimeError("encode-request", "Unable to encode a Pi RPC request.", cause),
        ),
      );
      yield* writeSemaphore.withPermits(1)(
        Stream.run(Stream.encodeText(Stream.make(`${encoded}\n`)), child.stdin).pipe(
          Effect.mapError((cause) =>
            runtimeError("write-request", "Unable to write to Pi RPC stdin.", cause),
          ),
        ),
      );
    });

    const processLine = Effect.fn("PiRuntime.processLine")(function* (line: string) {
      const message = yield* parsePiRpcLine(line);
      if (!isPiRpcResponse(message)) {
        yield* Queue.offer(events, message);
        return;
      }

      const id = message.id;
      if (!id) {
        return;
      }
      const deferred = (yield* Ref.get(pending)).get(id);
      if (deferred) {
        yield* Deferred.succeed(deferred, message);
      }
    });

    yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) =>
        Ref.modify(stdoutRemainder, (remainder) => {
          const framed = splitPiRpcLines(remainder, chunk);
          return [framed.lines, framed.remainder] as const;
        }).pipe(
          Effect.flatMap((lines) =>
            Effect.forEach(
              lines,
              (line) =>
                processLine(line).pipe(
                  Effect.catch((error) =>
                    failPending(error).pipe(
                      Effect.andThen(
                        Effect.logWarning("Pi RPC emitted invalid output.", {
                          operation: error.operation,
                          detail: error.detail,
                        }),
                      ),
                    ),
                  ),
                ),
              { discard: true },
            ),
          ),
        ),
      ),
      Effect.forkIn(scope),
    );

    yield* child.stderr.pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) =>
        Ref.update(stderrText, (current) => `${current}${chunk}`.slice(-MAX_STDERR_CHARS)),
      ),
      Effect.forkIn(scope),
    );

    yield* child.exitCode.pipe(
      Effect.map(Number),
      Effect.flatMap((code) =>
        Effect.gen(function* () {
          const stderr = yield* Ref.get(stderrText);
          if (code === 0) {
            yield* Deferred.succeed(exitDeferred, { code, stderr });
          } else {
            yield* Deferred.fail(
              exitDeferred,
              runtimeError(
                "rpc-exit",
                `Pi RPC exited with code ${code}.${stderr.trim() ? ` ${stderr.trim()}` : ""}`,
              ),
            );
          }
          yield* failPending(
            runtimeError(
              "rpc-exit",
              `Pi RPC exited with code ${code}.${stderr.trim() ? ` ${stderr.trim()}` : ""}`,
            ),
          );
          yield* Queue.shutdown(events);
        }),
      ),
      Effect.catch((cause) => {
        const error = runtimeError("rpc-exit", "Unable to read the Pi RPC exit status.", cause);
        return Deferred.fail(exitDeferred, error).pipe(
          Effect.andThen(failPending(error)),
          Effect.andThen(Queue.shutdown(events)),
        );
      }),
      Effect.forkIn(scope),
    );

    const request: PiRpcClient["request"] = Effect.fn("PiRuntime.request")(function* (command) {
      const sequence = yield* Ref.getAndUpdate(requestCounter, (value) => value + 1);
      const id = `t3-pi-${sequence + 1}`;
      const deferred = yield* Deferred.make<PiRpcResponse, PiRuntimeError>();
      yield* Ref.update(pending, (current) => {
        const next = new Map(current);
        next.set(id, deferred);
        return next;
      });
      return yield* writeMessage({ ...command, id }).pipe(
        Effect.andThen(
          Deferred.await(deferred).pipe(
            Effect.timeoutOrElse({
              duration: PI_RPC_REQUEST_TIMEOUT,
              orElse: () =>
                Effect.fail(
                  runtimeError(
                    "request-timeout",
                    `Timed out waiting for Pi RPC response to '${command.type}'.`,
                  ),
                ),
            }),
          ),
        ),
        Effect.ensuring(
          Ref.update(pending, (current) => {
            const next = new Map(current);
            next.delete(id);
            return next;
          }),
        ),
      );
    });

    const notify: PiRpcClient["notify"] = (message) => writeMessage(message);
    const stop = child.kill({ forceKillAfter: PI_FORCE_KILL_AFTER }).pipe(Effect.ignore);

    yield* Effect.addFinalizer(() =>
      failPending(runtimeError("rpc-stop", "Pi RPC session stopped.")).pipe(
        Effect.andThen(Queue.shutdown(events)),
      ),
    );

    return {
      request,
      notify,
      events: Stream.fromQueue(events),
      exit: Deferred.await(exitDeferred),
      stop,
    } satisfies PiRpcClient;
  });

  return { runCommand, startRpc } satisfies PiRuntimeShape;
});

export class PiRuntime extends Context.Service<PiRuntime, PiRuntimeShape>()(
  "t3/provider/piRuntime",
) {
  static readonly layer = Layer.effect(PiRuntime, makePiRuntime());
}

export const PiRuntimeLive = PiRuntime.layer;
