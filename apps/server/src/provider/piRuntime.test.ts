import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { makePiRuntime, parsePiRpcLine, PiRuntimeError, splitPiRpcLines } from "./piRuntime.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const decodeRpcRequest = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      id: Schema.String,
      type: Schema.String,
    }),
  ),
);
const encodeUnknownJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

function makeRpcHandle(input: {
  readonly stdin: ChildProcessSpawner.ChildProcessHandle["stdin"];
  readonly stdout: Stream.Stream<Uint8Array>;
  readonly stderr?: Stream.Stream<Uint8Array>;
  readonly exitCode: Effect.Effect<ChildProcessSpawner.ExitCode>;
  readonly kill?: () => Effect.Effect<void>;
}) {
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(1),
    exitCode: input.exitCode,
    isRunning: Effect.succeed(true),
    kill: input.kill ?? (() => Effect.void),
    unref: Effect.succeed(Effect.void),
    stdin: input.stdin,
    stdout: input.stdout,
    stderr: input.stderr ?? Stream.empty,
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

describe("Pi RPC framing", () => {
  it.effect("splits only on LF and accepts CRLF records", () =>
    Effect.gen(function* () {
      const framed = splitPiRpcLines(
        "",
        `{"type":"message_update","text":"alpha\u2028beta"}\r\n{"type":"agent_settled"`,
      );

      expect(framed.lines).toEqual([`{"type":"message_update","text":"alpha\u2028beta"}`]);
      expect(framed.remainder).toBe('{"type":"agent_settled"');

      const event = yield* parsePiRpcLine(framed.lines[0]!);
      expect(event).toEqual({
        type: "message_update",
        text: "alpha\u2028beta",
      });
    }),
  );

  it.effect("rejects invalid response envelopes", () =>
    Effect.gen(function* () {
      const error = yield* parsePiRpcLine(
        '{"type":"response","command":"get_state","success":"yes"}',
      ).pipe(Effect.flip);

      expect(error).toBeInstanceOf(PiRuntimeError);
      expect(error.operation).toBe("decode-response");
    }),
  );
});

describe("Pi RPC transport", () => {
  it.effect("correlates out-of-order responses by request id", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const stdout = yield* Queue.unbounded<Uint8Array>();
        const exitCode = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
        const received: Array<{ readonly id: string; readonly type: string }> = [];
        let spawnedArgs: ReadonlyArray<string> = [];

        const handle = makeRpcHandle({
          stdin: Sink.forEach((chunk: Uint8Array) =>
            Effect.gen(function* () {
              const request = decodeRpcRequest(decoder.decode(chunk));
              received.push(request);
              if (received.length === 2) {
                const [first, second] = received;
                yield* Queue.offer(
                  stdout,
                  encoder.encode(
                    `${encodeUnknownJson({
                      id: second!.id,
                      type: "response",
                      command: second!.type,
                      success: true,
                      data: second!.type,
                    })}\n`,
                  ),
                );
                yield* Queue.offer(
                  stdout,
                  encoder.encode(
                    `${encodeUnknownJson({
                      id: first!.id,
                      type: "response",
                      command: first!.type,
                      success: true,
                      data: first!.type,
                    })}\n`,
                  ),
                );
              }
            }),
          ),
          stdout: Stream.fromQueue(stdout),
          exitCode: Deferred.await(exitCode),
          kill: () =>
            Deferred.succeed(exitCode, ChildProcessSpawner.ExitCode(0)).pipe(Effect.asVoid),
        });
        const spawner = ChildProcessSpawner.make((command) => {
          spawnedArgs = (command as { readonly args: ReadonlyArray<string> }).args;
          return Effect.succeed(handle);
        });
        const runtime = yield* makePiRuntime().pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        );
        const rpc = yield* runtime.startRpc({
          binaryPath: "pi",
          cwd: process.cwd(),
        });

        const [firstResponse, secondResponse] = yield* Effect.all(
          [rpc.request({ type: "first" }), rpc.request({ type: "second" })],
          { concurrency: "unbounded" },
        );

        expect(firstResponse.data).toBe("first");
        expect(secondResponse.data).toBe("second");
        expect(received.map((request) => request.id)).toEqual(["t3-pi-1", "t3-pi-2"]);
        expect(spawnedArgs).toEqual(["--mode", "rpc", "--approve"]);
        yield* rpc.stop;
      }),
    ),
  );

  it.effect("fails pending requests when the Pi process exits", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const requestWritten = yield* Deferred.make<void>();
        const exitCode = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
        const handle = makeRpcHandle({
          stdin: Sink.forEach(() =>
            Deferred.succeed(requestWritten, undefined).pipe(Effect.asVoid),
          ),
          stdout: Stream.never,
          stderr: Stream.encodeText(Stream.make("fatal rpc failure\n")),
          exitCode: Deferred.await(exitCode),
        });
        const spawner = ChildProcessSpawner.make(() => Effect.succeed(handle));
        const runtime = yield* makePiRuntime().pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        );
        const rpc = yield* runtime.startRpc({
          binaryPath: "pi",
          cwd: process.cwd(),
        });

        const requestFiber = yield* rpc.request({ type: "get_state" }).pipe(Effect.forkChild);
        yield* Deferred.await(requestWritten);
        yield* Deferred.succeed(exitCode, ChildProcessSpawner.ExitCode(7));
        const error = yield* Fiber.join(requestFiber).pipe(Effect.flip);

        expect(error).toBeInstanceOf(PiRuntimeError);
        if (!Schema.is(PiRuntimeError)(error)) {
          throw error;
        }
        expect(error.operation).toBe("rpc-exit");
        expect(error.detail).toContain("code 7");
      }),
    ),
  );
});
