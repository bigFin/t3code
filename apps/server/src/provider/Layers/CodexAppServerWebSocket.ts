// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off - WebSocket readiness uses the ws callback API.
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import * as NodeNet from "node:net";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexErrors from "effect-codex-app-server/errors";

const APP_SERVER_OPEN_TIMEOUT_MS = 3_000;
const APP_SERVER_PROBE_TIMEOUT_MS = 500;
const MAX_INBOUND_QUEUE_MESSAGES = 1_024;
const MAX_INBOUND_QUEUE_BYTES = 32 * 1024 * 1024;
const MAX_INBOUND_FRAME_BYTES = 128 * 1024 * 1024;
const decoder = new TextDecoder();
const newline = Buffer.from("\n");

export interface CodexAppServerWebSocketClose {
  readonly code: number;
  readonly reason: string;
  readonly cause?: Error;
}

export interface CodexAppServerWebSocketConnection {
  readonly client: CodexClient.CodexAppServerClient["Service"];
  readonly closed: Effect.Effect<CodexAppServerWebSocketClose>;
}

interface CodexAppServerWebSocketConnectionOptions {
  readonly maxInboundQueueMessages?: number;
  readonly maxInboundQueueBytes?: number;
  readonly maxInboundFrameBytes?: number;
}

function makeSocket(socketPath: string, maxPayload = MAX_INBOUND_FRAME_BYTES) {
  return new NodeSocket.NodeWS.WebSocket("ws://localhost/", {
    createConnection: () => NodeNet.createConnection(socketPath),
    handshakeTimeout: APP_SERVER_OPEN_TIMEOUT_MS,
    maxPayload,
    perMessageDeflate: false,
  });
}

export function probeCodexAppServerWebSocket(socketPath: string): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = makeSocket(socketPath);
    let settled = false;
    const finish = (available: boolean) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (socket.readyState === NodeSocket.NodeWS.WebSocket.OPEN) {
        socket.close();
      } else {
        socket.terminate();
      }
      resolve(available);
    };
    const timer = setTimeout(() => finish(false), APP_SERVER_PROBE_TIMEOUT_MS);
    socket.once("open", () => finish(true));
    socket.once("error", () => finish(false));
    socket.once("close", () => finish(false));
  });
}

function websocketPayloadBytes(data: NodeSocket.NodeWS.RawData): Buffer {
  if (Array.isArray(data)) {
    return Buffer.concat(data);
  }
  if (data instanceof ArrayBuffer) {
    return Buffer.from(data);
  }
  return Buffer.isBuffer(data) ? data : Buffer.from(data);
}

function isWebSocketPayloadLimitError(cause: Error): boolean {
  return "code" in cause && cause.code === "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH";
}

const makeCodexAppServerWebSocketConnectionWithOptions = Effect.fn(
  "makeCodexAppServerWebSocketConnection",
)(function* (socketPath: string, options: CodexAppServerWebSocketConnectionOptions = {}) {
  const maxInboundQueueMessages = options.maxInboundQueueMessages ?? MAX_INBOUND_QUEUE_MESSAGES;
  const maxInboundQueueBytes = options.maxInboundQueueBytes ?? MAX_INBOUND_QUEUE_BYTES;
  const maxInboundFrameBytes = options.maxInboundFrameBytes ?? MAX_INBOUND_FRAME_BYTES;
  const input = yield* Queue.bounded<Uint8Array, Cause.Done<void>>(maxInboundQueueMessages);
  const runSync = Effect.runSyncWith(yield* Effect.context<never>());
  let retainedMessages = 0;
  let retainedBytes = 0;
  let resolveClosed: (status: CodexAppServerWebSocketClose) => void = () => undefined;
  const closedPromise = new Promise<CodexAppServerWebSocketClose>((resolve) => {
    resolveClosed = resolve;
  });
  let closed = false;
  let closedResolved = false;
  let pendingCloseStatus: CodexAppServerWebSocketClose | undefined;
  const resolveClosedOnce = (status: CodexAppServerWebSocketClose) => {
    if (closedResolved) return;
    closedResolved = true;
    resolveClosed(status);
  };
  const resolveDrainedClose = () => {
    if (pendingCloseStatus) {
      resolveClosedOnce(pendingCloseStatus);
    }
  };
  const settleClosed = (status: CodexAppServerWebSocketClose, discardQueuedInput = false) => {
    if (closed) return;
    closed = true;
    pendingCloseStatus = status;
    if (discardQueuedInput) {
      runSync(Queue.clear(input));
      retainedMessages = 0;
      retainedBytes = 0;
      resolveClosedOnce(status);
    }
    Queue.endUnsafe(input);
  };
  const overflowReason = () =>
    `Codex App Server inbound queue exceeded its ` +
    `${maxInboundQueueMessages}-message or ${maxInboundQueueBytes}-byte limit.`;
  const frameOverflowReason = () =>
    `Codex App Server inbound frame exceeded its ${maxInboundFrameBytes}-byte limit.`;
  const settleOverflow = (socket: NodeSocket.NodeWS.WebSocket) => {
    const reason = overflowReason();
    const cause = new Error(reason);
    settleClosed({ code: 1009, reason, cause }, true);
    socket.terminate();
  };
  const settleFrameOverflow = (socket: NodeSocket.NodeWS.WebSocket) => {
    const reason = frameOverflowReason();
    const cause = new Error(reason);
    settleClosed({ code: 1009, reason, cause }, true);
    socket.terminate();
  };
  let removeSocketListeners = () => undefined;

  const socket = yield* Effect.acquireRelease(
    Effect.callback<NodeSocket.NodeWS.WebSocket, CodexErrors.CodexAppServerSpawnError>((resume) => {
      const socket = makeSocket(socketPath, maxInboundFrameBytes);
      let opened = false;
      const onOpen = () => {
        opened = true;
        resume(Effect.succeed(socket));
      };
      const onError = (cause: Error) => {
        if (!opened) {
          removeSocketListeners();
          socket.terminate();
          resume(
            Effect.fail(
              new CodexErrors.CodexAppServerSpawnError({
                command: `connect to detached Codex App Server at ${socketPath}`,
                cause,
              }),
            ),
          );
          return;
        }
        removeSocketListeners();
        if (isWebSocketPayloadLimitError(cause)) {
          settleFrameOverflow(socket);
          return;
        }
        settleClosed({ code: 1006, reason: cause.message, cause }, true);
        socket.terminate();
      };
      const onMessage = (data: NodeSocket.NodeWS.RawData) => {
        if (closed) return;
        const payload = websocketPayloadBytes(data);
        if (payload.byteLength > maxInboundFrameBytes) {
          removeSocketListeners();
          settleFrameOverflow(socket);
          return;
        }
        const frame = Buffer.concat([payload, newline]);
        const exceedsMessageLimit = retainedMessages >= maxInboundQueueMessages;
        // A single response may legitimately exceed the queued-backlog budget
        // (notably thread/resume for a long-running Codex session). Allow one
        // bounded frame through an empty queue, then reject additional backlog
        // until the consumer releases it.
        const exceedsByteLimit =
          retainedMessages > 0 && frame.byteLength > maxInboundQueueBytes - retainedBytes;
        if (exceedsMessageLimit || exceedsByteLimit || !Queue.offerUnsafe(input, frame)) {
          removeSocketListeners();
          settleOverflow(socket);
          return;
        }
        retainedMessages += 1;
        retainedBytes += frame.byteLength;
      };
      const onClose = (code: number, reason: Buffer) => {
        removeSocketListeners();
        settleClosed({
          code,
          reason: reason.toString(),
        });
      };
      removeSocketListeners = () => {
        socket.removeListener("open", onOpen);
        socket.removeListener("error", onError);
        socket.removeListener("message", onMessage);
        socket.removeListener("close", onClose);
      };
      socket.once("open", onOpen);
      socket.on("error", onError);
      socket.on("message", onMessage);
      socket.on("close", onClose);
      return Effect.sync(() => {
        removeSocketListeners();
        if (socket.readyState !== NodeSocket.NodeWS.WebSocket.CLOSED) {
          socket.terminate();
        }
      });
    }),
    (socket) =>
      Effect.sync(() => {
        removeSocketListeners();
        settleClosed(
          {
            code: 1000,
            reason: "T3 session connection closed",
          },
          true,
        );
        if (socket.readyState === NodeSocket.NodeWS.WebSocket.OPEN) {
          socket.close(1000, "T3 session connection closed");
        } else if (socket.readyState !== NodeSocket.NodeWS.WebSocket.CLOSED) {
          socket.terminate();
        }
      }),
  );

  const closedEffect = Effect.promise(() => closedPromise);
  const stdio = Stdio.make({
    args: Effect.succeed([]),
    stdin: Stream.unfold(undefined as Uint8Array | undefined, (previousFrame) =>
      Effect.gen(function* () {
        if (previousFrame) {
          retainedMessages = Math.max(0, retainedMessages - 1);
          retainedBytes = Math.max(0, retainedBytes - previousFrame.byteLength);
        }
        return yield* Queue.take(input).pipe(
          Effect.match({
            onFailure: () => {
              resolveDrainedClose();
              return undefined;
            },
            onSuccess: (frame) => [frame, frame] as const,
          }),
        );
      }),
    ),
    stdout: () =>
      Sink.forEach((chunk: string | Uint8Array) => {
        const text = typeof chunk === "string" ? chunk : decoder.decode(chunk);
        const frames = text.split("\n").filter((frame) => frame.trim().length > 0);
        return Effect.forEach(
          frames,
          (frame) =>
            Effect.callback<void>((resume) => {
              if (socket.readyState !== NodeSocket.NodeWS.WebSocket.OPEN) {
                removeSocketListeners();
                settleClosed(
                  {
                    code: 1006,
                    reason: "Codex App Server WebSocket is not open.",
                    cause: new Error("Codex App Server WebSocket is not open."),
                  },
                  true,
                );
                socket.terminate();
                resume(Effect.void);
                return;
              }
              socket.send(frame, (cause) => {
                if (cause) {
                  removeSocketListeners();
                  settleClosed({ code: 1006, reason: cause.message, cause }, true);
                  socket.terminate();
                }
                resume(Effect.void);
              });
            }),
          { discard: true },
        );
      }),
    stderr: () => Sink.drain,
  });
  const terminationError = closedEffect.pipe(
    Effect.map((status) =>
      status.cause
        ? new CodexErrors.CodexAppServerTransportError({
            operation: "read-input-stream",
            cause: status.cause,
          })
        : new CodexErrors.CodexAppServerInputStreamEndedError({}),
    ),
  );
  const client = yield* CodexClient.make(stdio, {}, terminationError);

  return {
    client,
    closed: closedEffect,
  } satisfies CodexAppServerWebSocketConnection;
});

export const makeCodexAppServerWebSocketConnection = (socketPath: string) =>
  makeCodexAppServerWebSocketConnectionWithOptions(socketPath);

export const __testing = {
  makeConnection: makeCodexAppServerWebSocketConnectionWithOptions,
  maxInboundFrameBytes: MAX_INBOUND_FRAME_BYTES,
  maxInboundQueueBytes: MAX_INBOUND_QUEUE_BYTES,
  maxInboundQueueMessages: MAX_INBOUND_QUEUE_MESSAGES,
};
