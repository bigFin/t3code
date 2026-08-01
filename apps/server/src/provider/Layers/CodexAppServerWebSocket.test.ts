// @effect-diagnostics nodeBuiltinImport:off
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeHttp from "node:http";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { describe } from "vite-plus/test";

import {
  __testing,
  makeCodexAppServerWebSocketConnection,
  probeCodexAppServerWebSocket,
} from "./CodexAppServerWebSocket.ts";

const listen = (server: NodeHttp.Server, socketPath: string) =>
  Effect.callback<void, Error>((resume) => {
    server.once("error", (cause) => resume(Effect.fail(cause)));
    server.listen(socketPath, () => resume(Effect.void));
    return Effect.sync(() => server.close());
  });

const closeServer = (server: NodeHttp.Server, webSocketServer: NodeSocket.NodeWS.WebSocketServer) =>
  Effect.callback<void>((resume) => {
    for (const client of webSocketServer.clients) {
      client.terminate();
    }
    webSocketServer.close(() => {
      server.close(() => resume(Effect.void));
    });
  });

describe("CodexAppServerWebSocket", () => {
  it.effect("disconnects scoped clients without stopping the shared Unix-socket host", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-websocket-"));
      const socketPath = NodePath.join(root, "app-server.sock");
      const server = NodeHttp.createServer();
      const webSocketServer = new NodeSocket.NodeWS.WebSocketServer({
        perMessageDeflate: false,
        server,
      });
      let acceptedConnections = 0;
      webSocketServer.on("connection", (socket) => {
        acceptedConnections += 1;
        socket.on("message", (data) => {
          const request = JSON.parse(data.toString()) as {
            readonly id: number;
            readonly method: string;
            readonly params?: { readonly threadId?: string };
          };
          socket.send(
            JSON.stringify({
              id: request.id,
              result:
                request.method === "thread/resume"
                  ? { thread: { id: request.params?.threadId } }
                  : { connected: true },
            }),
          );
        });
      });

      yield* Effect.acquireRelease(listen(server, socketPath), () =>
        closeServer(server, webSocketServer).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              NodeFS.rmSync(root, { recursive: true, force: true });
            }),
          ),
        ),
      );

      NodeAssert.equal(yield* Effect.promise(() => probeCodexAppServerWebSocket(socketPath)), true);

      const initialize = () =>
        Effect.scoped(
          Effect.gen(function* () {
            const connection = yield* makeCodexAppServerWebSocketConnection(socketPath);
            return yield* connection.client.raw.request("initialize", {
              clientInfo: { name: "test", version: "0" },
            });
          }),
        );
      NodeAssert.deepStrictEqual(yield* initialize(), { connected: true });

      const resumed = yield* Effect.scoped(
        Effect.gen(function* () {
          const connection = yield* makeCodexAppServerWebSocketConnection(socketPath);
          return yield* connection.client.raw.request("thread/resume", {
            threadId: "provider-thread-1",
          });
        }),
      );

      NodeAssert.deepStrictEqual(resumed, {
        thread: { id: "provider-thread-1" },
      });
      NodeAssert.equal(server.listening, true);
      NodeAssert.equal(acceptedConnections, 3);
    }),
  );

  it.effect("terminates the connection when the inbound queue exceeds its byte limit", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-websocket-"));
      const socketPath = NodePath.join(root, "app-server.sock");
      const server = NodeHttp.createServer();
      const webSocketServer = new NodeSocket.NodeWS.WebSocketServer({
        perMessageDeflate: false,
        server,
      });
      let resolvePeerClosed: (code: number) => void = () => undefined;
      const peerClosed = new Promise<number>((resolve) => {
        resolvePeerClosed = resolve;
      });
      webSocketServer.on("connection", (socket) => {
        socket.once("close", (code) => resolvePeerClosed(code));
        socket.send(
          JSON.stringify({
            method: "test/oversized-notification",
            params: { payload: "x".repeat(256) },
          }),
        );
      });

      yield* Effect.acquireRelease(listen(server, socketPath), () =>
        closeServer(server, webSocketServer).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              NodeFS.rmSync(root, { recursive: true, force: true });
            }),
          ),
        ),
      );

      const maxInboundQueueBytes = 64;
      const status = yield* Effect.scoped(
        Effect.gen(function* () {
          const connection = yield* __testing.makeConnection(socketPath, {
            maxInboundQueueBytes,
            maxInboundQueueMessages: 4,
          });
          return yield* connection.closed;
        }),
      );

      NodeAssert.equal(status.code, 1009);
      NodeAssert.equal(
        status.reason,
        `Codex App Server inbound queue exceeded its 4-message or ` +
          `${maxInboundQueueBytes}-byte limit.`,
      );
      NodeAssert.equal(status.cause?.message, status.reason);
      NodeAssert.equal(yield* Effect.promise(() => peerClosed), 1009);
      NodeAssert.equal(__testing.maxInboundQueueMessages, 1_024);
      NodeAssert.equal(__testing.maxInboundQueueBytes, 32 * 1024 * 1024);
    }),
  );

  it.effect("terminates the connection when the inbound queue exceeds its message limit", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-websocket-"));
      const socketPath = NodePath.join(root, "app-server.sock");
      const server = NodeHttp.createServer();
      const webSocketServer = new NodeSocket.NodeWS.WebSocketServer({
        perMessageDeflate: false,
        server,
      });
      webSocketServer.on("connection", (socket) => {
        for (let index = 0; index < 64; index += 1) {
          socket.send(
            JSON.stringify({
              method: "test/notification",
              params: { index },
            }),
          );
        }
      });

      yield* Effect.acquireRelease(listen(server, socketPath), () =>
        closeServer(server, webSocketServer).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              NodeFS.rmSync(root, { recursive: true, force: true });
            }),
          ),
        ),
      );

      const status = yield* Effect.scoped(
        Effect.gen(function* () {
          const connection = yield* __testing.makeConnection(socketPath, {
            maxInboundQueueBytes: 1024 * 1024,
            maxInboundQueueMessages: 1,
          });
          return yield* connection.closed;
        }),
      );

      NodeAssert.equal(status.code, 1009);
      NodeAssert.equal(
        status.reason,
        "Codex App Server inbound queue exceeded its 1-message or 1048576-byte limit.",
      );
    }),
  );

  it.effect("reports normal closure only after queued notifications finish processing", () =>
    Effect.gen(function* () {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-websocket-"));
      const socketPath = NodePath.join(root, "app-server.sock");
      const server = NodeHttp.createServer();
      const webSocketServer = new NodeSocket.NodeWS.WebSocketServer({
        perMessageDeflate: false,
        server,
      });
      webSocketServer.on("connection", (socket) => {
        socket.on("message", (data) => {
          const request = JSON.parse(data.toString()) as { readonly id: number };
          socket.send(JSON.stringify({ id: request.id, result: { connected: true } }));
          socket.send(
            JSON.stringify({
              method: "test/queued-notification",
              params: { terminal: true },
            }),
          );
          socket.close(1000, "done");
        });
      });

      yield* Effect.acquireRelease(listen(server, socketPath), () =>
        closeServer(server, webSocketServer).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              NodeFS.rmSync(root, { recursive: true, force: true });
            }),
          ),
        ),
      );

      const notificationStarted = yield* Deferred.make<void>();
      const releaseNotification = yield* Deferred.make<void>();
      const connection = yield* makeCodexAppServerWebSocketConnection(socketPath);
      yield* connection.client.handleUnknownServerNotification((method) =>
        method === "test/queued-notification"
          ? Deferred.succeed(notificationStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseNotification)),
            )
          : Effect.void,
      );
      const closedFiber = yield* connection.closed.pipe(Effect.forkChild);

      NodeAssert.deepStrictEqual(
        yield* connection.client.raw.request("initialize", {
          clientInfo: { name: "test", version: "0" },
        }),
        { connected: true },
      );
      yield* Deferred.await(notificationStarted).pipe(Effect.timeout("1 second"));
      yield* Effect.yieldNow;
      NodeAssert.equal(closedFiber.pollUnsafe(), undefined);

      yield* Deferred.succeed(releaseNotification, undefined);
      const status = yield* Fiber.join(closedFiber).pipe(Effect.timeout("1 second"));
      NodeAssert.equal(status.code, 1000);
      NodeAssert.equal(status.reason, "done");
    }),
  );
});
