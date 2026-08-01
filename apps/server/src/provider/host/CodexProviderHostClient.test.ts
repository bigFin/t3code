// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off - The client owns bounded transport timers.
// @effect-diagnostics preferSchemaOverJson:off - Test sockets use known provider-host envelopes.
import {
  CommandId,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderEvent,
  type ProviderSession,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { it, vi } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as CodexErrors from "effect-codex-app-server/errors";

import { CodexSessionRuntimeThreadIdMissingError } from "../Layers/CodexSessionRuntime.ts";
import { __testing, makeCodexProviderHostRuntime } from "./CodexProviderHostClient.ts";
import { PROVIDER_HOST_PROTOCOL_VERSION } from "./ProviderHostProtocol.ts";

const isCodexAppServerTransportError = Schema.is(CodexErrors.CodexAppServerTransportError);
const isCodexSessionRuntimeThreadIdMissingError = Schema.is(
  CodexSessionRuntimeThreadIdMissingError,
);

interface ClientEnvelope {
  readonly type: string;
  readonly replayFrom?: number;
  readonly commandId?: string;
  readonly operation?: string;
  readonly session?: unknown;
}

const listen = (server: NodeNet.Server, socketPath: string): Promise<void> =>
  new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, () => {
      server.removeListener("error", reject);
      resolve();
    });
  });

const closeServer = (server: NodeNet.Server): Promise<void> =>
  new Promise((resolve) => {
    server.close(() => resolve());
  });

it.effect("emits the authoritative host snapshot after reconnecting", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-client-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-reconnect");
  const activeTurnId = TurnId.make("turn-provider-host-client-reconnect");
  const replayedEventId = EventId.make("event-provider-host-client-reconnect");
  const liveEventId = EventId.make("event-provider-host-client-live-after-reconnect");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = "2026-01-01T00:00:00.000Z";
  let connectionCount = 0;
  let firstSocket: NodeNet.Socket | undefined;
  let latestSocket: NodeNet.Socket | undefined;
  const replayCursors: Array<number | undefined> = [];
  const attachSessions: Array<unknown> = [];

  const server = NodeNet.createServer((socket) => {
    connectionCount += 1;
    const currentConnection = connectionCount;
    if (currentConnection === 1) {
      firstSocket = socket;
    }
    latestSocket = socket;
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => sockets.delete(socket));
    socket.write(
      `${JSON.stringify({
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        type: "hello",
        providerInstanceId,
        generationFingerprint: "generation-client-test",
        hostProcess: {
          pid: 1,
          startTimeMs: 0,
        },
        startedAt: now,
        latestCursor: 0,
      })}\n`,
    );

    let inbound = "";
    socket.on("data", (chunk: string) => {
      inbound += chunk;
      while (true) {
        const newline = inbound.indexOf("\n");
        if (newline < 0) break;
        const line = inbound.slice(0, newline).trim();
        inbound = inbound.slice(newline + 1);
        if (!line) continue;
        const envelope = JSON.parse(line) as ClientEnvelope;
        if (
          currentConnection === 1 &&
          envelope.type === "command" &&
          envelope.operation === "thread.read"
        ) {
          NodeAssert.ok(envelope.commandId);
          socket.write(
            `${JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "commandResult",
              commandId: CommandId.make(envelope.commandId),
              threadId,
              ok: true,
              result: {
                threadId,
                turns: [],
              },
            })}\n`,
          );
          continue;
        }
        if (envelope.type !== "attach") continue;
        replayCursors.push(envelope.replayFrom);
        attachSessions.push(envelope.session);

        const state: ProviderSession =
          currentConnection === 1
            ? {
                provider: ProviderDriverKind.make("codex"),
                providerInstanceId,
                status: "ready",
                runtimeMode: "full-access",
                cwd: root,
                threadId,
                createdAt: now,
                updatedAt: now,
              }
            : {
                provider: ProviderDriverKind.make("codex"),
                providerInstanceId,
                status: "running",
                runtimeMode: "full-access",
                cwd: root,
                threadId,
                activeTurnId,
                createdAt: now,
                updatedAt: "2026-01-01T00:00:01.000Z",
              };
        if (currentConnection === 2) {
          const replayedEvent: ProviderEvent = {
            id: replayedEventId,
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId,
            threadId,
            kind: "notification",
            method: "item/agentMessage/delta",
            textDelta: "replayed before the snapshot",
            createdAt: "2026-01-01T00:00:00.500Z",
          };
          socket.write(
            `${JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "event",
              threadId,
              sequence: 1,
              event: replayedEvent,
            })}\n`,
          );
        }
        socket.write(
          `${JSON.stringify({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "snapshot",
            threadId,
            cursor: currentConnection > 2 ? 2 : currentConnection > 1 ? 1 : 0,
            ...(currentConnection === 2 ? { replayTruncated: true } : {}),
            state,
          })}\n`,
        );
        if (currentConnection === 2) {
          const liveEvent: ProviderEvent = {
            id: liveEventId,
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId,
            threadId,
            kind: "notification",
            method: "item/agentMessage/delta",
            textDelta: "live after the snapshot",
            createdAt: "2026-01-01T00:00:01.500Z",
          };
          socket.write(
            `${JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "event",
              threadId,
              sequence: 2,
              event: liveEvent,
            })}\n`,
          );
        }
      }
    });
  });

  return Effect.gen(function* () {
    yield* Effect.promise(() => listen(server, socketPath));
    yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* makeCodexProviderHostRuntime({
          controlSocketPath: socketPath,
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
          },
        });
        const initial = yield* runtime.start();
        NodeAssert.equal(initial.status, "ready");
        NodeAssert.ok(firstSocket);

        const reconnectEventsFiber = yield* runtime.events.pipe(
          Stream.take(5),
          Stream.runCollect,
          Effect.forkChild,
        );
        firstSocket.destroy();

        const reconnectEvents = Array.from(yield* Fiber.join(reconnectEventsFiber));
        NodeAssert.deepStrictEqual(
          reconnectEvents.map((event) => event.method),
          [
            "session/connecting",
            "item/agentMessage/delta",
            "session/replay-truncated",
            "session/reattached",
            "item/agentMessage/delta",
          ],
        );
        NodeAssert.equal(reconnectEvents[1]?.id, replayedEventId);
        NodeAssert.deepStrictEqual(reconnectEvents[1]?.replay, {
          truncated: true,
        });
        const reattached = reconnectEvents[3]!;
        NodeAssert.equal(reattached.turnId, activeTurnId);
        NodeAssert.deepStrictEqual(reattached.payload, {
          status: "running",
          activeTurnId,
        });
        NodeAssert.equal(reconnectEvents[4]?.id, liveEventId);
        const current = yield* runtime.getSession;
        NodeAssert.equal(current.status, "running");
        NodeAssert.equal(current.activeTurnId, activeTurnId);
        NodeAssert.deepStrictEqual(replayCursors, [0, 0]);
        NodeAssert.equal(attachSessions.length, 2);
        NodeAssert.ok(attachSessions.every((session) => session !== undefined));

        const nextReconnectEventsFiber = yield* runtime.events.pipe(
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild,
        );
        NodeAssert.ok(latestSocket);
        latestSocket.destroy();
        const nextReconnectEvents = Array.from(yield* Fiber.join(nextReconnectEventsFiber));
        NodeAssert.deepStrictEqual(
          nextReconnectEvents.map((event) => event.method),
          ["session/connecting", "session/reattached"],
        );
        NodeAssert.deepStrictEqual(replayCursors, [0, 0, 2]);
      }),
    );
  }).pipe(
    Effect.ensuring(
      Effect.promise(async () => {
        for (const socket of sockets) {
          socket.destroy();
        }
        await closeServer(server);
        NodeFS.rmSync(root, { recursive: true, force: true });
      }),
    ),
  );
});

it.effect("closes an in-flight reconnect when the runtime detaches", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-release-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-release");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = "2026-01-01T00:00:00.000Z";
  let connectionCount = 0;
  let firstSocket: NodeNet.Socket | undefined;
  let resolveSecondAttach: () => void = () => undefined;
  const secondAttach = new Promise<void>((resolve) => {
    resolveSecondAttach = resolve;
  });
  let resolveSecondClosed: () => void = () => undefined;
  const secondClosed = new Promise<void>((resolve) => {
    resolveSecondClosed = resolve;
  });

  const server = NodeNet.createServer((socket) => {
    connectionCount += 1;
    const currentConnection = connectionCount;
    if (currentConnection === 1) {
      firstSocket = socket;
    }
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => {
      sockets.delete(socket);
      if (currentConnection === 2) {
        resolveSecondClosed();
      }
    });
    socket.write(
      `${JSON.stringify({
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        type: "hello",
        providerInstanceId,
        generationFingerprint: "generation-client-release-test",
        hostProcess: {
          pid: 1,
          startTimeMs: 0,
        },
        startedAt: now,
        latestCursor: 0,
      })}\n`,
    );

    let inbound = "";
    socket.on("data", (chunk: string) => {
      inbound += chunk;
      while (true) {
        const newline = inbound.indexOf("\n");
        if (newline < 0) break;
        const line = inbound.slice(0, newline).trim();
        inbound = inbound.slice(newline + 1);
        if (!line) continue;
        const envelope = JSON.parse(line) as ClientEnvelope;
        if (envelope.type !== "attach") continue;
        if (currentConnection === 2) {
          resolveSecondAttach();
          continue;
        }
        const state: ProviderSession = {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId,
          status: "ready",
          runtimeMode: "full-access",
          cwd: root,
          threadId,
          createdAt: now,
          updatedAt: now,
        };
        socket.write(
          `${JSON.stringify({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "snapshot",
            threadId,
            cursor: 0,
            state,
          })}\n`,
        );
      }
    });
  });

  return Effect.gen(function* () {
    yield* Effect.promise(() => listen(server, socketPath));
    yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* makeCodexProviderHostRuntime({
          controlSocketPath: socketPath,
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
          },
        });
        yield* runtime.start();
        NodeAssert.ok(firstSocket);

        firstSocket.destroy();
        yield* Effect.promise(() => secondAttach);
        yield* runtime.detach;
        yield* Effect.promise(() => secondClosed);
        yield* Effect.promise(
          () =>
            new Promise<void>((resolve) => {
              setImmediate(resolve);
            }),
        );

        NodeAssert.equal(connectionCount, 2);
      }),
    );
  }).pipe(
    Effect.ensuring(
      Effect.promise(async () => {
        for (const socket of sockets) {
          socket.destroy();
        }
        await closeServer(server);
        NodeFS.rmSync(root, { recursive: true, force: true });
      }),
    ),
  );
});

it.effect("drains provider events that arrive at the detach boundary", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-drain-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-drain");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const eventId = EventId.make("event-provider-host-client-drain");
  const now = "2026-01-01T00:00:00.000Z";

  const server = NodeNet.createServer((socket) => {
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => sockets.delete(socket));
    socket.write(
      `${JSON.stringify({
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        type: "hello",
        providerInstanceId,
        generationFingerprint: "generation-client-drain-test",
        hostProcess: {
          pid: 1,
          startTimeMs: 0,
        },
        startedAt: now,
        latestCursor: 0,
      })}\n`,
    );

    let inbound = "";
    socket.on("data", (chunk: string) => {
      inbound += chunk;
      while (true) {
        const newline = inbound.indexOf("\n");
        if (newline < 0) break;
        const line = inbound.slice(0, newline).trim();
        inbound = inbound.slice(newline + 1);
        if (!line) continue;
        const envelope = JSON.parse(line) as ClientEnvelope;
        if (envelope.type === "attach") {
          const state: ProviderSession = {
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId,
            status: "ready",
            runtimeMode: "full-access",
            cwd: root,
            threadId,
            createdAt: now,
            updatedAt: now,
          };
          socket.write(
            `${JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "snapshot",
              threadId,
              cursor: 0,
              state,
            })}\n`,
          );
        }
        if (envelope.type === "detach") {
          const event: ProviderEvent = {
            id: eventId,
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId,
            threadId,
            kind: "notification",
            method: "item/agentMessage/delta",
            textDelta: "delivered before detach completed",
            createdAt: now,
          };
          socket.end(
            `${JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "event",
              threadId,
              sequence: 1,
              event,
            })}\n`,
          );
        }
      }
    });
  });

  return Effect.gen(function* () {
    yield* Effect.promise(() => listen(server, socketPath));
    yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* makeCodexProviderHostRuntime({
          controlSocketPath: socketPath,
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
          },
        });
        yield* runtime.start();
        const eventFiber = yield* runtime.events.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild,
        );

        yield* runtime.detach;

        const drained = Array.from(yield* Fiber.join(eventFiber));
        NodeAssert.deepStrictEqual(
          drained.map((event) => event.id),
          [eventId],
        );
      }),
    );
  }).pipe(
    Effect.ensuring(
      Effect.promise(async () => {
        for (const socket of sockets) {
          socket.destroy();
        }
        await closeServer(server);
        NodeFS.rmSync(root, { recursive: true, force: true });
      }),
    ),
  );
});

it.effect("keeps the attach timeout active until the initial snapshot arrives", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-timeout-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-timeout");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = "2026-01-01T00:00:00.000Z";
  let attachedResolve: () => void = () => undefined;
  const attached = new Promise<void>((resolve) => {
    attachedResolve = resolve;
  });

  const nativeSetTimeout = globalThis.setTimeout;
  const nativeClearTimeout = globalThis.clearTimeout;
  let connectTimeoutHandle: ReturnType<typeof setTimeout> | undefined;
  let connectTimeoutCleared = false;
  let fireConnectTimeout: (() => void) | undefined;
  const setTimeoutSpy = vi.spyOn(globalThis, "setTimeout");
  setTimeoutSpy.mockImplementation(((
    ...args: Parameters<typeof setTimeout>
  ): ReturnType<typeof setTimeout> => {
    const [callback, delay, ...callbackArgs] = args;
    if (delay === 5_000 && connectTimeoutHandle === undefined) {
      connectTimeoutHandle = nativeSetTimeout(() => undefined, 60_000);
      connectTimeoutHandle.unref?.();
      fireConnectTimeout = () => {
        if (!connectTimeoutCleared) {
          callback(...callbackArgs);
        }
      };
      return connectTimeoutHandle;
    }
    return nativeSetTimeout(callback, delay, ...callbackArgs);
  }) as typeof setTimeout);
  const clearTimeoutSpy = vi.spyOn(globalThis, "clearTimeout");
  clearTimeoutSpy.mockImplementation(((
    ...args: Parameters<typeof clearTimeout>
  ): ReturnType<typeof clearTimeout> => {
    if (args[0] === connectTimeoutHandle) {
      connectTimeoutCleared = true;
    }
    return nativeClearTimeout(...args);
  }) as typeof clearTimeout);

  const server = NodeNet.createServer((socket) => {
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => sockets.delete(socket));
    socket.write(
      `${JSON.stringify({
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        type: "hello",
        providerInstanceId,
        generationFingerprint: "generation-client-timeout-test",
        hostProcess: {
          pid: 1,
          startTimeMs: 0,
        },
        startedAt: now,
        latestCursor: 0,
      })}\n`,
    );

    let inbound = "";
    socket.on("data", (chunk: string) => {
      inbound += chunk;
      while (true) {
        const newline = inbound.indexOf("\n");
        if (newline < 0) break;
        const line = inbound.slice(0, newline).trim();
        inbound = inbound.slice(newline + 1);
        if (!line) continue;
        const envelope = JSON.parse(line) as ClientEnvelope;
        if (envelope.type === "attach") {
          attachedResolve();
        }
      }
    });
  });

  return Effect.gen(function* () {
    yield* Effect.promise(() => listen(server, socketPath));
    yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* makeCodexProviderHostRuntime({
          controlSocketPath: socketPath,
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
          },
        });
        const startFiber = yield* runtime
          .start()
          .pipe(Effect.timeout("250 millis"), Effect.forkChild);
        yield* Effect.promise(() => attached);
        NodeAssert.ok(fireConnectTimeout);
        fireConnectTimeout();

        const startExit = yield* Fiber.await(startFiber);
        NodeAssert.equal(Exit.isFailure(startExit), true);
        if (Exit.isFailure(startExit)) {
          const error = Cause.squash(startExit.cause);
          NodeAssert.ok(isCodexAppServerTransportError(error));
        }
      }),
    );
  }).pipe(
    Effect.ensuring(
      Effect.promise(async () => {
        setTimeoutSpy.mockRestore();
        clearTimeoutSpy.mockRestore();
        if (connectTimeoutHandle) {
          nativeClearTimeout(connectTimeoutHandle);
        }
        for (const socket of sockets) {
          socket.destroy();
        }
        await closeServer(server);
        NodeFS.rmSync(root, { recursive: true, force: true });
      }),
    ),
  );
});

it.effect("attaches to an inventoried runtime without sending session options", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-existing-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-existing");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const replayedEventId = EventId.make("event-provider-host-client-existing-replay");
  const liveEventId = EventId.make("event-provider-host-client-existing-live");
  const now = "2026-01-01T00:00:00.000Z";
  let attachEnvelope: ClientEnvelope | undefined;

  const server = NodeNet.createServer((socket) => {
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => sockets.delete(socket));
    socket.write(
      [
        JSON.stringify({
          version: PROVIDER_HOST_PROTOCOL_VERSION,
          type: "hello",
          providerInstanceId,
          generationFingerprint: "generation-client-existing-test",
          hostProcess: {
            pid: 1,
            startTimeMs: 0,
          },
          startedAt: now,
          latestCursor: 0,
        }),
        JSON.stringify({
          version: PROVIDER_HOST_PROTOCOL_VERSION,
          type: "inventory",
          threads: [
            {
              threadId,
              status: "active",
              attachmentCount: 0,
              cursor: 0,
            },
          ],
        }),
      ].join("\n") + "\n",
    );

    let inbound = "";
    socket.on("data", (chunk: string) => {
      inbound += chunk;
      while (true) {
        const newline = inbound.indexOf("\n");
        if (newline < 0) break;
        const line = inbound.slice(0, newline).trim();
        inbound = inbound.slice(newline + 1);
        if (!line) continue;
        const envelope = JSON.parse(line) as ClientEnvelope;
        if (envelope.type !== "attach") continue;
        attachEnvelope = envelope;
        const state: ProviderSession = {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId,
          status: "running",
          runtimeMode: "full-access",
          cwd: root,
          threadId,
          createdAt: now,
          updatedAt: now,
        };
        const replayedEvent: ProviderEvent = {
          id: replayedEventId,
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId,
          threadId,
          kind: "notification",
          method: "item/agentMessage/delta",
          textDelta: "startup replay",
          createdAt: now,
        };
        const liveEvent: ProviderEvent = {
          ...replayedEvent,
          id: liveEventId,
          textDelta: "startup live",
          createdAt: "2026-01-01T00:00:01.000Z",
        };
        socket.write(
          [
            JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "event",
              threadId,
              sequence: 1,
              event: replayedEvent,
            }),
            JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "snapshot",
              threadId,
              cursor: 1,
              state,
            }),
            JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "event",
              threadId,
              sequence: 2,
              event: liveEvent,
            }),
          ].join("\n") + "\n",
        );
      }
    });
  });

  return Effect.gen(function* () {
    yield* Effect.promise(() => listen(server, socketPath));
    yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* makeCodexProviderHostRuntime({
          controlSocketPath: socketPath,
          attachExisting: true,
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
            threadConfig: {
              mcp_servers: {
                "t3-code": {
                  url: "http://127.0.0.1:3773/mcp",
                },
              },
            },
          },
        });

        const orderedEventsFiber = yield* runtime.events.pipe(
          Stream.take(3),
          Stream.runCollect,
          Effect.forkChild,
        );
        const session = yield* runtime.start();
        const orderedEvents = Array.from(yield* Fiber.join(orderedEventsFiber));

        NodeAssert.equal(session.status, "running");
        NodeAssert.ok(attachEnvelope);
        NodeAssert.equal(attachEnvelope.session, undefined);
        NodeAssert.deepStrictEqual(
          orderedEvents.map((event) => event.method),
          ["item/agentMessage/delta", "session/reattached", "item/agentMessage/delta"],
        );
        NodeAssert.equal(orderedEvents[0]?.id, replayedEventId);
        NodeAssert.equal(orderedEvents[2]?.id, liveEventId);
      }),
    );
  }).pipe(
    Effect.ensuring(
      Effect.promise(async () => {
        for (const socket of sockets) {
          socket.destroy();
        }
        await closeServer(server);
        NodeFS.rmSync(root, { recursive: true, force: true });
      }),
    ),
  );
});

it.effect("maps an authoritative missing inventory entry to the typed runtime error", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-missing-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-missing");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = "2026-01-01T00:00:00.000Z";
  let attachCount = 0;

  const server = NodeNet.createServer((socket) => {
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => sockets.delete(socket));
    socket.write(
      [
        JSON.stringify({
          version: PROVIDER_HOST_PROTOCOL_VERSION,
          type: "hello",
          providerInstanceId,
          generationFingerprint: "generation-client-missing-test",
          hostProcess: {
            pid: 1,
            startTimeMs: 0,
          },
          startedAt: now,
          latestCursor: 0,
        }),
        JSON.stringify({
          version: PROVIDER_HOST_PROTOCOL_VERSION,
          type: "inventory",
          threads: [],
        }),
      ].join("\n") + "\n",
    );
    socket.on("data", (chunk: Buffer) => {
      attachCount += chunk.toString("utf8").split('"type":"attach"').length - 1;
    });
  });

  return Effect.gen(function* () {
    yield* Effect.promise(() => listen(server, socketPath));
    yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* makeCodexProviderHostRuntime({
          controlSocketPath: socketPath,
          attachExisting: true,
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
          },
        });

        const result = yield* runtime.start().pipe(Effect.result);

        NodeAssert.equal(result._tag, "Failure");
        if (result._tag !== "Failure") return;
        NodeAssert.ok(isCodexSessionRuntimeThreadIdMissingError(result.failure));
        NodeAssert.equal(result.failure.threadId, threadId);
        NodeAssert.equal(attachCount, 0);
      }),
    );
  }).pipe(
    Effect.ensuring(
      Effect.promise(async () => {
        for (const socket of sockets) {
          socket.destroy();
        }
        await closeServer(server);
        NodeFS.rmSync(root, { recursive: true, force: true });
      }),
    ),
  );
});

it.effect("keeps attach-existing protocol failures transient", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-protocol-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-protocol");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = "2026-01-01T00:00:00.000Z";

  const server = NodeNet.createServer((socket) => {
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => sockets.delete(socket));
    socket.write(
      [
        JSON.stringify({
          version: PROVIDER_HOST_PROTOCOL_VERSION,
          type: "hello",
          providerInstanceId,
          generationFingerprint: "generation-client-protocol-test",
          hostProcess: {
            pid: 1,
            startTimeMs: 0,
          },
          startedAt: now,
          latestCursor: 0,
        }),
        "not-json",
      ].join("\n") + "\n",
    );
  });

  return Effect.gen(function* () {
    yield* Effect.promise(() => listen(server, socketPath));
    yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* makeCodexProviderHostRuntime({
          controlSocketPath: socketPath,
          attachExisting: true,
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
          },
        });

        const result = yield* runtime.start().pipe(Effect.result);

        NodeAssert.equal(result._tag, "Failure");
        if (result._tag !== "Failure") return;
        NodeAssert.ok(isCodexAppServerTransportError(result.failure));
      }),
    );
  }).pipe(
    Effect.ensuring(
      Effect.promise(async () => {
        for (const socket of sockets) {
          socket.destroy();
        }
        await closeServer(server);
        NodeFS.rmSync(root, { recursive: true, force: true });
      }),
    ),
  );
});

it.effect("bounds slow-reader buffering and resumes the missing event through replay", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-bounded-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-bounded");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = "2026-01-01T00:00:00.000Z";
  const replayCursors: Array<number | undefined> = [];
  let connectionCount = 0;
  let resolveFirstClosed: () => void = () => undefined;
  const firstClosed = new Promise<void>((resolve) => {
    resolveFirstClosed = resolve;
  });
  let floodFirstConnection: () => void = () => undefined;

  const eventEnvelope = (sequence: number) => {
    const event: ProviderEvent = {
      id: EventId.make(`event-provider-host-bounded-${sequence}`),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId,
      threadId,
      kind: "notification",
      method: "item/agentMessage/delta",
      textDelta: `bounded-${sequence}`,
      createdAt: now,
    };
    return JSON.stringify({
      version: PROVIDER_HOST_PROTOCOL_VERSION,
      type: "event",
      threadId,
      sequence,
      event,
    });
  };

  const server = NodeNet.createServer((socket) => {
    connectionCount += 1;
    const currentConnection = connectionCount;
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => {
      sockets.delete(socket);
      if (currentConnection === 1) {
        resolveFirstClosed();
      }
    });
    socket.write(
      `${JSON.stringify({
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        type: "hello",
        providerInstanceId,
        generationFingerprint: "generation-client-bounded-test",
        hostProcess: {
          pid: 1,
          startTimeMs: 0,
        },
        startedAt: now,
        latestCursor: 3,
      })}\n`,
    );

    let inbound = "";
    socket.on("data", (chunk: string) => {
      inbound += chunk;
      while (true) {
        const newline = inbound.indexOf("\n");
        if (newline < 0) break;
        const line = inbound.slice(0, newline).trim();
        inbound = inbound.slice(newline + 1);
        if (!line) continue;
        const envelope = JSON.parse(line) as ClientEnvelope;
        if (envelope.type !== "attach") continue;
        replayCursors.push(envelope.replayFrom);

        const state: ProviderSession = {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId,
          status: "ready",
          runtimeMode: "full-access",
          cwd: root,
          threadId,
          createdAt: now,
          updatedAt: now,
        };
        if (currentConnection === 1) {
          floodFirstConnection = () => {
            socket.write([eventEnvelope(1), eventEnvelope(2), eventEnvelope(3)].join("\n") + "\n");
          };
          socket.write(
            `${JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "snapshot",
              threadId,
              cursor: 0,
              state,
            })}\n`,
          );
          continue;
        }
        socket.write(
          [
            eventEnvelope(3),
            JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "snapshot",
              threadId,
              cursor: 3,
              state,
            }),
          ].join("\n") + "\n",
        );
      }
    });
  });

  return Effect.gen(function* () {
    yield* Effect.promise(() => listen(server, socketPath));
    yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* makeCodexProviderHostRuntime({
          controlSocketPath: socketPath,
          maxPendingProviderEvents: 2,
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
          },
        });
        NodeAssert.equal((yield* runtime.start()).status, "ready");
        floodFirstConnection();
        yield* Effect.promise(() => firstClosed).pipe(Effect.timeout("1 second"));

        const events = Array.from(
          yield* runtime.events.pipe(
            Stream.take(5),
            Stream.runCollect,
            Effect.timeout("2 seconds"),
          ),
        );

        NodeAssert.deepStrictEqual(
          events.map((event) => event.method),
          [
            "item/agentMessage/delta",
            "item/agentMessage/delta",
            "session/connecting",
            "item/agentMessage/delta",
            "session/reattached",
          ],
        );
        NodeAssert.deepStrictEqual(
          events.filter((event) => event.textDelta).map((event) => event.textDelta),
          ["bounded-1", "bounded-2", "bounded-3"],
        );
        NodeAssert.deepStrictEqual(replayCursors.slice(0, 2), [0, 2]);
        NodeAssert.equal(__testing.maxPendingProviderEvents, 4_096);
        NodeAssert.equal(__testing.maxPendingProviderEventBytes, 16 * 1024 * 1024);
      }),
    );
  }).pipe(
    Effect.ensuring(
      Effect.promise(async () => {
        for (const socket of sockets) {
          socket.destroy();
        }
        await closeServer(server);
        NodeFS.rmSync(root, { recursive: true, force: true });
      }),
    ),
  );
});

it.effect("bounds detach waiting when a control-socket peer does not close", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-detach-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-detach");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = "2026-01-01T00:00:00.000Z";
  let detachCount = 0;
  let resolvePeerEnded: () => void = () => undefined;
  const peerEnded = new Promise<void>((resolve) => {
    resolvePeerEnded = resolve;
  });

  const server = NodeNet.createServer({ allowHalfOpen: true }, (socket) => {
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => sockets.delete(socket));
    socket.once("end", resolvePeerEnded);
    socket.write(
      `${JSON.stringify({
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        type: "hello",
        providerInstanceId,
        generationFingerprint: "generation-client-detach-test",
        hostProcess: {
          pid: 1,
          startTimeMs: 0,
        },
        startedAt: now,
        latestCursor: 0,
      })}\n`,
    );

    let inbound = "";
    socket.on("data", (chunk: string) => {
      inbound += chunk;
      while (true) {
        const newline = inbound.indexOf("\n");
        if (newline < 0) break;
        const line = inbound.slice(0, newline).trim();
        inbound = inbound.slice(newline + 1);
        if (!line) continue;
        const envelope = JSON.parse(line) as ClientEnvelope;
        if (envelope.type === "detach") {
          detachCount += 1;
          continue;
        }
        if (envelope.type !== "attach") continue;
        const state: ProviderSession = {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId,
          status: "ready",
          runtimeMode: "full-access",
          cwd: root,
          threadId,
          createdAt: now,
          updatedAt: now,
        };
        socket.write(
          `${JSON.stringify({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "snapshot",
            threadId,
            cursor: 0,
            state,
          })}\n`,
        );
      }
    });
  });

  return Effect.gen(function* () {
    yield* Effect.promise(() => listen(server, socketPath));
    yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* makeCodexProviderHostRuntime({
          controlSocketPath: socketPath,
          detachCloseTimeoutMs: 25,
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
          },
        });
        yield* runtime.start();

        yield* runtime.detach.pipe(Effect.timeout("1 second"));
        yield* Effect.promise(() => peerEnded).pipe(Effect.timeout("1 second"));

        NodeAssert.equal(detachCount, 1);
        NodeAssert.equal(__testing.detachCloseTimeoutMs, 1_000);
      }),
    );
  }).pipe(
    Effect.ensuring(
      Effect.promise(async () => {
        for (const socket of sockets) {
          socket.destroy();
        }
        await closeServer(server);
        NodeFS.rmSync(root, { recursive: true, force: true });
      }),
    ),
  );
});

it.effect("drains terminal provider events before closing the event stream", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-drain-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-drain");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = "2026-01-01T00:00:00.000Z";
  const terminalEventCount = 64;

  const server = NodeNet.createServer((socket) => {
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => sockets.delete(socket));
    socket.write(
      `${JSON.stringify({
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        type: "hello",
        providerInstanceId,
        generationFingerprint: "generation-client-drain-test",
        hostProcess: {
          pid: 1,
          startTimeMs: 0,
        },
        startedAt: now,
        latestCursor: 0,
      })}\n`,
    );

    let inbound = "";
    socket.on("data", (chunk: string) => {
      inbound += chunk;
      while (true) {
        const newline = inbound.indexOf("\n");
        if (newline < 0) break;
        const line = inbound.slice(0, newline).trim();
        inbound = inbound.slice(newline + 1);
        if (!line) continue;
        const envelope = JSON.parse(line) as ClientEnvelope;
        if (envelope.type === "attach") {
          const state: ProviderSession = {
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId,
            status: "ready",
            runtimeMode: "full-access",
            cwd: root,
            threadId,
            createdAt: now,
            updatedAt: now,
          };
          socket.write(
            `${JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "snapshot",
              threadId,
              cursor: 0,
              state,
            })}\n`,
          );
          continue;
        }
        if (envelope.type !== "command" || envelope.operation !== "session.stop") {
          continue;
        }
        NodeAssert.ok(envelope.commandId);
        const frames = Array.from({ length: terminalEventCount }, (_, index) => {
          const event: ProviderEvent = {
            id: EventId.make(`event-provider-host-drain-${index + 1}`),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId,
            threadId,
            kind: "notification",
            method: "item/agentMessage/delta",
            textDelta: `terminal-${index + 1}`,
            createdAt: now,
          };
          return `${JSON.stringify({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "event",
            threadId,
            sequence: index + 1,
            event,
          })}\n`;
        });
        frames.push(
          `${JSON.stringify({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "commandResult",
            commandId: CommandId.make(envelope.commandId),
            threadId,
            ok: true,
          })}\n`,
        );
        socket.write(frames.join(""));
      }
    });
  });

  return Effect.gen(function* () {
    yield* Effect.promise(() => listen(server, socketPath));
    yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* makeCodexProviderHostRuntime({
          controlSocketPath: socketPath,
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
          },
        });
        yield* runtime.start();
        const eventsFiber = yield* Stream.runCollect(runtime.events).pipe(Effect.forkChild);

        yield* runtime.close;

        const events = Array.from(yield* Fiber.join(eventsFiber));
        NodeAssert.equal(events.length, terminalEventCount);
        NodeAssert.deepStrictEqual(
          events.map((event) => event.textDelta),
          Array.from({ length: terminalEventCount }, (_, index) => `terminal-${index + 1}`),
        );
        NodeAssert.ok(events.every((event) => event.replay === undefined));
      }),
    );
  }).pipe(
    Effect.ensuring(
      Effect.promise(async () => {
        for (const socket of sockets) {
          socket.destroy();
        }
        await closeServer(server);
        NodeFS.rmSync(root, { recursive: true, force: true });
      }),
    ),
  );
});
