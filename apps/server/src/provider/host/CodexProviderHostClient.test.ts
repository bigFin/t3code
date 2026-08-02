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
import * as TestClock from "effect/testing/TestClock";
import * as CodexErrors from "effect-codex-app-server/errors";

import {
  CodexSessionRuntimeMutationAmbiguousError,
  CodexSessionRuntimeThreadIdMissingError,
} from "../Layers/CodexSessionRuntime.ts";
import { __testing, makeCodexProviderHostRuntime } from "./CodexProviderHostClient.ts";
import {
  PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
  PROVIDER_HOST_PROTOCOL_VERSION,
  ProviderHostCommandDeadlineMs,
} from "./ProviderHostProtocol.ts";

const isCodexAppServerTransportError = Schema.is(CodexErrors.CodexAppServerTransportError);
const isCodexSessionRuntimeThreadIdMissingError = Schema.is(
  CodexSessionRuntimeThreadIdMissingError,
);
const isCodexSessionRuntimeMutationAmbiguousError = Schema.is(
  CodexSessionRuntimeMutationAmbiguousError,
);

interface ClientEnvelope {
  readonly version?: number;
  readonly type: string;
  readonly mode?: "create" | "reuse" | "adopt";
  readonly replayFrom?: number;
  readonly commandId?: string;
  readonly operation?: string;
  readonly session?: unknown;
  readonly deadlineAtMs?: number;
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

it("derives command response waits from the remaining absolute deadline plus grace", () => {
  const deadlineAtMs = ProviderHostCommandDeadlineMs.make(1_000);

  NodeAssert.equal(__testing.commandResponseWaitMs(deadlineAtMs, 50, 900), 150);
  NodeAssert.equal(__testing.commandResponseWaitMs(deadlineAtMs, 50, 1_020), 30);
  NodeAssert.equal(__testing.commandResponseWaitMs(deadlineAtMs, 50, 1_050), 0);
  NodeAssert.equal(__testing.commandResponseWaitMs(deadlineAtMs, 50, 1_100), 0);
});

it.effect("negotiates protocol v1 and reuses a legacy session with v1 envelopes", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-v1-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-v1");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = "2026-01-01T00:00:00.000Z";
  const received: ClientEnvelope[] = [];

  const server = NodeNet.createServer((socket) => {
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => sockets.delete(socket));
    socket.write(
      [
        JSON.stringify({
          version: PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
          type: "hello",
          providerInstanceId,
          generationFingerprint: "generation-provider-host-v1",
          hostProcess: {
            pid: 1,
            startTimeMs: 0,
          },
          startedAt: now,
          latestCursor: 0,
        }),
        JSON.stringify({
          version: PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
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
        received.push(envelope);
        if (envelope.type === "attach") {
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
          socket.write(
            `${JSON.stringify({
              version: PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
              type: "snapshot",
              threadId,
              cursor: 0,
              state,
            })}\n`,
          );
          continue;
        }
        if (envelope.type === "command") {
          NodeAssert.ok(envelope.commandId);
          socket.write(
            `${JSON.stringify({
              version: PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
              type: "commandResult",
              commandId: CommandId.make(envelope.commandId),
              threadId,
              ok: true,
              result: {
                threadId: "provider-thread-v1",
                turns: [],
              },
            })}\n`,
          );
          continue;
        }
        if (envelope.type === "detach") {
          socket.end();
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
          sessionMode: "adopt",
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
            resumeCursor: { threadId: "provider-thread-v1" },
          },
        });

        const session = yield* runtime.start();
        NodeAssert.equal(session.status, "running");
        NodeAssert.deepStrictEqual(yield* runtime.readThread, {
          threadId: "provider-thread-v1",
          turns: [],
        });
        yield* runtime.detach;

        NodeAssert.deepStrictEqual(
          received.map((envelope) => envelope.type),
          ["attach", "command", "detach"],
        );
        const [attach, command, detach] = received;
        NodeAssert.equal(attach?.version, PROVIDER_HOST_LEGACY_PROTOCOL_VERSION);
        NodeAssert.equal(attach?.mode, undefined);
        NodeAssert.equal(attach?.session, undefined);
        NodeAssert.equal(command?.version, PROVIDER_HOST_LEGACY_PROTOCOL_VERSION);
        NodeAssert.equal(command?.deadlineAtMs, undefined);
        NodeAssert.equal(detach?.version, PROVIDER_HOST_LEGACY_PROTOCOL_VERSION);
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
        buildFingerprint: "build-client-test",
        generationFingerprint: "generation-client-test",
        appServerMode: "spawn",
        canAdoptSessions: false,
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
            "session/reconnecting",
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
        NodeAssert.ok(attachSessions[0] !== undefined);
        NodeAssert.equal(attachSessions[1], undefined);

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
          ["session/reconnecting", "session/reattached"],
        );
        NodeAssert.deepStrictEqual(replayCursors, [0, 0, 2]);
        NodeAssert.equal(attachSessions[2], undefined);
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

it.effect("ignores a delayed close from an obsolete provider-host socket", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-stale-close-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-stale-close");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = "2026-01-01T00:00:00.000Z";
  let connectionCount = 0;
  let failedFirstCommandWrite = false;
  let obsoleteSocket: NodeNet.Socket | undefined;
  let resolveObsoleteClosed: () => void = () => undefined;
  const obsoleteClosed = new Promise<void>((resolve) => {
    resolveObsoleteClosed = resolve;
  });

  const server = NodeNet.createServer((socket) => {
    connectionCount += 1;
    const currentConnection = connectionCount;
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => sockets.delete(socket));
    socket.write(
      `${JSON.stringify({
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        type: "hello",
        providerInstanceId,
        buildFingerprint: "build-client-test",
        generationFingerprint: "generation-client-stale-close-test",
        appServerMode: "spawn",
        canAdoptSessions: false,
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
        if (envelope.type === "command") {
          NodeAssert.equal(currentConnection, 2);
          NodeAssert.ok(envelope.commandId);
          socket.write(
            `${JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "commandResult",
              commandId: CommandId.make(envelope.commandId),
              threadId,
              ok: true,
              result: {
                threadId: "provider-thread-stale-close",
                turns: [],
              },
            })}\n`,
          );
          continue;
        }
        if (envelope.type === "detach") {
          socket.end();
        }
      }
    });
  });

  const createConnection = (path: string): NodeNet.Socket => {
    const socket = NodeNet.createConnection(path);
    if (obsoleteSocket !== undefined) {
      return socket;
    }
    socket.once("close", resolveObsoleteClosed);
    const proxy = new Proxy(socket, {
      get(target, property) {
        if (property === "write") {
          return (chunk: string | Uint8Array, ...args: ReadonlyArray<unknown>) => {
            const text = typeof chunk === "string" ? chunk : Buffer.from(chunk).toString();
            if (!failedFirstCommandWrite && text.includes('"type":"command"')) {
              failedFirstCommandWrite = true;
              const callback = args.find(
                (argument): argument is (cause?: Error | null) => void =>
                  typeof argument === "function",
              );
              queueMicrotask(() =>
                callback?.(new Error("Synthetic first-socket command write failure.")),
              );
              return true;
            }
            return Reflect.apply(target.write, target, [chunk, ...args]) as boolean;
          };
        }
        const value = Reflect.get(target, property, target) as unknown;
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    obsoleteSocket = proxy;
    return proxy;
  };

  return Effect.gen(function* () {
    yield* Effect.promise(() => listen(server, socketPath));
    yield* Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* makeCodexProviderHostRuntime({
          controlSocketPath: socketPath,
          createConnection,
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
        NodeAssert.ok(obsoleteSocket);

        NodeAssert.deepStrictEqual(yield* runtime.readThread, {
          threadId: "provider-thread-stale-close",
          turns: [],
        });
        NodeAssert.equal(connectionCount, 2);
        NodeAssert.equal(failedFirstCommandWrite, true);

        const reattachedEvents = Array.from(
          yield* runtime.events.pipe(Stream.take(1), Stream.runCollect),
        );
        NodeAssert.deepStrictEqual(
          reattachedEvents.map((event) => event.method),
          ["session/reattached"],
        );

        const unexpectedEventFiber = yield* runtime.events.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild,
        );
        obsoleteSocket.destroy();
        yield* Effect.promise(() => obsoleteClosed);
        yield* Effect.yieldNow;

        NodeAssert.equal(unexpectedEventFiber.pollUnsafe(), undefined);
        NodeAssert.equal((yield* runtime.getSession).status, "ready");
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
        buildFingerprint: "build-client-test",
        generationFingerprint: "generation-client-release-test",
        appServerMode: "spawn",
        canAdoptSessions: false,
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
        buildFingerprint: "build-client-test",
        generationFingerprint: "generation-client-drain-test",
        appServerMode: "spawn",
        canAdoptSessions: false,
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
        buildFingerprint: "build-client-test",
        generationFingerprint: "generation-client-timeout-test",
        appServerMode: "spawn",
        canAdoptSessions: false,
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

it.effect("does not send commands until the authoritative snapshot arrives", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-ready-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-ready");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = "2026-01-01T00:00:00.000Z";
  const operations: string[] = [];
  let attachedResolve: () => void = () => undefined;
  const attached = new Promise<void>((resolve) => {
    attachedResolve = resolve;
  });
  let commandResolve: () => void = () => undefined;
  const commandReceived = new Promise<void>((resolve) => {
    commandResolve = resolve;
  });
  let attachedSocket: NodeNet.Socket | undefined;

  const server = NodeNet.createServer((socket) => {
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => sockets.delete(socket));
    socket.write(
      `${JSON.stringify({
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        type: "hello",
        providerInstanceId,
        buildFingerprint: "build-client-test",
        generationFingerprint: "generation-client-ready-test",
        appServerMode: "spawn",
        canAdoptSessions: false,
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
          attachedSocket = socket;
          attachedResolve();
          continue;
        }
        if (envelope.type !== "command") continue;
        NodeAssert.ok(envelope.commandId);
        NodeAssert.ok(envelope.operation);
        operations.push(envelope.operation);
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
        commandResolve();
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
        const startFiber = yield* runtime.start().pipe(Effect.forkChild);
        const readFiber = yield* runtime.readThread.pipe(Effect.forkChild);
        yield* Effect.promise(() => attached);
        yield* Effect.promise(
          () =>
            new Promise<void>((resolve) => {
              setImmediate(resolve);
            }),
        );
        NodeAssert.deepStrictEqual(operations, []);
        NodeAssert.ok(attachedSocket);

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
        attachedSocket.write(
          `${JSON.stringify({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "snapshot",
            threadId,
            cursor: 0,
            state,
          })}\n`,
        );

        yield* Effect.promise(() => commandReceived);
        NodeAssert.equal((yield* Fiber.join(startFiber)).status, "ready");
        NodeAssert.deepStrictEqual(yield* Fiber.join(readFiber), {
          threadId,
          turns: [],
        });
        NodeAssert.deepStrictEqual(operations, ["thread.read"]);
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

it.effect("attaches to an inventoried runtime without replacing it", () => {
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
          buildFingerprint: "build-client-test",
          generationFingerprint: "generation-client-existing-test",
          appServerMode: "spawn",
          canAdoptSessions: false,
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
          sessionMode: "reuse",
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
        NodeAssert.equal(attachEnvelope.mode, "reuse");
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

it.effect("does not adopt a missing session when the host lacks adoption capability", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-missing-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-missing");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const providerThreadId = "provider-thread-missing";
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
          buildFingerprint: "build-client-test",
          generationFingerprint: "generation-client-missing-test",
          appServerMode: "attach",
          canAdoptSessions: false,
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
          sessionMode: "reuse",
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
            resumeCursor: { threadId: providerThreadId },
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

it.effect("preserves a typed missing-thread error from v2 resume-only adoption", () => {
  const root = NodeFS.mkdtempSync(
    NodePath.join(NodeOS.tmpdir(), "t3-provider-host-adopt-missing-"),
  );
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-adopt-missing");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const providerThreadId = "provider-thread-adopt-missing";
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
        buildFingerprint: "build-client-test",
        generationFingerprint: "generation-client-adopt-missing-test",
        appServerMode: "attach",
        canAdoptSessions: true,
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
        NodeAssert.equal(envelope.mode, "adopt");
        socket.end(
          `${JSON.stringify({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "attachError",
            threadId,
            errorCode: "thread-id-missing",
            error: "Codex no longer has the detached thread.",
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
          sessionMode: "adopt",
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
            resumeCursor: { threadId: providerThreadId },
          },
        });

        const result = yield* runtime.start().pipe(Effect.result);

        NodeAssert.equal(result._tag, "Failure");
        if (result._tag !== "Failure") return;
        NodeAssert.ok(isCodexSessionRuntimeThreadIdMissingError(result.failure));
        NodeAssert.equal(result.failure.threadId, threadId);
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

it.effect(
  "does not resend sendTurn across provider-host generations and recovers thread state",
  () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-mutation-"));
    const socketPath = NodePath.join(root, "control.sock");
    const sockets = new Set<NodeNet.Socket>();
    const threadId = ThreadId.make("thread-provider-host-client-mutation");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const recoveredTurnId = TurnId.make("turn-provider-host-client-mutation-recovered");
    const now = "2026-01-01T00:00:00.000Z";
    const commands: Array<{ readonly connection: number; readonly operation: string }> = [];
    let connectionCount = 0;

    const server = NodeNet.createServer((socket) => {
      connectionCount += 1;
      const currentConnection = connectionCount;
      sockets.add(socket);
      socket.setEncoding("utf8");
      socket.on("close", () => sockets.delete(socket));
      socket.write(
        [
          JSON.stringify({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "hello",
            providerInstanceId,
            buildFingerprint: "build-client-test",
            generationFingerprint: `generation-client-mutation-${currentConnection}`,
            appServerMode: "spawn",
            canAdoptSessions: false,
            hostProcess: {
              pid: currentConnection,
              startTimeMs: currentConnection,
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
          if (envelope.type !== "command") continue;
          NodeAssert.ok(envelope.commandId);
          NodeAssert.ok(envelope.operation);
          commands.push({
            connection: currentConnection,
            operation: envelope.operation,
          });
          if (envelope.operation === "turn.start" && currentConnection === 1) {
            socket.destroy();
            continue;
          }
          socket.write(
            `${JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "commandResult",
              commandId: CommandId.make(envelope.commandId),
              threadId,
              ok: true,
              result:
                envelope.operation === "thread.read"
                  ? {
                      threadId,
                      status: {
                        type: "active",
                        activeFlags: [],
                      },
                      turns: [
                        {
                          id: recoveredTurnId,
                          status: "inProgress",
                          items: [],
                        },
                      ],
                    }
                  : {
                      threadId,
                      turnId: TurnId.make("turn-provider-host-client-mutation-duplicate"),
                    },
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
          const recoveredEventFiber = yield* runtime.events.pipe(
            Stream.filter(
              (event) => event.method === "session/reattached" && event.turnId === recoveredTurnId,
            ),
            Stream.take(1),
            Stream.runCollect,
            Effect.forkChild,
          );

          const result = yield* runtime.sendTurn({ input: "Proceed" }).pipe(Effect.result);
          const recoveredEvents = Array.from(yield* Fiber.join(recoveredEventFiber));

          NodeAssert.equal(result._tag, "Failure");
          if (result._tag !== "Failure") return;
          NodeAssert.ok(isCodexSessionRuntimeMutationAmbiguousError(result.failure));
          if (!isCodexSessionRuntimeMutationAmbiguousError(result.failure)) return;
          NodeAssert.equal(result.failure.operation, "turn.start");
          NodeAssert.equal(result.failure.threadReadSucceeded, true);
          NodeAssert.deepStrictEqual(
            recoveredEvents.map((event) => event.method),
            ["session/reattached"],
          );
          NodeAssert.equal(recoveredEvents[0]?.turnId, recoveredTurnId);
          NodeAssert.deepStrictEqual(recoveredEvents[0]?.payload, {
            status: "running",
            activeTurnId: recoveredTurnId,
          });
          const recoveredSession = yield* runtime.getSession;
          NodeAssert.equal(recoveredSession.status, "running");
          NodeAssert.equal(recoveredSession.activeTurnId, recoveredTurnId);
          NodeAssert.deepStrictEqual(commands, [
            { connection: 1, operation: "turn.start" },
            { connection: 2, operation: "thread.read" },
          ]);
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
  },
);

it.effect("reports exhausted same-generation mutation retries as ambiguous", () => {
  const root = NodeFS.mkdtempSync(
    NodePath.join(NodeOS.tmpdir(), "t3-provider-host-same-generation-mutation-"),
  );
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-same-generation-mutation");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = "2026-01-01T00:00:00.000Z";
  const commandIds: string[] = [];

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
          buildFingerprint: "build-client-test",
          generationFingerprint: "generation-client-same-mutation",
          appServerMode: "spawn",
          canAdoptSessions: false,
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
        if (envelope.type !== "command" || envelope.operation !== "turn.start") continue;
        NodeAssert.ok(envelope.commandId);
        commandIds.push(envelope.commandId);
        socket.destroy();
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

        const result = yield* runtime.sendTurn({ input: "Proceed" }).pipe(Effect.result);

        NodeAssert.equal(result._tag, "Failure");
        if (result._tag !== "Failure") return;
        NodeAssert.ok(isCodexSessionRuntimeMutationAmbiguousError(result.failure));
        if (!isCodexSessionRuntimeMutationAmbiguousError(result.failure)) return;
        NodeAssert.equal(result.failure.operation, "turn.start");
        NodeAssert.equal(result.failure.threadReadSucceeded, false);
        NodeAssert.equal(commandIds.length, 2);
        NodeAssert.equal(commandIds[0], commandIds[1]);
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

it.effect(
  "reports a mutation as ambiguous when the replacement host cannot reattach the thread",
  () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-missing-"));
    const socketPath = NodePath.join(root, "control.sock");
    const sockets = new Set<NodeNet.Socket>();
    const threadId = ThreadId.make("thread-provider-host-client-missing-after-mutation");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const providerThreadId = "provider-thread-missing-after-mutation";
    const now = "2026-01-01T00:00:00.000Z";
    const commands: string[] = [];
    let connectionCount = 0;

    const server = NodeNet.createServer((socket) => {
      connectionCount += 1;
      const currentConnection = connectionCount;
      sockets.add(socket);
      socket.setEncoding("utf8");
      socket.on("close", () => sockets.delete(socket));
      socket.write(
        [
          JSON.stringify({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "hello",
            providerInstanceId,
            buildFingerprint: "build-client-test",
            generationFingerprint: `generation-client-missing-${currentConnection}`,
            appServerMode: "spawn",
            canAdoptSessions: false,
            hostProcess: {
              pid: currentConnection,
              startTimeMs: currentConnection,
            },
            startedAt: now,
            latestCursor: 0,
          }),
          JSON.stringify({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "inventory",
            threads:
              currentConnection === 1
                ? [
                    {
                      threadId,
                      status: "active",
                      attachmentCount: 0,
                      cursor: 0,
                    },
                  ]
                : [],
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
          if (envelope.type === "attach") {
            const state: ProviderSession = {
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId,
              status: "ready",
              runtimeMode: "full-access",
              cwd: root,
              threadId,
              resumeCursor: { threadId: providerThreadId },
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
          if (envelope.type !== "command") continue;
          commands.push(envelope.operation ?? "");
          if (envelope.operation === "turn.start") {
            socket.destroy();
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

          const result = yield* runtime.sendTurn({ input: "Proceed" }).pipe(Effect.result);

          NodeAssert.equal(result._tag, "Failure");
          if (result._tag !== "Failure") return;
          NodeAssert.ok(isCodexSessionRuntimeMutationAmbiguousError(result.failure));
          if (!isCodexSessionRuntimeMutationAmbiguousError(result.failure)) return;
          NodeAssert.equal(result.failure.operation, "turn.start");
          NodeAssert.equal(result.failure.threadReadSucceeded, false);
          NodeAssert.deepStrictEqual(commands, ["turn.start"]);
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
  },
);

it.effect("bounds command waits when the provider host stops responding", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-deadline-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-deadline");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = "2026-01-01T00:00:00.000Z";
  let commandCount = 0;

  const server = NodeNet.createServer((socket) => {
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => sockets.delete(socket));
    socket.write(
      `${JSON.stringify({
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        type: "hello",
        providerInstanceId,
        buildFingerprint: "build-client-test",
        generationFingerprint: "generation-client-deadline",
        appServerMode: "spawn",
        canAdoptSessions: false,
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
            resumeCursor: { threadId: "provider-thread-deadline" },
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
        } else if (envelope.type === "command") {
          commandCount += 1;
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
          commandTimeoutMs: 20,
          commandClientGraceMs: 0,
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

        const result = yield* runtime.readThread.pipe(Effect.result);

        NodeAssert.equal(result._tag, "Failure");
        if (result._tag !== "Failure") return;
        NodeAssert.ok(isCodexAppServerTransportError(result.failure));
        NodeAssert.equal(commandCount, 1);
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

it.effect("does not reopen an attachment after the command deadline expires", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-expired-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-expired");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = "2026-01-01T00:00:00.000Z";
  let connectionCount = 0;
  let attachmentCount = 0;
  let commandCount = 0;
  let resolveFirstClosed: () => void = () => undefined;
  const firstClosed = new Promise<void>((resolve) => {
    resolveFirstClosed = resolve;
  });

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
        buildFingerprint: "build-client-test",
        generationFingerprint: "generation-client-expired",
        appServerMode: "spawn",
        canAdoptSessions: false,
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
          attachmentCount += 1;
          const state: ProviderSession = {
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId,
            status: "ready",
            runtimeMode: "full-access",
            cwd: root,
            threadId,
            resumeCursor: { threadId: "provider-thread-expired" },
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
        } else if (envelope.type === "command") {
          commandCount += 1;
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
          commandTimeoutMs: 10,
          commandClientGraceMs: 20,
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
          },
        });

        const result = yield* runtime.readThread.pipe(Effect.result);
        yield* Effect.promise(() => firstClosed).pipe(Effect.timeout("1 second"));

        NodeAssert.equal(result._tag, "Failure");
        if (result._tag !== "Failure") return;
        NodeAssert.ok(isCodexAppServerTransportError(result.failure));
        NodeAssert.equal(connectionCount, 1);
        NodeAssert.equal(attachmentCount, 1);
        NodeAssert.equal(commandCount, 1);
        NodeAssert.equal(sockets.size, 0);
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

it.effect("rehydrates an unlisted host session and emits an authoritative barrier", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-rehydrate-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-rehydrate");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const providerThreadId = "provider-thread-rehydrate";
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
          buildFingerprint: "build-client-test",
          generationFingerprint: "generation-client-rehydrate-test",
          appServerMode: "attach",
          canAdoptSessions: true,
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
          status: "ready",
          runtimeMode: "full-access",
          cwd: root,
          threadId,
          resumeCursor: { threadId: providerThreadId },
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
          sessionMode: "adopt",
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
            resumeCursor: { threadId: providerThreadId },
          },
        });

        const reattachedEventFiber = yield* runtime.events.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild,
        );
        const session = yield* runtime.start();
        const reattachedEvents = Array.from(yield* Fiber.join(reattachedEventFiber));

        NodeAssert.equal(session.status, "ready");
        NodeAssert.deepStrictEqual(
          reattachedEvents.map((event) => event.method),
          ["session/reattached"],
        );
        NodeAssert.ok(attachEnvelope);
        NodeAssert.equal(attachEnvelope.mode, "adopt");
        NodeAssert.deepStrictEqual(
          (attachEnvelope.session as { readonly resumeCursor?: unknown }).resumeCursor,
          { threadId: providerThreadId },
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

it.effect("bounds reconnect waiting when provider events remain unread", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-drain-wait-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-drain-wait");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const now = "2026-01-01T00:00:00.000Z";
  let firstSocket: NodeNet.Socket | undefined;

  const server = NodeNet.createServer((socket) => {
    firstSocket ??= socket;
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => sockets.delete(socket));
    socket.write(
      `${JSON.stringify({
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        type: "hello",
        providerInstanceId,
        buildFingerprint: "build-client-test",
        generationFingerprint: "generation-client-drain-wait-test",
        appServerMode: "spawn",
        canAdoptSessions: false,
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
        const unreadEvent: ProviderEvent = {
          id: EventId.make("event-provider-host-client-drain-wait"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId,
          threadId,
          kind: "notification",
          method: "item/agentMessage/delta",
          textDelta: "unread",
          createdAt: now,
        };
        socket.write(
          [
            JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "snapshot",
              threadId,
              cursor: 0,
              state,
            }),
            JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "event",
              threadId,
              sequence: 1,
              event: unreadEvent,
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
          reconnectWindowMs: 25,
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
        firstSocket?.destroy();
        yield* Effect.promise(
          () =>
            new Promise<void>((resolve) => {
              setImmediate(resolve);
            }),
        );
        yield* TestClock.adjust("25 millis");
        yield* Effect.yieldNow;

        const session = yield* runtime.getSession;
        NodeAssert.equal(session.status, "error");
        NodeAssert.equal(
          session.lastError,
          "T3 lost its provider-host attachment. Codex execution was not interrupted.",
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

it.effect("adopts after a same-generation runtime reports a provider disconnect", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-recover-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-recover");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const providerThreadId = "provider-thread-recover";
  const now = "2026-01-01T00:00:00.000Z";
  const attachModes: Array<ClientEnvelope["mode"]> = [];
  let connectionCount = 0;
  let firstSocket: NodeNet.Socket | undefined;

  const server = NodeNet.createServer((socket) => {
    connectionCount += 1;
    const currentConnection = connectionCount;
    if (currentConnection === 1) {
      firstSocket = socket;
    }
    sockets.add(socket);
    socket.setEncoding("utf8");
    socket.on("close", () => sockets.delete(socket));
    socket.write(
      [
        JSON.stringify({
          version: PROVIDER_HOST_PROTOCOL_VERSION,
          type: "hello",
          providerInstanceId,
          buildFingerprint: "build-client-test",
          generationFingerprint: "generation-client-recover",
          appServerMode: "spawn",
          canAdoptSessions: true,
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
              status: currentConnection === 1 ? "active" : "error",
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
        attachModes.push(envelope.mode);
        const state: ProviderSession = {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId,
          status: "ready",
          runtimeMode: "full-access",
          cwd: root,
          threadId,
          resumeCursor: { threadId: providerThreadId },
          createdAt: now,
          updatedAt: now,
        };
        socket.write(
          `${JSON.stringify({
            version: PROVIDER_HOST_PROTOCOL_VERSION,
            type: "snapshot",
            threadId,
            cursor: currentConnection === 1 ? 0 : 1,
            state,
          })}\n`,
        );
        if (currentConnection === 1) {
          const disconnected: ProviderEvent = {
            id: EventId.make("event-provider-host-client-disconnected"),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId,
            threadId,
            kind: "session",
            method: "session/disconnected",
            message: "provider runtime disconnected",
            createdAt: "2026-01-01T00:00:01.000Z",
          };
          socket.write(
            `${JSON.stringify({
              version: PROVIDER_HOST_PROTOCOL_VERSION,
              type: "event",
              threadId,
              sequence: 1,
              event: disconnected,
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
          sessionMode: "reuse",
          options: {
            threadId,
            providerInstanceId,
            cwd: root,
            binaryPath: "codex",
            launchArgs: "",
            runtimeMode: "full-access",
            resumeCursor: { threadId: providerThreadId },
          },
        });

        yield* runtime.start();
        const initialEvents = Array.from(
          yield* runtime.events.pipe(Stream.take(2), Stream.runCollect),
        );
        NodeAssert.deepStrictEqual(
          initialEvents.map((event) => event.method),
          ["session/reattached", "session/disconnected"],
        );
        NodeAssert.equal((yield* runtime.getSession).status, "error");

        const reconnectEventsFiber = yield* runtime.events.pipe(
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild,
        );
        firstSocket?.destroy();
        const reconnectEvents = Array.from(yield* Fiber.join(reconnectEventsFiber));

        NodeAssert.deepStrictEqual(
          reconnectEvents.map((event) => event.method),
          ["session/reconnecting", "session/reattached"],
        );
        NodeAssert.deepStrictEqual(attachModes, ["adopt", "adopt"]);
        NodeAssert.equal((yield* runtime.getSession).status, "ready");
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

it.effect("retries adoption after the first attach fails in the same host generation", () => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-readopt-"));
  const socketPath = NodePath.join(root, "control.sock");
  const sockets = new Set<NodeNet.Socket>();
  const threadId = ThreadId.make("thread-provider-host-client-readopt");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const providerThreadId = "provider-thread-readopt";
  const now = "2026-01-01T00:00:00.000Z";
  const attachModes: Array<ClientEnvelope["mode"]> = [];
  let connectionCount = 0;
  let firstSocket: NodeNet.Socket | undefined;
  let resolveSecondAttach: () => void = () => undefined;
  const secondAttach = new Promise<void>((resolve) => {
    resolveSecondAttach = resolve;
  });
  let resolveSecondAttachClosed: () => void = () => undefined;
  const secondAttachClosed = new Promise<void>((resolve) => {
    resolveSecondAttachClosed = resolve;
  });
  let resolveThirdAttach: () => void = () => undefined;
  const thirdAttach = new Promise<void>((resolve) => {
    resolveThirdAttach = resolve;
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
        resolveSecondAttachClosed();
      }
    });
    socket.write(
      [
        JSON.stringify({
          version: PROVIDER_HOST_PROTOCOL_VERSION,
          type: "hello",
          providerInstanceId,
          buildFingerprint: "build-client-test",
          generationFingerprint:
            currentConnection === 1
              ? "generation-client-readopt-old"
              : "generation-client-readopt-new",
          appServerMode: currentConnection === 1 ? "spawn" : "attach",
          canAdoptSessions: currentConnection !== 1,
          hostProcess: {
            pid: currentConnection,
            startTimeMs: currentConnection,
          },
          startedAt: now,
          latestCursor: 0,
        }),
        JSON.stringify({
          version: PROVIDER_HOST_PROTOCOL_VERSION,
          type: "inventory",
          threads:
            currentConnection === 1
              ? [
                  {
                    threadId,
                    status: "active",
                    attachmentCount: 0,
                    cursor: 0,
                  },
                ]
              : [],
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
        attachModes.push(envelope.mode);
        if (attachModes.length === 2) {
          resolveSecondAttach();
        }
        if (attachModes.length === 3) {
          resolveThirdAttach();
        }
        if (currentConnection === 2) {
          socket.destroy();
          continue;
        }
        const state: ProviderSession = {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId,
          status: "ready",
          runtimeMode: "full-access",
          cwd: root,
          threadId,
          resumeCursor: { threadId: providerThreadId },
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

        const session = yield* runtime.start();
        firstSocket?.destroy();
        yield* Effect.promise(() => secondAttach);
        yield* Effect.promise(() => secondAttachClosed);
        yield* Effect.promise(
          () =>
            new Promise<void>((resolve) => {
              setImmediate(resolve);
            }),
        );
        yield* TestClock.adjust("1 second");
        yield* Effect.promise(() => thirdAttach);

        NodeAssert.equal(session.status, "ready");
        NodeAssert.deepStrictEqual(attachModes, ["create", "adopt", "adopt"]);
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
          buildFingerprint: "build-client-test",
          generationFingerprint: "generation-client-protocol-test",
          appServerMode: "spawn",
          canAdoptSessions: false,
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
          sessionMode: "reuse",
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
        buildFingerprint: "build-client-test",
        generationFingerprint: "generation-client-bounded-test",
        appServerMode: "spawn",
        canAdoptSessions: false,
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
            "session/reconnecting",
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
        buildFingerprint: "build-client-test",
        generationFingerprint: "generation-client-detach-test",
        appServerMode: "spawn",
        canAdoptSessions: false,
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
        buildFingerprint: "build-client-test",
        generationFingerprint: "generation-client-drain-test",
        appServerMode: "spawn",
        canAdoptSessions: false,
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
