// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalDate:off - Connection retries use a bounded wall-clock deadline.
// @effect-diagnostics globalTimers:off - Socket startup and assertion waits are bounded.
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ApprovalRequestId,
  CommandId,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderApprovalDecision,
  type ProviderEvent,
  type ProviderSession,
  type ProviderTurnStartResult,
  type ProviderUserInputAnswers,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { it, vi } from "@effect/vitest";
import * as NodeAssert from "node:assert/strict";
import type * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeFS from "node:fs";
import * as NodeHttp from "node:http";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { describe } from "vite-plus/test";

import {
  type CodexSessionRuntimeOptions,
  type CodexSessionRuntimeSendTurnInput,
  type CodexSessionRuntimeShape,
  type CodexThreadSnapshot,
} from "../Layers/CodexSessionRuntime.ts";
import {
  CODEX_PROVIDER_HOST_CONFIG_VERSION,
  CodexProviderHostConfig,
} from "./CodexProviderHostConfig.ts";
import {
  CODEX_PROVIDER_HOST_OPERATIONS,
  type CodexProviderHostSessionOptions,
} from "./CodexProviderHostSession.ts";
import {
  PROVIDER_HOST_PROTOCOL_VERSION,
  ProviderHostAttachmentId,
  ProviderHostClientId,
  ProviderHostGenerationFingerprint,
  ProviderHostReplayCursor,
  type ProviderHostClientEnvelope,
} from "./ProviderHostProtocol.ts";
import {
  __testing,
  runCodexProviderHost,
  type CodexProviderHostServerOptions,
} from "./CodexProviderHostServer.ts";

interface WireEnvelope {
  readonly type: string;
  readonly [key: string]: unknown;
}

interface PendingEnvelope {
  readonly predicate: (envelope: WireEnvelope) => boolean;
  readonly resolve: (envelope: WireEnvelope) => void;
  readonly reject: (cause: Error) => void;
  readonly timer: NodeJS.Timeout;
}

class TestClient {
  private readonly buffered: Array<WireEnvelope> = [];
  private readonly pending: Array<PendingEnvelope> = [];
  private readonly socket: NodeNet.Socket;
  private readonly closed: Promise<void>;
  private inbound = "";

  private constructor(socket: NodeNet.Socket) {
    this.socket = socket;
    this.closed = new Promise((resolve) => {
      socket.once("close", resolve);
    });
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => {
      this.inbound += chunk;
      while (true) {
        const newline = this.inbound.indexOf("\n");
        if (newline < 0) break;
        const line = this.inbound.slice(0, newline).trim();
        this.inbound = this.inbound.slice(newline + 1);
        if (!line) continue;
        this.accept(JSON.parse(line) as WireEnvelope);
      }
    });
    socket.on("error", () => undefined);
    socket.on("close", () => {
      const cause = new Error("Provider-host test connection closed.");
      for (const waiter of this.pending.splice(0)) {
        clearTimeout(waiter.timer);
        waiter.reject(cause);
      }
    });
  }

  static async connect(socketPath: string): Promise<TestClient> {
    const deadline = Date.now() + 3_000;
    let lastError: unknown;
    while (Date.now() < deadline) {
      const socket = NodeNet.createConnection(socketPath);
      const client = new TestClient(socket);
      try {
        await new Promise<void>((resolve, reject) => {
          socket.once("connect", resolve);
          socket.once("error", reject);
        });
        return client;
      } catch (cause) {
        lastError = cause;
        socket.destroy();
        await new Promise<void>((resolve) => setTimeout(resolve, 10));
      }
    }
    throw lastError instanceof Error
      ? lastError
      : new Error(`Timed out connecting to ${socketPath}.`);
  }

  send(envelope: ProviderHostClientEnvelope): void {
    this.socket.write(`${JSON.stringify(envelope)}\n`);
  }

  sendMany(envelopes: ReadonlyArray<ProviderHostClientEnvelope>): void {
    this.socket.write(`${envelopes.map((envelope) => JSON.stringify(envelope)).join("\n")}\n`);
  }

  take(predicate: (envelope: WireEnvelope) => boolean): Promise<WireEnvelope> {
    const bufferedIndex = this.buffered.findIndex(predicate);
    if (bufferedIndex >= 0) {
      return Promise.resolve(this.buffered.splice(bufferedIndex, 1)[0]!);
    }
    return new Promise((resolve, reject) => {
      const waiter: PendingEnvelope = {
        predicate,
        resolve,
        reject,
        timer: setTimeout(() => {
          const index = this.pending.indexOf(waiter);
          if (index >= 0) this.pending.splice(index, 1);
          reject(new Error("Timed out waiting for a provider-host envelope."));
        }, 3_000),
      };
      this.pending.push(waiter);
    });
  }

  has(predicate: (envelope: WireEnvelope) => boolean): boolean {
    return this.buffered.some(predicate);
  }

  async close(): Promise<void> {
    if (this.socket.destroyed) return;
    await new Promise<void>((resolve) => {
      this.socket.once("close", resolve);
      this.socket.destroy();
    });
  }

  waitClosed(): Promise<void> {
    return this.closed;
  }

  private accept(envelope: WireEnvelope): void {
    const pendingIndex = this.pending.findIndex(({ predicate }) => predicate(envelope));
    if (pendingIndex < 0) {
      this.buffered.push(envelope);
      return;
    }
    const waiter = this.pending.splice(pendingIndex, 1)[0]!;
    clearTimeout(waiter.timer);
    waiter.resolve(envelope);
  }
}

class FakeCodexRuntime implements CodexSessionRuntimeShape {
  private readonly eventQueue: Queue.Queue<ProviderEvent, Cause.Done<void>>;
  private readonly eventOnStart: ProviderEvent | undefined;
  private readonly startError: Error | undefined;
  private emittedEventCountValue = 0;
  private readonly now = "2026-07-31T12:00:00.000Z";
  private readonly session: ProviderSession;
  readonly options: CodexSessionRuntimeOptions;

  readonly startImpl = vi.fn(async () => {
    if (this.startError) {
      throw this.startError;
    }
    if (this.eventOnStart) {
      await this.emit(this.eventOnStart);
    }
    return this.session;
  });
  readonly getSessionImpl = vi.fn(() => Promise.resolve(this.session));
  readonly sendTurnImpl = vi.fn(
    (_input: CodexSessionRuntimeSendTurnInput): Promise<ProviderTurnStartResult> =>
      Promise.resolve({
        threadId: this.options.threadId,
        turnId: TurnId.make("turn-1"),
      }),
  );
  readonly interruptTurnImpl = vi.fn((_turnId?: TurnId) => Promise.resolve());
  readonly readThreadImpl = vi.fn(
    (): Promise<CodexThreadSnapshot> =>
      Promise.resolve({
        threadId: "provider-thread-1",
        turns: [],
      }),
  );
  readonly rollbackThreadImpl = vi.fn(
    (_numTurns: number): Promise<CodexThreadSnapshot> =>
      Promise.resolve({
        threadId: "provider-thread-1",
        turns: [],
      }),
  );
  readonly respondToRequestImpl = vi.fn(
    (_requestId: ApprovalRequestId, _decision: ProviderApprovalDecision) => Promise.resolve(),
  );
  readonly respondToUserInputImpl = vi.fn(
    (_requestId: ApprovalRequestId, _answers: ProviderUserInputAnswers) => Promise.resolve(),
  );
  readonly detachImpl = vi.fn(() => Promise.resolve());
  readonly closeImpl = vi.fn(() => Promise.resolve());

  constructor(
    options: CodexSessionRuntimeOptions,
    eventQueue: Queue.Queue<ProviderEvent, Cause.Done<void>>,
    eventOnStart: ProviderEvent | undefined,
    startError: Error | undefined,
  ) {
    this.options = options;
    this.eventQueue = eventQueue;
    this.eventOnStart = eventOnStart;
    this.startError = startError;
    this.session = {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: options.providerInstanceId,
      status: "ready",
      runtimeMode: options.runtimeMode,
      threadId: options.threadId,
      cwd: options.cwd,
      createdAt: this.now,
      updatedAt: this.now,
    };
  }

  start() {
    return Effect.promise(() => this.startImpl());
  }

  getSession = Effect.promise(() => this.getSessionImpl());
  get emittedEventCount() {
    return Effect.sync(() => this.emittedEventCountValue);
  }

  sendTurn(input: CodexSessionRuntimeSendTurnInput) {
    return Effect.promise(() => this.sendTurnImpl(input));
  }

  interruptTurn(turnId?: TurnId) {
    return Effect.promise(() => this.interruptTurnImpl(turnId));
  }

  readThread = Effect.promise(() => this.readThreadImpl());

  rollbackThread(numTurns: number) {
    return Effect.promise(() => this.rollbackThreadImpl(numTurns));
  }

  respondToRequest(requestId: ApprovalRequestId, decision: ProviderApprovalDecision) {
    return Effect.promise(() => this.respondToRequestImpl(requestId, decision));
  }

  respondToUserInput(requestId: ApprovalRequestId, answers: ProviderUserInputAnswers) {
    return Effect.promise(() => this.respondToUserInputImpl(requestId, answers));
  }

  get events() {
    return Stream.fromQueue(this.eventQueue);
  }
  get detach() {
    return Effect.promise(() => this.detachImpl()).pipe(
      Effect.andThen(Queue.end(this.eventQueue)),
      Effect.asVoid,
    );
  }
  get close() {
    return Effect.promise(() => this.closeImpl()).pipe(
      Effect.andThen(Queue.end(this.eventQueue)),
      Effect.asVoid,
    );
  }

  emit(event: ProviderEvent): Promise<void> {
    if (!Queue.offerUnsafe(this.eventQueue, event)) {
      return Promise.reject(new Error("Provider-host test event queue rejected an event."));
    }
    this.emittedEventCountValue += 1;
    return Promise.resolve();
  }
}

interface HostTestContext {
  readonly root: string;
  readonly controlSocketPath: string;
  readonly threadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly runtimes: ReadonlyArray<FakeCodexRuntime>;
  readonly runtimeCreationCount: () => number;
  readonly waitForRuntimeCreation: () => Promise<void>;
  readonly blockRuntimeCreation: () => () => void;
  readonly emitOnRuntimeStart: (event: ProviderEvent) => void;
  readonly failNextRuntimeStart: (cause: Error) => void;
  readonly connect: () => Promise<TestClient>;
  readonly exitCodex: (code: number | null, signal?: NodeJS.Signals | null) => void;
}

const decodeConfig = Schema.decodeUnknownSync(CodexProviderHostConfig);
const decodeJson = Schema.decodeUnknownSync(Schema.Json);

function makeAttach(
  context: HostTestContext,
  clientId: string,
  attachmentId: string,
  replayFrom?: number,
  sessionOverrides?: Partial<CodexProviderHostSessionOptions>,
): ProviderHostClientEnvelope {
  const session = {
    threadId: context.threadId,
    providerInstanceId: context.providerInstanceId,
    cwd: context.root,
    runtimeMode: "full-access",
    ...sessionOverrides,
  } satisfies CodexProviderHostSessionOptions;
  return {
    version: PROVIDER_HOST_PROTOCOL_VERSION,
    type: "attach",
    clientId: ProviderHostClientId.make(clientId),
    attachmentId: ProviderHostAttachmentId.make(attachmentId),
    threadId: context.threadId,
    ...(replayFrom !== undefined ? { replayFrom: ProviderHostReplayCursor.make(replayFrom) } : {}),
    session: decodeJson(session),
  };
}

function makeDetach(
  context: HostTestContext,
  clientId: string,
  attachmentId: string,
): ProviderHostClientEnvelope {
  return {
    version: PROVIDER_HOST_PROTOCOL_VERSION,
    type: "detach",
    clientId: ProviderHostClientId.make(clientId),
    attachmentId: ProviderHostAttachmentId.make(attachmentId),
    threadId: context.threadId,
  };
}

function makeReaderAttach(
  context: HostTestContext,
  clientId: string,
  attachmentId: string,
  replayFrom?: number,
): ProviderHostClientEnvelope {
  return {
    version: PROVIDER_HOST_PROTOCOL_VERSION,
    type: "attach",
    clientId: ProviderHostClientId.make(clientId),
    attachmentId: ProviderHostAttachmentId.make(attachmentId),
    threadId: context.threadId,
    ...(replayFrom !== undefined ? { replayFrom: ProviderHostReplayCursor.make(replayFrom) } : {}),
  };
}

function makeCommand(
  context: HostTestContext,
  input: {
    readonly clientId: string;
    readonly attachmentId: string;
    readonly commandId: string;
    readonly operation: string;
    readonly payload?: Schema.Json;
  },
): ProviderHostClientEnvelope {
  return {
    version: PROVIDER_HOST_PROTOCOL_VERSION,
    type: "command",
    clientId: ProviderHostClientId.make(input.clientId),
    attachmentId: ProviderHostAttachmentId.make(input.attachmentId),
    commandId: CommandId.make(input.commandId),
    threadId: context.threadId,
    operation: input.operation,
    payload: input.payload ?? {},
  };
}

const isSnapshotFor = (threadId: ThreadId) => (envelope: WireEnvelope) =>
  envelope.type === "snapshot" && envelope.threadId === threadId;

const isEvent = (eventId: string) => (envelope: WireEnvelope) =>
  envelope.type === "event" &&
  typeof envelope.event === "object" &&
  envelope.event !== null &&
  "id" in envelope.event &&
  envelope.event.id === eventId;

const isCommandResult = (commandId: string) => (envelope: WireEnvelope) =>
  envelope.type === "commandResult" && envelope.commandId === commandId;

const nextImmediate = () => new Promise<void>((resolve) => setImmediate(resolve));

const listen = (server: NodeHttp.Server, socketPath: string): Promise<void> =>
  new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, () => {
      server.removeListener("error", reject);
      resolve();
    });
  });

const closeAppServer = (
  server: NodeHttp.Server,
  webSocketServer: NodeSocket.NodeWS.WebSocketServer,
): Promise<void> =>
  new Promise((resolve) => {
    for (const client of webSocketServer.clients) {
      client.terminate();
    }
    webSocketServer.close(() => {
      server.close(() => resolve());
    });
  });

function withHost(
  run: (context: HostTestContext) => Promise<void>,
  hostOptions?: Pick<
    CodexProviderHostServerOptions,
    | "idleTimeoutMs"
    | "maxCommandFibers"
    | "maxPendingEnvelopeBytesPerConnection"
    | "maxPendingEnvelopesPerConnection"
    | "maxReplayBytes"
    | "maxReplayEvents"
    | "priorityCommandFiberReserve"
  >,
): Effect.Effect<void> {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-host-server-"));
  const controlSocketPath = NodePath.join(root, "control.sock");
  const appServerSocketPath = NodePath.join(root, "app-server.sock");
  const appServer = NodeHttp.createServer();
  const webSocketServer = new NodeSocket.NodeWS.WebSocketServer({
    perMessageDeflate: false,
    server: appServer,
  });
  const threadId = ThreadId.make("thread-provider-host-test");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const runtimes: Array<FakeCodexRuntime> = [];
  let eventOnRuntimeStart: ProviderEvent | undefined;
  let nextRuntimeStartError: Error | undefined;
  let runtimeCreationGate: Promise<void> | undefined;
  let resolveRuntimeCreationStarted: () => void = () => undefined;
  const runtimeCreationStarted = new Promise<void>((resolve) => {
    resolveRuntimeCreationStarted = resolve;
  });
  const clients = new Set<TestClient>();
  const makeRuntime = vi.fn((options: CodexSessionRuntimeOptions) => {
    resolveRuntimeCreationStarted();
    const startError = nextRuntimeStartError;
    nextRuntimeStartError = undefined;
    return Effect.gen(function* () {
      const gate = runtimeCreationGate;
      if (gate) {
        yield* Effect.promise(() => gate);
      }
      const eventQueue = yield* Queue.unbounded<ProviderEvent, Cause.Done<void>>();
      const runtime = new FakeCodexRuntime(options, eventQueue, eventOnRuntimeStart, startError);
      runtimes.push(runtime);
      return runtime;
    });
  });
  const codexChildEmitter = new NodeEvents.EventEmitter();
  const codexChild = Object.assign(codexChildEmitter, {
    pid: 42_424,
    exitCode: null as number | null,
    signalCode: null as NodeJS.Signals | null,
    kill(signal: NodeJS.Signals = "SIGTERM") {
      if (this.exitCode !== null || this.signalCode !== null) return false;
      this.signalCode = signal;
      queueMicrotask(() => codexChildEmitter.emit("exit", null, signal));
      return true;
    },
  }) as NodeChildProcess.ChildProcess;
  const spawnCodex: NonNullable<CodexProviderHostServerOptions["spawnCodex"]> = vi.fn(() =>
    Promise.resolve(codexChild),
  );
  const config = decodeConfig({
    version: CODEX_PROVIDER_HOST_CONFIG_VERSION,
    providerInstanceId,
    generationFingerprint: ProviderHostGenerationFingerprint.make("generation-test"),
    controlSocketPath,
    appServerSocketPath,
    manifestPath: NodePath.join(root, "manifest.json"),
    codex: {
      binaryPath: process.execPath,
      cwd: root,
    },
  });

  return Effect.scoped(
    Effect.gen(function* () {
      yield* Effect.promise(() => listen(appServer, appServerSocketPath));
      yield* runCodexProviderHost(config, {
        makeRuntime,
        spawnCodex,
        ...hostOptions,
      }).pipe(Effect.forkScoped);
      const context: HostTestContext = {
        root,
        controlSocketPath,
        threadId,
        providerInstanceId,
        runtimes,
        runtimeCreationCount: () => makeRuntime.mock.calls.length,
        waitForRuntimeCreation: () => runtimeCreationStarted,
        blockRuntimeCreation: () => {
          let releaseGate: () => void = () => undefined;
          runtimeCreationGate = new Promise<void>((resolve) => {
            releaseGate = resolve;
          });
          return () => {
            runtimeCreationGate = undefined;
            releaseGate();
          };
        },
        emitOnRuntimeStart: (event) => {
          eventOnRuntimeStart = event;
        },
        failNextRuntimeStart: (cause) => {
          nextRuntimeStartError = cause;
        },
        connect: async () => {
          const client = await TestClient.connect(controlSocketPath);
          clients.add(client);
          return client;
        },
        exitCodex: (code, signal = null) => {
          if (codexChild.exitCode !== null || codexChild.signalCode !== null) return;
          Object.assign(codexChild, {
            exitCode: code,
            signalCode: signal,
          });
          codexChild.emit("exit", code, signal);
        },
      };
      yield* Effect.promise(() => run(context));
    }),
  ).pipe(
    Effect.ensuring(
      Effect.promise(async () => {
        await Promise.all(Array.from(clients, (client) => client.close()));
        await closeAppServer(appServer, webSocketServer);
        NodeFS.rmSync(root, { recursive: true, force: true });
      }),
    ),
    Effect.provide(NodeServices.layer),
  );
}

describe("CodexProviderHostServer", () => {
  it("terminates a captured Codex child whose SIGTERM exit is synchronous", async () => {
    vi.useFakeTimers();
    try {
      const emitter = new NodeEvents.EventEmitter();
      const child = Object.assign(emitter, {
        pid: 42_424,
        exitCode: null as number | null,
        signalCode: null as NodeJS.Signals | null,
      }) as NodeChildProcess.ChildProcess;
      const kill = vi.fn((signal?: NodeJS.Signals | number) => {
        if (signal === "SIGTERM") {
          Object.assign(child, { signalCode: "SIGTERM" });
          emitter.emit("exit", null, "SIGTERM");
        }
        return true;
      });
      child.kill = kill;

      await __testing.terminateChild(child);

      NodeAssert.deepStrictEqual(kill.mock.calls, [["SIGTERM"]]);
      NodeAssert.equal(vi.getTimerCount(), 0);
    } finally {
      vi.useRealTimers();
    }
  });

  it.effect("shuts down an unused provider host after the zero-session idle lease", () =>
    withHost(
      async (context) => {
        const client = await context.connect();
        await client.take((envelope) => envelope.type === "hello");
        await client.waitClosed();

        const deadline = Date.now() + 1_000;
        while (NodeFS.existsSync(context.controlSocketPath) && Date.now() < deadline) {
          await nextImmediate();
        }
        NodeAssert.equal(NodeFS.existsSync(context.controlSocketPath), false);
        NodeAssert.equal(context.runtimeCreationCount(), 0);
      },
      { idleTimeoutMs: 10 },
    ),
  );

  it.effect("closes the provider host when its Codex child exits", () =>
    withHost(async (context) => {
      const client = await context.connect();
      await client.take((envelope) => envelope.type === "hello");

      context.exitCodex(17);
      await client.waitClosed();

      const deadline = Date.now() + 1_000;
      while (NodeFS.existsSync(context.controlSocketPath) && Date.now() < deadline) {
        await nextImmediate();
      }
      NodeAssert.equal(NodeFS.existsSync(context.controlSocketPath), false);
    }),
  );

  it.effect("lets two attachments observe one shared runtime session", () =>
    withHost(async (context) => {
      const first = await context.connect();
      const second = await context.connect();
      const releaseRuntimeCreation = context.blockRuntimeCreation();
      first.send(makeAttach(context, "client-first", "attachment-first"));
      second.send(makeAttach(context, "client-second", "attachment-second"));
      await context.waitForRuntimeCreation();
      NodeAssert.equal(context.runtimeCreationCount(), 1);
      releaseRuntimeCreation();

      const [firstSnapshot, secondSnapshot] = await Promise.all([
        first.take(isSnapshotFor(context.threadId)),
        second.take(isSnapshotFor(context.threadId)),
      ]);
      const runtime = context.runtimes[0]!;
      NodeAssert.equal(runtime.startImpl.mock.calls.length, 1);
      NodeAssert.deepStrictEqual(firstSnapshot.state, secondSnapshot.state);

      const event: ProviderEvent = {
        id: EventId.make("event-shared"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: context.providerInstanceId,
        threadId: context.threadId,
        createdAt: "2026-07-31T12:01:00.000Z",
        method: "item/agentMessage/delta",
        textDelta: "shared output",
      };
      const firstEvent = first.take(isEvent(event.id));
      const secondEvent = second.take(isEvent(event.id));
      await runtime.emit(event);

      const [firstObserved, secondObserved] = await Promise.all([firstEvent, secondEvent]);
      NodeAssert.deepStrictEqual(firstObserved, secondObserved);
    }),
  );

  it.effect("attaches a reader without session options to an existing runtime", () =>
    withHost(async (context) => {
      const owner = await context.connect();
      owner.send(makeAttach(context, "client-owner", "attachment-owner"));
      await owner.take(isSnapshotFor(context.threadId));

      const reader = await context.connect();
      reader.send(makeReaderAttach(context, "client-reader", "attachment-reader"));
      await reader.take(isSnapshotFor(context.threadId));

      NodeAssert.equal(context.runtimeCreationCount(), 1);
      const event: ProviderEvent = {
        id: EventId.make("event-reader-only-attachment"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: context.providerInstanceId,
        threadId: context.threadId,
        createdAt: "2026-08-01T12:01:15.000Z",
        method: "item/agentMessage/delta",
        textDelta: "reader-only attachment",
      };
      const observed = reader.take(isEvent(event.id));
      await context.runtimes[0]!.emit(event);
      await observed;
    }),
  );

  it.effect("closes a reader-only attachment when no runtime exists", () =>
    withHost(async (context) => {
      const reader = await context.connect();
      reader.send(makeReaderAttach(context, "client-reader", "attachment-reader"));

      await reader.waitClosed();
      NodeAssert.equal(context.runtimeCreationCount(), 0);
    }),
  );

  it.effect("does not publish an attachment after its connection disconnects", () =>
    withHost(async (context) => {
      const releaseRuntimeCreation = context.blockRuntimeCreation();
      const disconnected = await context.connect();
      disconnected.send(makeAttach(context, "client-disconnected", "attachment-disconnected"));
      await context.waitForRuntimeCreation();
      await disconnected.close();

      releaseRuntimeCreation();
      const deadline = Date.now() + 1_000;
      while (
        (context.runtimes[0]?.startImpl.mock.calls.length ?? 0) === 0 &&
        Date.now() < deadline
      ) {
        await nextImmediate();
      }
      NodeAssert.equal(context.runtimes[0]?.startImpl.mock.calls.length, 1);

      const inventoryReader = await context.connect();
      const inventory = await inventoryReader.take((envelope) => envelope.type === "inventory");
      const threads = inventory.threads as ReadonlyArray<{
        readonly threadId: string;
        readonly attachmentCount: number;
      }>;
      NodeAssert.equal(
        threads.find((thread) => thread.threadId === context.threadId)?.attachmentCount,
        0,
      );
    }),
  );

  it.effect("replays output emitted while the first attachment creates its runtime", () =>
    withHost(async (context) => {
      const event: ProviderEvent = {
        id: EventId.make("event-during-runtime-start"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: context.providerInstanceId,
        threadId: context.threadId,
        createdAt: "2026-07-31T12:01:30.000Z",
        method: "item/agentMessage/delta",
        textDelta: "created before attachment registration",
      };
      context.emitOnRuntimeStart(event);

      const client = await context.connect();
      client.send(makeAttach(context, "client-first", "attachment-first"));

      const firstReconciliationEnvelope = await client.take(
        (envelope) => isSnapshotFor(context.threadId)(envelope) || isEvent(event.id)(envelope),
      );
      NodeAssert.equal(firstReconciliationEnvelope.type, "event");
      await client.take(isSnapshotFor(context.threadId));
    }),
  );

  it.effect("keeps the runtime alive when readers detach or disconnect", () =>
    withHost(async (context) => {
      const detached = await context.connect();
      const observer = await context.connect();
      detached.send(makeAttach(context, "client-detached", "attachment-detached"));
      observer.send(makeAttach(context, "client-observer", "attachment-observer"));
      await Promise.all([
        detached.take(isSnapshotFor(context.threadId)),
        observer.take(isSnapshotFor(context.threadId)),
      ]);
      const runtime = context.runtimes[0]!;

      detached.send(makeDetach(context, "client-detached", "attachment-detached"));
      await nextImmediate();
      const event: ProviderEvent = {
        id: EventId.make("event-after-detach"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: context.providerInstanceId,
        threadId: context.threadId,
        createdAt: "2026-07-31T12:02:00.000Z",
        method: "item/agentMessage/delta",
        textDelta: "still running",
      };
      const observed = observer.take(isEvent(event.id));
      await runtime.emit(event);
      await observed;
      await nextImmediate();

      NodeAssert.equal(detached.has(isEvent(event.id)), false);
      NodeAssert.equal(runtime.interruptTurnImpl.mock.calls.length, 0);
      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 0);

      const rejectedCommand = detached.take(isCommandResult("command-after-detach"));
      detached.send(
        makeCommand(context, {
          clientId: "client-detached",
          attachmentId: "attachment-detached",
          commandId: "command-after-detach",
          operation: CODEX_PROVIDER_HOST_OPERATIONS.sendTurn,
          payload: { input: "must not run" },
        }),
      );
      NodeAssert.equal((await rejectedCommand).ok, false);
      NodeAssert.equal(runtime.sendTurnImpl.mock.calls.length, 0);

      await observer.close();
      await nextImmediate();
      const reattached = await context.connect();
      reattached.send(makeAttach(context, "client-reattached", "attachment-reattached"));
      await reattached.take(isSnapshotFor(context.threadId));

      NodeAssert.equal(context.runtimeCreationCount(), 1);
      NodeAssert.equal(runtime.interruptTurnImpl.mock.calls.length, 0);
      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 0);
    }),
  );

  it.effect("replaces an errored runtime without stopping its Codex app-server session", () =>
    withHost(async (context) => {
      const first = await context.connect();
      first.send(makeAttach(context, "client-first", "attachment-first"));
      const firstSnapshot = await first.take(isSnapshotFor(context.threadId));
      const original = context.runtimes[0]!;
      original.getSessionImpl.mockResolvedValue({
        ...(firstSnapshot.state as ProviderSession),
        status: "error",
        resumeCursor: { threadId: "provider-thread-reconnect" },
        lastError: "Codex App Server connection closed.",
      });

      const second = await context.connect();
      second.send(makeAttach(context, "client-second", "attachment-second"));
      await second.take(isSnapshotFor(context.threadId));

      NodeAssert.equal(context.runtimeCreationCount(), 2);
      NodeAssert.equal(original.detachImpl.mock.calls.length, 1);
      NodeAssert.equal(original.closeImpl.mock.calls.length, 0);
      const replacement = context.runtimes[1]!;
      NodeAssert.deepStrictEqual(replacement.options.resumeCursor, {
        threadId: "provider-thread-reconnect",
      });

      const event: ProviderEvent = {
        id: EventId.make("event-after-runtime-replacement"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: context.providerInstanceId,
        threadId: context.threadId,
        createdAt: "2026-07-31T12:02:30.000Z",
        method: "item/agentMessage/delta",
        textDelta: "replacement output",
      };
      const observedByOriginalAttachment = first.take(isEvent(event.id));
      await replacement.emit(event);
      await observedByOriginalAttachment;
    }),
  );

  it.effect("broadcasts replacement startup output to existing readers", () =>
    withHost(async (context) => {
      const existingReader = await context.connect();
      existingReader.send(makeAttach(context, "client-existing", "attachment-existing"));
      const firstSnapshot = await existingReader.take(isSnapshotFor(context.threadId));
      const original = context.runtimes[0]!;
      original.getSessionImpl.mockResolvedValue({
        ...(firstSnapshot.state as ProviderSession),
        status: "error",
        resumeCursor: { threadId: "provider-thread-startup-broadcast" },
        lastError: "Codex App Server connection closed.",
      });

      const startupEvent: ProviderEvent = {
        id: EventId.make("event-replacement-runtime-start"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: context.providerInstanceId,
        threadId: context.threadId,
        createdAt: "2026-08-01T12:02:45.000Z",
        method: "item/agentMessage/delta",
        textDelta: "replacement started",
      };
      context.emitOnRuntimeStart(startupEvent);

      const replacementReader = await context.connect();
      const existingObserved = existingReader.take(isEvent(startupEvent.id));
      replacementReader.send(makeAttach(context, "client-replacement", "attachment-replacement"));

      await Promise.all([
        existingObserved,
        replacementReader.take(isSnapshotFor(context.threadId)),
      ]);
      NodeAssert.equal(context.runtimeCreationCount(), 2);
    }),
  );

  it.effect("removes a provisional replacement when runtime startup fails", () =>
    withHost(async (context) => {
      const existingReader = await context.connect();
      existingReader.send(makeAttach(context, "client-existing", "attachment-existing"));
      const firstSnapshot = await existingReader.take(isSnapshotFor(context.threadId));
      const original = context.runtimes[0]!;
      original.getSessionImpl.mockResolvedValue({
        ...(firstSnapshot.state as ProviderSession),
        status: "error",
        resumeCursor: { threadId: "provider-thread-startup-retry" },
        lastError: "Codex App Server connection closed.",
      });
      context.failNextRuntimeStart(new Error("replacement startup failed"));

      const failedReader = await context.connect();
      failedReader.send(makeAttach(context, "client-failed", "attachment-failed"));
      await failedReader.waitClosed();
      NodeAssert.equal(context.runtimeCreationCount(), 2);

      const retryingReader = await context.connect();
      retryingReader.send(makeAttach(context, "client-retry", "attachment-retry"));
      await retryingReader.take(isSnapshotFor(context.threadId));

      NodeAssert.equal(context.runtimeCreationCount(), 3);
    }),
  );

  it.effect("drains queued output before replacing a runtime attachment", () =>
    withHost(async (context) => {
      const first = await context.connect();
      first.send(makeAttach(context, "client-first", "attachment-first"));
      const firstSnapshot = await first.take(isSnapshotFor(context.threadId));
      const original = context.runtimes[0]!;
      let markSnapshotReadStarted: () => void = () => undefined;
      const snapshotReadStarted = new Promise<void>((resolve) => {
        markSnapshotReadStarted = resolve;
      });
      let releaseSnapshotRead: (session: ProviderSession) => void = () => undefined;
      const blockedSnapshotRead = new Promise<ProviderSession>((resolve) => {
        releaseSnapshotRead = resolve;
      });
      original.getSessionImpl
        .mockResolvedValueOnce(firstSnapshot.state as ProviderSession)
        .mockImplementationOnce(() => {
          markSnapshotReadStarted();
          return blockedSnapshotRead;
        });

      const blockingAttachment = await context.connect();
      blockingAttachment.send(makeAttach(context, "client-blocking", "attachment-blocking"));
      await snapshotReadStarted;

      const event: ProviderEvent = {
        id: EventId.make("event-before-runtime-replacement"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: context.providerInstanceId,
        threadId: context.threadId,
        createdAt: "2026-08-01T00:00:00.000Z",
        method: "item/agentMessage/delta",
        textDelta: "must survive runtime replacement",
      };
      const observed = first.take(isEvent(event.id));
      await original.emit(event);

      const replacementAttachment = await context.connect();
      replacementAttachment.send(
        makeAttach(context, "client-replacement", "attachment-replacement", undefined, {
          threadConfig: { model_instructions: "rotated runtime configuration" },
        }),
      );
      const replacementDeadline = Date.now() + 1_000;
      while (original.detachImpl.mock.calls.length === 0 && Date.now() < replacementDeadline) {
        await nextImmediate();
      }
      NodeAssert.equal(original.detachImpl.mock.calls.length, 1);

      releaseSnapshotRead(firstSnapshot.state as ProviderSession);
      await observed;
      await replacementAttachment.take(isSnapshotFor(context.threadId));

      NodeAssert.equal(context.runtimeCreationCount(), 2);
      NodeAssert.equal(blockingAttachment.has(isSnapshotFor(context.threadId)), false);
    }),
  );

  it.effect("refreshes runtime configuration when a reattachment rotates MCP credentials", () =>
    withHost(async (context) => {
      const oldThreadConfig = {
        mcp_servers: {
          "t3-code": {
            url: "http://127.0.0.1:3000/mcp",
            http_headers: { Authorization: "Bearer old-token" },
          },
        },
      };
      const first = await context.connect();
      first.send(
        makeAttach(context, "client-first", "attachment-first", undefined, {
          threadConfig: oldThreadConfig,
        }),
      );
      const firstSnapshot = await first.take(isSnapshotFor(context.threadId));
      const original = context.runtimes[0]!;
      original.getSessionImpl.mockResolvedValue({
        ...(firstSnapshot.state as ProviderSession),
        resumeCursor: { threadId: "provider-thread-mcp-refresh" },
      });

      const newThreadConfig = {
        mcp_servers: {
          "t3-code": {
            url: "http://127.0.0.1:3000/mcp",
            http_headers: { Authorization: "Bearer new-token" },
          },
        },
      };
      const second = await context.connect();
      second.send(
        makeAttach(context, "client-second", "attachment-second", undefined, {
          threadConfig: newThreadConfig,
        }),
      );
      await second.take(isSnapshotFor(context.threadId));

      NodeAssert.equal(context.runtimeCreationCount(), 2);
      NodeAssert.equal(original.detachImpl.mock.calls.length, 1);
      NodeAssert.equal(original.closeImpl.mock.calls.length, 0);
      NodeAssert.deepStrictEqual(context.runtimes[1]?.options.threadConfig, newThreadConfig);
      NodeAssert.deepStrictEqual(context.runtimes[1]?.options.resumeCursor, {
        threadId: "provider-thread-mcp-refresh",
      });
    }),
  );

  it.effect("waits for an in-flight replacement before honoring an explicit stop", () =>
    withHost(async (context) => {
      const first = await context.connect();
      first.send(makeAttach(context, "client-first", "attachment-first"));
      await first.take(isSnapshotFor(context.threadId));
      const original = context.runtimes[0]!;
      const releaseRuntimeCreation = context.blockRuntimeCreation();

      const replacementAttachment = await context.connect();
      replacementAttachment.send(
        makeAttach(context, "client-replacement", "attachment-replacement", undefined, {
          threadConfig: { model_instructions: "replacement in progress" },
        }),
      );
      const replacementDeadline = Date.now() + 1_000;
      while (context.runtimeCreationCount() < 2 && Date.now() < replacementDeadline) {
        await nextImmediate();
      }
      NodeAssert.equal(context.runtimeCreationCount(), 2);

      first.send(
        makeCommand(context, {
          clientId: "client-first",
          attachmentId: "attachment-first",
          commandId: "command-stop-during-replacement",
          operation: CODEX_PROVIDER_HOST_OPERATIONS.stopSession,
        }),
      );
      await nextImmediate();
      NodeAssert.equal(first.has(isCommandResult("command-stop-during-replacement")), false);

      releaseRuntimeCreation();
      const stopped = await first.take(isCommandResult("command-stop-during-replacement"));
      const replacement = context.runtimes[1]!;

      NodeAssert.equal(stopped.ok, true);
      NodeAssert.equal(original.detachImpl.mock.calls.length, 1);
      NodeAssert.equal(original.closeImpl.mock.calls.length, 0);
      NodeAssert.equal(replacement.interruptTurnImpl.mock.calls.length, 1);
      NodeAssert.equal(replacement.closeImpl.mock.calls.length, 1);
    }),
  );

  it.effect("coalesces duplicate command ids and returns the same result", () =>
    withHost(async (context) => {
      const client = await context.connect();
      client.send(makeAttach(context, "client-writer", "attachment-writer"));
      await client.take(isSnapshotFor(context.threadId));
      const runtime = context.runtimes[0]!;
      let resolveStarted: () => void = () => undefined;
      const started = new Promise<void>((resolve) => {
        resolveStarted = resolve;
      });
      let resolveTurn: (result: ProviderTurnStartResult) => void = () => undefined;
      const turn = new Promise<ProviderTurnStartResult>((resolve) => {
        resolveTurn = resolve;
      });
      runtime.sendTurnImpl.mockImplementation(() => {
        resolveStarted();
        return turn;
      });

      const command = makeCommand(context, {
        clientId: "client-writer",
        attachmentId: "attachment-writer",
        commandId: "command-duplicate",
        operation: CODEX_PROVIDER_HOST_OPERATIONS.sendTurn,
        payload: { input: "Proceed" },
      });
      const firstResult = client.take(isCommandResult("command-duplicate"));
      const secondResult = client.take(isCommandResult("command-duplicate"));
      client.sendMany([command, command]);
      await started;
      await nextImmediate();

      NodeAssert.equal(runtime.sendTurnImpl.mock.calls.length, 1);
      resolveTurn({
        threadId: context.threadId,
        turnId: TurnId.make("turn-deduplicated"),
      });
      const [first, second] = await Promise.all([firstResult, secondResult]);

      NodeAssert.deepStrictEqual(first, second);
      NodeAssert.equal(first.ok, true);
      NodeAssert.deepStrictEqual(first.result, {
        threadId: context.threadId,
        turnId: "turn-deduplicated",
      });
    }),
  );

  it.effect("replaces intrinsically oversized command results before caching them", () =>
    withHost(async (context) => {
      const client = await context.connect();
      client.send(makeAttach(context, "client-oversized", "attachment-oversized"));
      await client.take(isSnapshotFor(context.threadId));
      const runtime = context.runtimes[0]!;
      runtime.readThreadImpl.mockResolvedValue({
        threadId: "provider-thread-oversized",
        turns: [
          {
            id: TurnId.make("turn-oversized"),
            items: ["x".repeat(__testing.maxOutboundFrameBytes + 1)],
          },
        ],
      } as unknown as CodexThreadSnapshot);

      const command = makeCommand(context, {
        clientId: "client-oversized",
        attachmentId: "attachment-oversized",
        commandId: "command-oversized",
        operation: CODEX_PROVIDER_HOST_OPERATIONS.readThread,
      });
      client.send(command);
      const rejected = await client.take(isCommandResult("command-oversized"));
      NodeAssert.equal(rejected.ok, false);
      NodeAssert.match(String(rejected.error), /exceeded.*frame limit/);

      client.send(command);
      NodeAssert.deepStrictEqual(await client.take(isCommandResult("command-oversized")), rejected);
      NodeAssert.equal(runtime.readThreadImpl.mock.calls.length, 1);

      client.send(
        makeCommand(context, {
          clientId: "client-oversized",
          attachmentId: "attachment-oversized",
          commandId: "command-after-oversized",
          operation: CODEX_PROVIDER_HOST_OPERATIONS.interruptTurn,
        }),
      );
      NodeAssert.equal((await client.take(isCommandResult("command-after-oversized"))).ok, true);
    }),
  );

  it.effect("processes an attach before a command received in the same frame", () =>
    withHost(async (context) => {
      const client = await context.connect();
      const command = makeCommand(context, {
        clientId: "client-ordered",
        attachmentId: "attachment-ordered",
        commandId: "command-ordered",
        operation: CODEX_PROVIDER_HOST_OPERATIONS.readThread,
      });
      client.sendMany([makeAttach(context, "client-ordered", "attachment-ordered"), command]);

      await client.take(isSnapshotFor(context.threadId));
      const result = await client.take(isCommandResult("command-ordered"));

      NodeAssert.equal(result.ok, true);
      NodeAssert.equal(context.runtimes[0]?.readThreadImpl.mock.calls.length, 1);
    }),
  );

  it.effect("replays retained output only when an attachment supplies a cursor", () =>
    withHost(async (context) => {
      const observer = await context.connect();
      observer.send(makeAttach(context, "client-observer", "attachment-observer"));
      await observer.take(isSnapshotFor(context.threadId));
      const runtime = context.runtimes[0]!;
      const event: ProviderEvent = {
        id: EventId.make("event-replay-opt-in"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: context.providerInstanceId,
        threadId: context.threadId,
        createdAt: "2026-07-31T12:03:00.000Z",
        method: "item/agentMessage/delta",
        textDelta: "replay me once",
      };
      const observed = observer.take(isEvent(event.id));
      await runtime.emit(event);
      await observed;
      await observer.close();

      const fresh = await context.connect();
      fresh.send(makeAttach(context, "client-fresh", "attachment-fresh"));
      await fresh.take(isSnapshotFor(context.threadId));
      await nextImmediate();
      NodeAssert.equal(fresh.has(isEvent(event.id)), false);

      const reconnecting = await context.connect();
      reconnecting.send(makeAttach(context, "client-replay", "attachment-replay", 0));
      const firstReconciliationEnvelope = await reconnecting.take(
        (envelope) => isSnapshotFor(context.threadId)(envelope) || isEvent(event.id)(envelope),
      );
      NodeAssert.equal(firstReconciliationEnvelope.type, "event");
      await reconnecting.take(isSnapshotFor(context.threadId));
    }),
  );

  it.effect("delivers retained replay frames between eight and sixteen MiB", () =>
    withHost(async (context) => {
      const observer = await context.connect();
      observer.send(makeAttach(context, "client-large-live", "attachment-large-live"));
      await observer.take(isSnapshotFor(context.threadId));
      const runtime = context.runtimes[0]!;
      const largeText = "x".repeat(9 * 1024 * 1024);
      const event: ProviderEvent = {
        id: EventId.make("event-large-retained"),
        kind: "notification",
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: context.providerInstanceId,
        threadId: context.threadId,
        createdAt: "2026-08-01T12:03:15.000Z",
        method: "item/agentMessage/delta",
        textDelta: largeText,
      };
      const liveEvent = observer.take(isEvent(event.id));
      await runtime.emit(event);
      NodeAssert.equal(
        ((await liveEvent).event as { readonly textDelta?: string }).textDelta?.length,
        largeText.length,
      );
      await observer.close();

      const reconnecting = await context.connect();
      reconnecting.send(makeAttach(context, "client-large-replay", "attachment-large-replay", 0));
      const replayed = await reconnecting.take(isEvent(event.id));
      NodeAssert.equal(
        (replayed.event as { readonly textDelta?: string }).textDelta?.length,
        largeText.length,
      );
      await reconnecting.take(isSnapshotFor(context.threadId));
    }),
  );

  it.effect("marks replay as truncated when a requested cursor predates retained output", () =>
    withHost(
      async (context) => {
        const observer = await context.connect();
        observer.send(makeAttach(context, "client-observer", "attachment-observer"));
        await observer.take(isSnapshotFor(context.threadId));
        const runtime = context.runtimes[0]!;
        const firstEvent: ProviderEvent = {
          id: EventId.make("event-replay-evicted"),
          kind: "notification",
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: context.providerInstanceId,
          threadId: context.threadId,
          createdAt: "2026-07-31T12:03:30.000Z",
          method: "item/agentMessage/delta",
          textDelta: "evicted output",
        };
        const retainedEvent: ProviderEvent = {
          ...firstEvent,
          id: EventId.make("event-replay-retained"),
          createdAt: "2026-07-31T12:03:31.000Z",
          textDelta: "retained output",
        };
        await runtime.emit(firstEvent);
        const observedRetained = observer.take(isEvent(retainedEvent.id));
        await runtime.emit(retainedEvent);
        await observedRetained;
        await observer.close();

        const reconnecting = await context.connect();
        reconnecting.send(makeAttach(context, "client-replay-gap", "attachment-replay-gap", 0));
        const firstReconciliationEnvelope = await reconnecting.take(
          (envelope) =>
            isSnapshotFor(context.threadId)(envelope) || isEvent(retainedEvent.id)(envelope),
        );
        NodeAssert.equal(firstReconciliationEnvelope.type, "event");
        const snapshot = await reconnecting.take(isSnapshotFor(context.threadId));
        NodeAssert.equal(snapshot.replayTruncated, true);
        NodeAssert.equal(reconnecting.has(isEvent(firstEvent.id)), false);
      },
      { maxReplayEvents: 1 },
    ),
  );

  it.effect("interrupts and closes only for an explicit session.stop command", () =>
    withHost(async (context) => {
      const client = await context.connect();
      client.send(makeAttach(context, "client-stopper", "attachment-stopper"));
      await client.take(isSnapshotFor(context.threadId));
      const runtime = context.runtimes[0]!;

      const stopCommand = makeCommand(context, {
        clientId: "client-stopper",
        attachmentId: "attachment-stopper",
        commandId: "command-stop",
        operation: CODEX_PROVIDER_HOST_OPERATIONS.stopSession,
      });
      client.send(stopCommand);
      const stopped = await client.take(isCommandResult("command-stop"));

      NodeAssert.equal(stopped.ok, true);
      NodeAssert.equal(runtime.interruptTurnImpl.mock.calls.length, 1);
      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 1);

      client.send(stopCommand);
      NodeAssert.deepStrictEqual(await client.take(isCommandResult("command-stop")), stopped);
      NodeAssert.equal(runtime.interruptTurnImpl.mock.calls.length, 1);
      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 1);

      client.send(
        makeCommand(context, {
          clientId: "client-stopper",
          attachmentId: "attachment-stopper",
          commandId: "command-after-stop",
          operation: CODEX_PROVIDER_HOST_OPERATIONS.readThread,
        }),
      );
      const afterStop = await client.take(isCommandResult("command-after-stop"));
      NodeAssert.equal(afterStop.ok, false);
      NodeAssert.match(String(afterStop.error), /requires an active matching attachment/);
      NodeAssert.equal(runtime.readThreadImpl.mock.calls.length, 0);
    }),
  );

  it.effect("rejects non-stop commands after an explicit stop begins", () =>
    withHost(async (context) => {
      const client = await context.connect();
      client.send(makeAttach(context, "client-stopper", "attachment-stopper"));
      await client.take(isSnapshotFor(context.threadId));
      const runtime = context.runtimes[0]!;
      let markInterruptStarted: () => void = () => undefined;
      const interruptStarted = new Promise<void>((resolve) => {
        markInterruptStarted = resolve;
      });
      let releaseInterrupt: () => void = () => undefined;
      const interruptBarrier = new Promise<void>((resolve) => {
        releaseInterrupt = resolve;
      });
      runtime.interruptTurnImpl.mockImplementation(() => {
        markInterruptStarted();
        return interruptBarrier;
      });

      client.send(
        makeCommand(context, {
          clientId: "client-stopper",
          attachmentId: "attachment-stopper",
          commandId: "command-stop-with-barrier",
          operation: CODEX_PROVIDER_HOST_OPERATIONS.stopSession,
        }),
      );
      await interruptStarted;

      try {
        client.send(
          makeCommand(context, {
            clientId: "client-stopper",
            attachmentId: "attachment-stopper",
            commandId: "command-during-stop",
            operation: CODEX_PROVIDER_HOST_OPERATIONS.readThread,
          }),
        );
        const rejected = await client.take(isCommandResult("command-during-stop"));
        NodeAssert.equal(rejected.ok, false);
        NodeAssert.match(String(rejected.error), /being stopped/);
        NodeAssert.equal(runtime.readThreadImpl.mock.calls.length, 0);
      } finally {
        releaseInterrupt();
      }

      const stopped = await client.take(isCommandResult("command-stop-with-barrier"));
      NodeAssert.equal(stopped.ok, true);
    }),
  );

  it.effect("bounds command fibers while letting an explicit stop bypass a hung command", () =>
    withHost(
      async (context) => {
        const client = await context.connect();
        client.send(makeAttach(context, "client-stopper", "attachment-stopper"));
        await client.take(isSnapshotFor(context.threadId));
        const runtime = context.runtimes[0]!;
        let markReadStarted: () => void = () => undefined;
        const readStarted = new Promise<void>((resolve) => {
          markReadStarted = resolve;
        });
        runtime.readThreadImpl.mockImplementation(
          () =>
            new Promise<CodexThreadSnapshot>(() => {
              markReadStarted();
            }),
        );

        client.send(
          makeCommand(context, {
            clientId: "client-stopper",
            attachmentId: "attachment-stopper",
            commandId: "command-hung-read",
            operation: CODEX_PROVIDER_HOST_OPERATIONS.readThread,
          }),
        );
        await readStarted;

        client.send(
          makeCommand(context, {
            clientId: "client-stopper",
            attachmentId: "attachment-stopper",
            commandId: "command-over-capacity",
            operation: CODEX_PROVIDER_HOST_OPERATIONS.readThread,
          }),
        );
        const overloaded = await client.take(isCommandResult("command-over-capacity"));
        NodeAssert.equal(overloaded.ok, false);
        NodeAssert.match(String(overloaded.error), /admission is at capacity/);
        NodeAssert.equal(runtime.readThreadImpl.mock.calls.length, 1);

        client.send(
          makeCommand(context, {
            clientId: "client-stopper",
            attachmentId: "attachment-stopper",
            commandId: "command-priority-stop",
            operation: CODEX_PROVIDER_HOST_OPERATIONS.stopSession,
          }),
        );
        const stopped = await client.take(isCommandResult("command-priority-stop"));

        NodeAssert.equal(stopped.ok, true);
        NodeAssert.equal(runtime.interruptTurnImpl.mock.calls.length, 1);
        NodeAssert.equal(runtime.closeImpl.mock.calls.length, 1);
      },
      {
        maxCommandFibers: 1,
        priorityCommandFiberReserve: 1,
      },
    ),
  );

  it.effect("closes a connection whose queued envelope admission exceeds its bound", () =>
    withHost(
      async (context) => {
        const client = await context.connect();
        client.sendMany([
          makeCommand(context, {
            clientId: "client-queue-bound",
            attachmentId: "attachment-queue-bound",
            commandId: "command-queue-1",
            operation: CODEX_PROVIDER_HOST_OPERATIONS.readThread,
          }),
          makeCommand(context, {
            clientId: "client-queue-bound",
            attachmentId: "attachment-queue-bound",
            commandId: "command-queue-2",
            operation: CODEX_PROVIDER_HOST_OPERATIONS.readThread,
          }),
          makeCommand(context, {
            clientId: "client-queue-bound",
            attachmentId: "attachment-queue-bound",
            commandId: "command-queue-3",
            operation: CODEX_PROVIDER_HOST_OPERATIONS.readThread,
          }),
        ]);

        await client.waitClosed();
      },
      { maxPendingEnvelopesPerConnection: 2 },
    ),
  );

  it.effect("bounds queued envelope bytes independently of message count", () =>
    withHost(
      async (context) => {
        const client = await context.connect();
        const commands = [
          makeCommand(context, {
            clientId: "client-queue-byte-bound",
            attachmentId: "attachment-queue-byte-bound",
            commandId: "command-queue-byte-1",
            operation: CODEX_PROVIDER_HOST_OPERATIONS.readThread,
          }),
          makeCommand(context, {
            clientId: "client-queue-byte-bound",
            attachmentId: "attachment-queue-byte-bound",
            commandId: "command-queue-byte-2",
            operation: CODEX_PROVIDER_HOST_OPERATIONS.readThread,
          }),
        ];
        client.sendMany(commands);

        await client.waitClosed();
      },
      {
        maxPendingEnvelopeBytesPerConnection: 1,
        maxPendingEnvelopesPerConnection: 8,
      },
    ),
  );

  it.effect("reports an explicit stop failure without deleting the live host session", () =>
    withHost(async (context) => {
      const client = await context.connect();
      client.send(makeAttach(context, "client-stop-failure", "attachment-stop-failure"));
      await client.take(isSnapshotFor(context.threadId));
      const runtime = context.runtimes[0]!;
      runtime.closeImpl.mockRejectedValueOnce(new Error("close failed"));

      client.send(
        makeCommand(context, {
          clientId: "client-stop-failure",
          attachmentId: "attachment-stop-failure",
          commandId: "command-stop-failure",
          operation: CODEX_PROVIDER_HOST_OPERATIONS.stopSession,
        }),
      );
      const failed = await client.take(isCommandResult("command-stop-failure"));
      NodeAssert.equal(failed.ok, false);
      NodeAssert.match(String(failed.error), /close failed/);
      NodeAssert.equal(runtime.interruptTurnImpl.mock.calls.length, 1);
      NodeAssert.equal(runtime.closeImpl.mock.calls.length, 1);

      client.send(
        makeCommand(context, {
          clientId: "client-stop-failure",
          attachmentId: "attachment-stop-failure",
          commandId: "command-after-stop-failure",
          operation: CODEX_PROVIDER_HOST_OPERATIONS.readThread,
        }),
      );
      NodeAssert.equal((await client.take(isCommandResult("command-after-stop-failure"))).ok, true);
    }),
  );
});
