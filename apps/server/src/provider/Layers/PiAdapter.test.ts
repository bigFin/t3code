import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import { beforeEach } from "vite-plus/test";

import {
  PiSettings,
  OmpSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  ThreadId,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { ServerConfig } from "../../config.ts";
import {
  PiRuntime,
  type PiRpcClient,
  type PiRpcEvent,
  type PiRpcResponse,
  type PiRuntimeShape,
} from "../piRuntime.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";
import { isPiSessionFileOpen, makePiAdapter } from "./PiAdapter.ts";
import { makeOmpAdapter } from "./OmpAdapter.ts";

class PiAdapter extends Context.Service<PiAdapter, PiAdapterShape>()(
  "t3/provider/Layers/PiAdapter.test/PiAdapter",
) {}

interface TestPiClient {
  readonly commands: Array<Readonly<Record<string, unknown>> & { readonly type: string }>;
  readonly events: Queue.Queue<PiRpcEvent>;
  state: {
    model: { readonly provider: string; readonly id: string } | null;
    thinkingLevel: string;
    isStreaming: boolean;
    sessionFile: string;
    sessionId: string;
  };
  stopCalls: number;
}

const runtimeMock = {
  startInputs: [] as Array<Parameters<PiRuntimeShape["startRpc"]>[0]>,
  clients: [] as Array<TestPiClient>,
  reset() {
    this.startInputs.length = 0;
    this.clients.length = 0;
  },
};

function successResponse(command: string, data?: unknown): PiRpcResponse {
  return {
    type: "response",
    command,
    success: true,
    ...(data === undefined ? {} : { data }),
  };
}

const PiRuntimeTestDouble: PiRuntimeShape = {
  runCommand: () => Effect.succeed({ stdout: "", stderr: "", code: 0 }),
  startRpc: (input) =>
    Effect.gen(function* () {
      const events = yield* Queue.unbounded<PiRpcEvent>();
      const client: TestPiClient = {
        commands: [],
        events,
        state: {
          model: {
            provider: "openai-codex",
            id: "gpt-5.5",
          },
          thinkingLevel: "medium",
          isStreaming: false,
          sessionFile: input.resumeSessionFile ?? "/tmp/pi/session.jsonl",
          sessionId: "pi-session-1",
        },
        stopCalls: 0,
      };
      runtimeMock.startInputs.push(input);
      runtimeMock.clients.push(client);

      const rpc: PiRpcClient = {
        request: (command) =>
          Effect.sync(() => {
            client.commands.push(command);
            switch (command.type) {
              case "get_state":
                return successResponse(command.type, client.state);
              case "set_model":
                client.state.model = {
                  provider: String(command.provider),
                  id: String(command.modelId),
                };
                return successResponse(command.type, client.state.model);
              case "set_thinking_level":
                client.state.thinkingLevel = String(command.level);
                return successResponse(command.type);
              case "get_messages":
                return successResponse(command.type, { messages: [] });
              default:
                return successResponse(command.type);
            }
          }),
        notify: () => Effect.void,
        events: Stream.fromQueue(events),
        exit: Effect.never,
        stop: Effect.sync(() => {
          client.stopCalls += 1;
        }).pipe(Effect.andThen(Queue.shutdown(events))),
      };
      return rpc;
    }),
};

const piSettings = Schema.decodeSync(PiSettings)({
  enabled: true,
  binaryPath: "fake-pi",
  sessionDir: "~/.pi/t3-sessions",
  customModels: [],
});
const instanceId = ProviderInstanceId.make("piAgent");

const PiAdapterTestLayer = Layer.effect(PiAdapter, makePiAdapter(piSettings, { instanceId })).pipe(
  Layer.provideMerge(Layer.succeed(PiRuntime, PiRuntimeTestDouble)),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
  Layer.provideMerge(NodeServices.layer),
);

const ompSettings = Schema.decodeSync(OmpSettings)({
  enabled: true,
  binaryPath: "fake-omp",
  sessionDir: "~/.omp/agent/sessions",
  customModels: [],
});
const ompInstanceId = ProviderInstanceId.make("omp");
const OmpAdapterTestLayer = Layer.effect(
  PiAdapter,
  makeOmpAdapter(ompSettings, { instanceId: ompInstanceId }),
).pipe(
  Layer.provideMerge(Layer.succeed(PiRuntime, PiRuntimeTestDouble)),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
  Layer.provideMerge(NodeServices.layer),
);

beforeEach(() => {
  runtimeMock.reset();
});

const threadId = (value: string) => ThreadId.make(value);
const piProvider = ProviderDriverKind.make("piAgent");

it.effect("detects a Pi-compatible session file held by another process", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-pi-session-owner-" });
    const sessionFile = path.join(root, "session.jsonl");
    const procRoot = path.join(root, "proc");
    const descriptorRoot = path.join(procRoot, "42", "fd");
    yield* fileSystem.writeFileString(sessionFile, "");
    yield* fileSystem.makeDirectory(descriptorRoot, { recursive: true });
    yield* fileSystem.symlink(sessionFile, path.join(descriptorRoot, "7"));

    expect(
      yield* isPiSessionFileOpen(fileSystem, path, sessionFile, {
        procRoot,
        currentProcessId: "1",
      }),
    ).toBe(true);
    expect(
      yield* isPiSessionFileOpen(fileSystem, path, sessionFile, {
        procRoot,
        currentProcessId: "42",
      }),
    ).toBe(false);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.layer(PiAdapterTestLayer)("PiAdapter", (it) => {
  it.effect("starts and resumes Pi RPC sessions from the durable session file", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const session = yield* adapter.startSession({
        provider: piProvider,
        threadId: threadId("pi-resume"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        resumeCursor: {
          schemaVersion: 1,
          sessionId: "old-pi-session",
          sessionFile: "/persisted/pi/session.jsonl",
        },
      });

      expect(runtimeMock.startInputs).toHaveLength(1);
      expect(runtimeMock.startInputs[0]).toMatchObject({
        binaryPath: "fake-pi",
        cwd: process.cwd(),
        resumeSessionFile: "/persisted/pi/session.jsonl",
      });
      expect(runtimeMock.startInputs[0]?.sessionDir).toBe(`${process.env.HOME}/.pi/t3-sessions`);
      expect(session).toMatchObject({
        provider: "piAgent",
        providerInstanceId: "piAgent",
        threadId: "pi-resume",
        model: "openai-codex/gpt-5.5",
        resumeCursor: {
          schemaVersion: 1,
          sessionId: "pi-session-1",
          sessionFile: "/persisted/pi/session.jsonl",
        },
      });
    }),
  );

  it.effect("applies provider/model and thinking level through Pi RPC", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const session = yield* adapter.startSession({
        provider: piProvider,
        threadId: threadId("pi-model-selection"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: createModelSelection(instanceId, "azure-foundry/gpt-5.5", [
          { id: "thinkingLevel", value: "high" },
        ]),
      });
      const client = runtimeMock.clients[0]!;

      expect(client.commands).toEqual([
        {
          type: "set_model",
          provider: "azure-foundry",
          modelId: "gpt-5.5",
        },
        {
          type: "set_thinking_level",
          level: "high",
        },
        {
          type: "get_state",
        },
      ]);
      expect(session.model).toBe("azure-foundry/gpt-5.5");
      expect(client.state.thinkingLevel).toBe("high");
    }),
  );

  it.effect("uses prompt for a new turn and steer while the turn is active", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const id = threadId("pi-prompt-steer");
      yield* adapter.startSession({
        provider: piProvider,
        threadId: id,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const first = yield* adapter.sendTurn({
        threadId: id,
        input: "start the task",
      });
      const second = yield* adapter.sendTurn({
        threadId: id,
        input: "focus on the tests",
      });
      const commandTypes = runtimeMock.clients[0]!.commands.map((command) => command.type);

      expect(commandTypes).toEqual(["get_state", "prompt", "get_state", "steer", "get_state"]);
      expect(second.turnId).toBe(first.turnId);
    }),
  );

  it.effect("aborts the active Pi turn and returns the session to ready", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const id = threadId("pi-abort");
      yield* adapter.startSession({
        provider: piProvider,
        threadId: id,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const turn = yield* adapter.sendTurn({
        threadId: id,
        input: "keep working",
      });
      yield* adapter.interruptTurn(id, turn.turnId);

      expect(runtimeMock.clients[0]!.commands.at(-1)).toEqual({ type: "abort" });
      expect((yield* adapter.listSessions())[0]).toMatchObject({
        status: "ready",
      });
      expect((yield* adapter.listSessions())[0]?.activeTurnId).toBeUndefined();
    }),
  );

  it.effect("maps Pi text streaming and settlement into canonical runtime events", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.take(8),
        Stream.runCollect,
        Effect.map((events) => Array.from(events)),
        Effect.forkChild,
      );
      yield* Effect.yieldNow;

      const id = threadId("pi-events");
      yield* adapter.startSession({
        provider: piProvider,
        threadId: id,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const turn = yield* adapter.sendTurn({
        threadId: id,
        input: "say hello",
      });
      const client = runtimeMock.clients[0]!;
      yield* Queue.offerAll(client.events, [
        {
          type: "message_update",
          message: { role: "assistant" },
          assistantMessageEvent: {
            type: "text_start",
            contentIndex: 0,
          },
        },
        {
          type: "message_update",
          message: { role: "assistant" },
          assistantMessageEvent: {
            type: "text_delta",
            contentIndex: 0,
            delta: "hello",
          },
        },
        {
          type: "message_update",
          message: { role: "assistant" },
          assistantMessageEvent: {
            type: "text_end",
            contentIndex: 0,
            content: "hello",
          },
        },
        {
          type: "message_end",
          message: {
            role: "assistant",
            stopReason: "stop",
            usage: { input: 10, output: 2 },
          },
        },
        {
          type: "agent_settled",
        },
      ]);
      const events: ReadonlyArray<ProviderRuntimeEvent> = yield* Fiber.join(eventsFiber);

      expect(events.map((event) => event.type)).toEqual([
        "session.started",
        "session.state.changed",
        "thread.started",
        "turn.started",
        "item.started",
        "content.delta",
        "item.completed",
        "turn.completed",
      ]);
      expect(events[5]).toMatchObject({
        type: "content.delta",
        turnId: turn.turnId,
        payload: {
          streamKind: "assistant_text",
          delta: "hello",
          contentIndex: 0,
        },
      });
      expect(events[7]).toMatchObject({
        type: "turn.completed",
        turnId: turn.turnId,
        payload: {
          state: "completed",
          stopReason: "stop",
          usage: { input: 10, output: 2 },
        },
      });
    }),
  );
});

it.layer(OmpAdapterTestLayer)("OmpAdapter", (it) => {
  it.effect("uses OMP's approval flag and provider identity for shared RPC sessions", () =>
    Effect.gen(function* () {
      const adapter = yield* PiAdapter;
      const session = yield* adapter.startSession({
        provider: ProviderDriverKind.make("omp"),
        threadId: threadId("omp-session"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      expect(session.provider).toBe(ProviderDriverKind.make("omp"));
      expect(session.providerInstanceId).toBe(ompInstanceId);
      expect(runtimeMock.startInputs[0]).toMatchObject({
        binaryPath: "fake-omp",
        approvalFlag: "--auto-approve",
      });
    }),
  );
});
