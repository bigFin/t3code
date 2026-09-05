// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics preferSchemaOverJson:off - Test WebSocket fixtures use known JSON-RPC envelopes.
import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodeHttp from "node:http";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import { it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { describe } from "vite-plus/test";
import { DEFAULT_MODEL, ThreadId } from "@t3tools/contracts";
import * as CodexErrors from "effect-codex-app-server/errors";
import * as CodexRpc from "effect-codex-app-server/rpc";
import * as EffectCodexSchema from "effect-codex-app-server/schema";

import { buildCodexDeveloperInstructions } from "../CodexDeveloperInstructions.ts";
import { codexSessionAppServerArgs } from "./codexLaunchArgs.ts";
import {
  buildTurnStartParams,
  classifyCodexConnectionClosure,
  describeMcpElicitation,
  hasConfiguredMcpServer,
  isRecoverableThreadResumeError,
  makeCodexSessionRuntime,
  makeMemoryConsolidationNotificationFilter,
  openCodexThread,
  resolveCodexRecoveredThreadState,
  type CodexSessionRuntimeError,
  type CodexSessionRuntimeShape,
  toMcpElicitationResponse,
} from "./CodexSessionRuntime.ts";
const isCodexAppServerRequestError = Schema.is(CodexErrors.CodexAppServerRequestError);
const isCodexAppServerSpawnError = Schema.is(CodexErrors.CodexAppServerSpawnError);

function withDetachedCodexThread(input: {
  readonly thread: Readonly<Record<string, unknown>>;
  readonly resumePolicy?: "resume-only";
  readonly beforeOpenResponse?: (socket: NodeSocket.NodeWS.WebSocket) => void;
  readonly run: (
    runtime: CodexSessionRuntimeShape,
  ) => Effect.Effect<void, CodexSessionRuntimeError>;
}) {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-runtime-fixture-"));
  const socketPath = NodePath.join(root, "app-server.sock");
  const server = NodeHttp.createServer();
  const webSocketServer = new NodeSocket.NodeWS.WebSocketServer({
    perMessageDeflate: false,
    server,
  });
  webSocketServer.on("connection", (socket) => {
    socket.on("message", (data) => {
      const request = JSON.parse(data.toString()) as {
        readonly id?: number;
        readonly method: string;
      };
      if (request.id === undefined) {
        return;
      }
      const result =
        request.method === "initialize"
          ? {
              codexHome: root,
              platformFamily: "unix",
              platformOs: "linux",
              userAgent: "codex-test",
            }
          : {
              approvalPolicy: "never",
              approvalsReviewer: "user",
              cwd: root,
              model: "gpt-5.3-codex",
              modelProvider: "openai",
              sandbox: { type: "dangerFullAccess" },
              thread: input.thread,
            };
      if (request.method !== "initialize") {
        input.beforeOpenResponse?.(socket);
      }
      socket.send(JSON.stringify({ id: request.id, result }));
    });
  });

  const listen = Effect.callback<void, Error>((resume) => {
    server.once("error", (cause) => resume(Effect.fail(cause)));
    server.listen(socketPath, () => resume(Effect.void));
    return Effect.sync(() => server.close());
  });
  const close = Effect.callback<void>((resume) => {
    for (const client of webSocketServer.clients) {
      client.terminate();
    }
    webSocketServer.close(() => {
      server.close(() => resume(Effect.void));
    });
  });

  return Effect.scoped(
    Effect.gen(function* () {
      yield* Effect.acquireRelease(listen, () => close);
      const runtime = yield* makeCodexSessionRuntime({
        threadId: ThreadId.make("thread-detached-fixture"),
        binaryPath: "codex",
        cwd: root,
        runtimeMode: "full-access",
        resumeCursor: { threadId: String(input.thread.id) },
        appServerSocketPath: socketPath,
        ...(input.resumePolicy ? { resumePolicy: input.resumePolicy } : {}),
      });
      yield* input.run(runtime);
    }),
  ).pipe(
    Effect.provide(NodeServices.layer),
    Effect.ensuring(
      Effect.sync(() => {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }),
    ),
  );
}

describe("CodexSessionRuntimeIdentifierGenerationError", () => {
  it("retains identifier purpose and the random source failure", () => {
    const cause = new Error("random source unavailable");
    const error = new CodexErrors.CodexAppServerIdentifierGenerationError({
      purpose: "provider-event",
      cause,
    });

    NodeAssert.equal(error.purpose, "provider-event");
    NodeAssert.strictEqual(error.cause, cause);
    NodeAssert.equal(
      error.message,
      "Failed to generate Codex App Server identifier for provider-event.",
    );
  });
});

describe("classifyCodexConnectionClosure", () => {
  it("preserves active execution when only a detached T3 attachment is lost", () => {
    NodeAssert.deepStrictEqual(
      classifyCodexConnectionClosure({
        reattachable: true,
        failed: true,
        message: "WebSocket closed.",
      }),
      {
        status: "error",
        method: "session/disconnected",
        message: "WebSocket closed. T3 will reattach without interrupting Codex execution.",
        clearActiveTurn: false,
      },
    );
  });

  it("treats a scoped app-server process exit as an execution exit", () => {
    NodeAssert.deepStrictEqual(
      classifyCodexConnectionClosure({
        reattachable: false,
        failed: true,
        message: "Process exited.",
      }),
      {
        status: "error",
        method: "session/exited",
        message: "Process exited.",
        clearActiveTurn: true,
      },
    );
  });
});

describe("resolveCodexRecoveredThreadState", () => {
  it("restores an active turn without synthesizing a new turn", () => {
    NodeAssert.deepStrictEqual(
      resolveCodexRecoveredThreadState({
        status: { type: "active", activeFlags: [] },
        turns: [{ id: "turn-active", status: "inProgress" }],
      }),
      {
        sessionStatus: "running",
        activeTurnId: "turn-active",
        lifecycle: {
          type: "started",
          turnId: "turn-active",
        },
      },
    );
  });

  it("restores approval and user-input waiting states", () => {
    NodeAssert.deepStrictEqual(
      resolveCodexRecoveredThreadState({
        status: { type: "active", activeFlags: ["waitingOnApproval"] },
        turns: [{ id: "turn-approval", status: "inProgress" }],
      }),
      {
        sessionStatus: "running",
        activeTurnId: "turn-approval",
        lifecycle: {
          type: "started",
          turnId: "turn-approval",
        },
        waitingReason: "Codex is waiting for approval.",
      },
    );
    NodeAssert.deepStrictEqual(
      resolveCodexRecoveredThreadState({
        status: { type: "active", activeFlags: ["waitingOnUserInput"] },
        turns: [{ id: "turn-input", status: "inProgress" }],
      }),
      {
        sessionStatus: "running",
        activeTurnId: "turn-input",
        lifecycle: {
          type: "started",
          turnId: "turn-input",
        },
        waitingReason: "Codex is waiting for user input.",
      },
    );
  });

  it("restores completed, interrupted, and failed terminal turns", () => {
    NodeAssert.deepStrictEqual(
      resolveCodexRecoveredThreadState({
        status: { type: "idle" },
        turns: [{ id: "turn-completed", status: "completed" }],
      }),
      {
        sessionStatus: "ready",
        lifecycle: {
          type: "completed",
          turnId: "turn-completed",
          status: "completed",
        },
      },
    );
    NodeAssert.deepStrictEqual(
      resolveCodexRecoveredThreadState({
        status: { type: "idle" },
        turns: [{ id: "turn-interrupted", status: "interrupted" }],
      }),
      {
        sessionStatus: "ready",
        lifecycle: {
          type: "completed",
          turnId: "turn-interrupted",
          status: "interrupted",
        },
      },
    );
    NodeAssert.deepStrictEqual(
      resolveCodexRecoveredThreadState({
        status: { type: "systemError" },
        turns: [
          {
            id: "turn-failed",
            status: "failed",
            error: { message: "Provider failed." },
          },
        ],
      }),
      {
        sessionStatus: "error",
        lastError: "Provider failed.",
        lifecycle: {
          type: "completed",
          turnId: "turn-failed",
          status: "failed",
        },
      },
    );
  });

  it("keeps a recovered system-error thread in an error state without a turn", () => {
    NodeAssert.deepStrictEqual(
      resolveCodexRecoveredThreadState({
        status: { type: "systemError" },
        turns: [],
      }),
      {
        sessionStatus: "error",
        lastError: "Codex reported a system error for this thread.",
      },
    );
  });

  it("does not report a recovered in-progress turn as running after a system error", () => {
    NodeAssert.deepStrictEqual(
      resolveCodexRecoveredThreadState({
        status: { type: "systemError" },
        turns: [{ id: "turn-system-error-in-progress", status: "inProgress" }],
      }),
      {
        sessionStatus: "error",
        lastError: "Codex reported a system error for this thread.",
      },
    );
  });
});

describe("makeCodexSessionRuntime detached connection", () => {
  it.effect("does not spawn a scoped app-server fallback when the detached socket fails", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-runtime-socket-"));
    const socketPath = NodePath.join(root, "missing.sock");

    return Effect.gen(function* () {
      const exit = yield* Effect.scoped(
        makeCodexSessionRuntime({
          threadId: ThreadId.make("thread-detached-connect-failure"),
          binaryPath: NodePath.join(root, "missing-codex"),
          cwd: root,
          runtimeMode: "full-access",
          appServerSocketPath: socketPath,
        }),
      ).pipe(Effect.exit);

      NodeAssert.equal(exit._tag, "Failure");
      if (exit._tag === "Failure") {
        const error = Cause.squash(exit.cause);
        NodeAssert.ok(isCodexAppServerSpawnError(error));
        if (isCodexAppServerSpawnError(error)) {
          NodeAssert.equal(error.command, `connect to detached Codex App Server at ${socketPath}`);
        }
      }
    }).pipe(
      Effect.provide(NodeServices.layer),
      Effect.ensuring(
        Effect.sync(() => {
          NodeFS.rmSync(root, { recursive: true, force: true });
        }),
      ),
    );
  });

  it.effect("does not emit session/ready for a recovered system-error thread", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-codex-runtime-error-"));
    const socketPath = NodePath.join(root, "app-server.sock");
    const server = NodeHttp.createServer();
    const webSocketServer = new NodeSocket.NodeWS.WebSocketServer({
      perMessageDeflate: false,
      server,
    });
    webSocketServer.on("connection", (socket) => {
      socket.on("message", (data) => {
        const request = JSON.parse(data.toString()) as {
          readonly id?: number;
          readonly method: string;
        };
        if (request.id === undefined) {
          return;
        }
        const result =
          request.method === "initialize"
            ? {
                codexHome: root,
                platformFamily: "unix",
                platformOs: "linux",
                userAgent: "codex-test",
              }
            : {
                approvalPolicy: "never",
                approvalsReviewer: "user",
                cwd: root,
                model: "gpt-5.3-codex",
                modelProvider: "openai",
                sandbox: { type: "dangerFullAccess" },
                thread: {
                  cliVersion: "0.0.0-test",
                  createdAt: 0,
                  cwd: root,
                  ephemeral: false,
                  id: "provider-thread-system-error",
                  modelProvider: "openai",
                  preview: "",
                  sessionId: "provider-session-system-error",
                  source: "cli",
                  status: { type: "systemError" },
                  turns: [],
                  updatedAt: 0,
                },
              };
        socket.send(JSON.stringify({ id: request.id, result }));
      });
    });

    const listen = Effect.callback<void, Error>((resume) => {
      server.once("error", (cause) => resume(Effect.fail(cause)));
      server.listen(socketPath, () => resume(Effect.void));
      return Effect.sync(() => server.close());
    });
    const close = Effect.callback<void>((resume) => {
      for (const client of webSocketServer.clients) {
        client.terminate();
      }
      webSocketServer.close(() => {
        server.close(() => resume(Effect.void));
      });
    });

    return Effect.scoped(
      Effect.gen(function* () {
        yield* Effect.acquireRelease(listen, () => close);
        const runtime = yield* makeCodexSessionRuntime({
          threadId: ThreadId.make("thread-recovered-system-error"),
          binaryPath: "codex",
          cwd: root,
          runtimeMode: "full-access",
          resumeCursor: { threadId: "provider-thread-system-error" },
          appServerSocketPath: socketPath,
        });

        const session = yield* runtime.start();
        NodeAssert.equal(session.status, "error");
        NodeAssert.equal(yield* runtime.emittedEventCount, 1);
        const events = Array.from(yield* Stream.runCollect(Stream.take(runtime.events, 1)));
        NodeAssert.deepStrictEqual(
          events.map((event) => event.method),
          ["session/connecting"],
        );
        yield* runtime.detach;
      }),
    ).pipe(
      Effect.provide(NodeServices.layer),
      Effect.ensuring(
        Effect.sync(() => {
          NodeFS.rmSync(root, { recursive: true, force: true });
        }),
      ),
    );
  });

  it.effect("emits recovered turn state without a false session/ready transition", () =>
    withDetachedCodexThread({
      thread: {
        cliVersion: "0.0.0-test",
        createdAt: 0,
        cwd: "/tmp/project",
        ephemeral: false,
        id: "provider-thread-active",
        modelProvider: "openai",
        preview: "",
        sessionId: "provider-session-active",
        source: "cli",
        status: { type: "active", activeFlags: [] },
        turns: [
          {
            id: "turn-active",
            items: [],
            status: "inProgress",
          },
        ],
        updatedAt: 0,
      },
      run: (runtime) =>
        Effect.gen(function* () {
          const session = yield* runtime.start();
          NodeAssert.equal(session.status, "running");
          NodeAssert.equal(session.activeTurnId, "turn-active");
          NodeAssert.equal(yield* runtime.emittedEventCount, 2);
          const events = Array.from(yield* Stream.runCollect(Stream.take(runtime.events, 2)));
          NodeAssert.deepStrictEqual(
            events.map((event) => event.method),
            ["session/connecting", "turn/started"],
          );
          yield* runtime.detach;
        }),
    }),
  );

  it.effect("replays recovered assistant output during resume-only adoption", () =>
    withDetachedCodexThread({
      resumePolicy: "resume-only",
      thread: {
        cliVersion: "0.0.0-test",
        createdAt: 0,
        cwd: "/tmp/project",
        ephemeral: false,
        id: "provider-thread-recovered-output",
        modelProvider: "openai",
        preview: "",
        sessionId: "provider-session-recovered-output",
        source: "cli",
        status: { type: "idle" },
        turns: [
          {
            id: "turn-recovered-output",
            items: [
              {
                id: "item-recovered-output",
                type: "agentMessage",
                text: "Recovered final response.",
              },
            ],
            status: "completed",
          },
        ],
        updatedAt: 0,
      },
      run: (runtime) =>
        Effect.gen(function* () {
          const session = yield* runtime.start();
          NodeAssert.equal(session.status, "ready");
          NodeAssert.equal(yield* runtime.emittedEventCount, 4);
          const events = Array.from(yield* Stream.runCollect(Stream.take(runtime.events, 4)));
          NodeAssert.deepStrictEqual(
            events.map((event) => event.method),
            ["session/connecting", "session/ready", "item/completed", "turn/completed"],
          );
          NodeAssert.deepStrictEqual(events[2]?.payload, {
            threadId: "provider-thread-recovered-output",
            turnId: "turn-recovered-output",
            item: {
              id: "item-recovered-output",
              type: "agentMessage",
              text: "Recovered final response.",
            },
          });
          yield* runtime.detach;
        }),
    }),
  );

  it.effect("drains accepted server notifications before releasing a detached attachment", () => {
    const notificationCount = 512;
    return withDetachedCodexThread({
      thread: {
        cliVersion: "0.0.0-test",
        createdAt: 0,
        cwd: "/tmp/project",
        ephemeral: false,
        id: "provider-thread-drain",
        modelProvider: "openai",
        preview: "",
        sessionId: "provider-session-drain",
        source: "cli",
        status: { type: "idle" },
        turns: [],
        updatedAt: 0,
      },
      beforeOpenResponse: (socket) => {
        for (let index = 0; index < notificationCount; index += 1) {
          socket.send(
            JSON.stringify({
              method: "item/agentMessage/delta",
              params: {
                threadId: "provider-thread-drain",
                turnId: "turn-drain",
                itemId: `item-drain-${index}`,
                delta: `${index}`,
              },
            }),
          );
        }
      },
      run: (runtime) =>
        Effect.gen(function* () {
          yield* runtime.start();
          yield* runtime.detach;
          NodeAssert.equal(yield* runtime.emittedEventCount, notificationCount + 2);
        }),
    });
  });
});

function makeThreadOpenResponse(
  threadId: string,
): CodexRpc.ClientRequestResponsesByMethod["thread/start"] {
  return {
    cwd: "/tmp/project",
    model: "gpt-5.3-codex",
    modelProvider: "openai",
    approvalPolicy: "never",
    approvalsReviewer: "user",
    sandbox: { type: "danger-full-access" },
    thread: {
      id: threadId,
      createdAt: "2026-04-18T00:00:00.000Z",
      source: { session: "cli" },
      turns: [],
      status: {
        state: "idle",
        activeFlags: [],
      },
    },
  } as unknown as CodexRpc.ClientRequestResponsesByMethod["thread/start"];
}

describe("buildTurnStartParams", () => {
  it("keeps invalid turn values only in the schema cause", () => {
    const secret = "codex-turn-input-secret-sentinel";
    const error = Effect.runSync(
      buildTurnStartParams({
        threadId: "provider-thread-1",
        runtimeMode: "full-access",
        attachments: [
          {
            type: "image",
            url: { secret } as unknown as string,
          },
        ],
      }).pipe(Effect.flip),
    );
    const { cause, ...directDiagnostics } = error;

    NodeAssert.equal(error.operation, "decode-request-payload");
    NodeAssert.equal(error.method, "turn/start");
    NodeAssert.ok((error.issueCount ?? 0) > 0);
    NodeAssert.ok(error.issueKinds?.includes("Pointer"));
    NodeAssert.ok((error.maximumPathDepth ?? 0) > 0);
    NodeAssert.ok(Schema.isSchemaError(cause));
    NodeAssert.doesNotMatch(error.message, new RegExp(secret));
    NodeAssert.doesNotMatch(JSON.stringify(directDiagnostics), new RegExp(secret));
  });

  it("includes plan collaboration mode when requested", () => {
    const params = Effect.runSync(
      buildTurnStartParams({
        threadId: "provider-thread-1",
        runtimeMode: "full-access",
        prompt: "Make a plan",
        model: "gpt-5.3-codex",
        effort: "medium",
        interactionMode: "plan",
      }),
    );

    NodeAssert.deepStrictEqual(params, {
      threadId: "provider-thread-1",
      approvalPolicy: "never",
      approvalsReviewer: "user",
      sandboxPolicy: {
        type: "dangerFullAccess",
      },
      input: [
        {
          type: "text",
          text: "Make a plan",
        },
      ],
      model: "gpt-5.3-codex",
      effort: "medium",
      collaborationMode: {
        mode: "plan",
        settings: {
          model: "gpt-5.3-codex",
          reasoning_effort: "medium",
          developer_instructions: buildCodexDeveloperInstructions("plan", {
            model: "gpt-5.3-codex",
            reasoningEffort: "medium",
          }),
        },
      },
    });
  });

  it("includes default collaboration mode and image attachments", () => {
    const params = Effect.runSync(
      buildTurnStartParams({
        threadId: "provider-thread-1",
        runtimeMode: "auto-accept-edits",
        prompt: "Implement it",
        model: "gpt-5.3-codex",
        interactionMode: "default",
        attachments: [
          {
            type: "image",
            url: "data:image/png;base64,abc",
          },
        ],
      }),
    );

    NodeAssert.deepStrictEqual(params, {
      threadId: "provider-thread-1",
      approvalPolicy: "on-request",
      approvalsReviewer: "user",
      sandboxPolicy: {
        type: "workspaceWrite",
      },
      input: [
        {
          type: "text",
          text: "Implement it",
        },
        {
          type: "image",
          url: "data:image/png;base64,abc",
        },
      ],
      model: "gpt-5.3-codex",
      collaborationMode: {
        mode: "default",
        settings: {
          model: "gpt-5.3-codex",
          reasoning_effort: "medium",
          developer_instructions: buildCodexDeveloperInstructions("default", {
            model: "gpt-5.3-codex",
            reasoningEffort: "medium",
          }),
        },
      },
    });
  });

  it("reports the same fallback model and effort in settings and instructions", () => {
    const params = Effect.runSync(
      buildTurnStartParams({
        threadId: "provider-thread-1",
        runtimeMode: "full-access",
        prompt: "Go",
        interactionMode: "default",
      }),
    );

    const settings = params.collaborationMode?.settings;
    NodeAssert.equal(settings?.model, DEFAULT_MODEL);
    NodeAssert.equal(settings?.reasoning_effort, "medium");
    NodeAssert.ok(settings?.developer_instructions?.includes(`as ${DEFAULT_MODEL} with medium`));
  });

  it.effect("routes approvals to the auto reviewer in auto mode", () =>
    Effect.gen(function* () {
      const params = yield* buildTurnStartParams({
        threadId: "provider-thread-1",
        runtimeMode: "auto",
        prompt: "Ship it",
      });

      NodeAssert.deepStrictEqual(params, {
        threadId: "provider-thread-1",
        approvalPolicy: "on-request",
        approvalsReviewer: "auto_review",
        sandboxPolicy: {
          type: "workspaceWrite",
        },
        input: [
          {
            type: "text",
            text: "Ship it",
          },
        ],
      });
    }),
  );

  it("omits collaboration mode when interaction mode is absent", () => {
    const params = Effect.runSync(
      buildTurnStartParams({
        threadId: "provider-thread-1",
        runtimeMode: "approval-required",
        prompt: "Review",
      }),
    );

    NodeAssert.deepStrictEqual(params, {
      threadId: "provider-thread-1",
      approvalPolicy: "untrusted",
      approvalsReviewer: "user",
      sandboxPolicy: {
        type: "readOnly",
      },
      input: [
        {
          type: "text",
          text: "Review",
        },
      ],
    });
  });
});

describe("Codex MCP elicitation approvals", () => {
  const request = {
    mode: "form",
    message: "Allow ChatGPT to use Safari?",
    serverName: "computer-use",
    threadId: "provider-thread-1",
    turnId: "turn-1",
    _meta: {
      app_name: "Safari",
      persist: ["session", "always"],
    },
    requestedSchema: {
      type: "object",
      properties: {
        approval: {
          type: "string",
          oneOf: [
            { const: "once", title: "Allow once" },
            { const: "session", title: "Allow for this session" },
            { const: "always", title: "Always allow Safari" },
          ],
        },
      },
      required: ["approval"],
    },
  } satisfies EffectCodexSchema.McpServerElicitationRequestParams;

  it("preserves the app name and advertised persistence choices", () => {
    NodeAssert.deepStrictEqual(describeMcpElicitation(request), {
      appName: "Safari",
      options: [
        { decision: "cancel", label: "Cancel" },
        { decision: "decline", label: "Decline" },
        { decision: "acceptForSession", label: "Allow for this session" },
        { decision: "acceptAlways", label: "Always allow Safari" },
        { decision: "accept", label: "Approve" },
      ],
    });
  });

  it("extracts the app name from a Computer Use request without metadata", () => {
    const { _meta, ...requestWithoutMetadata } = request;

    NodeAssert.equal(describeMcpElicitation(requestWithoutMetadata).appName, "Safari");
  });

  it("returns the accepted form option to Codex", () => {
    NodeAssert.deepStrictEqual(toMcpElicitationResponse(request, "accept"), {
      action: "accept",
      content: { approval: "once" },
    });
  });

  it("returns session-scoped approval in the MCP response", () => {
    NodeAssert.deepStrictEqual(toMcpElicitationResponse(request, "acceptForSession"), {
      action: "accept",
      _meta: { persist: "session" },
      content: { approval: "session" },
    });
  });

  it("returns persistent approval in the MCP response", () => {
    NodeAssert.deepStrictEqual(toMcpElicitationResponse(request, "acceptAlways"), {
      action: "accept",
      _meta: { persist: "always" },
      content: { approval: "always" },
    });
  });

  it("returns rejection without form content", () => {
    NodeAssert.deepStrictEqual(toMcpElicitationResponse(request, "decline"), {
      action: "decline",
    });
  });

  it("returns cancellation without form content", () => {
    NodeAssert.deepStrictEqual(toMcpElicitationResponse(request, "cancel"), {
      action: "cancel",
    });
  });

  it("supports boolean permanent-approval fields", () => {
    const booleanRequest = {
      ...request,
      _meta: { app_name: "Safari" },
      requestedSchema: {
        type: "object",
        properties: {
          always: { type: "boolean", title: "Always allow Safari" },
        },
      },
    } satisfies EffectCodexSchema.McpServerElicitationRequestParams;

    NodeAssert.ok(
      describeMcpElicitation(booleanRequest).options.some(
        (option) => option.decision === "acceptAlways",
      ),
    );
    NodeAssert.deepStrictEqual(toMcpElicitationResponse(booleanRequest, "acceptAlways"), {
      action: "accept",
      _meta: { persist: "always" },
      content: { always: true },
    });
  });

  it("preserves valid nullable MCP form fields and persistence choices", () => {
    const nullableRequest = {
      ...request,
      _meta: {
        app_name: null,
        appName: "Safari",
        connector_name: null,
        persist: null,
        target: null,
        tool_params: null,
      },
      requestedSchema: {
        type: "object",
        properties: {
          approval: {
            type: "string",
            title: null,
            description: null,
            default: null,
            enum: ["once", "always"],
            enumNames: null,
          },
        },
        required: ["approval"],
      },
    } satisfies EffectCodexSchema.McpServerElicitationRequestParams;

    NodeAssert.equal(describeMcpElicitation(nullableRequest).appName, "Safari");
    NodeAssert.ok(
      describeMcpElicitation(nullableRequest).options.some(
        (option) => option.decision === "acceptAlways",
      ),
    );
    NodeAssert.deepStrictEqual(toMcpElicitationResponse(nullableRequest, "acceptAlways"), {
      action: "accept",
      _meta: { persist: "always" },
      content: { approval: "always" },
    });
  });

  it("declines required form fields that an approval prompt cannot collect", () => {
    const inputRequest = {
      ...request,
      requestedSchema: {
        type: "object",
        properties: {
          email: { type: "string", format: "email" },
        },
        required: ["email"],
      },
    } satisfies EffectCodexSchema.McpServerElicitationRequestParams;

    NodeAssert.deepStrictEqual(toMcpElicitationResponse(inputRequest, "accept"), {
      action: "decline",
    });
  });

  it("does not approve URL elicitations without opening their requested URL", () => {
    const urlRequest = {
      mode: "url",
      message: "Finish signing in to continue.",
      serverName: "computer-use",
      threadId: "provider-thread-1",
      turnId: "turn-1",
      elicitationId: "sign-in-1",
      url: "https://example.com/authorize",
    } satisfies EffectCodexSchema.McpServerElicitationRequestParams;

    NodeAssert.deepStrictEqual(toMcpElicitationResponse(urlRequest, "accept"), {
      action: "decline",
    });
  });

  it("omits persistence choices that cannot satisfy required form fields", () => {
    const onceOnlyRequest = {
      ...request,
      _meta: { app_name: "Safari", persist: ["session", "always"] },
      requestedSchema: {
        type: "object",
        properties: {
          approval: {
            type: "string",
            enum: ["once"],
          },
        },
        required: ["approval"],
      },
    } satisfies EffectCodexSchema.McpServerElicitationRequestParams;

    NodeAssert.deepStrictEqual(describeMcpElicitation(onceOnlyRequest).options, [
      { decision: "cancel", label: "Cancel" },
      { decision: "decline", label: "Decline" },
      { decision: "accept", label: "Approve" },
    ]);
  });
});

describe("buildCodexDeveloperInstructions", () => {
  it("appends runtime info after the mode instructions", () => {
    const instructions = buildCodexDeveloperInstructions("default", {
      model: "gpt-5.3-codex",
      reasoningEffort: "high",
    });

    NodeAssert.match(instructions, /^<collaboration_mode># Collaboration Mode: Default/);
    NodeAssert.match(instructions, /T3 Code/);
    NodeAssert.match(instructions, /Codex harness/);
    NodeAssert.match(instructions, /as gpt-5\.3-codex with high reasoning effort/);
  });

  it("describes Markdown media support in the runtime context in both modes", () => {
    for (const mode of ["default", "plan"] as const) {
      const instructions = buildCodexDeveloperInstructions(mode, {
        model: "gpt-5.3-codex",
        reasoningEffort: "high",
      });
      NodeAssert.match(
        instructions,
        /<runtime_info>.*embed images and videos.*Markdown.*<\/runtime_info>/,
      );
    }
  });

  it("includes runtime info alongside plan mode instructions", () => {
    const instructions = buildCodexDeveloperInstructions("plan", {
      model: "gpt-5.3-codex",
      reasoningEffort: "medium",
    });

    NodeAssert.match(instructions, /^<collaboration_mode># Plan Mode/);
    NodeAssert.match(instructions, /as gpt-5\.3-codex with medium reasoning effort/);
  });

  it("varies with the model and effort of each turn", () => {
    const first = buildCodexDeveloperInstructions("default", {
      model: "gpt-5.3-codex",
      reasoningEffort: "medium",
    });
    const second = buildCodexDeveloperInstructions("default", {
      model: "gpt-5.4",
      reasoningEffort: "high",
    });

    NodeAssert.notEqual(first, second);
  });

  it("flattens multiline metadata into single-line runtime info", () => {
    const instructions = buildCodexDeveloperInstructions("default", {
      model: "gpt\n5.3\ncodex",
      reasoningEffort: " high\neffort ",
    });

    NodeAssert.match(instructions, /as gpt 5\.3 codex with high effort reasoning effort/);
    NodeAssert.doesNotMatch(instructions, /<runtime_info>[^<]*\n/);
  });
});

describe("T3 browser developer instructions", () => {
  const runtime = { model: "gpt-5.3-codex", reasoningEffort: "high" };

  it("prefers the product-native preview tools in both collaboration modes", () => {
    for (const mode of ["default", "plan"] as const) {
      const instructions = buildCodexDeveloperInstructions(mode, runtime, true);
      NodeAssert.match(instructions, /t3-code/);
      NodeAssert.match(instructions, /preview_status/);
      NodeAssert.match(instructions, /preview_open/);
      NodeAssert.match(instructions, /Do not switch to global browser skills/);
    }
  });

  it("omits the browser block entirely when the preview tools are not attached", () => {
    for (const mode of ["default", "plan"] as const) {
      const instructions = buildCodexDeveloperInstructions(mode, runtime, false);
      NodeAssert.doesNotMatch(instructions, /preview_status/);
      NodeAssert.doesNotMatch(instructions, /preview_open/);
      NodeAssert.doesNotMatch(instructions, /T3 Code collaborative browser/);
      // Steering away from other browser automation must go with the tools;
      // keeping it would leave the model talked out of its only option.
      NodeAssert.doesNotMatch(instructions, /Do not switch to global browser skills/);
      // The rest of the collaboration mode is untouched.
      NodeAssert.match(instructions, /<collaboration_mode>/);
      NodeAssert.match(instructions, /<\/collaboration_mode>/);
    }
  });

  it("tracks the turn's MCP configuration rather than defaulting to on", () => {
    NodeAssert.match(buildCodexDeveloperInstructions("default", runtime, true), /preview_open/);
    NodeAssert.doesNotMatch(
      buildCodexDeveloperInstructions("default", runtime, false),
      /preview_open/,
    );
  });
});

describe("hasConfiguredMcpServer", () => {
  it("detects inline Codex MCP configuration arguments", () => {
    NodeAssert.equal(hasConfiguredMcpServer(undefined), false);
    NodeAssert.equal(hasConfiguredMcpServer(["--model", "gpt-5.4"]), false);
    NodeAssert.equal(
      hasConfiguredMcpServer(["-c", 'mcp_servers.t3-code.url="http://127.0.0.1/mcp"']),
      true,
    );
  });
});

function makeThreadStartedNotification(
  threadId: string,
  source: EffectCodexSchema.V2ThreadStartedNotification["thread"]["source"],
  threadSource?: string,
) {
  return {
    method: "thread/started" as const,
    params: {
      thread: {
        cliVersion: "0.0.0",
        createdAt: 0,
        cwd: "/tmp/project",
        ephemeral: true,
        id: threadId,
        modelProvider: "openai",
        preview: "",
        sessionId: threadId,
        source,
        status: { type: "idle" as const },
        ...(threadSource ? { threadSource } : {}),
        turns: [],
        updatedAt: 0,
      },
    },
  };
}

describe("makeMemoryConsolidationNotificationFilter", () => {
  it("suppresses memory consolidation without hiding other Codex subagents", () => {
    const shouldSuppress = makeMemoryConsolidationNotificationFilter();

    NodeAssert.equal(
      shouldSuppress(
        makeThreadStartedNotification("memory-thread", "unknown", "memory_consolidation"),
      ),
      true,
    );
    NodeAssert.equal(
      shouldSuppress({
        method: "item/agentMessage/delta",
        params: {
          delta: "internal memory update",
          itemId: "memory-message",
          threadId: "memory-thread",
          turnId: "memory-turn",
        },
      }),
      true,
    );
    NodeAssert.equal(
      shouldSuppress({
        method: "serverRequest/resolved",
        params: {
          requestId: "memory-approval",
          threadId: "memory-thread",
        },
      }),
      false,
    );
    NodeAssert.equal(
      shouldSuppress({
        method: "warning",
        params: {
          message: "internal warning",
          threadId: "memory-thread",
        },
      }),
      true,
    );
    NodeAssert.equal(
      shouldSuppress({
        method: "item/agentMessage/delta",
        params: {
          delta: "normal reply",
          itemId: "root-message",
          threadId: "root-thread",
          turnId: "root-turn",
        },
      }),
      false,
    );

    NodeAssert.equal(
      shouldSuppress(
        makeThreadStartedNotification("legacy-memory-thread", {
          subAgent: "memory_consolidation",
        }),
      ),
      true,
    );

    for (const source of [
      { subAgent: "review" as const },
      { subAgent: "compact" as const },
      {
        subAgent: {
          thread_spawn: {
            depth: 1,
            parent_thread_id: "root-thread",
          },
        },
      },
    ]) {
      NodeAssert.equal(
        shouldSuppress(makeThreadStartedNotification("visible-subagent", source)),
        false,
      );
    }
  });

  it("forgets memory consolidation threads after they close", () => {
    const shouldSuppress = makeMemoryConsolidationNotificationFilter();
    shouldSuppress(
      makeThreadStartedNotification("memory-thread", "unknown", "memory_consolidation"),
    );

    NodeAssert.equal(
      shouldSuppress({
        method: "thread/closed",
        params: { threadId: "memory-thread" },
      }),
      true,
    );
    NodeAssert.equal(
      shouldSuppress({
        method: "item/agentMessage/delta",
        params: {
          delta: "later message",
          itemId: "later-message",
          threadId: "memory-thread",
          turnId: "later-turn",
        },
      }),
      false,
    );
  });
});

describe("codexSessionAppServerArgs", () => {
  it("keeps the app-server subcommand when explicit args are provided", () => {
    NodeAssert.deepStrictEqual(codexSessionAppServerArgs(["-c", "model=gpt-5"], undefined), [
      "app-server",
      "-c",
      "model=gpt-5",
    ]);
  });

  it("keeps launch args when explicit app-server args are provided", () => {
    NodeAssert.deepStrictEqual(
      codexSessionAppServerArgs(
        ["-c", "mcp_servers.t3-code.url=http://127.0.0.1/mcp"],
        "--strict-config --enable foo",
      ),
      [
        "app-server",
        "--strict-config",
        "--enable",
        "foo",
        "-c",
        "mcp_servers.t3-code.url=http://127.0.0.1/mcp",
      ],
    );
  });
});

describe("isRecoverableThreadResumeError", () => {
  it("matches missing thread errors", () => {
    NodeAssert.equal(
      isRecoverableThreadResumeError(
        new CodexErrors.CodexAppServerRequestError({
          code: -32603,
          errorMessage: "Thread does not exist",
        }),
      ),
      true,
    );
  });

  it("matches a missing rollout for a known thread id", () => {
    NodeAssert.equal(
      isRecoverableThreadResumeError(
        new CodexErrors.CodexAppServerRequestError({
          code: -32603,
          errorMessage: "no rollout found for thread id 019fdf74-aaa9-7950-b252-7cc7a8650470",
        }),
      ),
      true,
    );
  });

  it("ignores non-recoverable resume errors", () => {
    NodeAssert.equal(
      isRecoverableThreadResumeError(
        new CodexErrors.CodexAppServerRequestError({
          code: -32603,
          errorMessage: "Permission denied",
        }),
      ),
      false,
    );
  });

  it("ignores unrelated missing-resource errors that do not mention threads", () => {
    NodeAssert.equal(
      isRecoverableThreadResumeError(
        new CodexErrors.CodexAppServerRequestError({
          code: -32603,
          errorMessage: "Config file not found",
        }),
      ),
      false,
    );
    NodeAssert.equal(
      isRecoverableThreadResumeError(
        new CodexErrors.CodexAppServerRequestError({
          code: -32603,
          errorMessage: "Model does not exist",
        }),
      ),
      false,
    );
  });
});

describe("openCodexThread", () => {
  it.effect("falls back to thread/start when resume fails recoverably", () =>
    Effect.gen(function* () {
      const calls: Array<{ method: "thread/start" | "thread/resume"; payload: unknown }> = [];
      const started = makeThreadOpenResponse("fresh-thread");
      const client = {
        request: <M extends "thread/start" | "thread/resume">(
          method: M,
          payload: CodexRpc.ClientRequestParamsByMethod[M],
        ) => {
          calls.push({ method, payload });
          if (method === "thread/resume") {
            return Effect.fail(
              new CodexErrors.CodexAppServerRequestError({
                code: -32603,
                errorMessage: "thread not found",
              }),
            );
          }
          return Effect.succeed(started as CodexRpc.ClientRequestResponsesByMethod[M]);
        },
      };

      const opened = yield* openCodexThread({
        client,
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "full-access",
        cwd: "/tmp/project",
        requestedModel: "gpt-5.3-codex",
        serviceTier: undefined,
        resumeThreadId: "stale-thread",
        threadConfig: {
          mcp_servers: {
            "t3-code": {
              url: "http://127.0.0.1:3773/mcp",
              http_headers: {
                Authorization: "Bearer thread-secret",
              },
            },
          },
        },
      });

      NodeAssert.equal(opened.thread.id, "fresh-thread");
      NodeAssert.deepStrictEqual(
        calls.map((call) => call.method),
        ["thread/resume", "thread/start"],
      );
      for (const call of calls) {
        NodeAssert.deepStrictEqual((call.payload as { readonly config?: unknown }).config, {
          mcp_servers: {
            "t3-code": {
              url: "http://127.0.0.1:3773/mcp",
              http_headers: {
                Authorization: "Bearer thread-secret",
              },
            },
          },
        });
      }
    }),
  );

  it.effect("does not fall back to thread/start when resume-only adoption fails", () =>
    Effect.gen(function* () {
      const calls: Array<"thread/start" | "thread/resume"> = [];
      const client = {
        request: <M extends "thread/start" | "thread/resume">(
          method: M,
          _payload: CodexRpc.ClientRequestParamsByMethod[M],
        ) => {
          calls.push(method);
          if (method === "thread/resume") {
            return Effect.fail(
              new CodexErrors.CodexAppServerRequestError({
                code: -32603,
                errorMessage: "thread not found",
              }),
            );
          }
          return Effect.succeed(
            makeThreadOpenResponse("fresh-thread") as CodexRpc.ClientRequestResponsesByMethod[M],
          );
        },
      };

      const error = yield* openCodexThread({
        client,
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "full-access",
        cwd: "/tmp/project",
        requestedModel: "gpt-5.3-codex",
        serviceTier: undefined,
        resumeThreadId: "stale-thread",
        resumePolicy: "resume-only",
      }).pipe(Effect.flip);

      NodeAssert.equal(error._tag, "CodexSessionRuntimeThreadIdMissingError");
      NodeAssert.deepStrictEqual(calls, ["thread/resume"]);
    }),
  );

  it.effect("does not start a new thread when resume-only adoption lacks a cursor", () =>
    Effect.gen(function* () {
      const calls: Array<"thread/start" | "thread/resume"> = [];
      const client = {
        request: <M extends "thread/start" | "thread/resume">(
          method: M,
          _payload: CodexRpc.ClientRequestParamsByMethod[M],
        ) => {
          calls.push(method);
          return Effect.succeed(
            makeThreadOpenResponse("fresh-thread") as CodexRpc.ClientRequestResponsesByMethod[M],
          );
        },
      };

      const error = yield* openCodexThread({
        client,
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "full-access",
        cwd: "/tmp/project",
        requestedModel: "gpt-5.3-codex",
        serviceTier: undefined,
        resumeThreadId: undefined,
        resumePolicy: "resume-only",
      }).pipe(Effect.flip);

      NodeAssert.equal(error._tag, "CodexSessionRuntimeThreadIdMissingError");
      NodeAssert.deepStrictEqual(calls, []);
    }),
  );

  it.effect("propagates non-recoverable resume failures", () =>
    Effect.gen(function* () {
      const client = {
        request: <M extends "thread/start" | "thread/resume">(
          method: M,
          _payload: CodexRpc.ClientRequestParamsByMethod[M],
        ) => {
          if (method === "thread/resume") {
            return Effect.fail(
              new CodexErrors.CodexAppServerRequestError({
                code: -32603,
                errorMessage: "timed out waiting for server",
              }),
            );
          }
          return Effect.succeed(
            makeThreadOpenResponse("fresh-thread") as CodexRpc.ClientRequestResponsesByMethod[M],
          );
        },
      };

      const error = yield* openCodexThread({
        client,
        threadId: ThreadId.make("thread-1"),
        runtimeMode: "full-access",
        cwd: "/tmp/project",
        requestedModel: "gpt-5.3-codex",
        serviceTier: undefined,
        resumeThreadId: "stale-thread",
      }).pipe(Effect.flip);

      NodeAssert.ok(isCodexAppServerRequestError(error));
      NodeAssert.equal(error.errorMessage, "timed out waiting for server");
    }),
  );
});
