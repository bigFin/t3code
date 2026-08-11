// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";

import { describe, expect, it } from "@effect/vitest";
import { ProviderDriverKind, ProviderInstanceId, ThreadId } from "@t3tools/contracts";

import {
  parsePiCompatibleSession,
  piCompatibleSubagentActivities,
  piCompatibleTurnReconcileCommand,
  resolvePiCompatibleObservedSession,
} from "./PiCompatibleSessionImporter.ts";

const PI = ProviderDriverKind.make("piAgent");
const OMP = ProviderDriverKind.make("omp");

const session = JSON.stringify({
  type: "session",
  version: 3,
  id: "019fe70c-c446-7000-bf0a-907e165a996f",
  timestamp: "2026-08-09T15:03:21.414Z",
  cwd: "/work/project",
});

function readOmpFixture(name: string): string {
  return NodeFS.readFileSync(new URL(`./fixtures/omp/${name}.jsonl`, import.meta.url), "utf8");
}

describe("PiCompatibleSessionImporter", () => {
  it("imports Pi v3 user and assistant text records", () => {
    const parsed = parsePiCompatibleSession(
      [
        session,
        JSON.stringify({
          type: "message",
          id: "user-1",
          timestamp: "2026-08-09T15:04:00.000Z",
          message: { role: "user", content: [{ type: "text", text: "Fix the test" }] },
        }),
        JSON.stringify({
          type: "message",
          id: "assistant-1",
          timestamp: "2026-08-09T15:05:00.000Z",
          message: { role: "assistant", content: [{ type: "text", text: "Fixed." }] },
        }),
      ].join("\n"),
      PI,
      "/tmp/pi.jsonl",
    );

    expect(parsed).toMatchObject({
      id: "019fe70c-c446-7000-bf0a-907e165a996f",
      cwd: "/work/project",
      title: "Fix the test",
    });
    expect(parsed?.messages.map((message) => [message.role, message.text])).toEqual([
      ["user", "Fix the test"],
      ["assistant", "Fixed."],
    ]);
    const command = parsed && piCompatibleTurnReconcileCommand(ThreadId.make("thread-pi"), parsed);
    expect(command).toMatchObject({
      type: "thread.turn.reconcile",
      threadId: "thread-pi",
      state: "completed",
      completedAt: "2026-08-09T15:05:00.000Z",
    });
  });
  it("keeps inferred sidebar titles concise and single-line", () => {
    const parsed = parsePiCompatibleSession(
      [
        session,
        JSON.stringify({
          type: "message",
          id: "user-1",
          timestamp: "2026-08-09T15:04:00.000Z",
          message: {
            role: "user",
            content: [
              {
                type: "text",
                text: `${"A request with   extra spacing ".repeat(8)}\nSupporting details`,
              },
            ],
          },
        }),
      ].join("\n"),
      PI,
      "/tmp/pi.jsonl",
    );

    expect(parsed?.title).toHaveLength(120);
    expect(parsed?.title).not.toContain("\n");
    expect(parsed?.title).not.toContain("  ");
    expect(parsed?.title).toMatch(/\.\.\.$/u);
  });
  it("accepts OMP title records and imports non-text tool records as activities", () => {
    const parsed = parsePiCompatibleSession(
      [
        JSON.stringify({ type: "title", title: "OMP transcript" }),
        session,
        "not json",
        JSON.stringify({
          type: "message",
          id: "user-1",
          timestamp: "2026-08-09T15:04:00.000Z",
          message: { role: "user", content: [{ type: "input_text", text: "Continue" }] },
        }),
        JSON.stringify({
          type: "message",
          id: "tool",
          timestamp: "2026-08-09T15:04:01.000Z",
          message: {
            role: "assistant",
            content: [
              {
                type: "tool_call",
                id: "call-read",
                name: "read",
                intent: "Reading the importer",
                arguments: { path: "src/importer.ts" },
              },
            ],
          },
        }),
      ].join("\n"),
      OMP,
      "/tmp/omp.jsonl",
    );

    expect(parsed?.title).toBe("OMP transcript");
    expect(parsed?.messages).toHaveLength(1);
    expect(parsed?.messages[0]?.text).toBe("Continue");
    expect(parsed?.activities).toMatchObject([
      {
        kind: "tool.updated",
        summary: "Reading the importer",
        payload: {
          itemType: "dynamic_tool_call",
          requestKind: "file-read",
          status: "inProgress",
          data: {
            toolCallId: "call-read",
            item: { name: "read", input: { path: "src/importer.ts" } },
          },
        },
        turnId: parsed?.messages[0]?.turnId,
      },
    ]);
    const command = parsed && piCompatibleTurnReconcileCommand(ThreadId.make("thread-omp"), parsed);
    expect(command).toMatchObject({
      type: "thread.turn.reconcile",
      threadId: "thread-omp",
      state: "interrupted",
      completedAt: "2026-08-09T15:04:00.000Z",
    });
  });
  it("imports OMP reasoning and collapsible tool lifecycles onto the completed turn", () => {
    const parsed = parsePiCompatibleSession(
      [
        session,
        JSON.stringify({
          type: "message",
          id: "user-1",
          timestamp: "2026-08-09T15:04:00.000Z",
          message: { role: "user", content: [{ type: "text", text: "Run the check" }] },
        }),
        JSON.stringify({
          type: "message",
          id: "assistant-tool",
          timestamp: "2026-08-09T15:04:01.000Z",
          message: {
            role: "assistant",
            stopReason: "toolUse",
            content: [
              { type: "thinking", thinking: "I should run the focused test first." },
              {
                type: "toolCall",
                id: "call-bash",
                name: "bash",
                intent: "Running focused tests",
                arguments: {
                  i: "Running focused tests",
                  command: "vp test run importer.test.ts",
                  cwd: "/work/project",
                },
              },
            ],
          },
        }),
        JSON.stringify({
          type: "custom",
          customType: "tool_execution_start",
          data: { toolCallId: "call-bash", toolName: "bash" },
        }),
        JSON.stringify({
          type: "message",
          id: "tool-result",
          timestamp: "2026-08-09T15:04:02.000Z",
          message: {
            role: "toolResult",
            toolCallId: "call-bash",
            toolName: "bash",
            isError: false,
            details: { exitCode: 0 },
            content: [{ type: "text", text: "1 test passed" }],
          },
        }),
        JSON.stringify({
          type: "message",
          id: "assistant-final",
          timestamp: "2026-08-09T15:04:03.000Z",
          message: {
            role: "assistant",
            stopReason: "stop",
            content: [{ type: "text", text: "Done." }],
          },
        }),
      ].join("\n"),
      OMP,
      "/tmp/omp.jsonl",
    );

    const turnId = parsed?.messages[1]?.turnId;
    expect(parsed?.activities).toMatchObject([
      {
        kind: "task.progress",
        payload: {
          summary: "I should run the focused test first.",
          detail: "I should run the focused test first.",
        },
        turnId,
      },
      {
        kind: "tool.updated",
        summary: "Running focused tests",
        payload: {
          itemType: "command_execution",
          requestKind: "command",
          status: "inProgress",
          title: "Terminal",
          data: {
            toolCallId: "call-bash",
            item: {
              command: "vp test run importer.test.ts",
            },
          },
        },
        turnId,
      },
      {
        kind: "tool.completed",
        summary: "Running focused tests",
        payload: {
          itemType: "command_execution",
          status: "completed",
          detail: "1 test passed",
          data: {
            toolCallId: "call-bash",
            rawOutput: { content: "1 test passed", exitCode: 0 },
          },
        },
        turnId,
      },
    ]);
    expect(parsed?.messages.map((message) => message.text)).toEqual(["Run the check", "Done."]);
  });
  it("marks open OMP turns as active externally owned sessions", () => {
    const parsed = parsePiCompatibleSession(
      [
        session,
        JSON.stringify({
          type: "message",
          id: "user-1",
          timestamp: "2026-08-09T15:04:00.000Z",
          message: { role: "user", content: [{ type: "text", text: "Keep working" }] },
        }),
      ].join("\n"),
      OMP,
      "/tmp/omp.jsonl",
    );
    expect(parsed).toBeDefined();

    const observed = resolvePiCompatibleObservedSession({
      currentSession: null,
      threadId: ThreadId.make("thread-omp"),
      imported: parsed!,
      sourcePath: "/tmp/omp.jsonl",
      instanceId: ProviderInstanceId.make("omp"),
      driver: OMP,
      isOpen: true,
      binaryPath: "omp",
      sessionDir: undefined,
      observedAt: "2026-08-09T15:05:00.000Z",
    });

    expect(observed).toMatchObject({
      status: "running",
      nativeSession: {
        id: "019fe70c-c446-7000-bf0a-907e165a996f",
        path: "/tmp/omp.jsonl",
        ownership: "external",
        supportsConcurrentAttach: false,
        cli: {
          command: "omp",
          args: ["--resume", "/tmp/omp.jsonl"],
          cwd: "/work/project",
        },
      },
    });
    expect(observed?.activeTurnId).toBe(parsed?.messages[0]?.turnId);
    expect(
      piCompatibleTurnReconcileCommand(ThreadId.make("thread-omp"), parsed!, true),
    ).toBeUndefined();
    expect(
      resolvePiCompatibleObservedSession({
        currentSession: {
          ...observed!,
          nativeSession: { ...observed!.nativeSession!, ownership: "t3" },
        },
        threadId: ThreadId.make("thread-omp"),
        imported: parsed!,
        sourcePath: "/tmp/omp.jsonl",
        instanceId: ProviderInstanceId.make("omp"),
        driver: OMP,
        isOpen: true,
        binaryPath: "omp",
        sessionDir: undefined,
        observedAt: "2026-08-09T15:06:00.000Z",
      }),
    ).toBeUndefined();
  });
  it("maps OMP child sessions to subagent activities", () => {
    const parsed = parsePiCompatibleSession(
      [
        JSON.stringify({
          ...JSON.parse(session),
          id: "child-session",
          parentSession: "/tmp/parent.jsonl",
        }),
        JSON.stringify({ type: "title", title: "Review the importer" }),
        JSON.stringify({
          type: "message",
          id: "user-1",
          timestamp: "2026-08-09T15:04:00.000Z",
          message: { role: "user", content: [{ type: "text", text: "Review this importer" }] },
        }),
        JSON.stringify({
          type: "message",
          id: "assistant-1",
          timestamp: "2026-08-09T15:05:00.000Z",
          message: {
            role: "assistant",
            content: [{ type: "text", text: `${"Found one issue. ".repeat(20)}\nDetails` }],
          },
        }),
      ].join("\n"),
      OMP,
      "/tmp/child.jsonl",
    );

    expect(parsed?.parentSession).toBe("/tmp/parent.jsonl");
    const activities = parsed && piCompatibleSubagentActivities(parsed);
    expect(activities).toMatchObject([
      {
        kind: "task.started",
        payload: {
          taskType: "subagent",
          agentKind: "agent",
          title: "Review the importer",
          timelineBypass: true,
        },
      },
      {
        kind: "task.completed",
        payload: {
          status: "completed",
          title: "Review the importer",
          timelineBypass: true,
        },
      },
    ]);
    const completedPayload = activities?.[1]?.payload as { readonly summary?: string } | undefined;
    expect(completedPayload?.summary).toHaveLength(180);
    expect(completedPayload?.summary).not.toContain("\n");
  });
  describe("OMP transcript compatibility fixtures", () => {
    const fixtures = [
      {
        name: "normal-response",
        messages: ["Explain the change", "The change is complete."],
        activityKinds: [],
        turnState: "completed",
      },
      {
        name: "tool-lifecycle",
        messages: ["Run the focused test", "Done."],
        activityKinds: ["task.progress", "tool.updated", "tool.completed"],
        turnState: "completed",
      },
      {
        name: "failed-tool",
        messages: ["Run the command", "The command failed."],
        activityKinds: ["tool.updated", "tool.completed"],
        turnState: "completed",
      },
      {
        name: "interrupted-turn",
        messages: ["Keep working"],
        activityKinds: [],
        turnState: "interrupted",
      },
      {
        name: "resumed-turn",
        messages: [
          "Start the task",
          "First turn complete.",
          "Resume and finish",
          "Resumed turn complete.",
        ],
        activityKinds: [],
        turnState: "completed",
      },
      {
        name: "subagent",
        messages: ["Review this importer", "Found one issue."],
        activityKinds: [],
        turnState: "completed",
        parentSession: "/work/parent.jsonl",
      },
    ] as const;

    for (const fixture of fixtures) {
      it(`imports ${fixture.name}`, () => {
        const parsed = parsePiCompatibleSession(
          readOmpFixture(fixture.name),
          OMP,
          `/fixtures/${fixture.name}.jsonl`,
        );

        expect(parsed?.messages.map((message) => message.text)).toEqual(fixture.messages);
        expect(parsed?.activities.map((activity) => activity.kind)).toEqual(fixture.activityKinds);
        expect(parsed?.parentSession).toBe(
          "parentSession" in fixture ? fixture.parentSession : undefined,
        );
        expect(
          parsed && piCompatibleTurnReconcileCommand(ThreadId.make("fixture-thread"), parsed),
        ).toMatchObject({ state: fixture.turnState });
        if (fixture.activityKinds.length > 0) {
          const completedTurnId = parsed?.messages.at(-1)?.turnId;
          expect(parsed?.activities.every((activity) => activity.turnId === completedTurnId)).toBe(
            true,
          );
        }
        if (fixture.name === "failed-tool") {
          expect(parsed?.activities.at(-1)?.payload).toMatchObject({ status: "failed" });
        }
        if (fixture.name === "subagent") {
          expect(parsed && piCompatibleSubagentActivities(parsed)).toHaveLength(2);
        }
      });
    }
  });
});
