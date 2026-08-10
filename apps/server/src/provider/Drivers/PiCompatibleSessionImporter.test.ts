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
  it("accepts OMP title records and skips non-text tool records", () => {
    const parsed = parsePiCompatibleSession(
      [
        JSON.stringify({ type: "title", title: "OMP transcript" }),
        session,
        "not json",
        JSON.stringify({
          type: "message",
          id: "tool",
          message: { role: "assistant", content: [{ type: "tool_call", name: "read" }] },
        }),
        JSON.stringify({
          type: "message",
          id: "user-1",
          timestamp: "2026-08-09T15:04:00.000Z",
          message: { role: "user", content: [{ type: "input_text", text: "Continue" }] },
        }),
      ].join("\n"),
      OMP,
      "/tmp/omp.jsonl",
    );

    expect(parsed?.title).toBe("OMP transcript");
    expect(parsed?.messages).toHaveLength(1);
    expect(parsed?.messages[0]?.text).toBe("Continue");
    const command = parsed && piCompatibleTurnReconcileCommand(ThreadId.make("thread-omp"), parsed);
    expect(command).toMatchObject({
      type: "thread.turn.reconcile",
      threadId: "thread-omp",
      state: "interrupted",
      completedAt: "2026-08-09T15:04:00.000Z",
    });
  });
  it("marks open OMP transcripts as externally owned native sessions", () => {
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
      status: "ready",
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
});
