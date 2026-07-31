import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { EnvironmentId, ProjectId, ProviderInstanceId, ThreadId, TurnId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { resolveThreadStatus } from "./threadPresentation";

const NOW = "2026-07-31T12:00:00.000Z";

function makeThread(overrides: Partial<EnvironmentThreadShell> = {}): EnvironmentThreadShell {
  const threadId = ThreadId.make("thread-1");
  return {
    environmentId: EnvironmentId.make("environment-1"),
    id: threadId,
    projectId: ProjectId.make("project-1"),
    title: "Interrupted work",
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    latestTurn: null,
    createdAt: NOW,
    updatedAt: NOW,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    session: null,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    ...overrides,
  };
}

describe("resolveThreadStatus interrupted attention", () => {
  const interruptedTurn = {
    turnId: TurnId.make("turn-1"),
    state: "interrupted" as const,
    requestedAt: NOW,
    startedAt: NOW,
    completedAt: NOW,
    assistantMessageId: null,
  };

  it("shows either interruption source as non-pulsing amber attention", () => {
    const fromSession = resolveThreadStatus(
      makeThread({
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "interrupted",
          providerName: "Codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: "Host restarted",
          updatedAt: NOW,
        },
      }),
    );
    const fromTurn = resolveThreadStatus(makeThread({ latestTurn: interruptedTurn }));

    for (const status of [fromSession, fromTurn]) {
      expect(status).toMatchObject({
        kind: "interrupted",
        label: "Interrupted",
        pulse: false,
        textClassName: expect.stringContaining("orange"),
      });
    }
  });

  it("keeps approval, input, retrying, and running ahead of stale interrupted turns", () => {
    const runningSession = {
      threadId: ThreadId.make("thread-1"),
      status: "running" as const,
      providerName: "Codex",
      providerInstanceId: ProviderInstanceId.make("codex"),
      runtimeMode: "full-access" as const,
      activeTurnId: TurnId.make("turn-2"),
      lastError: null,
      updatedAt: NOW,
    };

    expect(
      resolveThreadStatus(
        makeThread({
          latestTurn: interruptedTurn,
          hasPendingApprovals: true,
          session: runningSession,
        }),
      )?.kind,
    ).toBe("pending-approval");
    expect(
      resolveThreadStatus(
        makeThread({
          latestTurn: interruptedTurn,
          hasPendingUserInput: true,
          session: runningSession,
        }),
      )?.kind,
    ).toBe("awaiting-input");
    expect(
      resolveThreadStatus(
        makeThread({
          latestTurn: interruptedTurn,
          session: { ...runningSession, retrying: true },
        }),
      )?.kind,
    ).toBe("retrying");
    expect(
      resolveThreadStatus(makeThread({ latestTurn: interruptedTurn, session: runningSession }))
        ?.kind,
    ).toBe("working");
  });
});
