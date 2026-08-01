import {
  MessageId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import { it as effectIt } from "@effect/vitest";
import { describe, expect, it } from "vite-plus/test";
import type * as CodexSchema from "effect-codex-app-server/schema";

import {
  CODEX_INTERACTIVE_SOURCE_KINDS,
  codexCliMessageImportCommand,
  collectCodexCliImportedMessages,
  collectCodexCliRolloutMessages,
  isCodexRolloutPathWithinSessionsRoot,
  isCurrentCodexCliImport,
  isDetachedCodexCliMirrorSession,
  isDifferentlyKeyedCodexCliOwnerBinding,
  isImportableCodexInteractiveThread,
  isLiveCodexBinding,
  isRecentCodexCliActivity,
  parseCodexRolloutTerminalEvidence,
  reconcileCodexCliImportedMessages,
  resolveCodexCliImportBinding,
  resolveStaleCodexCliSession,
  shouldInspectInterruptedCodexCliMirror,
  shouldInterruptStaleCodexCliSession,
  shouldPreserveCurrentOpenCodexCliImport,
  shouldProbeCodexCliRolloutOwner,
  shouldReconcileCurrentCodexCliSessionWithoutRead,
  shouldSkipCurrentCodexCliImport,
} from "./CodexCliSessionImporter.ts";

function makeThread(): CodexSchema.V2ThreadReadResponse["thread"] {
  return {
    cliVersion: "1.2.3",
    createdAt: 1_700_000_000,
    cwd: "/tmp/project",
    ephemeral: false,
    id: "019codex-thread",
    modelProvider: "openai",
    preview: "Original prompt",
    sessionId: "session-1",
    source: "cli",
    status: { type: "idle" },
    turns: [
      {
        id: "turn-1",
        items: [
          {
            id: "message-user-1",
            type: "userMessage",
            content: [
              { type: "text", text: "Inspect this." },
              { type: "localImage", path: "/tmp/reference.png" },
              { type: "skill", name: "nix", path: "/skills/nix/SKILL.md" },
            ],
          },
          {
            id: "message-agent-1",
            type: "agentMessage",
            text: "I found the issue.",
          },
          {
            id: "plan-1",
            type: "plan",
            text: "This is intentionally not imported as a chat message.",
          },
        ],
        startedAt: 1_700_000_001,
        status: "completed",
      },
      {
        id: "turn-2",
        items: [
          {
            id: "message-user-2",
            type: "userMessage",
            content: [{ type: "mention", name: "flake.nix", path: "/tmp/project/flake.nix" }],
          },
        ],
        startedAt: 1_700_000_001,
        status: "completed",
      },
    ],
    updatedAt: 1_700_000_002,
  };
}

describe("CodexCliSessionImporter transcript conversion", () => {
  it("scopes provider-local message ids to the owning thread", () => {
    const messages = collectCodexCliImportedMessages(makeThread());

    expect(messages).toEqual([
      {
        messageId: MessageId.make("codex-cli:019codex-thread:message-user-1"),
        role: "user",
        text: [
          "Inspect this.",
          "[image: /tmp/reference.png]",
          "[skill: nix (/skills/nix/SKILL.md)]",
        ].join("\n"),
        turnId: TurnId.make("turn-1"),
        createdAt: "2023-11-14T22:13:21.000Z",
      },
      {
        messageId: MessageId.make("codex-cli:019codex-thread:message-agent-1"),
        role: "assistant",
        text: "I found the issue.",
        turnId: TurnId.make("turn-1"),
        createdAt: "2023-11-14T22:13:21.001Z",
      },
      {
        messageId: MessageId.make("codex-cli:019codex-thread:message-user-2"),
        role: "user",
        text: "[mention: flake.nix (/tmp/project/flake.nix)]",
        turnId: TurnId.make("turn-2"),
        createdAt: "2023-11-14T22:13:21.002Z",
      },
    ]);
  });

  it("does not collide when two Codex threads reuse app-server item ids", () => {
    const first = collectCodexCliImportedMessages(makeThread());
    const second = collectCodexCliImportedMessages({
      ...makeThread(),
      id: "019another-thread",
    });

    expect(first[0]?.messageId).toBe(MessageId.make("codex-cli:019codex-thread:message-user-1"));
    expect(second[0]?.messageId).toBe(MessageId.make("codex-cli:019another-thread:message-user-1"));
    expect(second[0]?.messageId).not.toBe(first[0]?.messageId);
  });

  it("does not synthesize messages for Codex metadata-only threads", () => {
    expect(
      collectCodexCliImportedMessages({
        ...makeThread(),
        turns: [],
      }),
    ).toEqual([]);
  });

  it("recovers user and assistant messages from legacy rollout JSONL", () => {
    const contents = [
      JSON.stringify({
        timestamp: "2026-07-16T15:29:37.585Z",
        type: "response_item",
        payload: {
          type: "message",
          role: "developer",
          content: [{ type: "input_text", text: "Hidden instructions." }],
        },
      }),
      JSON.stringify({
        timestamp: "2026-07-16T15:29:37.586Z",
        type: "response_item",
        payload: {
          type: "message",
          role: "user",
          content: [{ type: "input_text", text: "# AGENTS.md instructions for /tmp/project" }],
        },
      }),
      JSON.stringify({
        timestamp: "2026-07-16T15:29:37.589Z",
        type: "event_msg",
        payload: {
          type: "user_message",
          message: "Inspect the legacy session.",
          turn_id: "turn-legacy",
        },
      }),
      "{malformed",
      JSON.stringify({
        timestamp: "2026-07-16T15:29:38.100Z",
        type: "response_item",
        payload: {
          type: "message",
          id: "assistant-legacy",
          role: "assistant",
          content: [{ type: "output_text", text: "Recovered it." }],
          internal_chat_message_metadata_passthrough: {
            turn_id: "turn-legacy",
          },
        },
      }),
    ].join("\n");

    expect(
      collectCodexCliRolloutMessages({
        threadId: "019legacy-thread",
        contents,
        createdAt: 1_784_215_777,
      }),
    ).toEqual([
      {
        messageId: MessageId.make("codex-cli:019legacy-thread:rollout:2"),
        role: "user",
        text: "Inspect the legacy session.",
        turnId: TurnId.make("turn-legacy"),
        createdAt: "2026-07-16T15:29:37.589Z",
      },
      {
        messageId: MessageId.make("codex-cli:019legacy-thread:rollout:4"),
        role: "assistant",
        text: "Recovered it.",
        turnId: TurnId.make("turn-legacy"),
        createdAt: "2026-07-16T15:29:38.100Z",
      },
    ]);
  });

  it("falls back to safe raw user response items when user events are absent", () => {
    const contents = [
      JSON.stringify({
        timestamp: "2026-07-16T15:29:37.589Z",
        type: "response_item",
        payload: {
          type: "message",
          role: "user",
          content: [{ type: "input_text", text: "Legacy user prompt." }],
        },
      }),
      JSON.stringify({
        timestamp: "invalid",
        type: "response_item",
        payload: {
          type: "message",
          role: "user",
          content: [{ type: "input_text", text: "<turn_aborted>hidden</turn_aborted>" }],
        },
      }),
    ].join("\n");

    expect(
      collectCodexCliRolloutMessages({
        threadId: "019legacy-thread",
        contents,
        createdAt: 1_784_215_777,
      }),
    ).toEqual([
      {
        messageId: MessageId.make("codex-cli:019legacy-thread:rollout:0"),
        role: "user",
        text: "Legacy user prompt.",
        turnId: TurnId.make("codex-cli:019legacy-thread:rollout-turn:0"),
        createdAt: "2026-07-16T15:29:37.589Z",
      },
    ]);
  });

  it("uses stable thread-scoped rollout ids", () => {
    const input = {
      contents: JSON.stringify({
        timestamp: "2026-07-16T15:29:37.589Z",
        type: "event_msg",
        payload: {
          type: "user_message",
          message: "Stable.",
        },
      }),
      createdAt: 1_784_215_777,
    };
    const first = collectCodexCliRolloutMessages({
      threadId: "019legacy-thread",
      ...input,
    });
    const repeated = collectCodexCliRolloutMessages({
      threadId: "019legacy-thread",
      ...input,
    });
    const otherThread = collectCodexCliRolloutMessages({
      threadId: "019other-thread",
      ...input,
    });

    expect(repeated).toEqual(first);
    expect(otherThread[0]?.messageId).not.toBe(first[0]?.messageId);
    expect(otherThread[0]?.turnId).not.toBe(first[0]?.turnId);
  });

  effectIt.effect("rejects rollout paths outside the configured sessions root", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      expect(
        isCodexRolloutPathWithinSessionsRoot(
          path,
          "/home/fin/.codex/sessions",
          "/home/fin/.codex/sessions/2026/07/16/rollout.jsonl",
        ),
      ).toBe(true);
      expect(
        isCodexRolloutPathWithinSessionsRoot(
          path,
          "/home/fin/.codex/sessions",
          "/home/fin/.codex/sessions-other/rollout.jsonl",
        ),
      ).toBe(false);
      expect(
        isCodexRolloutPathWithinSessionsRoot(
          path,
          "/home/fin/.codex/sessions",
          "/home/fin/.codex/auth.json",
        ),
      ).toBe(false);
    }).pipe(Effect.provide(Path.layer)),
  );

  it("uses stable command ids and changes them when projected content changes", () => {
    const [message] = collectCodexCliImportedMessages(makeThread());
    expect(message).toBeDefined();
    if (message === undefined) {
      return;
    }

    const first = codexCliMessageImportCommand({
      threadId: ThreadId.make("019codex-thread"),
      message,
    });
    const repeated = codexCliMessageImportCommand({
      threadId: ThreadId.make("019codex-thread"),
      message,
    });
    const changed = codexCliMessageImportCommand({
      threadId: ThreadId.make("019codex-thread"),
      message: {
        ...message,
        text: `${message.text}\nUpdated`,
      },
    });

    expect(repeated.commandId).toBe(first.commandId);
    expect(changed.commandId).not.toBe(first.commandId);
    expect(first.messageId).toBe(message.messageId);
  });

  it("reuses a pending T3 message id for Codex's copy of the same prompt", () => {
    const imported = {
      messageId: MessageId.make("codex-cli:019codex-thread:item-200"),
      role: "user" as const,
      text: "Keep working after I close the window.",
      turnId: TurnId.make("turn-pending"),
      createdAt: "2026-07-25T19:53:14.000Z",
    };
    const projectedThread = {
      id: ThreadId.make("019codex-thread"),
      latestTurn: {
        turnId: TurnId.make("turn-pending"),
        state: "running" as const,
        requestedAt: "2026-07-25T19:53:12.490Z",
        startedAt: "2026-07-25T19:53:12.490Z",
        completedAt: null,
        assistantMessageId: null,
      },
      messages: [
        {
          id: MessageId.make("t3-pending-message"),
          role: "user" as const,
          text: "Keep working after I close the window.",
          turnId: null,
          streaming: false,
          createdAt: "2026-07-25T19:53:12.490Z",
          updatedAt: "2026-07-25T19:53:12.490Z",
        },
      ],
    } as Parameters<typeof reconcileCodexCliImportedMessages>[1];

    expect(reconcileCodexCliImportedMessages([imported], projectedThread)).toEqual([
      {
        ...imported,
        messageId: MessageId.make("t3-pending-message"),
        createdAt: "2026-07-25T19:53:12.490Z",
      },
    ]);
  });

  it("keeps distinct or temporally unrelated imported user messages", () => {
    const projectedThread = {
      id: ThreadId.make("019codex-thread"),
      latestTurn: {
        turnId: TurnId.make("turn-pending"),
        state: "running" as const,
        requestedAt: "2026-07-25T19:53:12.490Z",
        startedAt: "2026-07-25T19:53:12.490Z",
        completedAt: null,
        assistantMessageId: null,
      },
      messages: [
        {
          id: MessageId.make("t3-pending-message"),
          role: "user" as const,
          text: "Original prompt.",
          turnId: null,
          streaming: false,
          createdAt: "2026-07-25T19:53:12.490Z",
          updatedAt: "2026-07-25T19:53:12.490Z",
        },
      ],
    } as Parameters<typeof reconcileCodexCliImportedMessages>[1];
    const distinct = {
      messageId: MessageId.make("codex-cli:019codex-thread:item-201"),
      role: "user" as const,
      text: "A genuinely different follow-up.",
      turnId: TurnId.make("turn-pending"),
      createdAt: "2026-07-25T19:53:14.000Z",
    };
    const oldDuplicate = {
      messageId: MessageId.make("codex-cli:019codex-thread:item-100"),
      role: "user" as const,
      text: "Original prompt.",
      turnId: TurnId.make("turn-pending"),
      createdAt: "2026-07-25T18:53:14.000Z",
    };

    expect(reconcileCodexCliImportedMessages([distinct, oldDuplicate], projectedThread)).toEqual([
      distinct,
      oldDuplicate,
    ]);
  });

  it("protects starting and running provider bindings from periodic scan downgrades", () => {
    const baseBinding = {
      threadId: ThreadId.make("019codex-thread"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      runtimeMode: "full-access" as const,
    };

    expect(isLiveCodexBinding({ ...baseBinding, status: "starting" })).toBe(true);
    expect(isLiveCodexBinding({ ...baseBinding, status: "running" })).toBe(true);
    expect(isLiveCodexBinding({ ...baseBinding, status: "stopped" })).toBe(false);
    expect(isLiveCodexBinding({ ...baseBinding, status: "error" })).toBe(false);
    expect(isLiveCodexBinding(undefined)).toBe(false);
  });

  it("selects a differently keyed T3 binding that owns the provider thread", () => {
    const providerThreadId = "019codex-thread";
    const baseBinding = {
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "stopped" as const,
      runtimeMode: "full-access" as const,
      lastSeenAt: "2026-07-24T00:00:00.000Z",
    };
    const ownerBinding = {
      ...baseBinding,
      threadId: ThreadId.make("t3-owned-thread"),
      resumeCursor: { threadId: providerThreadId },
    };

    expect(resolveCodexCliImportBinding(providerThreadId, [ownerBinding])).toBe(ownerBinding);
    expect(isDifferentlyKeyedCodexCliOwnerBinding(providerThreadId, ownerBinding)).toBe(true);
    const sameKeyBinding = {
      ...baseBinding,
      threadId: ThreadId.make(providerThreadId),
      resumeCursor: { threadId: providerThreadId },
    };
    expect(resolveCodexCliImportBinding(providerThreadId, [sameKeyBinding])).toBe(sameKeyBinding);
    expect(isDifferentlyKeyedCodexCliOwnerBinding(providerThreadId, sameKeyBinding)).toBe(false);
    expect(
      resolveCodexCliImportBinding(providerThreadId, [
        {
          ...baseBinding,
          threadId: ThreadId.make("other-provider-thread"),
          provider: ProviderDriverKind.make("claude"),
          resumeCursor: { threadId: providerThreadId },
        },
        {
          ...baseBinding,
          threadId: ThreadId.make("malformed-cursor-thread"),
          resumeCursor: { threadId: 42 },
        },
      ]),
    ).toBeUndefined();
  });

  it("prefers the live T3 owner when duplicate resume bindings exist", () => {
    const providerThreadId = "019codex-thread";
    const stopped = {
      threadId: ThreadId.make("stopped-owner"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "stopped" as const,
      runtimeMode: "full-access" as const,
      resumeCursor: { threadId: providerThreadId },
      lastSeenAt: "2026-07-24T00:00:00.000Z",
    };
    const running = {
      ...stopped,
      threadId: ThreadId.make("running-owner"),
      status: "running" as const,
    };

    expect(resolveCodexCliImportBinding(providerThreadId, [stopped, running])).toBe(running);
  });

  it("interrupts stale projected work once the live provider binding is gone", () => {
    const baseBinding = {
      threadId: ThreadId.make("019codex-thread"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      runtimeMode: "full-access" as const,
    };

    expect(shouldInterruptStaleCodexCliSession(undefined, { status: "running" })).toBe(true);
    expect(
      shouldInterruptStaleCodexCliSession(
        { ...baseBinding, status: "stopped" },
        { status: "starting" },
      ),
    ).toBe(true);
    expect(
      shouldInterruptStaleCodexCliSession(
        { ...baseBinding, status: "running" },
        { status: "running" },
      ),
    ).toBe(false);
    expect(
      shouldInterruptStaleCodexCliSession(
        { ...baseBinding, status: "stopped" },
        { status: "ready" },
      ),
    ).toBe(false);
    expect(shouldInterruptStaleCodexCliSession(undefined, null)).toBe(false);
  });

  it("rechecks interrupted imported mirrors so live CLI work can recover their status", () => {
    const importedBinding = {
      threadId: ThreadId.make("019codex-thread"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "stopped" as const,
      runtimeMode: "full-access" as const,
      runtimePayload: {
        importedFrom: "codex-cli",
      },
    };

    expect(isDetachedCodexCliMirrorSession(importedBinding, { status: "ready" })).toBe(true);
    expect(isDetachedCodexCliMirrorSession(importedBinding, { status: "interrupted" })).toBe(true);
    expect(shouldInspectInterruptedCodexCliMirror(importedBinding, { status: "interrupted" })).toBe(
      true,
    );
    expect(shouldInspectInterruptedCodexCliMirror(importedBinding, { status: "error" })).toBe(true);
    expect(shouldInspectInterruptedCodexCliMirror(importedBinding, { status: "ready" })).toBe(
      false,
    );
    expect(
      shouldInspectInterruptedCodexCliMirror(
        {
          ...importedBinding,
          runtimePayload: {
            importedFrom: "t3",
          },
        },
        { status: "interrupted" },
      ),
    ).toBe(true);
    expect(
      shouldInspectInterruptedCodexCliMirror(
        {
          ...importedBinding,
          provider: ProviderDriverKind.make("claudeAgent"),
        },
        { status: "interrupted" },
      ),
    ).toBe(false);
    expect(
      shouldInspectInterruptedCodexCliMirror(
        {
          ...importedBinding,
          status: "error",
          runtimePayload: {
            sessionPersistence: "detached",
          },
        },
        { status: "error" },
      ),
    ).toBe(false);
    expect(
      shouldInspectInterruptedCodexCliMirror(
        { ...importedBinding, status: "running" },
        { status: "interrupted" },
      ),
    ).toBe(false);
  });

  it("probes rollout ownership for detached mirrors without a projected active turn", () => {
    expect(
      shouldProbeCodexCliRolloutOwner({
        rolloutPath: "/tmp/rollout.jsonl",
        staleActiveTurnId: null,
        hasDetachedMirrorSession: true,
      }),
    ).toBe(true);
    expect(
      shouldProbeCodexCliRolloutOwner({
        rolloutPath: "/tmp/rollout.jsonl",
        staleActiveTurnId: "turn-1",
        hasDetachedMirrorSession: false,
      }),
    ).toBe(true);
    expect(
      shouldProbeCodexCliRolloutOwner({
        rolloutPath: "/tmp/rollout.jsonl",
        staleActiveTurnId: null,
        hasDetachedMirrorSession: false,
      }),
    ).toBe(false);
    expect(
      shouldProbeCodexCliRolloutOwner({
        rolloutPath: undefined,
        staleActiveTurnId: null,
        hasDetachedMirrorSession: true,
      }),
    ).toBe(false);
  });

  it("reconciles stale projected work against the live rollout and upstream turn", () => {
    const activeTurn = {
      status: "interrupted" as const,
      error: null,
      items: [],
    };
    expect(
      resolveStaleCodexCliSession({
        rolloutIsOpen: true,
        rolloutIsRecent: true,
        rolloutTerminalState: null,
        upstreamTurn: activeTurn,
      }),
    ).toEqual({ status: "preserve" });
    expect(
      resolveStaleCodexCliSession({
        rolloutIsOpen: true,
        rolloutIsRecent: false,
        rolloutTerminalState: null,
        upstreamTurn: activeTurn,
      }),
    ).toEqual({
      status: "interrupted",
      lastError:
        "The Codex turn stopped producing activity before it produced a final response. T3 recovered the available transcript.",
    });
    expect(
      resolveStaleCodexCliSession({
        rolloutIsOpen: false,
        rolloutIsRecent: false,
        rolloutTerminalState: null,
        upstreamTurn: activeTurn,
      }),
    ).toEqual({
      status: "interrupted",
      lastError:
        "The Codex process ended before it produced a final response. T3 recovered the available transcript.",
    });
    expect(
      resolveStaleCodexCliSession({
        rolloutIsOpen: true,
        rolloutIsRecent: true,
        rolloutTerminalState: "interrupted",
        upstreamTurn: activeTurn,
      }),
    ).toEqual({
      status: "interrupted",
      lastError: "The Codex turn was interrupted before it produced a final response.",
    });
  });

  it("does not treat an unconfirmed final-answer item as root turn completion", () => {
    expect(
      resolveStaleCodexCliSession({
        rolloutIsOpen: true,
        rolloutIsRecent: true,
        rolloutTerminalState: null,
        upstreamTurn: {
          status: "interrupted",
          error: null,
          items: [
            {
              id: "message-1",
              type: "agentMessage",
              text: "Recovered final",
              phase: "final_answer",
            },
          ],
        },
      }),
    ).toEqual({ status: "preserve" });
  });

  it("settles recovered turns when Codex persisted terminal evidence", () => {
    expect(
      resolveStaleCodexCliSession({
        rolloutIsOpen: false,
        rolloutIsRecent: false,
        rolloutHasFinalResponse: true,
        rolloutTerminalState: "completed",
        upstreamTurn: {
          status: "completed",
          error: null,
          items: [],
        },
      }),
    ).toEqual({
      status: "ready",
      lastError: null,
    });
    expect(
      resolveStaleCodexCliSession({
        rolloutIsOpen: false,
        rolloutIsRecent: false,
        rolloutTerminalState: "completed",
        upstreamTurn: {
          status: "completed",
          error: null,
          items: [],
        },
      }),
    ).toEqual({
      status: "ready",
      lastError:
        "Codex completed this turn without a final response. T3 recovered the available transcript.",
    });
  });

  it("extracts final and interrupted terminal evidence from rollout tails", () => {
    const completed = [
      JSON.stringify({
        type: "event_msg",
        payload: {
          type: "task_complete",
          turn_id: "turn-other",
          last_agent_message: "Ignore this.",
          completed_at: 1_785_004_000,
        },
      }),
      "{malformed",
      JSON.stringify({
        type: "event_msg",
        payload: {
          type: "task_complete",
          turn_id: "turn-target",
          last_agent_message: "Recovered final response.",
          completed_at: 1_785_004_355,
        },
      }),
    ].join("\n");

    expect(parseCodexRolloutTerminalEvidence(completed, "turn-target")).toEqual({
      state: "completed",
      finalMessage: "Recovered final response.",
      completedAt: 1_785_004_355,
    });
    expect(
      parseCodexRolloutTerminalEvidence(
        JSON.stringify({
          type: "event_msg",
          payload: {
            type: "turn_aborted",
            turn_id: "turn-target",
          },
        }),
        "turn-target",
      ),
    ).toEqual({
      state: "interrupted",
      finalMessage: null,
      completedAt: null,
    });
  });

  it("keeps commentary-only recovered turns live until terminal evidence appears", () => {
    const commentaryOnlyTurn = {
      status: "interrupted" as const,
      error: null,
      items: [
        {
          id: "message-commentary",
          type: "agentMessage" as const,
          text: "I am still checking that.",
          phase: "commentary" as const,
        },
      ],
    };

    expect(
      resolveStaleCodexCliSession({
        rolloutIsOpen: true,
        rolloutIsRecent: true,
        rolloutTerminalState: null,
        upstreamTurn: commentaryOnlyTurn,
      }),
    ).toEqual({ status: "preserve" });
    expect(
      resolveStaleCodexCliSession({
        rolloutIsOpen: false,
        rolloutIsRecent: false,
        rolloutTerminalState: null,
        upstreamTurn: commentaryOnlyTurn,
      }),
    ).toEqual({
      status: "interrupted",
      lastError:
        "The Codex process ended before it produced a final response. T3 recovered the available transcript.",
    });
  });

  it("treats legacy unphased assistant messages as final responses", () => {
    expect(
      resolveStaleCodexCliSession({
        rolloutIsOpen: false,
        rolloutIsRecent: false,
        rolloutTerminalState: null,
        upstreamTurn: {
          status: "interrupted",
          error: null,
          items: [
            {
              id: "message-legacy",
              type: "agentMessage",
              text: "Legacy final response",
            },
          ],
        },
      }),
    ).toEqual({ status: "ready", lastError: null });
  });

  it("surfaces failed upstream turns as errors", () => {
    expect(
      resolveStaleCodexCliSession({
        rolloutIsOpen: false,
        rolloutIsRecent: false,
        rolloutTerminalState: null,
        upstreamTurn: {
          status: "failed",
          error: { message: "Provider quota exhausted." },
          items: [],
        },
      }),
    ).toEqual({
      status: "error",
      lastError: "Provider quota exhausted.",
    });
  });

  it("recognizes an unchanged CLI thread from its persisted upstream timestamp", () => {
    const binding = {
      threadId: ThreadId.make("019codex-thread"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "stopped" as const,
      runtimeMode: "full-access" as const,
      runtimePayload: {
        importedFrom: "codex-cli",
        codexCliImportVersion: 2,
        codexCliUpdatedAt: 1_700_000_002,
      },
    };

    expect(isCurrentCodexCliImport(binding, makeThread())).toBe(true);
    expect(
      isCurrentCodexCliImport(
        {
          ...binding,
          runtimePayload: {
            ...binding.runtimePayload,
            codexCliUpdatedAt: 1_700_000_001,
          },
        },
        makeThread(),
      ),
    ).toBe(false);
    expect(
      isCurrentCodexCliImport(
        {
          ...binding,
          runtimePayload: {
            importedFrom: "codex-cli",
            codexCliUpdatedAt: 1_700_000_002,
          },
        },
        makeThread(),
      ),
    ).toBe(false);
    expect(isCurrentCodexCliImport(undefined, makeThread())).toBe(false);
  });

  it("skips unchanged imports when archived threads are absent from active projections", () => {
    const binding = {
      threadId: ThreadId.make("019codex-thread"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "stopped" as const,
      runtimeMode: "full-access" as const,
      runtimePayload: {
        importedFrom: "codex-cli",
        codexCliImportVersion: 2,
        codexCliUpdatedAt: 1_700_000_002,
      },
    };

    expect(shouldSkipCurrentCodexCliImport(binding, makeThread(), false)).toBe(true);
    expect(shouldSkipCurrentCodexCliImport(binding, makeThread(), true)).toBe(false);
    expect(
      shouldSkipCurrentCodexCliImport(
        {
          ...binding,
          runtimePayload: {
            ...binding.runtimePayload,
            codexCliUpdatedAt: 1_700_000_001,
          },
        },
        makeThread(),
        false,
      ),
    ).toBe(false);
  });

  it("preserves unchanged open CLI turns without rereading their transcripts", () => {
    const binding = {
      threadId: ThreadId.make("019codex-thread"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "stopped" as const,
      runtimeMode: "full-access" as const,
      runtimePayload: {
        importedFrom: "codex-cli",
        codexCliImportVersion: 2,
        codexCliUpdatedAt: 1_700_000_002,
      },
    };
    const input = {
      binding,
      listedThread: makeThread(),
      staleActiveTurnId: "turn-2",
      rolloutIsOpen: true,
      rolloutTerminalEvidence: {
        state: null,
        finalMessage: null,
        completedAt: null,
      },
      nowMillis: 1_700_000_002_000 + 60_000,
    } as const;

    expect(shouldPreserveCurrentOpenCodexCliImport(input)).toBe(true);
    expect(isRecentCodexCliActivity(input.listedThread.updatedAt, input.nowMillis)).toBe(true);
    expect(
      shouldPreserveCurrentOpenCodexCliImport({
        ...input,
        nowMillis: 1_700_000_002_000 + 20 * 60_000,
      }),
    ).toBe(false);
    expect(
      shouldReconcileCurrentCodexCliSessionWithoutRead({
        ...input,
        nowMillis: 1_700_000_002_000 + 20 * 60_000,
      }),
    ).toBe(true);
    expect(
      isRecentCodexCliActivity(input.listedThread.updatedAt, 1_700_000_002_000 + 20 * 60_000),
    ).toBe(false);
    expect(
      shouldPreserveCurrentOpenCodexCliImport({
        ...input,
        listedThread: { ...input.listedThread, updatedAt: 1_700_000_003 },
      }),
    ).toBe(false);
    expect(
      shouldPreserveCurrentOpenCodexCliImport({
        ...input,
        staleActiveTurnId: null,
      }),
    ).toBe(false);
    expect(
      shouldPreserveCurrentOpenCodexCliImport({
        ...input,
        rolloutIsOpen: false,
      }),
    ).toBe(false);
    expect(
      shouldReconcileCurrentCodexCliSessionWithoutRead({
        ...input,
        rolloutIsOpen: false,
      }),
    ).toBe(true);
    expect(
      shouldPreserveCurrentOpenCodexCliImport({
        ...input,
        rolloutTerminalEvidence: {
          state: "completed",
          finalMessage: null,
          completedAt: 1_700_000_003,
        },
      }),
    ).toBe(false);
    expect(
      shouldPreserveCurrentOpenCodexCliImport({
        ...input,
        rolloutTerminalEvidence: {
          state: "interrupted",
          finalMessage: null,
          completedAt: null,
        },
      }),
    ).toBe(false);
    expect(
      shouldPreserveCurrentOpenCodexCliImport({
        ...input,
        rolloutTerminalEvidence: {
          state: null,
          finalMessage: "Recovered final response.",
          completedAt: null,
        },
      }),
    ).toBe(false);
    expect(
      shouldReconcileCurrentCodexCliSessionWithoutRead({
        ...input,
        rolloutTerminalEvidence: {
          state: null,
          finalMessage: "Recovered final response.",
          completedAt: null,
        },
      }),
    ).toBe(false);
  });

  it("discovers both Codex interactive source buckets without importing automation", () => {
    expect(CODEX_INTERACTIVE_SOURCE_KINDS).toEqual(["cli", "vscode"]);
    expect(isImportableCodexInteractiveThread(makeThread())).toBe(true);
    expect(
      isImportableCodexInteractiveThread({
        ...makeThread(),
        source: "vscode",
        threadSource: "user",
      }),
    ).toBe(true);
    expect(
      isImportableCodexInteractiveThread({
        ...makeThread(),
        source: "vscode",
        threadSource: "subagent",
      }),
    ).toBe(false);
    expect(
      isImportableCodexInteractiveThread({
        ...makeThread(),
        ephemeral: true,
      }),
    ).toBe(false);
  });
});
