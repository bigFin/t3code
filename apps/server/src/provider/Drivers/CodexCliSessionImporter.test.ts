import {
  MessageId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type OrchestrationThread,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import { it as effectIt } from "@effect/vitest";
import { describe, expect, it } from "vite-plus/test";
import type * as CodexSchema from "effect-codex-app-server/schema";

import {
  advanceCodexRolloutTaskCursor,
  advanceCodexRolloutMessageCursor,
  CODEX_INTERACTIVE_SOURCE_KINDS,
  codexRolloutCompleteUtf8PrefixLength,
  codexCliMessageImportCommand,
  collectCodexCliImportedMessages,
  collectCodexCliRolloutMessages,
  hasSynchronizedCodexCliTranscript,
  isCodexRolloutPathWithinSessionsRoot,
  isCodexCliImportFreshForRollout,
  isCompleteCodexRolloutMessageRead,
  isCurrentCodexCliImport,
  isDetachedCodexCliObserverBinding,
  isDetachedCodexCliTranscriptRefreshBinding,
  isDetachedCodexCliMirrorSession,
  isDifferentlyKeyedCodexCliOwnerBinding,
  isImportableCodexInteractiveThread,
  isLiveCodexBinding,
  isRecentCodexCliActivity,
  isRecentCodexCliRolloutActivity,
  mergeCodexCliRolloutMessages,
  parseCodexRolloutTerminalEvidence,
  parseLatestCodexRolloutTaskState,
  pruneCodexCliImportCache,
  reconcileCodexCliImportedMessages,
  resolveCodexCliImportBinding,
  resolveCodexCliExpectedUserMessageIds,
  resolveCodexCliProviderRuntimeExpectation,
  resolveCodexCliTranscriptMessages,
  readCodexCliImportedAt,
  resolveObservedCodexCliSessionState,
  resolveStaleCodexCliSession,
  shouldInspectInterruptedCodexCliMirror,
  shouldInspectDetachedCodexCliObserver,
  shouldApplyObservedCodexCliSessionState,
  shouldImportCodexCliMessages,
  shouldInterruptStaleCodexCliSession,
  shouldPersistCodexCliImportMetadata,
  shouldProbeCodexCliRolloutOwner,
  shouldSkipCurrentCodexCliImport,
  shouldSkipUnchangedDetachedCodexCliObserver,
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
        messageId: MessageId.make("codex-cli:019legacy-thread:rollout:55c625b84fed838b"),
        role: "user",
        text: "Inspect the legacy session.",
        turnId: TurnId.make("turn-legacy"),
        createdAt: "2026-07-16T15:29:37.589Z",
      },
      {
        messageId: MessageId.make("codex-cli:019legacy-thread:assistant-legacy"),
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
        messageId: MessageId.make("codex-cli:019legacy-thread:rollout:a1255403be90f725"),
        role: "user",
        text: "Legacy user prompt.",
        turnId: TurnId.make("codex-cli:019legacy-thread:rollout-turn:a1255403be90f725"),
        createdAt: "2026-07-16T15:29:37.589Z",
      },
    ]);
  });

  it("prefers raw user items over near-simultaneous legacy user events", () => {
    const contents = [
      JSON.stringify({
        timestamp: "2026-07-16T15:29:37.500Z",
        type: "response_item",
        payload: {
          id: "user-1",
          type: "message",
          role: "user",
          content: [{ type: "input_text", text: "Proceed." }],
          internal_chat_message_metadata_passthrough: {
            turn_id: "turn-1",
          },
        },
      }),
      JSON.stringify({
        timestamp: "2026-07-16T15:29:37.589Z",
        type: "event_msg",
        payload: {
          type: "user_message",
          message: "Proceed.",
          turn_id: "turn-1",
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
        messageId: MessageId.make("codex-cli:019legacy-thread:user-1"),
        role: "user",
        text: "Proceed.",
        turnId: TurnId.make("turn-1"),
        createdAt: "2026-07-16T15:29:37.500Z",
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

  it("merges canonical rollout messages without duplicate fallback scans", () => {
    const fallback = collectCodexCliRolloutMessages({
      threadId: "019legacy-thread",
      contents: JSON.stringify({
        timestamp: "2026-07-16T15:29:37.500Z",
        type: "event_msg",
        payload: {
          type: "user_message",
          message: "Recovered.",
        },
      }),
      createdAt: 1_784_215_777,
    })[0]!;
    const canonical = {
      ...fallback,
      messageId: MessageId.make("codex-cli:019legacy-thread:assistant-1"),
    };
    const longerCanonical = {
      ...canonical,
      text: "Recovered with the complete final response.",
    };

    expect(mergeCodexCliRolloutMessages([fallback], [canonical])).toEqual([canonical]);
    expect(mergeCodexCliRolloutMessages([canonical], [longerCanonical])).toEqual([longerCanonical]);
  });

  it("keeps raw Codex message identity stable across app-server and rollout sources", () => {
    const appServerMessage = collectCodexCliImportedMessages(makeThread())[0];
    const [rolloutMessage] = collectCodexCliRolloutMessages({
      threadId: "019codex-thread",
      contents: JSON.stringify({
        timestamp: "2023-11-14T22:13:21.000Z",
        type: "response_item",
        payload: {
          id: "message-user-1",
          type: "message",
          role: "user",
          content: [{ type: "input_text", text: "Inspect this." }],
          internal_chat_message_metadata_passthrough: {
            turn_id: "turn-1",
          },
        },
      }),
      createdAt: 1_700_000_000,
    });

    expect(appServerMessage?.messageId).toBe(
      MessageId.make("codex-cli:019codex-thread:message-user-1"),
    );
    expect(rolloutMessage?.messageId).toBe(appServerMessage?.messageId);
    expect(rolloutMessage?.turnId).toBe(appServerMessage?.turnId);
  });

  it("uses rollout recovery for non-empty partial app-server transcripts", () => {
    const thread = {
      ...makeThread(),
      turns: makeThread().turns.map((turn, index) =>
        index === 1
          ? {
              ...turn,
              itemsView: "summary" as const,
            }
          : turn,
      ),
    };
    const rollout = {
      complete: true,
      messages: collectCodexCliRolloutMessages({
        threadId: thread.id,
        contents: JSON.stringify({
          timestamp: "2023-11-14T22:13:22.000Z",
          type: "response_item",
          payload: {
            id: "message-agent-2",
            type: "message",
            role: "assistant",
            content: [{ type: "output_text", text: "Recovered from rollout." }],
            internal_chat_message_metadata_passthrough: {
              turn_id: "turn-2",
            },
          },
        }),
        createdAt: thread.createdAt,
      }),
    };

    const resolved = resolveCodexCliTranscriptMessages({ thread, rollout });

    expect(resolved.complete).toBe(true);
    expect(resolved.messages).toHaveLength(4);
    expect(resolved.messages.at(-1)).toMatchObject({
      messageId: MessageId.make("codex-cli:019codex-thread:message-agent-2"),
      text: "Recovered from rollout.",
    });
  });

  it("does not require rollout fallback for complete app-server transcripts", () => {
    const thread = makeThread();
    const resolved = resolveCodexCliTranscriptMessages({
      thread,
      rollout: {
        complete: false,
        messages: [
          {
            messageId: MessageId.make("rollout-only"),
            role: "assistant",
            text: "Should not be selected.",
            turnId: TurnId.make("turn-rollout"),
            createdAt: "2026-08-03T00:00:00.000Z",
          },
        ],
      },
    });

    expect(resolved).toEqual({
      complete: true,
      messages: collectCodexCliImportedMessages(thread),
    });
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

    const expectedProviderRuntime = {
      providerName: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running" as const,
      lastSeenAt: "2026-08-03T00:00:00.000Z",
      resumeCursor: { threadId: "019codex-thread" },
      requiresDetachedIdle: true,
    };
    const guarded = codexCliMessageImportCommand({
      threadId: ThreadId.make("019codex-thread"),
      message,
      expectedProviderRuntime,
    });
    expect(guarded.expectedProviderRuntime).toEqual(expectedProviderRuntime);
    expect(guarded.commandId).not.toBe(first.commandId);
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

  it("reuses the projected message id after the pending prompt gains its turn id", () => {
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
          text: imported.text,
          turnId: imported.turnId,
          streaming: false,
          createdAt: "2026-07-25T19:53:12.490Z",
          updatedAt: "2026-07-25T19:53:14.000Z",
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

  it("observes independently updated CLI threads through idle detached bindings", () => {
    const binding = {
      threadId: ThreadId.make("t3-owned-thread"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running" as const,
      runtimeMode: "full-access" as const,
      resumeCursor: { threadId: "019codex-thread" },
      runtimePayload: {
        activeTurnId: null,
        sessionPersistence: "detached",
        codexCliImportVersion: 2,
        codexCliUpdatedAt: 1_700_000_002,
      },
    };
    const thread = makeThread();

    expect(isDetachedCodexCliObserverBinding(binding)).toBe(true);
    expect(
      shouldInspectDetachedCodexCliObserver({
        binding,
        listedThread: thread,
        projectedSessionStatus: "ready",
      }),
    ).toBe(false);
    expect(
      shouldInspectDetachedCodexCliObserver({
        binding,
        listedThread: thread,
        projectedSessionStatus: "starting",
      }),
    ).toBe(true);
    expect(
      shouldInspectDetachedCodexCliObserver({
        binding,
        listedThread: { ...thread, status: { type: "active", activeFlags: [] } },
        projectedSessionStatus: "running",
      }),
    ).toBe(true);
    expect(
      shouldInspectDetachedCodexCliObserver({
        binding,
        listedThread: { ...thread, updatedAt: 1_700_000_003 },
        projectedSessionStatus: "ready",
      }),
    ).toBe(false);
    expect(
      shouldInspectDetachedCodexCliObserver({
        binding,
        listedThread: thread,
        projectedSessionStatus: "interrupted",
      }),
    ).toBe(false);
    expect(
      isDetachedCodexCliObserverBinding({
        ...binding,
        runtimePayload: {
          ...binding.runtimePayload,
          activeTurnId: "turn-owned-by-t3",
        },
      }),
    ).toBe(false);
    expect(
      isDetachedCodexCliObserverBinding({
        ...binding,
        runtimePayload: {
          ...binding.runtimePayload,
          sessionPersistence: "process-bound",
        },
      }),
    ).toBe(false);
  });

  it("hydrates replay gaps without taking ownership of an active detached session", () => {
    const binding = {
      threadId: ThreadId.make("t3-owned-thread"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running" as const,
      runtimeMode: "full-access" as const,
      resumeCursor: { threadId: "019codex-thread" },
      runtimePayload: {
        activeTurnId: "turn-owned-by-provider",
        sessionPersistence: "detached",
        codexCliTranscriptRefreshRequired: true,
      },
    };

    expect(isDetachedCodexCliObserverBinding(binding)).toBe(false);
    expect(isDetachedCodexCliTranscriptRefreshBinding(binding)).toBe(true);
    expect(
      shouldImportCodexCliMessages({
        importIsCurrent: true,
        hasStaleSession: false,
        observesDetachedCliSession: false,
        observerNeedsHydration: false,
        refreshesDetachedCliTranscript: true,
      }),
    ).toBe(true);
    expect(
      shouldPersistCodexCliImportMetadata({
        observesDetachedCliSession: false,
        observerSessionSynchronized: true,
        transcriptHydrationComplete: false,
        transcriptSynchronized: true,
      }),
    ).toBe(false);
  });

  it("allows a newer external turn to replace the turn observed at scan start", () => {
    const makeSession = (activeTurnId: TurnId) => ({
      threadId: ThreadId.make("t3-owned-thread"),
      status: "running" as const,
      providerName: "codex",
      providerInstanceId: ProviderInstanceId.make("codex"),
      runtimeMode: "full-access" as const,
      activeTurnId,
      lastError: null,
      retrying: false,
      updatedAt: "2026-08-02T12:00:00.000Z",
    });
    const preparedThread = {
      latestUserMessageAt: null,
      session: makeSession(TurnId.make("turn-a")),
      updatedAt: "2026-08-02T12:00:00.000Z",
    };

    expect(
      shouldApplyObservedCodexCliSessionState({
        currentThread: preparedThread,
        preparedThread,
      }),
    ).toBe(true);
    expect(
      shouldApplyObservedCodexCliSessionState({
        currentThread: {
          ...preparedThread,
          session: makeSession(TurnId.make("turn-c")),
        },
        preparedThread,
      }),
    ).toBe(false);
    expect(
      shouldApplyObservedCodexCliSessionState({
        currentThread: {
          ...preparedThread,
          latestUserMessageAt: "2026-08-02T12:00:01.000Z",
          updatedAt: "2026-08-02T12:00:01.000Z",
        },
        preparedThread,
      }),
    ).toBe(false);
    expect(
      shouldApplyObservedCodexCliSessionState({
        currentThread: {
          latestUserMessageAt: null,
          session: null,
          updatedAt: "2026-08-02T12:00:00.000Z",
        },
        preparedThread: undefined,
      }),
    ).toBe(true);
    expect(
      shouldApplyObservedCodexCliSessionState({
        currentThread: {
          latestUserMessageAt: "2026-08-02T12:00:01.000Z",
          session: null,
          updatedAt: "2026-08-02T12:00:01.000Z",
        },
        preparedThread: undefined,
      }),
    ).toBe(false);
  });

  it("tracks exact user message identity expected after transcript import", () => {
    const projectedThread = {
      messages: [
        {
          id: MessageId.make("existing-user-message"),
          role: "user",
          text: "Existing",
          turnId: null,
          streaming: false,
          createdAt: "2026-08-03T00:00:00.000Z",
          updatedAt: "2026-08-03T00:00:00.000Z",
        },
      ],
    } satisfies Pick<OrchestrationThread, "messages">;

    expect(
      resolveCodexCliExpectedUserMessageIds(projectedThread, [
        {
          messageId: MessageId.make("assistant-message"),
          role: "assistant",
          text: "Working",
          turnId: TurnId.make("turn-1"),
          createdAt: "2026-08-03T00:00:01.000Z",
        },
        {
          messageId: MessageId.make("existing-user-message"),
          role: "user",
          text: "Existing",
          turnId: TurnId.make("turn-1"),
          createdAt: "2026-08-04T00:00:00.000Z",
        },
        {
          messageId: MessageId.make("a"),
          role: "user",
          text: "Lowercase",
          turnId: TurnId.make("turn-2"),
          createdAt: "2026-08-03T00:00:01.000Z",
        },
        {
          messageId: MessageId.make("B"),
          role: "user",
          text: "Uppercase",
          turnId: TurnId.make("turn-2"),
          createdAt: "2026-08-03T00:00:01.000Z",
        },
        {
          messageId: MessageId.make("newer-user-message"),
          role: "user",
          text: "Continue",
          turnId: TurnId.make("turn-2"),
          createdAt: "2026-08-03T00:00:02.000Z",
        },
      ]),
    ).toEqual([
      MessageId.make("existing-user-message"),
      MessageId.make("B"),
      MessageId.make("a"),
      MessageId.make("newer-user-message"),
    ]);
  });

  it("prunes rollout and import caches to the current discovery window", () => {
    const cache = new Map([
      ["active", 1],
      ["expired", 2],
    ]);

    pruneCodexCliImportCache(cache, new Set(["active"]));

    expect([...cache.entries()]).toEqual([["active", 1]]);
  });

  it("incrementally parses rollout messages and recovers cleanly after truncation", () => {
    const user = JSON.stringify({
      timestamp: "2026-08-03T00:00:00.000Z",
      type: "response_item",
      payload: {
        id: "user-1",
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "Proceed." }],
      },
    });
    const assistant = JSON.stringify({
      timestamp: "2026-08-03T00:00:01.000Z",
      type: "response_item",
      payload: {
        id: "assistant-1",
        type: "message",
        role: "assistant",
        content: [{ type: "output_text", text: "Done." }],
      },
    });
    const splitAt = Math.floor(assistant.length / 2);
    const deferredTrailingRecord = advanceCodexRolloutMessageCursor({
      cursor: undefined,
      contents: user,
      offset: user.length,
      modifiedAtMillis: 1,
      threadId: "019codex-thread",
      createdAt: 1_700_000_000,
      isFinalChunk: false,
    });
    expect(deferredTrailingRecord.messages).toHaveLength(0);
    expect(deferredTrailingRecord.pendingLine).toBe(user);
    const flushedTrailingRecord = advanceCodexRolloutMessageCursor({
      cursor: deferredTrailingRecord,
      contents: "",
      offset: user.length,
      modifiedAtMillis: 1,
      threadId: "019codex-thread",
      createdAt: 1_700_000_000,
      isFinalChunk: true,
    });
    expect(flushedTrailingRecord.messages.map((message) => message.messageId)).toEqual([
      MessageId.make("codex-cli:019codex-thread:user-1"),
    ]);
    expect(flushedTrailingRecord.pendingLine).toBe("");

    const first = advanceCodexRolloutMessageCursor({
      cursor: undefined,
      contents: `${user}\n${assistant.slice(0, splitAt)}`,
      offset: user.length + 1 + splitAt,
      modifiedAtMillis: 1,
      threadId: "019codex-thread",
      createdAt: 1_700_000_000,
    });
    expect(first.messages).toHaveLength(1);
    expect(first.pendingLine).toBe(assistant.slice(0, splitAt));
    expect(isCompleteCodexRolloutMessageRead(first, first.offset)).toBe(false);

    const completed = advanceCodexRolloutMessageCursor({
      cursor: first,
      contents: assistant.slice(splitAt),
      offset: user.length + 1 + assistant.length,
      modifiedAtMillis: 2,
      threadId: "019codex-thread",
      createdAt: 1_700_000_000,
    });
    expect(completed.messages.map((message) => message.messageId)).toEqual([
      MessageId.make("codex-cli:019codex-thread:user-1"),
      MessageId.make("codex-cli:019codex-thread:assistant-1"),
    ]);
    expect(completed.pendingLine).toBe("");
    expect(isCompleteCodexRolloutMessageRead(completed, completed.offset)).toBe(true);
    expect(isCompleteCodexRolloutMessageRead(completed, completed.offset + 1)).toBe(false);

    const replacement = advanceCodexRolloutMessageCursor({
      cursor: undefined,
      contents: `${assistant}\n`,
      offset: assistant.length + 1,
      modifiedAtMillis: 3,
      threadId: "019codex-thread",
      createdAt: 1_700_000_000,
    });
    expect(replacement.messages.map((message) => message.messageId)).toEqual([
      MessageId.make("codex-cli:019codex-thread:assistant-1"),
    ]);
  });

  it("does not commit a rollout cursor through an incomplete UTF-8 character", () => {
    const record = JSON.stringify({
      timestamp: "2026-08-03T00:00:00.000Z",
      type: "response_item",
      payload: {
        id: "assistant-unicode",
        type: "message",
        role: "assistant",
        content: [{ type: "output_text", text: "Done 🙂" }],
      },
    });
    const bytes = new TextEncoder().encode(record);
    const emojiStart = bytes.findIndex((byte) => byte === 0xf0);
    expect(emojiStart).toBeGreaterThan(0);
    const firstBytes = bytes.slice(0, emojiStart + 2);
    const firstCompleteLength = codexRolloutCompleteUtf8PrefixLength(firstBytes);
    expect(firstCompleteLength).toBe(emojiStart);
    const first = advanceCodexRolloutMessageCursor({
      cursor: undefined,
      contents: new TextDecoder().decode(firstBytes.slice(0, firstCompleteLength)),
      offset: firstCompleteLength,
      modifiedAtMillis: 1,
      threadId: "019codex-thread",
      createdAt: 1_700_000_000,
      isFinalChunk: false,
    });
    expect(first.messages).toHaveLength(0);

    const remainingBytes = new Uint8Array(
      firstBytes.length - firstCompleteLength + bytes.length - firstBytes.length,
    );
    remainingBytes.set(firstBytes.slice(firstCompleteLength));
    remainingBytes.set(bytes.slice(firstBytes.length), firstBytes.length - firstCompleteLength);
    expect(codexRolloutCompleteUtf8PrefixLength(remainingBytes)).toBe(remainingBytes.length);
    const completed = advanceCodexRolloutMessageCursor({
      cursor: first,
      contents: new TextDecoder().decode(remainingBytes),
      offset: bytes.length,
      modifiedAtMillis: 2,
      threadId: "019codex-thread",
      createdAt: 1_700_000_000,
      isFinalChunk: true,
    });
    expect(completed.pendingLine).toBe("");
    expect(completed.messages).toHaveLength(1);
    expect(completed.messages[0]?.text).toBe("Done 🙂");
  });

  it("verifies imported transcript content before advancing observer freshness", () => {
    const imported = {
      messageId: MessageId.make("message-1"),
      role: "assistant" as const,
      text: "Recovered.",
      turnId: TurnId.make("turn-1"),
      createdAt: "2026-08-03T00:00:00.000Z",
    };
    const projected = {
      messages: [
        {
          id: imported.messageId,
          role: imported.role,
          text: imported.text,
          turnId: imported.turnId,
          streaming: false,
          createdAt: imported.createdAt,
          updatedAt: imported.createdAt,
        },
      ],
    } satisfies Pick<OrchestrationThread, "messages">;

    expect(hasSynchronizedCodexCliTranscript(projected, [imported])).toBe(true);
    expect(
      hasSynchronizedCodexCliTranscript(
        {
          messages: projected.messages.map((message) => ({
            ...message,
            text: "Stale.",
          })),
        },
        [imported],
      ),
    ).toBe(false);
  });

  it("uses precise rollout mtimes to detect same-second detached CLI updates", () => {
    const importedAtMillis = readCodexCliImportedAt({
      importedAt: "2026-08-02T12:00:04.000Z",
    });

    expect(importedAtMillis).toBe(Date.parse("2026-08-02T12:00:04.000Z"));
    expect(
      isCodexCliImportFreshForRollout(importedAtMillis, Date.parse("2026-08-02T12:00:03.999Z")),
    ).toBe(true);
    expect(
      isCodexCliImportFreshForRollout(importedAtMillis, Date.parse("2026-08-02T12:00:04.500Z")),
    ).toBe(false);
    expect(
      isRecentCodexCliRolloutActivity(
        Date.parse("2026-08-03T00:00:04.500Z"),
        Date.parse("2026-08-03T00:00:05.000Z"),
      ),
    ).toBe(true);
  });

  it("imports detached observer messages when rollout mtime exposes a same-second update", () => {
    expect(
      shouldImportCodexCliMessages({
        importIsCurrent: true,
        hasStaleSession: false,
        observesDetachedCliSession: true,
        observerNeedsHydration: true,
      }),
    ).toBe(true);
    expect(
      shouldImportCodexCliMessages({
        importIsCurrent: true,
        hasStaleSession: false,
        observesDetachedCliSession: true,
        observerNeedsHydration: false,
      }),
    ).toBe(false);
  });

  it("does not mark detached observer imports current until session state is synchronized", () => {
    expect(
      shouldPersistCodexCliImportMetadata({
        observesDetachedCliSession: true,
        observerSessionSynchronized: false,
      }),
    ).toBe(false);
    expect(
      shouldPersistCodexCliImportMetadata({
        observesDetachedCliSession: true,
        observerSessionSynchronized: true,
      }),
    ).toBe(true);
    expect(
      shouldPersistCodexCliImportMetadata({
        observesDetachedCliSession: false,
        observerSessionSynchronized: false,
      }),
    ).toBe(true);
    expect(
      shouldPersistCodexCliImportMetadata({
        observesDetachedCliSession: false,
        observerSessionSynchronized: true,
        transcriptHydrationComplete: true,
        transcriptSynchronized: false,
      }),
    ).toBe(false);
  });

  it("does not infer running state from a fresh rollout cursor initialized at EOF", () => {
    expect(
      shouldSkipUnchangedDetachedCodexCliObserver({
        observesDetachedCliSession: true,
        observerNeedsHydration: false,
        rolloutChanged: false,
        inspectDetachedCliSession: false,
      }),
    ).toBe(true);
    expect(
      shouldSkipUnchangedDetachedCodexCliObserver({
        observesDetachedCliSession: true,
        observerNeedsHydration: false,
        rolloutChanged: false,
        inspectDetachedCliSession: true,
      }),
    ).toBe(false);
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

  it("captures exact provider runtime ownership for session reconciliation", () => {
    const binding = {
      threadId: ThreadId.make("t3-owned-thread"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex-work"),
      status: "stopped" as const,
      runtimeMode: "full-access" as const,
      resumeCursor: { threadId: "019codex-thread" },
      lastSeenAt: "2026-08-03T00:00:00.000Z",
    };

    expect(resolveCodexCliProviderRuntimeExpectation(undefined, false)).toBeNull();
    expect(resolveCodexCliProviderRuntimeExpectation(binding, false)).toEqual({
      providerName: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex-work"),
      status: "stopped",
      lastSeenAt: "2026-08-03T00:00:00.000Z",
      resumeCursor: { threadId: "019codex-thread" },
      requiresDetachedIdle: false,
    });
    expect(
      resolveCodexCliProviderRuntimeExpectation(
        {
          ...binding,
          status: "running",
          lastSeenAt: "2026-08-03T00:00:01.000Z",
        },
        true,
      ),
    ).toEqual({
      providerName: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex-work"),
      status: "running",
      lastSeenAt: "2026-08-03T00:00:01.000Z",
      resumeCursor: { threadId: "019codex-thread" },
      requiresDetachedIdle: true,
    });
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
        rolloutPath: "/tmp/rollout.jsonl",
        staleActiveTurnId: null,
        hasDetachedMirrorSession: false,
        observesDetachedCliSession: true,
      }),
    ).toBe(true);
    expect(
      shouldProbeCodexCliRolloutOwner({
        rolloutPath: "/tmp/rollout.jsonl",
        staleActiveTurnId: null,
        hasDetachedMirrorSession: false,
        refreshesDetachedCliTranscript: true,
      }),
    ).toBe(true);
    expect(
      shouldProbeCodexCliRolloutOwner({
        rolloutPath: undefined,
        staleActiveTurnId: null,
        hasDetachedMirrorSession: true,
      }),
    ).toBe(false);
  });

  it("reads the latest global Codex rollout task lifecycle state", () => {
    const started = [
      JSON.stringify({
        type: "event_msg",
        payload: { type: "task_started", turn_id: "turn-1" },
      }),
    ].join("\n");
    expect(parseLatestCodexRolloutTaskState(started)).toEqual({
      state: "active",
      turnId: "turn-1",
    });

    const completed = [
      started,
      JSON.stringify({
        type: "event_msg",
        payload: { type: "task_complete", turn_id: "turn-1" },
      }),
    ].join("\n");
    expect(parseLatestCodexRolloutTaskState(completed)).toEqual({
      state: "completed",
      turnId: "turn-1",
    });

    const interrupted = [
      completed,
      JSON.stringify({
        type: "event_msg",
        payload: { type: "turn_aborted", turn_id: "turn-2" },
      }),
    ].join("\n");
    expect(parseLatestCodexRolloutTaskState(interrupted)).toEqual({
      state: "interrupted",
      turnId: "turn-2",
    });
  });

  it("incrementally tracks rollout task state without rereading unchanged history", () => {
    const started = JSON.stringify({
      type: "event_msg",
      payload: { type: "task_started", turn_id: "turn-1" },
    });
    const splitAt = Math.floor(started.length / 2);
    const partial = advanceCodexRolloutTaskCursor(undefined, started.slice(0, splitAt), splitAt);
    expect(partial.lifecycle.state).toBeNull();

    const active = advanceCodexRolloutTaskCursor(
      partial,
      `${started.slice(splitAt)}\n${JSON.stringify({
        type: "response_item",
        payload: { type: "function_call", name: "inspect" },
      })}\n`,
      started.length + 100,
    );
    expect(active.lifecycle).toEqual({
      state: "active",
      turnId: "turn-1",
    });

    const unchanged = advanceCodexRolloutTaskCursor(
      active,
      `${JSON.stringify({
        type: "response_item",
        payload: { type: "function_call_output", output: "done" },
      })}\n`,
      started.length + 200,
    );
    expect(unchanged.lifecycle).toEqual({
      state: "active",
      turnId: "turn-1",
    });

    const completed = advanceCodexRolloutTaskCursor(
      unchanged,
      `${JSON.stringify({
        type: "event_msg",
        payload: { type: "task_complete", turn_id: "turn-1" },
      })}\n`,
      started.length + 300,
    );
    expect(completed.lifecycle).toEqual({
      state: "completed",
      turnId: "turn-1",
    });
    expect(completed.terminalTransitionObserved).toBe(true);
    expect(completed.pendingLine).toBe("");

    const nextTurn = advanceCodexRolloutTaskCursor(
      completed,
      [
        JSON.stringify({
          type: "event_msg",
          payload: { type: "task_complete", turn_id: "turn-1" },
        }),
        JSON.stringify({
          type: "event_msg",
          payload: { type: "task_started", turn_id: "turn-2" },
        }),
        "",
      ].join("\n"),
      started.length + 500,
    );
    expect(nextTurn.lifecycle).toEqual({
      state: "active",
      turnId: "turn-2",
    });
    expect(nextTurn.terminalTransitionObserved).toBe(true);
  });

  it("settles detached CLI observation when active rollout evidence goes stale", () => {
    expect(
      resolveObservedCodexCliSessionState({
        taskState: "active",
        listedThreadIsActive: false,
        listedThreadHasSystemError: false,
        rolloutEvidenceAvailable: true,
        rolloutIsOpen: false,
        rolloutIsRecent: true,
      }),
    ).toEqual({
      status: "running",
      activeTurnId: null,
      lastError: null,
    });
    expect(
      resolveObservedCodexCliSessionState({
        taskState: "active",
        listedThreadIsActive: false,
        listedThreadHasSystemError: false,
        rolloutEvidenceAvailable: true,
        rolloutIsOpen: true,
        rolloutIsRecent: false,
      }),
    ).toEqual({
      status: "running",
      activeTurnId: null,
      lastError: null,
    });
    expect(
      resolveObservedCodexCliSessionState({
        taskState: "active",
        listedThreadIsActive: false,
        listedThreadHasSystemError: false,
        rolloutEvidenceAvailable: true,
        rolloutIsOpen: false,
        rolloutIsRecent: false,
      }),
    ).toEqual({
      status: "interrupted",
      activeTurnId: null,
      lastError: "The Codex process ended before it produced a final response.",
    });
    expect(
      resolveObservedCodexCliSessionState({
        taskState: "interrupted",
        listedThreadIsActive: false,
        listedThreadHasSystemError: false,
        rolloutEvidenceAvailable: true,
        rolloutIsOpen: false,
        rolloutIsRecent: false,
      }),
    ).toEqual({
      status: "interrupted",
      activeTurnId: null,
      lastError: "The Codex turn was interrupted before it produced a final response.",
    });
    expect(
      resolveObservedCodexCliSessionState({
        taskState: "completed",
        listedThreadIsActive: true,
        listedThreadHasSystemError: true,
        rolloutEvidenceAvailable: true,
        rolloutIsOpen: true,
        rolloutIsRecent: true,
      }),
    ).toEqual({
      status: "ready",
      activeTurnId: null,
      lastError: null,
    });
    expect(
      resolveObservedCodexCliSessionState({
        taskState: null,
        listedThreadIsActive: false,
        listedThreadHasSystemError: true,
        rolloutEvidenceAvailable: true,
        rolloutIsOpen: false,
        rolloutIsRecent: false,
      }),
    ).toEqual({
      status: "error",
      activeTurnId: null,
      lastError: "Codex reported a system error for this thread.",
    });
    expect(
      resolveObservedCodexCliSessionState({
        taskState: null,
        listedThreadIsActive: false,
        listedThreadHasSystemError: true,
        rolloutEvidenceAvailable: true,
        rolloutIsOpen: true,
        rolloutIsRecent: false,
      }),
    ).toEqual({
      status: "running",
      activeTurnId: null,
      lastError: null,
    });
    expect(
      resolveObservedCodexCliSessionState({
        taskState: null,
        listedThreadIsActive: true,
        listedThreadHasSystemError: false,
        rolloutEvidenceAvailable: true,
        rolloutIsOpen: false,
        rolloutIsRecent: false,
      }),
    ).toEqual({
      status: "ready",
      activeTurnId: null,
      lastError: null,
    });
    expect(
      resolveObservedCodexCliSessionState({
        taskState: null,
        listedThreadIsActive: true,
        listedThreadHasSystemError: false,
        rolloutEvidenceAvailable: false,
        rolloutIsOpen: false,
        rolloutIsRecent: false,
      }),
    ).toEqual({
      status: "running",
      activeTurnId: null,
      lastError: null,
    });
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
