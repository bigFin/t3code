import {
  CheckpointRef,
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
  advanceCodexCliImportFailureBackoff,
  CODEX_INTERACTIVE_SOURCE_KINDS,
  codexRolloutCompleteUtf8PrefixLength,
  codexCliMessagesImportCommand,
  collectCodexCliImportedMessages,
  collectCodexCliRolloutActivities,
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
  isRecentCodexCliRolloutActivity,
  mergeCodexCliRolloutMessages,
  parseCodexRolloutTerminalEvidence,
  parseLatestCodexRolloutTaskState,
  pruneCodexCliImportCache,
  reconcileCodexCliImportedMessages,
  resolveCodexCliImportBinding,
  resolveCodexCliExpectedUserMessageIds,
  resolveCodexCliProviderRuntimeExpectation,
  resolveCodexCliRecoveredCheckpointRequest,
  resolveObservedCodexCliSessionSyncAction,
  resolveCodexCliTranscriptMessages,
  readCodexCliImportedAt,
  resolveObservedCodexCliSessionState,
  resolveStaleCodexCliSession,
  selectUnsynchronizedCodexCliMessages,
  shouldInspectInterruptedCodexCliMirror,
  shouldInspectDetachedCodexCliObserver,
  shouldApplyObservedCodexCliSessionState,
  shouldBackoffCodexCliImportFailure,
  shouldHydrateObservedCodexCliTranscript,
  shouldImportCodexCliMessages,
  shouldInterruptStaleCodexCliSession,
  shouldPersistCodexCliImportMetadata,
  shouldProbeCodexCliRolloutOwner,
  shouldReadCodexRolloutActivities,
  shouldSkipCurrentCodexCliImport,
  shouldSkipUnchangedDetachedCodexCliObserver,
  shouldSynchronizeObservedCodexCliSessionBeforeHydration,
  shouldUseCodexRolloutTranscriptWithoutThreadRead,
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

  it("strips trailing internal memory citations from rollout assistant messages", () => {
    const contents = JSON.stringify({
      timestamp: "2026-08-03T00:00:00.000Z",
      type: "response_item",
      payload: {
        type: "message",
        id: "msg_final",
        role: "assistant",
        content: [
          {
            type: "output_text",
            text: [
              "The implementation is complete.",
              "",
              "<oai-mem-citation>",
              "<citation_entries>",
              "MEMORY.md:1-2|note=[internal]",
              "</citation_entries>",
              "<rollout_ids>",
              "</rollout_ids>",
              "</oai-mem-citation>",
            ].join("\n"),
          },
        ],
        internal_chat_message_metadata_passthrough: {
          turn_id: "turn-final",
        },
      },
    });

    expect(
      collectCodexCliRolloutMessages({
        threadId: "019legacy-thread",
        contents,
        createdAt: 1_784_215_777,
      }),
    ).toEqual([
      {
        messageId: MessageId.make("codex-cli:019legacy-thread:msg_final"),
        role: "assistant",
        text: "The implementation is complete.",
        turnId: TurnId.make("turn-final"),
        createdAt: "2026-08-03T00:00:00.000Z",
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
      JSON.stringify({
        timestamp: "2026-07-16T15:29:37.590Z",
        type: "response_item",
        payload: {
          type: "message",
          role: "user",
          content: [
            { type: "input_text", text: "<subagent_notification>hidden</subagent_notification>" },
          ],
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

  it("recovers the current turn's command lifecycle from rollout records", () => {
    const contents = [
      JSON.stringify({
        timestamp: "2026-08-03T00:00:00.000Z",
        type: "response_item",
        payload: {
          type: "function_call",
          id: "fc-command",
          call_id: "call-command",
          name: "exec_command",
          arguments: JSON.stringify({
            cmd: "rtk git status --short",
            workdir: "/tmp/project",
          }),
          internal_chat_message_metadata_passthrough: {
            turn_id: "turn-current",
          },
        },
      }),
      JSON.stringify({
        timestamp: "2026-08-03T00:00:00.100Z",
        type: "response_item",
        payload: {
          type: "function_call_output",
          id: "fco-command",
          call_id: "call-command",
          output: [
            "Chunk ID: command",
            "Wall time: 0.01 seconds",
            "Process exited with code 0",
            "Output:",
            " M importer.ts",
          ].join("\n"),
          internal_chat_message_metadata_passthrough: {
            turn_id: "turn-current",
          },
        },
      }),
      JSON.stringify({
        timestamp: "2026-08-03T00:00:00.200Z",
        type: "response_item",
        payload: {
          type: "function_call",
          id: "fc-other",
          call_id: "call-other",
          name: "exec_command",
          arguments: JSON.stringify({ cmd: "should not import" }),
          internal_chat_message_metadata_passthrough: {
            turn_id: "turn-other",
          },
        },
      }),
    ].join("\n");

    expect(
      collectCodexCliRolloutActivities({
        threadId: "019codex-thread",
        contents,
        createdAt: 1_700_000_000,
        turnId: "turn-current",
      }),
    ).toEqual([
      {
        id: "codex-cli:019codex-thread:tool:call-command:started",
        tone: "tool",
        kind: "tool.started",
        summary: "Ran command started",
        payload: {
          itemType: "command_execution",
          title: "Ran command",
          status: "inProgress",
          data: {
            toolCallId: "call-command",
            kind: "execute",
            item: {
              name: "Ran command",
              command: "rtk git status --short",
            },
            command: "rtk git status --short",
          },
        },
        turnId: TurnId.make("turn-current"),
        createdAt: "2026-08-03T00:00:00.000Z",
      },
      {
        id: "codex-cli:019codex-thread:tool:call-command:completed",
        tone: "tool",
        kind: "tool.completed",
        summary: "Ran command",
        payload: {
          itemType: "command_execution",
          title: "Ran command",
          status: "completed",
          detail: "M importer.ts",
          data: {
            toolCallId: "call-command",
            kind: "execute",
            item: {
              name: "Ran command",
              command: "rtk git status --short",
            },
            command: "rtk git status --short",
            rawOutput: {
              content: "M importer.ts",
            },
          },
        },
        turnId: TurnId.make("turn-current"),
        createdAt: "2026-08-03T00:00:00.100Z",
      },
    ]);
  });

  it("does not truncate a cold-start activity history at the import batch size", () => {
    const contents = Array.from({ length: 260 }, (_, index) => {
      const callId = `call-${index}`;
      const metadata = {
        turn_id: "turn-current",
      };
      return [
        JSON.stringify({
          timestamp: "2026-08-03T00:00:00.000Z",
          type: "response_item",
          payload: {
            type: "function_call",
            id: `fc-${index}`,
            call_id: callId,
            name: "exec_command",
            arguments: JSON.stringify({ cmd: `rtk echo ${index}` }),
            internal_chat_message_metadata_passthrough: metadata,
          },
        }),
        JSON.stringify({
          timestamp: "2026-08-03T00:00:00.100Z",
          type: "response_item",
          payload: {
            type: "function_call_output",
            id: `fco-${index}`,
            call_id: callId,
            output: `Process exited with code 0\nOutput:\n${index}`,
            internal_chat_message_metadata_passthrough: metadata,
          },
        }),
      ];
    })
      .flat()
      .join("\n");

    const activities = collectCodexCliRolloutActivities({
      threadId: "019codex-thread",
      contents,
      createdAt: 1_700_000_000,
      turnId: "turn-current",
    });

    expect(activities).toHaveLength(520);
    expect(activities[0]?.id).toBe("codex-cli:019codex-thread:tool:call-0:started");
    expect(activities.at(-1)?.id).toBe("codex-cli:019codex-thread:tool:call-259:completed");
  });

  it("retains active tool metadata across incremental rollout chunks", () => {
    const first = advanceCodexRolloutMessageCursor({
      cursor: undefined,
      contents: JSON.stringify({
        timestamp: "2026-08-03T00:00:00.000Z",
        type: "response_item",
        payload: {
          type: "function_call",
          id: "fc-command",
          call_id: "call-command",
          name: "exec_command",
          arguments: JSON.stringify({ cmd: "rtk git diff --check" }),
          internal_chat_message_metadata_passthrough: {
            turn_id: "turn-current",
          },
        },
      }),
      offset: 100,
      modifiedAtMillis: 1,
      threadId: "019codex-thread",
      createdAt: 1_700_000_000,
      activityTurnId: "turn-current",
    });
    const completed = advanceCodexRolloutMessageCursor({
      cursor: {
        ...first,
        messages: [],
        activities: [],
      },
      contents: JSON.stringify({
        timestamp: "2026-08-03T00:00:01.000Z",
        type: "response_item",
        payload: {
          type: "function_call_output",
          id: "fco-command",
          call_id: "call-command",
          output: "Process exited with code 1\nOutput:\ndiff failed",
          internal_chat_message_metadata_passthrough: {
            turn_id: "turn-current",
          },
        },
      }),
      offset: 200,
      modifiedAtMillis: 2,
      threadId: "019codex-thread",
      createdAt: 1_700_000_000,
      activityTurnId: "turn-current",
    });

    expect(first.activities).toHaveLength(1);
    expect(completed.activities).toHaveLength(1);
    expect(completed.activities[0]).toMatchObject({
      tone: "error",
      kind: "tool.completed",
      payload: {
        status: "failed",
        data: {
          toolCallId: "call-command",
          command: "rtk git diff --check",
        },
      },
    });
    expect(completed.toolCalls.size).toBe(0);
  });

  it("projects changed files from detached apply-patch activity", () => {
    const activities = collectCodexCliRolloutActivities({
      threadId: "019codex-thread",
      contents: [
        JSON.stringify({
          timestamp: "2026-08-03T00:00:00.000Z",
          type: "response_item",
          payload: {
            type: "custom_tool_call",
            id: "ctc-patch",
            call_id: "call-patch",
            name: "apply_patch",
            input: [
              "*** Begin Patch",
              "*** Update File: apps/server/src/importer.ts",
              "*** Add File: apps/server/src/importer.test.ts",
              "*** Move to: apps/server/src/importer-renamed.ts",
              "*** End Patch",
            ].join("\n"),
            internal_chat_message_metadata_passthrough: {
              turn_id: "turn-current",
            },
          },
        }),
        JSON.stringify({
          timestamp: "2026-08-03T00:00:00.100Z",
          type: "response_item",
          payload: {
            type: "custom_tool_call_output",
            id: "ctco-patch",
            call_id: "call-patch",
            output: [
              "Exit code: 0",
              "Output:",
              "Success. Updated the following files:",
              "M apps/server/src/importer.ts",
              "A apps/server/src/importer.test.ts",
            ].join("\n"),
            internal_chat_message_metadata_passthrough: {
              turn_id: "turn-current",
            },
          },
        }),
      ].join("\n"),
      createdAt: 1_700_000_000,
      turnId: "turn-current",
    });

    expect(activities.at(-1)).toMatchObject({
      kind: "tool.completed",
      summary: "Applied patch",
      payload: {
        itemType: "file_change",
        data: {
          toolCallId: "call-patch",
          files: [
            { path: "apps/server/src/importer.ts" },
            { path: "apps/server/src/importer.test.ts" },
            { path: "apps/server/src/importer-renamed.ts" },
          ],
          locations: [
            { path: "apps/server/src/importer.ts" },
            { path: "apps/server/src/importer.test.ts" },
            { path: "apps/server/src/importer-renamed.ts" },
          ],
        },
      },
    });
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

  it("merges a newer rollout final when app-server claims its older transcript is complete", () => {
    const thread = {
      ...makeThread(),
      turns: [
        {
          id: "turn-late-final",
          items: [
            {
              id: "message-user-late-final",
              type: "userMessage" as const,
              content: [{ type: "text" as const, text: "Check the implementation." }],
            },
            {
              id: "message-commentary",
              type: "agentMessage" as const,
              text: "I am checking the implementation now.",
              phase: "commentary" as const,
            },
          ],
          itemsView: "full" as const,
          startedAt: 1_700_000_001,
          status: "completed" as const,
        },
      ],
    };
    const finalMessage = {
      messageId: MessageId.make("codex-cli:019codex-thread:message-final"),
      role: "assistant" as const,
      text: "The implementation is complete.",
      turnId: TurnId.make("turn-late-final"),
      createdAt: "2026-08-03T00:00:00.000Z",
    };
    const resolved = resolveCodexCliTranscriptMessages({
      thread,
      rollout: {
        complete: true,
        messages: [
          finalMessage,
          {
            messageId: MessageId.make("codex-cli:019codex-thread:subagent-final"),
            role: "assistant",
            text: "Subagent handoff that is not part of the parent transcript.",
            turnId: TurnId.make("turn-subagent"),
            createdAt: "2026-08-03T00:00:00.001Z",
          },
        ],
      },
    });

    expect(resolved.complete).toBe(true);
    expect(resolved.messages).toEqual([...collectCodexCliImportedMessages(thread), finalMessage]);
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

  it("uses stable batch command ids and includes content, order, and runtime guards", () => {
    const messages = collectCodexCliImportedMessages(makeThread()).slice(0, 2);
    expect(messages).toHaveLength(2);
    const [firstMessage, secondMessage] = messages;
    if (firstMessage === undefined || secondMessage === undefined) {
      return;
    }

    const first = codexCliMessagesImportCommand({
      threadId: ThreadId.make("019codex-thread"),
      messages,
    });
    const repeated = codexCliMessagesImportCommand({
      threadId: ThreadId.make("019codex-thread"),
      messages,
    });
    const changed = codexCliMessagesImportCommand({
      threadId: ThreadId.make("019codex-thread"),
      messages: [
        {
          ...firstMessage,
          text: `${firstMessage.text}\nUpdated`,
        },
        secondMessage,
      ],
    });
    const reordered = codexCliMessagesImportCommand({
      threadId: ThreadId.make("019codex-thread"),
      messages: [secondMessage, firstMessage],
    });

    expect(repeated.commandId).toBe(first.commandId);
    expect(changed.commandId).not.toBe(first.commandId);
    expect(reordered.commandId).not.toBe(first.commandId);
    expect(first.messages).toEqual(messages);

    const expectedProviderRuntime = {
      providerName: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "running" as const,
      lastSeenAt: "2026-08-03T00:00:00.000Z",
      resumeCursor: { threadId: "019codex-thread" },
      requiresDetachedIdle: true,
    };
    const guarded = codexCliMessagesImportCommand({
      threadId: ThreadId.make("019codex-thread"),
      messages,
      expectedProviderRuntime,
    });
    expect(guarded.expectedProviderRuntime).toEqual(expectedProviderRuntime);
    expect(guarded.commandId).not.toBe(first.commandId);
  });

  it("does not re-dispatch a large synchronized transcript", () => {
    const imported = Array.from({ length: 700 }, (_, index) => ({
      messageId: MessageId.make(`codex-cli:019codex-thread:message-${index}`),
      role: index % 2 === 0 ? ("user" as const) : ("assistant" as const),
      text: `Message ${index}`,
      turnId: TurnId.make(`turn-${Math.floor(index / 2)}`),
      createdAt: `2026-08-03T00:00:00.${String(index).padStart(3, "0")}Z`,
    }));
    const projected = {
      messages: imported.map((message) => ({
        id: message.messageId,
        role: message.role,
        text: `\n${message.text}\r\n`,
        turnId: message.turnId,
        streaming: false,
        createdAt: message.createdAt,
        updatedAt: message.createdAt,
      })),
    } satisfies Pick<OrchestrationThread, "messages">;

    expect(selectUnsynchronizedCodexCliMessages(projected, imported)).toEqual([]);
    expect(hasSynchronizedCodexCliTranscript(projected, imported)).toBe(true);
  });

  it("selects only newly added or extended transcript messages", () => {
    const imported = Array.from({ length: 700 }, (_, index) => ({
      messageId: MessageId.make(`codex-cli:019codex-thread:message-${index}`),
      role: index % 2 === 0 ? ("user" as const) : ("assistant" as const),
      text: `Message ${index}`,
      turnId: TurnId.make(`turn-${Math.floor(index / 2)}`),
      createdAt: `2026-08-03T00:00:00.${String(index).padStart(3, "0")}Z`,
    }));
    const projected = {
      messages: imported.map((message) => ({
        id: message.messageId,
        role: message.role,
        text: message.text,
        turnId: message.turnId,
        streaming: false,
        createdAt: message.createdAt,
        updatedAt: message.createdAt,
      })),
    } satisfies Pick<OrchestrationThread, "messages">;
    const added = {
      messageId: MessageId.make("codex-cli:019codex-thread:message-700"),
      role: "user" as const,
      text: "New prompt",
      turnId: TurnId.make("turn-350"),
      createdAt: "2026-08-03T00:12:00.000Z",
    };
    const extended = {
      ...imported[699]!,
      text: `${imported[699]!.text} with the completed response`,
    };
    const reassigned = {
      ...imported[698]!,
      turnId: TurnId.make("turn-reassigned"),
    };

    expect(selectUnsynchronizedCodexCliMessages(projected, [...imported, added])).toEqual([added]);
    expect(
      selectUnsynchronizedCodexCliMessages(projected, [...imported.slice(0, -1), extended]),
    ).toEqual([extended]);
    expect(
      selectUnsynchronizedCodexCliMessages(projected, [
        ...imported.slice(0, -2),
        reassigned,
        imported[699]!,
      ]),
    ).toEqual([reassigned]);
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

  it("reuses app-server message ids when rollout timestamps differ within the same turn", () => {
    const imported = {
      messageId: MessageId.make("codex-cli:019codex-thread:msg-rollout"),
      role: "assistant" as const,
      text: "Completed after a long-running tool sequence.",
      turnId: TurnId.make("turn-long"),
      createdAt: "2026-08-03T02:00:00.000Z",
    };
    const projectedThread = {
      latestTurn: null,
      messages: [
        {
          id: MessageId.make("codex-cli:019codex-thread:item-200"),
          role: imported.role,
          text: imported.text,
          turnId: imported.turnId,
          streaming: false,
          createdAt: "2026-08-03T00:00:00.000Z",
          updatedAt: "2026-08-03T02:00:00.000Z",
        },
      ],
    } satisfies Parameters<typeof reconcileCodexCliImportedMessages>[1];

    expect(reconcileCodexCliImportedMessages([imported], projectedThread)).toEqual([
      {
        ...imported,
        messageId: MessageId.make("codex-cli:019codex-thread:item-200"),
        createdAt: "2026-08-03T00:00:00.000Z",
      },
    ]);
  });

  it("reuses projected runtime ids for the same native Codex message", () => {
    const imported = {
      messageId: MessageId.make("codex-cli:019codex-thread:msg_shared"),
      role: "assistant" as const,
      text: "The implementation is complete.",
      turnId: TurnId.make("turn-final"),
      createdAt: "2026-08-03T02:00:00.000Z",
    };
    const projectedThread = {
      latestTurn: null,
      messages: [
        {
          id: MessageId.make("assistant:msg_shared"),
          role: imported.role,
          text: imported.text,
          turnId: imported.turnId,
          streaming: false,
          createdAt: "2026-08-03T01:59:59.000Z",
          updatedAt: "2026-08-03T02:00:00.000Z",
        },
      ],
    } satisfies Parameters<typeof reconcileCodexCliImportedMessages>[1];

    expect(reconcileCodexCliImportedMessages([imported], projectedThread)).toEqual([
      {
        ...imported,
        messageId: MessageId.make("assistant:msg_shared"),
        createdAt: "2026-08-03T01:59:59.000Z",
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

  it("repairs a stale retrying observer before transcript hydration", () => {
    const threadId = ThreadId.make("t3-owned-thread");
    const activeTurnId = TurnId.make("turn-active");
    const preparedThread = {
      latestUserMessageAt: null,
      session: {
        threadId,
        status: "starting" as const,
        providerName: "codex",
        providerInstanceId: ProviderInstanceId.make("codex"),
        runtimeMode: "full-access" as const,
        activeTurnId: null,
        lastError:
          "T3 could not reattach to the detached provider execution yet. The provider was not interrupted; T3 will keep retrying.",
        retrying: true,
        updatedAt: "2026-08-03T12:00:00.000Z",
      },
      updatedAt: "2026-08-03T12:00:00.000Z",
    };
    const observedState = {
      status: "running" as const,
      activeTurnId,
      lastError: null,
    };

    expect(shouldSynchronizeObservedCodexCliSessionBeforeHydration(observedState)).toBe(true);
    expect(
      resolveObservedCodexCliSessionSyncAction({
        currentThread: preparedThread,
        preparedThread,
        observedState,
      }),
    ).toBe("apply");
  });

  it("treats an already repaired observer as synchronized after hydration", () => {
    const threadId = ThreadId.make("t3-owned-thread");
    const activeTurnId = TurnId.make("turn-active");
    const preparedThread = {
      latestUserMessageAt: null,
      session: {
        threadId,
        status: "starting" as const,
        providerName: "codex",
        providerInstanceId: ProviderInstanceId.make("codex"),
        runtimeMode: "full-access" as const,
        activeTurnId: null,
        lastError: "Retrying.",
        retrying: true,
        updatedAt: "2026-08-03T12:00:00.000Z",
      },
      updatedAt: "2026-08-03T12:00:00.000Z",
    };
    const observedState = {
      status: "running" as const,
      activeTurnId,
      lastError: null,
    };
    const currentThread = {
      latestUserMessageAt: null,
      session: {
        ...preparedThread.session,
        ...observedState,
        retrying: false,
        updatedAt: "2026-08-03T12:00:01.000Z",
      },
      updatedAt: "2026-08-03T12:00:01.000Z",
    };

    expect(
      resolveObservedCodexCliSessionSyncAction({
        currentThread,
        preparedThread,
        observedState,
      }),
    ).toBe("synchronized");
    expect(
      shouldSynchronizeObservedCodexCliSessionBeforeHydration({
        status: "interrupted",
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

  it("requests a recovered checkpoint only for a completed detached T3 turn", () => {
    const threadId = ThreadId.make("t3-owned-thread");
    const activeTurnId = TurnId.make("turn-recovered");
    const assistantMessageId = MessageId.make("assistant-recovered");
    const checkpointContext = {
      threadId,
      projectId: "project-1" as never,
      workspaceRoot: "/tmp/project",
      worktreePath: null,
      checkpoints: [
        {
          turnId: TurnId.make("turn-before"),
          checkpointTurnCount: 3,
          checkpointRef: CheckpointRef.make("checkpoint-before"),
          status: "ready" as const,
          files: [],
          assistantMessageId: MessageId.make("assistant-before"),
          completedAt: "2026-08-03T00:00:00.000Z",
        },
      ],
    };
    const messages = [
      {
        messageId: MessageId.make("assistant-commentary"),
        role: "assistant" as const,
        text: "Still working.",
        turnId: activeTurnId,
        createdAt: "2026-08-03T00:00:01.000Z",
      },
      {
        messageId: assistantMessageId,
        role: "assistant" as const,
        text: "Done.",
        turnId: activeTurnId,
        createdAt: "2026-08-03T00:00:02.000Z",
      },
    ];

    expect(
      resolveCodexCliRecoveredCheckpointRequest({
        threadId,
        activeTurnId,
        resolutionStatus: "ready",
        messages,
        checkpointContext,
        completedAt: "2026-08-03T00:00:03.000Z",
      }),
    ).toEqual({
      turnId: activeTurnId,
      assistantMessageId,
      completedAt: "2026-08-03T00:00:03.000Z",
      checkpointTurnCount: 4,
      checkpointRef: CheckpointRef.make("codex-cli-recovery:t3-owned-thread:turn-recovered:4"),
    });
    expect(
      resolveCodexCliRecoveredCheckpointRequest({
        threadId,
        activeTurnId,
        resolutionStatus: "error",
        messages,
        checkpointContext,
        completedAt: "2026-08-03T00:00:03.000Z",
      }),
    ).toBeUndefined();
    expect(
      resolveCodexCliRecoveredCheckpointRequest({
        threadId,
        activeTurnId,
        resolutionStatus: "ready",
        messages,
        checkpointContext: {
          ...checkpointContext,
          checkpoints: [
            ...checkpointContext.checkpoints,
            {
              ...checkpointContext.checkpoints[0]!,
              turnId: activeTurnId,
              checkpointTurnCount: 4,
            },
          ],
        },
        completedAt: "2026-08-03T00:00:03.000Z",
      }),
    ).toBeUndefined();
  });

  it("prunes rollout and import caches to the current discovery window", () => {
    const cache = new Map([
      ["active", 1],
      ["expired", 2],
    ]);

    pruneCodexCliImportCache(cache, new Set(["active"]));

    expect([...cache.entries()]).toEqual([["active", 1]]);
  });

  it("backs off unchanged import failures and retries immediately after provider progress", () => {
    const first = advanceCodexCliImportFailureBackoff({
      previous: undefined,
      providerUpdatedAt: 100,
      failedAtMillis: 1_000,
    });
    expect(first).toEqual({
      providerUpdatedAt: 100,
      failureCount: 1,
      retryAfterMillis: 61_000,
    });
    expect(
      shouldBackoffCodexCliImportFailure({
        failure: first,
        providerUpdatedAt: 100,
        nowMillis: 60_999,
      }),
    ).toBe(true);
    expect(
      shouldBackoffCodexCliImportFailure({
        failure: first,
        providerUpdatedAt: 101,
        nowMillis: 2_000,
      }),
    ).toBe(false);

    const second = advanceCodexCliImportFailureBackoff({
      previous: first,
      providerUpdatedAt: 100,
      failedAtMillis: 61_000,
    });
    expect(second).toEqual({
      providerUpdatedAt: 100,
      failureCount: 2,
      retryAfterMillis: 181_000,
    });
    expect(
      shouldBackoffCodexCliImportFailure({
        failure: second,
        providerUpdatedAt: 100,
        nowMillis: 181_000,
      }),
    ).toBe(false);
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

  it("reads rollout activities only for active observers or recent CLI-owned updates", () => {
    const importedBinding = {
      threadId: ThreadId.make("019codex-thread"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "stopped" as const,
      runtimeMode: "full-access" as const,
      resumeCursor: { threadId: "019codex-thread" },
      runtimePayload: {
        importedFrom: "codex-cli",
      },
    };
    const t3Binding = {
      ...importedBinding,
      runtimePayload: {},
    };

    expect(
      shouldReadCodexRolloutActivities({
        rolloutPathAvailable: true,
        activityTurnId: "turn-current",
        listedThreadIsRecent: true,
        importIsCurrent: false,
        binding: undefined,
        observesDetachedCliSession: false,
      }),
    ).toBe(true);
    expect(
      shouldReadCodexRolloutActivities({
        rolloutPathAvailable: true,
        activityTurnId: "turn-current",
        listedThreadIsRecent: true,
        importIsCurrent: false,
        binding: importedBinding,
        observesDetachedCliSession: false,
      }),
    ).toBe(true);
    expect(
      shouldReadCodexRolloutActivities({
        rolloutPathAvailable: true,
        activityTurnId: "turn-current",
        listedThreadIsRecent: false,
        importIsCurrent: false,
        binding: importedBinding,
        observesDetachedCliSession: false,
      }),
    ).toBe(false);
    expect(
      shouldReadCodexRolloutActivities({
        rolloutPathAvailable: true,
        activityTurnId: "turn-current",
        listedThreadIsRecent: true,
        importIsCurrent: false,
        binding: t3Binding,
        observesDetachedCliSession: false,
      }),
    ).toBe(false);
    expect(
      shouldReadCodexRolloutActivities({
        rolloutPathAvailable: true,
        activityTurnId: "turn-current",
        listedThreadIsRecent: false,
        importIsCurrent: true,
        binding: t3Binding,
        observesDetachedCliSession: true,
      }),
    ).toBe(true);
    expect(
      shouldReadCodexRolloutActivities({
        rolloutPathAvailable: false,
        activityTurnId: "turn-current",
        listedThreadIsRecent: true,
        importIsCurrent: false,
        binding: undefined,
        observesDetachedCliSession: true,
      }),
    ).toBe(false);
  });

  it("hydrates detached observers only when transcript messages or terminal state changed", () => {
    expect(
      shouldHydrateObservedCodexCliTranscript({
        observesDetachedCliSession: true,
        importIsCurrent: true,
        rolloutTranscriptInspected: true,
        rolloutTranscriptChanged: false,
        rolloutTranscriptComplete: true,
        rolloutTranscriptSynchronized: true,
        terminalTransitionObserved: false,
      }),
    ).toBe(false);
    expect(
      shouldHydrateObservedCodexCliTranscript({
        observesDetachedCliSession: true,
        importIsCurrent: true,
        rolloutTranscriptInspected: true,
        rolloutTranscriptChanged: true,
        rolloutTranscriptComplete: true,
        rolloutTranscriptSynchronized: false,
        terminalTransitionObserved: false,
      }),
    ).toBe(true);
    expect(
      shouldHydrateObservedCodexCliTranscript({
        observesDetachedCliSession: true,
        importIsCurrent: true,
        rolloutTranscriptInspected: true,
        rolloutTranscriptChanged: false,
        rolloutTranscriptComplete: true,
        rolloutTranscriptSynchronized: true,
        terminalTransitionObserved: true,
      }),
    ).toBe(true);
    expect(
      shouldHydrateObservedCodexCliTranscript({
        observesDetachedCliSession: true,
        importIsCurrent: false,
        rolloutTranscriptInspected: false,
        rolloutTranscriptChanged: false,
        rolloutTranscriptComplete: false,
        rolloutTranscriptSynchronized: false,
        terminalTransitionObserved: false,
      }),
    ).toBe(true);
    expect(
      shouldHydrateObservedCodexCliTranscript({
        observesDetachedCliSession: true,
        importIsCurrent: false,
        rolloutTranscriptInspected: true,
        rolloutTranscriptChanged: false,
        rolloutTranscriptComplete: true,
        rolloutTranscriptSynchronized: true,
        terminalTransitionObserved: false,
      }),
    ).toBe(false);
    expect(
      shouldHydrateObservedCodexCliTranscript({
        observesDetachedCliSession: true,
        importIsCurrent: true,
        rolloutTranscriptInspected: true,
        rolloutTranscriptChanged: true,
        rolloutTranscriptComplete: false,
        rolloutTranscriptSynchronized: true,
        terminalTransitionObserved: false,
      }),
    ).toBe(true);
    expect(
      shouldHydrateObservedCodexCliTranscript({
        observesDetachedCliSession: false,
        importIsCurrent: false,
        rolloutTranscriptInspected: true,
        rolloutTranscriptChanged: true,
        rolloutTranscriptComplete: false,
        rolloutTranscriptSynchronized: false,
        terminalTransitionObserved: true,
      }),
    ).toBe(false);
    expect(
      shouldHydrateObservedCodexCliTranscript({
        observesDetachedCliSession: true,
        importIsCurrent: true,
        rolloutTranscriptInspected: true,
        rolloutTranscriptChanged: false,
        rolloutTranscriptComplete: false,
        rolloutTranscriptSynchronized: false,
        terminalTransitionObserved: false,
      }),
    ).toBe(false);
  });

  it("uses complete rollout transcripts directly only for active detached observers", () => {
    expect(
      shouldUseCodexRolloutTranscriptWithoutThreadRead({
        observesDetachedCliSession: true,
        observedSessionStatus: "running",
        rolloutTranscriptComplete: true,
        terminalTransitionObserved: false,
      }),
    ).toBe(true);
    expect(
      shouldUseCodexRolloutTranscriptWithoutThreadRead({
        observesDetachedCliSession: true,
        observedSessionStatus: "ready",
        rolloutTranscriptComplete: true,
        terminalTransitionObserved: false,
      }),
    ).toBe(false);
    expect(
      shouldUseCodexRolloutTranscriptWithoutThreadRead({
        observesDetachedCliSession: true,
        observedSessionStatus: "running",
        rolloutTranscriptComplete: false,
        terminalTransitionObserved: false,
      }),
    ).toBe(false);
    expect(
      shouldUseCodexRolloutTranscriptWithoutThreadRead({
        observesDetachedCliSession: true,
        observedSessionStatus: "running",
        rolloutTranscriptComplete: true,
        terminalTransitionObserved: true,
      }),
    ).toBe(false);
    expect(
      shouldUseCodexRolloutTranscriptWithoutThreadRead({
        observesDetachedCliSession: false,
        observedSessionStatus: "running",
        rolloutTranscriptComplete: true,
        terminalTransitionObserved: false,
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

    const responseOnly = JSON.stringify({
      type: "response_item",
      payload: {
        type: "function_call",
        internal_chat_message_metadata_passthrough: { turn_id: "turn-tail" },
      },
    });
    expect(parseLatestCodexRolloutTaskState(responseOnly)).toEqual({
      state: null,
      turnId: "turn-tail",
    });

    const currentTurnAfterOlderCompletion = [
      JSON.stringify({
        type: "event_msg",
        payload: { type: "task_complete", turn_id: "turn-old" },
      }),
      responseOnly,
    ].join("\n");
    expect(parseLatestCodexRolloutTaskState(currentTurnAfterOlderCompletion)).toEqual({
      state: null,
      turnId: "turn-tail",
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

    const currentTurnFromTail = advanceCodexRolloutTaskCursor(
      completed,
      `${JSON.stringify({
        type: "response_item",
        payload: {
          type: "function_call",
          internal_chat_message_metadata_passthrough: { turn_id: "turn-tail" },
        },
      })}\n`,
      started.length + 400,
    );
    expect(currentTurnFromTail.lifecycle).toEqual({
      state: null,
      turnId: "turn-tail",
    });

    const nextTurn = advanceCodexRolloutTaskCursor(
      currentTurnFromTail,
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
