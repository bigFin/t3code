import {
  MessageId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import type * as CodexSchema from "effect-codex-app-server/schema";

import {
  CODEX_INTERACTIVE_SOURCE_KINDS,
  codexCliMessageImportCommand,
  collectCodexCliImportedMessages,
  isCurrentCodexCliImport,
  isImportableCodexInteractiveThread,
  isLiveCodexBinding,
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

  it("recognizes an unchanged CLI thread from its persisted upstream timestamp", () => {
    const binding = {
      threadId: ThreadId.make("019codex-thread"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      status: "stopped" as const,
      runtimeMode: "full-access" as const,
      runtimePayload: {
        importedFrom: "codex-cli",
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
    expect(isCurrentCodexCliImport(undefined, makeThread())).toBe(false);
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
