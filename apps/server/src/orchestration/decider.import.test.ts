import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type OrchestrationReadModel,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { decideOrchestrationCommand, isOrchestrationCommandNoop } from "./decider.ts";

const NOW = "2026-07-23T12:00:00.000Z";

const readModel: OrchestrationReadModel = {
  snapshotSequence: 2,
  projects: [
    {
      id: ProjectId.make("project-1"),
      title: "Project",
      workspaceRoot: "/tmp/project",
      defaultModelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5.6-sol",
      },
      scripts: [],
      createdAt: NOW,
      updatedAt: NOW,
      deletedAt: null,
    },
  ],
  threads: [
    {
      id: ThreadId.make("thread-1"),
      projectId: ProjectId.make("project-1"),
      title: "Imported thread",
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5.6-sol",
      },
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
      messages: [],
      proposedPlans: [],
      activities: [],
      checkpoints: [],
      deletedAt: null,
    },
  ],
  updatedAt: NOW,
};

it.layer(NodeServices.layer)("thread.message.import decider", (it) => {
  it.effect("emits a completed historical message without requesting a provider turn", () =>
    Effect.gen(function* () {
      const event = yield* decideOrchestrationCommand({
        command: {
          type: "thread.message.import",
          commandId: CommandId.make("command-import-1"),
          threadId: ThreadId.make("thread-1"),
          messageId: MessageId.make("message-1"),
          role: "assistant",
          text: "Historical response",
          turnId: TurnId.make("turn-1"),
          createdAt: NOW,
        },
        readModel,
      });
      const events = Array.isArray(event) ? event : [event];

      expect(events).toHaveLength(1);
      expect(events[0]?.type).toBe("thread.message-sent");
      if (events[0]?.type === "thread.message-sent") {
        expect(events[0].payload).toMatchObject({
          threadId: ThreadId.make("thread-1"),
          messageId: MessageId.make("message-1"),
          role: "assistant",
          text: "Historical response",
          turnId: TurnId.make("turn-1"),
          streaming: false,
          createdAt: NOW,
          updatedAt: NOW,
        });
      }
    }),
  );

  it.effect("emits an ordered event for every message in a historical batch", () =>
    Effect.gen(function* () {
      const result = yield* decideOrchestrationCommand({
        command: {
          type: "thread.messages.import",
          commandId: CommandId.make("command-import-batch"),
          threadId: ThreadId.make("thread-1"),
          messages: [
            {
              messageId: MessageId.make("message-1"),
              role: "user",
              text: "Historical prompt",
              turnId: TurnId.make("turn-1"),
              createdAt: "2026-07-23T12:00:01.000Z",
            },
            {
              messageId: MessageId.make("message-2"),
              role: "assistant",
              text: "Historical response",
              turnId: TurnId.make("turn-1"),
              createdAt: "2026-07-23T12:00:02.000Z",
            },
          ],
          createdAt: "2026-07-23T12:00:02.000Z",
        },
        readModel,
      });
      const events = Array.isArray(result) ? result : [result];

      expect(events).toHaveLength(2);
      expect(
        events.map((event) =>
          event.type === "thread.message-sent"
            ? {
                messageId: event.payload.messageId,
                role: event.payload.role,
                text: event.payload.text,
                occurredAt: event.occurredAt,
              }
            : event.type,
        ),
      ).toEqual([
        {
          messageId: MessageId.make("message-1"),
          role: "user",
          text: "Historical prompt",
          occurredAt: "2026-07-23T12:00:01.000Z",
        },
        {
          messageId: MessageId.make("message-2"),
          role: "assistant",
          text: "Historical response",
          occurredAt: "2026-07-23T12:00:02.000Z",
        },
      ]);
    }),
  );

  it.effect("treats an empty historical message batch as a no-op", () =>
    Effect.gen(function* () {
      const result = yield* decideOrchestrationCommand({
        command: {
          type: "thread.messages.import",
          commandId: CommandId.make("command-import-empty-batch"),
          threadId: ThreadId.make("thread-1"),
          messages: [],
          createdAt: NOW,
        },
        readModel,
      });

      expect(isOrchestrationCommandNoop(result)).toBe(true);
    }),
  );
});
