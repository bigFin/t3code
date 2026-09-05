import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type OrchestrationReadModel,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";

import { decideOrchestrationCommand, isOrchestrationCommandNoop } from "./decider.ts";
import { createEmptyReadModel, projectEvent } from "./projector.ts";

it.layer(NodeServices.layer)("thread history import", (it) => {
  it.effect("marks imported thread creation without changing live creation", () =>
    Effect.gen(function* () {
      const createdAt = "2026-08-24T10:00:00.000Z";
      const projectId = ProjectId.make("project-1");
      const readModel = yield* projectEvent(createEmptyReadModel(createdAt), {
        sequence: 1,
        eventId: EventId.make("event-project-created"),
        aggregateKind: "project",
        aggregateId: projectId,
        type: "project.created",
        occurredAt: createdAt,
        commandId: CommandId.make("command-project-created"),
        causationEventId: null,
        correlationId: CommandId.make("command-project-created"),
        metadata: {},
        payload: {
          projectId,
          title: "Project",
          workspaceRoot: "/tmp/project",
          defaultModelSelection: null,
          scripts: [],
          createdAt,
          updatedAt: createdAt,
        },
      });
      const makeCreateCommand = (threadId: ThreadId) => ({
        type: "thread.create" as const,
        commandId: CommandId.make(`command-create-${threadId}`),
        threadId,
        projectId,
        title: "Imported thread",
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
        runtimeMode: "full-access" as const,
        interactionMode: "default" as const,
        branch: null,
        worktreePath: null,
        createdAt,
      });

      const imported = yield* decideOrchestrationCommand({
        command: {
          ...makeCreateCommand(ThreadId.make("import:codex:session-1")),
          historyImport: true,
        },
        readModel,
      });
      const live = yield* decideOrchestrationCommand({
        command: makeCreateCommand(ThreadId.make("live-thread")),
        readModel,
      });

      expect(imported).toMatchObject({
        type: "thread.created",
        metadata: { historyImport: true },
      });
      expect(live).toMatchObject({ type: "thread.created" });
      expect(live).not.toMatchObject({ metadata: { historyImport: true } });
    }),
  );

  it.effect("settles imported messages at the latest absolute timestamp", () =>
    Effect.gen(function* () {
      const createdAt = "2026-08-24T10:30:00.000+02:00";
      const threadId = ThreadId.make("import:codex:session-1");
      const readModel = yield* projectEvent(createEmptyReadModel(createdAt), {
        sequence: 1,
        eventId: EventId.make("event-thread-created"),
        aggregateKind: "thread",
        aggregateId: threadId,
        type: "thread.created",
        occurredAt: createdAt,
        commandId: CommandId.make("command-thread-created"),
        causationEventId: null,
        correlationId: CommandId.make("command-thread-created"),
        metadata: {},
        payload: {
          threadId,
          projectId: ProjectId.make("project-1"),
          title: "Imported thread",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt,
          updatedAt: createdAt,
        },
      });

      const events = yield* decideOrchestrationCommand({
        command: {
          type: "thread.history.import",
          commandId: CommandId.make("command-import-history"),
          threadId,
          messages: [
            {
              messageId: MessageId.make(`${threadId}:000000`),
              role: "user",
              text: "Fix the bug",
              createdAt,
            },
            {
              messageId: MessageId.make(`${threadId}:000001`),
              role: "assistant",
              text: "Fixed",
              createdAt: "2026-08-24T09:00:00.000Z",
            },
          ],
        },
        readModel,
      });

      expect(events).toMatchObject([
        {
          type: "thread.message-sent",
          metadata: { historyImport: true },
          payload: { role: "user", text: "Fix the bug", turnId: null, streaming: false },
        },
        {
          type: "thread.message-sent",
          metadata: { historyImport: true },
          payload: { role: "assistant", text: "Fixed", turnId: null, streaming: false },
        },
        {
          type: "thread.settled",
          metadata: { historyImport: true },
          occurredAt: "2026-08-24T09:00:00.000Z",
          payload: {
            settledAt: "2026-08-24T09:00:00.000Z",
            updatedAt: "2026-08-24T09:00:00.000Z",
          },
        },
      ]);

      let projected = readModel;
      const plannedEvents = Array.isArray(events) ? events : [events];
      for (const [index, event] of plannedEvents.entries()) {
        projected = yield* projectEvent(projected, { ...event, sequence: index + 2 });
      }
      projected = yield* projectEvent(projected, {
        sequence: 5,
        eventId: EventId.make("event-import-reverted"),
        aggregateKind: "thread",
        aggregateId: threadId,
        type: "thread.reverted",
        occurredAt: "2026-08-24T10:02:00.000Z",
        commandId: CommandId.make("command-import-reverted"),
        causationEventId: null,
        correlationId: CommandId.make("command-import-reverted"),
        metadata: {},
        payload: { threadId, turnCount: 0 },
      });
      expect(projected.threads[0]?.messages.map((message) => message.text)).toEqual([
        "Fix the bug",
        "Fixed",
      ]);
    }),
  );

  it.effect("allows a thread with a newly imported user message to be settled", () =>
    Effect.gen(function* () {
      const createdAt = "2026-08-24T10:00:00.000Z";
      yield* TestClock.setTime(Date.parse("2026-08-24T10:00:30.000Z"));
      const threadId = ThreadId.make("import:codex:session-1");
      const withThread = yield* projectEvent(createEmptyReadModel(createdAt), {
        sequence: 1,
        eventId: EventId.make("event-import-thread-created"),
        aggregateKind: "thread",
        aggregateId: threadId,
        type: "thread.created",
        occurredAt: createdAt,
        commandId: CommandId.make("command-import-thread-created"),
        causationEventId: null,
        correlationId: CommandId.make("command-import-thread-created"),
        metadata: {},
        payload: {
          threadId,
          projectId: ProjectId.make("project-1"),
          title: "Imported thread",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt,
          updatedAt: createdAt,
        },
      });
      const readModel = yield* projectEvent(withThread, {
        sequence: 2,
        eventId: EventId.make("event-import-user-message"),
        aggregateKind: "thread",
        aggregateId: threadId,
        type: "thread.message-sent",
        occurredAt: createdAt,
        commandId: CommandId.make("command-import-user-message"),
        causationEventId: null,
        correlationId: CommandId.make("command-import-user-message"),
        metadata: { historyImport: true },
        payload: {
          threadId,
          messageId: MessageId.make("import:codex:session-1:0"),
          role: "user",
          text: "Existing prompt",
          turnId: null,
          streaming: false,
          createdAt,
          updatedAt: createdAt,
        },
      });

      const result = yield* decideOrchestrationCommand({
        command: {
          type: "thread.settle",
          commandId: CommandId.make("command-settle-imported-thread"),
          threadId,
        },
        readModel,
      });

      expect(result).toMatchObject({ type: "thread.settled" });
    }),
  );

  it.effect("rejects history import after a client message reaches the thread", () =>
    Effect.gen(function* () {
      const createdAt = "2026-08-24T10:00:00.000Z";
      const liveMessageAt = "2026-08-24T10:02:00.000Z";
      const threadId = ThreadId.make("import:codex:client-race");
      const withThread = yield* projectEvent(createEmptyReadModel(createdAt), {
        sequence: 1,
        eventId: EventId.make("event-client-race-thread-created"),
        aggregateKind: "thread",
        aggregateId: threadId,
        type: "thread.created",
        occurredAt: createdAt,
        commandId: CommandId.make("command-client-race-thread-created"),
        causationEventId: null,
        correlationId: CommandId.make("command-client-race-thread-created"),
        metadata: {},
        payload: {
          threadId,
          projectId: ProjectId.make("project-1"),
          title: "Imported thread",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt,
          updatedAt: createdAt,
        },
      });
      const readModel = yield* projectEvent(withThread, {
        sequence: 2,
        eventId: EventId.make("event-client-race-message"),
        aggregateKind: "thread",
        aggregateId: threadId,
        type: "thread.message-sent",
        occurredAt: liveMessageAt,
        commandId: CommandId.make("command-client-race-message"),
        causationEventId: null,
        correlationId: CommandId.make("command-client-race-message"),
        metadata: {},
        payload: {
          threadId,
          messageId: MessageId.make("client-race-message"),
          role: "user",
          text: "Start live work",
          turnId: null,
          streaming: false,
          createdAt: liveMessageAt,
          updatedAt: liveMessageAt,
        },
      });

      const error = yield* Effect.flip(
        decideOrchestrationCommand({
          command: {
            type: "thread.history.import",
            commandId: CommandId.make("command-client-race-import"),
            threadId,
            messages: [
              {
                messageId: MessageId.make(`${threadId}:000000`),
                role: "user",
                text: "Old work",
                createdAt,
              },
            ],
          },
          readModel,
        }),
      );

      expect(error._tag).toBe("OrchestrationCommandInvariantError");
      expect(error.message).toContain("must be active and empty");
      expect(readModel.threads[0]?.updatedAt).toBe(liveMessageAt);
    }),
  );

  for (const requestKind of ["approval.requested", "user-input.requested"] as const) {
    it.effect(`rejects history import with an open ${requestKind} activity`, () =>
      Effect.gen(function* () {
        const createdAt = "2026-08-24T10:00:00.000Z";
        const threadId = ThreadId.make(`import:codex:${requestKind}`);
        const withThread = yield* projectEvent(createEmptyReadModel(createdAt), {
          sequence: 1,
          eventId: EventId.make(`event-${requestKind}-thread-created`),
          aggregateKind: "thread",
          aggregateId: threadId,
          type: "thread.created",
          occurredAt: createdAt,
          commandId: CommandId.make(`command-${requestKind}-thread-created`),
          causationEventId: null,
          correlationId: CommandId.make(`command-${requestKind}-thread-created`),
          metadata: {},
          payload: {
            threadId,
            projectId: ProjectId.make("project-1"),
            title: "Imported thread",
            modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdAt,
            updatedAt: createdAt,
          },
        });
        const readModel = yield* projectEvent(withThread, {
          sequence: 2,
          eventId: EventId.make(`event-${requestKind}`),
          aggregateKind: "thread",
          aggregateId: threadId,
          type: "thread.activity-appended",
          occurredAt: createdAt,
          commandId: CommandId.make(`command-${requestKind}`),
          causationEventId: null,
          correlationId: CommandId.make(`command-${requestKind}`),
          metadata: {},
          payload: {
            threadId,
            activity: {
              id: EventId.make(`activity-${requestKind}`),
              tone: "approval",
              kind: requestKind,
              summary: "Pending request",
              payload: { requestId: "request-1" },
              turnId: null,
              createdAt,
            },
          },
        });

        const error = yield* Effect.flip(
          decideOrchestrationCommand({
            command: {
              type: "thread.history.import",
              commandId: CommandId.make(`command-import-${requestKind}`),
              threadId,
              messages: [
                {
                  messageId: MessageId.make(`${threadId}:000000`),
                  role: "user",
                  text: "Old work",
                  createdAt,
                },
              ],
            },
            readModel,
          }),
        );

        expect(error._tag).toBe("OrchestrationCommandInvariantError");
        expect(error.message).toContain("must be active and empty");
      }),
    );
  }

  it.effect("rejects a live user message in the imported-session namespace", () =>
    Effect.gen(function* () {
      const createdAt = "2026-08-24T10:00:00.000Z";
      const threadId = ThreadId.make("thread-live-message");
      const readModel = yield* projectEvent(createEmptyReadModel(createdAt), {
        sequence: 1,
        eventId: EventId.make("event-live-thread-created"),
        aggregateKind: "thread",
        aggregateId: threadId,
        type: "thread.created",
        occurredAt: createdAt,
        commandId: CommandId.make("command-live-thread-created"),
        causationEventId: null,
        correlationId: CommandId.make("command-live-thread-created"),
        metadata: {},
        payload: {
          threadId,
          projectId: ProjectId.make("project-1"),
          title: "Live thread",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt,
          updatedAt: createdAt,
        },
      });

      const error = yield* Effect.flip(
        decideOrchestrationCommand({
          command: {
            type: "thread.turn.start",
            commandId: CommandId.make("command-live-import-id"),
            threadId,
            message: {
              messageId: MessageId.make("import:forged-live-message"),
              role: "user",
              text: "Live work",
              attachments: [],
            },
            runtimeMode: "full-access",
            interactionMode: "default",
            createdAt,
          },
          readModel,
        }),
      );

      expect(error._tag).toBe("OrchestrationCommandInvariantError");
      expect(error.message).toContain("reserved imported-session namespace");
    }),
  );

  it.effect("rejects live assistant messages in the imported-session namespace", () =>
    Effect.gen(function* () {
      const createdAt = "2026-08-24T10:00:00.000Z";
      const threadId = ThreadId.make("thread-live-assistant-message");
      const readModel = yield* projectEvent(createEmptyReadModel(createdAt), {
        sequence: 1,
        eventId: EventId.make("event-live-assistant-thread-created"),
        aggregateKind: "thread",
        aggregateId: threadId,
        type: "thread.created",
        occurredAt: createdAt,
        commandId: CommandId.make("command-live-assistant-thread-created"),
        causationEventId: null,
        correlationId: CommandId.make("command-live-assistant-thread-created"),
        metadata: {},
        payload: {
          threadId,
          projectId: ProjectId.make("project-1"),
          title: "Live thread",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt,
          updatedAt: createdAt,
        },
      });

      for (const commandType of [
        "thread.message.assistant.delta",
        "thread.message.assistant.complete",
      ] as const) {
        const command =
          commandType === "thread.message.assistant.delta"
            ? {
                type: commandType,
                commandId: CommandId.make("command-live-assistant-delta-import-id"),
                threadId,
                messageId: MessageId.make("import:forged-live-assistant-message"),
                delta: "Live work",
                createdAt,
              }
            : {
                type: commandType,
                commandId: CommandId.make("command-live-assistant-complete-import-id"),
                threadId,
                messageId: MessageId.make("import:forged-live-assistant-message"),
                createdAt,
              };
        const error = yield* Effect.flip(decideOrchestrationCommand({ command, readModel }));

        expect(error._tag).toBe("OrchestrationCommandInvariantError");
        expect(error.message).toContain("reserved imported-session namespace");
      }
    }),
  );
});

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
