import type {
  OrchestrationClientOrigin,
  OrchestrationEvent,
  OrchestrationReadModel,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import { OrchestrationCommand } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  metricAttributes,
  orchestrationCommandAckDuration,
  orchestrationCommandsTotal,
  orchestrationCommandDuration,
} from "../../observability/Metrics.ts";
import { toPersistenceSqlError } from "../../persistence/Errors.ts";
import { OrchestrationEventStore } from "../../persistence/Services/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepository } from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import {
  isOrchestrationCommandRejection,
  OrchestrationCommandIdConflictError,
  OrchestrationCommandInvariantError,
  OrchestrationCommandPreviouslyRejectedError,
  type OrchestrationDispatchError,
  type OrchestrationProjectorDecodeError,
} from "../Errors.ts";
import { decideOrchestrationCommand, isOrchestrationCommandNoop } from "../decider.ts";
import { createEmptyReadModel, projectEvent } from "../projector.ts";
import { OrchestrationProjectionPipeline } from "../Services/ProjectionPipeline.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { ThreadBackgroundLivenessService } from "../ThreadBackgroundLiveness.ts";
import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "../Services/OrchestrationEngine.ts";
const isOrchestrationCommandPreviouslyRejectedError = Schema.is(
  OrchestrationCommandPreviouslyRejectedError,
);
const isOrchestrationCommandIdConflictError = Schema.is(OrchestrationCommandIdConflictError);
const isOrchestrationCommandInvariantError = Schema.is(OrchestrationCommandInvariantError);
const encodeUnknownJsonString = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

interface CommandEnvelope {
  command: OrchestrationCommand;
  origin: OrchestrationClientOrigin | undefined;
  result: Deferred.Deferred<{ sequence: number }, OrchestrationDispatchError>;
  startedAtMs: number;
}

function commandToAggregateRef(command: OrchestrationCommand): {
  readonly aggregateKind: "project" | "thread";
  readonly aggregateId: ProjectId | ThreadId;
} {
  switch (command.type) {
    case "project.create":
    case "project.meta.update":
    case "project.delete":
      return {
        aggregateKind: "project",
        aggregateId: command.projectId,
      };
    default:
      return {
        aggregateKind: "thread",
        aggregateId: command.threadId,
      };
  }
}

const makeOrchestrationEngine = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const eventStore = yield* OrchestrationEventStore;
  const commandReceiptRepository = yield* OrchestrationCommandReceiptRepository;
  const projectionPipeline = yield* OrchestrationProjectionPipeline;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const threadBackgroundLiveness = yield* ThreadBackgroundLivenessService;
  const crypto = yield* Crypto.Crypto;

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  let commandReadModel = createEmptyReadModel(yield* nowIso);

  const commandQueue = yield* Queue.unbounded<CommandEnvelope>();
  const eventPubSub = yield* PubSub.unbounded<OrchestrationEvent>();

  const projectEventsOntoReadModel = (
    baseReadModel: OrchestrationReadModel,
    events: ReadonlyArray<OrchestrationEvent>,
  ): Effect.Effect<OrchestrationReadModel, OrchestrationProjectorDecodeError, never> =>
    Effect.gen(function* () {
      let nextReadModel = baseReadModel;
      for (const event of events) {
        nextReadModel = yield* projectEvent(nextReadModel, event);
      }
      return nextReadModel;
    });

  const processEnvelope = (envelope: CommandEnvelope): Effect.Effect<void> => {
    const dispatchStartSequence = commandReadModel.snapshotSequence;
    let processingStartedAtMs = 0;
    const aggregateRef = commandToAggregateRef(envelope.command);
    const baseMetricAttributes = {
      commandType: envelope.command.type,
      aggregateKind: aggregateRef.aggregateKind,
    } as const;
    const reconcileReadModelAfterDispatchFailure = Effect.gen(function* () {
      const persistedEvents = yield* Stream.runCollect(
        eventStore.readFromSequence(dispatchStartSequence),
      ).pipe(Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)));
      if (persistedEvents.length === 0) {
        return;
      }

      commandReadModel = yield* projectEventsOntoReadModel(commandReadModel, persistedEvents);

      for (const persistedEvent of persistedEvents) {
        yield* PubSub.publish(eventPubSub, persistedEvent);
      }
    });

    return Effect.exit(
      Effect.gen(function* () {
        processingStartedAtMs = yield* Clock.currentTimeMillis;
        yield* Effect.annotateCurrentSpan({
          "orchestration.command_id": envelope.command.commandId,
          "orchestration.command_type": envelope.command.type,
          "orchestration.aggregate_kind": aggregateRef.aggregateKind,
          "orchestration.aggregate_id": aggregateRef.aggregateId,
        });

        const existingReceipt = yield* commandReceiptRepository.getByCommandId({
          commandId: envelope.command.commandId,
        });
        if (Option.isSome(existingReceipt)) {
          // A receipt only proves this exact command was handled. Replaying it
          // for a command aimed at another aggregate would report success for
          // work that never happened.
          if (
            existingReceipt.value.aggregateKind !== aggregateRef.aggregateKind ||
            existingReceipt.value.aggregateId !== aggregateRef.aggregateId
          ) {
            return yield* new OrchestrationCommandIdConflictError({
              commandId: envelope.command.commandId,
              receiptAggregateKind: existingReceipt.value.aggregateKind,
              receiptAggregateId: existingReceipt.value.aggregateId,
              commandAggregateKind: aggregateRef.aggregateKind,
              commandAggregateId: aggregateRef.aggregateId,
            });
          }
          if (existingReceipt.value.status === "accepted") {
            return {
              sequence: existingReceipt.value.resultSequence,
            };
          }
          return yield* new OrchestrationCommandPreviouslyRejectedError({
            commandId: envelope.command.commandId,
            detail: existingReceipt.value.error ?? "Previously rejected.",
          });
        }

        if (
          envelope.command.type === "thread.auto-settle" &&
          (yield* eventStore.hasEventAfter({
            aggregateKind: "thread",
            aggregateId: envelope.command.threadId,
            sequenceExclusive: envelope.command.snapshotSequence,
          }))
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: envelope.command.type,
            detail: `thread ${envelope.command.threadId} changed before automatic settlement`,
          });
        }

        if (
          envelope.command.type === "thread.auto-settle" &&
          threadBackgroundLiveness.getThreadBackgroundLiveness(envelope.command.threadId) !== null
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: envelope.command.type,
            detail: `thread ${envelope.command.threadId} has live background work`,
          });
        }

        // Command snapshots omit activities at startup and cap them while running.
        // Read this request's durable state before deciding how to send the answer.
        const userInputActivity =
          envelope.command.type === "thread.user-input.respond"
            ? yield* projectionSnapshotQuery.getUserInputActivity(envelope.command)
            : Option.none();
        const eventBase = yield* decideOrchestrationCommand({
          command: envelope.command,
          readModel: commandReadModel,
          ...(Option.isSome(userInputActivity)
            ? { userInputActivity: userInputActivity.value }
            : {}),
        }).pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.mapError((cause) =>
            isOrchestrationCommandRejection(cause)
              ? cause
              : new OrchestrationCommandInvariantError({
                  commandType: envelope.command.type,
                  detail: "Failed to generate an event identifier.",
                  cause,
                }),
          ),
        );
        const decidedCommandIsNoop = isOrchestrationCommandNoop(eventBase);
        const plannedEvents = decidedCommandIsNoop
          ? []
          : Array.isArray(eventBase)
            ? eventBase
            : [eventBase];
        // Stamp the dispatching client's origin onto every event the command
        // produced. The decider stays pure; attribution is an engine concern.
        const plannedEventBases =
          envelope.origin === undefined
            ? plannedEvents
            : plannedEvents.map((planned) => ({
                ...planned,
                metadata: { ...planned.metadata, origin: envelope.origin },
              }));
        const committedCommand = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const providerRuntimeGuardedCommand =
                envelope.command.type === "thread.session.set" ||
                envelope.command.type === "thread.turn.reconcile" ||
                envelope.command.type === "thread.message.import" ||
                envelope.command.type === "thread.messages.import"
                  ? envelope.command
                  : undefined;
              const expectedProviderRuntime =
                providerRuntimeGuardedCommand?.expectedProviderRuntime;
              const expectedUserMessageIds =
                envelope.command.type === "thread.session.set" ||
                envelope.command.type === "thread.turn.reconcile"
                  ? envelope.command.expectedUserMessageIds
                  : undefined;
              const guardedThreadId =
                envelope.command.type === "thread.session.set" ||
                envelope.command.type === "thread.turn.reconcile"
                  ? envelope.command.threadId
                  : undefined;
              const expectedResumeCursorJson =
                expectedProviderRuntime === undefined || expectedProviderRuntime === null
                  ? null
                  : expectedProviderRuntime.resumeCursor === null
                    ? null
                    : encodeUnknownJsonString(expectedProviderRuntime.resumeCursor);
              const providerRuntimeMatches =
                providerRuntimeGuardedCommand === undefined || expectedProviderRuntime === undefined
                  ? true
                  : expectedProviderRuntime === null
                    ? (yield* sql<{ readonly matches: number }>`
                          SELECT 1 AS matches
                          FROM provider_session_runtime
                          WHERE thread_id = ${providerRuntimeGuardedCommand.threadId}
                          LIMIT 1
                        `).length === 0
                    : (yield* sql<{ readonly matches: number }>`
                        SELECT 1 AS matches
                        FROM provider_session_runtime
                        WHERE thread_id = ${providerRuntimeGuardedCommand.threadId}
                          AND provider_name = ${expectedProviderRuntime.providerName}
                          AND COALESCE(provider_instance_id, provider_name) =
                            ${expectedProviderRuntime.providerInstanceId}
                          AND status = ${expectedProviderRuntime.status}
                          AND last_seen_at = ${expectedProviderRuntime.lastSeenAt}
                          AND resume_cursor_json IS ${expectedResumeCursorJson}
                          AND (
                            ${expectedProviderRuntime.requiresDetachedIdle ? 1 : 0} = 0
                            OR (
                              json_extract(
                                runtime_payload_json,
                                '$.sessionPersistence'
                              ) = 'detached'
                              AND json_type(
                                runtime_payload_json,
                                '$.activeTurnId'
                              ) = 'null'
                            )
                          )
                        LIMIT 1
                      `).length > 0;
              const projectedUserMessageIds =
                expectedUserMessageIds === undefined || guardedThreadId === undefined
                  ? []
                  : (yield* sql<{ readonly messageId: string }>`
                        SELECT message_id AS "messageId"
                        FROM projection_thread_messages
                        WHERE thread_id = ${guardedThreadId}
                          AND role = 'user'
                        ORDER BY created_at ASC, message_id ASC
                      `).map((row) => row.messageId);
              const projectedUserMessagesMatch =
                expectedUserMessageIds === undefined ||
                (expectedUserMessageIds.length === projectedUserMessageIds.length &&
                  expectedUserMessageIds.every(
                    (messageId, index) => messageId === projectedUserMessageIds[index],
                  ));
              const commandIsNoop =
                decidedCommandIsNoop || !providerRuntimeMatches || !projectedUserMessagesMatch;
              const eventBases = commandIsNoop ? [] : plannedEventBases;
              const committedEvents: OrchestrationEvent[] = [];
              const attachmentCleanups: Effect.Effect<void>[] = [];
              let nextCommandReadModel = commandReadModel;

              for (const nextEvent of eventBases) {
                const savedEvent = yield* eventStore.append(nextEvent);
                nextCommandReadModel = yield* projectEvent(nextCommandReadModel, savedEvent);
                const cleanup = yield* projectionPipeline.projectEventDeferred(savedEvent);
                attachmentCleanups.push(cleanup);
                committedEvents.push(savedEvent);
              }
              const lastSavedEvent = committedEvents.at(-1) ?? null;
              if (lastSavedEvent === null) {
                if (!commandIsNoop) {
                  return yield* new OrchestrationCommandInvariantError({
                    commandType: envelope.command.type,
                    detail: "Command produced no events.",
                  });
                }
                const acceptedAt = yield* nowIso;
                yield* commandReceiptRepository.upsert({
                  commandId: envelope.command.commandId,
                  aggregateKind: aggregateRef.aggregateKind,
                  aggregateId: aggregateRef.aggregateId,
                  acceptedAt,
                  resultSequence: commandReadModel.snapshotSequence,
                  status: "accepted",
                  error: null,
                });
                return {
                  committedEvents,
                  attachmentCleanups,
                  lastSequence: commandReadModel.snapshotSequence,
                  nextCommandReadModel,
                } as const;
              }

              yield* commandReceiptRepository.upsert({
                commandId: envelope.command.commandId,
                aggregateKind: lastSavedEvent.aggregateKind,
                aggregateId: lastSavedEvent.aggregateId,
                acceptedAt: lastSavedEvent.occurredAt,
                resultSequence: lastSavedEvent.sequence,
                status: "accepted",
                error: null,
              });

              return {
                committedEvents,
                attachmentCleanups,
                lastSequence: lastSavedEvent.sequence,
                nextCommandReadModel,
              } as const;
            }),
          )
          .pipe(
            Effect.catchTag("SqlError", (sqlError) =>
              Effect.fail(
                toPersistenceSqlError("OrchestrationEngine.processEnvelope:transaction")(sqlError),
              ),
            ),
          );

        commandReadModel = committedCommand.nextCommandReadModel;
        for (const cleanup of committedCommand.attachmentCleanups) {
          yield* cleanup;
        }
        for (const [index, event] of committedCommand.committedEvents.entries()) {
          yield* PubSub.publish(eventPubSub, event);
          if (index === 0) {
            yield* Metric.update(
              Metric.withAttributes(
                orchestrationCommandAckDuration,
                metricAttributes({
                  ...baseMetricAttributes,
                  ackEventType: event.type,
                }),
              ),
              Duration.millis(Math.max(0, (yield* Clock.currentTimeMillis) - envelope.startedAtMs)),
            );
          }
        }
        return { sequence: committedCommand.lastSequence };
      }).pipe(Effect.withSpan(`orchestration.command.${envelope.command.type}`)),
    ).pipe(
      Effect.flatMap((exit) =>
        Effect.gen(function* () {
          const outcome = Exit.isSuccess(exit)
            ? "success"
            : Cause.hasInterruptsOnly(exit.cause)
              ? "interrupt"
              : "failure";
          yield* Metric.update(
            Metric.withAttributes(
              orchestrationCommandDuration,
              metricAttributes(baseMetricAttributes),
            ),
            Duration.millis(Math.max(0, (yield* Clock.currentTimeMillis) - processingStartedAtMs)),
          );
          yield* Metric.update(
            Metric.withAttributes(
              orchestrationCommandsTotal,
              metricAttributes({
                ...baseMetricAttributes,
                outcome,
              }),
            ),
            1,
          );

          if (Exit.isSuccess(exit)) {
            yield* Deferred.succeed(envelope.result, exit.value);
            return;
          }

          const error = Cause.squash(exit.cause) as OrchestrationDispatchError;
          if (
            !isOrchestrationCommandPreviouslyRejectedError(error) &&
            !isOrchestrationCommandIdConflictError(error)
          ) {
            yield* reconcileReadModelAfterDispatchFailure.pipe(
              Effect.catch(() =>
                Effect.logWarning(
                  "failed to reconcile orchestration read model after dispatch failure",
                ).pipe(
                  Effect.annotateLogs({
                    commandId: envelope.command.commandId,
                    snapshotSequence: commandReadModel.snapshotSequence,
                  }),
                ),
              ),
            );

            if (isOrchestrationCommandRejection(error)) {
              yield* commandReceiptRepository
                .upsert({
                  commandId: envelope.command.commandId,
                  aggregateKind: aggregateRef.aggregateKind,
                  aggregateId: aggregateRef.aggregateId,
                  acceptedAt: yield* nowIso,
                  resultSequence: commandReadModel.snapshotSequence,
                  status: "rejected",
                  error: error.message,
                })
                .pipe(Effect.catch(() => Effect.void));
            }
          }

          yield* Deferred.fail(envelope.result, error);
        }),
      ),
    );
  };

  yield* projectionPipeline.bootstrap;
  commandReadModel = yield* projectionSnapshotQuery.getCommandReadModel();

  const worker = Effect.forever(Queue.take(commandQueue).pipe(Effect.flatMap(processEnvelope)));
  yield* Effect.forkScoped(worker);
  yield* Effect.logDebug("orchestration engine started").pipe(
    Effect.annotateLogs({ sequence: commandReadModel.snapshotSequence }),
  );

  const readEvents: OrchestrationEngineShape["readEvents"] = (fromSequenceExclusive, limit) =>
    eventStore.readFromSequence(fromSequenceExclusive, limit);
  const readAggregateEvents: OrchestrationEngineShape["readAggregateEvents"] = (input) =>
    eventStore.readAggregateFromSequence(input);

  const readThreadEvents: OrchestrationEngineShape["readThreadEvents"] = ({ threadId, ...range }) =>
    eventStore.readAggregateRange({ ...range, aggregateKind: "thread", aggregateId: threadId });

  const getThreadReplayStats: OrchestrationEngineShape["getThreadReplayStats"] = ({
    threadId,
    ...range
  }) =>
    eventStore.getAggregateReplayStats({
      ...range,
      aggregateKind: "thread",
      aggregateId: threadId,
    });

  const dispatch: OrchestrationEngineShape["dispatch"] = (command, options) =>
    Effect.gen(function* () {
      const result = yield* Deferred.make<{ sequence: number }, OrchestrationDispatchError>();
      yield* Queue.offer(commandQueue, {
        command,
        origin: options?.origin,
        result,
        startedAtMs: yield* Clock.currentTimeMillis,
      });
      return yield* Deferred.await(result);
    });

  return {
    readEvents,
    readAggregateEvents,
    readThreadEvents,
    getThreadReplayStats,
    dispatch,
    subscribeDomainEvents: PubSub.subscribe(eventPubSub).pipe(Effect.map(Stream.fromSubscription)),
    // Each access creates a fresh PubSub subscription so that multiple
    // consumers (wsServer, ProviderRuntimeIngestion, CheckpointReactor, etc.)
    // each independently receive all domain events.
    get streamDomainEvents(): OrchestrationEngineShape["streamDomainEvents"] {
      return Stream.fromPubSub(eventPubSub);
    },
    // The command read model's snapshotSequence tracks the latest committed
    // event sequence (updated on the worker fiber). A plain property read is a
    // consistent, committed value — reassignment of `commandReadModel` is
    // atomic on the single-threaded event loop.
    latestSequence: Effect.sync(() => commandReadModel.snapshotSequence),
  } satisfies OrchestrationEngineShape;
});

export const OrchestrationEngineLive = Layer.effect(
  OrchestrationEngineService,
  makeOrchestrationEngine,
);
