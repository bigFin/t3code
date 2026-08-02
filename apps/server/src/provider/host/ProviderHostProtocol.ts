import {
  CommandId,
  NonNegativeInt,
  PositiveInt,
  ProviderInstanceId,
  ResourceTelemetryProcessIdentity,
  ThreadId,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export const PROVIDER_HOST_LEGACY_PROTOCOL_VERSION = 1 as const;
export const PROVIDER_HOST_PROTOCOL_VERSION = 2 as const;

export const ProviderHostProtocolVersion = Schema.Literals([
  PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
  PROVIDER_HOST_PROTOCOL_VERSION,
]);
export type ProviderHostProtocolVersion = typeof ProviderHostProtocolVersion.Type;

export const ProviderHostClientId = TrimmedNonEmptyString.pipe(
  Schema.brand("ProviderHostClientId"),
);
export type ProviderHostClientId = typeof ProviderHostClientId.Type;

export const ProviderHostAttachmentId = TrimmedNonEmptyString.pipe(
  Schema.brand("ProviderHostAttachmentId"),
);
export type ProviderHostAttachmentId = typeof ProviderHostAttachmentId.Type;

export const ProviderHostGenerationFingerprint = TrimmedNonEmptyString.pipe(
  Schema.brand("ProviderHostGenerationFingerprint"),
);
export type ProviderHostGenerationFingerprint = typeof ProviderHostGenerationFingerprint.Type;

export const ProviderHostBuildFingerprint = TrimmedNonEmptyString.pipe(
  Schema.brand("ProviderHostBuildFingerprint"),
);
export type ProviderHostBuildFingerprint = typeof ProviderHostBuildFingerprint.Type;

export const ProviderHostConfigurationFingerprint = TrimmedNonEmptyString.pipe(
  Schema.brand("ProviderHostConfigurationFingerprint"),
);
export type ProviderHostConfigurationFingerprint = typeof ProviderHostConfigurationFingerprint.Type;

export const ProviderHostAppServerMode = Schema.Literals(["spawn", "attach"]);
export type ProviderHostAppServerMode = typeof ProviderHostAppServerMode.Type;

export const ProviderHostAttachMode = Schema.Literals(["create", "reuse", "adopt"]);
export type ProviderHostAttachMode = typeof ProviderHostAttachMode.Type;

/**
 * Host-global event position. Event sequences start at one and only advance
 * within a single provider-host process generation.
 */
export const ProviderHostEventSequence = PositiveInt.pipe(
  Schema.brand("ProviderHostEventSequence"),
);
export type ProviderHostEventSequence = typeof ProviderHostEventSequence.Type;

/**
 * Last event sequence applied by a reader. Zero identifies a reader that has
 * not consumed any events in the current host generation.
 */
export const ProviderHostReplayCursor = NonNegativeInt.pipe(
  Schema.brand("ProviderHostReplayCursor"),
);
export type ProviderHostReplayCursor = typeof ProviderHostReplayCursor.Type;

export const ProviderHostCommandDeadlineMs = NonNegativeInt.pipe(
  Schema.brand("ProviderHostCommandDeadlineMs"),
);
export type ProviderHostCommandDeadlineMs = typeof ProviderHostCommandDeadlineMs.Type;

export const ProviderHostStatus = Schema.Literals(["starting", "healthy", "degraded", "draining"]);
export type ProviderHostStatus = typeof ProviderHostStatus.Type;

export const ProviderHostThreadStatus = Schema.Literals([
  "unknown",
  "idle",
  "active",
  "waiting",
  "error",
]);
export type ProviderHostThreadStatus = typeof ProviderHostThreadStatus.Type;

export const ProviderHostHelloEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("hello"),
  providerInstanceId: ProviderInstanceId,
  buildFingerprint: ProviderHostBuildFingerprint,
  generationFingerprint: ProviderHostGenerationFingerprint,
  appServerMode: ProviderHostAppServerMode,
  canAdoptSessions: Schema.Boolean,
  hostProcess: ResourceTelemetryProcessIdentity,
  startedAt: Schema.DateTimeUtcFromString,
  latestCursor: ProviderHostReplayCursor,
});
export type ProviderHostHelloEnvelope = typeof ProviderHostHelloEnvelope.Type;

export const ProviderHostV1HelloEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_LEGACY_PROTOCOL_VERSION),
  type: Schema.Literal("hello"),
  providerInstanceId: ProviderInstanceId,
  generationFingerprint: ProviderHostGenerationFingerprint,
  hostProcess: ResourceTelemetryProcessIdentity,
  startedAt: Schema.DateTimeUtcFromString,
  latestCursor: ProviderHostReplayCursor,
});
export type ProviderHostV1HelloEnvelope = typeof ProviderHostV1HelloEnvelope.Type;

export const ProviderHostCompatibleHelloEnvelope = Schema.Union([
  ProviderHostHelloEnvelope,
  ProviderHostV1HelloEnvelope,
]);
export type ProviderHostCompatibleHelloEnvelope = typeof ProviderHostCompatibleHelloEnvelope.Type;

export const ProviderHostHealthEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("health"),
  status: ProviderHostStatus,
  buildFingerprint: ProviderHostBuildFingerprint,
  generationFingerprint: ProviderHostGenerationFingerprint,
  appServerMode: ProviderHostAppServerMode,
  canAdoptSessions: Schema.Boolean,
  hostProcess: ResourceTelemetryProcessIdentity,
  codexChildProcess: Schema.optionalKey(ResourceTelemetryProcessIdentity),
  latestCursor: ProviderHostReplayCursor,
});
export type ProviderHostHealthEnvelope = typeof ProviderHostHealthEnvelope.Type;

export const ProviderHostV1HealthEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_LEGACY_PROTOCOL_VERSION),
  type: Schema.Literal("health"),
  status: ProviderHostStatus,
  generationFingerprint: ProviderHostGenerationFingerprint,
  hostProcess: ResourceTelemetryProcessIdentity,
  codexChildProcess: Schema.optionalKey(ResourceTelemetryProcessIdentity),
  latestCursor: ProviderHostReplayCursor,
});
export type ProviderHostV1HealthEnvelope = typeof ProviderHostV1HealthEnvelope.Type;

export const ProviderHostInventoryEntry = Schema.Struct({
  threadId: ThreadId,
  status: ProviderHostThreadStatus,
  attachmentCount: NonNegativeInt,
  cursor: ProviderHostReplayCursor,
});
export type ProviderHostInventoryEntry = typeof ProviderHostInventoryEntry.Type;

export const ProviderHostInventoryEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("inventory"),
  threads: Schema.Array(ProviderHostInventoryEntry),
});
export type ProviderHostInventoryEnvelope = typeof ProviderHostInventoryEnvelope.Type;

export const ProviderHostV1InventoryEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_LEGACY_PROTOCOL_VERSION),
  type: Schema.Literal("inventory"),
  threads: Schema.Array(ProviderHostInventoryEntry),
});
export type ProviderHostV1InventoryEnvelope = typeof ProviderHostV1InventoryEnvelope.Type;

export const ProviderHostAttachEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("attach"),
  clientId: ProviderHostClientId,
  attachmentId: ProviderHostAttachmentId,
  threadId: ThreadId,
  replayFrom: Schema.optionalKey(ProviderHostReplayCursor),
  mode: ProviderHostAttachMode,
  session: Schema.optionalKey(Schema.Json),
});
export type ProviderHostAttachEnvelope = typeof ProviderHostAttachEnvelope.Type;

export const ProviderHostV1AttachEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_LEGACY_PROTOCOL_VERSION),
  type: Schema.Literal("attach"),
  clientId: ProviderHostClientId,
  attachmentId: ProviderHostAttachmentId,
  threadId: ThreadId,
  replayFrom: Schema.optionalKey(ProviderHostReplayCursor),
  session: Schema.optionalKey(Schema.Json),
});
export type ProviderHostV1AttachEnvelope = typeof ProviderHostV1AttachEnvelope.Type;

export const ProviderHostDetachEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("detach"),
  clientId: ProviderHostClientId,
  attachmentId: ProviderHostAttachmentId,
  threadId: ThreadId,
});
export type ProviderHostDetachEnvelope = typeof ProviderHostDetachEnvelope.Type;

export const ProviderHostV1DetachEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_LEGACY_PROTOCOL_VERSION),
  type: Schema.Literal("detach"),
  clientId: ProviderHostClientId,
  attachmentId: ProviderHostAttachmentId,
  threadId: ThreadId,
});
export type ProviderHostV1DetachEnvelope = typeof ProviderHostV1DetachEnvelope.Type;

/**
 * `commandId` is the idempotency key. A writer must retain it across retries,
 * and a host must return the original outcome rather than execute it twice.
 */
export const ProviderHostCommandEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("command"),
  clientId: ProviderHostClientId,
  attachmentId: ProviderHostAttachmentId,
  commandId: CommandId,
  threadId: ThreadId,
  operation: TrimmedNonEmptyString,
  payload: Schema.Json,
  deadlineAtMs: Schema.optionalKey(ProviderHostCommandDeadlineMs),
});
export type ProviderHostCommandEnvelope = typeof ProviderHostCommandEnvelope.Type;

export const ProviderHostV1CommandEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_LEGACY_PROTOCOL_VERSION),
  type: Schema.Literal("command"),
  clientId: ProviderHostClientId,
  attachmentId: ProviderHostAttachmentId,
  commandId: CommandId,
  threadId: ThreadId,
  operation: TrimmedNonEmptyString,
  payload: Schema.Json,
});
export type ProviderHostV1CommandEnvelope = typeof ProviderHostV1CommandEnvelope.Type;

export const ProviderHostSnapshotEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("snapshot"),
  threadId: ThreadId,
  cursor: ProviderHostReplayCursor,
  replayTruncated: Schema.optionalKey(Schema.Boolean),
  state: Schema.Json,
});
export type ProviderHostSnapshotEnvelope = typeof ProviderHostSnapshotEnvelope.Type;

export const ProviderHostAttachErrorCode = Schema.Literal("thread-id-missing");
export type ProviderHostAttachErrorCode = typeof ProviderHostAttachErrorCode.Type;

export const ProviderHostAttachErrorEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("attachError"),
  threadId: ThreadId,
  errorCode: ProviderHostAttachErrorCode,
  error: TrimmedNonEmptyString,
});
export type ProviderHostAttachErrorEnvelope = typeof ProviderHostAttachErrorEnvelope.Type;

export const ProviderHostV1SnapshotEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_LEGACY_PROTOCOL_VERSION),
  type: Schema.Literal("snapshot"),
  threadId: ThreadId,
  cursor: ProviderHostReplayCursor,
  replayTruncated: Schema.optionalKey(Schema.Boolean),
  state: Schema.Json,
});
export type ProviderHostV1SnapshotEnvelope = typeof ProviderHostV1SnapshotEnvelope.Type;

export const ProviderHostEventEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("event"),
  threadId: ThreadId,
  sequence: ProviderHostEventSequence,
  event: Schema.Json,
});
export type ProviderHostEventEnvelope = typeof ProviderHostEventEnvelope.Type;

export const ProviderHostV1EventEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_LEGACY_PROTOCOL_VERSION),
  type: Schema.Literal("event"),
  threadId: ThreadId,
  sequence: ProviderHostEventSequence,
  event: Schema.Json,
});
export type ProviderHostV1EventEnvelope = typeof ProviderHostV1EventEnvelope.Type;

export const ProviderHostCommandResultEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("commandResult"),
  commandId: CommandId,
  threadId: ThreadId,
  ok: Schema.Boolean,
  result: Schema.optionalKey(Schema.Json),
  error: Schema.optionalKey(Schema.String),
  errorCode: Schema.optionalKey(Schema.Literal("deadline-exceeded")),
});
export type ProviderHostCommandResultEnvelope = typeof ProviderHostCommandResultEnvelope.Type;

export const ProviderHostV1CommandResultEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_LEGACY_PROTOCOL_VERSION),
  type: Schema.Literal("commandResult"),
  commandId: CommandId,
  threadId: ThreadId,
  ok: Schema.Boolean,
  result: Schema.optionalKey(Schema.Json),
  error: Schema.optionalKey(Schema.String),
});
export type ProviderHostV1CommandResultEnvelope = typeof ProviderHostV1CommandResultEnvelope.Type;

export const ProviderHostCompatibleCommandResultEnvelope = Schema.Union([
  ProviderHostCommandResultEnvelope,
  ProviderHostV1CommandResultEnvelope,
]);
export type ProviderHostCompatibleCommandResultEnvelope =
  typeof ProviderHostCompatibleCommandResultEnvelope.Type;

export const ProviderHostClientEnvelope = Schema.Union([
  ProviderHostAttachEnvelope,
  ProviderHostDetachEnvelope,
  ProviderHostCommandEnvelope,
]);
export type ProviderHostClientEnvelope = typeof ProviderHostClientEnvelope.Type;

export const ProviderHostV1ClientEnvelope = Schema.Union([
  ProviderHostV1AttachEnvelope,
  ProviderHostV1DetachEnvelope,
  ProviderHostV1CommandEnvelope,
]);
export type ProviderHostV1ClientEnvelope = typeof ProviderHostV1ClientEnvelope.Type;

export const ProviderHostServerEnvelope = Schema.Union([
  ProviderHostHelloEnvelope,
  ProviderHostHealthEnvelope,
  ProviderHostInventoryEnvelope,
  ProviderHostSnapshotEnvelope,
  ProviderHostAttachErrorEnvelope,
  ProviderHostEventEnvelope,
  ProviderHostCommandResultEnvelope,
]);
export type ProviderHostServerEnvelope = typeof ProviderHostServerEnvelope.Type;

export const ProviderHostV1ServerEnvelope = Schema.Union([
  ProviderHostV1HelloEnvelope,
  ProviderHostV1HealthEnvelope,
  ProviderHostV1InventoryEnvelope,
  ProviderHostV1SnapshotEnvelope,
  ProviderHostV1EventEnvelope,
  ProviderHostV1CommandResultEnvelope,
]);
export type ProviderHostV1ServerEnvelope = typeof ProviderHostV1ServerEnvelope.Type;

export const ProviderHostCompatibleServerEnvelope = Schema.Union([
  ProviderHostServerEnvelope,
  ProviderHostV1ServerEnvelope,
]);
export type ProviderHostCompatibleServerEnvelope = typeof ProviderHostCompatibleServerEnvelope.Type;

export const ProviderHostEnvelope = Schema.Union([
  ProviderHostHelloEnvelope,
  ProviderHostHealthEnvelope,
  ProviderHostInventoryEnvelope,
  ProviderHostAttachEnvelope,
  ProviderHostDetachEnvelope,
  ProviderHostCommandEnvelope,
  ProviderHostSnapshotEnvelope,
  ProviderHostAttachErrorEnvelope,
  ProviderHostEventEnvelope,
  ProviderHostCommandResultEnvelope,
]);
export type ProviderHostEnvelope = typeof ProviderHostEnvelope.Type;

export const providerHostReplayCursorForSequence = (
  sequence: ProviderHostEventSequence,
): ProviderHostReplayCursor => ProviderHostReplayCursor.make(Number(sequence));
