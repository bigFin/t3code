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

export const PROVIDER_HOST_PROTOCOL_VERSION = 1 as const;

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
  generationFingerprint: ProviderHostGenerationFingerprint,
  hostProcess: ResourceTelemetryProcessIdentity,
  startedAt: Schema.DateTimeUtcFromString,
  latestCursor: ProviderHostReplayCursor,
});
export type ProviderHostHelloEnvelope = typeof ProviderHostHelloEnvelope.Type;

export const ProviderHostHealthEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("health"),
  status: ProviderHostStatus,
  generationFingerprint: ProviderHostGenerationFingerprint,
  hostProcess: ResourceTelemetryProcessIdentity,
  codexChildProcess: Schema.optionalKey(ResourceTelemetryProcessIdentity),
  latestCursor: ProviderHostReplayCursor,
});
export type ProviderHostHealthEnvelope = typeof ProviderHostHealthEnvelope.Type;

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

export const ProviderHostAttachEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("attach"),
  clientId: ProviderHostClientId,
  attachmentId: ProviderHostAttachmentId,
  threadId: ThreadId,
  replayFrom: Schema.optionalKey(ProviderHostReplayCursor),
  session: Schema.optionalKey(Schema.Json),
});
export type ProviderHostAttachEnvelope = typeof ProviderHostAttachEnvelope.Type;

export const ProviderHostDetachEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("detach"),
  clientId: ProviderHostClientId,
  attachmentId: ProviderHostAttachmentId,
  threadId: ThreadId,
});
export type ProviderHostDetachEnvelope = typeof ProviderHostDetachEnvelope.Type;

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
});
export type ProviderHostCommandEnvelope = typeof ProviderHostCommandEnvelope.Type;

export const ProviderHostSnapshotEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("snapshot"),
  threadId: ThreadId,
  cursor: ProviderHostReplayCursor,
  replayTruncated: Schema.optionalKey(Schema.Boolean),
  state: Schema.Json,
});
export type ProviderHostSnapshotEnvelope = typeof ProviderHostSnapshotEnvelope.Type;

export const ProviderHostEventEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("event"),
  threadId: ThreadId,
  sequence: ProviderHostEventSequence,
  event: Schema.Json,
});
export type ProviderHostEventEnvelope = typeof ProviderHostEventEnvelope.Type;

export const ProviderHostCommandResultEnvelope = Schema.Struct({
  version: Schema.Literal(PROVIDER_HOST_PROTOCOL_VERSION),
  type: Schema.Literal("commandResult"),
  commandId: CommandId,
  threadId: ThreadId,
  ok: Schema.Boolean,
  result: Schema.optionalKey(Schema.Json),
  error: Schema.optionalKey(Schema.String),
});
export type ProviderHostCommandResultEnvelope = typeof ProviderHostCommandResultEnvelope.Type;

export const ProviderHostClientEnvelope = Schema.Union([
  ProviderHostAttachEnvelope,
  ProviderHostDetachEnvelope,
  ProviderHostCommandEnvelope,
]);
export type ProviderHostClientEnvelope = typeof ProviderHostClientEnvelope.Type;

export const ProviderHostServerEnvelope = Schema.Union([
  ProviderHostHelloEnvelope,
  ProviderHostHealthEnvelope,
  ProviderHostInventoryEnvelope,
  ProviderHostSnapshotEnvelope,
  ProviderHostEventEnvelope,
  ProviderHostCommandResultEnvelope,
]);
export type ProviderHostServerEnvelope = typeof ProviderHostServerEnvelope.Type;

export const ProviderHostEnvelope = Schema.Union([
  ProviderHostHelloEnvelope,
  ProviderHostHealthEnvelope,
  ProviderHostInventoryEnvelope,
  ProviderHostAttachEnvelope,
  ProviderHostDetachEnvelope,
  ProviderHostCommandEnvelope,
  ProviderHostSnapshotEnvelope,
  ProviderHostEventEnvelope,
  ProviderHostCommandResultEnvelope,
]);
export type ProviderHostEnvelope = typeof ProviderHostEnvelope.Type;

export const providerHostReplayCursorForSequence = (
  sequence: ProviderHostEventSequence,
): ProviderHostReplayCursor => ProviderHostReplayCursor.make(Number(sequence));
