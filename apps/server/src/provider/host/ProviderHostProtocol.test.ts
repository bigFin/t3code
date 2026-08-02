import { CommandId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";

import {
  PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
  PROVIDER_HOST_PROTOCOL_VERSION,
  ProviderHostAttachmentId,
  ProviderHostBuildFingerprint,
  ProviderHostClientEnvelope,
  ProviderHostClientId,
  ProviderHostCommandEnvelope,
  ProviderHostCompatibleServerEnvelope,
  ProviderHostEnvelope,
  ProviderHostEventEnvelope,
  ProviderHostEventSequence,
  ProviderHostGenerationFingerprint,
  ProviderHostHelloEnvelope,
  ProviderHostReplayCursor,
  ProviderHostServerEnvelope,
  ProviderHostV1AttachEnvelope,
  ProviderHostV1ClientEnvelope,
  ProviderHostV1CommandEnvelope,
  providerHostReplayCursorForSequence,
} from "./ProviderHostProtocol.ts";

const decodeEnvelope = Schema.decodeUnknownSync(ProviderHostEnvelope);
const decodeClientEnvelope = Schema.decodeUnknownSync(ProviderHostClientEnvelope);
const decodeServerEnvelope = Schema.decodeUnknownSync(ProviderHostServerEnvelope);
const decodeCompatibleServerEnvelope = Schema.decodeUnknownSync(
  ProviderHostCompatibleServerEnvelope,
);
const decodeV1ClientEnvelope = Schema.decodeUnknownSync(ProviderHostV1ClientEnvelope);
const decodeCommandEnvelope = Schema.decodeUnknownSync(ProviderHostCommandEnvelope);
const decodeEventEnvelope = Schema.decodeUnknownSync(ProviderHostEventEnvelope);

describe("ProviderHostProtocol", () => {
  it("decodes every versioned envelope", () => {
    const common = {
      version: PROVIDER_HOST_PROTOCOL_VERSION,
    } as const;
    const clientId = "client-1";
    const attachmentId = "attachment-1";
    const threadId = "thread-1";

    const envelopes = [
      {
        ...common,
        type: "hello",
        providerInstanceId: "codex",
        buildFingerprint: "build-a",
        generationFingerprint: "generation-a",
        appServerMode: "spawn",
        canAdoptSessions: false,
        hostProcess: { pid: 123, startTimeMs: 1_000 },
        startedAt: "2026-07-31T12:00:00.000Z",
        latestCursor: 2,
      },
      {
        ...common,
        type: "health",
        status: "healthy",
        buildFingerprint: "build-a",
        generationFingerprint: "generation-a",
        appServerMode: "spawn",
        canAdoptSessions: false,
        hostProcess: { pid: 123, startTimeMs: 1_000 },
        codexChildProcess: { pid: 124, startTimeMs: 1_001 },
        latestCursor: 2,
      },
      {
        ...common,
        type: "inventory",
        threads: [
          {
            threadId,
            status: "active",
            attachmentCount: 2,
            cursor: 2,
          },
        ],
      },
      {
        ...common,
        type: "attach",
        clientId,
        attachmentId,
        threadId,
        mode: "reuse",
        replayFrom: 1,
      },
      {
        ...common,
        type: "detach",
        clientId,
        attachmentId,
        threadId,
      },
      {
        ...common,
        type: "command",
        clientId,
        attachmentId,
        commandId: "command-1",
        threadId,
        operation: "turn.start",
        payload: { prompt: "Proceed" },
      },
      {
        ...common,
        type: "snapshot",
        threadId,
        cursor: 2,
        replayTruncated: true,
        state: { status: "active" },
      },
      {
        ...common,
        type: "attachError",
        threadId,
        errorCode: "thread-id-missing",
        error: "Codex no longer has the detached thread.",
      },
      {
        ...common,
        type: "event",
        threadId,
        sequence: 3,
        event: { type: "content.delta", delta: "hello" },
      },
    ];

    const decoded = envelopes.map((envelope) => decodeEnvelope(envelope));

    assert.deepEqual(
      decoded.map((envelope) => envelope.type),
      [
        "hello",
        "health",
        "inventory",
        "attach",
        "detach",
        "command",
        "snapshot",
        "attachError",
        "event",
      ],
    );
    const hello = decoded[0];
    assert.strictEqual(hello?.type, "hello");
    if (hello?.type === "hello") {
      assert.equal(DateTime.formatIso(hello.startedAt), "2026-07-31T12:00:00.000Z");
    }
  });

  it("separates client commands from host stream envelopes", () => {
    const attach = decodeClientEnvelope({
      version: PROVIDER_HOST_PROTOCOL_VERSION,
      type: "attach",
      clientId: "client-1",
      attachmentId: "attachment-1",
      threadId: "thread-1",
      mode: "reuse",
    });
    const snapshot = decodeServerEnvelope({
      version: PROVIDER_HOST_PROTOCOL_VERSION,
      type: "snapshot",
      threadId: "thread-1",
      cursor: 0,
      state: {},
    });

    assert.equal(attach.type, "attach");
    assert.equal(snapshot.type, "snapshot");
    assert.throws(() => decodeServerEnvelope(attach));
    assert.throws(() => decodeClientEnvelope(snapshot));
  });

  it("decodes legacy host hello and client envelopes without v2-only fields", () => {
    const hello = decodeCompatibleServerEnvelope({
      version: PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
      type: "hello",
      providerInstanceId: "codex",
      generationFingerprint: "generation-legacy",
      hostProcess: { pid: 123, startTimeMs: 1_000 },
      startedAt: "2026-07-31T12:00:00.000Z",
      latestCursor: 2,
    });
    const attach = ProviderHostV1AttachEnvelope.make({
      version: PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
      type: "attach",
      clientId: ProviderHostClientId.make("client-legacy"),
      attachmentId: ProviderHostAttachmentId.make("attachment-legacy"),
      threadId: ThreadId.make("thread-legacy"),
      replayFrom: ProviderHostReplayCursor.make(1),
    });
    const command = ProviderHostV1CommandEnvelope.make({
      version: PROVIDER_HOST_LEGACY_PROTOCOL_VERSION,
      type: "command",
      clientId: ProviderHostClientId.make("client-legacy"),
      attachmentId: ProviderHostAttachmentId.make("attachment-legacy"),
      commandId: CommandId.make("command-legacy"),
      threadId: ThreadId.make("thread-legacy"),
      operation: "thread.read",
      payload: {},
    });

    assert.equal(hello.version, PROVIDER_HOST_LEGACY_PROTOCOL_VERSION);
    assert.deepEqual(decodeV1ClientEnvelope(attach), attach);
    assert.deepEqual(decodeV1ClientEnvelope(command), command);
    assert.equal("mode" in attach, false);
    assert.equal("deadlineAtMs" in command, false);
    assert.throws(() => decodeServerEnvelope(hello));
    assert.throws(() => decodeClientEnvelope(attach));
  });

  it("uses the contracts command brand as the required idempotency key", () => {
    const command = ProviderHostCommandEnvelope.make({
      version: PROVIDER_HOST_PROTOCOL_VERSION,
      type: "command",
      clientId: ProviderHostClientId.make("client-1"),
      attachmentId: ProviderHostAttachmentId.make("attachment-1"),
      commandId: CommandId.make("command-stable-across-retries"),
      threadId: ThreadId.make("thread-1"),
      operation: "turn.start",
      payload: { prompt: "Proceed" },
    });

    assert.equal(command.commandId, "command-stable-across-retries");
    assert.throws(() =>
      decodeCommandEnvelope({
        ...command,
        commandId: "",
      }),
    );
  });

  it("rejects incompatible versions and invalid replay positions", () => {
    assert.throws(() =>
      decodeEnvelope({
        version: 1,
        type: "snapshot",
        threadId: "thread-1",
        cursor: 0,
        state: {},
      }),
    );
    assert.throws(() =>
      decodeEventEnvelope({
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        type: "event",
        threadId: "thread-1",
        sequence: 0,
        event: {},
      }),
    );
    assert.throws(() =>
      decodeEnvelope({
        version: PROVIDER_HOST_PROTOCOL_VERSION,
        type: "attach",
        clientId: "client-1",
        attachmentId: "attachment-1",
        threadId: "thread-1",
        mode: "reuse",
        replayFrom: -1,
      }),
    );
  });

  it("converts an event sequence into the reader cursor for that event", () => {
    const sequence = ProviderHostEventSequence.make(42);

    assert.equal(providerHostReplayCursorForSequence(sequence), ProviderHostReplayCursor.make(42));
  });

  it("retains the established contracts brands at protocol boundaries", () => {
    const hello = ProviderHostHelloEnvelope.make({
      version: PROVIDER_HOST_PROTOCOL_VERSION,
      type: "hello",
      providerInstanceId: ProviderInstanceId.make("codex"),
      buildFingerprint: ProviderHostBuildFingerprint.make("build-a"),
      generationFingerprint: ProviderHostGenerationFingerprint.make("generation-a"),
      appServerMode: "spawn",
      canAdoptSessions: false,
      hostProcess: { pid: 123, startTimeMs: 1_000 },
      startedAt: DateTime.makeUnsafe("2026-07-31T12:00:00.000Z"),
      latestCursor: ProviderHostReplayCursor.make(0),
    });

    assert.equal(hello.providerInstanceId, "codex");
  });
});
