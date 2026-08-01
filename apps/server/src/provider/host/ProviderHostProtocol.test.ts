import { CommandId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";

import {
  PROVIDER_HOST_PROTOCOL_VERSION,
  ProviderHostAttachmentId,
  ProviderHostClientEnvelope,
  ProviderHostClientId,
  ProviderHostCommandEnvelope,
  ProviderHostEnvelope,
  ProviderHostEventEnvelope,
  ProviderHostEventSequence,
  ProviderHostGenerationFingerprint,
  ProviderHostHelloEnvelope,
  ProviderHostReplayCursor,
  ProviderHostServerEnvelope,
  providerHostReplayCursorForSequence,
} from "./ProviderHostProtocol.ts";

const decodeEnvelope = Schema.decodeUnknownSync(ProviderHostEnvelope);
const decodeClientEnvelope = Schema.decodeUnknownSync(ProviderHostClientEnvelope);
const decodeServerEnvelope = Schema.decodeUnknownSync(ProviderHostServerEnvelope);
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
        generationFingerprint: "generation-a",
        hostProcess: { pid: 123, startTimeMs: 1_000 },
        startedAt: "2026-07-31T12:00:00.000Z",
        latestCursor: 2,
      },
      {
        ...common,
        type: "health",
        status: "healthy",
        generationFingerprint: "generation-a",
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
        type: "event",
        threadId,
        sequence: 3,
        event: { type: "content.delta", delta: "hello" },
      },
    ];

    const decoded = envelopes.map((envelope) => decodeEnvelope(envelope));

    assert.deepEqual(
      decoded.map((envelope) => envelope.type),
      ["hello", "health", "inventory", "attach", "detach", "command", "snapshot", "event"],
    );
    const hello = decoded[0];
    assert.strictEqual(hello?.type, "hello");
    if (hello?.type === "hello") {
      assert.equal(DateTime.formatIso(hello.startedAt), "2026-07-31T12:00:00.000Z");
    }
  });

  it("separates client commands from host stream envelopes", () => {
    const attach = decodeClientEnvelope({
      version: 1,
      type: "attach",
      clientId: "client-1",
      attachmentId: "attachment-1",
      threadId: "thread-1",
    });
    const snapshot = decodeServerEnvelope({
      version: 1,
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
        version: 2,
        type: "snapshot",
        threadId: "thread-1",
        cursor: 0,
        state: {},
      }),
    );
    assert.throws(() =>
      decodeEventEnvelope({
        version: 1,
        type: "event",
        threadId: "thread-1",
        sequence: 0,
        event: {},
      }),
    );
    assert.throws(() =>
      decodeEnvelope({
        version: 1,
        type: "attach",
        clientId: "client-1",
        attachmentId: "attachment-1",
        threadId: "thread-1",
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
      generationFingerprint: ProviderHostGenerationFingerprint.make("generation-a"),
      hostProcess: { pid: 123, startTimeMs: 1_000 },
      startedAt: DateTime.makeUnsafe("2026-07-31T12:00:00.000Z"),
      latestCursor: ProviderHostReplayCursor.make(0),
    });

    assert.equal(hello.providerInstanceId, "codex");
  });
});
