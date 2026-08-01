import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import {
  PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
  ProviderHostManifest,
  ProviderHostManifestError,
  persistProviderHostManifest,
  readProviderHostManifest,
} from "./ProviderHostManifest.ts";
import {
  PROVIDER_HOST_PROTOCOL_VERSION,
  ProviderHostGenerationFingerprint,
} from "./ProviderHostProtocol.ts";

const manifest: ProviderHostManifest = {
  schemaVersion: PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
  protocolVersion: PROVIDER_HOST_PROTOCOL_VERSION,
  generationFingerprint: ProviderHostGenerationFingerprint.make("generation-a"),
  hostProcess: {
    pid: 123,
    startTimeMs: 1_000,
  },
  socketPath: "/tmp/t3-provider-host.sock",
  codex: {
    childProcess: {
      pid: 124,
      startTimeMs: 1_001,
    },
    resolvedBinary: "/nix/store/codex/bin/codex",
    version: "codex-cli 0.146.0",
    launchConfig: {
      arguments: ["app-server", "--listen", "unix:///tmp/codex.sock"],
      workingDirectory: "/workspace",
      environmentKeys: ["CODEX_HOME", "PATH"],
    },
  },
  startedAt: DateTime.makeUnsafe("2026-07-31T12:00:00.000Z"),
};

const isManifestError = Schema.is(ProviderHostManifestError);

describe("ProviderHostManifest", () => {
  it.effect("atomically persists and restores the complete host identity", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-provider-host-manifest-test-",
      });
      const manifestPath = path.join(root, "provider", "host.json");

      yield* persistProviderHostManifest({ path: manifestPath, manifest });
      const restored = yield* readProviderHostManifest(manifestPath);
      const persistedInfo = yield* fs.stat(manifestPath);

      assert.deepEqual(Option.getOrThrow(restored), manifest);
      assert.isTrue((yield* fs.readFileString(manifestPath)).endsWith("\n"));
      assert.equal(persistedInfo.mode & 0o777, 0o600);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("represents a missing or empty manifest as absent", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-provider-host-manifest-test-",
      });
      const manifestPath = path.join(root, "host.json");

      assert.isTrue(Option.isNone(yield* readProviderHostManifest(manifestPath)));
      yield* fs.writeFileString(manifestPath, "\n");
      assert.isTrue(Option.isNone(yield* readProviderHostManifest(manifestPath)));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("surfaces malformed durable state as a typed decode failure", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-provider-host-manifest-test-",
      });
      const manifestPath = path.join(root, "host.json");
      yield* fs.writeFileString(manifestPath, '{"schemaVersion":1}');

      const error = yield* readProviderHostManifest(manifestPath).pipe(Effect.flip);

      assert.isTrue(isManifestError(error));
      if (isManifestError(error)) {
        assert.equal(error.operation, "decode");
        assert.equal(error.manifestPath, manifestPath);
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it("requires process start identity and the matching protocol version", () => {
    const decode = Schema.decodeUnknownSync(ProviderHostManifest);
    const encoded = {
      schemaVersion: 1,
      protocolVersion: 1,
      generationFingerprint: "generation-a",
      hostProcess: {
        pid: 123,
        startTimeMs: 1_000,
      },
      socketPath: "/tmp/t3-provider-host.sock",
      codex: {
        childProcess: {
          pid: 124,
          startTimeMs: 1_001,
        },
        resolvedBinary: "/usr/bin/codex",
        version: "codex-cli 0.146.0",
        launchConfig: {
          arguments: [],
          environmentKeys: [],
        },
      },
      startedAt: "2026-07-31T12:00:00.000Z",
    };

    assert.doesNotThrow(() => decode(encoded));
    assert.throws(() =>
      decode({
        ...encoded,
        hostProcess: { pid: 123 },
      }),
    );
    assert.throws(() =>
      decode({
        ...encoded,
        protocolVersion: 2,
      }),
    );
  });

  it("allows the Codex child identity to be absent before child readiness", () => {
    const { childProcess: _childProcess, ...codex } = manifest.codex;
    const withoutChild = ProviderHostManifest.make({
      ...manifest,
      codex,
    });

    assert.isUndefined(withoutChild.codex.childProcess);
  });
});
