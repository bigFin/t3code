// @effect-diagnostics preferSchemaOverJson:off - Legacy manifest fixtures exercise JSON migration.
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
  PROVIDER_HOST_MANIFEST_V2_PROTOCOL_VERSION,
  ProviderHostManifest,
  ProviderHostManifestV2,
  ProviderHostManifestError,
  persistProviderHostManifest,
  readProviderHostManifest,
} from "./ProviderHostManifest.ts";
import {
  PROVIDER_HOST_PROTOCOL_VERSION,
  ProviderHostBuildFingerprint,
  ProviderHostGenerationFingerprint,
} from "./ProviderHostProtocol.ts";

const manifest: ProviderHostManifest = {
  schemaVersion: PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
  protocolVersion: PROVIDER_HOST_PROTOCOL_VERSION,
  buildFingerprint: ProviderHostBuildFingerprint.make("build-a"),
  generationFingerprint: ProviderHostGenerationFingerprint.make("generation-a"),
  hostProcess: {
    pid: 223,
    startTimeMs: 2_000,
  },
  controlSocketPath: "/tmp/t3-provider-host-v2.sock",
  codex: {
    appServerMode: "attach",
    owner: {
      generationFingerprint: ProviderHostGenerationFingerprint.make("generation-a"),
      process: {
        pid: 123,
        startTimeMs: 1_000,
      },
    },
    appServer: {
      process: {
        pid: 124,
        startTimeMs: 1_001,
      },
      socketPath: "/tmp/codex.sock",
      resolvedBinary: "/nix/store/codex/bin/codex",
      version: "codex-cli 0.146.0",
      launchConfig: {
        arguments: ["app-server", "--listen", "unix:///tmp/codex.sock"],
        workingDirectory: "/workspace",
        environmentKeys: ["CODEX_HOME", "PATH"],
      },
    },
  },
  startedAt: DateTime.makeUnsafe("2026-07-31T13:00:00.000Z"),
};

const v1Manifest = {
  schemaVersion: 1 as const,
  protocolVersion: 1 as const,
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
  it.effect("round-trips v2 owner and app-server provenance", () =>
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

  it.effect("decodes existing v1 manifests without requiring v2 provenance", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-provider-host-manifest-test-",
      });
      const manifestPath = path.join(root, "host.json");
      yield* fs.writeFileString(
        manifestPath,
        JSON.stringify({
          ...v1Manifest,
          startedAt: "2026-07-31T12:00:00.000Z",
        }),
      );

      const restored = Option.getOrThrow(yield* readProviderHostManifest(manifestPath));

      assert.equal(restored.schemaVersion, 1);
      if (restored.schemaVersion === 1) {
        assert.deepEqual(restored, v1Manifest);
        assert.isUndefined(restored.codex.appServerMode);
      }
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
      schemaVersion: PROVIDER_HOST_MANIFEST_SCHEMA_VERSION,
      protocolVersion: PROVIDER_HOST_PROTOCOL_VERSION,
      buildFingerprint: "build-a",
      generationFingerprint: "generation-a",
      hostProcess: {
        pid: 223,
        startTimeMs: 2_000,
      },
      controlSocketPath: "/tmp/t3-provider-host-v2.sock",
      codex: {
        appServerMode: "attach",
        owner: {
          generationFingerprint: "generation-a",
          process: {
            pid: 123,
            startTimeMs: 1_000,
          },
        },
        appServer: {
          process: {
            pid: 124,
            startTimeMs: 1_001,
          },
          socketPath: "/tmp/codex.sock",
          resolvedBinary: "/usr/bin/codex",
          version: "codex-cli 0.146.0",
          launchConfig: {
            arguments: [],
            environmentKeys: [],
          },
        },
      },
      startedAt: "2026-07-31T12:00:00.000Z",
    };

    assert.doesNotThrow(() => decode(encoded));
    assert.throws(() =>
      decode({
        ...encoded,
        codex: {
          ...encoded.codex,
          appServer: {
            ...encoded.codex.appServer,
            process: { pid: 124 },
          },
        },
      }),
    );
    assert.throws(() =>
      decode({
        ...encoded,
        protocolVersion: 1,
      }),
    );
  });

  it("keeps the schema-v2 decoder pinned to protocol 2", () => {
    const decode = Schema.decodeUnknownSync(ProviderHostManifestV2);

    assert.equal(PROVIDER_HOST_MANIFEST_V2_PROTOCOL_VERSION, 2);
    assert.doesNotThrow(() =>
      decode({
        ...manifest,
        protocolVersion: 2,
        startedAt: "2026-07-31T13:00:00.000Z",
      }),
    );
  });

  it("requires distinct owner and app-server socket provenance in v2", () => {
    const decode = Schema.decodeUnknownSync(ProviderHostManifest);

    assert.throws(() =>
      decode({
        ...manifest,
        codex: {
          ...manifest.codex,
          appServer: {
            ...manifest.codex.appServer,
            socketPath: undefined,
          },
        },
      }),
    );
  });
});
