import { ResourceTelemetryProcessIdentity, TrimmedNonEmptyString } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { writeFileStringAtomically } from "../../atomicWrite.ts";
import {
  ProviderHostAppServerMode,
  ProviderHostBuildFingerprint,
  ProviderHostGenerationFingerprint,
} from "./ProviderHostProtocol.ts";

export const PROVIDER_HOST_MANIFEST_SCHEMA_VERSION = 2 as const;
export const PROVIDER_HOST_MANIFEST_V2_PROTOCOL_VERSION = 2 as const;

export const ProviderHostCodexLaunchConfig = Schema.Struct({
  arguments: Schema.Array(Schema.String),
  workingDirectory: Schema.optionalKey(TrimmedNonEmptyString),
  environmentKeys: Schema.Array(TrimmedNonEmptyString),
});
export type ProviderHostCodexLaunchConfig = typeof ProviderHostCodexLaunchConfig.Type;

export const ProviderHostManifestV1 = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  protocolVersion: Schema.Literals([1, 2]),
  generationFingerprint: ProviderHostGenerationFingerprint,
  hostProcess: ResourceTelemetryProcessIdentity,
  socketPath: TrimmedNonEmptyString,
  codex: Schema.Struct({
    appServerMode: Schema.optionalKey(ProviderHostAppServerMode),
    childProcess: Schema.optionalKey(ResourceTelemetryProcessIdentity),
    resolvedBinary: TrimmedNonEmptyString,
    version: TrimmedNonEmptyString,
    launchConfig: ProviderHostCodexLaunchConfig,
  }),
  startedAt: Schema.DateTimeUtcFromString,
});
export type ProviderHostManifestV1 = typeof ProviderHostManifestV1.Type;

export const ProviderHostAppServerOwner = Schema.Struct({
  generationFingerprint: ProviderHostGenerationFingerprint,
  process: ResourceTelemetryProcessIdentity,
});
export type ProviderHostAppServerOwner = typeof ProviderHostAppServerOwner.Type;

export const ProviderHostAppServerProvenance = Schema.Struct({
  owner: ProviderHostAppServerOwner,
  appServer: Schema.Struct({
    process: ResourceTelemetryProcessIdentity,
    socketPath: TrimmedNonEmptyString,
    resolvedBinary: TrimmedNonEmptyString,
    version: TrimmedNonEmptyString,
    launchConfig: ProviderHostCodexLaunchConfig,
  }),
});
export type ProviderHostAppServerProvenance = typeof ProviderHostAppServerProvenance.Type;

export const ProviderHostManifestV2 = Schema.Struct({
  schemaVersion: Schema.Literal(PROVIDER_HOST_MANIFEST_SCHEMA_VERSION),
  protocolVersion: Schema.Literal(PROVIDER_HOST_MANIFEST_V2_PROTOCOL_VERSION),
  buildFingerprint: ProviderHostBuildFingerprint,
  generationFingerprint: ProviderHostGenerationFingerprint,
  hostProcess: ResourceTelemetryProcessIdentity,
  controlSocketPath: TrimmedNonEmptyString,
  codex: Schema.Struct({
    appServerMode: ProviderHostAppServerMode,
    ...ProviderHostAppServerProvenance.fields,
  }),
  startedAt: Schema.DateTimeUtcFromString,
});
export type ProviderHostManifestV2 = typeof ProviderHostManifestV2.Type;

export const ProviderHostManifest = ProviderHostManifestV2;
export type ProviderHostManifest = ProviderHostManifestV2;

export const DecodedProviderHostManifest = Schema.Union([
  ProviderHostManifestV2,
  ProviderHostManifestV1,
]);
export type DecodedProviderHostManifest = typeof DecodedProviderHostManifest.Type;

export class ProviderHostManifestError extends Schema.TaggedErrorClass<ProviderHostManifestError>()(
  "ProviderHostManifestError",
  {
    operation: Schema.Literals(["encode", "persist", "read", "decode"]),
    manifestPath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to ${this.operation} provider-host manifest at ${this.manifestPath}.`;
  }
}

const ProviderHostManifestFromJsonString = Schema.fromJsonString(ProviderHostManifest);
const DecodedProviderHostManifestFromJsonString = Schema.fromJsonString(
  DecodedProviderHostManifest,
);
const encodeProviderHostManifest = Schema.encodeEffect(ProviderHostManifestFromJsonString);
const decodeProviderHostManifest = Schema.decodeUnknownEffect(
  DecodedProviderHostManifestFromJsonString,
);

export const persistProviderHostManifest = Effect.fn("persistProviderHostManifest")(
  function* (input: { readonly path: string; readonly manifest: ProviderHostManifest }) {
    const contents = yield* encodeProviderHostManifest(input.manifest).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderHostManifestError({
            operation: "encode",
            manifestPath: input.path,
            cause,
          }),
      ),
    );

    yield* writeFileStringAtomically({
      filePath: input.path,
      contents: `${contents}\n`,
      mode: 0o600,
    }).pipe(
      Effect.uninterruptible,
      Effect.mapError(
        (cause) =>
          new ProviderHostManifestError({
            operation: "persist",
            manifestPath: input.path,
            cause,
          }),
      ),
    );
  },
);

export const readProviderHostManifest = Effect.fn("readProviderHostManifest")(function* (
  path: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const raw = yield* fs.readFileString(path).pipe(
    Effect.matchEffect({
      onFailure: (cause) =>
        cause.reason._tag === "NotFound"
          ? Effect.succeed(Option.none<string>())
          : Effect.fail(
              new ProviderHostManifestError({
                operation: "read",
                manifestPath: path,
                cause,
              }),
            ),
      onSuccess: (contents) => Effect.succeed(Option.some(contents)),
    }),
  );

  if (Option.isNone(raw) || raw.value.trim().length === 0) {
    return Option.none<DecodedProviderHostManifest>();
  }

  return yield* decodeProviderHostManifest(raw.value.trim()).pipe(
    Effect.map(Option.some),
    Effect.mapError(
      (cause) =>
        new ProviderHostManifestError({
          operation: "decode",
          manifestPath: path,
          cause,
        }),
    ),
  );
});
