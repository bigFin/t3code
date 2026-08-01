import { ProviderInstanceId, TrimmedNonEmptyString } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";

import { writeFileStringAtomically } from "../../atomicWrite.ts";
import { ProviderHostGenerationFingerprint } from "./ProviderHostProtocol.ts";

export const CODEX_PROVIDER_HOST_CONFIG_VERSION = 1 as const;

export const CodexProviderHostConfig = Schema.Struct({
  version: Schema.Literal(CODEX_PROVIDER_HOST_CONFIG_VERSION),
  providerInstanceId: ProviderInstanceId,
  generationFingerprint: ProviderHostGenerationFingerprint,
  controlSocketPath: TrimmedNonEmptyString,
  appServerSocketPath: TrimmedNonEmptyString,
  manifestPath: TrimmedNonEmptyString,
  codex: Schema.Struct({
    binaryPath: TrimmedNonEmptyString,
    launchArgs: Schema.optionalKey(Schema.String),
    homePath: Schema.optionalKey(TrimmedNonEmptyString),
    cwd: TrimmedNonEmptyString,
  }),
});
export type CodexProviderHostConfig = typeof CodexProviderHostConfig.Type;

export class CodexProviderHostConfigError extends Schema.TaggedErrorClass<CodexProviderHostConfigError>()(
  "CodexProviderHostConfigError",
  {
    operation: Schema.Literals(["read", "decode", "encode", "persist"]),
    path: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to ${this.operation} Codex provider-host config at ${this.path}.`;
  }
}

const CodexProviderHostConfigJson = Schema.fromJsonString(CodexProviderHostConfig);
const decodeConfig = Schema.decodeUnknownEffect(CodexProviderHostConfigJson);
const encodeConfig = Schema.encodeEffect(CodexProviderHostConfigJson);

export const readCodexProviderHostConfig = Effect.fn("readCodexProviderHostConfig")(function* (
  path: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const contents = yield* fs.readFileString(path).pipe(
    Effect.mapError(
      (cause) =>
        new CodexProviderHostConfigError({
          operation: "read",
          path,
          cause,
        }),
    ),
  );
  return yield* decodeConfig(contents).pipe(
    Effect.mapError(
      (cause) =>
        new CodexProviderHostConfigError({
          operation: "decode",
          path,
          cause,
        }),
    ),
  );
});

export const persistCodexProviderHostConfig = Effect.fn("persistCodexProviderHostConfig")(
  function* (input: { readonly path: string; readonly config: CodexProviderHostConfig }) {
    const contents = yield* encodeConfig(input.config).pipe(
      Effect.mapError(
        (cause) =>
          new CodexProviderHostConfigError({
            operation: "encode",
            path: input.path,
            cause,
          }),
      ),
    );
    yield* writeFileStringAtomically({
      filePath: input.path,
      contents: `${contents}\n`,
      mode: 0o600,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new CodexProviderHostConfigError({
            operation: "persist",
            path: input.path,
            cause,
          }),
      ),
    );
  },
);
