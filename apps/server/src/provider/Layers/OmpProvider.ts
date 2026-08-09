import {
  type ModelCapabilities,
  type OmpSettings,
  type ServerProviderModel,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import {
  buildServerProvider,
  parseGenericCliVersion,
  providerModelsFromSettings,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import { PiRuntime } from "../piRuntime.ts";

const OMP_PRESENTATION = {
  displayName: "Oh My Pi",
  showInteractionModeToggle: false,
} as const;
const OMP_PROBE_TIMEOUT = "5 seconds";

const OmpModel = Schema.Struct({
  provider: TrimmedNonEmptyString,
  id: TrimmedNonEmptyString,
  selector: TrimmedNonEmptyString,
  name: TrimmedNonEmptyString,
  thinking: Schema.optionalKey(Schema.NullOr(Schema.Array(TrimmedNonEmptyString))),
});
type OmpModel = typeof OmpModel.Type;

const OmpModelCatalog = Schema.Struct({ models: Schema.Array(OmpModel) });
const decodeOmpModelCatalogExit = Schema.decodeUnknownExit(Schema.fromJsonString(OmpModelCatalog));

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({ optionDescriptors: [] });

function capabilitiesForOmpModel(thinking: ReadonlyArray<string>): ModelCapabilities {
  if (thinking.length === 0) {
    return EMPTY_CAPABILITIES;
  }

  const defaultValue = thinking.includes("medium") ? "medium" : thinking[0]!;
  return createModelCapabilities({
    optionDescriptors: [
      {
        id: "thinkingLevel",
        label: "Thinking",
        description: "Reasoning effort requested from Oh My Pi.",
        type: "select",
        currentValue: defaultValue,
        options: thinking.map((level) =>
          level === defaultValue
            ? {
                id: level,
                label: level.charAt(0).toUpperCase() + level.slice(1),
                isDefault: true as const,
              }
            : { id: level, label: level.charAt(0).toUpperCase() + level.slice(1) },
        ),
      },
    ],
  });
}

export function parseOmpModelCatalog(output: string): ReadonlyArray<OmpModel> {
  const decoded = decodeOmpModelCatalogExit(output);
  return Exit.isSuccess(decoded) ? decoded.value.models : [];
}

export function ompModelsFromCatalog(
  models: ReadonlyArray<OmpModel>,
  customModels: ReadonlyArray<string>,
): ReadonlyArray<ServerProviderModel> {
  const discovered = models.map(
    (model): ServerProviderModel => ({
      slug: model.selector,
      name: model.name,
      subProvider: model.provider,
      isCustom: false,
      capabilities: capabilitiesForOmpModel(model.thinking ?? []),
    }),
  );
  return providerModelsFromSettings(discovered, customModels, EMPTY_CAPABILITIES);
}

function buildOmpProvider(input: {
  readonly enabled: boolean;
  readonly checkedAt: string;
  readonly models: ReadonlyArray<ServerProviderModel>;
  readonly installed: boolean;
  readonly version: string | null;
  readonly status: "error" | "ready" | "warning";
  readonly message: string;
}): ServerProviderDraft {
  return buildServerProvider({
    presentation: OMP_PRESENTATION,
    enabled: input.enabled,
    checkedAt: input.checkedAt,
    models: input.models,
    probe: {
      installed: input.installed,
      version: input.version,
      status: input.status,
      auth: { status: "unknown", type: "pi" },
      message: input.message,
    },
  });
}

export const makePendingOmpProvider = (
  ompSettings: OmpSettings,
): Effect.Effect<ServerProviderDraft> =>
  Effect.gen(function* () {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const models = ompModelsFromCatalog([], ompSettings.customModels);

    return buildOmpProvider({
      enabled: ompSettings.enabled,
      checkedAt,
      models,
      installed: ompSettings.enabled,
      version: null,
      status: "warning",
      message: ompSettings.enabled
        ? "Checking Oh My Pi and its configured models."
        : "Oh My Pi is disabled in T3 Code settings.",
    });
  });

export const checkOmpProviderStatus = Effect.fn("checkOmpProviderStatus")(function* (
  ompSettings: OmpSettings,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
): Effect.fn.Return<ServerProviderDraft, never, PiRuntime> {
  const runtime = yield* PiRuntime;
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = ompModelsFromCatalog([], ompSettings.customModels);

  if (!ompSettings.enabled) {
    return buildOmpProvider({
      enabled: false,
      checkedAt,
      models: fallbackModels,
      installed: false,
      version: null,
      status: "warning",
      message: "Oh My Pi is disabled in T3 Code settings.",
    });
  }

  const [versionExit, modelsExit] = yield* Effect.all(
    [
      Effect.exit(
        runtime
          .runCommand({
            binaryPath: ompSettings.binaryPath,
            args: ["--version"],
            cwd,
            ...(environment ? { environment } : {}),
          })
          .pipe(Effect.timeout(OMP_PROBE_TIMEOUT)),
      ),
      Effect.exit(
        runtime
          .runCommand({
            binaryPath: ompSettings.binaryPath,
            args: ["models", "--json"],
            cwd,
            ...(environment ? { environment } : {}),
          })
          .pipe(Effect.timeout(OMP_PROBE_TIMEOUT)),
      ),
    ],
    { concurrency: "unbounded" },
  );

  if (Exit.isFailure(versionExit)) {
    const cause = Cause.squash(versionExit.cause);
    const detail = cause instanceof Error ? cause.message : String(cause);
    return buildOmpProvider({
      enabled: true,
      checkedAt,
      models: fallbackModels,
      installed: false,
      version: null,
      status: "error",
      message: detail.includes("timed out")
        ? "Timed out while checking Oh My Pi provider status."
        : "Oh My Pi CLI (`omp`) is not installed or not on PATH.",
    });
  }

  const version =
    versionExit.value.code === 0 ? parseGenericCliVersion(versionExit.value.stdout) : null;
  if (versionExit.value.code !== 0) {
    return buildOmpProvider({
      enabled: true,
      checkedAt,
      models: fallbackModels,
      installed: true,
      version,
      status: "error",
      message: "Oh My Pi CLI is installed but `omp --version` failed.",
    });
  }

  if (Exit.isFailure(modelsExit) || modelsExit.value.code !== 0) {
    let detail = "";
    if (Exit.isFailure(modelsExit)) {
      const cause = Cause.squash(modelsExit.cause);
      detail = cause instanceof Error ? cause.message : String(cause);
    }
    return buildOmpProvider({
      enabled: true,
      checkedAt,
      models: fallbackModels,
      installed: true,
      version,
      status: "error",
      message: detail.includes("timed out")
        ? "Timed out while discovering Oh My Pi models."
        : "Oh My Pi model discovery failed.",
    });
  }

  const models = ompModelsFromCatalog(
    parseOmpModelCatalog(modelsExit.value.stdout),
    ompSettings.customModels,
  );
  return buildOmpProvider({
    enabled: true,
    checkedAt,
    models,
    installed: true,
    version,
    status: models.length > 0 ? "ready" : "warning",
    message:
      models.length > 0
        ? `${models.length} configured Oh My Pi model${models.length === 1 ? "" : "s"} discovered.`
        : "Oh My Pi is available, but no configured models were discovered.",
  });
});
