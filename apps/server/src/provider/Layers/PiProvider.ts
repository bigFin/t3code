import {
  type ModelCapabilities,
  type PiSettings,
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
import { PiRuntime, type PiCommandResult } from "../piRuntime.ts";

const PI_PRESENTATION = {
  displayName: "Pi Agent",
  showInteractionModeToggle: false,
} as const;
const PI_PROBE_TIMEOUT = "5 seconds";

const PI_THINKING_LEVELS = ["off", "minimal", "low", "medium", "high", "xhigh", "max"] as const;

const PiModelTableRow = Schema.Struct({
  provider: TrimmedNonEmptyString,
  model: TrimmedNonEmptyString,
  context: TrimmedNonEmptyString,
  maxOut: TrimmedNonEmptyString,
  thinking: Schema.Literals(["yes", "no"]),
  images: Schema.Literals(["yes", "no"]),
});
export type PiModelTableRow = typeof PiModelTableRow.Type;

const decodePiModelTableRowExit = Schema.decodeUnknownExit(PiModelTableRow);

const EMPTY_CAPABILITIES = createModelCapabilities({ optionDescriptors: [] });
const THINKING_CAPABILITIES = createModelCapabilities({
  optionDescriptors: [
    {
      id: "thinkingLevel",
      label: "Thinking",
      description: "Reasoning effort requested from Pi Agent.",
      type: "select",
      currentValue: "medium",
      options: PI_THINKING_LEVELS.map((level) =>
        level === "medium"
          ? { id: level, label: titleCase(level), isDefault: true as const }
          : { id: level, label: titleCase(level) },
      ),
    },
  ],
});

function titleCase(value: string): string {
  return value.charAt(0).toUpperCase() + value.slice(1);
}

export function parsePiModelTable(output: string): ReadonlyArray<PiModelTableRow> {
  const lines = output
    .split(/\r?\n/u)
    .map((line) => line.trimEnd())
    .filter((line) => line.trim().length > 0);
  if (lines.length < 2) {
    return [];
  }

  const header = lines[0]!.trim().split(/\s{2,}/u);
  if (
    header.length !== 6 ||
    header[0] !== "provider" ||
    header[1] !== "model" ||
    header[2] !== "context" ||
    header[3] !== "max-out" ||
    header[4] !== "thinking" ||
    header[5] !== "images"
  ) {
    return [];
  }

  const rows: Array<PiModelTableRow> = [];
  for (const line of lines.slice(1)) {
    const columns = line.trim().split(/\s{2,}/u);
    if (columns.length !== 6) {
      continue;
    }
    const decoded = decodePiModelTableRowExit({
      provider: columns[0],
      model: columns[1],
      context: columns[2],
      maxOut: columns[3],
      thinking: columns[4],
      images: columns[5],
    });
    if (Exit.isSuccess(decoded)) {
      rows.push(decoded.value);
    }
  }
  return rows;
}

export function piModelsFromTable(
  rows: ReadonlyArray<PiModelTableRow>,
  customModels: ReadonlyArray<string>,
): ReadonlyArray<ServerProviderModel> {
  const discovered = rows.map((row): ServerProviderModel => ({
    slug: `${row.provider}/${row.model}`,
    name: row.model,
    subProvider: row.provider,
    isCustom: false,
    capabilities: row.thinking === "yes" ? THINKING_CAPABILITIES : EMPTY_CAPABILITIES,
  }));
  return providerModelsFromSettings(discovered, customModels, EMPTY_CAPABILITIES);
}

function commandSucceeded(result: PiCommandResult): boolean {
  return result.code === 0;
}

function withProbeTimeout<A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | Cause.TimeoutError, R> {
  return effect.pipe(Effect.timeout(PI_PROBE_TIMEOUT));
}

export const makePendingPiProvider = (piSettings: PiSettings): Effect.Effect<ServerProviderDraft> =>
  Effect.gen(function* () {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const models = providerModelsFromSettings([], piSettings.customModels, EMPTY_CAPABILITIES);

    if (!piSettings.enabled) {
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Pi Agent is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown", type: "pi" },
        message: "Checking Pi Agent and its configured models.",
      },
    });
  });

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  piSettings: PiSettings,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
): Effect.fn.Return<ServerProviderDraft, never, PiRuntime> {
  const runtime = yield* PiRuntime;
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = providerModelsFromSettings(
    [],
    piSettings.customModels,
    EMPTY_CAPABILITIES,
  );

  if (!piSettings.enabled) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi Agent is disabled in T3 Code settings.",
      },
    });
  }

  const [versionExit, modelsExit] = yield* Effect.all(
    [
      Effect.exit(
        withProbeTimeout(
          runtime.runCommand({
            binaryPath: piSettings.binaryPath,
            args: ["--version"],
            cwd,
            ...(environment ? { environment } : {}),
          }),
        ),
      ),
      Effect.exit(
        withProbeTimeout(
          runtime.runCommand({
            binaryPath: piSettings.binaryPath,
            args: ["--list-models", "--offline"],
            cwd,
            ...(environment ? { environment } : {}),
          }),
        ),
      ),
    ],
    { concurrency: "unbounded" },
  );

  if (Exit.isFailure(versionExit)) {
    const cause = Cause.squash(versionExit.cause);
    const detail = cause instanceof Error ? cause.message : String(cause);
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: detail.includes("timed out")
          ? "Timed out while checking Pi Agent provider status."
          : "Pi Agent CLI (`pi`) is not installed or not on PATH.",
      },
    });
  }

  const version = commandSucceeded(versionExit.value)
    ? parseGenericCliVersion(versionExit.value.stdout)
    : null;
  if (!commandSucceeded(versionExit.value)) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown", type: "pi" },
        message: "Pi Agent CLI is installed but `pi --version` failed.",
      },
    });
  }

  if (Exit.isFailure(modelsExit)) {
    const cause = Cause.squash(modelsExit.cause);
    const detail = cause instanceof Error ? cause.message : String(cause);
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown", type: "pi" },
        message: detail.includes("timed out")
          ? "Timed out while discovering Pi Agent models."
          : "Pi Agent model discovery failed.",
      },
    });
  }

  if (!commandSucceeded(modelsExit.value)) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown", type: "pi" },
        message: "Pi Agent CLI is installed but `pi --list-models --offline` failed.",
      },
    });
  }

  const rows = parsePiModelTable(modelsExit.value.stdout);
  const models = piModelsFromTable(rows, piSettings.customModels);
  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: true,
    checkedAt,
    models,
    probe: {
      installed: true,
      version,
      status: models.length > 0 ? "ready" : "warning",
      auth: { status: "unknown", type: "pi" },
      message:
        models.length > 0
          ? `${models.length} configured Pi Agent model${models.length === 1 ? "" : "s"} discovered.`
          : "Pi Agent is available, but no configured models were discovered.",
    },
  });
});
