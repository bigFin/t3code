import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { OmpSettings } from "@t3tools/contracts";
import {
  PiRuntime,
  PiRuntimeError,
  type PiCommandResult,
  type PiRuntimeShape,
} from "../piRuntime.ts";
import {
  checkOmpProviderStatus,
  ompModelsFromCatalog,
  parseOmpModelCatalog,
} from "./OmpProvider.ts";

const decodeOmpSettings = Schema.decodeSync(OmpSettings);
const modelCatalog = JSON.stringify({
  models: [
    {
      provider: "openai-codex",
      id: "gpt-5.5",
      selector: "openai-codex/gpt-5.5",
      name: "GPT-5.5",
      thinking: ["low", "medium", "high"],
    },
    {
      provider: "local",
      id: "fast-model",
      selector: "local/fast-model",
      name: "Fast model",
      thinking: null,
    },
  ],
});

const runtimeMock = {
  calls: [] as Array<ReadonlyArray<string>>,
  results: new Map<string, PiCommandResult>(),
  errors: new Map<string, PiRuntimeError>(),
  reset() {
    this.calls.length = 0;
    this.results = new Map([
      ["--version", { stdout: "omp/17.2.12\n", stderr: "", code: 0 }],
      ["models --json", { stdout: modelCatalog, stderr: "", code: 0 }],
    ]);
    this.errors.clear();
  },
};

runtimeMock.reset();

const PiRuntimeTestDouble: PiRuntimeShape = {
  runCommand: ({ args }) => {
    const key = args.join(" ");
    runtimeMock.calls.push(args);
    const error = runtimeMock.errors.get(key);
    if (error) {
      return Effect.fail(error);
    }
    return Effect.succeed(
      runtimeMock.results.get(key) ?? { stdout: "", stderr: "unexpected command", code: 2 },
    );
  },
  startRpc: () =>
    Effect.fail(
      new PiRuntimeError({
        operation: "start-rpc",
        detail: "PiRuntimeTestDouble.startRpc is not used by provider tests.",
      }),
    ),
};

const testLayer = Layer.succeed(PiRuntime, PiRuntimeTestDouble);
const makeSettings = (overrides?: Partial<OmpSettings>): OmpSettings =>
  decodeOmpSettings({
    enabled: true,
    binaryPath: "omp",
    sessionDir: "",
    customModels: [],
    ...overrides,
  });

describe("Oh My Pi model discovery", () => {
  it("parses OMP's JSON catalog and preserves per-model thinking options", () => {
    const models = ompModelsFromCatalog(parseOmpModelCatalog(modelCatalog), [
      "openai-codex/gpt-5.5",
      "local/custom-model",
    ]);

    expect(models.map((model) => model.slug)).toEqual([
      "openai-codex/gpt-5.5",
      "local/fast-model",
      "local/custom-model",
    ]);
    expect(models[0]).toMatchObject({
      name: "GPT-5.5",
      subProvider: "openai-codex",
    });
    expect(
      models[0]?.capabilities?.optionDescriptors?.find(
        (descriptor) => descriptor.id === "thinkingLevel",
      ),
    ).toMatchObject({
      type: "select",
      currentValue: "medium",
    });
    expect(models[1]?.capabilities?.optionDescriptors).toEqual([]);
    expect(models[2]?.isCustom).toBe(true);
  });

  it("rejects malformed model catalogs without exposing partial models", () => {
    expect(parseOmpModelCatalog('{"models":[{"provider":"openai"}]}')).toEqual([]);
  });
});

it.layer(testLayer)("checkOmpProviderStatus", (it) => {
  it.effect("reports OMP's discovered models and CLI version", () =>
    Effect.gen(function* () {
      runtimeMock.reset();
      const snapshot = yield* checkOmpProviderStatus(makeSettings(), process.cwd());

      expect(snapshot.status).toBe("ready");
      expect(snapshot.version).toBe("17.2.12");
      expect(snapshot.models.map((model) => model.slug)).toEqual([
        "openai-codex/gpt-5.5",
        "local/fast-model",
      ]);
      expect(runtimeMock.calls.map((args) => args.join(" ")).toSorted()).toEqual([
        "--version",
        "models --json",
      ]);
    }),
  );

  it.effect("retains configured models when OMP model discovery fails", () =>
    Effect.gen(function* () {
      runtimeMock.reset();
      runtimeMock.results.set("models --json", { stdout: "", stderr: "catalog failed", code: 2 });
      const snapshot = yield* checkOmpProviderStatus(
        makeSettings({ customModels: ["fallback/local-model"] }),
        process.cwd(),
      );

      expect(snapshot.status).toBe("error");
      expect(snapshot.installed).toBe(true);
      expect(snapshot.message).toBe("Oh My Pi model discovery failed.");
      expect(snapshot.models.map((model) => model.slug)).toEqual(["fallback/local-model"]);
    }),
  );
});
