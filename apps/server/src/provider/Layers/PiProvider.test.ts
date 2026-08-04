import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { PiSettings } from "@t3tools/contracts";
import {
  PiRuntime,
  PiRuntimeError,
  type PiCommandResult,
  type PiRuntimeShape,
} from "../piRuntime.ts";
import { checkPiProviderStatus, parsePiModelTable, piModelsFromTable } from "./PiProvider.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);

const modelTable = `provider              model                  context  max-out  thinking  images
azure-foundry         gpt-5.5                400K     128K     yes       yes
sika                  Qwythos-9B-v2-MTP-Q8_0  262.1K   32.8K    no        no
`;

const runtimeMock = {
  calls: [] as Array<ReadonlyArray<string>>,
  results: new Map<string, PiCommandResult>(),
  errors: new Map<string, PiRuntimeError>(),
  reset() {
    this.calls.length = 0;
    this.results = new Map([
      ["--version", { stdout: "0.83.0\n", stderr: "", code: 0 }],
      ["--list-models --offline", { stdout: modelTable, stderr: "", code: 0 }],
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
const makeSettings = (overrides?: Partial<PiSettings>): PiSettings =>
  decodePiSettings({
    enabled: true,
    binaryPath: "pi",
    sessionDir: "",
    customModels: [],
    ...overrides,
  });

describe("Pi model discovery", () => {
  it("parses Pi's stable six-column model table", () => {
    expect(parsePiModelTable(modelTable)).toEqual([
      {
        provider: "azure-foundry",
        model: "gpt-5.5",
        context: "400K",
        maxOut: "128K",
        thinking: "yes",
        images: "yes",
      },
      {
        provider: "sika",
        model: "Qwythos-9B-v2-MTP-Q8_0",
        context: "262.1K",
        maxOut: "32.8K",
        thinking: "no",
        images: "no",
      },
    ]);
  });

  it("preserves custom providers and exposes thinking capability only when supported", () => {
    const models = piModelsFromTable(parsePiModelTable(modelTable), [
      "topo/gemma-4-12B-it-Q4_K_M-thinking",
      "azure-foundry/gpt-5.5",
    ]);

    expect(models.map((model) => model.slug)).toEqual([
      "azure-foundry/gpt-5.5",
      "sika/Qwythos-9B-v2-MTP-Q8_0",
      "topo/gemma-4-12B-it-Q4_K_M-thinking",
    ]);
    expect(models[0]?.subProvider).toBe("azure-foundry");
    expect(models[1]?.subProvider).toBe("sika");
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
});

it.layer(testLayer)("checkPiProviderStatus", (it) => {
  it.effect("reports dynamically discovered custom-provider models", () =>
    Effect.gen(function* () {
      runtimeMock.reset();
      const snapshot = yield* checkPiProviderStatus(makeSettings(), process.cwd());

      expect(snapshot.status).toBe("ready");
      expect(snapshot.version).toBe("0.83.0");
      expect(snapshot.models.map((model) => model.slug)).toEqual([
        "azure-foundry/gpt-5.5",
        "sika/Qwythos-9B-v2-MTP-Q8_0",
      ]);
      expect(runtimeMock.calls.map((args) => args.join(" ")).toSorted()).toEqual([
        "--list-models --offline",
        "--version",
      ]);
    }),
  );

  it.effect("reports non-zero Pi command exits without hiding configured fallback models", () =>
    Effect.gen(function* () {
      runtimeMock.reset();
      runtimeMock.results.set("--list-models --offline", {
        stdout: "",
        stderr: "provider configuration failed",
        code: 2,
      });
      const snapshot = yield* checkPiProviderStatus(
        makeSettings({ customModels: ["fallback/local-model"] }),
        process.cwd(),
      );

      expect(snapshot.status).toBe("error");
      expect(snapshot.installed).toBe(true);
      expect(snapshot.message).toBe(
        "Pi Agent CLI is installed but `pi --list-models --offline` failed.",
      );
      expect(snapshot.models.map((model) => model.slug)).toEqual(["fallback/local-model"]);
    }),
  );

  it.effect("reports model probe transport failures separately from missing binaries", () =>
    Effect.gen(function* () {
      runtimeMock.reset();
      runtimeMock.errors.set(
        "--list-models --offline",
        new PiRuntimeError({
          operation: "collect-command",
          detail: "model inventory stream failed",
        }),
      );
      const snapshot = yield* checkPiProviderStatus(makeSettings(), process.cwd());

      expect(snapshot.status).toBe("error");
      expect(snapshot.installed).toBe(true);
      expect(snapshot.message).toBe("Pi Agent model discovery failed.");
    }),
  );
});
