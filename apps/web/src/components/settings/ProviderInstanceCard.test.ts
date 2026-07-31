import { describe, expect, it } from "vite-plus/test";
import type { ServerProviderModel } from "@t3tools/contracts";

import {
  deriveProviderModelsForDisplay,
  deriveProviderUsageForDisplay,
} from "./ProviderInstanceCard";

describe("deriveProviderModelsForDisplay", () => {
  it("uses current config custom models instead of stale live custom rows", () => {
    const liveModels: ReadonlyArray<ServerProviderModel> = [
      {
        slug: "server-model",
        name: "Server Model",
        isCustom: false,
        capabilities: null,
      },
      {
        slug: "removed-custom",
        name: "Removed Custom",
        isCustom: true,
        capabilities: null,
      },
      {
        slug: "kept-custom",
        name: "Kept Custom",
        isCustom: true,
        capabilities: null,
      },
    ];

    expect(
      deriveProviderModelsForDisplay({
        liveModels,
        customModels: ["kept-custom"],
      }).map((model) => model.slug),
    ).toEqual(["server-model", "kept-custom"]);
  });
});

describe("deriveProviderUsageForDisplay", () => {
  it("shows every weekly limit without assuming the primary field is weekly", () => {
    expect(
      deriveProviderUsageForDisplay({
        primaryLimitId: "codex",
        usage: {
          rateLimits: [
            {
              id: "codex_bengalfox",
              name: "GPT-5.3-Codex-Spark",
              windows: [
                { usedPercent: 12, windowDurationMins: 300 },
                { usedPercent: 0, windowDurationMins: 10_080 },
              ],
            },
            {
              id: "codex",
              windows: [{ usedPercent: 4, windowDurationMins: 10_080 }],
            },
          ],
        },
      }),
    ).toEqual([
      {
        key: "codex:10080:0",
        label: "Weekly",
        usedPercent: 4,
      },
      {
        key: "codex_bengalfox:10080:1",
        label: "GPT-5.3-Codex-Spark · Weekly",
        usedPercent: 0,
      },
    ]);
  });

  it("falls back to shorter windows when a provider has no weekly limit", () => {
    expect(
      deriveProviderUsageForDisplay({
        primaryLimitId: "codex",
        usage: {
          rateLimits: [
            {
              id: "codex",
              windows: [{ usedPercent: 25, windowDurationMins: 300 }],
            },
          ],
        },
      }),
    ).toEqual([
      {
        key: "codex:300:0",
        label: "5-hour",
        usedPercent: 25,
      },
    ]);
  });
});
