import { USAGE_CONTRACT_VERSION, type UsageDay, type UsageSummary } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/unstable/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";

import type { EnvironmentConnectionPresentation } from "../connection/presentation.ts";
import { readEnvironmentUsageQueryState } from "./usage.ts";

const summary: UsageSummary = {
  contractVersion: USAGE_CONTRACT_VERSION,
  readAt: "2026-08-09T00:00:00.000Z",
  timeZone: "UTC",
  sinceDay: "2026-08-01" as UsageDay,
  untilDay: "2026-08-09" as UsageDay,
  buckets: [],
  sources: [],
  pricing: { status: "fresh", source: "litellm", fetchedAt: null, knownModels: 0 },
  scanDurationMs: 1,
};

function connection(
  phase: EnvironmentConnectionPresentation["phase"],
  error: string | null = null,
): EnvironmentConnectionPresentation {
  return { phase, error, traceId: null };
}

describe("readEnvironmentUsageQueryState", () => {
  it("makes an unavailable environment terminal without starting its query", () => {
    const readResult = vi.fn(() => AsyncResult.initial<UsageSummary>(true));

    expect(readEnvironmentUsageQueryState(connection("offline"), readResult)).toEqual({
      isPending: false,
      error: "This environment could not report usage.",
      summary: null,
    });
    expect(readResult).not.toHaveBeenCalled();
  });

  it("keeps a connecting environment pending", () => {
    expect(
      readEnvironmentUsageQueryState(connection("connecting"), () =>
        AsyncResult.initial<UsageSummary>(true),
      ),
    ).toEqual({ isPending: true, error: null, summary: null });
  });

  it("isolates a failed environment query", () => {
    expect(
      readEnvironmentUsageQueryState(connection("connected"), () =>
        AsyncResult.failure<UsageSummary, Error>(Cause.fail(new Error("unsupported RPC"))),
      ),
    ).toEqual({
      isPending: false,
      error: "This environment could not report usage.",
      summary: null,
    });
  });

  it("returns a successful environment summary", () => {
    expect(
      readEnvironmentUsageQueryState(connection("connected"), () => AsyncResult.success(summary)),
    ).toEqual({ isPending: false, error: null, summary });
  });
});
