import type { UsageSummary } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/unstable/reactivity";

import {
  isEnvironmentUnavailable,
  type EnvironmentConnectionPresentation,
} from "../connection/presentation.ts";

export interface EnvironmentUsageQueryState {
  readonly isPending: boolean;
  readonly error: string | null;
  readonly summary: UsageSummary | null;
}

/**
 * Reads a usage query only while its environment can still answer. Environment
 * RPC queries intentionally wait for a connection, so treating an unavailable
 * connection as terminal here prevents one offline host from blocking totals.
 */
export function readEnvironmentUsageQueryState(
  connection: EnvironmentConnectionPresentation,
  readResult: () => AsyncResult.AsyncResult<UsageSummary, unknown>,
): EnvironmentUsageQueryState {
  if (isEnvironmentUnavailable(connection)) {
    return {
      isPending: false,
      error: "This environment could not report usage.",
      summary: null,
    };
  }

  const result = readResult();
  return {
    isPending: result.waiting,
    error: result._tag === "Failure" ? "This environment could not report usage." : null,
    summary: Option.getOrNull(AsyncResult.value(result)),
  };
}
