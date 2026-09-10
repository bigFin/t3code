import type { EnvironmentId, UsageSummaryInput, UsageSummary } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/unstable/reactivity";
import {
  isEnvironmentUnavailable,
  type EnvironmentConnectionPresentation,
} from "../connection/presentation.ts";
import * as Schema from "effect/Schema";
import type { AtomRegistry } from "effect/unstable/reactivity";

import { EnvironmentRpcUnavailableError } from "../rpc/client.ts";
import type { createEnvironmentPresentationAtoms } from "./presentation.ts";
import { executeAtomQuery, runAtomCommand, squashAtomCommandFailure } from "./runtime.ts";
import type { createServerEnvironmentAtoms } from "./server.ts";

const isEnvironmentRpcUnavailable = Schema.is(EnvironmentRpcUnavailableError);

/** Refresh pricing, then await each selected environment's rescan while it remains connected. */
export async function refreshUsage({
  registry,
  server,
  presentations,
  environmentIds,
  input,
}: {
  registry: AtomRegistry.AtomRegistry;
  server: Pick<
    ReturnType<typeof createServerEnvironmentAtoms>,
    "usageSummary" | "refreshUsageRates"
  >;
  presentations: Pick<ReturnType<typeof createEnvironmentPresentationAtoms>, "presentationAtom">;
  environmentIds: readonly EnvironmentId[];
  input: UsageSummaryInput;
}): Promise<void> {
  await Promise.all(
    environmentIds.map(async (environmentId) => {
      const query = server.usageSummary({ environmentId, input });
      const presentation = presentations.presentationAtom(environmentId);
      const controller = new AbortController();
      const abortWhenDisconnected = () => {
        if (registry.get(presentation)?.connection.phase !== "connected") controller.abort();
      };
      const unsubscribe = registry.subscribe(presentation, abortWhenDisconnected);
      abortWhenDisconnected();
      try {
        const ratesResult = await runAtomCommand(
          registry,
          server.refreshUsageRates,
          { environmentId, input: {} },
          { reportFailure: false },
        );
        const sessionUnavailable =
          ratesResult._tag === "Failure" &&
          isEnvironmentRpcUnavailable(squashAtomCommandFailure(ratesResult));
        // Invalidate even on failure so reconnects cannot reuse the old summary.
        registry.refresh(query);
        if (sessionUnavailable || controller.signal.aborted) return;
        await executeAtomQuery(registry, query, {
          reportFailure: false,
          signal: controller.signal,
        });
      } finally {
        unsubscribe();
      }
    }),
  );
}

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
