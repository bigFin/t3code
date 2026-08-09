import { ProviderDriverKind, ProviderInstanceId, type OmpSettings } from "@t3tools/contracts";

import { makePiAdapter, type PiAdapterLiveOptions } from "./PiAdapter.ts";

const PROVIDER = ProviderDriverKind.make("omp");

export type OmpAdapterLiveOptions = Omit<PiAdapterLiveOptions, "provider" | "rpcApprovalFlag">;

/**
 * OMP retains Pi's JSONL RPC protocol, with its current approval flag. The
 * shared adapter preserves its session/event semantics while binding events
 * and sessions to OMP's distinct provider identity.
 */
export function makeOmpAdapter(ompSettings: OmpSettings, options?: OmpAdapterLiveOptions) {
  const instanceId = options?.instanceId ?? ProviderInstanceId.make("omp");

  return makePiAdapter(ompSettings, {
    ...options,
    instanceId,
    provider: PROVIDER,
    rpcApprovalFlag: "--auto-approve",
  });
}
