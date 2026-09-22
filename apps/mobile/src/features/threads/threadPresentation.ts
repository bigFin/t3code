import type { StatusTone } from "../../components/StatusPill";
import type { OrchestrationLatestTurn, OrchestrationSession } from "@t3tools/contracts";
import { classifyThreadFailure } from "@t3tools/client-runtime/state/thread-failure";
import { isThreadInterrupted } from "@t3tools/client-runtime/state/thread-settled";
import { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";

export type ThreadStatusKind =
  | "pending-approval"
  | "awaiting-input"
  | "working"
  | "connecting"
  | "retrying"
  | "interrupted"
  | "capacity-limited"
  | "error"
  | "plan-ready";

export interface ThreadStatusPresentation extends StatusTone {
  readonly kind: ThreadStatusKind;
  /** Whether the indicator represents in-flight activity. */
  readonly pulse: boolean;
}

function isLatestTurnSettled(
  latestTurn: OrchestrationLatestTurn | null,
  session: OrchestrationSession | null,
): boolean {
  if (!latestTurn?.startedAt) return false;
  if (!latestTurn.completedAt) return false;
  if (!session) return true;
  return session.status !== "running";
}

/**
 * Resolves the user-facing status of a thread, in priority order. Returns
 * `null` for quiescent threads so rows stay free of "Idle"-style noise.
 * Mirrors `resolveThreadStatusPill` in apps/web/src/components/Sidebar.logic.ts.
 */
export function resolveThreadStatus(
  thread: EnvironmentThreadShell,
): ThreadStatusPresentation | null {
  if (thread.hasPendingApprovals) {
    return {
      kind: "pending-approval",
      label: "Needs Approval",
      pillClassName: "bg-warning",
      textClassName: "text-warning-foreground",
      pulse: false,
    };
  }

  if (thread.hasPendingUserInput) {
    return {
      kind: "awaiting-input",
      label: "Awaiting Input",
      pillClassName: "bg-adaptive-indigo-500-a12-a16",
      textClassName: "text-adaptive-indigo-600-300",
      pulse: false,
    };
  }

  if (thread.session?.retrying === true) {
    return {
      kind: "retrying",
      label: "Retrying",
      pillClassName: "bg-amber-500/12 dark:bg-amber-500/16",
      textClassName: "text-amber-700 dark:text-amber-300",
      iconColor: "#ff9f0a",
      iconBackground: "rgba(255,159,10,0.22)",
      pulse: true,
    };
  }

  if (thread.session?.status === "running") {
    return {
      kind: "working",
      label: "Working",
      pillClassName: "bg-adaptive-sky-500-a12-a16",
      textClassName: "text-adaptive-sky-600-400",
      pulse: true,
    };
  }

  if (thread.session?.status === "starting") {
    return {
      kind: "connecting",
      label: "Connecting",
      pillClassName: "bg-adaptive-sky-500-a12-a16",
      textClassName: "text-adaptive-sky-600-400",
      pulse: true,
    };
  }

  if (isThreadInterrupted(thread)) {
    return {
      kind: "interrupted",
      label: "Interrupted",
      pillClassName: "bg-orange-500/12 dark:bg-orange-500/16",
      textClassName: "text-orange-700 dark:text-orange-300",
      pulse: false,
    };
  }

  if (thread.session?.status === "error" || thread.latestTurn?.state === "error") {
    const capacityLimited = classifyThreadFailure(thread.session?.lastError) === "capacity";
    return {
      kind: capacityLimited ? "capacity-limited" : "error",
      label: capacityLimited ? "Capacity Limited" : "Error",
      pillClassName: capacityLimited ? "bg-rose-500/12 dark:bg-rose-500/16" : "bg-danger",
      textClassName: capacityLimited
        ? "text-rose-700 dark:text-rose-300"
        : "text-danger-foreground",
      pulse: false,
    };
  }

  const hasPlanReadyPrompt =
    thread.interactionMode === "plan" &&
    isLatestTurnSettled(thread.latestTurn, thread.session) &&
    thread.hasActionableProposedPlan;
  if (hasPlanReadyPrompt) {
    return {
      kind: "plan-ready",
      label: "Plan Ready",
      pillClassName: "bg-adaptive-violet-500-a12-a16",
      textClassName: "text-adaptive-violet-600-400",
      pulse: false,
    };
  }

  return null;
}
