import type { AgentAwarenessState } from "./agentAwareness.ts";

/**
 * Flux's supplied v2 sprite sheet uses an 8 by 11 grid. Cells are addressed
 * in source pixels so consumers can scale it without resampling the source
 * artwork or touching transparent/unused cells.
 */
export const FLUX_V2_SPRITE_SHEET = {
  assetPath: "/companions/flux-v2.png",
  cellHeight: 208,
  cellWidth: 192,
  columns: 8,
  height: 2_288,
  rows: 11,
  version: 2,
  width: 1_536,
} as const;

export const FLUX_V2_ANIMATIONS = {
  idle: { row: 0, frameCount: 6 },
  "run-right": { row: 1, frameCount: 8 },
  "run-left": { row: 2, frameCount: 8 },
  wave: { row: 3, frameCount: 4 },
  jump: { row: 4, frameCount: 5 },
  failed: { row: 5, frameCount: 8 },
  waiting: { row: 6, frameCount: 6 },
  "active-work": { row: 7, frameCount: 6 },
  review: { row: 8, frameCount: 6 },
} as const;

export type FluxAnimationName = keyof typeof FLUX_V2_ANIMATIONS;

export interface FluxSpriteCell {
  readonly column: number;
  readonly row: number;
}

export function fluxV2AnimationCell(animation: FluxAnimationName, frame: number): FluxSpriteCell {
  const spec = FLUX_V2_ANIMATIONS[animation];
  return {
    column: normalizeFrame(frame, spec.frameCount),
    row: spec.row,
  };
}

/**
 * Rows 10–11 contain one static pointer-facing pose for each of Flux's 16
 * tracking directions. Direction ordering deliberately remains row-major:
 * consumers may map their own pointer coordinate system without modifying the
 * source asset.
 */
export function fluxV2PointerCell(direction: number): FluxSpriteCell {
  const normalizedDirection = normalizeFrame(direction, 16);
  return {
    column: normalizedDirection % FLUX_V2_SPRITE_SHEET.columns,
    row: 9 + Math.floor(normalizedDirection / FLUX_V2_SPRITE_SHEET.columns),
  };
}

function normalizeFrame(frame: number, frameCount: number): number {
  const integerFrame = Number.isFinite(frame) ? Math.trunc(frame) : 0;
  return ((integerFrame % frameCount) + frameCount) % frameCount;
}

export type FluxCompanionActivity = AgentAwarenessState & {
  readonly retrying: boolean;
};

export type FluxCompanionStatus = "attention" | "failed" | "review" | "working" | "idle";

export interface FluxCompanionPresentation {
  readonly activeEnvironmentCount: number;
  readonly activeThreadCount: number;
  readonly animation: FluxAnimationName;
  readonly status: FluxCompanionStatus;
  readonly target: FluxCompanionActivity | null;
}

export const FLUX_COMPLETION_ATTENTION_MS = 15 * 60 * 1_000;

/**
 * Keep Flux's focus actionable and stable across all connected environments:
 * input/approval, failures, recent completions, active work, then rest.
 */
export function selectFluxCompanionPresentation(input: {
  readonly activities: ReadonlyArray<FluxCompanionActivity>;
  /** Epoch milliseconds supplied by the rendering/runtime boundary. */
  readonly now: number;
}): FluxCompanionPresentation {
  const { now } = input;
  const ranked = input.activities
    .map((activity) => ({
      activity,
      priority: fluxActivityPriority(activity, now),
    }))
    .filter((candidate) => candidate.priority > 0)
    .sort(compareFluxCandidates);
  const target = ranked[0]?.activity ?? null;
  const activeEnvironmentCount = new Set(
    input.activities
      .filter((activity) => fluxActivityPriority(activity, now) > 0)
      .map((activity) => activity.environmentId),
  ).size;

  if (target === null) {
    return {
      activeEnvironmentCount,
      activeThreadCount: 0,
      animation: "idle",
      status: "idle",
      target: null,
    };
  }

  return {
    activeEnvironmentCount,
    activeThreadCount: ranked.length,
    animation: animationForActivity(target),
    status: statusForActivity(target, now),
    target,
  };
}

function compareFluxCandidates(
  left: { readonly activity: FluxCompanionActivity; readonly priority: number },
  right: { readonly activity: FluxCompanionActivity; readonly priority: number },
): number {
  if (left.priority !== right.priority) {
    return right.priority - left.priority;
  }

  const updatedAtDifference =
    parseUpdatedAt(right.activity.updatedAt) - parseUpdatedAt(left.activity.updatedAt);
  if (updatedAtDifference !== 0) {
    return updatedAtDifference;
  }

  return activityIdentity(left.activity).localeCompare(activityIdentity(right.activity));
}

function activityIdentity(activity: FluxCompanionActivity): string {
  return `${activity.environmentId}:${activity.threadId}`;
}

function parseUpdatedAt(updatedAt: string): number {
  const parsed = Date.parse(updatedAt);
  return Number.isFinite(parsed) ? parsed : 0;
}

function fluxActivityPriority(activity: FluxCompanionActivity, now: number): number {
  switch (activity.phase) {
    case "waiting_for_approval":
    case "waiting_for_input":
      return 5;
    case "failed":
      return 4;
    case "completed":
      return now - parseUpdatedAt(activity.updatedAt) <= FLUX_COMPLETION_ATTENTION_MS ? 3 : 0;
    case "starting":
    case "running":
      return 2;
    case "stale":
      return 0;
  }
}

function statusForActivity(activity: FluxCompanionActivity, now: number): FluxCompanionStatus {
  switch (activity.phase) {
    case "waiting_for_approval":
    case "waiting_for_input":
      return "attention";
    case "failed":
      return "failed";
    case "completed":
      return now - parseUpdatedAt(activity.updatedAt) <= FLUX_COMPLETION_ATTENTION_MS
        ? "review"
        : "idle";
    case "starting":
    case "running":
      return "working";
    case "stale":
      return "idle";
  }
}

function animationForActivity(activity: FluxCompanionActivity): FluxAnimationName {
  switch (activity.phase) {
    case "waiting_for_approval":
      return "wave";
    case "waiting_for_input":
      return "waiting";
    case "failed":
      return "failed";
    case "completed":
      return "review";
    case "starting":
      return "jump";
    case "running":
      return "active-work";
    case "stale":
      return "idle";
  }
}

export function fluxStatusLabel(status: FluxCompanionStatus): string {
  switch (status) {
    case "attention":
      return "Needs you";
    case "failed":
      return "Needs recovery";
    case "review":
      return "Ready to review";
    case "working":
      return "Working";
    case "idle":
      return "Keeping watch";
  }
}
