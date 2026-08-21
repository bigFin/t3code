import { describe, expect, it } from "@effect/vitest";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";

import {
  FLUX_V2_ANIMATIONS,
  FLUX_V2_SPRITE_SHEET,
  fluxV2AnimationCell,
  fluxV2PointerCell,
  selectFluxCompanionPresentation,
  type FluxCompanionActivity,
} from "./fluxCompanion.ts";

const NOW = 1_785_412_800_000;
const NOW_ISO = "2026-07-30T12:00:00.000Z";
const FIVE_SECONDS_AFTER_NOW_ISO = "2026-07-30T12:00:05.000Z";
const FIVE_SECONDS_BEFORE_NOW_ISO = "2026-07-30T11:59:55.000Z";
const RECENT_COMPLETION_ISO = "2026-07-30T11:45:00.001Z";
const STALE_COMPLETION_ISO = "2026-07-30T11:44:59.999Z";

function activity(overrides: Partial<FluxCompanionActivity> = {}): FluxCompanionActivity {
  return {
    environmentId: "topo" as EnvironmentId,
    threadId: "thread-1" as ThreadId,
    projectTitle: "t3code",
    threadTitle: "Ship Flux",
    phase: "running",
    headline: "Agent is working",
    modelTitle: "gpt-5.6",
    updatedAt: NOW_ISO,
    deepLink: "/threads/topo/thread-1",
    retrying: false,
    ...overrides,
  };
}

describe("Flux v2 companion", () => {
  it("maps animation frames and pointer directions inside the supplied 8 by 11 grid", () => {
    expect(FLUX_V2_SPRITE_SHEET).toMatchObject({
      columns: 8,
      rows: 11,
      cellWidth: 192,
      cellHeight: 208,
      width: 1536,
      height: 2288,
      version: 2,
    });
    expect(FLUX_V2_ANIMATIONS["active-work"]).toEqual({ row: 7, frameCount: 6 });
    expect(fluxV2AnimationCell("review", 7)).toEqual({ column: 1, row: 8 });
    expect(fluxV2PointerCell(0)).toEqual({ column: 0, row: 9 });
    expect(fluxV2PointerCell(15)).toEqual({ column: 7, row: 10 });
    expect(fluxV2PointerCell(16)).toEqual({ column: 0, row: 9 });
  });

  it("prioritizes an approval over failures, review work, and active work across hosts", () => {
    const presentation = selectFluxCompanionPresentation({
      now: NOW,
      activities: [
        activity({
          environmentId: "sika" as EnvironmentId,
          threadId: "failed-thread" as ThreadId,
          phase: "failed",
          updatedAt: FIVE_SECONDS_AFTER_NOW_ISO,
        }),
        activity({
          environmentId: "kitu" as EnvironmentId,
          threadId: "approval-thread" as ThreadId,
          phase: "waiting_for_approval",
          updatedAt: FIVE_SECONDS_BEFORE_NOW_ISO,
        }),
        activity({
          environmentId: "topo" as EnvironmentId,
          threadId: "working-thread" as ThreadId,
        }),
      ],
    });

    expect(presentation).toMatchObject({
      status: "attention",
      animation: "wave",
      activeEnvironmentCount: 3,
      activeThreadCount: 3,
      target: {
        environmentId: "kitu",
        threadId: "approval-thread",
      },
    });
  });

  it("shows recent completions for review before active work, then lets Flux rest", () => {
    const recentCompletion = activity({
      environmentId: "sika" as EnvironmentId,
      threadId: "done-thread" as ThreadId,
      phase: "completed",
      updatedAt: RECENT_COMPLETION_ISO,
    });

    expect(
      selectFluxCompanionPresentation({
        now: NOW,
        activities: [activity(), recentCompletion],
      }),
    ).toMatchObject({
      status: "review",
      animation: "review",
      target: { threadId: "done-thread" },
    });

    expect(
      selectFluxCompanionPresentation({
        now: NOW,
        activities: [
          {
            ...recentCompletion,
            updatedAt: STALE_COMPLETION_ISO,
          },
        ],
      }),
    ).toEqual({
      activeEnvironmentCount: 0,
      activeThreadCount: 0,
      animation: "idle",
      status: "idle",
      target: null,
    });
  });

  it("uses deterministic recency and identity tie breakers for matching priority", () => {
    const presentation = selectFluxCompanionPresentation({
      now: NOW,
      activities: [
        activity({
          environmentId: "sika" as EnvironmentId,
          threadId: "thread-b" as ThreadId,
          updatedAt: "not-a-date",
        }),
        activity({
          environmentId: "kitu" as EnvironmentId,
          threadId: "thread-a" as ThreadId,
          updatedAt: "not-a-date",
        }),
      ],
    });

    expect(presentation.target).toMatchObject({
      environmentId: "kitu",
      threadId: "thread-a",
    });
  });
});
