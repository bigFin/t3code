import { useEffect, useMemo, useState, type CSSProperties } from "react";
import { useNavigate } from "@tanstack/react-router";
import { XIcon } from "lucide-react";
import {
  FLUX_V2_ANIMATIONS,
  FLUX_V2_SPRITE_SHEET,
  fluxStatusLabel,
  fluxV2AnimationCell,
  selectFluxCompanionPresentation,
} from "@t3tools/shared/fluxCompanion";
import { projectThreadAwareness } from "@t3tools/shared/agentAwareness";

import { useMediaQuery } from "~/hooks/useMediaQuery";
import { useClientSettings, useUpdateClientSettings } from "~/hooks/useSettings";
import { useEnvironments } from "~/state/environments";
import { useProjects, useThreadShells } from "~/state/entities";
import { cn } from "~/lib/utils";

const FLUX_SPRITE_SCALE = 0.75;
const FLUX_FRAME_INTERVAL_MS = 220;
const FLUX_COMPLETION_CLOCK_INTERVAL_MS = 60_000;
const FLUX_SPRITE_URL = `${import.meta.env.BASE_URL}companions/flux-v2.png`;

function currentEpochMilliseconds(): number {
  return typeof performance === "undefined" ? 0 : performance.timeOrigin + performance.now();
}

function usePageVisible(): boolean {
  const [visible, setVisible] = useState(() =>
    typeof document === "undefined" ? false : document.visibilityState !== "hidden",
  );

  useEffect(() => {
    const updateVisibility = () => setVisible(document.visibilityState !== "hidden");
    document.addEventListener("visibilitychange", updateVisibility);
    return () => document.removeEventListener("visibilitychange", updateVisibility);
  }, []);

  return visible;
}

function useFluxClock(enabled: boolean, pageVisible: boolean): number {
  const [now, setNow] = useState(currentEpochMilliseconds);

  useEffect(() => {
    if (!enabled || !pageVisible) {
      return;
    }
    const interval = window.setInterval(
      () => setNow(currentEpochMilliseconds()),
      FLUX_COMPLETION_CLOCK_INTERVAL_MS,
    );
    return () => window.clearInterval(interval);
  }, [enabled, pageVisible]);

  return now;
}

function FluxSprite({
  animation,
  frame,
}: {
  readonly animation: keyof typeof FLUX_V2_ANIMATIONS;
  readonly frame: number;
}) {
  const cell = fluxV2AnimationCell(animation, frame);
  const width = FLUX_V2_SPRITE_SHEET.cellWidth * FLUX_SPRITE_SCALE;
  const height = FLUX_V2_SPRITE_SHEET.cellHeight * FLUX_SPRITE_SCALE;
  const style = {
    backgroundImage: `url("${FLUX_SPRITE_URL}")`,
    backgroundPosition: `${-cell.column * width}px ${-cell.row * height}px`,
    backgroundRepeat: "no-repeat",
    backgroundSize: `${FLUX_V2_SPRITE_SHEET.width * FLUX_SPRITE_SCALE}px ${
      FLUX_V2_SPRITE_SHEET.height * FLUX_SPRITE_SCALE
    }px`,
    height,
    width,
  } satisfies CSSProperties;

  return <span aria-hidden className="block shrink-0" style={style} />;
}

function pluralize(count: number, noun: string): string {
  return `${count} ${noun}${count === 1 ? "" : "s"}`;
}

export function FluxCompanion() {
  const navigate = useNavigate();
  const enabled = useClientSettings((settings) => settings.companionEnabled);
  const updateClientSettings = useUpdateClientSettings();
  const reducedMotion = useMediaQuery("(prefers-reduced-motion: reduce)");
  const finePointer = useMediaQuery({ pointer: "fine" });
  const pageVisible = usePageVisible();
  const now = useFluxClock(enabled, pageVisible);
  const { environments } = useEnvironments();
  const projects = useProjects();
  const threads = useThreadShells();
  const [frame, setFrame] = useState(0);

  const connectedEnvironmentIds = useMemo(
    () =>
      new Set(
        environments
          .filter((environment) => environment.connection.phase === "connected")
          .map((environment) => environment.environmentId),
      ),
    [environments],
  );
  const environmentLabelById = useMemo(
    () =>
      new Map(environments.map((environment) => [environment.environmentId, environment.label])),
    [environments],
  );
  const projectTitleByKey = useMemo(
    () =>
      new Map(
        projects.map(
          (project) => [`${project.environmentId}:${project.id}`, project.title] as const,
        ),
      ),
    [projects],
  );
  const activities = useMemo(
    () =>
      threads.flatMap((thread) => {
        if (!connectedEnvironmentIds.has(thread.environmentId)) {
          return [];
        }
        const awareness = projectThreadAwareness({
          environmentId: thread.environmentId,
          project: {
            title:
              projectTitleByKey.get(`${thread.environmentId}:${thread.projectId}`) ??
              "Untitled project",
          },
          thread,
        });
        return awareness === null
          ? []
          : [
              {
                ...awareness,
                retrying: thread.session?.retrying === true,
              },
            ];
      }),
    [connectedEnvironmentIds, projectTitleByKey, threads],
  );
  const presentation = useMemo(
    () => selectFluxCompanionPresentation({ activities, now }),
    [activities, now],
  );
  const frameCount = FLUX_V2_ANIMATIONS[presentation.animation].frameCount;
  const target = presentation.target;
  const targetEnvironmentLabel = target
    ? (environmentLabelById.get(target.environmentId) ?? target.environmentId)
    : null;
  const statusLabel =
    target?.retrying === true && presentation.status === "working"
      ? "Retrying"
      : fluxStatusLabel(presentation.status);
  const monitoredLabel = presentation.activeEnvironmentCount
    ? `${pluralize(presentation.activeThreadCount, "active thread")} across ${pluralize(
        presentation.activeEnvironmentCount,
        "host",
      )}`
    : `Watching ${pluralize(connectedEnvironmentIds.size, "connected host")}`;

  useEffect(() => {
    if (reducedMotion || !pageVisible) {
      setFrame(0);
      return;
    }
    const interval = window.setInterval(
      () => setFrame((current) => (current + 1) % frameCount),
      FLUX_FRAME_INTERVAL_MS,
    );
    return () => window.clearInterval(interval);
  }, [frameCount, pageVisible, reducedMotion]);

  if (!enabled || !finePointer) {
    return null;
  }

  const openTarget = () => {
    if (target === null) {
      return;
    }
    void navigate({
      to: "/$environmentId/$threadId",
      params: {
        environmentId: target.environmentId,
        threadId: target.threadId,
      },
    });
  };

  const targetDescription =
    target === null
      ? monitoredLabel
      : `${targetEnvironmentLabel} · ${target.projectTitle} · ${target.threadTitle}`;

  return (
    <aside
      aria-label="Flux companion"
      className="pointer-events-none fixed right-3 bottom-3 z-50 hidden max-w-[min(26rem,calc(100vw-1.5rem))] lg:block"
    >
      <div className="group relative flex items-end gap-1.5">
        <button
          type="button"
          className={cn(
            "pointer-events-auto flex items-end gap-1 rounded-2xl text-left outline-none",
            target !== null
              ? "cursor-pointer focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background"
              : "cursor-default",
          )}
          onClick={openTarget}
          title={target === null ? "Flux is keeping watch" : `Open ${targetDescription}`}
        >
          <div className="relative drop-shadow-[0_8px_14px_rgba(0,0,0,0.35)]">
            <FluxSprite animation={presentation.animation} frame={reducedMotion ? 0 : frame} />
          </div>
          <div className="mb-3 max-w-52 rounded-xl border border-border/70 bg-popover/92 px-3 py-2 shadow-lg shadow-black/12 backdrop-blur-md">
            <div className="flex items-center gap-2">
              <span className="font-mono text-[10px] font-semibold tracking-[0.16em] text-emerald-600 uppercase dark:text-emerald-300">
                Flux
              </span>
              <span className="truncate text-xs font-medium text-foreground">{statusLabel}</span>
            </div>
            <p
              className="mt-0.5 truncate text-[11px] text-muted-foreground"
              title={targetDescription}
            >
              {targetDescription}
            </p>
            <p className="mt-1 text-[10px] text-muted-foreground/80">{monitoredLabel}</p>
          </div>
        </button>
        <button
          type="button"
          aria-label="Hide Flux"
          className="pointer-events-auto absolute -top-1 right-0 inline-flex size-6 cursor-pointer items-center justify-center rounded-full border border-border/70 bg-popover/92 text-muted-foreground opacity-0 shadow-sm outline-none transition-opacity hover:text-foreground focus-visible:opacity-100 focus-visible:ring-2 focus-visible:ring-ring group-hover:opacity-100"
          onClick={() => updateClientSettings({ companionEnabled: false })}
          title="Hide Flux"
        >
          <XIcon className="size-3.5" />
        </button>
      </div>
    </aside>
  );
}
