import { describe, expect, it } from "@effect/vitest";

import {
  APPLICATION_ACTIVE_PROBE_INTERVAL_MS,
  type ApplicationActiveWakeup,
  type ApplicationActiveWakeupHost,
  startApplicationActiveWakeups,
} from "./applicationActiveWakeups";

function makeHost(initiallyVisible: boolean) {
  let visible = initiallyVisible;
  let visibilityChangeListener: (() => void) | undefined;
  let nextIntervalId = 1;
  const intervals = new Map<
    number,
    { readonly listener: () => void; readonly intervalMs: number }
  >();

  const host: ApplicationActiveWakeupHost = {
    isVisible: () => visible,
    addVisibilityChangeListener: (listener) => {
      visibilityChangeListener = listener;
    },
    removeVisibilityChangeListener: (listener) => {
      if (visibilityChangeListener === listener) {
        visibilityChangeListener = undefined;
      }
    },
    setInterval: (listener, intervalMs) => {
      const intervalId = nextIntervalId++;
      intervals.set(intervalId, { listener, intervalMs });
      return intervalId;
    },
    clearInterval: (intervalId) => {
      intervals.delete(intervalId);
    },
  };

  return {
    host,
    intervals,
    setVisible(nextVisible: boolean) {
      visible = nextVisible;
      visibilityChangeListener?.();
    },
    fireProbeInterval() {
      for (const interval of intervals.values()) {
        interval.listener();
      }
    },
    hasVisibilityChangeListener() {
      return visibilityChangeListener !== undefined;
    },
  };
}

describe("application active wakeups", () => {
  it("emits periodic probes while the application is visible", () => {
    const harness = makeHost(true);
    const wakeups: ApplicationActiveWakeup[] = [];

    const cleanup = startApplicationActiveWakeups(harness.host, (wakeup) => {
      wakeups.push(wakeup);
    });

    expect([...harness.intervals.values()].map((interval) => interval.intervalMs)).toEqual([
      APPLICATION_ACTIVE_PROBE_INTERVAL_MS,
    ]);
    expect(wakeups).toEqual([]);

    harness.fireProbeInterval();
    harness.fireProbeInterval();

    expect(wakeups).toEqual(["connection-watchdog-probe", "connection-watchdog-probe"]);
    cleanup();
  });

  it("pauses probes while hidden and restarts them after application activation", () => {
    const harness = makeHost(false);
    const wakeups: ApplicationActiveWakeup[] = [];

    const cleanup = startApplicationActiveWakeups(harness.host, (wakeup) => {
      wakeups.push(wakeup);
    });

    expect(harness.intervals.size).toBe(0);

    harness.setVisible(true);
    expect(wakeups).toEqual(["application-active"]);
    expect(harness.intervals.size).toBe(1);

    harness.fireProbeInterval();
    expect(wakeups).toEqual(["application-active", "connection-watchdog-probe"]);

    const staleProbeInterval = [...harness.intervals.values()][0]?.listener;
    expect(staleProbeInterval).toBeDefined();
    harness.setVisible(false);
    expect(harness.intervals.size).toBe(0);
    staleProbeInterval?.();
    harness.fireProbeInterval();
    expect(wakeups).toEqual(["application-active", "connection-watchdog-probe"]);

    cleanup();
  });

  it("cleans up the probe interval and visibility listener", () => {
    const harness = makeHost(true);
    const wakeups: ApplicationActiveWakeup[] = [];
    const cleanup = startApplicationActiveWakeups(harness.host, (wakeup) => {
      wakeups.push(wakeup);
    });

    expect(harness.intervals.size).toBe(1);
    expect(harness.hasVisibilityChangeListener()).toBe(true);

    cleanup();

    expect(harness.intervals.size).toBe(0);
    expect(harness.hasVisibilityChangeListener()).toBe(false);
    harness.setVisible(false);
    harness.setVisible(true);
    harness.fireProbeInterval();
    expect(wakeups).toEqual([]);
  });
});
