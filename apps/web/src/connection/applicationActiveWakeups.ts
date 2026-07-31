export const APPLICATION_ACTIVE_PROBE_INTERVAL_MS = 60_000;

export type ApplicationActiveWakeup = "application-active" | "connection-watchdog-probe";

export interface ApplicationActiveWakeupHost {
  readonly isVisible: () => boolean;
  readonly addVisibilityChangeListener: (listener: () => void) => void;
  readonly removeVisibilityChangeListener: (listener: () => void) => void;
  readonly setInterval: (listener: () => void, intervalMs: number) => number;
  readonly clearInterval: (intervalId: number) => void;
}

export function startApplicationActiveWakeups(
  host: ApplicationActiveWakeupHost,
  emit: (wakeup: ApplicationActiveWakeup) => void,
): () => void {
  let probeIntervalId: number | undefined;

  const stopProbeInterval = () => {
    if (probeIntervalId === undefined) {
      return;
    }
    host.clearInterval(probeIntervalId);
    probeIntervalId = undefined;
  };

  const startProbeInterval = () => {
    if (probeIntervalId !== undefined || !host.isVisible()) {
      return;
    }
    probeIntervalId = host.setInterval(() => {
      if (!host.isVisible()) {
        stopProbeInterval();
        return;
      }
      emit("connection-watchdog-probe");
    }, APPLICATION_ACTIVE_PROBE_INTERVAL_MS);
  };

  const onVisibilityChange = () => {
    if (!host.isVisible()) {
      stopProbeInterval();
      return;
    }
    emit("application-active");
    startProbeInterval();
  };

  host.addVisibilityChangeListener(onVisibilityChange);
  startProbeInterval();

  return () => {
    stopProbeInterval();
    host.removeVisibilityChangeListener(onVisibilityChange);
  };
}
