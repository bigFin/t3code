import { describe, expect, it } from "@effect/vitest";

import { isApplicationActiveWakeup, shouldResubscribeAfterWakeup } from "./wakeups.ts";

describe("connection wakeups", () => {
  it("uses watchdog probes for connection health without restarting data subscriptions", () => {
    expect(isApplicationActiveWakeup("connection-watchdog-probe")).toBe(true);
    expect(shouldResubscribeAfterWakeup("connection-watchdog-probe")).toBe(false);
  });
});
