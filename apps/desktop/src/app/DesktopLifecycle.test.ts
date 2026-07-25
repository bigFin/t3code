import { assert, describe, it } from "@effect/vitest";

import { shouldQuitAfterLastWindowCloses } from "./DesktopLifecycle.ts";

describe("DesktopLifecycle", () => {
  it("keeps Linux alive after the last window closes", () => {
    assert.equal(shouldQuitAfterLastWindowCloses("linux"), false);
  });

  it("keeps macOS alive and preserves Windows last-window quit behavior", () => {
    assert.equal(shouldQuitAfterLastWindowCloses("darwin"), false);
    assert.equal(shouldQuitAfterLastWindowCloses("win32"), true);
  });
});
