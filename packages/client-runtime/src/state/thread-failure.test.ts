import { describe, expect, it } from "vite-plus/test";

import { classifyThreadFailure } from "./thread-failure.ts";

describe("classifyThreadFailure", () => {
  it.each([
    "At capacity. Please try again later.",
    "Server overloaded",
    "rate_limit_reached",
    "Too many requests (429)",
    "Temporarily unavailable",
  ])("classifies capacity failures: %s", (message) => {
    expect(classifyThreadFailure(message)).toBe("capacity");
  });

  it.each([undefined, null, "The working tree is not clean", "Authentication failed"])(
    "keeps non-capacity failures as errors: %s",
    (message) => {
      expect(classifyThreadFailure(message)).toBe("error");
    },
  );
});
