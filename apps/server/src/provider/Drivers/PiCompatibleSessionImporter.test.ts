import { describe, expect, it } from "@effect/vitest";
import { ProviderDriverKind } from "@t3tools/contracts";

import { parsePiCompatibleSession } from "./PiCompatibleSessionImporter.ts";

const PI = ProviderDriverKind.make("piAgent");
const OMP = ProviderDriverKind.make("omp");

const session = JSON.stringify({
  type: "session",
  version: 3,
  id: "019fe70c-c446-7000-bf0a-907e165a996f",
  timestamp: "2026-08-09T15:03:21.414Z",
  cwd: "/work/project",
});

describe("PiCompatibleSessionImporter", () => {
  it("imports Pi v3 user and assistant text records", () => {
    const parsed = parsePiCompatibleSession(
      [
        session,
        JSON.stringify({
          type: "message",
          id: "user-1",
          timestamp: "2026-08-09T15:04:00.000Z",
          message: { role: "user", content: [{ type: "text", text: "Fix the test" }] },
        }),
        JSON.stringify({
          type: "message",
          id: "assistant-1",
          timestamp: "2026-08-09T15:05:00.000Z",
          message: { role: "assistant", content: [{ type: "text", text: "Fixed." }] },
        }),
      ].join("\n"),
      PI,
      "/tmp/pi.jsonl",
    );

    expect(parsed).toMatchObject({
      id: "019fe70c-c446-7000-bf0a-907e165a996f",
      cwd: "/work/project",
    });
    expect(parsed?.messages.map((message) => [message.role, message.text])).toEqual([
      ["user", "Fix the test"],
      ["assistant", "Fixed."],
    ]);
  });

  it("accepts OMP title records and skips non-text tool records", () => {
    const parsed = parsePiCompatibleSession(
      [
        JSON.stringify({ type: "title", title: "OMP transcript" }),
        session,
        "not json",
        JSON.stringify({
          type: "message",
          id: "tool",
          message: { role: "assistant", content: [{ type: "tool_call", name: "read" }] },
        }),
        JSON.stringify({
          type: "message",
          id: "user-1",
          timestamp: "2026-08-09T15:04:00.000Z",
          message: { role: "user", content: [{ type: "input_text", text: "Continue" }] },
        }),
      ].join("\n"),
      OMP,
      "/tmp/omp.jsonl",
    );

    expect(parsed?.title).toBe("OMP transcript");
    expect(parsed?.messages).toHaveLength(1);
    expect(parsed?.messages[0]?.text).toBe("Continue");
  });
});
