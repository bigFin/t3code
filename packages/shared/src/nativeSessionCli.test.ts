import { describe, expect, it } from "vite-plus/test";

import { formatNativeSessionCliCommand } from "./nativeSessionCli.ts";

describe("formatNativeSessionCliCommand", () => {
  it("quotes POSIX arguments and preserves the launch directory", () => {
    expect(
      formatNativeSessionCliCommand(
        {
          command: "omp",
          args: ["--resume", "session with spaces", "it's-live"],
          cwd: "/tmp/project path",
        },
        "linux",
      ),
    ).toBe("cd -- '/tmp/project path' && 'omp' '--resume' 'session with spaces' 'it'\\''s-live'");
  });

  it("quotes Windows command arguments and the launch directory", () => {
    expect(
      formatNativeSessionCliCommand(
        { command: "omp.exe", args: ["--resume", "session id"], cwd: "C:\\Project Files" },
        "win32",
      ),
    ).toBe("Set-Location -LiteralPath 'C:\\Project Files'; & 'omp.exe' '--resume' 'session id'");
  });
});
