import type { NativeSessionCliLaunch } from "@t3tools/contracts";

const quotePosix = (value: string): string => `'${value.replaceAll("'", `'\\''`)}'`;
const quotePowerShell = (value: string): string => `'${value.replaceAll("'", "''")}'`;

export function formatNativeSessionCliCommand(
  launch: NativeSessionCliLaunch,
  platform: "win32" | "darwin" | "linux",
): string {
  if (platform === "win32") {
    const parts = [
      ...(launch.cwd ? [`Set-Location -LiteralPath ${quotePowerShell(launch.cwd)}`] : []),
      ...Object.entries(launch.env ?? {}).map(
        ([name, value]) => `$env:${name}=${quotePowerShell(value)}`,
      ),
      `& ${[launch.command, ...launch.args].map(quotePowerShell).join(" ")}`,
    ];
    return parts.join("; ");
  }

  const command = [
    ...Object.entries(launch.env ?? {}).map(([name, value]) => `${name}=${quotePosix(value)}`),
    quotePosix(launch.command),
    ...launch.args.map(quotePosix),
  ].join(" ");
  return launch.cwd ? `cd -- ${quotePosix(launch.cwd)} && ${command}` : command;
}
