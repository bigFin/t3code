import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { APP_VERSION } from "./branding";
import {
  appendVersionMismatchHint,
  buildVersionMismatchDismissalKey,
  dismissVersionMismatch,
  isVersionMismatchDismissed,
  resolveServerConfigVersionMismatch,
  resolveServerSelfUpdateCapability,
  resolveVersionMismatch,
  resolveVersionMismatchDirection,
  serverUpdateGuidance,
  shouldOfferServerUpdate,
  versionMismatchGuidance,
} from "./versionSkew";

describe("versionSkew", () => {
  it("does not warn when versions match", () => {
    expect(resolveVersionMismatch(APP_VERSION)).toBeNull();
  });

  it("returns a mismatch when the server version differs from the client", () => {
    expect(resolveVersionMismatch("9.9.9")).toEqual({
      clientVersion: APP_VERSION,
      serverVersion: "9.9.9",
      direction: "client-older",
      hint: "Version mismatch. Update and relaunch this T3 Code client; the connected server is newer.",
    });
  });

  it("marks an older server as the update target", () => {
    expect(resolveVersionMismatch("0.0.0-rc.1")).toEqual({
      clientVersion: APP_VERSION,
      serverVersion: "0.0.0-rc.1",
      direction: "server-older",
      hint: "Version mismatch. Update the connected server to the client version.",
    });
  });

  it("classifies which side of a semver mismatch is older", () => {
    expect(resolveVersionMismatchDirection("0.0.30", "0.0.31")).toBe("client-older");
    expect(resolveVersionMismatchDirection("0.0.31", "0.0.30")).toBe("server-older");
    expect(resolveVersionMismatchDirection("dev", "0.0.31")).toBe("unknown");
  });

  it("reads the server version from config descriptors", () => {
    expect(
      resolveServerConfigVersionMismatch({
        environment: {
          environmentId: EnvironmentId.make("environment-1"),
          label: "Remote",
          platform: {
            os: "darwin",
            arch: "arm64",
          },
          serverVersion: "9.9.9",
          capabilities: {
            repositoryIdentity: true,
          },
        },
      }),
    ).toMatchObject({
      serverVersion: "9.9.9",
    });
  });

  it("keys dismissals by environment, client version, and server version", () => {
    const environmentId = EnvironmentId.make("environment-dismissal");
    const key = buildVersionMismatchDismissalKey(environmentId, {
      clientVersion: APP_VERSION,
      serverVersion: "9.9.9",
    });

    expect(key).toBe(`${environmentId}:${APP_VERSION}:9.9.9`);
    expect(isVersionMismatchDismissed(key)).toBe(false);

    dismissVersionMismatch(key);

    expect(isVersionMismatchDismissed(key)).toBe(true);
    expect(
      isVersionMismatchDismissed(
        buildVersionMismatchDismissalKey(environmentId, {
          clientVersion: APP_VERSION,
          serverVersion: "9.9.10",
        }),
      ),
    ).toBe(false);
  });

  it("appends a hint to connection errors when versions differ", () => {
    const mismatch = resolveVersionMismatch("9.9.9");

    expect(appendVersionMismatchHint("Socket closed.", mismatch)).toBe(
      "Socket closed. Hint: Version mismatch. Update and relaunch this T3 Code client; the connected server is newer.",
    );
  });

  it("reads desktop-managed update capabilities from config descriptors", () => {
    expect(
      resolveServerSelfUpdateCapability({
        environment: {
          environmentId: EnvironmentId.make("environment-desktop"),
          label: "Desktop",
          platform: { os: "darwin", arch: "arm64" },
          serverVersion: "9.9.9",
          capabilities: {
            repositoryIdentity: true,
            serverSelfUpdate: "desktop-managed",
          },
        },
      }),
    ).toBe("desktop-managed");
    expect(resolveServerSelfUpdateCapability(null)).toBeNull();
  });

  it("matches version-drift guidance to the advertised update path", () => {
    expect(serverUpdateGuidance("respawn", "Remote server")).toBe(
      "Update the Remote server so they stay in sync.",
    );
    expect(serverUpdateGuidance("desktop-managed", "Desktop server")).toBe(
      "Update the desktop app that runs the Desktop server.",
    );
    expect(serverUpdateGuidance(null, "Local server")).toBe(
      "Relaunch the Local server with the copied command to sync them.",
    );
  });

  it("directs updates toward the older side of a mismatch", () => {
    const clientOlder = {
      clientVersion: "0.0.30",
      serverVersion: "0.0.31",
      direction: "client-older" as const,
      hint: "",
    };
    const serverOlder = {
      clientVersion: "0.0.31",
      serverVersion: "0.0.30",
      direction: "server-older" as const,
      hint: "",
    };
    const unknown = {
      clientVersion: "dev-client",
      serverVersion: "dev-server",
      direction: "unknown" as const,
      hint: "",
    };

    expect(versionMismatchGuidance(clientOlder, null, "Kitu server")).toBe(
      "This client is older than the Kitu server. Update and relaunch T3 Code on this device; the newer server will not be downgraded.",
    );
    expect(shouldOfferServerUpdate(clientOlder)).toBe(false);
    expect(versionMismatchGuidance(serverOlder, null, "Kitu server")).toBe(
      "Relaunch the Kitu server with the copied command to sync them.",
    );
    expect(shouldOfferServerUpdate(serverOlder)).toBe(true);
    expect(versionMismatchGuidance(unknown, null, "Kitu server")).toBe(
      "Sync this client and the Kitu server to the same T3 Code version.",
    );
    expect(shouldOfferServerUpdate(unknown)).toBe(false);
  });
});
