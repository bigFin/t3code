import type { EnvironmentId, ServerConfig, ServerSelfUpdateCapability } from "@t3tools/contracts";
import type { ServerUpdateState } from "@t3tools/client-runtime/state/server";
import { compareSemverVersions, parseSemver } from "@t3tools/shared/semver";
import * as Schema from "effect/Schema";

import { APP_VERSION } from "./branding";
import { getLocalStorageItem, setLocalStorageItem } from "./hooks/useLocalStorage";

export interface VersionMismatch {
  readonly clientVersion: string;
  readonly serverVersion: string;
  readonly direction: VersionMismatchDirection;
  readonly hint: string;
}

export type VersionMismatchDirection = "client-older" | "server-older" | "unknown";

export const VERSION_MISMATCH_DISMISSALS_STORAGE_KEY = "t3code:version-mismatch-dismissals:v1";

// Runtime failures retain their identity until the next attempt. Dismiss only
// that attempt, across chat remounts, without clearing the error in Settings.
const dismissedServerUpdateFailures = new WeakSet<ServerUpdateState>();

export function isServerUpdateFailureDismissed(state: ServerUpdateState): boolean {
  return state.status === "failed" && dismissedServerUpdateFailures.has(state);
}

export function dismissServerUpdateFailure(state: ServerUpdateState): void {
  if (state.status === "failed") dismissedServerUpdateFailures.add(state);
}

const VersionMismatchDismissalsSchema = Schema.Struct({
  keys: Schema.Array(Schema.String),
});

type VersionMismatchDismissals = typeof VersionMismatchDismissalsSchema.Type;

function normalizeVersion(version: string | null | undefined): string | null {
  const trimmed = version?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : null;
}

export function resolveVersionMismatchDirection(
  clientVersion: string,
  serverVersion: string,
): VersionMismatchDirection {
  if (!parseSemver(clientVersion) || !parseSemver(serverVersion)) {
    return "unknown";
  }
  const comparison = compareSemverVersions(clientVersion, serverVersion);
  if (comparison < 0) {
    return "client-older";
  }
  if (comparison > 0) {
    return "server-older";
  }
  return "unknown";
}

function versionMismatchHint(direction: VersionMismatchDirection): string {
  switch (direction) {
    case "client-older":
      return "Version mismatch. Update and relaunch this T3 Code client; the connected server is newer.";
    case "server-older":
      return "Version mismatch. Update the connected server to the client version.";
    case "unknown":
      return "Version mismatch. Sync the client and server to the same T3 Code version.";
  }
}

/** Core `major.minor.patch`, dropping any prerelease or build suffix. */
function versionCore(version: string): string {
  return version.replace(/[-+].*$/, "");
}

/**
 * The skew a user can act on: the connected server runs an older T3 Code than
 * this client, so the server is the side that needs updating.
 *
 * Two nightly builds compare their full versions, including the date and run.
 * Other combinations compare their core `major.minor.patch` only, so a stable
 * build and a nightly build with the same core do not cause an update warning.
 * A server ahead of the client does not need an update. Versions that do not
 * parse as semver fall back to plain string inequality.
 */
export function resolveVersionMismatch(
  serverVersion: string | null | undefined,
): VersionMismatch | null {
  const normalizedClientVersion = normalizeVersion(APP_VERSION);
  const normalizedServerVersion = normalizeVersion(serverVersion);
  if (!normalizedClientVersion || !normalizedServerVersion) {
    return null;
  }

  const clientCore = versionCore(normalizedClientVersion);
  const serverCore = versionCore(normalizedServerVersion);
  const compareNightlyBuilds =
    parseSemver(normalizedClientVersion)?.prerelease[0] === "nightly" &&
    parseSemver(normalizedServerVersion)?.prerelease[0] === "nightly";
  const serverIsBehind =
    parseSemver(clientCore) && parseSemver(serverCore)
      ? compareSemverVersions(
          compareNightlyBuilds ? normalizedServerVersion : serverCore,
          compareNightlyBuilds ? normalizedClientVersion : clientCore,
        ) < 0
      : normalizedServerVersion !== normalizedClientVersion;
  if (!serverIsBehind) {
    return null;
  }

  const direction = resolveVersionMismatchDirection(
    normalizedClientVersion,
    normalizedServerVersion,
  );
  return {
    clientVersion: normalizedClientVersion,
    serverVersion: normalizedServerVersion,
    direction,
    hint: versionMismatchHint(direction),
  };
}

export function resolveServerConfigVersionMismatch(
  serverConfig: Pick<ServerConfig, "environment"> | null | undefined,
): VersionMismatch | null {
  return resolveVersionMismatch(serverConfig?.environment.serverVersion);
}

/** The update path the connected server offers, or null when it only
    supports a manual relaunch (older servers, dev checkouts, Windows). */
export function resolveServerSelfUpdateCapability(
  serverConfig: Pick<ServerConfig, "environment"> | null | undefined,
): ServerSelfUpdateCapability | null {
  return serverConfig?.environment.capabilities.serverSelfUpdate ?? null;
}

/** True when the desktop app supervising this server can be told to update
    itself over RPC. Older desktop servers only get the manual instruction. */
export function supportsDesktopAppUpdate(
  serverConfig: Pick<ServerConfig, "environment"> | null | undefined,
): boolean {
  return serverConfig?.environment.capabilities.desktopAppUpdate === true;
}

/** True when the connected server can recover opted-in running turns after
    its self-update restart. */
export function supportsServerUpdateThreadContinuation(
  serverConfig: Pick<ServerConfig, "environment"> | null | undefined,
): boolean {
  return serverConfig?.environment.capabilities.serverUpdateThreadContinuation === true;
}

/** The command to hand users whose server cannot update itself. */
export function manualServerUpdateCommand(targetVersion: string): string {
  return `npx t3@${targetVersion}`;
}

export function serverUpdateGuidance(
  capability: ServerSelfUpdateCapability | null,
  serverLabel?: string,
): string {
  if (serverLabel === undefined) {
    return capability === "desktop-managed" ? "Update the desktop app" : "Update to stay in sync";
  }
  switch (capability) {
    case "boot-service":
    case "respawn":
      return `Update the ${serverLabel} so they stay in sync.`;
    case "desktop-managed":
      return `Update the desktop app that runs the ${serverLabel}.`;
    default:
      return `Relaunch the ${serverLabel} with the copied command to sync them.`;
  }
}

export function versionMismatchGuidance(
  mismatch: VersionMismatch,
  capability: ServerSelfUpdateCapability | null,
  serverLabel: string,
): string {
  switch (mismatch.direction) {
    case "client-older":
      return `This client is older than the ${serverLabel}. Update and relaunch T3 Code on this device; the newer server will not be downgraded.`;
    case "server-older":
      return serverUpdateGuidance(capability, serverLabel);
    case "unknown":
      return `Sync this client and the ${serverLabel} to the same T3 Code version.`;
  }
}

export function shouldOfferServerUpdate(mismatch: VersionMismatch): boolean {
  return mismatch.direction === "server-older";
}

export function buildVersionMismatchDismissalKey(
  environmentId: EnvironmentId,
  mismatch: Pick<VersionMismatch, "clientVersion" | "serverVersion">,
): string {
  return `${environmentId}:${mismatch.clientVersion}:${mismatch.serverVersion}`;
}

function readVersionMismatchDismissals(): VersionMismatchDismissals {
  try {
    return (
      getLocalStorageItem(
        VERSION_MISMATCH_DISMISSALS_STORAGE_KEY,
        VersionMismatchDismissalsSchema,
      ) ?? { keys: [] }
    );
  } catch (error) {
    console.error("Could not read version-mismatch dismissals.", error);
    return { keys: [] };
  }
}

function writeVersionMismatchDismissals(document: VersionMismatchDismissals): void {
  try {
    setLocalStorageItem(
      VERSION_MISMATCH_DISMISSALS_STORAGE_KEY,
      document,
      VersionMismatchDismissalsSchema,
    );
  } catch (error) {
    console.error("Could not persist version-mismatch dismissals.", error);
  }
}

export function isVersionMismatchDismissed(dismissalKey: string | null | undefined): boolean {
  if (!dismissalKey) {
    return false;
  }
  return readVersionMismatchDismissals().keys.includes(dismissalKey);
}

export function dismissVersionMismatch(dismissalKey: string | null | undefined): void {
  if (!dismissalKey) {
    return;
  }
  const document = readVersionMismatchDismissals();
  if (document.keys.includes(dismissalKey)) {
    return;
  }
  writeVersionMismatchDismissals({
    keys: [...document.keys, dismissalKey],
  });
}
