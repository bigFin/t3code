import type { RemoteT3RunnerOptions } from "@t3tools/ssh/tunnel";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";

import serverPackageJson from "../../../server/package.json" with { type: "json" };
import type * as DesktopEnvironment from "../app/DesktopEnvironment.ts";

const findBundledServerPackage = Effect.fn("desktop.sshRunner.findBundledServerPackage")(function* (
  environment: DesktopEnvironment.DesktopEnvironment["Service"],
) {
  const configured = Option.getOrUndefined(environment.remoteT3PackageArchivePath);
  if (configured !== undefined) {
    return configured;
  }

  const fs = yield* FileSystem.FileSystem;
  const candidates = environment.isPackaged
    ? [
        environment.path.join(environment.resourcesPath, "t3-server.tgz"),
        environment.path.join(environment.appRoot, "apps/server/t3-server.tgz"),
      ]
    : [environment.path.join(environment.appRoot, "apps/server/t3-server.tgz")];

  for (const candidate of candidates) {
    if (yield* fs.exists(candidate).pipe(Effect.orElseSucceed(() => false))) {
      return candidate;
    }
  }

  return undefined;
});

export const resolveDesktopSshCliRunner = Effect.fn("desktop.sshRunner.resolve")(function* (
  environment: DesktopEnvironment.DesktopEnvironment["Service"],
): Effect.fn.Return<RemoteT3RunnerOptions, never, FileSystem.FileSystem> {
  const devRemoteEntryPath = Option.getOrUndefined(environment.devRemoteT3ServerEntryPath);
  if (environment.isDevelopment && devRemoteEntryPath !== undefined) {
    return {
      nodeScriptPath: devRemoteEntryPath,
      nodeEngineRange: serverPackageJson.engines.node,
      requireExactBuild: true,
    };
  }

  const localPackageArchivePath = yield* findBundledServerPackage(environment);
  return {
    ...(localPackageArchivePath === undefined ? {} : { localPackageArchivePath }),
    nodeEngineRange: serverPackageJson.engines.node,
    requireExactBuild: true,
    version: environment.appVersion,
  };
});
