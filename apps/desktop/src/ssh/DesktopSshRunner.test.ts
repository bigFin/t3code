import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import type * as DesktopEnvironment from "../app/DesktopEnvironment.ts";
import { resolveDesktopSshCliRunner } from "./DesktopSshRunner.ts";

const makeEnvironment = (
  path: Path.Path,
  overrides: Partial<DesktopEnvironment.DesktopEnvironment["Service"]> = {},
): DesktopEnvironment.DesktopEnvironment["Service"] =>
  ({
    path,
    appVersion: "0.0.31",
    appRoot: "/opt/t3code",
    resourcesPath: "/opt/t3code/resources",
    isDevelopment: false,
    isPackaged: false,
    devRemoteT3ServerEntryPath: Option.none(),
    remoteT3PackageArchivePath: Option.none(),
    ...overrides,
  }) as DesktopEnvironment.DesktopEnvironment["Service"];

describe("DesktopSshRunner", () => {
  it.effect("uses the exact packaged server archive instead of an npm version match", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const resourcesPath = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-desktop-ssh-runner-",
      });
      const archivePath = path.join(resourcesPath, "t3-server.tgz");
      yield* fs.writeFile(archivePath, Uint8Array.from([1, 2, 3]));

      const runner = yield* resolveDesktopSshCliRunner(
        makeEnvironment(path, {
          appRoot: path.join(resourcesPath, "app.asar"),
          resourcesPath,
          isPackaged: true,
        }),
      );

      assert.equal(runner.localPackageArchivePath, archivePath);
      assert.equal(runner.requireExactBuild, true);
      assert.notProperty(runner, "packageSpec");
      assert.equal(runner.version, "0.0.31");
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("discovers a matching archive beside an unpackaged controlling server", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const appRoot = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-desktop-ssh-runner-",
      });
      const serverDir = path.join(appRoot, "apps/server");
      const archivePath = path.join(serverDir, "t3-server.tgz");
      yield* fs.makeDirectory(serverDir, { recursive: true });
      yield* fs.writeFile(archivePath, Uint8Array.from([4, 5, 6]));

      const runner = yield* resolveDesktopSshCliRunner(makeEnvironment(path, { appRoot }));

      assert.equal(runner.localPackageArchivePath, archivePath);
      assert.equal(runner.requireExactBuild, true);
      assert.notProperty(runner, "packageSpec");
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("keeps an explicit archive override authoritative", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const runner = yield* resolveDesktopSshCliRunner(
        makeEnvironment(path, {
          remoteT3PackageArchivePath: Option.some("/custom/t3-server.tgz"),
        }),
      );

      assert.equal(runner.localPackageArchivePath, "/custom/t3-server.tgz");
      assert.equal(runner.requireExactBuild, true);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("fails closed instead of resolving a same-version public package", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const runner = yield* resolveDesktopSshCliRunner(
        makeEnvironment(path, {
          appRoot: "/missing/t3code",
        }),
      );

      assert.notProperty(runner, "localPackageArchivePath");
      assert.notProperty(runner, "packageSpec");
      assert.equal(runner.requireExactBuild, true);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps the explicit development server path exact", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const runner = yield* resolveDesktopSshCliRunner(
        makeEnvironment(path, {
          devRemoteT3ServerEntryPath: Option.some("/remote/t3/dist/bin.mjs"),
          isDevelopment: true,
        }),
      );

      assert.equal(runner.nodeScriptPath, "/remote/t3/dist/bin.mjs");
      assert.equal(runner.requireExactBuild, true);
      assert.notProperty(runner, "packageSpec");
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
