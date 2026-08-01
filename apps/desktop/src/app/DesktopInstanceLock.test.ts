import { assert, describe, it, vi } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as ElectronApp from "../electron/ElectronApp.ts";
import * as DesktopEnvironment from "./DesktopEnvironment.ts";
import * as DesktopInstanceLock from "./DesktopInstanceLock.ts";

const appPath = "/nix/store/current-t3code/apps/desktop";

const makeInstanceLockLayer = (options: {
  readonly events: Array<unknown>;
  readonly lockAcquired?: boolean;
}) => {
  const electronAppLayer = Layer.succeed(ElectronApp.ElectronApp, {
    metadata: Effect.succeed({
      appVersion: "1.2.3",
      appPath,
      isPackaged: true,
      resourcesPath: "/nix/store/current-t3code/resources",
      runningUnderArm64Translation: false,
    }),
    name: Effect.succeed("T3 Code"),
    whenReady: Effect.void,
    quit: Effect.sync(() => {
      options.events.push("quit");
    }),
    exit: () => Effect.void,
    relaunch: () => Effect.void,
    setPath: (name, path) =>
      Effect.sync(() => {
        options.events.push(`setPath:${name}:${path}`);
      }),
    setName: () => Effect.void,
    setAboutPanelOptions: () => Effect.void,
    setAppUserModelId: () => Effect.void,
    requestSingleInstanceLock: (identity) =>
      Effect.sync(() => {
        options.events.push({ event: "requestLock", identity });
        return options.lockAcquired ?? true;
      }),
    getAppMetrics: Effect.succeed([]),
    isDefaultProtocolClient: () => Effect.succeed(false),
    setAsDefaultProtocolClient: () => Effect.succeed(true),
    setDesktopName: () => Effect.void,
    setDockIcon: () => Effect.void,
    appendCommandLineSwitch: () => Effect.void,
    onBeforeQuitForUpdate: () => Effect.void,
    on: () => Effect.void,
  } satisfies ElectronApp.ElectronApp["Service"]);

  const environmentLayer = Layer.succeed(DesktopEnvironment.DesktopEnvironment, {
    appDataDirectory: "/tmp/app-data",
    legacyUserDataDirName: "T3 Code (Alpha)",
    userDataDirName: "t3code",
    desktopLauncherPath: Option.none(),
    path: {
      join: (...parts: ReadonlyArray<string>) => parts.join("/"),
    },
  } as unknown as DesktopEnvironment.DesktopEnvironment["Service"]);

  return DesktopInstanceLock.layer.pipe(
    Layer.provideMerge(
      FileSystem.layerNoop({
        exists: () => Effect.succeed(false),
      }),
    ),
    Layer.provideMerge(electronAppLayer),
    Layer.provideMerge(environmentLayer),
  );
};

describe("DesktopInstanceLock", () => {
  it.effect("sets the stable userData path before acquiring the app-owned lock", () => {
    const events: Array<unknown> = [];

    return Effect.gen(function* () {
      const instanceLock = yield* DesktopInstanceLock.DesktopInstanceLock;

      assert.deepEqual(events, [
        "setPath:userData:/tmp/app-data/t3code",
        { event: "requestLock", identity: instanceLock.launchIdentity },
      ]);
      assert.deepEqual(instanceLock.launchIdentity, {
        type: "t3code-desktop-launch",
        version: 1,
        appPath,
        execPath: process.execPath,
        args: process.argv.slice(1),
      });
    }).pipe(Effect.provide(makeInstanceLockLayer({ events })));
  });

  it.effect("quits and interrupts a secondary process before later layers initialize", () => {
    const events: Array<unknown> = [];

    return Effect.gen(function* () {
      const exit = yield* DesktopInstanceLock.DesktopInstanceLock.pipe(
        Effect.provide(makeInstanceLockLayer({ events, lockAcquired: false })),
        Effect.exit,
      );

      assert.isTrue(Exit.hasInterrupts(exit));
      assert.equal(events[0], "setPath:userData:/tmp/app-data/t3code");
      assert.deepInclude(events[1] as object, { event: "requestLock" });
      assert.equal(events[2], "quit");
    });
  });

  it("routes packaged relaunches through the configured launcher", () => {
    assert.deepEqual(
      DesktopInstanceLock.resolveDesktopRelaunchTarget({
        configuredLauncherPath: Option.some("/nix/store/t3code/bin/t3code-desktop"),
        processExecPath: "/nix/store/electron/bin/electron",
        processArgv: [
          "/nix/store/electron/bin/electron",
          "--password-store=gnome-libsecret",
          "/nix/store/t3code/apps/desktop",
          "t3code://pair?token=not-logged",
        ],
        appPath: "/nix/store/t3code/apps/desktop",
      }),
      {
        execPath: "/nix/store/t3code/bin/t3code-desktop",
        args: ["t3code://pair?token=not-logged"],
      },
    );
  });

  it("preserves Electron relaunch behavior without a configured launcher", () => {
    assert.deepEqual(
      DesktopInstanceLock.resolveDesktopRelaunchTarget({
        configuredLauncherPath: Option.none(),
        processExecPath: "/Applications/T3 Code.app/Contents/MacOS/T3 Code",
        processArgv: [
          "/Applications/T3 Code.app/Contents/MacOS/T3 Code",
          "t3code://pair?token=not-logged",
        ],
        appPath: "/Applications/T3 Code.app/Contents/Resources/app.asar",
      }),
      {
        execPath: "/Applications/T3 Code.app/Contents/MacOS/T3 Code",
        args: ["t3code://pair?token=not-logged"],
      },
    );
  });

  it("rejects malformed launch identities", () => {
    assert.isTrue(
      Option.isSome(
        DesktopInstanceLock.decodeDesktopLaunchIdentity({
          type: "t3code-desktop-launch",
          version: 1,
          appPath,
          execPath: "/nix/store/electron/bin/electron",
          args: [appPath],
        }),
      ),
    );
    assert.isTrue(
      Option.isNone(
        DesktopInstanceLock.decodeDesktopLaunchIdentity({
          type: "t3code-desktop-launch",
          version: 2,
          appPath,
        }),
      ),
    );
  });
});
