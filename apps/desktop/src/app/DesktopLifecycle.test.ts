import { assert, describe, it, vi } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";

import type * as Electron from "electron";

import * as ElectronApp from "../electron/ElectronApp.ts";
import * as ElectronTheme from "../electron/ElectronTheme.ts";
import * as DesktopEnvironment from "./DesktopEnvironment.ts";
import * as DesktopLifecycle from "./DesktopLifecycle.ts";
import * as DesktopShutdown from "./DesktopShutdown.ts";
import * as DesktopState from "./DesktopState.ts";
import * as DesktopWindow from "../window/DesktopWindow.ts";

type AppListener = (...args: ReadonlyArray<unknown>) => void;

const makeLifecycleTestLayer = (options: {
  readonly appListeners: Map<string, AppListener>;
  readonly appPath?: string;
  readonly activate?: Effect.Effect<void>;
  readonly exit?: (code: number) => Effect.Effect<void>;
  readonly quit?: Effect.Effect<void>;
  readonly relaunch?: (options: Electron.RelaunchOptions) => Effect.Effect<void>;
  readonly desktopLauncherPath?: string;
  readonly requestSingleInstanceLock?: (
    additionalData?: Readonly<Record<string, unknown>>,
  ) => Effect.Effect<boolean>;
}) => {
  const electronAppLayer = Layer.succeed(ElectronApp.ElectronApp, {
    metadata: Effect.succeed({
      appVersion: "1.2.3",
      appPath: options.appPath ?? "/nix/store/current-t3code/apps/desktop",
      isPackaged: true,
      resourcesPath: "/nix/store/current-t3code/resources",
      runningUnderArm64Translation: false,
    }),
    name: Effect.succeed("T3 Code"),
    whenReady: Effect.void,
    quit: options.quit ?? Effect.void,
    exit: options.exit ?? (() => Effect.void),
    relaunch: options.relaunch ?? (() => Effect.void),
    setPath: () => Effect.void,
    setName: () => Effect.void,
    setAboutPanelOptions: () => Effect.void,
    setAppUserModelId: () => Effect.void,
    requestSingleInstanceLock: options.requestSingleInstanceLock ?? (() => Effect.succeed(true)),
    getAppMetrics: Effect.succeed([]),
    isDefaultProtocolClient: () => Effect.succeed(false),
    setAsDefaultProtocolClient: () => Effect.succeed(true),
    setDesktopName: () => Effect.void,
    setDockIcon: () => Effect.void,
    appendCommandLineSwitch: () => Effect.void,
    onBeforeQuitForUpdate: (listener) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          options.appListeners.set("before-quit-for-update", listener);
        }),
        () =>
          Effect.sync(() => {
            options.appListeners.delete("before-quit-for-update");
          }),
      ).pipe(Effect.asVoid),
    on: (eventName, listener) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          options.appListeners.set(eventName, listener as AppListener);
        }),
        () =>
          Effect.sync(() => {
            options.appListeners.delete(eventName);
          }),
      ).pipe(Effect.asVoid),
  } satisfies ElectronApp.ElectronApp["Service"]);

  const electronThemeLayer = Layer.succeed(ElectronTheme.ElectronTheme, {
    shouldUseDarkColors: Effect.succeed(false),
    setSource: () => Effect.void,
    onUpdated: () => Effect.void,
  });

  const desktopWindowLayer = Layer.succeed(DesktopWindow.DesktopWindow, {
    createMain: Effect.die("unexpected window creation"),
    ensureMain: Effect.die("unexpected window creation"),
    revealOrCreateMain: Effect.die("unexpected window creation"),
    activate: options.activate ?? Effect.void,
    createMainIfBackendReady: Effect.void,
    showConnectingSplash: Effect.void,
    handleBackendReady: () => Effect.void,
    handleBackendNotReady: Effect.void,
    flushMainWindowBounds: Effect.void,
    dispatchMenuAction: () => Effect.void,
    syncAppearance: Effect.void,
  });

  const environmentLayer = Layer.succeed(DesktopEnvironment.DesktopEnvironment, {
    platform: "linux",
    isDevelopment: false,
    appPath: options.appPath ?? "/nix/store/current-t3code/apps/desktop",
    desktopLauncherPath:
      options.desktopLauncherPath === undefined
        ? Option.none()
        : Option.some(options.desktopLauncherPath),
  } as DesktopEnvironment.DesktopEnvironment["Service"]);

  const desktopShutdownLayer = Layer.succeed(DesktopShutdown.DesktopShutdown, {
    request: Effect.void,
    awaitRequest: Effect.void,
    markComplete: Effect.void,
    awaitComplete: Effect.void,
    isComplete: Effect.succeed(true),
  });

  return DesktopLifecycle.layer.pipe(
    Layer.provideMerge(electronAppLayer),
    Layer.provideMerge(electronThemeLayer),
    Layer.provideMerge(desktopWindowLayer),
    Layer.provideMerge(environmentLayer),
    Layer.provideMerge(desktopShutdownLayer),
    Layer.provideMerge(DesktopState.layer),
  );
};

const dispatchSecondInstance = (
  appListeners: Map<string, AppListener>,
  additionalData: unknown,
): void => {
  appListeners.get("second-instance")?.({} as Electron.Event, [], "/tmp", additionalData);
};

describe("DesktopLifecycle", () => {
  it("quits after the shutdown watchdog expires", async () => {
    vi.useFakeTimers();
    try {
      let resolveShutdown!: () => void;
      const shutdown = new Promise<void>((resolve) => {
        resolveShutdown = resolve;
      });
      const onTimeout = vi.fn();
      const quit = vi.fn();

      DesktopLifecycle.quitAfterShutdownOrTimeout({
        shutdown,
        timeoutMs: 1_000,
        onTimeout,
        quit,
      });

      await vi.advanceTimersByTimeAsync(999);
      assert.equal(quit.mock.calls.length, 0);

      await vi.advanceTimersByTimeAsync(1);
      assert.equal(onTimeout.mock.calls.length, 1);
      assert.equal(quit.mock.calls.length, 1);

      resolveShutdown();
      await Promise.resolve();
      assert.equal(quit.mock.calls.length, 1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("cancels the shutdown watchdog after graceful completion", async () => {
    vi.useFakeTimers();
    try {
      const onTimeout = vi.fn();
      const quit = vi.fn();

      DesktopLifecycle.quitAfterShutdownOrTimeout({
        shutdown: Promise.resolve(),
        timeoutMs: 1_000,
        onTimeout,
        quit,
      });

      await Promise.resolve();
      assert.equal(quit.mock.calls.length, 1);

      await vi.advanceTimersByTimeAsync(1_000);
      assert.equal(onTimeout.mock.calls.length, 0);
      assert.equal(quit.mock.calls.length, 1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("keeps Linux alive after the last window closes", () => {
    assert.equal(DesktopLifecycle.shouldQuitAfterLastWindowCloses("linux"), false);
  });

  it("keeps macOS alive and preserves Windows last-window quit behavior", () => {
    assert.equal(DesktopLifecycle.shouldQuitAfterLastWindowCloses("darwin"), false);
    assert.equal(DesktopLifecycle.shouldQuitAfterLastWindowCloses("win32"), true);
  });

  it("routes packaged relaunches through the configured launcher", () => {
    assert.deepEqual(
      DesktopLifecycle.resolveDesktopRelaunchTarget({
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
      DesktopLifecycle.resolveDesktopRelaunchTarget({
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

  it.effect("activates the resident package when a matching instance launches", () =>
    Effect.gen(function* () {
      const appListeners = new Map<string, AppListener>();
      const activated = yield* Deferred.make<void>();
      const requestSingleInstanceLock = vi.fn(
        (_additionalData?: Readonly<Record<string, unknown>>) => Effect.succeed(true),
      );
      const relaunch = vi.fn((_options: Electron.RelaunchOptions) => Effect.void);
      const layer = makeLifecycleTestLayer({
        appListeners,
        activate: Deferred.succeed(activated, undefined).pipe(Effect.asVoid),
        desktopLauncherPath: "/nix/store/current-t3code/bin/t3code-desktop",
        relaunch,
        requestSingleInstanceLock,
      });

      yield* Effect.scoped(
        Effect.gen(function* () {
          const lifecycle = yield* DesktopLifecycle.DesktopLifecycle;
          yield* lifecycle.register;

          assert.deepEqual(requestSingleInstanceLock.mock.calls[0]?.[0], {
            type: "t3code-desktop-launch",
            version: 1,
            appPath: "/nix/store/current-t3code/apps/desktop",
            execPath: "/nix/store/current-t3code/bin/t3code-desktop",
            args: process.argv.slice(1),
          });

          dispatchSecondInstance(appListeners, {
            type: "t3code-desktop-launch",
            version: 1,
            appPath: "/nix/store/current-t3code/apps/desktop",
            execPath: "/nix/store/current-electron/bin/electron",
            args: ["/nix/store/current-t3code/apps/desktop"],
          });

          yield* Deferred.await(activated);
          assert.equal(relaunch.mock.calls.length, 0);
        }),
      ).pipe(Effect.provide(layer));
    }),
  );

  it.effect("hands off to a newly launched desktop package", () =>
    Effect.gen(function* () {
      const appListeners = new Map<string, AppListener>();
      const relaunched = yield* Deferred.make<Electron.RelaunchOptions>();
      const exited = yield* Deferred.make<number>();
      const layer = makeLifecycleTestLayer({
        appListeners,
        relaunch: (options) => Deferred.succeed(relaunched, options).pipe(Effect.asVoid),
        exit: (code) => Deferred.succeed(exited, code).pipe(Effect.asVoid),
      });

      yield* Effect.scoped(
        Effect.gen(function* () {
          const lifecycle = yield* DesktopLifecycle.DesktopLifecycle;
          yield* lifecycle.register;

          dispatchSecondInstance(appListeners, {
            type: "t3code-desktop-launch",
            version: 1,
            appPath: "/nix/store/new-t3code/apps/desktop",
            execPath: "/nix/store/new-electron/bin/electron",
            args: ["/nix/store/new-t3code/apps/desktop", "t3code://pair?token=not-logged"],
          });

          assert.deepEqual(yield* Deferred.await(relaunched), {
            execPath: "/nix/store/new-electron/bin/electron",
            args: ["/nix/store/new-t3code/apps/desktop", "t3code://pair?token=not-logged"],
          });
          assert.equal(yield* Deferred.await(exited), 0);

          const state = yield* DesktopState.DesktopState;
          assert.isTrue(yield* Ref.get(state.quitting));
        }),
      ).pipe(Effect.provide(layer));
    }),
  );

  it.effect("activates the resident package when launch data is malformed", () =>
    Effect.gen(function* () {
      const appListeners = new Map<string, AppListener>();
      const activated = yield* Deferred.make<void>();
      const relaunch = vi.fn((_options: Electron.RelaunchOptions) => Effect.void);
      const layer = makeLifecycleTestLayer({
        appListeners,
        activate: Deferred.succeed(activated, undefined).pipe(Effect.asVoid),
        relaunch,
      });

      yield* Effect.scoped(
        Effect.gen(function* () {
          const lifecycle = yield* DesktopLifecycle.DesktopLifecycle;
          yield* lifecycle.register;

          dispatchSecondInstance(appListeners, {
            type: "t3code-desktop-launch",
            version: 999,
            appPath: "/nix/store/new-t3code/apps/desktop",
          });

          yield* Deferred.await(activated);
          assert.equal(relaunch.mock.calls.length, 0);
        }),
      ).pipe(Effect.provide(layer));
    }),
  );

  it.effect("quits a secondary process that cannot acquire the instance lock", () =>
    Effect.gen(function* () {
      const appListeners = new Map<string, AppListener>();
      const quit = vi.fn();
      const layer = makeLifecycleTestLayer({
        appListeners,
        quit: Effect.sync(quit),
        requestSingleInstanceLock: () => Effect.succeed(false),
      });

      const exit = yield* Effect.scoped(
        Effect.gen(function* () {
          const lifecycle = yield* DesktopLifecycle.DesktopLifecycle;
          yield* lifecycle.register;
        }),
      ).pipe(Effect.provide(layer), Effect.exit);

      assert.equal(exit._tag, "Failure");
      assert.equal(quit.mock.calls.length, 1);
      assert.equal(appListeners.size, 0);
    }),
  );

  for (const platform of ["darwin", "win32", "linux"] satisfies ReadonlyArray<NodeJS.Platform>) {
    it.effect(`lets the updater's quit event proceed on ${platform}`, () => {
      const appListeners = new Map<string, (...args: readonly unknown[]) => void>();

      const electronAppLayer = Layer.succeed(ElectronApp.ElectronApp, {
        metadata: Effect.succeed({
          appVersion: "1.2.3",
          appPath: "/nix/store/current-t3code/apps/desktop",
          isPackaged: true,
          resourcesPath: "/nix/store/current-t3code/resources",
          runningUnderArm64Translation: false,
        }),
        name: Effect.succeed("T3 Code"),
        whenReady: Effect.void,
        quit: Effect.void,
        exit: () => Effect.void,
        relaunch: () => Effect.void,
        setPath: () => Effect.void,
        setName: () => Effect.void,
        setAboutPanelOptions: () => Effect.void,
        setAppUserModelId: () => Effect.void,
        requestSingleInstanceLock: () => Effect.succeed(true),
        getAppMetrics: Effect.succeed([]),
        isDefaultProtocolClient: () => Effect.succeed(false),
        setAsDefaultProtocolClient: () => Effect.succeed(true),
        setDesktopName: () => Effect.void,
        setDockIcon: () => Effect.void,
        appendCommandLineSwitch: () => Effect.void,
        onBeforeQuitForUpdate: (listener) =>
          Effect.acquireRelease(
            Effect.sync(() => {
              appListeners.set("before-quit-for-update", listener);
            }),
            () =>
              Effect.sync(() => {
                appListeners.delete("before-quit-for-update");
              }),
          ).pipe(Effect.asVoid),
        on: (eventName, listener) =>
          Effect.acquireRelease(
            Effect.sync(() => {
              appListeners.set(
                eventName,
                listener as unknown as (...args: readonly unknown[]) => void,
              );
            }),
            () =>
              Effect.sync(() => {
                appListeners.delete(eventName);
              }),
          ).pipe(Effect.asVoid),
      } satisfies ElectronApp.ElectronApp["Service"]);

      const electronThemeLayer = Layer.succeed(ElectronTheme.ElectronTheme, {
        shouldUseDarkColors: Effect.succeed(false),
        setSource: () => Effect.void,
        onUpdated: () => Effect.void,
      });

      const desktopWindowLayer = Layer.succeed(DesktopWindow.DesktopWindow, {
        createMain: Effect.die("unexpected window creation"),
        ensureMain: Effect.die("unexpected window creation"),
        revealOrCreateMain: Effect.die("unexpected window creation"),
        activate: Effect.void,
        createMainIfBackendReady: Effect.void,
        showConnectingSplash: Effect.void,
        handleBackendReady: () => Effect.void,
        handleBackendNotReady: Effect.void,
        flushMainWindowBounds: Effect.void,
        dispatchMenuAction: () => Effect.void,
        syncAppearance: Effect.void,
      });

      const environmentLayer = Layer.succeed(DesktopEnvironment.DesktopEnvironment, {
        platform,
        isDevelopment: false,
        appPath: "/nix/store/current-t3code/apps/desktop",
        desktopLauncherPath: Option.none(),
      } as DesktopEnvironment.DesktopEnvironment["Service"]);

      const layer = DesktopLifecycle.layer.pipe(
        Layer.provideMerge(electronAppLayer),
        Layer.provideMerge(electronThemeLayer),
        Layer.provideMerge(desktopWindowLayer),
        Layer.provideMerge(environmentLayer),
        Layer.provideMerge(DesktopShutdown.layer),
        Layer.provideMerge(DesktopState.layer),
      );

      return Effect.scoped(
        Effect.gen(function* () {
          const lifecycle = yield* DesktopLifecycle.DesktopLifecycle;
          yield* lifecycle.register;

          appListeners.get("before-quit-for-update")?.();

          let prevented = false;
          const event = {
            preventDefault: () => {
              prevented = true;
            },
          } as Electron.Event;
          appListeners.get("before-quit")?.(event);

          assert.isFalse(
            prevented,
            "cancelling this event prevents the updater from completing its relaunch",
          );

          const state = yield* DesktopState.DesktopState;
          assert.isTrue(yield* Ref.get(state.quitting));
        }),
      ).pipe(Effect.provide(layer));
    });
  }
});
