import { assert, describe, it, vi } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";

import type * as Electron from "electron";

import * as ElectronApp from "../electron/ElectronApp.ts";
import * as ElectronTheme from "../electron/ElectronTheme.ts";
import * as ElectronWindow from "../electron/ElectronWindow.ts";
import * as DesktopEnvironment from "./DesktopEnvironment.ts";
import * as DesktopInstanceLock from "./DesktopInstanceLock.ts";
import * as DesktopLifecycle from "./DesktopLifecycle.ts";
import * as DesktopShutdown from "./DesktopShutdown.ts";
import * as DesktopState from "./DesktopState.ts";
import * as DesktopWindow from "../window/DesktopWindow.ts";

type AppListener = (...args: ReadonlyArray<unknown>) => void;

function makeElectronAppLayer(
  appListeners: Map<string, AppListener>,
  quit: Effect.Effect<void> = Effect.void,
  input: {
    readonly appPath?: string;
    readonly exit?: (code: number) => Effect.Effect<void>;
    readonly relaunch?: (options: Electron.RelaunchOptions) => Effect.Effect<void>;
  } = {},
) {
  const appPath = input.appPath ?? "/nix/store/current-t3code/apps/desktop";
  const registerListener = (eventName: string, listener: AppListener) =>
    Effect.acquireRelease(
      Effect.sync(() => {
        appListeners.set(eventName, listener);
      }),
      () =>
        Effect.sync(() => {
          appListeners.delete(eventName);
        }),
    ).pipe(Effect.asVoid);

  return Layer.succeed(ElectronApp.ElectronApp, {
    metadata: Effect.succeed({
      appVersion: "1.2.3",
      appPath,
      isPackaged: true,
      resourcesPath: "/nix/store/current-t3code/resources",
      runningUnderArm64Translation: false,
    }),
    name: Effect.succeed("T3 Code"),
    systemLocale: Effect.succeed("en-US"),
    whenReady: Effect.void,
    quit,
    exit: input.exit ?? (() => Effect.void),
    relaunch: input.relaunch ?? (() => Effect.void),
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
    removeCommandLineSwitch: () => Effect.void,
    onBeforeQuitForUpdate: (listener) => registerListener("before-quit-for-update", listener),
    on: (eventName, listener) =>
      registerListener(eventName, listener as unknown as (...args: readonly unknown[]) => void),
  } satisfies ElectronApp.ElectronApp["Service"]);
}

const electronThemeLayer = Layer.succeed(ElectronTheme.ElectronTheme, {
  shouldUseDarkColors: Effect.succeed(false),
  setSource: () => Effect.void,
  onUpdated: () => Effect.void,
});

function makeElectronWindowLayer(destroyAll: Effect.Effect<void> = Effect.void) {
  return Layer.succeed(ElectronWindow.ElectronWindow, {
    create: () => Effect.die("unexpected window creation"),
    main: Effect.die("unexpected main window read"),
    currentMainOrFirst: Effect.die("unexpected current window read"),
    focusedMainOrFirst: Effect.die("unexpected focused window read"),
    setMain: () => Effect.void,
    clearMain: () => Effect.void,
    reveal: () => Effect.void,
    sendAll: () => Effect.void,
    destroyAll,
    syncAllAppearance: () => Effect.void,
  });
}

function makeDesktopWindowLayer(
  input: {
    readonly activate?: Effect.Effect<void>;
    readonly flushMainWindowBounds?: Effect.Effect<void>;
  } = {},
) {
  return Layer.succeed(DesktopWindow.DesktopWindow, {
    createMain: Effect.die("unexpected window creation"),
    ensureMain: Effect.die("unexpected window creation"),
    revealOrCreateMain: Effect.die("unexpected window creation"),
    activate: input.activate ?? Effect.void,
    createMainIfBackendReady: Effect.void,
    showConnectingSplash: Effect.void,
    handleBackendReady: () => Effect.void,
    handleBackendNotReady: Effect.void,
    flushMainWindowBounds: input.flushMainWindowBounds ?? Effect.void,
    dispatchMenuAction: () => Effect.void,
    zoomMain: () => Effect.void,
    syncAppearance: Effect.void,
  });
}

const makeLifecycleTestLayer = (options: {
  readonly appListeners: Map<string, AppListener>;
  readonly appPath?: string;
  readonly activate?: Effect.Effect<void>;
  readonly exit?: (code: number) => Effect.Effect<void>;
  readonly quit?: Effect.Effect<void>;
  readonly relaunch?: (options: Electron.RelaunchOptions) => Effect.Effect<void>;
  readonly desktopLauncherPath?: string;
}) => {
  const appPath = options.appPath ?? "/nix/store/current-t3code/apps/desktop";
  const environmentLayer = Layer.succeed(DesktopEnvironment.DesktopEnvironment, {
    platform: "linux",
    isDevelopment: false,
    appPath,
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

  const instanceLockLayer = Layer.succeed(DesktopInstanceLock.DesktopInstanceLock, {
    launchIdentity: {
      type: "t3code-desktop-launch",
      version: 1,
      appPath,
      execPath: options.desktopLauncherPath ?? "/nix/store/current-electron/bin/electron",
      args: [appPath],
    },
  });

  return DesktopLifecycle.layer.pipe(
    Layer.provideMerge(
      makeElectronAppLayer(options.appListeners, options.quit, {
        appPath,
        ...(options.exit ? { exit: options.exit } : {}),
        ...(options.relaunch ? { relaunch: options.relaunch } : {}),
      }),
    ),
    Layer.provideMerge(electronThemeLayer),
    Layer.provideMerge(makeElectronWindowLayer()),
    Layer.provideMerge(
      makeDesktopWindowLayer(options.activate ? { activate: options.activate } : {}),
    ),
    Layer.provideMerge(environmentLayer),
    Layer.provideMerge(desktopShutdownLayer),
    Layer.provideMerge(DesktopState.layer),
    Layer.provideMerge(instanceLockLayer),
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

  it.effect("activates the resident package when a matching instance launches", () =>
    Effect.gen(function* () {
      const appListeners = new Map<string, AppListener>();
      const activated = yield* Deferred.make<void>();
      const relaunch = vi.fn((_options: Electron.RelaunchOptions) => Effect.void);
      const layer = makeLifecycleTestLayer({
        appListeners,
        activate: Deferred.succeed(activated, undefined).pipe(Effect.asVoid),
        desktopLauncherPath: "/nix/store/current-t3code/bin/t3code-desktop",
        relaunch,
      });

      yield* Effect.scoped(
        Effect.gen(function* () {
          const lifecycle = yield* DesktopLifecycle.DesktopLifecycle;
          yield* lifecycle.register;

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

  for (const platform of ["darwin", "win32", "linux"] satisfies ReadonlyArray<NodeJS.Platform>) {
    it.effect(`lets the updater's quit event proceed on ${platform}`, () => {
      const appListeners = new Map<string, (...args: readonly unknown[]) => void>();
      let windowsDestroyed = false;
      const environmentLayer = Layer.succeed(DesktopEnvironment.DesktopEnvironment, {
        platform,
        isDevelopment: false,
        appPath: "/nix/store/current-t3code/apps/desktop",
        desktopLauncherPath: Option.none(),
      } as DesktopEnvironment.DesktopEnvironment["Service"]);

      const layer = DesktopLifecycle.layer.pipe(
        Layer.provideMerge(makeElectronAppLayer(appListeners)),
        Layer.provideMerge(electronThemeLayer),
        Layer.provideMerge(
          makeElectronWindowLayer(
            Effect.sync(() => {
              windowsDestroyed = true;
            }),
          ),
        ),
        Layer.provideMerge(makeDesktopWindowLayer()),
        Layer.provideMerge(environmentLayer),
        Layer.provideMerge(DesktopShutdown.layer),
        Layer.provideMerge(DesktopState.layer),
        Layer.provideMerge(
          Layer.succeed(DesktopInstanceLock.DesktopInstanceLock, {
            launchIdentity: {
              type: "t3code-desktop-launch",
              version: 1,
              appPath: "/nix/store/current-t3code/apps/desktop",
              execPath: "/nix/store/current-electron/bin/electron",
              args: ["/nix/store/current-t3code/apps/desktop"],
            },
          }),
        ),
      );

      return Effect.scoped(
        Effect.gen(function* () {
          const lifecycle = yield* DesktopLifecycle.DesktopLifecycle;
          yield* lifecycle.register;

          appListeners.get("before-quit-for-update")?.();
          yield* Effect.yieldNow;

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
          assert.isTrue(windowsDestroyed);

          const state = yield* DesktopState.DesktopState;
          assert.isTrue(yield* Ref.get(state.quitting));
        }),
      ).pipe(Effect.provide(layer));
    });
  }

  it.effect("destroys windows before waiting for backend shutdown", () =>
    Effect.gen(function* () {
      const appListeners = new Map<string, (...args: readonly unknown[]) => void>();
      const shutdownRequested = yield* Deferred.make<void>();
      const allowShutdown = yield* Deferred.make<void>();
      const quitRequested = yield* Deferred.make<void>();
      const events: string[] = [];

      const quit = Effect.sync(() => {
        events.push("quit");
      }).pipe(Effect.andThen(Deferred.succeed(quitRequested, undefined)), Effect.asVoid);
      const destroyAll = Effect.sync(() => {
        events.push("destroy");
      });
      const flushMainWindowBounds = Effect.sync(() => {
        events.push("flush");
      });

      const desktopShutdownLayer = Layer.succeed(DesktopShutdown.DesktopShutdown, {
        request: Effect.sync(() => {
          events.push("request");
        }).pipe(Effect.andThen(Deferred.succeed(shutdownRequested, undefined)), Effect.asVoid),
        awaitRequest: Deferred.await(shutdownRequested),
        markComplete: Deferred.succeed(allowShutdown, undefined).pipe(Effect.asVoid),
        awaitComplete: Deferred.await(allowShutdown),
        isComplete: Deferred.isDone(allowShutdown),
      });

      const environmentLayer = Layer.succeed(DesktopEnvironment.DesktopEnvironment, {
        platform: "darwin",
        isDevelopment: false,
      } as DesktopEnvironment.DesktopEnvironment["Service"]);

      const layer = DesktopLifecycle.layer.pipe(
        Layer.provideMerge(makeElectronAppLayer(appListeners, quit)),
        Layer.provideMerge(electronThemeLayer),
        Layer.provideMerge(makeElectronWindowLayer(destroyAll)),
        Layer.provideMerge(makeDesktopWindowLayer({ flushMainWindowBounds })),
        Layer.provideMerge(environmentLayer),
        Layer.provideMerge(desktopShutdownLayer),
        Layer.provideMerge(DesktopState.layer),
        Layer.provideMerge(
          Layer.succeed(DesktopInstanceLock.DesktopInstanceLock, {
            launchIdentity: {
              type: "t3code-desktop-launch",
              version: 1,
              appPath: "/nix/store/current-t3code/apps/desktop",
              execPath: "/nix/store/current-electron/bin/electron",
              args: ["/nix/store/current-t3code/apps/desktop"],
            },
          }),
        ),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const lifecycle = yield* DesktopLifecycle.DesktopLifecycle;
          yield* lifecycle.register;

          const event = { preventDefault: () => undefined } as Electron.Event;
          appListeners.get("before-quit")?.(event);

          yield* Deferred.await(shutdownRequested);
          const eventsBeforeCleanup = [...events];
          yield* Deferred.succeed(allowShutdown, undefined);
          yield* Deferred.await(quitRequested);

          assert.deepEqual(eventsBeforeCleanup, ["flush", "destroy", "request"]);
          assert.deepEqual(events, ["flush", "destroy", "request", "quit"]);
        }),
      ).pipe(Effect.provide(layer));
    }),
  );

  it.effect("ignores app activation while quitting", () =>
    Effect.gen(function* () {
      const appListeners = new Map<string, (...args: readonly unknown[]) => void>();
      let activationCount = 0;
      const activate = Effect.sync(() => {
        activationCount += 1;
      });
      const environmentLayer = Layer.succeed(DesktopEnvironment.DesktopEnvironment, {
        platform: "darwin",
        isDevelopment: false,
      } as DesktopEnvironment.DesktopEnvironment["Service"]);
      const layer = DesktopLifecycle.layer.pipe(
        Layer.provideMerge(makeElectronAppLayer(appListeners)),
        Layer.provideMerge(electronThemeLayer),
        Layer.provideMerge(makeElectronWindowLayer()),
        Layer.provideMerge(makeDesktopWindowLayer({ activate })),
        Layer.provideMerge(environmentLayer),
        Layer.provideMerge(DesktopShutdown.layer),
        Layer.provideMerge(DesktopState.layer),
        Layer.provideMerge(
          Layer.succeed(DesktopInstanceLock.DesktopInstanceLock, {
            launchIdentity: {
              type: "t3code-desktop-launch",
              version: 1,
              appPath: "/nix/store/current-t3code/apps/desktop",
              execPath: "/nix/store/current-electron/bin/electron",
              args: ["/nix/store/current-t3code/apps/desktop"],
            },
          }),
        ),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const lifecycle = yield* DesktopLifecycle.DesktopLifecycle;
          const state = yield* DesktopState.DesktopState;
          yield* lifecycle.register;
          yield* Ref.set(state.quitting, true);

          appListeners.get("activate")?.();

          assert.equal(activationCount, 0);
        }),
      ).pipe(Effect.provide(layer));
    }),
  );
});
