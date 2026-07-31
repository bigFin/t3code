import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as DesktopEnvironment from "../../app/DesktopEnvironment.ts";
import * as DesktopLifecycle from "../../app/DesktopLifecycle.ts";
import * as DesktopShutdown from "../../app/DesktopShutdown.ts";
import * as DesktopState from "../../app/DesktopState.ts";
import * as ElectronApp from "../../electron/ElectronApp.ts";
import * as ElectronTheme from "../../electron/ElectronTheme.ts";
import * as DesktopWindow from "../../window/DesktopWindow.ts";
import { restartApp } from "./lifecycle.ts";

describe("lifecycle IPC", () => {
  it.effect("routes restart requests through the graceful desktop lifecycle", () => {
    const reasons: string[] = [];
    const layer = Layer.mergeAll(
      Layer.succeed(
        DesktopLifecycle.DesktopLifecycle,
        DesktopLifecycle.DesktopLifecycle.of({
          relaunch: (reason) =>
            Effect.sync(() => {
              reasons.push(reason);
            }),
          register: Effect.void,
        }),
      ),
      DesktopShutdown.layer,
      DesktopState.layer,
      Layer.succeed(
        DesktopEnvironment.DesktopEnvironment,
        DesktopEnvironment.DesktopEnvironment.of(
          {} as DesktopEnvironment.DesktopEnvironment["Service"],
        ),
      ),
      Layer.succeed(
        DesktopWindow.DesktopWindow,
        DesktopWindow.DesktopWindow.of({} as DesktopWindow.DesktopWindow["Service"]),
      ),
      Layer.succeed(
        ElectronApp.ElectronApp,
        ElectronApp.ElectronApp.of({} as ElectronApp.ElectronApp["Service"]),
      ),
      Layer.succeed(
        ElectronTheme.ElectronTheme,
        ElectronTheme.ElectronTheme.of({} as ElectronTheme.ElectronTheme["Service"]),
      ),
    );

    return Effect.gen(function* () {
      yield* restartApp.handler(undefined);
      assert.deepEqual(reasons, ["user-requested"]);
    }).pipe(Effect.provide(layer));
  });
});
