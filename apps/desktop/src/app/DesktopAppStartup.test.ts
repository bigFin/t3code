import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";

import * as ElectronApp from "../electron/ElectronApp.ts";
import * as ElectronTheme from "../electron/ElectronTheme.ts";
import * as DesktopWindow from "../window/DesktopWindow.ts";
import * as DesktopApp from "./DesktopApp.ts";
import * as DesktopAppIdentity from "./DesktopAppIdentity.ts";
import * as DesktopClerk from "./DesktopClerk.ts";
import * as DesktopEnvironment from "./DesktopEnvironment.ts";
import * as DesktopLifecycle from "./DesktopLifecycle.ts";
import * as DesktopShutdown from "./DesktopShutdown.ts";
import * as DesktopState from "./DesktopState.ts";

const makeInstanceSetupLayer = (options: {
  readonly events: Array<string>;
  readonly interruptAtLifecycle?: boolean;
}) =>
  Layer.mergeAll(
    Layer.succeed(DesktopAppIdentity.DesktopAppIdentity, {
      resolveUserDataPath: Effect.succeed("/tmp/t3code"),
      configure: Effect.sync(() => {
        options.events.push("identity");
      }),
    }),
    Layer.succeed(DesktopLifecycle.DesktopLifecycle, {
      relaunch: () => Effect.void,
      register: Effect.sync(() => {
        options.events.push("lifecycle");
      }).pipe(options.interruptAtLifecycle ? Effect.andThen(Effect.interrupt) : Effect.asVoid),
    }),
    Layer.succeed(DesktopClerk.DesktopClerk, {
      configure: Effect.sync(() => {
        options.events.push("clerk");
      }),
    }),
    Layer.succeed(
      DesktopEnvironment.DesktopEnvironment,
      {} as DesktopEnvironment.DesktopEnvironment["Service"],
    ),
    Layer.succeed(
      DesktopShutdown.DesktopShutdown,
      {} as DesktopShutdown.DesktopShutdown["Service"],
    ),
    Layer.succeed(DesktopState.DesktopState, {} as DesktopState.DesktopState["Service"]),
    Layer.succeed(DesktopWindow.DesktopWindow, {} as DesktopWindow.DesktopWindow["Service"]),
    Layer.succeed(ElectronApp.ElectronApp, {} as ElectronApp.ElectronApp["Service"]),
    Layer.succeed(ElectronTheme.ElectronTheme, {} as ElectronTheme.ElectronTheme["Service"]),
  );

describe("DesktopApp startup", () => {
  it.effect("acquires the app-owned instance lock before configuring Clerk", () => {
    const events: Array<string> = [];

    return Effect.gen(function* () {
      yield* Effect.scoped(
        DesktopApp.configureDesktopInstance.pipe(
          Effect.provide(makeInstanceSetupLayer({ events })),
        ),
      );

      assert.deepEqual(events, ["identity", "lifecycle", "clerk"]);
    });
  });

  it.effect("does not initialize Clerk when a secondary instance is interrupted", () => {
    const events: Array<string> = [];

    return Effect.gen(function* () {
      const exit = yield* Effect.exit(
        Effect.scoped(
          DesktopApp.configureDesktopInstance.pipe(
            Effect.provide(makeInstanceSetupLayer({ events, interruptAtLifecycle: true })),
          ),
        ),
      );

      assert.isTrue(Exit.hasInterrupts(exit));
      assert.deepEqual(events, ["identity", "lifecycle"]);
    });
  });
});
