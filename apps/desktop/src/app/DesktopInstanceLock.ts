import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as ElectronApp from "../electron/ElectronApp.ts";
import * as DesktopAppIdentity from "./DesktopAppIdentity.ts";
import * as DesktopEnvironment from "./DesktopEnvironment.ts";

const DESKTOP_LAUNCH_IDENTITY_VERSION = 1;

export interface DesktopRelaunchTarget {
  readonly execPath: string;
  readonly args: ReadonlyArray<string>;
}

export function resolveDesktopRelaunchTarget(input: {
  readonly configuredLauncherPath: Option.Option<string>;
  readonly processExecPath: string;
  readonly processArgv: ReadonlyArray<string>;
  readonly appPath: string;
}): DesktopRelaunchTarget {
  if (Option.isNone(input.configuredLauncherPath)) {
    return {
      execPath: input.processExecPath,
      args: input.processArgv.slice(1),
    };
  }

  const appPathIndex = input.processArgv.indexOf(input.appPath, 1);
  return {
    execPath: input.configuredLauncherPath.value,
    args: input.processArgv.slice(appPathIndex === -1 ? 1 : appPathIndex + 1),
  };
}

export const DesktopLaunchIdentity = Schema.Struct({
  type: Schema.Literal("t3code-desktop-launch"),
  version: Schema.Literal(DESKTOP_LAUNCH_IDENTITY_VERSION),
  appPath: Schema.String,
  execPath: Schema.String,
  args: Schema.Array(Schema.String),
});
export type DesktopLaunchIdentity = typeof DesktopLaunchIdentity.Type;

export const decodeDesktopLaunchIdentity = Schema.decodeUnknownOption(DesktopLaunchIdentity);

export class DesktopInstanceLock extends Context.Service<
  DesktopInstanceLock,
  {
    readonly launchIdentity: DesktopLaunchIdentity;
  }
>()("@t3tools/desktop/app/DesktopInstanceLock") {}

export const make = Effect.gen(function* () {
  const electronApp = yield* ElectronApp.ElectronApp;
  const environment = yield* DesktopEnvironment.DesktopEnvironment;

  // Electron scopes the lock to userData, so establish the stable directory
  // before acquiring it. This layer runs before Clerk and the rest of the
  // desktop graph so both the lock identity and privileged renderer scheme
  // are configured before Electron becomes ready.
  const userDataPath = yield* DesktopAppIdentity.resolveUserDataPath;
  yield* electronApp.setPath("userData", userDataPath);

  const metadata = yield* electronApp.metadata.pipe(Effect.orDie);
  const relaunchTarget = resolveDesktopRelaunchTarget({
    configuredLauncherPath: environment.desktopLauncherPath,
    processExecPath: process.execPath,
    processArgv: process.argv,
    appPath: metadata.appPath,
  });
  const launchIdentity: DesktopLaunchIdentity = {
    type: "t3code-desktop-launch",
    version: DESKTOP_LAUNCH_IDENTITY_VERSION,
    appPath: metadata.appPath,
    execPath: relaunchTarget.execPath,
    args: relaunchTarget.args,
  };

  if (!(yield* electronApp.requestSingleInstanceLock(launchIdentity))) {
    yield* electronApp.quit;
    return yield* Effect.interrupt;
  }

  return DesktopInstanceLock.of({ launchIdentity });
}).pipe(Effect.withSpan("desktop.instanceLock.acquire"));

export const layer = Layer.effect(DesktopInstanceLock, make);
