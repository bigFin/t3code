import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

export interface AntigravitySessionImporterShape {
  /** Run a discovery sweep. */
  readonly scan: (mode?: "full" | "active") => Effect.Effect<void>;
  /** Start periodic discovery of Antigravity CLI sessions. */
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
}

export class AntigravitySessionImporter extends Context.Service<
  AntigravitySessionImporter,
  AntigravitySessionImporterShape
>()("t3/provider/Services/AntigravitySessionImporter") {}
