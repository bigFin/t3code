import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

export interface PiCompatibleSessionImporterShape {
  /** Start periodic discovery of Pi and Oh My Pi session transcripts. */
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
}

export class PiCompatibleSessionImporter extends Context.Service<
  PiCompatibleSessionImporter,
  PiCompatibleSessionImporterShape
>()("t3/provider/Services/PiCompatibleSessionImporter") {}
