import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

export interface CodexCliSessionImporterShape {
  /**
   * Start periodic discovery of Codex CLI sessions within the provided scope.
   */
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
}

export class CodexCliSessionImporter extends Context.Service<
  CodexCliSessionImporter,
  CodexCliSessionImporterShape
>()("t3/provider/Services/CodexCliSessionImporter") {}
