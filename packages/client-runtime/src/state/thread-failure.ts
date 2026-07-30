export type ThreadFailureKind = "capacity" | "error";

const CAPACITY_FAILURE_PATTERN =
  /\b(?:capacity|overload(?:ed)?|server busy|rate[ _-]?limit(?:[ _-]?reached|ed)?|too many requests|429|try again later|temporarily unavailable)\b/i;

/**
 * Groups provider failures into a small, actionable set of user-facing states.
 * Keep this conservative: an unknown failure should remain an error rather than
 * promise that simply waiting will make it go away.
 */
export function classifyThreadFailure(lastError: string | null | undefined): ThreadFailureKind {
  return lastError !== undefined && lastError !== null && CAPACITY_FAILURE_PATTERN.test(lastError)
    ? "capacity"
    : "error";
}
