import {
  ApprovalRequestId,
  ProviderApprovalDecision,
  ProviderInteractionMode,
  ProviderInstanceId,
  ProviderUserInputAnswers,
  RuntimeMode,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import { CodexResumeCursorSchema } from "../Layers/CodexSessionRuntime.ts";

export const CodexProviderHostSessionOptions = Schema.Struct({
  threadId: ThreadId,
  providerInstanceId: ProviderInstanceId,
  cwd: Schema.String,
  runtimeMode: RuntimeMode,
  model: Schema.optionalKey(Schema.String),
  serviceTier: Schema.optionalKey(Schema.String),
  resumeCursor: Schema.optionalKey(CodexResumeCursorSchema),
  threadConfig: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
});
export type CodexProviderHostSessionOptions = typeof CodexProviderHostSessionOptions.Type;

export const CodexProviderHostSendTurnPayload = Schema.Struct({
  input: Schema.optionalKey(Schema.String),
  attachments: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        type: Schema.Literal("image"),
        url: Schema.String,
      }),
    ),
  ),
  model: Schema.optionalKey(Schema.String),
  serviceTier: Schema.optionalKey(Schema.String),
  effort: Schema.optionalKey(Schema.String),
  interactionMode: Schema.optionalKey(ProviderInteractionMode),
});
export type CodexProviderHostSendTurnPayload = typeof CodexProviderHostSendTurnPayload.Type;

export const CodexProviderHostInterruptPayload = Schema.Struct({
  turnId: Schema.optionalKey(TurnId),
});
export const CodexProviderHostRollbackPayload = Schema.Struct({
  numTurns: Schema.Int,
});
export const CodexProviderHostApprovalPayload = Schema.Struct({
  requestId: ApprovalRequestId,
  decision: ProviderApprovalDecision,
});
export const CodexProviderHostUserInputPayload = Schema.Struct({
  requestId: ApprovalRequestId,
  answers: ProviderUserInputAnswers,
});
export const CodexProviderHostFeedbackPayload = Schema.Struct({
  reason: Schema.optionalKey(Schema.String),
});

export const CODEX_PROVIDER_HOST_OPERATIONS = {
  sendTurn: "turn.start",
  interruptTurn: "turn.interrupt",
  readThread: "thread.read",
  rollbackThread: "thread.rollback",
  respondToRequest: "request.respond",
  respondToUserInput: "userInput.respond",
  uploadFeedback: "feedback.upload",
  stopSession: "session.stop",
} as const;
