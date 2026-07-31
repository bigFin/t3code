import {
  CodexSettings,
  CommandId,
  DEFAULT_MODEL,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  ModelSelection,
  type OrchestrationThread,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexErrors from "effect-codex-app-server/errors";
import type * as CodexSchema from "effect-codex-app-server/schema";

import { ServerConfig } from "../../config.ts";
import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  ProviderSessionDirectory,
  type ProviderRuntimeBinding,
  type ProviderRuntimeBindingWithMetadata,
} from "../Services/ProviderSessionDirectory.ts";
import {
  CodexCliSessionImporter,
  type CodexCliSessionImporterShape,
} from "../Services/CodexCliSessionImporter.ts";
import { makeCodexClientLeasePool, type CodexClientLeasePool } from "./CodexClientLeasePool.ts";
import { resolveCodexHomeLayout, type CodexHomeLayout } from "./CodexHomeLayout.ts";
import { deriveProviderInstanceConfigMap } from "../Layers/ProviderInstanceRegistryHydration.ts";
import { buildCodexInitializeParams } from "../Layers/CodexProvider.ts";
import { codexAppServerArgs, resolveCodexLaunchArgs } from "../Layers/codexLaunchArgs.ts";

const CODEX_DRIVER = ProviderDriverKind.make("codex");
const DEFAULT_SCAN_INTERVAL_MS = 60_000;
const LIVE_RECOVERY_SCAN_INTERVAL_MS = 5_000;
const CODEX_APP_SERVER_FORCE_KILL_AFTER = "2 seconds" as const;
const THREAD_LIST_PAGE_SIZE = 100;
const MAX_INTERACTIVE_THREADS_PER_SCAN = 100;
const ROLLOUT_TERMINAL_EVENT_TAIL_BYTES = 4 * 1024 * 1024;
const CODEX_CLI_LIVE_INACTIVITY_GRACE_MS = 15 * 60_000;
const CODEX_CLI_IMPORT_VERSION = 2;
export const CODEX_INTERACTIVE_SOURCE_KINDS = ["cli", "vscode"] as const;

const decodeCodexSettings = Schema.decodeUnknownEffect(CodexSettings);
const isModelSelection = Schema.is(ModelSelection);

type CodexListedThread = CodexSchema.V2ThreadListResponse["data"][number];
type CodexReadThread = CodexSchema.V2ThreadReadResponse["thread"];
type CodexThreadItem = CodexReadThread["turns"][number]["items"][number];
type CodexUserInput = Extract<CodexThreadItem, { readonly type: "userMessage" }>["content"][number];

interface CodexDiscoveryTarget {
  readonly leaseKey: string;
  readonly configKey: string;
  readonly instanceId: ProviderInstanceId;
  readonly config: CodexSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly homeLayout: CodexHomeLayout;
}

interface CodexClientLeaseResource {
  readonly child: ChildProcessSpawner.ChildProcessHandle;
  readonly client: CodexClient.CodexAppServerClient["Service"];
  readonly startupMs: number;
}

interface CodexClientAcquisitionMetrics {
  readonly appServerRestarted: boolean;
  readonly appServerReused: boolean;
  readonly appServerStartupMs: number;
}

export interface CodexCliImportedMessage {
  readonly messageId: MessageId;
  readonly role: "user" | "assistant";
  readonly text: string;
  readonly turnId: TurnId;
  readonly createdAt: string;
}

type UnknownRecord = Record<string, unknown>;

function isUnknownRecord(value: unknown): value is UnknownRecord {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function parseUnknownJsonRecord(value: string): UnknownRecord | undefined {
  try {
    const parsed: unknown = JSON.parse(value);
    return isUnknownRecord(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

function stableTextHash(value: string): string {
  let hash = 0xcbf29ce484222325n;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= BigInt(value.charCodeAt(index));
    hash = BigInt.asUintN(64, hash * 0x100000001b3n);
  }
  return hash.toString(16).padStart(16, "0");
}

function codexDiscoveryTargetConfigKey(input: {
  readonly instanceId: ProviderInstanceId;
  readonly config: CodexSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly homeLayout: CodexHomeLayout;
}): string {
  return stableTextHash(
    JSON.stringify({
      instanceId: input.instanceId,
      sharedHomePath: input.homeLayout.sharedHomePath,
      config: input.config,
      environment: Object.entries(input.environment).sort(([left], [right]) =>
        left.localeCompare(right),
      ),
    }),
  );
}

function stableProjectId(cwd: string): ProjectId {
  return ProjectId.make(`codex-cli-${stableTextHash(cwd)}`);
}

function stableCommandId(...parts: ReadonlyArray<string>): CommandId {
  return CommandId.make(["codex-cli-import", ...parts].join(":"));
}

function unixSecondsToMillis(value: number | null | undefined, fallbackMillis: number): number {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return fallbackMillis;
  }
  const millis = value * 1_000;
  return Number.isFinite(millis) ? millis : fallbackMillis;
}

function formatCodexUserInput(input: CodexUserInput): string {
  switch (input.type) {
    case "text":
      return input.text;
    case "image":
      return `[image: ${input.url}]`;
    case "localImage":
      return `[image: ${input.path}]`;
    case "audio":
      return `[audio: ${input.url}]`;
    case "localAudio":
      return `[audio: ${input.path}]`;
    case "skill":
      return `[skill: ${input.name} (${input.path})]`;
    case "mention":
      return `[mention: ${input.name} (${input.path})]`;
  }
}

function importableItem(
  threadId: string,
  item: CodexThreadItem,
): Pick<CodexCliImportedMessage, "messageId" | "role" | "text"> | undefined {
  // Codex's app-server materializes item ids as thread-local counters
  // (`item-1`, `item-2`, ...). T3 message ids are global projection keys, so
  // using the raw item id makes imports from different threads overwrite one
  // another. Scope the provider id to its owning thread while keeping it
  // deterministic across periodic rescans.
  const messageId = MessageId.make(`codex-cli:${threadId}:${item.id}`);
  switch (item.type) {
    case "userMessage": {
      const text = item.content.map(formatCodexUserInput).join("\n");
      return text.length > 0
        ? {
            messageId,
            role: "user",
            text,
          }
        : undefined;
    }
    case "agentMessage":
      return item.text.length > 0
        ? {
            messageId,
            role: "assistant",
            text: item.text,
          }
        : undefined;
    default:
      return undefined;
  }
}

/**
 * Convert Codex's turn/item transcript into the two message roles T3 renders
 * as conversation history. Item ids and turn ids are preserved so repeated
 * scans update existing projected messages rather than creating duplicates.
 */
export function collectCodexCliImportedMessages(
  thread: CodexReadThread,
): ReadonlyArray<CodexCliImportedMessage> {
  const messages: CodexCliImportedMessage[] = [];
  let lastCreatedAtMillis = unixSecondsToMillis(thread.createdAt, 0) - 1;

  for (const turn of thread.turns) {
    const turnMillis = unixSecondsToMillis(
      turn.startedAt ?? turn.completedAt,
      lastCreatedAtMillis + 1,
    );
    const turnId = TurnId.make(turn.id);

    for (const item of turn.items) {
      const imported = importableItem(thread.id, item);
      if (imported === undefined) {
        continue;
      }
      lastCreatedAtMillis = Math.max(lastCreatedAtMillis + 1, turnMillis);
      messages.push({
        ...imported,
        turnId,
        createdAt: DateTime.formatIso(DateTime.makeUnsafe(lastCreatedAtMillis)),
      });
    }
  }

  return messages;
}

function rolloutMessageText(content: unknown): string {
  if (!Array.isArray(content)) {
    return "";
  }

  return content
    .flatMap((item) => {
      if (!isUnknownRecord(item) || typeof item.text !== "string") {
        return [];
      }
      return item.type === "input_text" || item.type === "output_text" || item.type === "text"
        ? [item.text]
        : [];
    })
    .join("\n");
}

function isSyntheticRolloutUserMessage(text: string): boolean {
  const trimmed = text.trimStart();
  return (
    trimmed.startsWith("# AGENTS.md instructions for ") ||
    trimmed.startsWith("<environment_context>") ||
    trimmed.startsWith("<permissions instructions>") ||
    trimmed.startsWith("<collaboration_mode>") ||
    trimmed.startsWith("<skills_instructions>") ||
    trimmed.startsWith("<apps_instructions>") ||
    trimmed.startsWith("<plugins_instructions>") ||
    trimmed.startsWith("<recommended_plugins>") ||
    trimmed.startsWith("<model_switch>") ||
    trimmed.startsWith("<turn_aborted>")
  );
}

function rolloutTurnId(threadId: string, payload: UnknownRecord, lineIndex: number): TurnId {
  const metadata = payload.internal_chat_message_metadata_passthrough;
  if (isUnknownRecord(metadata) && typeof metadata.turn_id === "string") {
    return TurnId.make(metadata.turn_id);
  }
  if (typeof payload.turn_id === "string") {
    return TurnId.make(payload.turn_id);
  }
  return TurnId.make(`codex-cli:${threadId}:rollout-turn:${lineIndex}`);
}

function rolloutTimestampMillis(
  timestamp: unknown,
  fallbackMillis: number,
  lastCreatedAtMillis: number,
): number {
  const parsed = typeof timestamp === "string" ? Date.parse(timestamp) : Number.NaN;
  return Math.max(lastCreatedAtMillis + 1, Number.isFinite(parsed) ? parsed : fallbackMillis);
}

/**
 * Recover renderable messages directly from a Codex rollout when app-server's
 * legacy history reader returns an empty transcript. User-facing `event_msg`
 * records are preferred over raw user response items so injected AGENTS,
 * environment, and harness context does not appear in the conversation.
 */
export function collectCodexCliRolloutMessages(input: {
  readonly threadId: string;
  readonly contents: string;
  readonly createdAt: number;
}): ReadonlyArray<CodexCliImportedMessage> {
  const records = input.contents.split(/\r?\n/).flatMap((line, lineIndex) => {
    if (line.trim().length === 0) {
      return [];
    }
    try {
      const value: unknown = JSON.parse(line);
      return isUnknownRecord(value) ? [{ lineIndex, value }] : [];
    } catch {
      return [];
    }
  });
  const hasUserMessageEvents = records.some(
    ({ value }) =>
      value.type === "event_msg" &&
      isUnknownRecord(value.payload) &&
      value.payload.type === "user_message" &&
      typeof value.payload.message === "string" &&
      value.payload.message.length > 0,
  );
  const messages: CodexCliImportedMessage[] = [];
  const fallbackMillis = unixSecondsToMillis(input.createdAt, 0);
  let lastCreatedAtMillis = fallbackMillis - 1;

  for (const { lineIndex, value } of records) {
    const payload = value.payload;
    if (!isUnknownRecord(payload)) {
      continue;
    }

    let role: "user" | "assistant" | undefined;
    let text = "";
    if (
      value.type === "event_msg" &&
      payload.type === "user_message" &&
      typeof payload.message === "string"
    ) {
      role = "user";
      text = payload.message;
    } else if (
      value.type === "response_item" &&
      payload.type === "message" &&
      payload.role === "assistant"
    ) {
      role = "assistant";
      text = rolloutMessageText(payload.content);
    } else if (
      !hasUserMessageEvents &&
      value.type === "response_item" &&
      payload.type === "message" &&
      payload.role === "user"
    ) {
      role = "user";
      text = rolloutMessageText(payload.content);
    }

    if (role === undefined || text.length === 0 || isSyntheticRolloutUserMessage(text)) {
      continue;
    }

    lastCreatedAtMillis = rolloutTimestampMillis(
      value.timestamp,
      fallbackMillis,
      lastCreatedAtMillis,
    );
    messages.push({
      messageId: MessageId.make(`codex-cli:${input.threadId}:rollout:${lineIndex}`),
      role,
      text,
      turnId: rolloutTurnId(input.threadId, payload, lineIndex),
      createdAt: DateTime.formatIso(DateTime.makeUnsafe(lastCreatedAtMillis)),
    });
  }

  return messages;
}

export function isCodexRolloutPathWithinSessionsRoot(
  path: Path.Path,
  sessionsRoot: string,
  rolloutPath: string,
): boolean {
  const relative = path.relative(path.resolve(sessionsRoot), path.resolve(rolloutPath));
  return relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
}

export function codexCliMessageImportCommand(input: {
  readonly threadId: ThreadId;
  readonly message: CodexCliImportedMessage;
}) {
  return {
    type: "thread.message.import" as const,
    commandId: stableCommandId(
      "message",
      input.threadId,
      input.message.messageId,
      stableTextHash(`${input.message.role}\0${input.message.text}\0${input.message.createdAt}`),
    ),
    threadId: input.threadId,
    messageId: input.message.messageId,
    role: input.message.role,
    text: input.message.text,
    turnId: input.message.turnId,
    createdAt: input.message.createdAt,
  };
}

function normalizedMessageText(text: string): string {
  return text.replace(/\r\n?/gu, "\n").trim();
}

const T3_PENDING_MESSAGE_MATCH_WINDOW_MS = 30_000;

/**
 * A T3-started Codex turn persists the optimistic user message before Codex
 * writes the same prompt into its rollout. If the desktop dies between those
 * writes, restart recovery sees two independently keyed copies. Reuse the
 * original T3 message id when the latest projected turn, text, and timestamps
 * all identify the imported user item as that pending message.
 */
export function reconcileCodexCliImportedMessages(
  messages: ReadonlyArray<CodexCliImportedMessage>,
  projectedThread: Pick<OrchestrationThread, "latestTurn" | "messages"> | undefined,
): ReadonlyArray<CodexCliImportedMessage> {
  const latestTurn = projectedThread?.latestTurn;
  if (projectedThread === undefined || latestTurn === undefined || latestTurn === null) {
    return messages;
  }

  const requestedAtMillis = Date.parse(latestTurn.requestedAt);
  if (!Number.isFinite(requestedAtMillis)) {
    return messages;
  }

  return messages.map((message) => {
    if (message.role !== "user" || message.turnId !== latestTurn.turnId) {
      return message;
    }

    const importedAtMillis = Date.parse(message.createdAt);
    if (
      !Number.isFinite(importedAtMillis) ||
      Math.abs(importedAtMillis - requestedAtMillis) > T3_PENDING_MESSAGE_MATCH_WINDOW_MS
    ) {
      return message;
    }

    const normalizedImportedText = normalizedMessageText(message.text);
    const match = projectedThread.messages
      .filter(
        (candidate) =>
          candidate.role === "user" &&
          candidate.turnId === null &&
          normalizedMessageText(candidate.text) === normalizedImportedText,
      )
      .map((candidate) => ({
        candidate,
        distance: Math.abs(Date.parse(candidate.createdAt) - requestedAtMillis),
      }))
      .filter(
        ({ distance }) =>
          Number.isFinite(distance) && distance <= T3_PENDING_MESSAGE_MATCH_WINDOW_MS,
      )
      .sort((left, right) => left.distance - right.distance)[0]?.candidate;

    return match === undefined
      ? message
      : {
          ...message,
          messageId: match.id,
          createdAt: match.createdAt,
        };
  });
}

function resolveThreadTitle(thread: CodexListedThread | CodexReadThread): string {
  const name = thread.name?.trim();
  if (name) {
    return name;
  }
  const preview = thread.preview.trim().split(/\r?\n/, 1)[0]?.trim();
  return preview || `Codex CLI ${thread.id.slice(0, 8)}`;
}

function resolveThreadBranch(thread: CodexReadThread): string | null {
  const branch = thread.gitInfo?.branch?.trim();
  return branch ? branch : null;
}

function readBindingModelSelection(runtimePayload: unknown): ModelSelection | undefined {
  if (
    runtimePayload === null ||
    typeof runtimePayload !== "object" ||
    Array.isArray(runtimePayload) ||
    !("modelSelection" in runtimePayload)
  ) {
    return undefined;
  }
  return isModelSelection(runtimePayload.modelSelection)
    ? runtimePayload.modelSelection
    : undefined;
}

function readImportedCodexUpdatedAt(runtimePayload: unknown): number | undefined {
  if (
    runtimePayload === null ||
    typeof runtimePayload !== "object" ||
    Array.isArray(runtimePayload) ||
    !("codexCliUpdatedAt" in runtimePayload)
  ) {
    return undefined;
  }
  return typeof runtimePayload.codexCliUpdatedAt === "number" &&
    Number.isFinite(runtimePayload.codexCliUpdatedAt)
    ? runtimePayload.codexCliUpdatedAt
    : undefined;
}

function readCodexCliImportVersion(runtimePayload: unknown): number | undefined {
  if (
    runtimePayload === null ||
    typeof runtimePayload !== "object" ||
    Array.isArray(runtimePayload) ||
    !("codexCliImportVersion" in runtimePayload)
  ) {
    return undefined;
  }
  return typeof runtimePayload.codexCliImportVersion === "number" &&
    Number.isFinite(runtimePayload.codexCliImportVersion)
    ? runtimePayload.codexCliImportVersion
    : undefined;
}

function readCodexResumeCursorThreadId(resumeCursor: unknown): string | undefined {
  if (
    resumeCursor === null ||
    typeof resumeCursor !== "object" ||
    Array.isArray(resumeCursor) ||
    !("threadId" in resumeCursor)
  ) {
    return undefined;
  }
  return typeof resumeCursor.threadId === "string" ? resumeCursor.threadId : undefined;
}

export function resolveCodexCliImportBinding(
  providerThreadId: string,
  bindings: ReadonlyArray<ProviderRuntimeBindingWithMetadata>,
): ProviderRuntimeBindingWithMetadata | undefined {
  const resumeOwners = bindings.filter(
    (binding) =>
      binding.provider === CODEX_DRIVER &&
      readCodexResumeCursorThreadId(binding.resumeCursor) === providerThreadId,
  );
  return (
    resumeOwners.find(isLiveCodexBinding) ??
    resumeOwners[0] ??
    bindings.find((binding) => binding.threadId === providerThreadId)
  );
}

export function isDifferentlyKeyedCodexCliOwnerBinding(
  providerThreadId: string,
  binding: ProviderRuntimeBinding | undefined,
): boolean {
  return (
    binding?.provider === CODEX_DRIVER &&
    binding.threadId !== providerThreadId &&
    readCodexResumeCursorThreadId(binding.resumeCursor) === providerThreadId
  );
}

export function isLiveCodexBinding(binding: ProviderRuntimeBinding | undefined): boolean {
  return binding?.status === "starting" || binding?.status === "running";
}

export function shouldInterruptStaleCodexCliSession(
  binding: ProviderRuntimeBinding | undefined,
  session: { readonly status: string } | null | undefined,
): boolean {
  return (
    !isLiveCodexBinding(binding) &&
    (session?.status === "starting" || session?.status === "running")
  );
}

export function isDetachedCodexCliMirrorSession(
  binding: ProviderRuntimeBinding | undefined,
  session: { readonly status: string } | null | undefined,
): boolean {
  return (
    binding?.provider === CODEX_DRIVER &&
    !isLiveCodexBinding(binding) &&
    session !== null &&
    session !== undefined &&
    session.status !== "starting" &&
    session.status !== "running"
  );
}

export function shouldInspectInterruptedCodexCliMirror(
  binding: ProviderRuntimeBinding | undefined,
  session: { readonly status: string } | null | undefined,
): boolean {
  return (
    isDetachedCodexCliMirrorSession(binding, session) &&
    (session?.status === "interrupted" || session?.status === "error")
  );
}

export function shouldProbeCodexCliRolloutOwner(input: {
  readonly rolloutPath: string | undefined;
  readonly staleActiveTurnId: string | null;
  readonly hasDetachedMirrorSession: boolean;
}): boolean {
  return (
    input.rolloutPath !== undefined &&
    (input.staleActiveTurnId !== null || input.hasDetachedMirrorSession)
  );
}

type CodexCliStaleSessionResolution =
  | { readonly status: "preserve" }
  | { readonly status: "ready"; readonly lastError: string | null }
  | { readonly status: "interrupted" | "error"; readonly lastError: string };
type SettledCodexCliStaleSessionResolution = Exclude<
  CodexCliStaleSessionResolution,
  { readonly status: "preserve" }
>;

type CodexCliRolloutTerminalState = "completed" | "interrupted" | null;
type CodexCliThreadImportResult = "skipped" | "imported" | "recovering-live";

interface CodexCliThreadImportCandidate {
  readonly listedThread: CodexListedThread;
  readonly threadId: ThreadId;
  readonly existingBinding: ProviderRuntimeBindingWithMetadata | undefined;
}

interface PreparedCodexCliThreadImport {
  readonly listedThread: CodexListedThread;
  readonly threadId: ThreadId;
  readonly existingBinding: ProviderRuntimeBindingWithMetadata | undefined;
  readonly existingThread: Option.Option<OrchestrationThreadShell>;
  readonly projectedThread: OrchestrationThreadShell | undefined;
  readonly staleSession: NonNullable<OrchestrationThreadShell["session"]> | undefined;
  readonly detachedMirrorSession: NonNullable<OrchestrationThreadShell["session"]> | undefined;
  readonly importIsCurrent: boolean;
  readonly rolloutPath: string | undefined;
}

interface CodexCliImportScanMetrics {
  threadReadCount: number;
  rolloutTailReadCount: number;
  skippedCurrentCount: number;
}

export interface CodexCliRolloutTerminalEvidence {
  readonly state: CodexCliRolloutTerminalState;
  readonly finalMessage: string | null;
  readonly completedAt: number | null;
}

const NO_CODEX_ROLLOUT_TERMINAL_EVIDENCE: CodexCliRolloutTerminalEvidence = {
  state: null,
  finalMessage: null,
  completedAt: null,
};

/**
 * A missing in-memory provider binding does not prove that a Codex CLI turn
 * stopped: the owning CLI may still be running and appending to the shared
 * rollout after T3 reconnects. Prefer explicit terminal evidence, then keep a
 * projected running turn alive while another Linux process still owns and
 * recently updated the rollout file.
 */
export function resolveStaleCodexCliSession(input: {
  readonly rolloutIsOpen: boolean;
  readonly rolloutIsRecent: boolean;
  readonly rolloutHasFinalResponse?: boolean;
  readonly rolloutTerminalState: CodexCliRolloutTerminalState;
  readonly upstreamTurn:
    | Pick<CodexReadThread["turns"][number], "error" | "items" | "status">
    | undefined;
}): CodexCliStaleSessionResolution {
  const upstreamHasFinalAssistantMessage =
    input.upstreamTurn?.items.some(
      (item) =>
        item.type === "agentMessage" &&
        item.text.length > 0 &&
        (item.phase === undefined || item.phase === null || item.phase === "final_answer"),
    ) ?? false;
  const upstreamHasLegacyFinalAssistantMessage =
    input.upstreamTurn?.items.some(
      (item) =>
        item.type === "agentMessage" &&
        item.text.length > 0 &&
        (item.phase === undefined || item.phase === null),
    ) ?? false;
  const completionIsAuthoritative =
    input.rolloutTerminalState === "completed" || input.upstreamTurn?.status === "completed";
  const hasFinalAssistantMessage =
    input.rolloutHasFinalResponse === true ||
    upstreamHasLegacyFinalAssistantMessage ||
    (completionIsAuthoritative && upstreamHasFinalAssistantMessage);

  if (
    input.rolloutTerminalState === "completed" ||
    input.upstreamTurn?.status === "completed" ||
    hasFinalAssistantMessage
  ) {
    return {
      status: "ready",
      lastError: hasFinalAssistantMessage
        ? null
        : "Codex completed this turn without a final response. T3 recovered the available transcript.",
    };
  }

  if (input.upstreamTurn?.status === "failed") {
    return {
      status: "error",
      lastError: input.upstreamTurn.error?.message ?? "The Codex turn failed.",
    };
  }

  if (input.rolloutTerminalState === "interrupted") {
    return {
      status: "interrupted",
      lastError: "The Codex turn was interrupted before it produced a final response.",
    };
  }

  if (input.rolloutIsOpen && input.rolloutIsRecent) {
    return { status: "preserve" };
  }

  return {
    status: "interrupted",
    lastError: input.rolloutIsOpen
      ? "The Codex turn stopped producing activity before it produced a final response. T3 recovered the available transcript."
      : "The Codex process ended before it produced a final response. T3 recovered the available transcript.",
  };
}

const collectOpenCodexRolloutPaths = Effect.fn("CodexCliSessionImporter.collectOpenRollouts")(
  function* (
    fileSystem: FileSystem.FileSystem,
    path: Path.Path,
    candidatePaths: ReadonlySet<string>,
  ) {
    const platform = yield* HostProcessPlatform;
    if (platform !== "linux" || candidatePaths.size === 0) {
      return new Set<string>();
    }

    const normalizedCandidates = new Set(
      [...candidatePaths].map((candidatePath) => path.resolve(candidatePath)),
    );
    const openPaths = new Set<string>();
    const procEntries = yield* fileSystem
      .readDirectory("/proc")
      .pipe(Effect.orElseSucceed(() => [] as string[]));

    for (const pid of procEntries) {
      if (!/^\d+$/.test(pid)) {
        continue;
      }
      const descriptorRoot = path.join("/proc", pid, "fd");
      const descriptors = yield* fileSystem
        .readDirectory(descriptorRoot)
        .pipe(Effect.orElseSucceed(() => [] as string[]));
      for (const descriptor of descriptors) {
        const target = yield* fileSystem
          .readLink(path.join(descriptorRoot, descriptor))
          .pipe(Effect.orElseSucceed(() => ""));
        if (target.length === 0) {
          continue;
        }
        const normalizedTarget = path.resolve(target.replace(/ \(deleted\)$/u, ""));
        if (!normalizedCandidates.has(normalizedTarget)) {
          continue;
        }
        openPaths.add(normalizedTarget);
        if (openPaths.size === normalizedCandidates.size) {
          return openPaths;
        }
      }
    }

    return openPaths;
  },
);

export function parseCodexRolloutTerminalEvidence(
  contents: string,
  turnId: string,
): CodexCliRolloutTerminalEvidence {
  if (contents.length === 0) {
    return NO_CODEX_ROLLOUT_TERMINAL_EVIDENCE;
  }

  const lines = contents.split(/\r?\n/u);
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    const line = lines[index];
    if (!line?.includes(turnId)) {
      continue;
    }
    const value = parseUnknownJsonRecord(line);
    if (value === undefined || value.type !== "event_msg") {
      continue;
    }
    const payload = value.payload;
    if (!isUnknownRecord(payload) || payload.turn_id !== turnId) {
      continue;
    }
    if (payload.type === "task_complete") {
      const finalMessage =
        typeof payload.last_agent_message === "string" &&
        payload.last_agent_message.trim().length > 0
          ? payload.last_agent_message
          : null;
      return {
        state: "completed",
        finalMessage,
        completedAt:
          typeof payload.completed_at === "number" && Number.isFinite(payload.completed_at)
            ? payload.completed_at
            : null,
      };
    }
    if (payload.type === "turn_aborted") {
      return {
        state: "interrupted",
        finalMessage: null,
        completedAt: null,
      };
    }
  }
  return NO_CODEX_ROLLOUT_TERMINAL_EVIDENCE;
}

const readCodexRolloutTerminalEvidence = Effect.fn(
  "CodexCliSessionImporter.readRolloutTerminalEvidence",
)(function* (fileSystem: FileSystem.FileSystem, rolloutPath: string, turnId: string) {
  const contents = yield* Effect.scoped(
    Effect.gen(function* () {
      const file = yield* fileSystem.open(rolloutPath, { flag: "r" });
      const stat = yield* file.stat;
      const size = Number(stat.size);
      const length = Math.min(size, ROLLOUT_TERMINAL_EVENT_TAIL_BYTES);
      if (length <= 0) {
        return "";
      }
      yield* file.seek(size - length, "start");
      const bytes = yield* file.readAlloc(length);
      return Option.match(bytes, {
        onNone: () => "",
        onSome: (value) => new TextDecoder().decode(value),
      });
    }),
  ).pipe(Effect.orElseSucceed(() => ""));
  return parseCodexRolloutTerminalEvidence(contents, turnId);
});

export function isCurrentCodexCliImport(
  binding: ProviderRuntimeBinding | undefined,
  listedThread: CodexListedThread,
): boolean {
  return (
    readCodexCliImportVersion(binding?.runtimePayload) === CODEX_CLI_IMPORT_VERSION &&
    readImportedCodexUpdatedAt(binding?.runtimePayload) === listedThread.updatedAt
  );
}

export function shouldSkipCurrentCodexCliImport(
  binding: ProviderRuntimeBinding | undefined,
  listedThread: CodexListedThread,
  hasStaleSession: boolean,
): boolean {
  return isCurrentCodexCliImport(binding, listedThread) && !hasStaleSession;
}

export function shouldPreserveCurrentOpenCodexCliImport(input: {
  readonly binding: ProviderRuntimeBinding | undefined;
  readonly listedThread: CodexListedThread;
  readonly staleActiveTurnId: string | null;
  readonly rolloutIsOpen: boolean;
  readonly rolloutTerminalEvidence: CodexCliRolloutTerminalEvidence;
  readonly nowMillis: number;
}): boolean {
  return (
    isCurrentCodexCliImport(input.binding, input.listedThread) &&
    input.staleActiveTurnId !== null &&
    input.rolloutIsOpen &&
    isRecentCodexCliActivity(input.listedThread.updatedAt, input.nowMillis) &&
    input.rolloutTerminalEvidence.state === null &&
    input.rolloutTerminalEvidence.finalMessage === null
  );
}

export function shouldReconcileCurrentCodexCliSessionWithoutRead(input: {
  readonly binding: ProviderRuntimeBinding | undefined;
  readonly listedThread: CodexListedThread;
  readonly staleActiveTurnId: string | null;
  readonly rolloutIsOpen: boolean;
  readonly rolloutTerminalEvidence: CodexCliRolloutTerminalEvidence;
  readonly nowMillis: number;
}): boolean {
  return (
    isCurrentCodexCliImport(input.binding, input.listedThread) &&
    input.staleActiveTurnId !== null &&
    input.rolloutTerminalEvidence.finalMessage === null &&
    !shouldPreserveCurrentOpenCodexCliImport(input)
  );
}

export function isRecentCodexCliActivity(updatedAt: number, nowMillis: number): boolean {
  return nowMillis - unixSecondsToMillis(updatedAt, 0) <= CODEX_CLI_LIVE_INACTIVITY_GRACE_MS;
}

export function isImportableCodexInteractiveThread(thread: CodexListedThread): boolean {
  return (
    !thread.ephemeral &&
    thread.cwd.trim().length > 0 &&
    (thread.threadSource === null ||
      thread.threadSource === undefined ||
      thread.threadSource === "user")
  );
}

const isCodexAppServerProcessExitedError = Schema.is(CodexErrors.CodexAppServerProcessExitedError);
const isCodexAppServerTransportError = Schema.is(CodexErrors.CodexAppServerTransportError);
const isCodexAppServerInputStreamEndedError = Schema.is(
  CodexErrors.CodexAppServerInputStreamEndedError,
);

type RestartableCodexAppServerError =
  | CodexErrors.CodexAppServerProcessExitedError
  | CodexErrors.CodexAppServerTransportError
  | CodexErrors.CodexAppServerInputStreamEndedError;

function isRestartableCodexAppServerError(error: unknown): error is RestartableCodexAppServerError {
  return (
    isCodexAppServerProcessExitedError(error) ||
    isCodexAppServerTransportError(error) ||
    isCodexAppServerInputStreamEndedError(error)
  );
}

function findRestartableCodexAppServerError(
  cause: Cause.Cause<unknown>,
): RestartableCodexAppServerError | undefined {
  const error = Option.getOrUndefined(Cause.findErrorOption(cause));
  return isRestartableCodexAppServerError(error) ? error : undefined;
}

function discoveryTargetPreference(target: CodexDiscoveryTarget): number {
  if (target.instanceId === ProviderInstanceId.make("codex")) {
    return 0;
  }
  return target.homeLayout.mode === "direct" ? 1 : 2;
}

const resolveDiscoveryTargets = Effect.fn("CodexCliSessionImporter.resolveDiscoveryTargets")(
  function* () {
    const settingsService = yield* ServerSettingsService;
    const settings = yield* settingsService.getSettings;
    const entries = Object.entries(deriveProviderInstanceConfigMap(settings));
    const targets: CodexDiscoveryTarget[] = [];

    for (const [rawInstanceId, entry] of entries) {
      if (entry.driver !== CODEX_DRIVER) {
        continue;
      }

      const instanceId = ProviderInstanceId.make(rawInstanceId);
      const config = yield* decodeCodexSettings(entry.config ?? {}).pipe(
        Effect.catch((cause) =>
          Effect.logWarning("codex.cli-import.invalid-instance-config", {
            instanceId,
            cause,
          }).pipe(Effect.as(undefined)),
        ),
      );
      if (config === undefined || !(entry.enabled ?? config.enabled)) {
        continue;
      }

      const homeLayout = yield* resolveCodexHomeLayout(config);
      const environment = mergeProviderInstanceEnvironment(entry.environment);
      const target = {
        leaseKey: homeLayout.sharedHomePath,
        instanceId,
        config,
        environment,
        homeLayout,
      };
      targets.push({
        ...target,
        configKey: codexDiscoveryTargetConfigKey(target),
      });
    }

    const targetsBySharedHome = new Map<string, CodexDiscoveryTarget>();
    for (const target of targets) {
      const existing = targetsBySharedHome.get(target.homeLayout.sharedHomePath);
      if (
        existing === undefined ||
        discoveryTargetPreference(target) < discoveryTargetPreference(existing)
      ) {
        targetsBySharedHome.set(target.homeLayout.sharedHomePath, target);
      }
    }
    return [...targetsBySharedHome.values()];
  },
);

const openCodexClientLease = Effect.fn("CodexCliSessionImporter.openCodexClientLease")(function* (
  target: CodexDiscoveryTarget,
  scope: Scope.Closeable,
): Effect.fn.Return<
  CodexClientLeaseResource,
  CodexErrors.CodexAppServerError,
  ChildProcessSpawner.ChildProcessSpawner | ServerConfig
> {
  const startupStartedAt = yield* Clock.currentTimeMillis;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const serverConfig = yield* ServerConfig;
  const environment = {
    ...target.environment,
    CODEX_HOME: target.homeLayout.sharedHomePath,
  };
  const launchArgs = resolveCodexLaunchArgs(target.config.launchArgs, environment);
  const spawnCommand = yield* resolveSpawnCommand(
    target.config.binaryPath,
    codexAppServerArgs(launchArgs),
    {
      env: environment,
      extendEnv: true,
    },
  );
  const child = yield* spawner
    .spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        cwd: serverConfig.cwd,
        env: environment,
        extendEnv: true,
        forceKillAfter: CODEX_APP_SERVER_FORCE_KILL_AFTER,
        shell: spawnCommand.shell,
      }),
    )
    .pipe(
      Effect.provideService(Scope.Scope, scope),
      Effect.mapError(
        (cause) =>
          new CodexErrors.CodexAppServerSpawnError({
            command: `${target.config.binaryPath} app-server`,
            cause,
          }),
      ),
    );
  const clientContext = yield* Layer.buildWithScope(CodexClient.layerChildProcess(child), scope);
  const client = yield* Effect.service(CodexClient.CodexAppServerClient).pipe(
    Effect.provide(clientContext),
  );
  yield* client.request("initialize", buildCodexInitializeParams());
  yield* client.notify("initialized", undefined);
  return {
    child,
    client,
    startupMs: Math.max(0, (yield* Clock.currentTimeMillis) - startupStartedAt),
  };
});

type CodexClientPool = CodexClientLeasePool<
  CodexDiscoveryTarget,
  CodexClientLeaseResource,
  CodexErrors.CodexAppServerError,
  ChildProcessSpawner.ChildProcessSpawner | ServerConfig
>;

const withCodexClient = <A, E, R>(
  pool: CodexClientPool,
  target: CodexDiscoveryTarget,
  use: (
    client: CodexClient.CodexAppServerClient["Service"],
    metrics: CodexClientAcquisitionMetrics,
  ) => Effect.Effect<A, E, R>,
): Effect.Effect<
  A,
  E | CodexErrors.CodexAppServerError,
  R | ChildProcessSpawner.ChildProcessSpawner | ServerConfig
> => {
  return Effect.gen(function* () {
    const acquisition = yield* pool.acquire(target);
    return yield* use(acquisition.lease.resource.client, {
      appServerRestarted: acquisition.restarted,
      appServerReused: acquisition.reused,
      appServerStartupMs: acquisition.reused ? 0 : acquisition.lease.resource.startupMs,
    }).pipe(
      Effect.catch((error) =>
        isRestartableCodexAppServerError(error)
          ? pool.invalidate(target, acquisition.lease).pipe(Effect.andThen(Effect.fail(error)))
          : Effect.fail(error),
      ),
    );
  });
};

const listInteractiveThreads = Effect.fn("CodexCliSessionImporter.listInteractiveThreads")(
  function* (client: CodexClient.CodexAppServerClient["Service"]) {
    const threads: CodexListedThread[] = [];
    const seenCursors = new Set<string>();
    let cursor: string | null = null;

    while (threads.length < MAX_INTERACTIVE_THREADS_PER_SCAN) {
      const remaining = MAX_INTERACTIVE_THREADS_PER_SCAN - threads.length;
      const response: CodexSchema.V2ThreadListResponse = yield* client.request("thread/list", {
        archived: false,
        cursor,
        limit: Math.min(THREAD_LIST_PAGE_SIZE, remaining),
        modelProviders: null,
        sortDirection: "desc",
        sortKey: "updated_at",
        sourceKinds: [...CODEX_INTERACTIVE_SOURCE_KINDS],
        useStateDbOnly: true,
      });
      threads.push(...response.data.filter(isImportableCodexInteractiveThread));
      const nextCursor: string | null = response.nextCursor ?? null;
      if (nextCursor === null || seenCursors.has(nextCursor)) {
        break;
      }
      seenCursors.add(nextCursor);
      cursor = nextCursor;
    }

    return threads;
  },
);

const makeCodexCliSessionImporter = (options?: { readonly scanIntervalMs?: number }) =>
  Effect.gen(function* () {
    const orchestrationEngine = yield* OrchestrationEngineService;
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
    const directory = yield* ProviderSessionDirectory;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const runtimeContext = yield* Effect.context<
      | ChildProcessSpawner.ChildProcessSpawner
      | FileSystem.FileSystem
      | Path.Path
      | ServerConfig
      | ServerSettingsService
    >();
    const clientPool = yield* makeCodexClientLeasePool<
      CodexDiscoveryTarget,
      CodexClientLeaseResource,
      CodexErrors.CodexAppServerError,
      ChildProcessSpawner.ChildProcessSpawner | ServerConfig
    >({
      open: openCodexClientLease,
      isRunning: (resource) => resource.child.isRunning.pipe(Effect.orElseSucceed(() => false)),
    });
    const scanIntervalMs = Math.max(1, options?.scanIntervalMs ?? DEFAULT_SCAN_INTERVAL_MS);

    const resolveRolloutPath = Effect.fn("CodexCliSessionImporter.resolveRolloutPath")(function* (
      target: CodexDiscoveryTarget,
      listedThread: CodexListedThread,
      sessionsRoot: string,
      realSessionsRoot: string | undefined,
    ) {
      if (realSessionsRoot === undefined) {
        return undefined;
      }
      const listedRolloutPath = listedThread.path?.trim();
      if (!listedRolloutPath) {
        return undefined;
      }

      const candidate = path.resolve(listedRolloutPath);
      if (!isCodexRolloutPathWithinSessionsRoot(path, sessionsRoot, candidate)) {
        yield* Effect.logWarning("codex.cli-import.rollout-path-rejected", {
          instanceId: target.instanceId,
          providerThreadId: listedThread.id,
          sessionsRoot,
          rolloutPath: candidate,
        });
        return undefined;
      }

      const realRolloutPath = yield* fileSystem.realPath(candidate).pipe(Effect.option);
      if (Option.isNone(realRolloutPath)) {
        return undefined;
      }
      if (!isCodexRolloutPathWithinSessionsRoot(path, realSessionsRoot, realRolloutPath.value)) {
        yield* Effect.logWarning("codex.cli-import.rollout-path-rejected", {
          instanceId: target.instanceId,
          providerThreadId: listedThread.id,
          sessionsRoot: realSessionsRoot,
          rolloutPath: realRolloutPath.value,
        });
        return undefined;
      }
      return realRolloutPath.value;
    });

    const resolveThreadImportCandidate = Effect.fn(
      "CodexCliSessionImporter.resolveThreadImportCandidate",
    )(function* (
      target: CodexDiscoveryTarget,
      listedThread: CodexListedThread,
      bindings: ReadonlyArray<ProviderRuntimeBindingWithMetadata>,
    ) {
      if (!isImportableCodexInteractiveThread(listedThread)) {
        return undefined;
      }

      const existingBinding = resolveCodexCliImportBinding(listedThread.id, bindings);
      const threadId = existingBinding?.threadId ?? ThreadId.make(listedThread.id);
      if (existingBinding !== undefined && existingBinding.provider !== CODEX_DRIVER) {
        yield* Effect.logWarning("codex.cli-import.binding-conflict", {
          threadId,
          existingProvider: existingBinding.provider,
          discoveredInstanceId: target.instanceId,
        });
        return undefined;
      }
      if (isLiveCodexBinding(existingBinding)) {
        return undefined;
      }

      return {
        listedThread,
        threadId,
        existingBinding,
      } satisfies CodexCliThreadImportCandidate;
    });

    const prepareThreadImport = Effect.fn("CodexCliSessionImporter.prepareThreadImport")(function* (
      target: CodexDiscoveryTarget,
      candidate: CodexCliThreadImportCandidate,
      existingThread: Option.Option<OrchestrationThreadShell>,
      sessionsRoot: string,
      realSessionsRoot: string | undefined,
      metrics: CodexCliImportScanMetrics,
    ) {
      const { listedThread, threadId, existingBinding } = candidate;
      if (
        Option.isNone(existingThread) &&
        isDifferentlyKeyedCodexCliOwnerBinding(listedThread.id, existingBinding)
      ) {
        return undefined;
      }
      const projectedThread = Option.getOrUndefined(existingThread);
      const staleSession =
        shouldInterruptStaleCodexCliSession(existingBinding, projectedThread?.session) &&
        projectedThread?.session !== null
          ? projectedThread?.session
          : undefined;
      const detachedMirrorSession =
        isDetachedCodexCliMirrorSession(existingBinding, projectedThread?.session) &&
        projectedThread?.session !== null
          ? projectedThread?.session
          : undefined;
      const shouldInspectCurrentMirror = shouldInspectInterruptedCodexCliMirror(
        existingBinding,
        projectedThread?.session,
      );
      const importIsCurrent = isCurrentCodexCliImport(existingBinding, listedThread);
      // Currentness is persisted independently of the active thread projection.
      // Archived threads are intentionally absent from active shell queries and
      // must not replay their complete transcripts on every periodic scan.
      if (
        shouldSkipCurrentCodexCliImport(
          existingBinding,
          listedThread,
          staleSession !== undefined || shouldInspectCurrentMirror,
        )
      ) {
        metrics.skippedCurrentCount += 1;
        return undefined;
      }

      return {
        listedThread,
        threadId,
        existingBinding,
        existingThread,
        projectedThread,
        staleSession,
        detachedMirrorSession,
        importIsCurrent,
        rolloutPath: yield* resolveRolloutPath(
          target,
          listedThread,
          sessionsRoot,
          realSessionsRoot,
        ),
      };
    });

    const settleStaleSession = Effect.fn("CodexCliSessionImporter.settleStaleSession")(function* (
      threadId: ThreadId,
      staleSession: NonNullable<OrchestrationThreadShell["session"]>,
      resolution: SettledCodexCliStaleSessionResolution,
      rolloutTerminalState: CodexCliRolloutTerminalState,
    ) {
      const settledAt = DateTime.formatIso(yield* DateTime.now);
      yield* orchestrationEngine.dispatch({
        type: "thread.session.set",
        commandId: stableCommandId(
          "session-reconciled",
          resolution.status,
          threadId,
          staleSession.activeTurnId ?? staleSession.updatedAt,
        ),
        threadId,
        session: {
          ...staleSession,
          status: resolution.status,
          activeTurnId: null,
          lastError: resolution.lastError,
          updatedAt: settledAt,
        },
        createdAt: settledAt,
      });
      yield* Effect.logInfo("codex.cli-import.reconciled-stale-session", {
        threadId,
        previousStatus: staleSession.status,
        previousActiveTurnId: staleSession.activeTurnId,
        reconciledStatus: resolution.status,
        rolloutTerminalState,
      });
    });

    const setMirroredSessionRunning = Effect.fn(
      "CodexCliSessionImporter.setMirroredSessionRunning",
    )(function* (
      threadId: ThreadId,
      session: NonNullable<OrchestrationThreadShell["session"]>,
      activeTurnId: TurnId,
    ) {
      const recoveredAt = DateTime.formatIso(yield* DateTime.now);
      yield* orchestrationEngine.dispatch({
        type: "thread.session.set",
        commandId: stableCommandId(
          "session-mirror-running",
          threadId,
          activeTurnId,
          session.updatedAt,
        ),
        threadId,
        session: {
          ...session,
          status: "running",
          activeTurnId,
          lastError: null,
          retrying: false,
          updatedAt: recoveredAt,
        },
        createdAt: recoveredAt,
      });
      yield* Effect.logInfo("codex.cli-import.mirror-session-running", {
        threadId,
        activeTurnId,
      });
    });

    const importThread = Effect.fn("CodexCliSessionImporter.importThread")(function* (
      target: CodexDiscoveryTarget,
      client: CodexClient.CodexAppServerClient["Service"],
      prepared: PreparedCodexCliThreadImport,
      openRolloutPaths: ReadonlySet<string>,
      nowMillis: number,
      metrics: CodexCliImportScanMetrics,
    ) {
      const {
        listedThread,
        threadId,
        existingBinding,
        existingThread,
        projectedThread,
        staleSession,
        detachedMirrorSession,
        importIsCurrent,
        rolloutPath,
      } = prepared;
      const staleActiveTurnId = staleSession?.activeTurnId ?? null;
      const rolloutTerminalEvidence =
        rolloutPath === undefined || staleActiveTurnId === null
          ? ({
              state: null,
              finalMessage: null,
              completedAt: null,
            } satisfies CodexCliRolloutTerminalEvidence)
          : yield* Effect.gen(function* () {
              metrics.rolloutTailReadCount += 1;
              return yield* readCodexRolloutTerminalEvidence(
                fileSystem,
                rolloutPath,
                staleActiveTurnId,
              );
            });
      const rolloutIsOpen = rolloutPath !== undefined && openRolloutPaths.has(rolloutPath);
      const rolloutIsRecent = isRecentCodexCliActivity(listedThread.updatedAt, nowMillis);
      if (
        shouldPreserveCurrentOpenCodexCliImport({
          binding: existingBinding,
          listedThread,
          staleActiveTurnId,
          rolloutIsOpen,
          rolloutTerminalEvidence,
          nowMillis,
        })
      ) {
        yield* Effect.logInfo("codex.cli-import.recovering-live-session", {
          threadId,
          activeTurnId: staleActiveTurnId,
          providerThreadId: listedThread.id,
        });
        return "recovering-live" satisfies CodexCliThreadImportResult;
      }
      if (
        staleSession !== undefined &&
        shouldReconcileCurrentCodexCliSessionWithoutRead({
          binding: existingBinding,
          listedThread,
          staleActiveTurnId,
          rolloutIsOpen,
          rolloutTerminalEvidence,
          nowMillis,
        })
      ) {
        const resolution = resolveStaleCodexCliSession({
          rolloutIsOpen,
          rolloutIsRecent,
          rolloutHasFinalResponse: false,
          rolloutTerminalState: rolloutTerminalEvidence.state,
          upstreamTurn: undefined,
        });
        if (resolution.status !== "preserve") {
          yield* settleStaleSession(
            threadId,
            staleSession,
            resolution,
            rolloutTerminalEvidence.state,
          );
          return "imported" satisfies CodexCliThreadImportResult;
        }
      }

      metrics.threadReadCount += 1;
      const response = yield* client.request("thread/read", {
        threadId: listedThread.id,
        includeTurns: true,
      });
      const thread = response.thread;
      const appServerMessages = collectCodexCliImportedMessages(thread);
      const recoveredMessages =
        appServerMessages.length > 0
          ? appServerMessages
          : yield* Effect.gen(function* () {
              if (rolloutPath === undefined) {
                return [];
              }

              const contents = yield* fileSystem.readFileString(rolloutPath);
              const recovered = collectCodexCliRolloutMessages({
                threadId: thread.id,
                contents,
                createdAt: thread.createdAt,
              });
              if (recovered.length > 0) {
                yield* Effect.logInfo("codex.cli-import.rollout-fallback-used", {
                  instanceId: target.instanceId,
                  providerThreadId: listedThread.id,
                  rolloutPath,
                  messageCount: recovered.length,
                });
              }
              return recovered;
            }).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("codex.cli-import.rollout-fallback-failed", {
                  instanceId: target.instanceId,
                  providerThreadId: listedThread.id,
                  rolloutPath: listedThread.path,
                  cause,
                }).pipe(Effect.as([])),
              ),
            );
      const recoveredTerminalMessage =
        staleActiveTurnId !== null && rolloutTerminalEvidence.finalMessage !== null
          ? ({
              messageId: MessageId.make(
                `codex-cli:${thread.id}:task-complete:${staleActiveTurnId}`,
              ),
              role: "assistant",
              text: rolloutTerminalEvidence.finalMessage,
              turnId: TurnId.make(staleActiveTurnId),
              createdAt: DateTime.formatIso(
                DateTime.makeUnsafe(
                  unixSecondsToMillis(
                    rolloutTerminalEvidence.completedAt,
                    unixSecondsToMillis(thread.updatedAt, 0),
                  ),
                ),
              ),
            } satisfies CodexCliImportedMessage)
          : undefined;
      const rawMessages =
        recoveredTerminalMessage === undefined ||
        recoveredMessages.some(
          (message) =>
            message.role === recoveredTerminalMessage.role &&
            message.turnId === recoveredTerminalMessage.turnId &&
            message.text === recoveredTerminalMessage.text,
        )
          ? recoveredMessages
          : [...recoveredMessages, recoveredTerminalMessage];
      const projectedThreadDetail =
        projectedThread === undefined
          ? undefined
          : Option.getOrUndefined(yield* projectionSnapshotQuery.getThreadDetailById(threadId));
      const messages = reconcileCodexCliImportedMessages(rawMessages, projectedThreadDetail);
      if (messages.length === 0 && projectedThread === undefined) {
        return "skipped" satisfies CodexCliThreadImportResult;
      }
      const cwd = path.resolve(thread.cwd);
      const modelSelection =
        Option.getOrUndefined(existingThread)?.modelSelection ??
        readBindingModelSelection(existingBinding?.runtimePayload) ??
        ({
          instanceId: existingBinding?.providerInstanceId ?? target.instanceId,
          model: DEFAULT_MODEL,
        } satisfies ModelSelection);
      const existingProject = yield* projectionSnapshotQuery.getActiveProjectByWorkspaceRoot(cwd);
      const projectId = Option.match(existingProject, {
        onSome: (project) => project.id,
        onNone: () => stableProjectId(cwd),
      });
      const createdAt = DateTime.formatIso(
        DateTime.makeUnsafe(unixSecondsToMillis(thread.createdAt, 0)),
      );

      if (Option.isNone(existingProject)) {
        yield* orchestrationEngine.dispatch({
          type: "project.create",
          commandId: stableCommandId("project", projectId),
          projectId,
          title: path.basename(cwd) || "Codex CLI",
          workspaceRoot: cwd,
          defaultModelSelection: modelSelection,
          createdAt,
        });
      }

      if (Option.isNone(existingThread)) {
        yield* orchestrationEngine.dispatch({
          type: "thread.create",
          commandId: stableCommandId("thread", threadId),
          threadId,
          projectId,
          title: resolveThreadTitle(thread),
          modelSelection,
          runtimeMode: existingBinding?.runtimeMode ?? "full-access",
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          branch: resolveThreadBranch(thread),
          worktreePath: null,
          createdAt,
        });
      }

      if (!importIsCurrent || staleSession !== undefined) {
        for (const message of messages) {
          yield* orchestrationEngine.dispatch(
            codexCliMessageImportCommand({
              threadId,
              message,
            }),
          );
        }
      }

      let recoveringLive = false;
      if (staleSession !== undefined) {
        const upstreamTurn =
          staleActiveTurnId === null
            ? undefined
            : thread.turns.find((turn) => turn.id === staleActiveTurnId);
        const resolution = resolveStaleCodexCliSession({
          rolloutIsOpen,
          rolloutIsRecent,
          rolloutHasFinalResponse: rolloutTerminalEvidence.finalMessage !== null,
          rolloutTerminalState: rolloutTerminalEvidence.state,
          upstreamTurn,
        });

        if (resolution.status === "preserve") {
          recoveringLive = true;
          yield* Effect.logInfo("codex.cli-import.recovering-live-session", {
            threadId,
            activeTurnId: staleActiveTurnId,
            providerThreadId: thread.id,
          });
        } else {
          yield* settleStaleSession(
            threadId,
            staleSession,
            resolution,
            rolloutTerminalEvidence.state,
          );
        }
      } else if (detachedMirrorSession !== undefined) {
        const upstreamTurn = thread.turns.at(-1);
        if (upstreamTurn !== undefined) {
          const mirrorTerminalEvidence =
            rolloutPath === undefined
              ? NO_CODEX_ROLLOUT_TERMINAL_EVIDENCE
              : yield* Effect.gen(function* () {
                  metrics.rolloutTailReadCount += 1;
                  return yield* readCodexRolloutTerminalEvidence(
                    fileSystem,
                    rolloutPath,
                    upstreamTurn.id,
                  );
                });
          const resolution = resolveStaleCodexCliSession({
            rolloutIsOpen,
            rolloutIsRecent,
            rolloutHasFinalResponse: mirrorTerminalEvidence.finalMessage !== null,
            rolloutTerminalState: mirrorTerminalEvidence.state,
            upstreamTurn,
          });

          if (resolution.status === "preserve") {
            recoveringLive = true;
            yield* setMirroredSessionRunning(
              threadId,
              detachedMirrorSession,
              TurnId.make(upstreamTurn.id),
            );
          } else if (
            detachedMirrorSession.status !== resolution.status ||
            detachedMirrorSession.lastError !== resolution.lastError
          ) {
            yield* settleStaleSession(
              threadId,
              detachedMirrorSession,
              resolution,
              mirrorTerminalEvidence.state,
            );
          }
        }
      }

      // A periodic scan must never downgrade a T3-owned session that is
      // currently starting or running. Its adapter owns the live binding and
      // will persist the latest resume cursor when the session stops.
      if (!isLiveCodexBinding(existingBinding)) {
        yield* directory.upsert({
          threadId,
          provider: CODEX_DRIVER,
          providerInstanceId: existingBinding?.providerInstanceId ?? target.instanceId,
          status: "stopped",
          runtimeMode: existingBinding?.runtimeMode ?? "full-access",
          resumeCursor: { threadId: thread.id },
          runtimePayload: {
            cwd,
            modelSelection,
            importedFrom: "codex-cli",
            importedAt: DateTime.formatIso(yield* DateTime.now),
            codexCliImportVersion: CODEX_CLI_IMPORT_VERSION,
            codexCliUpdatedAt: thread.updatedAt,
          },
        });
      }
      return recoveringLive ? "recovering-live" : "imported";
    });

    const scanTarget = Effect.fn("CodexCliSessionImporter.scanTarget")(function* (
      target: CodexDiscoveryTarget,
    ) {
      const scanStartedAt = yield* Clock.currentTimeMillis;
      const result = yield* withCodexClient(clientPool, target, (client, appServerMetrics) =>
        Effect.gen(function* () {
          const metrics: CodexCliImportScanMetrics = {
            threadReadCount: 0,
            rolloutTailReadCount: 0,
            skippedCurrentCount: 0,
          };
          const threadListStartedAt = yield* Clock.currentTimeMillis;
          const threads = yield* listInteractiveThreads(client);
          const threadListMs = Math.max(0, (yield* Clock.currentTimeMillis) - threadListStartedAt);

          const prepareStartedAt = yield* Clock.currentTimeMillis;
          const bindings = yield* directory.listBindings();
          const sessionsRoot = path.resolve(target.homeLayout.sharedHomePath, "sessions");
          const realSessionsRoot = yield* fileSystem.realPath(sessionsRoot).pipe(Effect.option);
          const candidates: CodexCliThreadImportCandidate[] = [];
          for (const thread of threads) {
            const candidate = yield* resolveThreadImportCandidate(target, thread, bindings).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("codex.cli-import.thread-prepare-failed", {
                  instanceId: target.instanceId,
                  codexHome: target.homeLayout.sharedHomePath,
                  providerThreadId: thread.id,
                  cause,
                }).pipe(Effect.as(undefined)),
              ),
            );
            if (candidate !== undefined) {
              candidates.push(candidate);
            }
          }
          const preProjectionPrepareMs = Math.max(
            0,
            (yield* Clock.currentTimeMillis) - prepareStartedAt,
          );

          const projectionLookupStartedAt = yield* Clock.currentTimeMillis;
          const projectedThreads = yield* projectionSnapshotQuery.getThreadShellsByIds([
            ...new Set(candidates.map((candidate) => candidate.threadId)),
          ]);
          const projectionLookupCompletedAt = yield* Clock.currentTimeMillis;
          const projectionLookupMs = Math.max(
            0,
            projectionLookupCompletedAt - projectionLookupStartedAt,
          );

          const preparedImports: PreparedCodexCliThreadImport[] = [];
          for (const candidate of candidates) {
            const projectedThread = projectedThreads.get(candidate.threadId);
            const prepared = yield* prepareThreadImport(
              target,
              candidate,
              projectedThread === undefined ? Option.none() : Option.some(projectedThread),
              sessionsRoot,
              Option.getOrUndefined(realSessionsRoot),
              metrics,
            ).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("codex.cli-import.thread-prepare-failed", {
                  instanceId: target.instanceId,
                  codexHome: target.homeLayout.sharedHomePath,
                  providerThreadId: candidate.listedThread.id,
                  cause,
                }).pipe(Effect.as(undefined)),
              ),
            );
            if (prepared !== undefined) {
              preparedImports.push(prepared);
            }
          }
          const prepareMs =
            preProjectionPrepareMs +
            Math.max(0, (yield* Clock.currentTimeMillis) - projectionLookupCompletedAt);

          const openRolloutCandidates = new Set(
            preparedImports.flatMap((prepared) =>
              prepared.rolloutPath !== undefined &&
              shouldProbeCodexCliRolloutOwner({
                rolloutPath: prepared.rolloutPath,
                staleActiveTurnId: prepared.staleSession?.activeTurnId ?? null,
                hasDetachedMirrorSession: prepared.detachedMirrorSession !== undefined,
              })
                ? [prepared.rolloutPath]
                : [],
            ),
          );
          const procScanStartedAt = yield* Clock.currentTimeMillis;
          const openRolloutPaths = yield* collectOpenCodexRolloutPaths(
            fileSystem,
            path,
            openRolloutCandidates,
          );
          const procScanMs = Math.max(0, (yield* Clock.currentTimeMillis) - procScanStartedAt);
          const nowMillis = yield* Clock.currentTimeMillis;
          const importStartedAt = yield* Clock.currentTimeMillis;
          let importedCount = 0;
          let recoveringLiveCount = 0;
          for (const prepared of preparedImports) {
            const importResult = yield* importThread(
              target,
              client,
              prepared,
              openRolloutPaths,
              nowMillis,
              metrics,
            ).pipe(
              Effect.catchCause((cause) => {
                const restartableError = findRestartableCodexAppServerError(cause);
                if (restartableError !== undefined) {
                  return Effect.fail(restartableError);
                }
                return Effect.logWarning("codex.cli-import.thread-failed", {
                  instanceId: target.instanceId,
                  codexHome: target.homeLayout.sharedHomePath,
                  providerThreadId: prepared.listedThread.id,
                  cause,
                }).pipe(Effect.as("skipped" satisfies CodexCliThreadImportResult));
              }),
            );
            if (importResult !== "skipped") {
              importedCount += 1;
            }
            if (importResult === "recovering-live") {
              recoveringLiveCount += 1;
            }
          }
          const importMs = Math.max(0, (yield* Clock.currentTimeMillis) - importStartedAt);
          return {
            discoveredCount: threads.length,
            importedCount,
            recoveringLiveCount,
            ...appServerMetrics,
            threadListMs,
            projectionLookupMs,
            prepareMs,
            procScanMs,
            importMs,
            ...metrics,
          };
        }),
      );
      return {
        ...result,
        durationMs: Math.max(0, (yield* Clock.currentTimeMillis) - scanStartedAt),
      };
    });

    const scan = Effect.gen(function* () {
      const targets = yield* resolveDiscoveryTargets();
      yield* clientPool.reconcile(targets);
      let recoveringLiveCount = 0;
      for (const target of targets) {
        const result = yield* scanTarget(target).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("codex.cli-import.scan-failed", {
              instanceId: target.instanceId,
              codexHome: target.homeLayout.sharedHomePath,
              cause,
            }).pipe(
              Effect.as({
                discoveredCount: 0,
                importedCount: 0,
                recoveringLiveCount: 0,
                durationMs: 0,
                appServerRestarted: false,
                appServerReused: false,
                appServerStartupMs: 0,
                threadListMs: 0,
                projectionLookupMs: 0,
                prepareMs: 0,
                procScanMs: 0,
                importMs: 0,
                threadReadCount: 0,
                rolloutTailReadCount: 0,
                skippedCurrentCount: 0,
              }),
            ),
          ),
        );
        recoveringLiveCount += result.recoveringLiveCount;
        if (result.discoveredCount > 0) {
          yield* Effect.logInfo("codex.cli-import.scan-complete", {
            instanceId: target.instanceId,
            codexHome: target.homeLayout.sharedHomePath,
            ...result,
          });
        }
      }
      return recoveringLiveCount > 0;
    });

    const start: CodexCliSessionImporterShape["start"] = () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(
          Effect.forever(
            Effect.gen(function* () {
              const recoveringLive = yield* scan.pipe(
                Effect.provideContext(runtimeContext),
                Effect.catchCause((cause) =>
                  Effect.logWarning("codex.cli-import.sweep-failed", { cause }).pipe(
                    Effect.as(false),
                  ),
                ),
              );
              yield* Effect.sleep(
                Duration.millis(
                  recoveringLive
                    ? Math.min(scanIntervalMs, LIVE_RECOVERY_SCAN_INTERVAL_MS)
                    : scanIntervalMs,
                ),
              );
            }),
          ),
        );
        yield* Effect.logInfo("codex.cli-import.started", { scanIntervalMs });
      });

    return {
      start,
    } satisfies CodexCliSessionImporterShape;
  });

export const makeCodexCliSessionImporterLive = (options?: { readonly scanIntervalMs?: number }) =>
  Layer.effect(CodexCliSessionImporter, makeCodexCliSessionImporter(options));

export const CodexCliSessionImporterLive = makeCodexCliSessionImporterLive();
