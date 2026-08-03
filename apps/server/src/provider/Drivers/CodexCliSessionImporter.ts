import {
  CheckpointRef,
  CodexSettings,
  CommandId,
  DEFAULT_MODEL,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  defaultInstanceIdForDriver,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderSessionRuntimeStatus,
  ThreadId,
  TurnId,
  ModelSelection,
  type OrchestrationSession,
  type OrchestrationThread,
  type OrchestrationThreadActivity,
  type OrchestrationThreadShell,
  type ToolLifecycleItemType,
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
import {
  ProjectionSnapshotQuery,
  type ProjectionThreadCheckpointContext,
  type ProjectionThreadTranscript,
} from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { forkParked } from "../../serverActivation.ts";
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
const ROLLOUT_ACTIVITY_TAIL_BYTES = 8 * 1024 * 1024;
const ROLLOUT_MESSAGE_READ_CHUNK_BYTES = 1024 * 1024;
const CODEX_CLI_LIVE_INACTIVITY_GRACE_MS = 15 * 60_000;
const CODEX_CLI_IMPORT_VERSION = 2;
const CODEX_CLI_IMPORT_FAILURE_BACKOFF_INITIAL_MS = 60_000;
const CODEX_CLI_IMPORT_FAILURE_BACKOFF_MAX_MS = 60 * 60_000;
const CODEX_CLI_MESSAGE_IMPORT_BATCH_SIZE = 100;
const CODEX_CLI_ACTIVITY_IMPORT_BATCH_SIZE = 500;
const CODEX_CLI_COLD_ACTIVITY_LIMIT = 500;
export const CODEX_INTERACTIVE_SOURCE_KINDS = ["cli", "vscode"] as const;

const decodeCodexSettings = Schema.decodeUnknownEffect(CodexSettings);
const encodeUnknownJsonString = Schema.encodeUnknownSync(Schema.UnknownFromJsonString);
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

interface CodexCliRolloutToolCall {
  readonly callId: string;
  readonly itemType: ToolLifecycleItemType;
  readonly title: string;
  readonly turnId: TurnId;
  readonly command: string | undefined;
  readonly files: ReadonlyArray<string>;
}

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

export function isCodexCliThreadTranscriptComplete(thread: CodexReadThread): boolean {
  return thread.turns.every((turn) => turn.itemsView === undefined || turn.itemsView === "full");
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

function stripTrailingMemoryCitation(text: string): string {
  return text.replace(/\n*<oai-mem-citation>[\s\S]*?<\/oai-mem-citation>\s*$/u, "").trimEnd();
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
    trimmed.startsWith("<turn_aborted>") ||
    trimmed.startsWith("<subagent_notification>")
  );
}

function rolloutRecordStableSuffix(
  value: UnknownRecord,
  role: "user" | "assistant",
  text: string,
): string {
  return stableTextHash(
    `${typeof value.timestamp === "string" ? value.timestamp : ""}\0${role}\0${normalizedMessageText(text)}`,
  );
}

function rolloutTurnId(
  threadId: string,
  value: UnknownRecord,
  payload: UnknownRecord,
  role: "user" | "assistant",
  text: string,
): TurnId {
  const metadata = payload.internal_chat_message_metadata_passthrough;
  if (isUnknownRecord(metadata) && typeof metadata.turn_id === "string") {
    return TurnId.make(metadata.turn_id);
  }
  if (typeof payload.turn_id === "string") {
    return TurnId.make(payload.turn_id);
  }
  return TurnId.make(
    `codex-cli:${threadId}:rollout-turn:${rolloutRecordStableSuffix(value, role, text)}`,
  );
}

function rolloutMessageId(
  threadId: string,
  value: UnknownRecord,
  payload: UnknownRecord,
  role: "user" | "assistant",
  text: string,
): MessageId {
  return MessageId.make(
    typeof payload.id === "string"
      ? `codex-cli:${threadId}:${payload.id}`
      : `codex-cli:${threadId}:rollout:${rolloutRecordStableSuffix(value, role, text)}`,
  );
}

function rolloutTimestampMillis(
  timestamp: unknown,
  fallbackMillis: number,
  lastCreatedAtMillis: number,
): number {
  const parsed = typeof timestamp === "string" ? Date.parse(timestamp) : Number.NaN;
  return Math.max(lastCreatedAtMillis + 1, Number.isFinite(parsed) ? parsed : fallbackMillis);
}

function parseCodexRolloutRecords(contents: string): ReadonlyArray<UnknownRecord> {
  return contents.split(/\r?\n/).flatMap((line) => {
    if (line.trim().length === 0) {
      return [];
    }
    const value = parseUnknownJsonRecord(line);
    return value === undefined ? [] : [value];
  });
}

/**
 * Recover renderable messages directly from a Codex rollout when app-server's
 * history reader is empty or partial. Raw response items preserve Codex item
 * and turn ids; matching user events remain a legacy fallback. Injected AGENTS,
 * environment, and harness context is filtered from either representation.
 */
function collectCodexCliRolloutMessagesFromRecords(input: {
  readonly threadId: string;
  readonly records: ReadonlyArray<UnknownRecord>;
  readonly createdAt: number;
}): ReadonlyArray<CodexCliImportedMessage> {
  const rawUserMessages = input.records.flatMap((value) => {
    const payload = value.payload;
    if (
      value.type !== "response_item" ||
      !isUnknownRecord(payload) ||
      payload.type !== "message" ||
      payload.role !== "user"
    ) {
      return [];
    }
    const text = rolloutMessageText(payload.content);
    return text.length === 0 || isSyntheticRolloutUserMessage(text)
      ? []
      : [
          {
            text: normalizedMessageText(text),
            timestamp:
              typeof value.timestamp === "string" ? Date.parse(value.timestamp) : Number.NaN,
          },
        ];
  });
  const messages: CodexCliImportedMessage[] = [];
  const fallbackMillis = unixSecondsToMillis(input.createdAt, 0);
  let lastCreatedAtMillis = fallbackMillis - 1;

  for (const value of input.records) {
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
      const message = payload.message;
      const eventTimestamp =
        typeof value.timestamp === "string" ? Date.parse(value.timestamp) : Number.NaN;
      if (
        rawUserMessages.some(
          (raw) =>
            raw.text === normalizedMessageText(message) &&
            Number.isFinite(raw.timestamp) &&
            Number.isFinite(eventTimestamp) &&
            Math.abs(raw.timestamp - eventTimestamp) <= 1_000,
        )
      ) {
        continue;
      }
      role = "user";
      text = message;
    } else if (
      value.type === "response_item" &&
      payload.type === "message" &&
      payload.role === "assistant"
    ) {
      role = "assistant";
      text = stripTrailingMemoryCitation(rolloutMessageText(payload.content));
    } else if (
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
      messageId: rolloutMessageId(input.threadId, value, payload, role, text),
      role,
      text,
      turnId: rolloutTurnId(input.threadId, value, payload, role, text),
      createdAt: DateTime.formatIso(DateTime.makeUnsafe(lastCreatedAtMillis)),
    });
  }

  return messages;
}

export function collectCodexCliRolloutMessages(input: {
  readonly threadId: string;
  readonly contents: string;
  readonly createdAt: number;
}): ReadonlyArray<CodexCliImportedMessage> {
  return collectCodexCliRolloutMessagesFromRecords({
    threadId: input.threadId,
    records: parseCodexRolloutRecords(input.contents),
    createdAt: input.createdAt,
  });
}

function rolloutRecordTurnId(payload: UnknownRecord): TurnId | undefined {
  const metadata = payload.internal_chat_message_metadata_passthrough;
  if (isUnknownRecord(metadata) && typeof metadata.turn_id === "string") {
    return TurnId.make(metadata.turn_id);
  }
  return typeof payload.turn_id === "string" ? TurnId.make(payload.turn_id) : undefined;
}

function parseToolArguments(value: unknown): UnknownRecord | undefined {
  if (isUnknownRecord(value)) {
    return value;
  }
  return typeof value === "string" ? parseUnknownJsonRecord(value) : undefined;
}

function rolloutToolItemType(name: string): ToolLifecycleItemType {
  const normalized = name.toLowerCase();
  if (normalized.endsWith("exec_command") || normalized.endsWith("write_stdin")) {
    return "command_execution";
  }
  if (normalized.endsWith("apply_patch")) {
    return "file_change";
  }
  if (normalized.endsWith("view_image")) {
    return "image_view";
  }
  if (normalized.includes("web_search")) {
    return "web_search";
  }
  return "dynamic_tool_call";
}

function humanizeRolloutToolName(name: string): string {
  const leaf = name.split(/[./]/u).at(-1) ?? name;
  const words = leaf.replace(/[_-]+/gu, " ").trim();
  return words.length === 0 ? "Tool call" : `${words[0]!.toUpperCase()}${words.slice(1)}`;
}

function rolloutToolTitle(name: string, itemType: ToolLifecycleItemType): string {
  if (name.toLowerCase().endsWith("write_stdin")) {
    return "Continued command";
  }
  switch (itemType) {
    case "command_execution":
      return "Ran command";
    case "file_change":
      return "Applied patch";
    case "image_view":
      return "Viewed image";
    case "web_search":
      return "Web search";
    default:
      return humanizeRolloutToolName(name);
  }
}

function rolloutToolCommand(name: string, payload: UnknownRecord): string | undefined {
  if (!name.toLowerCase().endsWith("exec_command")) {
    return undefined;
  }
  const args = parseToolArguments(payload.arguments);
  return typeof args?.cmd === "string" && args.cmd.trim().length > 0 ? args.cmd : undefined;
}

function collectRolloutPatchFiles(payload: UnknownRecord): ReadonlyArray<string> {
  const candidates = [
    typeof payload.input === "string" ? payload.input : "",
    typeof payload.output === "string" ? payload.output : "",
  ];
  const files: string[] = [];
  const seen = new Set<string>();
  const append = (value: string | undefined) => {
    const normalized = value?.trim();
    if (!normalized || seen.has(normalized) || files.length >= 12) {
      return;
    }
    seen.add(normalized);
    files.push(normalized);
  };
  for (const candidate of candidates) {
    for (const line of candidate.split(/\r?\n/u)) {
      append(
        /^\*\*\* (?:Add|Delete|Update) File:\s*(.+)$/u.exec(line)?.[1] ??
          /^\*\*\* Move to:\s*(.+)$/u.exec(line)?.[1] ??
          /^[MAD]\s+(.+)$/u.exec(line)?.[1],
      );
      if (files.length >= 12) {
        return files;
      }
    }
  }
  return files;
}

function summarizeRolloutToolOutput(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const lines = value.split(/\r?\n/u);
  const outputMarker = lines.findLastIndex((line) => line.trim() === "Output:");
  const candidates = outputMarker >= 0 ? lines.slice(outputMarker + 1) : lines;
  const first = candidates.map((line) => line.replace(/\s+/gu, " ").trim()).find(Boolean);
  if (!first) {
    return undefined;
  }
  return first.length <= 180 ? first : `${first.slice(0, 177).trimEnd()}...`;
}

function rolloutToolCompletionStatus(output: unknown): "completed" | "failed" {
  if (typeof output !== "string") {
    return "completed";
  }
  const exitCode = /(?:Process exited with code|Exit code:)\s*(-?\d+)/iu.exec(output)?.[1];
  return exitCode !== undefined && Number.parseInt(exitCode, 10) !== 0 ? "failed" : "completed";
}

function rolloutToolActivityId(
  threadId: string,
  callId: string,
  phase: "started" | "completed",
): EventId {
  return EventId.make(`codex-cli:${threadId}:tool:${callId}:${phase}`);
}

function rolloutToolActivityData(call: CodexCliRolloutToolCall, output: unknown): UnknownRecord {
  const outputSummary = summarizeRolloutToolOutput(output);
  return {
    toolCallId: call.callId,
    kind: call.itemType === "command_execution" ? "execute" : call.itemType,
    item: {
      name: call.title,
      ...(call.command !== undefined ? { command: call.command } : {}),
    },
    ...(call.command !== undefined ? { command: call.command } : {}),
    ...(call.files.length > 0
      ? {
          files: call.files.map((path) => ({ path })),
          locations: call.files.map((path) => ({ path })),
        }
      : {}),
    ...(outputSummary !== undefined ? { rawOutput: { content: outputSummary } } : {}),
  };
}

function rolloutToolActivity(input: {
  readonly threadId: string;
  readonly call: CodexCliRolloutToolCall;
  readonly phase: "started" | "completed";
  readonly timestamp: unknown;
  readonly fallbackMillis: number;
  readonly output?: unknown;
}): OrchestrationThreadActivity {
  const parsedTimestamp =
    typeof input.timestamp === "string" ? Date.parse(input.timestamp) : Number.NaN;
  const createdAt = DateTime.formatIso(
    DateTime.makeUnsafe(Number.isFinite(parsedTimestamp) ? parsedTimestamp : input.fallbackMillis),
  );
  const outputSummary = summarizeRolloutToolOutput(input.output);
  const status =
    input.phase === "started" ? "inProgress" : rolloutToolCompletionStatus(input.output);
  return {
    id: rolloutToolActivityId(input.threadId, input.call.callId, input.phase),
    tone: status === "failed" ? "error" : "tool",
    kind: input.phase === "started" ? "tool.started" : "tool.completed",
    summary: input.phase === "started" ? `${input.call.title} started` : input.call.title,
    payload: {
      itemType: input.call.itemType,
      title: input.call.title,
      status,
      ...(outputSummary !== undefined ? { detail: outputSummary } : {}),
      data: rolloutToolActivityData(input.call, input.output),
    },
    turnId: input.call.turnId,
    createdAt,
  };
}

function collectCodexCliRolloutActivitiesFromRecords(input: {
  readonly threadId: string;
  readonly records: ReadonlyArray<UnknownRecord>;
  readonly createdAt: number;
  readonly turnId: string | null;
  readonly toolCalls: ReadonlyMap<string, CodexCliRolloutToolCall>;
}): {
  readonly activities: ReadonlyArray<OrchestrationThreadActivity>;
  readonly toolCalls: ReadonlyMap<string, CodexCliRolloutToolCall>;
} {
  if (input.turnId === null) {
    return {
      activities: [],
      toolCalls: new Map(),
    };
  }
  const expectedTurnId = TurnId.make(input.turnId);
  const fallbackMillis = unixSecondsToMillis(input.createdAt, 0);
  const toolCalls = new Map(input.toolCalls);
  const activities: OrchestrationThreadActivity[] = [];

  for (const value of input.records) {
    if (value.type !== "response_item" || !isUnknownRecord(value.payload)) {
      continue;
    }
    const payload = value.payload;
    const turnId = rolloutRecordTurnId(payload);
    if (turnId !== expectedTurnId) {
      continue;
    }
    const payloadType = typeof payload.type === "string" ? payload.type : "";
    const callId =
      typeof payload.call_id === "string"
        ? payload.call_id
        : typeof payload.id === "string"
          ? payload.id
          : undefined;
    if (callId === undefined) {
      continue;
    }

    if (
      payloadType === "function_call" ||
      payloadType === "custom_tool_call" ||
      payloadType === "tool_search_call"
    ) {
      const name =
        typeof payload.name === "string"
          ? payload.name
          : payloadType === "tool_search_call"
            ? "tool_search"
            : "tool";
      const itemType = rolloutToolItemType(name);
      const call = {
        callId,
        itemType,
        title: rolloutToolTitle(name, itemType),
        turnId,
        command: rolloutToolCommand(name, payload),
        files: itemType === "file_change" ? collectRolloutPatchFiles(payload) : [],
      } satisfies CodexCliRolloutToolCall;
      toolCalls.set(callId, call);
      activities.push(
        rolloutToolActivity({
          threadId: input.threadId,
          call,
          phase: "started",
          timestamp: value.timestamp,
          fallbackMillis,
        }),
      );
      continue;
    }

    if (
      payloadType === "function_call_output" ||
      payloadType === "custom_tool_call_output" ||
      payloadType === "tool_search_output"
    ) {
      const call =
        toolCalls.get(callId) ??
        ({
          callId,
          itemType: "dynamic_tool_call",
          title: "Tool call",
          turnId,
          command: undefined,
          files: [],
        } satisfies CodexCliRolloutToolCall);
      const files =
        call.itemType === "file_change"
          ? [...new Set([...call.files, ...collectRolloutPatchFiles(payload)])]
          : call.files;
      activities.push(
        rolloutToolActivity({
          threadId: input.threadId,
          call: {
            ...call,
            files,
          },
          phase: "completed",
          timestamp: value.timestamp,
          fallbackMillis,
          output: payload.output,
        }),
      );
      toolCalls.delete(callId);
      continue;
    }

    if (payloadType === "web_search_call") {
      const call = {
        callId,
        itemType: "web_search",
        title: "Web search",
        turnId,
        command: undefined,
        files: [],
      } satisfies CodexCliRolloutToolCall;
      activities.push(
        rolloutToolActivity({
          threadId: input.threadId,
          call,
          phase: payload.status === "completed" ? "completed" : "started",
          timestamp: value.timestamp,
          fallbackMillis,
        }),
      );
    }
  }

  return {
    activities,
    toolCalls,
  };
}

export function collectCodexCliRolloutActivities(input: {
  readonly threadId: string;
  readonly contents: string;
  readonly createdAt: number;
  readonly turnId: string;
}): ReadonlyArray<OrchestrationThreadActivity> {
  return collectCodexCliRolloutActivitiesFromRecords({
    threadId: input.threadId,
    records: parseCodexRolloutRecords(input.contents),
    createdAt: input.createdAt,
    turnId: input.turnId,
    toolCalls: new Map(),
  }).activities;
}

function mergeCodexCliRolloutActivities(
  existing: ReadonlyArray<OrchestrationThreadActivity>,
  appended: ReadonlyArray<OrchestrationThreadActivity>,
): ReadonlyArray<OrchestrationThreadActivity> {
  const merged = new Map(existing.map((activity) => [activity.id, activity]));
  for (const activity of appended) {
    merged.set(activity.id, activity);
  }
  return [...merged.values()];
}

function isRolloutFallbackMessageId(messageId: MessageId): boolean {
  return String(messageId).includes(":rollout:");
}

function rolloutMessageMergeKey(message: CodexCliImportedMessage): string {
  return `${message.role}\0${normalizedMessageText(message.text)}`;
}

export function mergeCodexCliRolloutMessages(
  existing: ReadonlyArray<CodexCliImportedMessage>,
  appended: ReadonlyArray<CodexCliImportedMessage>,
): ReadonlyArray<CodexCliImportedMessage> {
  const merged = [...existing];
  const exactIndexes = new Map<MessageId, number>();
  const mergeKeyIndexes = new Map<string, number[]>();
  const indexMessage = (message: CodexCliImportedMessage, index: number) => {
    if (!exactIndexes.has(message.messageId)) {
      exactIndexes.set(message.messageId, index);
    }
    const key = rolloutMessageMergeKey(message);
    const indexes = mergeKeyIndexes.get(key);
    if (indexes === undefined) {
      mergeKeyIndexes.set(key, [index]);
    } else {
      indexes.push(index);
    }
  };
  const replaceMessage = (index: number, message: CodexCliImportedMessage) => {
    const current = merged[index]!;
    const currentKey = rolloutMessageMergeKey(current);
    const nextKey = rolloutMessageMergeKey(message);
    exactIndexes.delete(current.messageId);
    exactIndexes.set(message.messageId, index);
    merged[index] = message;
    if (currentKey === nextKey) {
      return;
    }
    const currentIndexes = mergeKeyIndexes.get(currentKey);
    if (currentIndexes !== undefined) {
      const retainedIndexes = currentIndexes.filter((candidate) => candidate !== index);
      if (retainedIndexes.length === 0) {
        mergeKeyIndexes.delete(currentKey);
      } else {
        mergeKeyIndexes.set(currentKey, retainedIndexes);
      }
    }
    const nextIndexes = mergeKeyIndexes.get(nextKey);
    if (nextIndexes === undefined) {
      mergeKeyIndexes.set(nextKey, [index]);
    } else {
      nextIndexes.push(index);
    }
  };
  for (const [index, message] of merged.entries()) {
    indexMessage(message, index);
  }
  for (const message of appended) {
    const exactIndex = exactIndexes.get(message.messageId);
    if (exactIndex !== undefined) {
      const current = merged[exactIndex]!;
      if (
        current.role !== message.role ||
        normalizedMessageText(message.text).length > normalizedMessageText(current.text).length
      ) {
        replaceMessage(exactIndex, message);
      }
      continue;
    }
    const messageCreatedAt = Date.parse(message.createdAt);
    const fallbackDuplicateIndex = (
      mergeKeyIndexes.get(rolloutMessageMergeKey(message)) ?? []
    ).find((index) => {
      const candidate = merged[index]!;
      if (
        !isRolloutFallbackMessageId(candidate.messageId) &&
        !isRolloutFallbackMessageId(message.messageId)
      ) {
        return false;
      }
      const candidateCreatedAt = Date.parse(candidate.createdAt);
      return (
        Number.isFinite(candidateCreatedAt) &&
        Number.isFinite(messageCreatedAt) &&
        Math.abs(candidateCreatedAt - messageCreatedAt) <= 1_000
      );
    });
    if (fallbackDuplicateIndex !== undefined) {
      if (
        isRolloutFallbackMessageId(merged[fallbackDuplicateIndex]!.messageId) &&
        !isRolloutFallbackMessageId(message.messageId)
      ) {
        replaceMessage(fallbackDuplicateIndex, message);
      }
      continue;
    }
    merged.push(message);
    indexMessage(message, merged.length - 1);
  }
  return merged.toSorted(
    (left, right) =>
      left.createdAt.localeCompare(right.createdAt) ||
      Buffer.compare(Buffer.from(left.messageId, "utf8"), Buffer.from(right.messageId, "utf8")),
  );
}

export function isCodexRolloutPathWithinSessionsRoot(
  path: Path.Path,
  sessionsRoot: string,
  rolloutPath: string,
): boolean {
  const relative = path.relative(path.resolve(sessionsRoot), path.resolve(rolloutPath));
  return relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
}

export function codexCliMessagesImportCommand(input: {
  readonly threadId: ThreadId;
  readonly messages: ReadonlyArray<CodexCliImportedMessage>;
  readonly expectedProviderRuntime?: CodexCliProviderRuntimeExpectation | null;
}) {
  const createdAt = input.messages.at(-1)?.createdAt;
  if (createdAt === undefined) {
    throw new Error("Cannot create an empty Codex CLI message import command.");
  }
  return {
    type: "thread.messages.import" as const,
    commandId: stableCommandId(
      "messages",
      input.threadId,
      stableTextHash(
        JSON.stringify(
          input.messages.map((message) => [
            message.messageId,
            message.role,
            message.text,
            message.turnId,
            message.createdAt,
          ]),
        ),
      ),
      ...providerRuntimeExpectationCommandParts(input.expectedProviderRuntime ?? null),
    ),
    threadId: input.threadId,
    messages: input.messages,
    expectedProviderRuntime: input.expectedProviderRuntime,
    createdAt,
  };
}

function normalizedMessageText(text: string): string {
  return text.replace(/\r\n?/gu, "\n").trim();
}

function nativeCodexMessageId(messageId: MessageId): string | undefined {
  return /(?:^|:)(msg_[^:]+)$/u.exec(messageId)?.[1];
}

function codexCliTranscriptMessagesMatch(
  projected: OrchestrationThread["messages"][number],
  imported: CodexCliImportedMessage,
): boolean {
  return (
    projected.role === imported.role &&
    normalizedMessageText(projected.text) === normalizedMessageText(imported.text) &&
    (projected.turnId ?? null) === imported.turnId
  );
}

function indexCodexCliProjectedMessagesById(
  projectedThread: Pick<OrchestrationThread, "messages">,
): ReadonlyMap<MessageId, ReadonlyArray<OrchestrationThread["messages"][number]>> {
  const messagesById = new Map<MessageId, Array<OrchestrationThread["messages"][number]>>();
  for (const message of projectedThread.messages) {
    const matches = messagesById.get(message.id);
    if (matches === undefined) {
      messagesById.set(message.id, [message]);
    } else {
      matches.push(message);
    }
  }
  return messagesById;
}

export function selectUnsynchronizedCodexCliMessages(
  projectedThread: Pick<OrchestrationThread, "messages"> | undefined,
  importedMessages: ReadonlyArray<CodexCliImportedMessage>,
): ReadonlyArray<CodexCliImportedMessage> {
  if (projectedThread === undefined) {
    return importedMessages;
  }
  const projectedMessagesById = indexCodexCliProjectedMessagesById(projectedThread);
  return importedMessages.filter((imported) => {
    const projectedMatches = projectedMessagesById.get(imported.messageId);
    return (
      projectedMatches === undefined ||
      !projectedMatches.some((projected) => codexCliTranscriptMessagesMatch(projected, imported))
    );
  });
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
  if (projectedThread === undefined) {
    return messages;
  }
  const projectedMessages = projectedThread.messages.map((message) => ({
    message,
    normalizedText: normalizedMessageText(message.text),
    createdAtMillis: Date.parse(message.createdAt),
  }));
  const consumedProjectedMessages = new Set<number>();
  const exactIndexes = new Map<MessageId, Array<number>>();
  const nativeMessageIndexes = new Map<string, Array<number>>();
  const sameTurnIndexes = new Map<string, Array<number>>();
  const pendingIndexes = new Map<string, Array<number>>();
  const addIndex = (indexMap: Map<string, Array<number>>, key: string, index: number) => {
    const indexes = indexMap.get(key);
    if (indexes === undefined) {
      indexMap.set(key, [index]);
    } else {
      indexes.push(index);
    }
  };
  const matchKey = (role: string, turnId: TurnId | null, normalizedText: string) =>
    JSON.stringify([role, turnId, normalizedText]);
  for (const [index, projected] of projectedMessages.entries()) {
    addIndex(exactIndexes, projected.message.id, index);
    const nativeMessageId = nativeCodexMessageId(projected.message.id);
    if (nativeMessageId !== undefined) {
      addIndex(nativeMessageIndexes, nativeMessageId, index);
    }
    addIndex(
      sameTurnIndexes,
      matchKey(projected.message.role, projected.message.turnId, projected.normalizedText),
      index,
    );
    if (projected.message.role === "user" && projected.message.turnId === null) {
      addIndex(pendingIndexes, projected.normalizedText, index);
    }
  }
  const latestTurn = projectedThread.latestTurn;
  const requestedAtMillis =
    latestTurn === undefined || latestTurn === null
      ? Number.NaN
      : Date.parse(latestTurn.requestedAt);

  return messages.map((message) => {
    const exactIndex =
      exactIndexes.get(message.messageId)?.find((index) => !consumedProjectedMessages.has(index)) ??
      -1;
    const nativeMessageId = nativeCodexMessageId(message.messageId);
    const nativeMessageIndex =
      exactIndex >= 0 || nativeMessageId === undefined
        ? -1
        : (nativeMessageIndexes
            .get(nativeMessageId)
            ?.find((index) => !consumedProjectedMessages.has(index)) ?? -1);
    const normalizedText = normalizedMessageText(message.text);
    const messageCreatedAtMillis = Date.parse(message.createdAt);
    const sameTurnIndex =
      exactIndex >= 0
        ? exactIndex
        : nativeMessageIndex >= 0
          ? nativeMessageIndex
          : (sameTurnIndexes
              .get(matchKey(message.role, message.turnId, normalizedText))
              ?.find((index) => !consumedProjectedMessages.has(index)) ?? -1);
    const pendingIndex =
      sameTurnIndex >= 0 ||
      message.role !== "user" ||
      latestTurn === undefined ||
      latestTurn === null ||
      message.turnId !== latestTurn.turnId ||
      !Number.isFinite(requestedAtMillis)
        ? -1
        : Math.abs(messageCreatedAtMillis - requestedAtMillis) > T3_PENDING_MESSAGE_MATCH_WINDOW_MS
          ? -1
          : (pendingIndexes.get(normalizedText) ?? []).reduce(
              (closest, index) => {
                if (consumedProjectedMessages.has(index)) {
                  return closest;
                }
                const distance = Math.abs(
                  projectedMessages[index]!.createdAtMillis - requestedAtMillis,
                );
                return Number.isFinite(distance) &&
                  distance <= T3_PENDING_MESSAGE_MATCH_WINDOW_MS &&
                  (closest.index < 0 || distance < closest.distance)
                  ? { index, distance }
                  : closest;
              },
              { index: -1, distance: Number.POSITIVE_INFINITY },
            ).index;
    const matchIndex = sameTurnIndex >= 0 ? sameTurnIndex : pendingIndex;
    if (matchIndex < 0) {
      return message;
    }
    consumedProjectedMessages.add(matchIndex);
    const match = projectedMessages[matchIndex]!.message;
    return {
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

export function readCodexCliImportedAt(runtimePayload: unknown): number | undefined {
  if (
    runtimePayload === null ||
    typeof runtimePayload !== "object" ||
    Array.isArray(runtimePayload) ||
    !("importedAt" in runtimePayload) ||
    typeof runtimePayload.importedAt !== "string"
  ) {
    return undefined;
  }
  const importedAt = Date.parse(runtimePayload.importedAt);
  return Number.isFinite(importedAt) ? importedAt : undefined;
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

function isCodexCliImportedBinding(
  binding: ProviderRuntimeBinding | undefined,
): binding is ProviderRuntimeBinding {
  const runtimePayload = binding?.runtimePayload;
  return (
    runtimePayload !== null &&
    typeof runtimePayload === "object" &&
    !Array.isArray(runtimePayload) &&
    "importedFrom" in runtimePayload &&
    runtimePayload.importedFrom === "codex-cli"
  );
}

function hasDetachedSessionPersistence(runtimePayload: unknown): boolean {
  return (
    runtimePayload !== null &&
    typeof runtimePayload === "object" &&
    !Array.isArray(runtimePayload) &&
    "sessionPersistence" in runtimePayload &&
    runtimePayload.sessionPersistence === "detached"
  );
}

function readBindingActiveTurnId(runtimePayload: unknown): string | null | undefined {
  if (
    runtimePayload === null ||
    typeof runtimePayload !== "object" ||
    Array.isArray(runtimePayload) ||
    !("activeTurnId" in runtimePayload)
  ) {
    return undefined;
  }
  return runtimePayload.activeTurnId === null || typeof runtimePayload.activeTurnId === "string"
    ? runtimePayload.activeTurnId
    : undefined;
}

function requiresCodexCliTranscriptRefresh(runtimePayload: unknown): boolean {
  return (
    runtimePayload !== null &&
    typeof runtimePayload === "object" &&
    !Array.isArray(runtimePayload) &&
    "codexCliTranscriptRefreshRequired" in runtimePayload &&
    runtimePayload.codexCliTranscriptRefreshRequired === true
  );
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
  return (
    binding?.status === "starting" ||
    binding?.status === "running" ||
    (binding?.status === "error" && hasDetachedSessionPersistence(binding.runtimePayload))
  );
}

export function isDetachedCodexCliObserverBinding(
  binding: ProviderRuntimeBinding | undefined,
): binding is ProviderRuntimeBinding {
  return (
    isLiveCodexBinding(binding) &&
    hasDetachedSessionPersistence(binding?.runtimePayload) &&
    readBindingActiveTurnId(binding?.runtimePayload) === null
  );
}

export function isDetachedCodexCliTranscriptRefreshBinding(
  binding: ProviderRuntimeBinding | undefined,
): binding is ProviderRuntimeBinding {
  return (
    isLiveCodexBinding(binding) &&
    hasDetachedSessionPersistence(binding?.runtimePayload) &&
    requiresCodexCliTranscriptRefresh(binding?.runtimePayload)
  );
}

function providerInstanceIdForBinding(binding: ProviderRuntimeBinding): ProviderInstanceId {
  return binding.providerInstanceId ?? defaultInstanceIdForDriver(binding.provider);
}

export function resolveCodexCliProviderRuntimeExpectation(
  binding: ProviderRuntimeBindingWithMetadata | undefined,
  requiresDetachedIdle: boolean,
): CodexCliProviderRuntimeExpectation | null {
  if (binding === undefined) {
    return null;
  }
  return {
    providerName: binding.provider,
    providerInstanceId: providerInstanceIdForBinding(binding),
    status: binding.status ?? "running",
    lastSeenAt: binding.lastSeenAt,
    resumeCursor: binding.resumeCursor ?? null,
    requiresDetachedIdle,
  };
}

function providerRuntimeExpectationCommandParts(
  expectation: CodexCliProviderRuntimeExpectation | null,
): ReadonlyArray<string> {
  return expectation === null
    ? ["provider-runtime-absent"]
    : [
        "provider-runtime",
        expectation.providerName,
        expectation.providerInstanceId,
        expectation.status,
        expectation.lastSeenAt,
        stableTextHash(encodeUnknownJsonString(expectation.resumeCursor)),
        expectation.requiresDetachedIdle ? "detached-idle" : "exact",
      ];
}

function isSameCodexCliProviderRuntimeOwner(
  left: ProviderRuntimeBinding,
  right: ProviderRuntimeBinding,
): boolean {
  return (
    left.provider === right.provider &&
    providerInstanceIdForBinding(left) === providerInstanceIdForBinding(right) &&
    readCodexResumeCursorThreadId(left.resumeCursor) ===
      readCodexResumeCursorThreadId(right.resumeCursor)
  );
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
  readonly observesDetachedCliSession?: boolean;
  readonly refreshesDetachedCliTranscript?: boolean;
}): boolean {
  return (
    input.rolloutPath !== undefined &&
    (input.staleActiveTurnId !== null ||
      input.hasDetachedMirrorSession ||
      input.observesDetachedCliSession === true ||
      input.refreshesDetachedCliTranscript === true)
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
type CodexCliRolloutTaskState = "active" | "completed" | "interrupted" | null;
type CodexCliThreadImportResult = "skipped" | "imported" | "recovering-live";

interface CodexCliRolloutTaskLifecycle {
  readonly state: CodexCliRolloutTaskState;
  readonly turnId: string | null;
}

interface CodexCliRolloutTaskCursor {
  readonly offset: number;
  readonly pendingLine: string;
  readonly lifecycle: CodexCliRolloutTaskLifecycle;
  readonly terminalTransitionObserved: boolean;
}

interface CodexCliRolloutMessageCursor {
  readonly offset: number;
  readonly modifiedAtMillis: number | undefined;
  readonly pendingLine: string;
  readonly messages: ReadonlyArray<CodexCliImportedMessage>;
  readonly activityTurnId: string | null;
  readonly activities: ReadonlyArray<OrchestrationThreadActivity>;
  readonly toolCalls: ReadonlyMap<string, CodexCliRolloutToolCall>;
}

interface CodexCliRolloutMessageRead {
  readonly messages: ReadonlyArray<CodexCliImportedMessage>;
  readonly complete: boolean;
}

interface CodexCliRolloutMessageCursorRead extends CodexCliRolloutMessageRead {
  readonly changed: boolean;
  readonly activities: ReadonlyArray<OrchestrationThreadActivity>;
  readonly activityChanged: boolean;
}

export function resolveCodexCliTranscriptMessages(input: {
  readonly thread: CodexReadThread;
  readonly rollout: CodexCliRolloutMessageRead | undefined;
}): CodexCliRolloutMessageRead {
  const appServerMessages = collectCodexCliImportedMessages(input.thread);
  if (input.rollout === undefined) {
    return {
      messages: appServerMessages,
      complete: isCodexCliThreadTranscriptComplete(input.thread),
    };
  }
  const appServerTurnIds = new Set(input.thread.turns.map((turn) => TurnId.make(turn.id)));
  const rolloutMessages =
    appServerTurnIds.size === 0
      ? input.rollout.messages
      : input.rollout.messages.filter((message) => appServerTurnIds.has(message.turnId));
  return {
    messages: mergeCodexCliRolloutMessages(appServerMessages, rolloutMessages),
    complete: input.rollout.complete,
  };
}

interface CodexCliRolloutTaskObservation {
  readonly offset: number;
  readonly lifecycle: CodexCliRolloutTaskLifecycle;
  readonly changed: boolean;
  readonly terminalTransitionObserved: boolean;
  readonly rolloutUpdatedAtMillis: number | undefined;
  readonly importIsFresh: boolean;
}

interface ObservedCodexCliSessionState {
  readonly status: "ready" | "running" | "interrupted" | "error";
  readonly activeTurnId: null;
  readonly lastError: string | null;
}

interface CodexCliThreadImportCandidate {
  readonly listedThread: CodexListedThread;
  readonly threadId: ThreadId;
  readonly existingBinding: ProviderRuntimeBindingWithMetadata | undefined;
  readonly observesDetachedCliSession: boolean;
  readonly refreshesDetachedCliTranscript: boolean;
}

export interface CodexCliRecoveredCheckpointRequest {
  readonly turnId: TurnId;
  readonly assistantMessageId: MessageId;
  readonly completedAt: string;
  readonly checkpointTurnCount: number;
  readonly checkpointRef: CheckpointRef;
}

export function resolveCodexCliRecoveredCheckpointRequest(input: {
  readonly threadId: ThreadId;
  readonly activeTurnId: string | null;
  readonly resolutionStatus: SettledCodexCliStaleSessionResolution["status"];
  readonly messages: ReadonlyArray<CodexCliImportedMessage>;
  readonly checkpointContext: ProjectionThreadCheckpointContext | undefined;
  readonly completedAt: string;
}): CodexCliRecoveredCheckpointRequest | undefined {
  if (
    input.activeTurnId === null ||
    input.resolutionStatus !== "ready" ||
    input.checkpointContext === undefined
  ) {
    return undefined;
  }
  const turnId = TurnId.make(input.activeTurnId);
  if (input.checkpointContext.checkpoints.some((checkpoint) => checkpoint.turnId === turnId)) {
    return undefined;
  }
  const assistantMessage = input.messages
    .toReversed()
    .find((message) => message.role === "assistant" && message.turnId === turnId);
  if (assistantMessage === undefined) {
    return undefined;
  }
  const checkpointTurnCount =
    input.checkpointContext.checkpoints.reduce(
      (maxTurnCount, checkpoint) => Math.max(maxTurnCount, checkpoint.checkpointTurnCount),
      0,
    ) + 1;
  return {
    turnId,
    assistantMessageId: assistantMessage.messageId,
    completedAt: input.completedAt,
    checkpointTurnCount,
    checkpointRef: CheckpointRef.make(
      `codex-cli-recovery:${input.threadId}:${turnId}:${checkpointTurnCount}`,
    ),
  };
}

interface PreparedCodexCliThreadImport {
  readonly listedThread: CodexListedThread;
  readonly threadId: ThreadId;
  readonly existingBinding: ProviderRuntimeBindingWithMetadata | undefined;
  readonly existingThread: Option.Option<OrchestrationThreadShell>;
  readonly projectedThread: OrchestrationThreadShell | undefined;
  readonly projectedThreadTranscript: ProjectionThreadTranscript | undefined;
  readonly staleSession: NonNullable<OrchestrationThreadShell["session"]> | undefined;
  readonly detachedMirrorSession: NonNullable<OrchestrationThreadShell["session"]> | undefined;
  readonly observesDetachedCliSession: boolean;
  readonly refreshesDetachedCliTranscript: boolean;
  readonly inspectDetachedCliSession: boolean;
  readonly importIsCurrent: boolean;
  readonly importedAtMillis: number | undefined;
  readonly rolloutPath: string | undefined;
}

interface CodexCliImportScanMetrics {
  threadReadCount: number;
  rolloutTailReadCount: number;
  skippedCurrentCount: number;
  failureBackoffCount: number;
}

export interface CodexCliImportFailureBackoff {
  readonly providerUpdatedAt: number;
  readonly failureCount: number;
  readonly retryAfterMillis: number;
}

export interface CodexCliProviderRuntimeExpectation {
  readonly providerName: ProviderDriverKind;
  readonly providerInstanceId: ProviderInstanceId;
  readonly status: ProviderSessionRuntimeStatus;
  readonly lastSeenAt: string;
  readonly resumeCursor: unknown | null;
  readonly requiresDetachedIdle: boolean;
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

function parseCodexRolloutTaskTransitions(contents: string): {
  readonly lifecycle: CodexCliRolloutTaskLifecycle;
  readonly terminalTransitionObserved: boolean;
} {
  const lines = contents.split(/\r?\n/u);
  let lifecycle: CodexCliRolloutTaskLifecycle = {
    state: null,
    turnId: null,
  };
  let terminalTransitionObserved = false;
  for (const line of lines) {
    if (!line?.includes('"type":"event_msg"') && !line?.includes('"type":"response_item"')) {
      continue;
    }
    const value = parseUnknownJsonRecord(line);
    if (value === undefined || !isUnknownRecord(value.payload)) {
      continue;
    }
    if (value.type === "response_item") {
      const responseTurnId = rolloutRecordTurnId(value.payload);
      if (responseTurnId !== undefined && responseTurnId !== lifecycle.turnId) {
        lifecycle = {
          state: null,
          turnId: responseTurnId,
        };
      }
      continue;
    }
    if (value.type !== "event_msg") {
      continue;
    }
    switch (value.payload.type) {
      case "task_started":
        lifecycle = {
          state: "active",
          turnId: typeof value.payload.turn_id === "string" ? value.payload.turn_id : null,
        };
        break;
      case "task_complete":
        lifecycle = {
          state: "completed",
          turnId: typeof value.payload.turn_id === "string" ? value.payload.turn_id : null,
        };
        terminalTransitionObserved = true;
        break;
      case "turn_aborted":
        lifecycle = {
          state: "interrupted",
          turnId: typeof value.payload.turn_id === "string" ? value.payload.turn_id : null,
        };
        terminalTransitionObserved = true;
        break;
      default:
        continue;
    }
  }
  return {
    lifecycle,
    terminalTransitionObserved,
  };
}

export function parseLatestCodexRolloutTaskState(contents: string): CodexCliRolloutTaskLifecycle {
  return parseCodexRolloutTaskTransitions(contents).lifecycle;
}

function splitCodexRolloutContents(
  contents: string,
  parseTrailingRecord = true,
): {
  readonly completeContents: string;
  readonly pendingLine: string;
} {
  const finalLineBreak = Math.max(contents.lastIndexOf("\n"), contents.lastIndexOf("\r"));
  const completeContents = finalLineBreak < 0 ? "" : contents.slice(0, finalLineBreak + 1);
  const pendingLine = finalLineBreak < 0 ? contents : contents.slice(finalLineBreak + 1);
  return parseTrailingRecord &&
    pendingLine.length > 0 &&
    parseUnknownJsonRecord(pendingLine) !== undefined
    ? {
        completeContents: `${completeContents}${pendingLine}`,
        pendingLine: "",
      }
    : {
        completeContents,
        pendingLine,
      };
}

export function advanceCodexRolloutTaskCursor(
  cursor: CodexCliRolloutTaskCursor | undefined,
  contents: string,
  offset: number,
): CodexCliRolloutTaskCursor {
  const combined = `${cursor?.pendingLine ?? ""}${contents}`;
  const { completeContents, pendingLine } = splitCodexRolloutContents(combined);
  const observed = parseCodexRolloutTaskTransitions(completeContents);
  const observedTurnChanged =
    observed.lifecycle.turnId !== null &&
    cursor?.lifecycle.turnId !== null &&
    cursor?.lifecycle.turnId !== undefined &&
    observed.lifecycle.turnId !== cursor.lifecycle.turnId;
  return {
    offset,
    pendingLine,
    lifecycle:
      observed.lifecycle.state === null
        ? {
            state: observedTurnChanged ? null : (cursor?.lifecycle.state ?? null),
            turnId: observed.lifecycle.turnId ?? cursor?.lifecycle.turnId ?? null,
          }
        : observed.lifecycle,
    terminalTransitionObserved: observed.terminalTransitionObserved,
  };
}

export function advanceCodexRolloutMessageCursor(input: {
  readonly cursor: CodexCliRolloutMessageCursor | undefined;
  readonly contents: string;
  readonly offset: number;
  readonly modifiedAtMillis: number | undefined;
  readonly threadId: string;
  readonly createdAt: number;
  readonly activityTurnId?: string | null;
  readonly isFinalChunk?: boolean;
}): CodexCliRolloutMessageCursor {
  const combined = `${input.cursor?.pendingLine ?? ""}${input.contents}`;
  const { completeContents, pendingLine } = splitCodexRolloutContents(
    combined,
    input.isFinalChunk ?? true,
  );
  const records = completeContents.length === 0 ? [] : parseCodexRolloutRecords(completeContents);
  const appendedMessages =
    records.length === 0
      ? []
      : collectCodexCliRolloutMessagesFromRecords({
          threadId: input.threadId,
          records,
          createdAt: input.createdAt,
        });
  const activityTurnId = input.activityTurnId ?? null;
  const parsedActivities =
    records.length === 0
      ? {
          activities: [] as ReadonlyArray<OrchestrationThreadActivity>,
          toolCalls:
            input.cursor?.activityTurnId === activityTurnId
              ? input.cursor.toolCalls
              : new Map<string, CodexCliRolloutToolCall>(),
        }
      : collectCodexCliRolloutActivitiesFromRecords({
          threadId: input.threadId,
          records,
          createdAt: input.createdAt,
          turnId: activityTurnId,
          toolCalls:
            input.cursor?.activityTurnId === activityTurnId ? input.cursor.toolCalls : new Map(),
        });
  return {
    offset: input.offset,
    modifiedAtMillis: input.modifiedAtMillis,
    pendingLine,
    messages: mergeCodexCliRolloutMessages(input.cursor?.messages ?? [], appendedMessages),
    activityTurnId,
    activities: parsedActivities.activities,
    toolCalls: parsedActivities.toolCalls,
  };
}

export function isCompleteCodexRolloutMessageRead(
  cursor: Pick<CodexCliRolloutMessageCursor, "offset" | "pendingLine"> | undefined,
  expectedSize: number,
): boolean {
  return cursor !== undefined && cursor.offset === expectedSize && cursor.pendingLine.length === 0;
}

export function codexRolloutCompleteUtf8PrefixLength(bytes: Uint8Array): number {
  if (bytes.length === 0) {
    return 0;
  }
  let leadIndex = bytes.length - 1;
  while (leadIndex >= 0 && (bytes[leadIndex]! & 0xc0) === 0x80) {
    leadIndex -= 1;
  }
  if (leadIndex < 0) {
    return bytes.length;
  }
  const lead = bytes[leadIndex]!;
  const expectedLength =
    lead < 0x80
      ? 1
      : (lead & 0xe0) === 0xc0
        ? 2
        : (lead & 0xf0) === 0xe0
          ? 3
          : (lead & 0xf8) === 0xf0
            ? 4
            : 1;
  return bytes.length - leadIndex < expectedLength ? leadIndex : bytes.length;
}

export function resolveObservedCodexCliSessionState(input: {
  readonly taskState: CodexCliRolloutTaskState;
  readonly listedThreadIsActive: boolean;
  readonly listedThreadHasSystemError: boolean;
  readonly rolloutEvidenceAvailable: boolean;
  readonly rolloutIsOpen: boolean;
  readonly rolloutIsRecent: boolean;
}): ObservedCodexCliSessionState {
  if (input.taskState === "interrupted") {
    return {
      status: "interrupted",
      activeTurnId: null,
      lastError: "The Codex turn was interrupted before it produced a final response.",
    };
  }
  if (input.taskState === "completed") {
    return {
      status: "ready",
      activeTurnId: null,
      lastError: null,
    };
  }
  if (input.taskState === "active") {
    if (input.rolloutIsOpen || input.rolloutIsRecent) {
      return {
        status: "running",
        activeTurnId: null,
        lastError: null,
      };
    }
    return {
      status: "interrupted",
      activeTurnId: null,
      lastError: input.rolloutIsOpen
        ? "The Codex turn stopped producing activity before it produced a final response."
        : "The Codex process ended before it produced a final response.",
    };
  }
  if (input.rolloutEvidenceAvailable) {
    if (input.rolloutIsOpen || input.rolloutIsRecent) {
      return {
        status: "running",
        activeTurnId: null,
        lastError: null,
      };
    }
    if (input.listedThreadHasSystemError) {
      return {
        status: "error",
        activeTurnId: null,
        lastError: "Codex reported a system error for this thread.",
      };
    }
    return {
      status: "ready",
      activeTurnId: null,
      lastError: null,
    };
  }
  if (input.listedThreadHasSystemError) {
    return {
      status: "error",
      activeTurnId: null,
      lastError: "Codex reported a system error for this thread.",
    };
  }
  if (input.listedThreadIsActive) {
    return {
      status: "running",
      activeTurnId: null,
      lastError: null,
    };
  }
  return {
    status: "ready",
    activeTurnId: null,
    lastError: null,
  };
}

export function shouldApplyObservedCodexCliSessionState(input: {
  readonly currentThread:
    | Pick<OrchestrationThreadShell, "latestUserMessageAt" | "session" | "updatedAt">
    | undefined;
  readonly preparedThread:
    | Pick<OrchestrationThreadShell, "latestUserMessageAt" | "session" | "updatedAt">
    | undefined;
}): boolean {
  if (input.currentThread === undefined) {
    return false;
  }
  if (input.preparedThread === undefined) {
    return input.currentThread.session === null && input.currentThread.latestUserMessageAt === null;
  }
  return (
    input.currentThread.updatedAt === input.preparedThread.updatedAt &&
    orchestrationSessionsEqual(input.currentThread.session, input.preparedThread.session)
  );
}

function codexCliSessionMatchesObservedState(
  session: OrchestrationSession | null | undefined,
  state: {
    readonly status: "ready" | "running" | "interrupted" | "error";
    readonly activeTurnId: TurnId | null;
    readonly lastError: string | null;
  },
): boolean {
  return (
    session?.status === state.status &&
    (session.activeTurnId ?? null) === state.activeTurnId &&
    session.lastError === state.lastError &&
    !session.retrying
  );
}

export function resolveObservedCodexCliSessionSyncAction(input: {
  readonly currentThread:
    | Pick<OrchestrationThreadShell, "latestUserMessageAt" | "session" | "updatedAt">
    | undefined;
  readonly preparedThread:
    | Pick<OrchestrationThreadShell, "latestUserMessageAt" | "session" | "updatedAt">
    | undefined;
  readonly observedState: {
    readonly status: "ready" | "running" | "interrupted" | "error";
    readonly activeTurnId: TurnId | null;
    readonly lastError: string | null;
  };
}): "apply" | "skip" | "synchronized" {
  if (input.currentThread === undefined) {
    return "skip";
  }
  if (codexCliSessionMatchesObservedState(input.currentThread.session, input.observedState)) {
    return "synchronized";
  }
  return shouldApplyObservedCodexCliSessionState({
    currentThread: input.currentThread,
    preparedThread: input.preparedThread,
  })
    ? "apply"
    : "skip";
}

export function shouldSynchronizeObservedCodexCliSessionBeforeHydration(state: {
  readonly status: "ready" | "running" | "interrupted" | "error";
}): boolean {
  return state.status === "running";
}

export function resolveCodexCliExpectedUserMessageIds(
  projectedThread: Pick<OrchestrationThread, "messages"> | undefined,
  importedMessages: ReadonlyArray<CodexCliImportedMessage>,
): ReadonlyArray<MessageId> {
  const messages =
    projectedThread?.messages.map((message) => ({
      id: message.id,
      role: message.role,
      createdAt: message.createdAt,
    })) ?? [];
  const messageIndexes = new Map<MessageId, number>();
  for (const [index, message] of messages.entries()) {
    if (!messageIndexes.has(message.id)) {
      messageIndexes.set(message.id, index);
    }
  }
  for (const imported of importedMessages) {
    const existingIndex = messageIndexes.get(imported.messageId) ?? -1;
    const nextMessage = {
      id: imported.messageId,
      role: imported.role,
      createdAt: imported.createdAt,
    };
    if (existingIndex >= 0) {
      messages[existingIndex] = {
        ...nextMessage,
        createdAt: messages[existingIndex]?.createdAt ?? imported.createdAt,
      };
    } else {
      messages.push(nextMessage);
      messageIndexes.set(imported.messageId, messages.length - 1);
    }
  }
  return messages
    .toSorted(
      (left, right) =>
        left.createdAt.localeCompare(right.createdAt) ||
        Buffer.compare(Buffer.from(left.id, "utf8"), Buffer.from(right.id, "utf8")),
    )
    .filter((message) => message.role === "user")
    .map((message) => message.id);
}

export function hasSynchronizedCodexCliTranscript(
  projectedThread: Pick<OrchestrationThread, "messages"> | undefined,
  importedMessages: ReadonlyArray<CodexCliImportedMessage>,
): boolean {
  if (projectedThread === undefined) {
    return importedMessages.length === 0;
  }
  const projectedMessagesById = indexCodexCliProjectedMessagesById(projectedThread);
  return importedMessages.every((imported) =>
    projectedMessagesById
      .get(imported.messageId)
      ?.some((projected) => codexCliTranscriptMessagesMatch(projected, imported)),
  );
}

function orchestrationSessionsEqual(
  left: OrchestrationSession | null,
  right: OrchestrationSession | null,
): boolean {
  if (left === null || right === null) {
    return left === right;
  }
  return (
    left.threadId === right.threadId &&
    left.status === right.status &&
    left.providerName === right.providerName &&
    left.providerInstanceId === right.providerInstanceId &&
    left.runtimeMode === right.runtimeMode &&
    left.activeTurnId === right.activeTurnId &&
    left.lastError === right.lastError &&
    (left.retrying ?? false) === (right.retrying ?? false) &&
    left.updatedAt === right.updatedAt
  );
}

export function pruneCodexCliImportCache<K, V>(
  cache: Map<K, V>,
  retainedKeys: ReadonlySet<K>,
): void {
  for (const key of cache.keys()) {
    if (!retainedKeys.has(key)) {
      cache.delete(key);
    }
  }
}

export function shouldBackoffCodexCliImportFailure(input: {
  readonly failure: CodexCliImportFailureBackoff | undefined;
  readonly providerUpdatedAt: number;
  readonly nowMillis: number;
}): boolean {
  return (
    input.failure !== undefined &&
    input.failure.providerUpdatedAt === input.providerUpdatedAt &&
    input.nowMillis < input.failure.retryAfterMillis
  );
}

export function advanceCodexCliImportFailureBackoff(input: {
  readonly previous: CodexCliImportFailureBackoff | undefined;
  readonly providerUpdatedAt: number;
  readonly failedAtMillis: number;
}): CodexCliImportFailureBackoff {
  const failureCount =
    input.previous?.providerUpdatedAt === input.providerUpdatedAt
      ? input.previous.failureCount + 1
      : 1;
  const delayMillis = Math.min(
    CODEX_CLI_IMPORT_FAILURE_BACKOFF_INITIAL_MS * 2 ** Math.min(failureCount - 1, 16),
    CODEX_CLI_IMPORT_FAILURE_BACKOFF_MAX_MS,
  );
  return {
    providerUpdatedAt: input.providerUpdatedAt,
    failureCount,
    retryAfterMillis: input.failedAtMillis + delayMillis,
  };
}

const readCodexRolloutTail = Effect.fn("CodexCliSessionImporter.readRolloutTail")(function* (
  fileSystem: FileSystem.FileSystem,
  rolloutPath: string,
  maxBytes = ROLLOUT_TERMINAL_EVENT_TAIL_BYTES,
) {
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const file = yield* fileSystem.open(rolloutPath, { flag: "r" });
      const stat = yield* file.stat;
      const size = Number(stat.size);
      const length = Math.min(size, maxBytes);
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
});

const readCodexRolloutActivityTail = Effect.fn("CodexCliSessionImporter.readRolloutActivityTail")(
  function* (
    fileSystem: FileSystem.FileSystem,
    rolloutPath: string,
    threadId: string,
    createdAt: number,
    turnId: string,
  ) {
    const contents = yield* readCodexRolloutTail(
      fileSystem,
      rolloutPath,
      ROLLOUT_ACTIVITY_TAIL_BYTES,
    );
    return collectCodexCliRolloutActivities({
      threadId,
      contents,
      createdAt,
      turnId,
    }).slice(-CODEX_CLI_COLD_ACTIVITY_LIMIT);
  },
);

const readCodexRolloutTerminalEvidence = Effect.fn(
  "CodexCliSessionImporter.readRolloutTerminalEvidence",
)(function* (fileSystem: FileSystem.FileSystem, rolloutPath: string, turnId: string) {
  const contents = yield* readCodexRolloutTail(fileSystem, rolloutPath);
  return parseCodexRolloutTerminalEvidence(contents, turnId);
});

export function isCurrentCodexCliImport(
  binding: ProviderRuntimeBinding | undefined,
  listedThread: CodexListedThread,
): boolean {
  return (
    hasCurrentCodexCliImportVersion(binding) &&
    readImportedCodexUpdatedAt(binding?.runtimePayload) === listedThread.updatedAt
  );
}

function observedCodexCliSessionStatus(
  listedThread: CodexListedThread,
): "ready" | "running" | "error" {
  return listedThread.status.type === "active"
    ? "running"
    : listedThread.status.type === "systemError"
      ? "error"
      : "ready";
}

export function isCodexCliImportFreshForRollout(
  importedAtMillis: number | undefined,
  rolloutUpdatedAtMillis: number | undefined,
): boolean {
  return (
    importedAtMillis !== undefined &&
    rolloutUpdatedAtMillis !== undefined &&
    importedAtMillis >= rolloutUpdatedAtMillis
  );
}

export function shouldImportCodexCliMessages(input: {
  readonly importIsCurrent: boolean;
  readonly hasStaleSession: boolean;
  readonly observesDetachedCliSession: boolean;
  readonly observerNeedsHydration: boolean;
  readonly refreshesDetachedCliTranscript?: boolean;
}): boolean {
  return (
    !input.importIsCurrent ||
    input.hasStaleSession ||
    (input.observesDetachedCliSession && input.observerNeedsHydration) ||
    input.refreshesDetachedCliTranscript === true
  );
}

export function shouldReadCodexRolloutActivities(input: {
  readonly rolloutPathAvailable: boolean;
  readonly activityTurnId: string | null;
  readonly listedThreadIsRecent: boolean;
  readonly importIsCurrent: boolean;
  readonly binding: ProviderRuntimeBinding | undefined;
  readonly observesDetachedCliSession: boolean;
  readonly refreshesDetachedCliTranscript?: boolean;
}): boolean {
  return (
    input.rolloutPathAvailable &&
    input.activityTurnId !== null &&
    (input.observesDetachedCliSession ||
      input.refreshesDetachedCliTranscript === true ||
      (input.listedThreadIsRecent &&
        !input.importIsCurrent &&
        (input.binding === undefined || isCodexCliImportedBinding(input.binding))))
  );
}

export function shouldPersistCodexCliImportMetadata(input: {
  readonly observesDetachedCliSession: boolean;
  readonly observerSessionSynchronized: boolean;
  readonly transcriptHydrationComplete?: boolean;
  readonly transcriptSynchronized?: boolean;
}): boolean {
  return (
    input.transcriptHydrationComplete !== false &&
    input.transcriptSynchronized !== false &&
    (!input.observesDetachedCliSession || input.observerSessionSynchronized)
  );
}

export function shouldHydrateObservedCodexCliTranscript(input: {
  readonly observesDetachedCliSession: boolean;
  readonly importIsCurrent: boolean;
  readonly rolloutTranscriptInspected: boolean;
  readonly rolloutTranscriptChanged: boolean;
  readonly rolloutTranscriptComplete: boolean;
  readonly rolloutTranscriptSynchronized: boolean;
  readonly terminalTransitionObserved: boolean;
}): boolean {
  return (
    input.observesDetachedCliSession &&
    (input.terminalTransitionObserved ||
      (input.rolloutTranscriptInspected
        ? (input.rolloutTranscriptChanged || !input.importIsCurrent) &&
          (!input.rolloutTranscriptComplete || !input.rolloutTranscriptSynchronized)
        : !input.importIsCurrent))
  );
}

export function shouldUseCodexRolloutTranscriptWithoutThreadRead(input: {
  readonly observesDetachedCliSession: boolean;
  readonly observedSessionStatus: "ready" | "running" | "interrupted" | "error";
  readonly rolloutTranscriptComplete: boolean;
  readonly terminalTransitionObserved: boolean;
}): boolean {
  return (
    input.observesDetachedCliSession &&
    input.observedSessionStatus === "running" &&
    input.rolloutTranscriptComplete &&
    !input.terminalTransitionObserved
  );
}

export function shouldSkipUnchangedDetachedCodexCliObserver(input: {
  readonly observesDetachedCliSession: boolean;
  readonly observerNeedsHydration: boolean;
  readonly rolloutChanged: boolean;
  readonly inspectDetachedCliSession: boolean;
}): boolean {
  return (
    input.observesDetachedCliSession &&
    !input.observerNeedsHydration &&
    !input.rolloutChanged &&
    !input.inspectDetachedCliSession
  );
}

export function shouldInspectDetachedCodexCliObserver(input: {
  readonly binding: ProviderRuntimeBinding | undefined;
  readonly listedThread: CodexListedThread;
  readonly projectedSessionStatus: string | undefined;
}): boolean {
  return (
    isDetachedCodexCliObserverBinding(input.binding) &&
    (input.listedThread.status.type === "active" ||
      input.projectedSessionStatus === "starting" ||
      input.projectedSessionStatus === "running" ||
      (input.listedThread.status.type === "systemError" &&
        input.projectedSessionStatus !== observedCodexCliSessionStatus(input.listedThread)))
  );
}

function hasCurrentCodexCliImportVersion(binding: ProviderRuntimeBinding | undefined): boolean {
  return readCodexCliImportVersion(binding?.runtimePayload) === CODEX_CLI_IMPORT_VERSION;
}

export function shouldSkipCurrentCodexCliImport(
  binding: ProviderRuntimeBinding | undefined,
  listedThread: CodexListedThread,
  hasStaleSession: boolean,
): boolean {
  return isCurrentCodexCliImport(binding, listedThread) && !hasStaleSession;
}

export function isRecentCodexCliActivity(updatedAt: number, nowMillis: number): boolean {
  return nowMillis - unixSecondsToMillis(updatedAt, 0) <= CODEX_CLI_LIVE_INACTIVITY_GRACE_MS;
}

export function isRecentCodexCliRolloutActivity(
  rolloutUpdatedAtMillis: number | undefined,
  nowMillis: number,
): boolean {
  return (
    rolloutUpdatedAtMillis !== undefined &&
    nowMillis - rolloutUpdatedAtMillis <= CODEX_CLI_LIVE_INACTIVITY_GRACE_MS
  );
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
    const rolloutTaskCursors = new Map<string, CodexCliRolloutTaskCursor>();
    const rolloutMessageCursors = new Map<string, CodexCliRolloutMessageCursor>();
    const observedImportedUpdatedAt = new Map<ThreadId, number>();
    const importFailureBackoffs = new Map<string, CodexCliImportFailureBackoff>();

    const readCodexRolloutMessages = Effect.fn("CodexCliSessionImporter.readRolloutMessages")(
      function* (
        rolloutPath: string,
        threadId: string,
        createdAt: number,
        activityTurnId: string | null,
      ) {
        return yield* Effect.scoped(
          Effect.gen(function* () {
            const file = yield* fileSystem.open(rolloutPath, { flag: "r" });
            const stat = yield* file.stat;
            const size = Number(stat.size);
            const modifiedAtMillis = Option.getOrUndefined(stat.mtime)?.getTime();
            const cached = rolloutMessageCursors.get(rolloutPath);
            if (
              cached !== undefined &&
              cached.offset === size &&
              cached.modifiedAtMillis === modifiedAtMillis
            ) {
              return {
                messages: cached.messages,
                complete: isCompleteCodexRolloutMessageRead(cached, size),
                changed: false,
                activities: cached.activities,
                activityChanged: false,
              } satisfies CodexCliRolloutMessageCursorRead;
            }
            const canContinue =
              cached !== undefined &&
              cached.offset < size &&
              (cached.modifiedAtMillis === undefined ||
                modifiedAtMillis === undefined ||
                cached.modifiedAtMillis <= modifiedAtMillis);
            const start = canContinue ? cached.offset : 0;
            const length = size - start;
            if (length <= 0) {
              const resetCachedHistory = cached !== undefined && !canContinue;
              const cursor = {
                offset: size,
                modifiedAtMillis,
                pendingLine: "",
                messages: [],
                activityTurnId,
                activities: resetCachedHistory ? [] : (cached?.activities ?? []),
                toolCalls: new Map(),
              } satisfies CodexCliRolloutMessageCursor;
              rolloutMessageCursors.set(rolloutPath, cursor);
              return {
                messages: cursor.messages,
                complete: isCompleteCodexRolloutMessageRead(cursor, size),
                changed: cached !== undefined && cached.messages.length > 0,
                activities: cursor.activities,
                activityChanged: resetCachedHistory && cached.activities.length > 0,
              } satisfies CodexCliRolloutMessageCursorRead;
            }
            yield* file.seek(start, "start");
            const existingMessages = canContinue ? cached.messages : [];
            const appendedMessages: CodexCliImportedMessage[] = [];
            const appendedActivities: OrchestrationThreadActivity[] = [];
            const decoder = new TextDecoder();
            let parserCursor =
              canContinue && cached !== undefined
                ? {
                    ...cached,
                    messages: [],
                    activities: [],
                  }
                : undefined;
            let pendingUtf8Bytes = new Uint8Array();
            let readOffset = start;
            while (readOffset < size) {
              const bytes = yield* file.readAlloc(
                Math.min(ROLLOUT_MESSAGE_READ_CHUNK_BYTES, size - readOffset),
              );
              if (Option.isNone(bytes) || bytes.value.length === 0) {
                break;
              }
              readOffset += bytes.value.length;
              const combinedBytes =
                pendingUtf8Bytes.length === 0
                  ? bytes.value
                  : new Uint8Array(pendingUtf8Bytes.length + bytes.value.length);
              if (pendingUtf8Bytes.length > 0) {
                combinedBytes.set(pendingUtf8Bytes);
                combinedBytes.set(bytes.value, pendingUtf8Bytes.length);
              }
              const completePrefixLength = codexRolloutCompleteUtf8PrefixLength(combinedBytes);
              const completeBytes = combinedBytes.subarray(0, completePrefixLength);
              pendingUtf8Bytes = combinedBytes.slice(completePrefixLength);
              if (completeBytes.length > 0) {
                const parsedCursor = advanceCodexRolloutMessageCursor({
                  cursor: parserCursor,
                  contents: decoder.decode(completeBytes),
                  offset: readOffset - pendingUtf8Bytes.length,
                  modifiedAtMillis,
                  threadId,
                  createdAt,
                  activityTurnId,
                  isFinalChunk: readOffset === size && pendingUtf8Bytes.length === 0,
                });
                appendedMessages.push(...parsedCursor.messages);
                appendedActivities.push(...parsedCursor.activities);
                parserCursor = {
                  ...parsedCursor,
                  messages: [],
                  activities: [],
                };
              }
              if (readOffset < size) {
                yield* Effect.yieldNow;
              }
            }
            const cursor = {
              offset: parserCursor?.offset ?? readOffset - pendingUtf8Bytes.length,
              modifiedAtMillis,
              pendingLine:
                parserCursor?.pendingLine ??
                (canContinue && cached !== undefined ? cached.pendingLine : ""),
              messages: mergeCodexCliRolloutMessages(existingMessages, appendedMessages),
              activityTurnId,
              activities: mergeCodexCliRolloutActivities(
                canContinue ? (cached?.activities ?? []) : [],
                appendedActivities,
              ),
              toolCalls:
                parserCursor?.activityTurnId === activityTurnId
                  ? parserCursor.toolCalls
                  : new Map(),
            } satisfies CodexCliRolloutMessageCursor;
            rolloutMessageCursors.set(rolloutPath, cursor);
            return {
              messages: cursor.messages,
              complete: isCompleteCodexRolloutMessageRead(cursor, size),
              changed:
                appendedMessages.length > 0 ||
                (cached !== undefined &&
                  !canContinue &&
                  (cached.offset !== cursor.offset ||
                    cached.messages.length > 0 ||
                    cached.pendingLine.length > 0)),
              activities: cursor.activities,
              activityChanged:
                appendedActivities.length > 0 ||
                (cached !== undefined && !canContinue && cached.activities.length > 0),
            } satisfies CodexCliRolloutMessageCursorRead;
          }),
        ).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("codex.cli-import.rollout-fallback-failed", {
              providerThreadId: threadId,
              rolloutPath,
              cause,
            }).pipe(
              Effect.as({
                messages: [],
                complete: false,
                changed: false,
                activities: [],
                activityChanged: false,
              } satisfies CodexCliRolloutMessageCursorRead),
            ),
          ),
        );
      },
    );

    const acknowledgeCodexRolloutActivities = (
      rolloutPath: string,
      activities: ReadonlyArray<OrchestrationThreadActivity>,
    ) => {
      const cursor = rolloutMessageCursors.get(rolloutPath);
      if (cursor === undefined || activities.length === 0) {
        return;
      }
      const acknowledgedIds = new Set(activities.map((activity) => activity.id));
      rolloutMessageCursors.set(rolloutPath, {
        ...cursor,
        activities: cursor.activities.filter((activity) => !acknowledgedIds.has(activity.id)),
      });
    };

    const observeCodexRolloutTaskState = Effect.fn(
      "CodexCliSessionImporter.observeRolloutTaskState",
    )(function* (
      rolloutPath: string,
      initializeWithoutRead: boolean,
      importedAtMillis: number | undefined,
    ) {
      return yield* Effect.scoped(
        Effect.gen(function* () {
          const file = yield* fileSystem.open(rolloutPath, { flag: "r" });
          const stat = yield* file.stat;
          const size = Number(stat.size);
          const rolloutUpdatedAtMillis = Option.getOrUndefined(stat.mtime)?.getTime();
          const importIsFresh = isCodexCliImportFreshForRollout(
            importedAtMillis,
            rolloutUpdatedAtMillis,
          );
          const cached = rolloutTaskCursors.get(rolloutPath);
          if (cached !== undefined && cached.offset === size) {
            return {
              offset: cached.offset,
              lifecycle: cached.lifecycle,
              changed: false,
              terminalTransitionObserved: false,
              rolloutUpdatedAtMillis,
              importIsFresh,
            } satisfies CodexCliRolloutTaskObservation;
          }
          if (cached === undefined && initializeWithoutRead && importIsFresh) {
            rolloutTaskCursors.set(rolloutPath, {
              offset: size,
              pendingLine: "",
              lifecycle: {
                state: null,
                turnId: null,
              },
              terminalTransitionObserved: false,
            });
            return {
              offset: size,
              lifecycle: {
                state: null,
                turnId: null,
              },
              changed: false,
              terminalTransitionObserved: false,
              rolloutUpdatedAtMillis,
              importIsFresh,
            } satisfies CodexCliRolloutTaskObservation;
          }

          const canContinue = cached !== undefined && cached.offset < size;
          const skippedIncrementalBytes =
            canContinue && size - cached.offset > ROLLOUT_TERMINAL_EVENT_TAIL_BYTES;
          const start =
            canContinue && !skippedIncrementalBytes
              ? cached.offset
              : Math.max(0, size - ROLLOUT_TERMINAL_EVENT_TAIL_BYTES);
          const length = size - start;
          if (length <= 0) {
            const cursor = {
              offset: size,
              pendingLine: "",
              lifecycle: {
                state: null,
                turnId: null,
              },
              terminalTransitionObserved: false,
            } satisfies CodexCliRolloutTaskCursor;
            rolloutTaskCursors.set(rolloutPath, cursor);
            return {
              offset: cursor.offset,
              lifecycle: cursor.lifecycle,
              changed: cached?.offset !== cursor.offset,
              terminalTransitionObserved: false,
              rolloutUpdatedAtMillis,
              importIsFresh,
            } satisfies CodexCliRolloutTaskObservation;
          }

          yield* file.seek(start, "start");
          const bytes = yield* file.readAlloc(length);
          const contents = Option.match(bytes, {
            onNone: () => "",
            onSome: (value) => new TextDecoder().decode(value),
          });
          const cursor = advanceCodexRolloutTaskCursor(
            canContinue && !skippedIncrementalBytes ? cached : undefined,
            contents,
            size,
          );
          rolloutTaskCursors.set(rolloutPath, cursor);
          return {
            offset: cursor.offset,
            lifecycle: cursor.lifecycle,
            changed: true,
            terminalTransitionObserved: cursor.terminalTransitionObserved,
            rolloutUpdatedAtMillis,
            importIsFresh,
          } satisfies CodexCliRolloutTaskObservation;
        }),
      ).pipe(
        Effect.orElseSucceed(
          () =>
            ({
              offset: 0,
              lifecycle: {
                state: null,
                turnId: null,
              },
              changed: false,
              terminalTransitionObserved: false,
              rolloutUpdatedAtMillis: undefined,
              importIsFresh: false,
            }) satisfies CodexCliRolloutTaskObservation,
        ),
      );
    });

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
      const observesDetachedCliSession = isDetachedCodexCliObserverBinding(existingBinding);
      const refreshesDetachedCliTranscript =
        !observesDetachedCliSession && isDetachedCodexCliTranscriptRefreshBinding(existingBinding);
      if (
        isLiveCodexBinding(existingBinding) &&
        !observesDetachedCliSession &&
        !refreshesDetachedCliTranscript
      ) {
        return undefined;
      }

      return {
        listedThread,
        threadId,
        existingBinding,
        observesDetachedCliSession,
        refreshesDetachedCliTranscript,
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
      const {
        listedThread,
        threadId,
        existingBinding,
        observesDetachedCliSession,
        refreshesDetachedCliTranscript,
      } = candidate;
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
      const importIsCurrent =
        isCurrentCodexCliImport(existingBinding, listedThread) ||
        observedImportedUpdatedAt.get(threadId) === listedThread.updatedAt;
      const inspectDetachedCliSession = shouldInspectDetachedCodexCliObserver({
        binding: existingBinding,
        listedThread,
        projectedSessionStatus: projectedThread?.session?.status,
      });
      const importedAtMillis = readCodexCliImportedAt(existingBinding?.runtimePayload);
      // Currentness is persisted independently of the active thread projection.
      // Archived threads are intentionally absent from active shell queries and
      // must not replay their complete transcripts on every periodic scan.
      if (
        shouldSkipCurrentCodexCliImport(
          existingBinding,
          listedThread,
          staleSession !== undefined || shouldInspectCurrentMirror,
        ) &&
        !observesDetachedCliSession &&
        !refreshesDetachedCliTranscript
      ) {
        metrics.skippedCurrentCount += 1;
        return undefined;
      }
      const projectedThreadTranscript =
        projectedThread !== undefined &&
        (staleSession !== undefined || detachedMirrorSession !== undefined)
          ? Option.getOrUndefined(yield* projectionSnapshotQuery.getThreadTranscriptById(threadId))
          : undefined;

      return {
        listedThread,
        threadId,
        existingBinding,
        existingThread,
        projectedThread,
        projectedThreadTranscript,
        staleSession,
        detachedMirrorSession,
        observesDetachedCliSession,
        refreshesDetachedCliTranscript,
        inspectDetachedCliSession,
        importIsCurrent,
        importedAtMillis,
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
      expectedUserMessageIds: ReadonlyArray<MessageId>,
      binding: ProviderRuntimeBindingWithMetadata | undefined,
      resolution: SettledCodexCliStaleSessionResolution,
      rolloutTerminalState: CodexCliRolloutTerminalState,
    ) {
      const settledAt = DateTime.formatIso(yield* DateTime.now);
      const expectedProviderRuntime = resolveCodexCliProviderRuntimeExpectation(binding, false);
      yield* orchestrationEngine.dispatch({
        type: "thread.session.set",
        commandId: stableCommandId(
          "session-reconciled",
          resolution.status,
          threadId,
          staleSession.activeTurnId ?? staleSession.updatedAt,
          stableTextHash(expectedUserMessageIds.join("\0")),
          ...providerRuntimeExpectationCommandParts(expectedProviderRuntime),
          settledAt,
        ),
        threadId,
        expectedSession: staleSession,
        expectedUserMessageIds,
        expectedProviderRuntime,
        session: {
          ...staleSession,
          status: resolution.status,
          activeTurnId: null,
          lastError: resolution.lastError,
          updatedAt: settledAt,
        },
        createdAt: settledAt,
      });
      const settledThread = (yield* projectionSnapshotQuery.getThreadShellsByIds([threadId])).get(
        threadId,
      );
      if (
        settledThread?.session?.updatedAt !== settledAt ||
        settledThread.session.status !== resolution.status ||
        settledThread.session.activeTurnId !== null
      ) {
        return undefined;
      }
      yield* Effect.logInfo("codex.cli-import.reconciled-stale-session", {
        threadId,
        previousStatus: staleSession.status,
        previousActiveTurnId: staleSession.activeTurnId,
        reconciledStatus: resolution.status,
        rolloutTerminalState,
      });
      return settledAt;
    });

    const recoverCodexCliCheckpoint = Effect.fn("CodexCliSessionImporter.recoverCheckpoint")(
      function* (
        threadId: ThreadId,
        activeTurnId: string | null,
        resolutionStatus: SettledCodexCliStaleSessionResolution["status"],
        messages: ReadonlyArray<CodexCliImportedMessage>,
        completedAt: string,
      ) {
        const checkpointContext = Option.getOrUndefined(
          yield* projectionSnapshotQuery.getThreadCheckpointContext(threadId),
        );
        const request = resolveCodexCliRecoveredCheckpointRequest({
          threadId,
          activeTurnId,
          resolutionStatus,
          messages,
          checkpointContext,
          completedAt,
        });
        if (request === undefined) {
          return;
        }
        yield* orchestrationEngine.dispatch({
          type: "thread.turn.diff.complete",
          commandId: stableCommandId(
            "checkpoint-recovery",
            threadId,
            request.turnId,
            request.assistantMessageId,
            String(request.checkpointTurnCount),
          ),
          threadId,
          turnId: request.turnId,
          completedAt: request.completedAt,
          checkpointRef: request.checkpointRef,
          status: "missing",
          files: [],
          assistantMessageId: request.assistantMessageId,
          checkpointTurnCount: request.checkpointTurnCount,
          createdAt: request.completedAt,
        });
        yield* Effect.logInfo("codex.cli-import.checkpoint-recovery-requested", {
          threadId,
          turnId: request.turnId,
          checkpointTurnCount: request.checkpointTurnCount,
        });
      },
    );

    const setMirroredSessionRunning = Effect.fn(
      "CodexCliSessionImporter.setMirroredSessionRunning",
    )(function* (
      threadId: ThreadId,
      session: NonNullable<OrchestrationThreadShell["session"]>,
      expectedUserMessageIds: ReadonlyArray<MessageId>,
      binding: ProviderRuntimeBindingWithMetadata | undefined,
      activeTurnId: TurnId,
    ) {
      const recoveredAt = DateTime.formatIso(yield* DateTime.now);
      const expectedProviderRuntime = resolveCodexCliProviderRuntimeExpectation(binding, false);
      yield* orchestrationEngine.dispatch({
        type: "thread.session.set",
        commandId: stableCommandId(
          "session-mirror-running",
          threadId,
          activeTurnId,
          session.updatedAt,
          stableTextHash(expectedUserMessageIds.join("\0")),
          ...providerRuntimeExpectationCommandParts(expectedProviderRuntime),
          recoveredAt,
        ),
        threadId,
        expectedSession: session,
        expectedUserMessageIds,
        expectedProviderRuntime,
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

    const syncObservedDetachedCliSession = Effect.fn(
      "CodexCliSessionImporter.syncObservedDetachedCliSession",
    )(function* (
      target: CodexDiscoveryTarget,
      prepared: PreparedCodexCliThreadImport,
      state: {
        readonly status: "ready" | "running" | "interrupted" | "error";
        readonly activeTurnId: TurnId | null;
        readonly lastError: string | null;
      },
      expectedUserMessageIds?: ReadonlyArray<MessageId>,
    ) {
      const currentThread = (yield* projectionSnapshotQuery.getThreadShellsByIds([
        prepared.threadId,
      ])).get(prepared.threadId);
      if (currentThread === undefined) {
        return false;
      }
      const currentBinding = Option.getOrUndefined(yield* directory.getBinding(prepared.threadId));
      if (
        !isDetachedCodexCliObserverBinding(currentBinding) ||
        prepared.existingBinding === undefined ||
        !isSameCodexCliProviderRuntimeOwner(currentBinding, prepared.existingBinding)
      ) {
        return false;
      }
      const session = currentThread.session;
      const preparedThread = prepared.projectedThread;
      const syncAction =
        expectedUserMessageIds === undefined
          ? resolveObservedCodexCliSessionSyncAction({
              currentThread,
              preparedThread,
              observedState: state,
            })
          : codexCliSessionMatchesObservedState(session, state)
            ? "synchronized"
            : orchestrationSessionsEqual(session, preparedThread?.session ?? null)
              ? "apply"
              : "skip";
      if (syncAction === "synchronized") {
        return true;
      }
      if (syncAction === "skip") {
        return false;
      }

      const resolvedExpectedUserMessageIds =
        expectedUserMessageIds ??
        resolveCodexCliExpectedUserMessageIds(
          Option.getOrUndefined(
            yield* projectionSnapshotQuery.getThreadTranscriptById(prepared.threadId),
          ),
          [],
        );
      const observedAt = DateTime.formatIso(yield* DateTime.now);
      const expectedProviderRuntime = resolveCodexCliProviderRuntimeExpectation(
        currentBinding,
        true,
      );
      yield* orchestrationEngine.dispatch({
        type: "thread.session.set",
        commandId: stableCommandId(
          "session-cli-observed",
          prepared.threadId,
          state.status,
          state.activeTurnId ?? "no-active-turn",
          session?.updatedAt ?? String(prepared.listedThread.updatedAt),
          stableTextHash(resolvedExpectedUserMessageIds.join("\0")),
          ...providerRuntimeExpectationCommandParts(expectedProviderRuntime),
          observedAt,
        ),
        threadId: prepared.threadId,
        expectedSession: session ?? null,
        expectedUserMessageIds: resolvedExpectedUserMessageIds,
        expectedProviderRuntime,
        session: {
          threadId: prepared.threadId,
          status: state.status,
          providerName: session?.providerName ?? CODEX_DRIVER,
          providerInstanceId:
            session?.providerInstanceId ?? currentBinding.providerInstanceId ?? target.instanceId,
          runtimeMode: session?.runtimeMode ?? currentBinding.runtimeMode ?? "full-access",
          activeTurnId: state.activeTurnId,
          lastError: state.lastError,
          retrying: false,
          updatedAt: observedAt,
        },
        createdAt: observedAt,
      });
      const synchronizedThread = (yield* projectionSnapshotQuery.getThreadShellsByIds([
        prepared.threadId,
      ])).get(prepared.threadId);
      if (!codexCliSessionMatchesObservedState(synchronizedThread?.session, state)) {
        return false;
      }
      yield* Effect.logInfo("codex.cli-import.observed-session-state", {
        threadId: prepared.threadId,
        providerThreadId: prepared.listedThread.id,
        status: state.status,
        activeTurnId: state.activeTurnId,
      });
      return true;
    });

    const bindingAllowsCodexCliActivityImport = (
      prepared: PreparedCodexCliThreadImport,
      binding: ProviderRuntimeBindingWithMetadata | undefined,
    ): boolean => {
      if (prepared.existingBinding === undefined) {
        return prepared.projectedThread === undefined && binding === undefined;
      }
      if (prepared.observesDetachedCliSession) {
        return (
          binding !== undefined &&
          isDetachedCodexCliObserverBinding(binding) &&
          isSameCodexCliProviderRuntimeOwner(binding, prepared.existingBinding)
        );
      }
      return (
        binding !== undefined &&
        isCodexCliImportedBinding(prepared.existingBinding) &&
        isCodexCliImportedBinding(binding) &&
        !isLiveCodexBinding(binding) &&
        isSameCodexCliProviderRuntimeOwner(binding, prepared.existingBinding)
      );
    };

    const importCodexCliActivities = Effect.fn("CodexCliSessionImporter.importActivities")(
      function* (
        prepared: PreparedCodexCliThreadImport,
        activities: ReadonlyArray<OrchestrationThreadActivity>,
      ) {
        if (activities.length === 0) {
          return activities.length === 0;
        }
        const currentBinding = Option.getOrUndefined(
          yield* directory.getBinding(prepared.threadId),
        );
        if (!bindingAllowsCodexCliActivityImport(prepared, currentBinding)) {
          return false;
        }
        for (
          let start = 0;
          start < activities.length;
          start += CODEX_CLI_ACTIVITY_IMPORT_BATCH_SIZE
        ) {
          const batch = activities.slice(start, start + CODEX_CLI_ACTIVITY_IMPORT_BATCH_SIZE);
          const existingActivityIds = yield* projectionSnapshotQuery.getExistingThreadActivityIds({
            threadId: prepared.threadId,
            activityIds: batch.map((activity) => activity.id),
          });
          const missingActivities = batch.filter(
            (activity) => !existingActivityIds.has(activity.id),
          );
          if (missingActivities.length === 0) {
            continue;
          }
          const latestBinding = Option.getOrUndefined(
            yield* directory.getBinding(prepared.threadId),
          );
          if (!bindingAllowsCodexCliActivityImport(prepared, latestBinding)) {
            return false;
          }
          yield* orchestrationEngine.dispatch({
            type: "thread.activities.import",
            commandId: stableCommandId(
              "activities",
              prepared.threadId,
              stableTextHash(missingActivities.map((activity) => activity.id).join("\0")),
            ),
            threadId: prepared.threadId,
            activities: missingActivities,
            createdAt: missingActivities.at(-1)!.createdAt,
          });
        }
        return true;
      },
    );

    const synchronizeCodexCliActivities = Effect.fn(
      "CodexCliSessionImporter.synchronizeObservedActivities",
    )(function* (
      prepared: PreparedCodexCliThreadImport,
      rolloutPath: string | undefined,
      activities: ReadonlyArray<OrchestrationThreadActivity>,
    ) {
      const synchronized = yield* importCodexCliActivities(prepared, activities);
      if (synchronized && rolloutPath !== undefined) {
        acknowledgeCodexRolloutActivities(rolloutPath, activities);
      }
      return synchronized;
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
        projectedThreadTranscript: preparedThreadTranscript,
        staleSession,
        detachedMirrorSession,
        observesDetachedCliSession,
        refreshesDetachedCliTranscript,
        inspectDetachedCliSession,
        importIsCurrent,
        importedAtMillis,
        rolloutPath,
      } = prepared;
      const phaseTimings = {
        messageImportMs: 0,
        postImportTranscriptReadMs: 0,
        sessionSyncMs: 0,
        activitySyncMs: 0,
        metadataWriteMs: 0,
      };
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
      const listedThreadIsRecent = isRecentCodexCliActivity(listedThread.updatedAt, nowMillis);
      const observedRolloutTask =
        (!observesDetachedCliSession && !refreshesDetachedCliTranscript) ||
        rolloutPath === undefined
          ? ({
              offset: 0,
              lifecycle: {
                state: null,
                turnId: null,
              },
              changed: false,
              terminalTransitionObserved: false,
              rolloutUpdatedAtMillis: undefined,
              importIsFresh: false,
            } satisfies CodexCliRolloutTaskObservation)
          : yield* Effect.gen(function* () {
              metrics.rolloutTailReadCount += 1;
              return yield* observeCodexRolloutTaskState(
                rolloutPath,
                !inspectDetachedCliSession && !refreshesDetachedCliTranscript,
                importedAtMillis,
              );
            });
      const rolloutEvidenceAvailable =
        rolloutPath !== undefined &&
        (observedRolloutTask.rolloutUpdatedAtMillis !== undefined ||
          rolloutIsOpen ||
          observedRolloutTask.lifecycle.state !== null);
      const rolloutIsRecent =
        observedRolloutTask.rolloutUpdatedAtMillis === undefined
          ? listedThreadIsRecent
          : isRecentCodexCliRolloutActivity(observedRolloutTask.rolloutUpdatedAtMillis, nowMillis);
      const observedSessionState = resolveObservedCodexCliSessionState({
        taskState: observedRolloutTask.lifecycle.state,
        listedThreadIsActive: listedThread.status.type === "active",
        listedThreadHasSystemError: listedThread.status.type === "systemError",
        rolloutEvidenceAvailable,
        rolloutIsOpen,
        rolloutIsRecent,
      });
      const observedSessionStateWithTurn =
        observedRolloutTask.lifecycle.turnId === null
          ? observedSessionState
          : {
              ...observedSessionState,
              activeTurnId:
                observedSessionState.status === "running"
                  ? TurnId.make(observedRolloutTask.lifecycle.turnId)
                  : null,
              observedTurnId: TurnId.make(observedRolloutTask.lifecycle.turnId),
            };
      let observerSessionSynchronized = !observesDetachedCliSession;
      if (
        observesDetachedCliSession &&
        shouldSynchronizeObservedCodexCliSessionBeforeHydration(observedSessionStateWithTurn)
      ) {
        observerSessionSynchronized = yield* syncObservedDetachedCliSession(
          target,
          prepared,
          observedSessionStateWithTurn,
        );
      }
      const observerImportIsCurrent =
        observedRolloutTask.rolloutUpdatedAtMillis === undefined
          ? importIsCurrent
          : observedRolloutTask.importIsFresh;
      const observedActivityTurnId = observedRolloutTask.lifecycle.turnId ?? staleActiveTurnId;
      const observedRolloutMessages =
        rolloutPath === undefined ||
        (!observesDetachedCliSession && !refreshesDetachedCliTranscript) ||
        (!observedRolloutTask.changed && observerImportIsCurrent)
          ? undefined
          : yield* Effect.gen(function* () {
              metrics.rolloutTailReadCount += 1;
              return yield* readCodexRolloutMessages(
                rolloutPath,
                listedThread.id,
                listedThread.createdAt,
                observedActivityTurnId,
              );
            });
      const observedRolloutTranscriptSynchronized =
        observedRolloutMessages !== undefined &&
        observedRolloutMessages.complete &&
        (!observedRolloutMessages.changed && observerImportIsCurrent
          ? true
          : preparedThreadTranscript !== undefined &&
            hasSynchronizedCodexCliTranscript(
              preparedThreadTranscript,
              reconcileCodexCliImportedMessages(
                observedRolloutMessages.messages,
                preparedThreadTranscript,
              ),
            ));
      const observerNeedsHydration = shouldHydrateObservedCodexCliTranscript({
        observesDetachedCliSession,
        importIsCurrent: observerImportIsCurrent,
        rolloutTranscriptInspected: observedRolloutMessages !== undefined,
        rolloutTranscriptChanged: observedRolloutMessages?.changed ?? false,
        rolloutTranscriptComplete: observedRolloutMessages?.complete ?? false,
        rolloutTranscriptSynchronized: observedRolloutTranscriptSynchronized,
        terminalTransitionObserved: observedRolloutTask.terminalTransitionObserved,
      });
      if (
        shouldSkipUnchangedDetachedCodexCliObserver({
          observesDetachedCliSession,
          observerNeedsHydration,
          rolloutChanged: observedRolloutTask.changed,
          inspectDetachedCliSession,
        })
      ) {
        return "skipped" satisfies CodexCliThreadImportResult;
      }
      if (
        observesDetachedCliSession &&
        observedSessionState.status === "running" &&
        !observerNeedsHydration
      ) {
        if (!observerSessionSynchronized) {
          observerSessionSynchronized = yield* syncObservedDetachedCliSession(
            target,
            prepared,
            observedSessionStateWithTurn,
          );
        }
        if (observerSessionSynchronized) {
          yield* synchronizeCodexCliActivities(
            prepared,
            rolloutPath,
            observedRolloutMessages?.activities ?? [],
          );
        }
        return observerSessionSynchronized
          ? ("recovering-live" satisfies CodexCliThreadImportResult)
          : ("skipped" satisfies CodexCliThreadImportResult);
      }
      if (observesDetachedCliSession && !observerNeedsHydration) {
        if (!observerSessionSynchronized) {
          observerSessionSynchronized = yield* syncObservedDetachedCliSession(
            target,
            prepared,
            observedSessionStateWithTurn,
          );
        }
        if (observerSessionSynchronized) {
          yield* synchronizeCodexCliActivities(
            prepared,
            rolloutPath,
            observedRolloutMessages?.activities ?? [],
          );
        }
        return observerSessionSynchronized
          ? ("imported" satisfies CodexCliThreadImportResult)
          : ("skipped" satisfies CodexCliThreadImportResult);
      }
      const useRolloutTranscriptWithoutThreadRead =
        observedRolloutMessages !== undefined &&
        shouldUseCodexRolloutTranscriptWithoutThreadRead({
          observesDetachedCliSession,
          observedSessionStatus: observedSessionState.status,
          rolloutTranscriptComplete: observedRolloutMessages.complete,
          terminalTransitionObserved: observedRolloutTask.terminalTransitionObserved,
        });
      const projectedThreadTranscriptBeforeImport =
        preparedThreadTranscript ??
        (projectedThread === undefined
          ? undefined
          : Option.getOrUndefined(
              yield* projectionSnapshotQuery.getThreadTranscriptById(threadId),
            ));
      const thread = useRolloutTranscriptWithoutThreadRead
        ? listedThread
        : yield* Effect.gen(function* () {
            metrics.threadReadCount += 1;
            const response = yield* client.request("thread/read", {
              threadId: listedThread.id,
              includeTurns: true,
            });
            return response.thread;
          });
      const rolloutActivityTurnId = observedActivityTurnId ?? thread.turns.at(-1)?.id ?? null;
      const appServerTranscriptComplete =
        !useRolloutTranscriptWithoutThreadRead && isCodexCliThreadTranscriptComplete(thread);
      const readRolloutActivities = shouldReadCodexRolloutActivities({
        rolloutPathAvailable: rolloutPath !== undefined,
        activityTurnId: rolloutActivityTurnId,
        listedThreadIsRecent,
        importIsCurrent,
        binding: existingBinding,
        observesDetachedCliSession,
        refreshesDetachedCliTranscript,
      });
      const rolloutMessages =
        observedRolloutMessages ??
        (appServerTranscriptComplete || rolloutPath === undefined
          ? undefined
          : yield* Effect.gen(function* () {
              metrics.rolloutTailReadCount += 1;
              return yield* readCodexRolloutMessages(
                rolloutPath,
                thread.id,
                thread.createdAt,
                rolloutActivityTurnId,
              );
            }));
      const rolloutActivities =
        observedRolloutMessages?.activities ??
        rolloutMessages?.activities ??
        (readRolloutActivities && rolloutPath !== undefined && rolloutActivityTurnId !== null
          ? yield* Effect.gen(function* () {
              metrics.rolloutTailReadCount += 1;
              return yield* readCodexRolloutActivityTail(
                fileSystem,
                rolloutPath,
                thread.id,
                thread.createdAt,
                rolloutActivityTurnId,
              );
            })
          : []);
      const transcript = useRolloutTranscriptWithoutThreadRead
        ? {
            messages: observedRolloutMessages.messages,
            complete: observedRolloutMessages.complete,
          }
        : resolveCodexCliTranscriptMessages({
            thread,
            rollout: rolloutMessages,
          });
      if (!appServerTranscriptComplete && rolloutMessages !== undefined) {
        yield* Effect.logInfo("codex.cli-import.rollout-fallback-used", {
          instanceId: target.instanceId,
          providerThreadId: listedThread.id,
          rolloutPath,
          messageCount: rolloutMessages.messages.length,
          complete: rolloutMessages.complete,
        });
      }
      const recoveredMessages = transcript.messages;
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
      const projectedThreadTranscript = projectedThreadTranscriptBeforeImport;
      const messages = reconcileCodexCliImportedMessages(rawMessages, projectedThreadTranscript);
      const expectedUserMessageIds = resolveCodexCliExpectedUserMessageIds(
        projectedThreadTranscript,
        messages,
      );
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

      const messageImportExpectedProviderRuntime = resolveCodexCliProviderRuntimeExpectation(
        existingBinding,
        observesDetachedCliSession,
      );
      if (
        shouldImportCodexCliMessages({
          importIsCurrent,
          hasStaleSession: staleSession !== undefined,
          observesDetachedCliSession,
          observerNeedsHydration,
          refreshesDetachedCliTranscript,
        })
      ) {
        const messageImportStartedAt = yield* Clock.currentTimeMillis;
        const unsynchronizedMessages = selectUnsynchronizedCodexCliMessages(
          projectedThreadTranscript,
          messages,
        );
        for (
          let start = 0;
          start < unsynchronizedMessages.length;
          start += CODEX_CLI_MESSAGE_IMPORT_BATCH_SIZE
        ) {
          const batch = unsynchronizedMessages.slice(
            start,
            start + CODEX_CLI_MESSAGE_IMPORT_BATCH_SIZE,
          );
          yield* orchestrationEngine.dispatch(
            codexCliMessagesImportCommand({
              threadId,
              messages: batch,
              expectedProviderRuntime: messageImportExpectedProviderRuntime,
            }),
          );
        }
        phaseTimings.messageImportMs = Math.max(
          0,
          (yield* Clock.currentTimeMillis) - messageImportStartedAt,
        );
      }
      const postImportTranscriptReadStartedAt = yield* Clock.currentTimeMillis;
      const projectedThreadTranscriptAfterImport = Option.getOrUndefined(
        yield* projectionSnapshotQuery.getThreadTranscriptById(threadId),
      );
      phaseTimings.postImportTranscriptReadMs = Math.max(
        0,
        (yield* Clock.currentTimeMillis) - postImportTranscriptReadStartedAt,
      );
      const transcriptSynchronized = hasSynchronizedCodexCliTranscript(
        projectedThreadTranscriptAfterImport,
        messages,
      );
      const transcriptHydrationSucceeded = transcript.complete && transcriptSynchronized;

      const sessionSyncStartedAt = yield* Clock.currentTimeMillis;
      if (observesDetachedCliSession && transcriptHydrationSucceeded) {
        const latestTurn = thread.turns.at(-1);
        if (observedRolloutTask.lifecycle.state !== null) {
          observerSessionSynchronized = yield* syncObservedDetachedCliSession(
            target,
            prepared,
            observedSessionStateWithTurn,
            expectedUserMessageIds,
          );
        } else if (latestTurn?.status === "interrupted") {
          observerSessionSynchronized = yield* syncObservedDetachedCliSession(
            target,
            prepared,
            {
              status: "interrupted",
              activeTurnId: null,
              lastError: "The Codex turn was interrupted before it produced a final response.",
            },
            expectedUserMessageIds,
          );
        } else if (thread.status.type === "systemError" || latestTurn?.status === "failed") {
          observerSessionSynchronized = yield* syncObservedDetachedCliSession(
            target,
            prepared,
            {
              status: "error",
              activeTurnId: null,
              lastError:
                latestTurn?.status === "failed"
                  ? (latestTurn.error?.message ?? "The Codex turn failed.")
                  : "Codex reported a system error for this thread.",
            },
            expectedUserMessageIds,
          );
        } else {
          observerSessionSynchronized = yield* syncObservedDetachedCliSession(
            target,
            prepared,
            observedSessionStateWithTurn,
            expectedUserMessageIds,
          );
        }
      }
      phaseTimings.sessionSyncMs = Math.max(
        0,
        (yield* Clock.currentTimeMillis) - sessionSyncStartedAt,
      );

      let recoveringLive = false;
      if (staleSession !== undefined && transcriptHydrationSucceeded) {
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
          const settledAt = yield* settleStaleSession(
            threadId,
            staleSession,
            expectedUserMessageIds,
            existingBinding,
            resolution,
            rolloutTerminalEvidence.state,
          );
          if (settledAt !== undefined) {
            yield* recoverCodexCliCheckpoint(
              threadId,
              staleActiveTurnId,
              resolution.status,
              messages,
              settledAt,
            );
          }
        }
      } else if (detachedMirrorSession !== undefined && transcriptHydrationSucceeded) {
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
              expectedUserMessageIds,
              existingBinding,
              TurnId.make(upstreamTurn.id),
            );
          } else if (
            detachedMirrorSession.status !== resolution.status ||
            detachedMirrorSession.lastError !== resolution.lastError
          ) {
            yield* settleStaleSession(
              threadId,
              detachedMirrorSession,
              expectedUserMessageIds,
              existingBinding,
              resolution,
              mirrorTerminalEvidence.state,
            );
          }
        }
      }

      const shouldSynchronizeActivities =
        observesDetachedCliSession ||
        (existingBinding === undefined && projectedThread === undefined) ||
        isCodexCliImportedBinding(existingBinding);
      const activitySyncStartedAt = yield* Clock.currentTimeMillis;
      const activitiesSynchronized =
        !shouldSynchronizeActivities ||
        (observerSessionSynchronized &&
          (yield* synchronizeCodexCliActivities(prepared, rolloutPath, rolloutActivities)));
      phaseTimings.activitySyncMs = Math.max(
        0,
        (yield* Clock.currentTimeMillis) - activitySyncStartedAt,
      );

      // A periodic scan must never downgrade a T3-owned session that is
      // currently starting or running. Its adapter owns the live binding and
      // will persist the latest resume cursor when the session stops.
      const importedAt =
        observedRolloutTask.rolloutUpdatedAtMillis === undefined
          ? DateTime.formatIso(yield* DateTime.now)
          : DateTime.formatIso(DateTime.makeUnsafe(observedRolloutTask.rolloutUpdatedAtMillis));
      const importMetadata = {
        importedAt,
        codexCliImportVersion: CODEX_CLI_IMPORT_VERSION,
        codexCliUpdatedAt: thread.updatedAt,
        ...(refreshesDetachedCliTranscript ? { codexCliTranscriptRefreshRequired: false } : {}),
      };
      const metadataWriteStartedAt = yield* Clock.currentTimeMillis;
      const importMetadataPersisted =
        shouldPersistCodexCliImportMetadata({
          observesDetachedCliSession,
          observerSessionSynchronized,
          transcriptHydrationComplete: transcript.complete,
          transcriptSynchronized,
        }) &&
        activitiesSynchronized &&
        (existingBinding !== undefined
          ? yield* directory.mergeRuntimePayloadIfCurrent(threadId, existingBinding, importMetadata)
          : yield* directory.insertIfAbsent({
              threadId,
              provider: CODEX_DRIVER,
              providerInstanceId: target.instanceId,
              status: "stopped",
              runtimeMode: "full-access",
              resumeCursor: { threadId: thread.id },
              runtimePayload: {
                cwd,
                modelSelection,
                importedFrom: "codex-cli",
                ...importMetadata,
              },
            }));
      phaseTimings.metadataWriteMs = Math.max(
        0,
        (yield* Clock.currentTimeMillis) - metadataWriteStartedAt,
      );
      if (importMetadataPersisted) {
        observedImportedUpdatedAt.set(threadId, thread.updatedAt);
      } else {
        yield* Effect.logInfo("codex.cli-import.metadata-write-skipped", {
          threadId,
          providerThreadId: thread.id,
        });
      }
      yield* Effect.logDebug("codex.cli-import.thread-phase-timings", {
        threadId,
        providerThreadId: thread.id,
        importedMessageCount: messages.length,
        ...phaseTimings,
      });
      return recoveringLive ||
        (observesDetachedCliSession &&
          observerSessionSynchronized &&
          observedSessionState.status === "running")
        ? "recovering-live"
        : "imported";
    });

    const scanTarget = Effect.fn("CodexCliSessionImporter.scanTarget")(function* (
      target: CodexDiscoveryTarget,
      retainedRolloutPaths: Set<string>,
      retainedThreadIds: Set<ThreadId>,
      retainedImportFailureKeys: Set<string>,
    ) {
      const scanStartedAt = yield* Clock.currentTimeMillis;
      const result = yield* withCodexClient(clientPool, target, (client, appServerMetrics) =>
        Effect.gen(function* () {
          const metrics: CodexCliImportScanMetrics = {
            threadReadCount: 0,
            rolloutTailReadCount: 0,
            skippedCurrentCount: 0,
            failureBackoffCount: 0,
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
              retainedThreadIds.add(candidate.threadId);
              retainedImportFailureKeys.add(`${target.instanceId}\0${candidate.listedThread.id}`);
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
              if (
                (prepared.observesDetachedCliSession || prepared.refreshesDetachedCliTranscript) &&
                prepared.rolloutPath !== undefined
              ) {
                retainedRolloutPaths.add(prepared.rolloutPath);
              }
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
                observesDetachedCliSession: prepared.inspectDetachedCliSession,
                refreshesDetachedCliTranscript: prepared.refreshesDetachedCliTranscript,
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
            const importFailureKey = `${target.instanceId}\0${prepared.listedThread.id}`;
            if (
              shouldBackoffCodexCliImportFailure({
                failure: importFailureBackoffs.get(importFailureKey),
                providerUpdatedAt: prepared.listedThread.updatedAt,
                nowMillis,
              })
            ) {
              metrics.failureBackoffCount += 1;
              continue;
            }
            const importResult = yield* importThread(
              target,
              client,
              prepared,
              openRolloutPaths,
              nowMillis,
              metrics,
            ).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  importFailureBackoffs.delete(importFailureKey);
                }),
              ),
              Effect.catchCause((cause) => {
                const restartableError = findRestartableCodexAppServerError(cause);
                if (restartableError !== undefined) {
                  return Effect.fail(restartableError);
                }
                return Effect.gen(function* () {
                  const failure = advanceCodexCliImportFailureBackoff({
                    previous: importFailureBackoffs.get(importFailureKey),
                    providerUpdatedAt: prepared.listedThread.updatedAt,
                    failedAtMillis: nowMillis,
                  });
                  importFailureBackoffs.set(importFailureKey, failure);
                  yield* Effect.logWarning("codex.cli-import.thread-failed", {
                    instanceId: target.instanceId,
                    codexHome: target.homeLayout.sharedHomePath,
                    providerThreadId: prepared.listedThread.id,
                    retryAfterMillis: failure.retryAfterMillis,
                    cause,
                  });
                  return "skipped" satisfies CodexCliThreadImportResult;
                });
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
      const retainedRolloutPaths = new Set<string>();
      const retainedThreadIds = new Set<ThreadId>();
      const retainedImportFailureKeys = new Set<string>();
      let recoveringLiveCount = 0;
      for (const target of targets) {
        const result = yield* scanTarget(
          target,
          retainedRolloutPaths,
          retainedThreadIds,
          retainedImportFailureKeys,
        ).pipe(
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
                failureBackoffCount: 0,
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
      pruneCodexCliImportCache(rolloutTaskCursors, retainedRolloutPaths);
      pruneCodexCliImportCache(rolloutMessageCursors, retainedRolloutPaths);
      pruneCodexCliImportCache(observedImportedUpdatedAt, retainedThreadIds);
      pruneCodexCliImportCache(importFailureBackoffs, retainedImportFailureKeys);
      return recoveringLiveCount > 0;
    });

    const start: CodexCliSessionImporterShape["start"] = () =>
      Effect.gen(function* () {
        yield* forkParked(
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
