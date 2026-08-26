import {
  CommandId,
  DEFAULT_MODEL,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  ModelSelection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeTaskId,
  ThreadId,
  TurnId,
  type OrchestrationThreadActivity,
  type OrchestrationSession,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { forkParked } from "../../serverActivation.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { deriveProviderInstanceConfigMap } from "../Layers/ProviderInstanceRegistryHydration.ts";
import { listActivePiSessionFiles } from "../piSessionFiles.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import {
  PiCompatibleSessionImporter,
  type PiCompatibleSessionImporterShape,
} from "../Services/PiCompatibleSessionImporter.ts";

// Discover historical sessions on the slower sweep; between those walks,
// re-read only transcripts owned by live CLI processes.
const FULL_SCAN_INTERVAL_MS = 60_000;
const ACTIVE_SCAN_INTERVAL_MS = 5_000;
const OMP_DRIVER = ProviderDriverKind.make("omp");
const PI_DRIVER = ProviderDriverKind.make("piAgent");
type UnknownRecord = Record<string, unknown>;
type PiCompatibleToolCall = {
  readonly name: string;
  readonly title: string;
  readonly summary: string;
  readonly itemType:
    | "command_execution"
    | "file_change"
    | "mcp_tool_call"
    | "dynamic_tool_call"
    | "collab_agent_tool_call"
    | "web_search";
  readonly input: UnknownRecord;
  readonly turnId: TurnId | null;
};
const decodeUnknownJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeUnknownJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const ImportedRuntimePayload = Schema.Struct({
  importedFrom: Schema.String,
  importedTitle: Schema.optionalKey(Schema.String),
  hiddenFromSidebar: Schema.optionalKey(Schema.Boolean),
  importedAsSubagent: Schema.optionalKey(Schema.Boolean),
});
const decodeImportedRuntimePayload = Schema.decodeUnknownOption(ImportedRuntimePayload);

export interface PiCompatibleImportedMessage {
  readonly messageId: MessageId;
  readonly role: "user" | "assistant";
  readonly text: string;
  readonly turnId: TurnId;
  readonly createdAt: string;
}

type PiCompatibleImportedActivity = OrchestrationThreadActivity;

export interface PiCompatibleSession {
  readonly id: string;
  readonly cwd: string;
  readonly title: string | undefined;
  readonly parentSession: string | undefined;
  readonly createdAt: string;
  readonly messages: ReadonlyArray<PiCompatibleImportedMessage>;
  readonly activities: ReadonlyArray<PiCompatibleImportedActivity>;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function stableTextHash(value: string): string {
  let hash = 0x811c9dc5;
  for (const char of value) {
    hash ^= char.codePointAt(0)!;
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(36);
}

function stableProjectId(cwd: string): ProjectId {
  return ProjectId.make(`pi-compatible-${stableTextHash(cwd)}`);
}

function stableThreadId(driver: ProviderDriverKind, sessionId: string): ThreadId {
  return ThreadId.make(`${driver}-${stableTextHash(sessionId)}`);
}

function stableCommandId(...parts: ReadonlyArray<string>): CommandId {
  return CommandId.make(["pi-compatible-import", ...parts].join(":"));
}

function isoTimestamp(value: unknown, fallback: string): string {
  if (typeof value !== "string") return fallback;
  try {
    return DateTime.formatIso(DateTime.makeUnsafe(value));
  } catch {
    return fallback;
  }
}

function textFromContent(content: unknown): string | undefined {
  if (typeof content === "string") return content.trim() || undefined;
  if (!Array.isArray(content)) return undefined;
  const text = content
    .flatMap((part) => {
      if (typeof part !== "object" || part === null || Array.isArray(part)) return [];
      const entry = part as UnknownRecord;
      if (entry.type !== "text" && entry.type !== "input_text") return [];
      const value = stringValue(entry.text);
      return value === undefined ? [] : [value];
    })
    .join("\n")
    .trim();
  return text || undefined;
}

const conciseLine = (text: string, maxLength: number): string | undefined => {
  const firstLine = text
    .split(/\r?\n/u)
    .map((line) => line.replace(/\s+/gu, " ").trim())
    .find(Boolean);
  if (firstLine === undefined) return undefined;
  if (firstLine.length <= maxLength) return firstLine;
  return `${firstLine.slice(0, maxLength - 3).trimEnd()}...`;
};

const ACTIVITY_DETAIL_MAX_LENGTH = 2_000;
const TOOL_ARGUMENT_MAX_LENGTH = 1_000;
const TOOL_ARGUMENT_KEYS = new Set([
  "args",
  "command",
  "cwd",
  "file",
  "i",
  "line",
  "name",
  "new_name",
  "op",
  "path",
  "paths",
  "pattern",
  "query",
  "symbol",
  "task",
  "to",
  "url",
]);

function boundedText(value: string, maxLength: number): string {
  if (value.length <= maxLength) return value;
  return `${value.slice(0, maxLength - 21).trimEnd()}\n… output truncated`;
}

function compactToolArguments(value: unknown): UnknownRecord {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return {};
  const compact: UnknownRecord = {};
  for (const [key, entry] of Object.entries(value as UnknownRecord)) {
    if (!TOOL_ARGUMENT_KEYS.has(key)) continue;
    if (typeof entry === "string") compact[key] = boundedText(entry, TOOL_ARGUMENT_MAX_LENGTH);
    else if (typeof entry === "number" || typeof entry === "boolean" || entry === null)
      compact[key] = entry;
    else if (Array.isArray(entry))
      compact[key] = entry
        .slice(0, 20)
        .map((item) =>
          typeof item === "string" ? boundedText(item, TOOL_ARGUMENT_MAX_LENGTH) : item,
        );
  }
  return compact;
}

function pathsFromEditPatch(argumentsValue: unknown): ReadonlyArray<{ readonly path: string }> {
  if (typeof argumentsValue !== "object" || argumentsValue === null) return [];
  const input = (argumentsValue as UnknownRecord).input;
  if (typeof input !== "string") return [];
  const paths = new Set<string>();
  for (const match of input.matchAll(/^\[([^#\]\r\n]+)#[0-9A-F]{4}\]$/gmu)) {
    const path = match[1]?.trim();
    if (path) paths.add(path);
    if (paths.size >= 12) break;
  }
  return [...paths].map((path) => ({ path }));
}

function toolTitle(name: string): string {
  const known: Readonly<Record<string, string>> = {
    bash: "Terminal",
    edit: "Edit",
    glob: "Find files",
    grep: "Search",
    read: "Read",
    task: "Agent",
    todo: "Plan",
    web_search: "Web search",
    write: "Write",
  };
  const normalized = name.toLowerCase();
  const exact = known[normalized];
  if (exact !== undefined) return exact;
  const words = name
    .replace(/([a-z0-9])([A-Z])/gu, "$1 $2")
    .replace(/[_-]+/gu, " ")
    .trim();
  return words.length === 0 ? "Tool" : `${words[0]!.toUpperCase()}${words.slice(1)}`;
}

function toolItemType(name: string): PiCompatibleToolCall["itemType"] {
  const normalized = name.toLowerCase();
  if (normalized === "bash") return "command_execution";
  if (normalized === "edit" || normalized === "write" || normalized === "ast_edit")
    return "file_change";
  if (normalized === "web_search") return "web_search";
  if (normalized === "task" || normalized === "agent") return "collab_agent_tool_call";
  if (normalized.includes("browser")) return "mcp_tool_call";
  return "dynamic_tool_call";
}

function toolRequestKind(
  itemType: PiCompatibleToolCall["itemType"],
  name: string,
): "command" | "file-read" | "file-change" | undefined {
  if (itemType === "command_execution") return "command";
  if (itemType === "file_change") return "file-change";
  return name === "read" || name === "grep" || name === "glob" ? "file-read" : undefined;
}

const IMPORTED_TITLE_MAX_LENGTH = 120;

function titleFromFirstUserMessage(
  messages: ReadonlyArray<PiCompatibleImportedMessage>,
): string | undefined {
  const text = messages.find((message) => message.role === "user")?.text;
  return text === undefined ? undefined : conciseLine(text, IMPORTED_TITLE_MAX_LENGTH);
}

/** Parses Pi v3 and OMP v3 JSONL transcripts. Unknown record types are ignored. */
export function parsePiCompatibleSession(
  contents: string,
  driver: ProviderDriverKind,
  sourcePath: string,
): PiCompatibleSession | undefined {
  const fallbackTime = "1970-01-01T00:00:00.000Z";
  let session: UnknownRecord | undefined;
  let sessionTitle: string | undefined;
  let updatedAt = fallbackTime;
  let activeTurnId: TurnId | null = null;
  const messages: PiCompatibleImportedMessage[] = [];
  const activities: PiCompatibleImportedActivity[] = [];
  const toolCalls = new Map<string, PiCompatibleToolCall>();
  let anonymousRecordIndex = 0;
  let turnActivityStartIndex = 0;
  let lastVisibleAssistantTurnId: TurnId | null = null;
  const finalizeActivityTurn = () => {
    if (lastVisibleAssistantTurnId === null) return;
    for (let index = turnActivityStartIndex; index < activities.length; index += 1) {
      const activity = activities[index];
      if (activity !== undefined) {
        activities[index] = { ...activity, turnId: lastVisibleAssistantTurnId };
      }
    }
  };

  for (const line of contents.split(/\r?\n/u)) {
    if (line.trim().length === 0) continue;
    let record: UnknownRecord;
    try {
      record = decodeUnknownJson(line) as UnknownRecord;
    } catch {
      continue;
    }
    const timestamp = isoTimestamp(record.timestamp ?? record.updatedAt, fallbackTime);
    if (timestamp > updatedAt) updatedAt = timestamp;
    if (record.type === "session") session = record;
    if (record.type === "title" && sessionTitle === undefined)
      sessionTitle = stringValue(record.title);
    if (
      record.type !== "message" ||
      typeof record.message !== "object" ||
      record.message === null ||
      Array.isArray(record.message)
    )
      continue;

    const message = record.message as UnknownRecord;
    const recordId = stringValue(record.id) ?? `${anonymousRecordIndex++}`;
    const sessionId = stringValue(session?.id) ?? sourcePath;
    const sessionHash = stableTextHash(sessionId);
    if (message.role === "user" || message.role === "assistant") {
      const turnId = TurnId.make(`${driver}:${sessionHash}:${recordId}`);
      if (message.role === "user") {
        finalizeActivityTurn();
        activeTurnId = turnId;
        turnActivityStartIndex = activities.length;
        lastVisibleAssistantTurnId = null;
      }
      const text = textFromContent(message.content);
      if (text !== undefined)
        messages.push({
          messageId: MessageId.make(`${driver}:${sessionHash}:${recordId}`),
          role: message.role,
          text,
          turnId,
          createdAt: timestamp,
        });
      if (message.role === "assistant" && text !== undefined) {
        lastVisibleAssistantTurnId = turnId;
      }

      if (message.role !== "assistant" || !Array.isArray(message.content)) continue;
      for (const [partIndex, value] of message.content.entries()) {
        if (typeof value !== "object" || value === null || Array.isArray(value)) continue;
        const part = value as UnknownRecord;
        if (part.type === "thinking" || part.type === "reasoning") {
          const reasoning = stringValue(part.thinking) ?? stringValue(part.text);
          if (reasoning === undefined) continue;
          activities.push({
            id: EventId.make(
              `${driver}:${sessionHash}:reasoning:${stableTextHash(`${recordId}:${partIndex}`)}`,
            ),
            tone: "info",
            kind: "task.progress",
            summary: "Reasoning",
            payload: {
              summary: conciseLine(reasoning, 180) ?? "Reasoning",
              detail: boundedText(reasoning, ACTIVITY_DETAIL_MAX_LENGTH),
            },
            turnId: activeTurnId,
            createdAt: timestamp,
          });
          continue;
        }
        if (part.type !== "toolCall" && part.type !== "tool_call") continue;
        const name = stringValue(part.name) ?? "tool";
        const callId =
          stringValue(part.id) ??
          `${recordId}:${partIndex}:${stableTextHash(encodeUnknownJson(part))}`;
        const input = compactToolArguments(part.arguments);
        const patchFiles = pathsFromEditPatch(part.arguments);
        if (patchFiles.length > 0) input.files = patchFiles;
        const itemType = toolItemType(name);
        const title = toolTitle(name);
        const summary = stringValue(part.intent) ?? stringValue(input.i) ?? `${title} tool call`;
        const toolCall = {
          name,
          title,
          summary,
          itemType,
          input,
          turnId: activeTurnId,
        } satisfies PiCompatibleToolCall;
        toolCalls.set(callId, toolCall);
        const requestKind = toolRequestKind(itemType, name);
        activities.push({
          id: EventId.make(`${driver}:${sessionHash}:tool:${callId}:updated`),
          tone: "tool",
          kind: "tool.updated",
          summary,
          payload: {
            itemType,
            status: "inProgress",
            title,
            ...(requestKind === undefined ? {} : { requestKind }),
            data: {
              toolCallId: callId,
              kind: itemType === "command_execution" ? "execute" : name,
              item: {
                name,
                input,
                ...(typeof input.command === "string" ? { command: input.command } : {}),
              },
            },
          },
          turnId: activeTurnId,
          createdAt: timestamp,
        });
      }
      continue;
    }

    if (message.role !== "toolResult") continue;
    const callId = stringValue(message.toolCallId);
    if (callId === undefined) continue;
    const call = toolCalls.get(callId);
    const name = call?.name ?? stringValue(message.toolName) ?? "tool";
    const title = call?.title ?? toolTitle(name);
    const itemType = call?.itemType ?? toolItemType(name);
    const input = call?.input ?? {};
    const output = textFromContent(message.content);
    const detail =
      output === undefined ? undefined : boundedText(output, ACTIVITY_DETAIL_MAX_LENGTH);
    const resultDetails =
      typeof message.details === "object" &&
      message.details !== null &&
      !Array.isArray(message.details)
        ? (message.details as UnknownRecord)
        : {};
    const requestKind = toolRequestKind(itemType, name);
    activities.push({
      id: EventId.make(`${driver}:${sessionHash}:tool:${callId}:completed`),
      tone: message.isError === true ? "error" : "tool",
      kind: "tool.completed",
      summary: call?.summary ?? `${title} tool call`,
      payload: {
        itemType,
        status: message.isError === true ? "failed" : "completed",
        title,
        ...(requestKind === undefined ? {} : { requestKind }),
        ...(detail === undefined ? {} : { detail }),
        data: {
          toolCallId: callId,
          kind: itemType === "command_execution" ? "execute" : name,
          item: {
            name,
            input,
            ...(typeof input.command === "string" ? { command: input.command } : {}),
          },
          ...(detail === undefined
            ? {}
            : {
                rawOutput: {
                  content: detail,
                  ...(typeof resultDetails.exitCode === "number"
                    ? { exitCode: resultDetails.exitCode }
                    : {}),
                },
              }),
        },
      },
      turnId: call?.turnId ?? activeTurnId,
      createdAt: timestamp,
    });
    toolCalls.delete(callId);
  }
  finalizeActivityTurn();

  const id = stringValue(session?.id);
  const cwd = stringValue(session?.cwd);
  if (id === undefined || cwd === undefined) return undefined;
  return {
    id,
    cwd,
    parentSession: stringValue(session?.parentSession),
    title: sessionTitle ?? stringValue(session?.title) ?? titleFromFirstUserMessage(messages),
    createdAt: isoTimestamp(session?.timestamp, updatedAt),
    messages,
    activities,
  };
}

export function piCompatibleSubagentActivities(
  session: PiCompatibleSession,
): ReadonlyArray<OrchestrationThreadActivity> {
  const title = session.title ?? "Subagent";
  const taskId = RuntimeTaskId.make(`omp:${stableTextHash(session.id)}`);
  const latestMessage = session.messages.at(-1);
  const completedAt = latestMessage?.createdAt ?? session.createdAt;
  const resultSummary =
    latestMessage?.role === "assistant" ? conciseLine(latestMessage.text, 180) : undefined;
  const linkage = {
    taskId,
    taskType: "subagent",
    agentKind: "agent",
    title,
    role: "subagent",
    timelineBypass: true,
  } as const;
  return [
    {
      id: EventId.make(`omp-subagent-started:${stableTextHash(session.id)}`),
      tone: "info",
      kind: "task.started",
      summary: `${title} started`,
      payload: { ...linkage, description: title },
      turnId: null,
      createdAt: session.createdAt,
    },
    {
      id: EventId.make(`omp-subagent-completed:${stableTextHash(session.id)}`),
      tone: latestMessage?.role === "assistant" ? "info" : "error",
      kind: "task.completed",
      summary: `${title} ${latestMessage?.role === "assistant" ? "completed" : "stopped"}`,
      payload: {
        ...linkage,
        status: latestMessage?.role === "assistant" ? "completed" : "stopped",
        ...(resultSummary === undefined ? {} : { summary: resultSummary }),
      },
      turnId: null,
      createdAt: completedAt,
    },
  ];
}

export function isPiCompatibleSessionManagedByT3(
  session: OrchestrationSession | null | undefined,
): boolean {
  return session?.nativeSession?.ownership === "t3";
}

export function resolvePiCompatibleObservedSession(input: {
  readonly currentSession: OrchestrationSession | null | undefined;
  readonly threadId: ThreadId;
  readonly imported: PiCompatibleSession;
  readonly sourcePath: string;
  readonly instanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  readonly isOpen: boolean;
  readonly binaryPath: string;
  readonly sessionDir: string | undefined;
  readonly observedAt: string;
}): OrchestrationSession | undefined {
  const currentNativeSession = input.currentSession?.nativeSession;
  if (
    isPiCompatibleSessionManagedByT3(input.currentSession) ||
    (currentNativeSession?.ownership === "released" && !input.isOpen)
  )
    return undefined;

  const latestMessage = input.imported.messages.at(-1);
  const hasActiveTurn = input.isOpen && latestMessage?.role === "user";
  const desiredStatus = hasActiveTurn ? "running" : input.isOpen ? "ready" : "stopped";
  if (
    input.currentSession?.status === desiredStatus &&
    input.currentSession.activeTurnId === (hasActiveTurn ? latestMessage.turnId : null) &&
    currentNativeSession?.ownership === "external" &&
    currentNativeSession.id === input.imported.id &&
    currentNativeSession.path === input.sourcePath
  )
    return undefined;

  return {
    threadId: input.threadId,
    status: desiredStatus,
    providerName: input.driver,
    providerInstanceId: input.instanceId,
    runtimeMode: "full-access",
    activeTurnId: hasActiveTurn ? latestMessage.turnId : null,
    lastError: null,
    nativeSession: {
      id: input.imported.id,
      path: input.sourcePath,
      ownership: "external",
      supportsConcurrentAttach: false,
      cli: {
        command: input.binaryPath,
        args: [
          ...(input.driver === OMP_DRIVER ? ["--resume"] : ["--session"]),
          input.sourcePath,
          ...(input.sessionDir === undefined ? [] : ["--session-dir", input.sessionDir]),
        ],
        cwd: input.imported.cwd,
      },
    },
    updatedAt: input.observedAt,
  };
}

export function piCompatibleTurnReconcileCommand(
  threadId: ThreadId,
  session: PiCompatibleSession,
  isOpen = false,
) {
  const latestMessage = session.messages.at(-1);
  if (latestMessage === undefined || (isOpen && latestMessage.role === "user")) return undefined;
  const state: "completed" | "interrupted" =
    latestMessage.role === "assistant" ? "completed" : "interrupted";
  return {
    type: "thread.turn.reconcile" as const,
    commandId: stableCommandId(
      "turn",
      threadId,
      latestMessage.turnId,
      state,
      latestMessage.createdAt,
    ),
    threadId,
    turnId: latestMessage.turnId,
    state,
    completedAt: latestMessage.createdAt,
    createdAt: latestMessage.createdAt,
  };
}

function sessionRoot(configured: string, defaultRoot: string): string {
  const root = configured.trim() || defaultRoot;
  const home = process.env.HOME ?? "";
  return root === "~" ? home : root.startsWith("~/") ? `${home}/${root.slice(2)}` : root;
}

const listSessionFiles = Effect.fn("PiCompatibleSessionImporter.listSessionFiles")(function* (
  fileSystem: FileSystem.FileSystem,
  path: Path.Path,
  root: string,
): Effect.fn.Return<ReadonlyArray<string>> {
  const files: string[] = [];
  const visit = Effect.fn("PiCompatibleSessionImporter.visit")(function* (
    directory: string,
  ): Effect.fn.Return<void> {
    const entries = yield* fileSystem.readDirectory(directory).pipe(Effect.orElseSucceed(() => []));
    for (const entry of entries) {
      const candidate = path.join(directory, entry);
      const stat = yield* fileSystem.stat(candidate).pipe(Effect.option);
      if (Option.isNone(stat)) continue;
      if (stat.value.type === "Directory") yield* visit(candidate);
      else if (stat.value.type === "File" && entry.endsWith(".jsonl")) files.push(candidate);
    }
  });
  yield* visit(root);
  return files.toSorted();
});

function isWithinSessionRoot(path: Path.Path, root: string, candidate: string): boolean {
  const relative = path.relative(path.resolve(root), path.resolve(candidate));
  return (
    relative === "" ||
    (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative))
  );
}

type PiCompatibleSessionFile = {
  readonly sourcePath: string;
  readonly session: PiCompatibleSession;
};

type PiCompatibleSubagentSessionFile = PiCompatibleSessionFile & {
  readonly parent: PiCompatibleSession;
};

export function partitionPiCompatibleSessions(
  path: Pick<Path.Path, "dirname" | "resolve">,
  driver: ProviderDriverKind,
  parsed: ReadonlyArray<PiCompatibleSessionFile>,
): {
  readonly topLevel: ReadonlyArray<PiCompatibleSessionFile>;
  readonly subagents: ReadonlyArray<PiCompatibleSubagentSessionFile>;
} {
  const byPath = new Map(
    parsed.map(({ sourcePath, session }) => [path.resolve(sourcePath), session] as const),
  );
  const topLevel: PiCompatibleSessionFile[] = [];
  const subagents: PiCompatibleSubagentSessionFile[] = [];

  for (const item of parsed) {
    const explicitParent =
      item.session.parentSession === undefined
        ? undefined
        : path.resolve(path.dirname(item.sourcePath), item.session.parentSession);
    const inferredOmpParent =
      explicitParent === undefined && driver === OMP_DRIVER
        ? path.resolve(`${path.dirname(item.sourcePath)}.jsonl`)
        : undefined;
    const parentPath =
      explicitParent ??
      (inferredOmpParent !== undefined && byPath.has(inferredOmpParent)
        ? inferredOmpParent
        : undefined);
    if (parentPath === undefined) {
      topLevel.push(item);
      continue;
    }
    const parent = byPath.get(parentPath);
    if (parent !== undefined) subagents.push({ ...item, parent });
  }

  return { topLevel, subagents };
}

const makePiCompatibleSessionImporter = (options?: {
  readonly scanIntervalMs?: number;
  readonly activeScanIntervalMs?: number;
}) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    const snapshots = yield* ProjectionSnapshotQuery;
    const directory = yield* ProviderSessionDirectory;
    const settingsService = yield* ServerSettingsService;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const scanIntervalMs = options?.scanIntervalMs ?? FULL_SCAN_INTERVAL_MS;
    const activeScanIntervalMs = options?.activeScanIntervalMs ?? ACTIVE_SCAN_INTERVAL_MS;
    const importedActivityIdsByThread = new Map<ThreadId, Set<EventId>>();
    let previouslyActiveSessionFiles = {
      omp: new Set<string>(),
      piAgent: new Set<string>(),
    };

    const importActivities = Effect.fn("PiCompatibleSessionImporter.importActivities")(function* (
      threadId: ThreadId,
      activities: ReadonlyArray<PiCompatibleImportedActivity>,
    ) {
      if (activities.length === 0) return;
      let known = importedActivityIdsByThread.get(threadId);
      if (known === undefined) {
        known = new Set<EventId>();
        const candidateIds = activities.map((activity) => activity.id);
        for (let index = 0; index < candidateIds.length; index += 400) {
          const existing = yield* snapshots.getExistingThreadActivityIds({
            threadId,
            activityIds: candidateIds.slice(index, index + 400),
          });
          for (const id of existing) known.add(id);
        }
        importedActivityIdsByThread.set(threadId, known);
      }
      const unseen = activities.filter((activity) => !known.has(activity.id));
      if (unseen.length === 0) return;
      yield* engine.dispatch({
        type: "thread.activities.import",
        commandId: stableCommandId(
          "activities",
          threadId,
          stableTextHash(encodeUnknownJson(unseen.map((activity) => activity.id))),
        ),
        threadId,
        activities: unseen,
        createdAt: unseen.at(-1)!.createdAt,
      });
      for (const activity of unseen) known.add(activity.id);
    });

    const archiveThreadIfVisible = Effect.fn("PiCompatibleSessionImporter.archiveThreadIfVisible")(
      function* (threadId: ThreadId) {
        const thread = (yield* snapshots.getThreadShellsByIds([threadId])).get(threadId);
        if (thread === undefined || thread.archivedAt !== null) return;
        yield* engine.dispatch({
          type: "thread.archive",
          commandId: stableCommandId("archive", threadId),
          threadId,
        });
      },
    );

    const importTopLevelSession = Effect.fn("PiCompatibleSessionImporter.importTopLevelSession")(
      function* (
        sourcePath: string,
        imported: PiCompatibleSession,
        instanceId: ProviderInstanceId,
        driver: ProviderDriverKind,
        isOpen: boolean,
        binaryPath: string,
        sessionDir: string | undefined,
      ) {
        const cwd = path.resolve(imported.cwd);
        const threadId = stableThreadId(driver, imported.id);
        const modelSelection = { instanceId, model: DEFAULT_MODEL } satisfies ModelSelection;
        const existingThread = (yield* snapshots.getThreadShellsByIds([threadId])).get(threadId);
        if (isPiCompatibleSessionManagedByT3(existingThread?.session)) return;
        const project = yield* snapshots.getActiveProjectByWorkspaceRoot(cwd);
        const projectId = Option.match(project, {
          onSome: (value) => value.id,
          onNone: () => stableProjectId(cwd),
        });
        if (Option.isNone(project))
          yield* engine.dispatch({
            type: "project.create",
            commandId: stableCommandId("project", projectId),
            projectId,
            title: path.basename(cwd) || (driver === OMP_DRIVER ? "Oh My Pi" : "Pi Agent"),
            workspaceRoot: cwd,
            defaultModelSelection: modelSelection,
            createdAt: imported.createdAt,
          });
        const transcript = yield* snapshots.getThreadTranscriptById(threadId);
        if (Option.isNone(transcript))
          yield* engine.dispatch({
            type: "thread.create",
            commandId: stableCommandId("thread", threadId),
            threadId,
            projectId,
            title: imported.title ?? "Imported session",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            branch: null,
            worktreePath: null,
            createdAt: imported.createdAt,
          });
        const known = new Set(
          Option.getOrUndefined(transcript)?.messages.map((message) => message.id) ?? [],
        );
        const messages = imported.messages.filter((message) => !known.has(message.messageId));
        if (messages.length > 0)
          yield* engine.dispatch({
            type: "thread.messages.import",
            commandId: stableCommandId(
              "messages",
              threadId,
              stableTextHash(encodeUnknownJson(messages)),
            ),
            threadId,
            messages,
            createdAt: messages.at(-1)!.createdAt,
          });
        yield* importActivities(threadId, imported.activities);
        const turnCommand = piCompatibleTurnReconcileCommand(threadId, imported, isOpen);
        if (turnCommand !== undefined) yield* engine.dispatch(turnCommand);
        const binding = yield* directory.getBinding(threadId);
        if (Option.isNone(binding))
          yield* directory.insertIfAbsent({
            threadId,
            provider: driver,
            providerInstanceId: instanceId,
            status: "stopped",
            runtimeMode: "full-access",
            resumeCursor: {
              schemaVersion: 1,
              sessionId: imported.id,
              sessionFile: sourcePath,
            },
            runtimePayload: {
              cwd,
              importedFrom: driver,
              ...(imported.title === undefined ? {} : { importedTitle: imported.title }),
            },
          });
        else if (imported.title !== undefined) {
          const payload = decodeImportedRuntimePayload(binding.value.runtimePayload);
          if (Option.isSome(payload) && payload.value.importedFrom === driver) {
            const thread = (yield* snapshots.getThreadShellsByIds([threadId])).get(threadId);
            const previousImportedTitle = payload.value.importedTitle;
            if (
              thread !== undefined &&
              thread.title !== imported.title &&
              (previousImportedTitle === undefined
                ? thread.title === "Imported session"
                : thread.title === previousImportedTitle)
            )
              yield* engine.dispatch({
                type: "thread.meta.update",
                commandId: stableCommandId("title", threadId, stableTextHash(imported.title)),
                threadId,
                title: imported.title,
              });
            if (previousImportedTitle !== imported.title)
              yield* directory.mergeRuntimePayload(threadId, {
                importedTitle: imported.title,
              });
          }
        }
        const observedAt = DateTime.formatIso(yield* DateTime.now);
        const currentThread = (yield* snapshots.getThreadShellsByIds([threadId])).get(threadId);
        const observedSession = resolvePiCompatibleObservedSession({
          currentSession: currentThread?.session,
          threadId,
          imported: { ...imported, cwd },
          sourcePath,
          instanceId,
          driver,
          isOpen,
          binaryPath,
          sessionDir,
          observedAt,
        });
        if (observedSession !== undefined)
          yield* engine.dispatch({
            type: "thread.session.set",
            commandId: stableCommandId("native-session", threadId, observedAt),
            threadId,
            session: observedSession,
            createdAt: observedAt,
          });
      },
    );

    const importSubagentSession = Effect.fn("PiCompatibleSessionImporter.importSubagentSession")(
      function* (
        imported: PiCompatibleSession,
        parent: PiCompatibleSession,
        driver: ProviderDriverKind,
      ) {
        const childThreadId = stableThreadId(driver, imported.id);
        const childBinding = yield* directory.getBinding(childThreadId);
        if (Option.isSome(childBinding)) {
          const payload = decodeImportedRuntimePayload(childBinding.value.runtimePayload);
          if (
            Option.isSome(payload) &&
            payload.value.importedFrom === driver &&
            payload.value.importedAsSubagent !== true
          ) {
            yield* archiveThreadIfVisible(childThreadId);
            yield* directory.mergeRuntimePayload(childThreadId, { importedAsSubagent: true });
          }
        }
        const parentThreadId = stableThreadId(driver, parent.id);
        if (Option.isNone(yield* snapshots.getThreadTranscriptById(parentThreadId))) return;
        const activities = piCompatibleSubagentActivities(imported);
        yield* engine.dispatch({
          type: "thread.activities.import",
          commandId: stableCommandId("subagent", parentThreadId, stableTextHash(imported.id)),
          threadId: parentThreadId,
          activities,
          createdAt: activities.at(-1)!.createdAt,
        });
      },
    );

    const scan = Effect.fn("PiCompatibleSessionImporter.scan")(function* (mode: "full" | "active") {
      const activeSessionFiles = yield* listActivePiSessionFiles(fileSystem, path);
      const settings = yield* settingsService.getSettings;
      for (const [rawInstanceId, entry] of Object.entries(
        deriveProviderInstanceConfigMap(settings),
      )) {
        const driver = entry.driver;
        if ((driver !== OMP_DRIVER && driver !== PI_DRIVER) || entry.enabled === false) continue;
        const config = (entry.config ?? {}) as UnknownRecord;
        if (config.enabled === false) continue;
        const defaultRoot =
          driver === OMP_DRIVER ? "~/.omp/agent/sessions" : "~/.pi/agent/sessions";
        const root = sessionRoot(stringValue(config.sessionDir) ?? "", defaultRoot);
        const configuredSessionDir = stringValue(config.sessionDir);
        const binaryPath = stringValue(config.binaryPath) ?? (driver === OMP_DRIVER ? "omp" : "pi");
        const activeFiles =
          driver === OMP_DRIVER ? activeSessionFiles.omp : activeSessionFiles.piAgent;
        const previousActiveFiles =
          driver === OMP_DRIVER
            ? previouslyActiveSessionFiles.omp
            : previouslyActiveSessionFiles.piAgent;
        const sourcePaths =
          mode === "full"
            ? yield* listSessionFiles(fileSystem, path, root)
            : [...new Set([...activeFiles, ...previousActiveFiles])]
                .filter(
                  (sourcePath) =>
                    sourcePath.endsWith(".jsonl") && isWithinSessionRoot(path, root, sourcePath),
                )
                .toSorted();
        const parsed: Array<{ sourcePath: string; session: PiCompatibleSession }> = [];
        for (const sourcePath of sourcePaths) {
          const contents = yield* fileSystem.readFileString(sourcePath).pipe(Effect.option);
          if (Option.isNone(contents)) continue;
          const session = parsePiCompatibleSession(contents.value, driver, sourcePath);
          if (session !== undefined && session.messages.some((message) => message.role === "user"))
            parsed.push({ sourcePath, session });
        }
        const partitioned = partitionPiCompatibleSessions(path, driver, parsed);
        const instanceId = ProviderInstanceId.make(rawInstanceId);
        for (const item of partitioned.topLevel)
          yield* importTopLevelSession(
            item.sourcePath,
            item.session,
            instanceId,
            driver,
            activeFiles.has(path.resolve(item.sourcePath)),
            binaryPath,
            configuredSessionDir === undefined
              ? undefined
              : sessionRoot(configuredSessionDir, defaultRoot),
          );
        for (const item of partitioned.subagents)
          yield* importSubagentSession(item.session, item.parent, driver);
      }
      previouslyActiveSessionFiles = {
        omp: new Set(activeSessionFiles.omp),
        piAgent: new Set(activeSessionFiles.piAgent),
      };
    });

    const start: PiCompatibleSessionImporterShape["start"] = () =>
      Effect.gen(function* () {
        let nextFullScanAt = 0;
        yield* forkParked(
          Effect.forever(
            Effect.gen(function* () {
              const now = yield* Clock.currentTimeMillis;
              const mode = now >= nextFullScanAt ? "full" : "active";
              if (mode === "full") nextFullScanAt = now + scanIntervalMs;
              yield* scan(mode).pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning("pi-compatible-import.sweep-failed", { cause, mode }),
                ),
              );
              yield* Effect.sleep(Duration.millis(activeScanIntervalMs));
            }),
          ),
        );
        yield* Effect.logInfo("pi-compatible-import.started", {
          activeScanIntervalMs,
          fullScanIntervalMs: scanIntervalMs,
        });
      });
    return { start } satisfies PiCompatibleSessionImporterShape;
  });

export const PiCompatibleSessionImporterLive = Layer.effect(
  PiCompatibleSessionImporter,
  makePiCompatibleSessionImporter(),
);
