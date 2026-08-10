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
import { listOpenProcessFiles } from "../piSessionFiles.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import {
  PiCompatibleSessionImporter,
  type PiCompatibleSessionImporterShape,
} from "../Services/PiCompatibleSessionImporter.ts";

// Discover historical sessions on the slower sweep; between those walks, only
// re-read files another process currently has open so CLI progress stays fresh.
const FULL_SCAN_INTERVAL_MS = 60_000;
const ACTIVE_SCAN_INTERVAL_MS = 2_000;
const OMP_DRIVER = ProviderDriverKind.make("omp");
const PI_DRIVER = ProviderDriverKind.make("piAgent");
type UnknownRecord = Record<string, unknown>;
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

export interface PiCompatibleSession {
  readonly id: string;
  readonly cwd: string;
  readonly title: string | undefined;
  readonly parentSession: string | undefined;
  readonly createdAt: string;
  readonly messages: ReadonlyArray<PiCompatibleImportedMessage>;
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
  let title: string | undefined;
  let updatedAt = fallbackTime;
  const messages: PiCompatibleImportedMessage[] = [];

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
    if (record.type === "title" && title === undefined) title = stringValue(record.title);
    if (
      record.type !== "message" ||
      typeof record.message !== "object" ||
      record.message === null ||
      Array.isArray(record.message)
    )
      continue;
    const message = record.message as UnknownRecord;
    if (message.role !== "user" && message.role !== "assistant") continue;
    const text = textFromContent(message.content);
    if (text === undefined) continue;
    const recordId = stringValue(record.id) ?? `${messages.length}`;
    const sessionId = stringValue(session?.id) ?? sourcePath;
    messages.push({
      messageId: MessageId.make(`${driver}:${stableTextHash(sessionId)}:${recordId}`),
      role: message.role,
      text,
      turnId: TurnId.make(`${driver}:${stableTextHash(sessionId)}:${recordId}`),
      createdAt: timestamp,
    });
  }

  const id = stringValue(session?.id);
  const cwd = stringValue(session?.cwd);
  if (id === undefined || cwd === undefined) return undefined;
  return {
    id,
    cwd,
    parentSession: stringValue(session?.parentSession),
    title: title ?? stringValue(session?.title) ?? titleFromFirstUserMessage(messages),
    createdAt: isoTimestamp(session?.timestamp, updatedAt),
    messages,
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
    currentNativeSession?.ownership === "t3" ||
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
      const openFiles = yield* listOpenProcessFiles(fileSystem, path);
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
        const sourcePaths =
          mode === "full"
            ? yield* listSessionFiles(fileSystem, path, root)
            : [...openFiles]
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
        const byPath = new Map(
          parsed.map(({ sourcePath, session }) => [path.resolve(sourcePath), session] as const),
        );
        const instanceId = ProviderInstanceId.make(rawInstanceId);
        for (const item of parsed)
          if (item.session.parentSession === undefined)
            yield* importTopLevelSession(
              item.sourcePath,
              item.session,
              instanceId,
              driver,
              openFiles.has(path.resolve(item.sourcePath)),
              binaryPath,
              configuredSessionDir === undefined
                ? undefined
                : sessionRoot(configuredSessionDir, defaultRoot),
            );
        for (const item of parsed) {
          if (item.session.parentSession === undefined) continue;
          const parentPath = path.resolve(
            path.dirname(item.sourcePath),
            item.session.parentSession,
          );
          const parent = byPath.get(parentPath);
          if (parent !== undefined) yield* importSubagentSession(item.session, parent, driver);
        }
      }
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
