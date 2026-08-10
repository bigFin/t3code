import {
  CommandId,
  DEFAULT_MODEL,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  MessageId,
  ModelSelection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
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
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import {
  PiCompatibleSessionImporter,
  type PiCompatibleSessionImporterShape,
} from "../Services/PiCompatibleSessionImporter.ts";

const SCAN_INTERVAL_MS = 60_000;
const PI_DRIVER = ProviderDriverKind.make("piAgent");
const OMP_DRIVER = ProviderDriverKind.make("omp");
type UnknownRecord = Record<string, unknown>;
const decodeUnknownJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeUnknownJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const ImportedRuntimePayload = Schema.Struct({
  importedFrom: Schema.String,
  importedTitle: Schema.optionalKey(Schema.String),
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

const IMPORTED_TITLE_MAX_LENGTH = 120;

function titleFromFirstUserMessage(
  messages: ReadonlyArray<PiCompatibleImportedMessage>,
): string | undefined {
  const text = messages.find((message) => message.role === "user")?.text;
  if (text === undefined) return undefined;
  const firstLine = text
    .split(/\r?\n/u)
    .map((line) => line.replace(/\s+/gu, " ").trim())
    .find(Boolean);
  if (firstLine === undefined) return undefined;
  if (firstLine.length <= IMPORTED_TITLE_MAX_LENGTH) return firstLine;
  return `${firstLine.slice(0, IMPORTED_TITLE_MAX_LENGTH - 3).trimEnd()}...`;
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
    title: title ?? stringValue(session?.title) ?? titleFromFirstUserMessage(messages),
    createdAt: isoTimestamp(session?.timestamp, updatedAt),
    messages,
  };
}

export function piCompatibleTurnReconcileCommand(threadId: ThreadId, session: PiCompatibleSession) {
  const latestMessage = session.messages.at(-1);
  if (latestMessage === undefined) return undefined;
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

const makePiCompatibleSessionImporter = (options?: { readonly scanIntervalMs?: number }) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    const snapshots = yield* ProjectionSnapshotQuery;
    const directory = yield* ProviderSessionDirectory;
    const settingsService = yield* ServerSettingsService;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const scanIntervalMs = options?.scanIntervalMs ?? SCAN_INTERVAL_MS;

    const scan = Effect.fn("PiCompatibleSessionImporter.scan")(function* () {
      const settings = yield* settingsService.getSettings;
      for (const [rawInstanceId, entry] of Object.entries(
        deriveProviderInstanceConfigMap(settings),
      )) {
        const driver = entry.driver;
        if ((driver !== PI_DRIVER && driver !== OMP_DRIVER) || entry.enabled === false) continue;
        const importedFrom = driver === OMP_DRIVER ? "omp" : "pi";
        const config = (entry.config ?? {}) as UnknownRecord;
        if (config.enabled === false) continue;
        const root = sessionRoot(
          stringValue(config.sessionDir) ?? "",
          driver === OMP_DRIVER ? "~/.omp/agent/sessions" : "~/.pi/agent/sessions",
        );
        const instanceId = ProviderInstanceId.make(rawInstanceId);
        for (const sourcePath of yield* listSessionFiles(fileSystem, path, root)) {
          const contents = yield* fileSystem.readFileString(sourcePath).pipe(Effect.option);
          if (Option.isNone(contents)) continue;
          const imported = parsePiCompatibleSession(contents.value, driver, sourcePath);
          if (
            imported === undefined ||
            !imported.messages.some((message) => message.role === "user")
          )
            continue;
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
          const turnCommand = piCompatibleTurnReconcileCommand(threadId, imported);
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
                importedFrom,
                ...(imported.title === undefined ? {} : { importedTitle: imported.title }),
              },
            });
          else if (imported.title !== undefined) {
            const payload = decodeImportedRuntimePayload(binding.value.runtimePayload);
            if (Option.isSome(payload) && payload.value.importedFrom === importedFrom) {
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
        }
      }
    });

    const start: PiCompatibleSessionImporterShape["start"] = () =>
      Effect.gen(function* () {
        yield* forkParked(
          Effect.forever(
            Effect.gen(function* () {
              yield* scan().pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning("pi-compatible-import.sweep-failed", { cause }),
                ),
              );
              yield* Effect.sleep(Duration.millis(scanIntervalMs));
            }),
          ),
        );
        yield* Effect.logInfo("pi-compatible-import.started", { scanIntervalMs });
      });
    return { start } satisfies PiCompatibleSessionImporterShape;
  });

export const PiCompatibleSessionImporterLive = Layer.effect(
  PiCompatibleSessionImporter,
  makePiCompatibleSessionImporter(),
);
