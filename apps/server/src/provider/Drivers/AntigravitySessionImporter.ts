import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";
import { fileURLToPath } from "node:url";

import {
  ANTIGRAVITY_DEFAULT_MODEL,
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  defaultInstanceIdForDriver,
  EventId,
  MessageId,
  ModelSelection,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  TurnId,
  type OrchestrationThreadActivity,
} from "@t3tools/contracts";
import { HostProcessEnvironment, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";

import { ServerConfig } from "../../config.ts";
import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { forkParked } from "../../serverActivation.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import { resolveAntigravityUserHome } from "./AntigravitySkills.ts";
import {
  AntigravitySessionImporter,
  type AntigravitySessionImporterShape,
} from "../Services/AntigravitySessionImporter.ts";

const ANTIGRAVITY_DRIVER = ProviderDriverKind.make("antigravity");
const FULL_SCAN_INTERVAL_MS = 60_000;
const ACTIVE_SCAN_INTERVAL_MS = 5_000;

export interface AntigravityImportedMessage {
  readonly messageId: MessageId;
  readonly role: "user" | "assistant";
  readonly text: string;
  readonly turnId: TurnId;
  readonly createdAt: string;
}

interface AntigravitySummaryRow {
  readonly conversation_id: string;
  readonly title: string | null;
  readonly preview: string | null;
  readonly step_count: number | null;
  readonly last_modified_time: string | null;
  readonly workspace_uris: string | null;
  readonly status: string | null;
  readonly project_id: string | null;
  readonly parent_conversation_id: string | null;
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
  return ProjectId.make(`antigravity-project-${stableTextHash(cwd)}`);
}

function stableThreadId(conversationId: string): ThreadId {
  return ThreadId.make(`antigravity-${conversationId}`);
}

function stableCommandId(...parts: ReadonlyArray<string>): CommandId {
  return CommandId.make(["antigravity-import", ...parts].join(":"));
}

function isoTimestamp(value: unknown, fallback: string): string {
  if (typeof value !== "string" || value.trim().length === 0) return fallback;
  try {
    return DateTime.formatIso(DateTime.makeUnsafe(value));
  } catch {
    return fallback;
  }
}

export function parseWorkspaceUris(raw: string | null | undefined): string | undefined {
  if (!raw || typeof raw !== "string") return undefined;
  const trimmed = raw.trim();
  if (!trimmed) return undefined;
  let uris: Array<string> = [];
  try {
    const parsed: unknown = JSON.parse(trimmed);
    if (Array.isArray(parsed)) {
      uris = parsed.filter(
        (item): item is string => typeof item === "string" && item.trim().length > 0,
      );
    } else if (typeof parsed === "string") {
      uris = [parsed];
    }
  } catch {
    uris = [trimmed];
  }
  for (const uri of uris) {
    try {
      if (uri.startsWith("file://")) {
        return fileURLToPath(uri);
      }
      return uri;
    } catch {
      // Continue to next candidate
    }
  }
  return undefined;
}

export function extractAntigravityUserRequest(content: string): string {
  const match = /<USER_REQUEST>([\s\S]*?)<\/USER_REQUEST>/i.exec(content);
  if (match && match[1]) {
    return match[1].trim();
  }
  return (
    content
      .replace(/<ADDITIONAL_METADATA>[\s\S]*?<\/ADDITIONAL_METADATA>/gi, "")
      .replace(/<USER_SETTINGS_CHANGE>[\s\S]*?<\/USER_SETTINGS_CHANGE>/gi, "")
      .replace(/<CONTEXT_SUMMARY>[\s\S]*?<\/CONTEXT_SUMMARY>/gi, "")
      .trim() || content.trim()
  );
}

function toolItemType(
  name: string,
):
  | "command_execution"
  | "file_change"
  | "web_search"
  | "collab_agent_tool_call"
  | "dynamic_tool_call" {
  const normalized = name.toLowerCase();
  if (normalized === "run_command" || normalized === "bash" || normalized === "execute_command") {
    return "command_execution";
  }
  if (
    normalized === "write_to_file" ||
    normalized === "replace_file_content" ||
    normalized === "edit" ||
    normalized === "write"
  ) {
    return "file_change";
  }
  if (
    normalized === "search_web" ||
    normalized === "read_url_content" ||
    normalized === "web_search"
  ) {
    return "web_search";
  }
  if (
    normalized === "invoke_subagent" ||
    normalized === "define_subagent" ||
    normalized === "task" ||
    normalized === "agent"
  ) {
    return "collab_agent_tool_call";
  }
  return "dynamic_tool_call";
}

export function parseAntigravityTranscript(
  rawJsonl: string,
  threadId: ThreadId,
  fallbackTimestamp: string,
): {
  readonly messages: ReadonlyArray<AntigravityImportedMessage>;
  readonly activities: ReadonlyArray<OrchestrationThreadActivity>;
} {
  const lines = rawJsonl.split("\n");
  const messages: Array<AntigravityImportedMessage> = [];
  const activities: Array<OrchestrationThreadActivity> = [];
  let currentTurnId: TurnId = TurnId.make(`${threadId}:turn:0`);

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    let entry: Record<string, unknown>;
    try {
      entry = JSON.parse(trimmed);
    } catch {
      continue;
    }
    if (typeof entry !== "object" || entry === null) continue;

    const stepIndex = typeof entry.step_index === "number" ? entry.step_index : messages.length;
    const createdAt = isoTimestamp(entry.created_at, fallbackTimestamp);
    const type = typeof entry.type === "string" ? entry.type : "";
    const source = typeof entry.source === "string" ? entry.source : "";

    if (type === "USER_INPUT" || source === "USER_EXPLICIT") {
      currentTurnId = TurnId.make(`${threadId}:turn:${stepIndex}`);
      const rawContent = typeof entry.content === "string" ? entry.content : "";
      const text = extractAntigravityUserRequest(rawContent);
      if (text.length > 0) {
        messages.push({
          messageId: MessageId.make(`${threadId}:msg:${stepIndex}`),
          role: "user",
          text,
          turnId: currentTurnId,
          createdAt,
        });
      }
      continue;
    }

    if (type === "PLANNER_RESPONSE" || source === "MODEL") {
      const rawContent = typeof entry.content === "string" ? entry.content.trim() : "";
      if (rawContent.length > 0) {
        messages.push({
          messageId: MessageId.make(`${threadId}:msg:${stepIndex}`),
          role: "assistant",
          text: rawContent,
          turnId: currentTurnId,
          createdAt,
        });
      }

      if (Array.isArray(entry.tool_calls)) {
        for (let callIndex = 0; callIndex < entry.tool_calls.length; callIndex++) {
          const call = entry.tool_calls[callIndex] as Record<string, unknown> | null;
          if (!call || typeof call !== "object") continue;
          const name = typeof call.name === "string" ? call.name : "tool";
          const args = (call.args && typeof call.args === "object" ? call.args : {}) as Record<
            string,
            unknown
          >;
          const itemType = toolItemType(name);
          const summary =
            (typeof args.toolSummary === "string" &&
              args.toolSummary.trim().replace(/^"|"$/g, "")) ||
            (typeof args.toolAction === "string" && args.toolAction.trim().replace(/^"|"$/g, "")) ||
            `${name} call`;

          activities.push({
            id: EventId.make(`${threadId}:act:${stepIndex}:${callIndex}`),
            tone: "tool",
            kind: "tool.updated",
            summary,
            payload: {
              itemType,
              status: "completed",
              title: name,
              data: {
                toolCallId: `${stepIndex}:${callIndex}`,
                kind: itemType === "command_execution" ? "execute" : name,
                item: {
                  name,
                  input: args,
                  ...(typeof args.CommandLine === "string" ? { command: args.CommandLine } : {}),
                },
              },
            },
            turnId: currentTurnId,
            createdAt,
          });
        }
      }
    }
  }

  return { messages, activities };
}

export function antigravityTurnReconcileCommand(
  threadId: ThreadId,
  latestMessage: AntigravityImportedMessage | undefined,
  isRunning = false,
) {
  if (latestMessage === undefined || (isRunning && latestMessage.role === "user")) return undefined;
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

function normalizeForWorktreeMatch(value: string, caseFold: boolean): string {
  const normalized = `${value.replaceAll("\\", "/")}/`;
  return caseFold ? normalized.toLowerCase() : normalized;
}

function isT3ManagedWorktree(
  candidatePath: string,
  worktreesDir: string,
  caseFold: boolean,
): boolean {
  const normalized = normalizeForWorktreeMatch(candidatePath, caseFold);
  return (
    normalized.startsWith(normalizeForWorktreeMatch(worktreesDir, caseFold)) ||
    normalized.includes("/.t3/worktrees/")
  );
}

export const makeAntigravitySessionImporter = (options?: { readonly customDataDir?: string }) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    const snapshots = yield* ProjectionSnapshotQuery;
    const directory = yield* ProviderSessionDirectory;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const serverConfig = yield* ServerConfig;
    const platform = yield* HostProcessPlatform;
    const environment = yield* HostProcessEnvironment;
    const worktreesDir = path.resolve(serverConfig.worktreesDir);
    const foldWorktreeCase = platform === "win32";

    const userHome = resolveAntigravityUserHome(platform, environment);
    const baseDataDir =
      options?.customDataDir ??
      process.env.ANTIGRAVITY_APP_DATA_DIR ??
      (process.env.GEMINI_HOME
        ? path.join(process.env.GEMINI_HOME, "antigravity-cli")
        : path.join(userHome, ".gemini", "antigravity-cli"));

    const dbPath = path.join(baseDataDir, "conversation_summaries.db");
    const brainDir = path.join(baseDataDir, "brain");
    const instanceId = defaultInstanceIdForDriver(ANTIGRAVITY_DRIVER);
    const modelSelection = {
      instanceId,
      model: ANTIGRAVITY_DEFAULT_MODEL,
    } satisfies ModelSelection;

    const importedActivityIdsByThread = new Map<ThreadId, Set<EventId>>();
    let lastSummaryDbMtimeMs = 0;

    const importActivities = Effect.fn("AntigravitySessionImporter.importActivities")(function* (
      threadId: ThreadId,
      activities: ReadonlyArray<OrchestrationThreadActivity>,
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
          stableTextHash(JSON.stringify(unseen.map((activity) => activity.id))),
        ),
        threadId,
        activities: unseen,
        createdAt: unseen.at(-1)!.createdAt,
      });
      for (const activity of unseen) known.add(activity.id);
    });

    const importSessionRow = Effect.fn("AntigravitySessionImporter.importSessionRow")(function* (
      row: AntigravitySummaryRow,
    ) {
      if (!row.conversation_id || row.conversation_id.trim().length === 0) return;
      // Skip subagent sessions as standalone threads
      if (row.parent_conversation_id && row.parent_conversation_id.trim().length > 0) return;

      const rawCwd = parseWorkspaceUris(row.workspace_uris);
      if (!rawCwd) return;
      const cwd = path.resolve(rawCwd);

      // Verify cwd exists and is not a T3 managed sandbox
      const cwdStat = yield* fileSystem.stat(cwd).pipe(Effect.option);
      if (Option.isNone(cwdStat) || cwdStat.value.type !== "Directory") return;
      if (isT3ManagedWorktree(cwd, worktreesDir, foldWorktreeCase)) return;

      const threadId = stableThreadId(row.conversation_id);
      const title = row.title?.trim() || row.preview?.trim() || "Antigravity Session";
      const createdAt = isoTimestamp(
        row.last_modified_time,
        DateTime.formatIso(DateTime.makeUnsafe(Date.now())),
      );

      // Find or create Project
      const project = yield* snapshots.getActiveProjectByWorkspaceRoot(cwd);
      const projectId = Option.match(project, {
        onSome: (value) => value.id,
        onNone: () => stableProjectId(cwd),
      });

      if (Option.isNone(project)) {
        yield* engine.dispatch({
          type: "project.create",
          commandId: stableCommandId("project", projectId),
          projectId,
          title: path.basename(cwd) || "Antigravity Project",
          workspaceRoot: cwd,
          defaultModelSelection: modelSelection,
          createdAt,
        });
      }

      // Find or create Thread
      const existingThread = (yield* snapshots.getThreadShellsByIds([threadId])).get(threadId);
      if (existingThread?.session !== null && existingThread?.session !== undefined) {
        // Active session already running in T3, leave it alone
        return;
      }

      const transcript = yield* snapshots.getThreadTranscriptById(threadId);
      if (Option.isNone(transcript)) {
        yield* engine.dispatch({
          type: "thread.create",
          commandId: stableCommandId("thread", threadId),
          threadId,
          projectId,
          title,
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          branch: null,
          worktreePath: null,
          createdAt,
        });
      }

      // Read transcript JSONL if present
      const transcriptFile = path.join(
        brainDir,
        row.conversation_id,
        ".system_generated",
        "logs",
        "transcript.jsonl",
      );
      let parsed = {
        messages: [] as ReadonlyArray<AntigravityImportedMessage>,
        activities: [] as ReadonlyArray<OrchestrationThreadActivity>,
      };
      const transcriptExists = yield* fileSystem
        .exists(transcriptFile)
        .pipe(Effect.orElseSucceed(() => false));

      if (transcriptExists) {
        const content = yield* fileSystem
          .readFileString(transcriptFile)
          .pipe(Effect.orElseSucceed(() => ""));
        if (content.trim().length > 0) {
          parsed = parseAntigravityTranscript(content, threadId, createdAt);
        }
      }

      // Fallback message if no transcript was found or parsed
      let messagesToImport = parsed.messages;
      if (messagesToImport.length === 0 && (row.preview?.trim() || row.title?.trim())) {
        const previewText = row.preview?.trim() || row.title?.trim() || "Antigravity Session";
        messagesToImport = [
          {
            messageId: MessageId.make(`${threadId}:msg:initial`),
            role: "user",
            text: previewText,
            turnId: TurnId.make(`${threadId}:turn:0`),
            createdAt,
          },
        ];
      }

      // Import unseen messages
      const knownMessageIds = new Set(
        Option.getOrUndefined(transcript)?.messages.map((msg) => msg.id) ?? [],
      );
      const newMessages = messagesToImport.filter((msg) => !knownMessageIds.has(msg.messageId));
      if (newMessages.length > 0) {
        yield* engine.dispatch({
          type: "thread.messages.import",
          commandId: stableCommandId(
            "messages",
            threadId,
            stableTextHash(JSON.stringify(newMessages.map((msg) => msg.messageId))),
          ),
          threadId,
          messages: newMessages,
          createdAt: newMessages.at(-1)!.createdAt,
        });
      }

      // Import unseen activities
      yield* importActivities(threadId, parsed.activities);

      // Reconcile turn state
      const isRunning = row.status === "CASCADE_RUN_STATUS_RUNNING";
      const turnCommand = antigravityTurnReconcileCommand(
        threadId,
        messagesToImport.at(-1),
        isRunning,
      );
      if (turnCommand !== undefined) {
        yield* engine.dispatch(turnCommand);
      }

      // Register binding with resume cursor in ProviderSessionDirectory
      const binding = yield* directory.getBinding(threadId);
      if (Option.isNone(binding)) {
        yield* directory.insertIfAbsent({
          threadId,
          provider: ANTIGRAVITY_DRIVER,
          providerInstanceId: instanceId,
          status: isRunning ? "running" : "stopped",
          runtimeMode: "full-access",
          resumeCursor: {
            schemaVersion: 1,
            sessionId: row.conversation_id,
          },
          runtimePayload: {
            cwd,
            importedFrom: "antigravity",
            importedTitle: title,
          },
        });
      } else {
        const payload = binding.value.runtimePayload as Record<string, unknown> | undefined;
        if (payload?.importedFrom === "antigravity" && title !== payload.importedTitle) {
          yield* engine.dispatch({
            type: "thread.meta.update",
            commandId: stableCommandId("title", threadId, stableTextHash(title)),
            threadId,
            title,
          });
          yield* directory.mergeRuntimePayload(threadId, { importedTitle: title });
        }
      }
    });

    const scan = Effect.fn("AntigravitySessionImporter.scan")(function* (
      mode: "full" | "active" = "full",
    ) {
      const exists = yield* fileSystem.exists(dbPath).pipe(Effect.orElseSucceed(() => false));
      if (!exists) return;

      if (mode === "active") {
        const stat = yield* fileSystem.stat(dbPath).pipe(Effect.option);
        const walStat = yield* fileSystem.stat(`${dbPath}-wal`).pipe(Effect.option);
        const mtimeMs = Math.max(
          Option.match(stat, {
            onNone: () => 0,
            onSome: (s) => Option.match(s.mtime, { onNone: () => 0, onSome: (d) => d.getTime() }),
          }),
          Option.match(walStat, {
            onNone: () => 0,
            onSome: (s) => Option.match(s.mtime, { onNone: () => 0, onSome: (d) => d.getTime() }),
          }),
        );
        if (mtimeMs <= lastSummaryDbMtimeMs && mtimeMs > 0) {
          return;
        }
        lastSummaryDbMtimeMs = mtimeMs;
      }

      let rows: ReadonlyArray<AntigravitySummaryRow> = [];
      try {
        const db = new NodeSqlite.DatabaseSync(dbPath, { readOnly: true });
        try {
          const query = `
            SELECT conversation_id, title, preview, step_count, last_modified_time, workspace_uris, status, project_id, parent_conversation_id
            FROM conversation_summaries
            WHERE workspace_uris IS NOT NULL AND workspace_uris != ''
            ORDER BY last_modified_time DESC
            LIMIT 100
          `;
          rows = db.prepare(query).all() as unknown as ReadonlyArray<AntigravitySummaryRow>;
        } finally {
          db.close();
        }
      } catch (cause) {
        yield* Effect.logWarning("Could not read Antigravity conversation summaries database", {
          dbPath,
          cause,
        });
        return;
      }

      for (const row of rows) {
        yield* importSessionRow(row).pipe(
          Effect.catch((cause) =>
            Effect.logWarning("Failed to import Antigravity session", {
              conversationId: row.conversation_id,
              cause,
            }),
          ),
        );
      }
    });

    const start: AntigravitySessionImporterShape["start"] = () =>
      Effect.gen(function* () {
        let nextFullScanAt = 0;
        yield* forkParked(
          Effect.forever(
            Effect.gen(function* () {
              const now = yield* Clock.currentTimeMillis;
              const mode = now >= nextFullScanAt ? "full" : "active";
              if (mode === "full") nextFullScanAt = now + FULL_SCAN_INTERVAL_MS;
              yield* scan(mode).pipe(
                Effect.catch((cause) =>
                  Effect.logWarning("antigravity-session-importer.sweep-failed", { cause, mode }),
                ),
              );
              yield* Effect.sleep(Duration.millis(ACTIVE_SCAN_INTERVAL_MS));
            }),
          ),
        );
      });

    return { scan, start } satisfies AntigravitySessionImporterShape;
  });

export const AntigravitySessionImporterLive = Layer.effect(
  AntigravitySessionImporter,
  makeAntigravitySessionImporter(),
);
