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
} from "@t3tools/contracts";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexErrors from "effect-codex-app-server/errors";
import type * as CodexSchema from "effect-codex-app-server/schema";

import { ServerConfig } from "../../config.ts";
import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { expandHomePath } from "../../pathExpansion.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  ProviderSessionDirectory,
  type ProviderRuntimeBinding,
} from "../Services/ProviderSessionDirectory.ts";
import {
  CodexCliSessionImporter,
  type CodexCliSessionImporterShape,
} from "../Services/CodexCliSessionImporter.ts";
import { resolveCodexHomeLayout, type CodexHomeLayout } from "./CodexHomeLayout.ts";
import { deriveProviderInstanceConfigMap } from "../Layers/ProviderInstanceRegistryHydration.ts";
import { buildCodexInitializeParams } from "../Layers/CodexProvider.ts";
import { codexAppServerArgs, resolveCodexLaunchArgs } from "../Layers/codexLaunchArgs.ts";

const CODEX_DRIVER = ProviderDriverKind.make("codex");
const DEFAULT_SCAN_INTERVAL_MS = 60_000;
const CODEX_APP_SERVER_FORCE_KILL_AFTER = "2 seconds" as const;
const THREAD_LIST_PAGE_SIZE = 100;
const MAX_INTERACTIVE_THREADS_PER_SCAN = 100;
export const CODEX_INTERACTIVE_SOURCE_KINDS = ["cli", "vscode"] as const;

const decodeCodexSettings = Schema.decodeUnknownEffect(CodexSettings);
const isModelSelection = Schema.is(ModelSelection);

type CodexListedThread = CodexSchema.V2ThreadListResponse["data"][number];
type CodexReadThread = CodexSchema.V2ThreadReadResponse["thread"];
type CodexThreadItem = CodexReadThread["turns"][number]["items"][number];
type CodexUserInput = Extract<CodexThreadItem, { readonly type: "userMessage" }>["content"][number];

interface CodexDiscoveryTarget {
  readonly instanceId: ProviderInstanceId;
  readonly config: CodexSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly homeLayout: CodexHomeLayout;
}

export interface CodexCliImportedMessage {
  readonly messageId: MessageId;
  readonly role: "user" | "assistant";
  readonly text: string;
  readonly turnId: TurnId;
  readonly createdAt: string;
}

function stableTextHash(value: string): string {
  let hash = 0xcbf29ce484222325n;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= BigInt(value.charCodeAt(index));
    hash = BigInt.asUintN(64, hash * 0x100000001b3n);
  }
  return hash.toString(16).padStart(16, "0");
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

export function isCurrentCodexCliImport(
  binding: ProviderRuntimeBinding | undefined,
  listedThread: CodexListedThread,
): boolean {
  return readImportedCodexUpdatedAt(binding?.runtimePayload) === listedThread.updatedAt;
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
      targets.push({
        instanceId,
        config,
        environment: mergeProviderInstanceEnvironment(entry.environment),
        homeLayout,
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

const withCodexClient = <A, E, R>(
  target: CodexDiscoveryTarget,
  use: (client: CodexClient.CodexAppServerClient["Service"]) => Effect.Effect<A, E, R>,
): Effect.Effect<
  A,
  E | CodexErrors.CodexAppServerError,
  R | ChildProcessSpawner.ChildProcessSpawner | ServerConfig
> =>
  Effect.scoped(
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const serverConfig = yield* ServerConfig;
      const environment = {
        ...target.environment,
        CODEX_HOME: expandHomePath(target.homeLayout.sharedHomePath),
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
          Effect.mapError(
            (cause) =>
              new CodexErrors.CodexAppServerSpawnError({
                command: `${target.config.binaryPath} app-server`,
                cause,
              }),
          ),
        );
      const clientContext = yield* Layer.build(CodexClient.layerChildProcess(child));
      const client = yield* Effect.service(CodexClient.CodexAppServerClient).pipe(
        Effect.provide(clientContext),
      );
      yield* client.request("initialize", buildCodexInitializeParams());
      yield* client.notify("initialized", undefined);
      return yield* use(client);
    }),
  );

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
    const path = yield* Path.Path;
    const runtimeContext = yield* Effect.context<
      ChildProcessSpawner.ChildProcessSpawner | Path.Path | ServerConfig | ServerSettingsService
    >();
    const scanIntervalMs = Math.max(1, options?.scanIntervalMs ?? DEFAULT_SCAN_INTERVAL_MS);

    const importThread = Effect.fn("CodexCliSessionImporter.importThread")(function* (
      target: CodexDiscoveryTarget,
      client: CodexClient.CodexAppServerClient["Service"],
      listedThread: CodexListedThread,
    ) {
      if (!isImportableCodexInteractiveThread(listedThread)) {
        return false;
      }

      const threadId = ThreadId.make(listedThread.id);
      const existingBinding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      if (existingBinding !== undefined && existingBinding.provider !== CODEX_DRIVER) {
        yield* Effect.logWarning("codex.cli-import.binding-conflict", {
          threadId,
          existingProvider: existingBinding.provider,
          discoveredInstanceId: target.instanceId,
        });
        return false;
      }
      if (isLiveCodexBinding(existingBinding)) {
        return false;
      }

      const existingThread = yield* projectionSnapshotQuery.getThreadShellById(threadId);
      const projectedThread = Option.getOrUndefined(existingThread);
      if (shouldInterruptStaleCodexCliSession(existingBinding, projectedThread?.session)) {
        const staleSession = projectedThread?.session;
        if (staleSession !== null && staleSession !== undefined) {
          const interruptedAt = DateTime.formatIso(yield* DateTime.now);
          yield* orchestrationEngine.dispatch({
            type: "thread.session.set",
            commandId: stableCommandId(
              "session-interrupted",
              threadId,
              staleSession.activeTurnId ?? staleSession.updatedAt,
            ),
            threadId,
            session: {
              ...staleSession,
              status: "interrupted",
              activeTurnId: null,
              updatedAt: interruptedAt,
            },
            createdAt: interruptedAt,
          });
          yield* Effect.logInfo("codex.cli-import.interrupted-stale-session", {
            threadId,
            previousStatus: staleSession.status,
            previousActiveTurnId: staleSession.activeTurnId,
          });
        }
      }

      if (projectedThread !== undefined && isCurrentCodexCliImport(existingBinding, listedThread)) {
        return false;
      }

      const response = yield* client.request("thread/read", {
        threadId: listedThread.id,
        includeTurns: true,
      });
      const thread = response.thread;
      const messages = collectCodexCliImportedMessages(thread);
      if (messages.length === 0) {
        return false;
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

      for (const message of messages) {
        yield* orchestrationEngine.dispatch(
          codexCliMessageImportCommand({
            threadId,
            message,
          }),
        );
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
            codexCliUpdatedAt: thread.updatedAt,
          },
        });
      }
      return true;
    });

    const scanTarget = Effect.fn("CodexCliSessionImporter.scanTarget")(function* (
      target: CodexDiscoveryTarget,
    ) {
      return yield* withCodexClient(target, (client) =>
        Effect.gen(function* () {
          const threads = yield* listInteractiveThreads(client);
          let importedCount = 0;
          for (const thread of threads) {
            const imported = yield* importThread(target, client, thread).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("codex.cli-import.thread-failed", {
                  instanceId: target.instanceId,
                  codexHome: target.homeLayout.sharedHomePath,
                  providerThreadId: thread.id,
                  cause,
                }).pipe(Effect.as(false)),
              ),
            );
            if (imported) {
              importedCount += 1;
            }
          }
          return {
            discoveredCount: threads.length,
            importedCount,
          };
        }),
      );
    });

    const scan = Effect.gen(function* () {
      const targets = yield* resolveDiscoveryTargets();
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
              }),
            ),
          ),
        );
        if (result.discoveredCount > 0) {
          yield* Effect.logInfo("codex.cli-import.scan-complete", {
            instanceId: target.instanceId,
            codexHome: target.homeLayout.sharedHomePath,
            ...result,
          });
        }
      }
    });

    const start: CodexCliSessionImporterShape["start"] = () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(
          scan.pipe(
            Effect.provideContext(runtimeContext),
            Effect.catchCause((cause) =>
              Effect.logWarning("codex.cli-import.sweep-failed", { cause }),
            ),
            Effect.repeat(Schedule.spaced(Duration.millis(scanIntervalMs))),
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
