import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { MessageId, ProjectId, ProviderDriverKind, ThreadId, TurnId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import { ServerConfig } from "../../config.ts";
import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import {
  antigravityTurnReconcileCommand,
  extractAntigravityUserRequest,
  makeAntigravitySessionImporter,
  parseAntigravityTranscript,
  parseWorkspaceUris,
} from "./AntigravitySessionImporter.ts";

const THREAD_ID = ThreadId.make("antigravity-test-conv");

describe("AntigravitySessionImporter", () => {
  describe("extractAntigravityUserRequest", () => {
    it("extracts clean request from USER_REQUEST tags", () => {
      const input = `
<USER_REQUEST>
Please fix the bug in server.ts
</USER_REQUEST>
<ADDITIONAL_METADATA>
The current local time is 2026-09-14
</ADDITIONAL_METADATA>
<USER_SETTINGS_CHANGE>
Model changed to Gemini
</USER_SETTINGS_CHANGE>
      `;
      expect(extractAntigravityUserRequest(input)).toBe("Please fix the bug in server.ts");
    });

    it("strips metadata tags when USER_REQUEST tags are omitted", () => {
      const input = `
Inspect the codebase.
<ADDITIONAL_METADATA>
Some metadata
</ADDITIONAL_METADATA>
      `;
      expect(extractAntigravityUserRequest(input)).toBe("Inspect the codebase.");
    });

    it("returns raw text when no XML tags exist", () => {
      expect(extractAntigravityUserRequest("Simple prompt")).toBe("Simple prompt");
    });
  });

  describe("parseWorkspaceUris", () => {
    it("parses file:// URI from JSON array", () => {
      const parsed = parseWorkspaceUris('["file:///home/user/code"]');
      expect(parsed).toBe(NodePath.resolve("/home/user/code"));
    });

    it("parses multiple URIs picking the first valid one", () => {
      const parsed = parseWorkspaceUris('["file:///primary/path", "file:///secondary/path"]');
      expect(parsed).toBe(NodePath.resolve("/primary/path"));
    });

    it("returns undefined for empty, null, or invalid input", () => {
      expect(parseWorkspaceUris(null)).toBeUndefined();
      expect(parseWorkspaceUris("")).toBeUndefined();
      expect(parseWorkspaceUris("[]")).toBeUndefined();
    });
  });

  describe("parseAntigravityTranscript", () => {
    it("parses USER_INPUT and PLANNER_RESPONSE with tool calls", () => {
      const jsonl = [
        JSON.stringify({
          step_index: 0,
          source: "USER_EXPLICIT",
          type: "USER_INPUT",
          created_at: "2026-09-14T20:00:00Z",
          content: "<USER_REQUEST>Run tests</USER_REQUEST>",
        }),
        JSON.stringify({
          step_index: 1,
          source: "MODEL",
          type: "PLANNER_RESPONSE",
          created_at: "2026-09-14T20:00:05Z",
          content: "I will run the tests now.",
          tool_calls: [
            {
              name: "run_command",
              args: {
                CommandLine: "npm test",
                toolSummary: "Run tests",
              },
            },
          ],
        }),
      ].join("\n");

      const result = parseAntigravityTranscript(jsonl, THREAD_ID, "2026-09-14T20:00:00Z");
      expect(result.messages).toHaveLength(2);
      expect(result.messages[0]).toMatchObject({
        role: "user",
        text: "Run tests",
      });
      expect(result.messages[1]).toMatchObject({
        role: "assistant",
        text: "I will run the tests now.",
      });
      expect(result.activities).toHaveLength(1);
      expect(result.activities[0]).toMatchObject({
        tone: "tool",
        kind: "tool.updated",
        summary: "Run tests",
      });
    });
  });

  describe("antigravityTurnReconcileCommand", () => {
    it("reconciles to completed when latest message is assistant", () => {
      const command = antigravityTurnReconcileCommand(
        THREAD_ID,
        {
          messageId: MessageId.make("msg-1"),
          role: "assistant",
          text: "Done",
          turnId: TurnId.make("turn-1"),
          createdAt: "2026-09-14T20:00:00Z",
        },
        false,
      );

      expect(command).toMatchObject({
        type: "thread.turn.reconcile",
        threadId: THREAD_ID,
        state: "completed",
      });
    });

    it("reconciles to interrupted when latest message is user and session is stopped", () => {
      const command = antigravityTurnReconcileCommand(
        THREAD_ID,
        {
          messageId: MessageId.make("msg-1"),
          role: "user",
          text: "What happened?",
          turnId: TurnId.make("turn-1"),
          createdAt: "2026-09-14T20:00:00Z",
        },
        false,
      );

      expect(command).toMatchObject({
        type: "thread.turn.reconcile",
        threadId: THREAD_ID,
        state: "interrupted",
      });
    });
  });

  describe("importer scan and dispatch", () => {
    it.effect("discovers session from conversation_summaries.db and dispatches commands", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;

        // Prepare temporary directories
        const tempDir = yield* fs.makeTempDirectoryScoped({
          prefix: "t3-antigravity-importer-test-",
        });
        const workspaceDir = path.join(tempDir, "workspace");
        const agyDataDir = path.join(tempDir, "agy-data");
        const brainDir = path.join(agyDataDir, "brain", "conv-uuid-1", ".system_generated", "logs");

        yield* fs.makeDirectory(workspaceDir, { recursive: true });
        yield* fs.makeDirectory(brainDir, { recursive: true });

        // Write transcript.jsonl
        const transcriptPath = path.join(brainDir, "transcript.jsonl");
        const transcriptContent = [
          JSON.stringify({
            step_index: 0,
            source: "USER_EXPLICIT",
            type: "USER_INPUT",
            created_at: "2026-09-14T20:00:00Z",
            content: "<USER_REQUEST>Hello from AGY</USER_REQUEST>",
          }),
          JSON.stringify({
            step_index: 1,
            source: "MODEL",
            type: "PLANNER_RESPONSE",
            created_at: "2026-09-14T20:00:02Z",
            content: "Hello! How can I help?",
          }),
        ].join("\n");
        yield* fs.writeFileString(transcriptPath, transcriptContent);

        // Write SQLite database with conversation summary
        const dbPath = path.join(agyDataDir, "conversation_summaries.db");
        const db = new NodeSqlite.DatabaseSync(dbPath);
        db.exec(`
          CREATE TABLE conversation_summaries (
            conversation_id TEXT PRIMARY KEY,
            title TEXT,
            preview TEXT,
            step_count INTEGER,
            last_modified_time TEXT,
            workspace_uris TEXT,
            status TEXT,
            project_id TEXT,
            parent_conversation_id TEXT
          );
        `);
        const insertStmt = db.prepare(`
          INSERT INTO conversation_summaries (
            conversation_id, title, preview, step_count, last_modified_time, workspace_uris, status, project_id, parent_conversation_id
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        `);
        insertStmt.run(
          "conv-uuid-1",
          "My Imported Session",
          "Hello from AGY",
          2,
          "2026-09-14T20:00:02Z",
          JSON.stringify([`file://${workspaceDir}`]),
          "CASCADE_RUN_STATUS_IDLE",
          "default-cli-project",
          "",
        );
        db.close();

        // Track dispatched commands and bindings
        const dispatchedCommands: Array<any> = [];
        const bindings = new Map<string, any>();

        const mockEngine = OrchestrationEngineService.of({
          dispatch: (command: any) =>
            Effect.sync(() => {
              dispatchedCommands.push(command);
              return { commandId: command.commandId };
            }),
        } as any);

        const mockSnapshots = ProjectionSnapshotQuery.of({
          getActiveProjectByWorkspaceRoot: (_cwd: string) => Effect.succeed(Option.none()),
          getThreadShellsByIds: (_ids: ReadonlyArray<ThreadId>) => Effect.succeed(new Map()),
          getThreadTranscriptById: (_id: ThreadId) => Effect.succeed(Option.none()),
          getExistingThreadActivityIds: () => Effect.succeed([]),
        } as any);

        const mockDirectory = ProviderSessionDirectory.of({
          getBinding: (threadId: ThreadId) =>
            Effect.succeed(
              bindings.has(threadId) ? Option.some(bindings.get(threadId)) : Option.none(),
            ),
          insertIfAbsent: (binding: any) =>
            Effect.sync(() => {
              bindings.set(binding.threadId, binding);
              return Option.some(binding);
            }),
          mergeRuntimePayload: () => Effect.void,
        } as any);

        const mockConfig = Layer.succeed(ServerConfig, {
          worktreesDir: path.join(tempDir, "worktrees"),
          baseDir: tempDir,
        } as any);

        const importer = yield* makeAntigravitySessionImporter({ customDataDir: agyDataDir }).pipe(
          Effect.provide(
            Layer.mergeAll(
              Layer.succeed(OrchestrationEngineService, mockEngine),
              Layer.succeed(ProjectionSnapshotQuery, mockSnapshots),
              Layer.succeed(ProviderSessionDirectory, mockDirectory),
              mockConfig,
            ),
          ),
        );

        // Run scan
        yield* importer.scan("full");

        // Verify project, thread, messages, and turn reconcile were dispatched
        const commandTypes = dispatchedCommands.map((c) => c.type);
        expect(commandTypes).toContain("project.create");
        expect(commandTypes).toContain("thread.create");
        expect(commandTypes).toContain("thread.messages.import");
        expect(commandTypes).toContain("thread.turn.reconcile");

        const threadCreate = dispatchedCommands.find((c) => c.type === "thread.create");
        expect(threadCreate).toMatchObject({
          threadId: "antigravity-conv-uuid-1",
          title: "My Imported Session",
        });

        const messagesImport = dispatchedCommands.find((c) => c.type === "thread.messages.import");
        expect(messagesImport.messages).toHaveLength(2);
        expect(messagesImport.messages[0].text).toBe("Hello from AGY");
        expect(messagesImport.messages[1].text).toBe("Hello! How can I help?");

        // Verify binding in ProviderSessionDirectory
        const binding = bindings.get("antigravity-conv-uuid-1");
        expect(binding).toBeDefined();
        expect(binding).toMatchObject({
          provider: ProviderDriverKind.make("antigravity"),
          resumeCursor: {
            schemaVersion: 1,
            sessionId: "conv-uuid-1",
          },
          runtimePayload: {
            importedFrom: "antigravity",
            importedTitle: "My Imported Session",
          },
        });
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );
  });
});
