import * as Effect from "effect/Effect";
import type * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import type * as Path from "effect/Path";

interface OpenSessionFileOptions {
  readonly procRoot?: string;
  readonly currentProcessId?: string;
}
type PiCompatibleProcessDriver = "omp" | "piAgent";

export interface ActivePiSessionFiles {
  readonly omp: ReadonlySet<string>;
  readonly piAgent: ReadonlySet<string>;
}

interface ActivePiSessionFileOptions extends OpenSessionFileOptions {
  readonly terminalSessionsRoots?: Partial<
    Record<PiCompatibleProcessDriver, ReadonlyArray<string>>
  >;
}

function processDriver(cmdline: string, path: Path.Path): PiCompatibleProcessDriver | undefined {
  const arguments_ = cmdline.split("\0").filter((argument) => argument.length > 0);
  if (
    arguments_.some(
      (argument) =>
        argument.includes("@oh-my-pi/pi-coding-agent") || path.basename(argument) === "omp",
    )
  ) {
    return "omp";
  }
  if (
    arguments_.some(
      (argument) =>
        argument.includes("@mariozechner/pi-coding-agent") || path.basename(argument) === "pi",
    )
  ) {
    return "piAgent";
  }
  return undefined;
}

function terminalBreadcrumbName(terminal: string, path: Path.Path): string | undefined {
  const devicePrefix = `/dev${path.sep}`;
  if (!terminal.startsWith(devicePrefix)) return undefined;
  const name = terminal.slice(devicePrefix.length).split(path.sep).join("-");
  return /^[A-Za-z0-9._-]+$/u.test(name) ? name : undefined;
}

/**
 * Finds transcripts owned by live interactive Pi/OMP processes.
 *
 * These CLIs close their JSONL between writes, so open descriptors alone do
 * not prove ownership. Their per-terminal breadcrumbs provide the exact
 * session even when concurrent processes share a working directory.
 */
export const listActivePiSessionFiles = Effect.fn("PiSessionFiles.listActivePiSessionFiles")(
  function* (
    fileSystem: FileSystem.FileSystem,
    path: Path.Path,
    options?: ActivePiSessionFileOptions,
  ): Effect.fn.Return<ActivePiSessionFiles> {
    const procRoot = options?.procRoot ?? "/proc";
    const currentProcessId = options?.currentProcessId ?? String(process.pid);
    const home = process.env.HOME ?? "";
    const configuredRoots = options?.terminalSessionsRoots ?? {};
    const terminalSessionsRoots = {
      omp: [...(configuredRoots.omp ?? []), path.join(home, ".omp", "agent", "terminal-sessions")],
      piAgent: [
        ...(configuredRoots.piAgent ?? []),
        path.join(home, ".pi", "agent", "terminal-sessions"),
      ],
    };
    const active = {
      omp: new Set<string>(),
      piAgent: new Set<string>(),
    };
    const processes = yield* fileSystem
      .readDirectory(procRoot)
      .pipe(Effect.orElseSucceed(() => []));

    for (const processId of processes) {
      if (!/^\d+$/u.test(processId) || processId === currentProcessId) continue;
      const processRoot = path.join(procRoot, processId);
      const cmdline = yield* fileSystem
        .readFileString(path.join(processRoot, "cmdline"))
        .pipe(Effect.option);
      if (Option.isNone(cmdline)) continue;
      const detectedDriver = processDriver(cmdline.value, path);
      // Wrappers such as kitu can hide the underlying CLI from /proc/cmdline.
      // A validated terminal breadcrumb still identifies the live session.
      const candidateDrivers =
        detectedDriver === undefined ? (["omp", "piAgent"] as const) : [detectedDriver];

      const terminal = yield* fileSystem
        .readLink(path.join(processRoot, "fd", "0"))
        .pipe(Effect.option);
      const cwd = yield* fileSystem.readLink(path.join(processRoot, "cwd")).pipe(Effect.option);
      const breadcrumbName = Option.isSome(terminal)
        ? terminalBreadcrumbName(terminal.value, path)
        : undefined;

      for (const driver of candidateDrivers) {
        if (breadcrumbName !== undefined && Option.isSome(cwd)) {
          for (const root of terminalSessionsRoots[driver]) {
            const breadcrumb = yield* fileSystem
              .readFileString(path.join(root, breadcrumbName))
              .pipe(Effect.option);
            if (Option.isNone(breadcrumb)) continue;
            const [breadcrumbCwd, sessionFile] = breadcrumb.value.split(/\r?\n/u);
            if (
              breadcrumbCwd === undefined ||
              sessionFile === undefined ||
              path.resolve(breadcrumbCwd) !== path.resolve(cwd.value)
            ) {
              continue;
            }
            active[driver].add(path.resolve(sessionFile));
            break;
          }
        }
        if (detectedDriver !== driver) continue;

        const descriptors = yield* fileSystem
          .readDirectory(path.join(processRoot, "fd"))
          .pipe(Effect.orElseSucceed(() => []));
        for (const descriptor of descriptors) {
          const target = yield* fileSystem
            .readLink(path.join(processRoot, "fd", descriptor))
            .pipe(Effect.option);
          if (Option.isSome(target) && target.value.endsWith(".jsonl")) {
            active[driver].add(path.resolve(target.value));
          }
        }
      }
    }

    return active;
  },
);

/** Snapshots regular files held open by other processes using Linux procfs. */
export const listOpenProcessFiles = Effect.fn("PiSessionFiles.listOpenProcessFiles")(function* (
  fileSystem: FileSystem.FileSystem,
  path: Path.Path,
  options?: OpenSessionFileOptions,
): Effect.fn.Return<ReadonlySet<string>> {
  const procRoot = options?.procRoot ?? "/proc";
  const currentProcessId = options?.currentProcessId ?? String(process.pid);
  const openFiles = new Set<string>();
  const processes = yield* fileSystem.readDirectory(procRoot).pipe(Effect.orElseSucceed(() => []));

  for (const processId of processes) {
    if (!/^\d+$/u.test(processId) || processId === currentProcessId) continue;
    const descriptors = yield* fileSystem
      .readDirectory(path.join(procRoot, processId, "fd"))
      .pipe(Effect.orElseSucceed(() => []));
    for (const descriptor of descriptors) {
      const target = yield* fileSystem
        .readLink(path.join(procRoot, processId, "fd", descriptor))
        .pipe(Effect.option);
      if (Option.isSome(target)) openFiles.add(path.resolve(target.value));
    }
  }

  return openFiles;
});

export const isPiSessionFileOpen = Effect.fn("PiSessionFiles.isPiSessionFileOpen")(function* (
  fileSystem: FileSystem.FileSystem,
  path: Path.Path,
  sessionFile: string,
  options?: OpenSessionFileOptions,
): Effect.fn.Return<boolean> {
  const openFiles = yield* listOpenProcessFiles(fileSystem, path, options);
  return openFiles.has(path.resolve(sessionFile));
});
