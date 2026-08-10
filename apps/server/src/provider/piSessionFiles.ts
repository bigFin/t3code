import * as Effect from "effect/Effect";
import type * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import type * as Path from "effect/Path";

interface OpenSessionFileOptions {
  readonly procRoot?: string;
  readonly currentProcessId?: string;
}

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
