import { formatNativeSessionCliCommand } from "@t3tools/shared/nativeSessionCli";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { useAtomValue } from "@effect/atom-react";
import { useCallback } from "react";

import { copyTextWithHaptic } from "../../lib/copyTextWithHaptic";
import { environmentServerConfigsAtom } from "../../state/server";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";

export function useNativeSessionActions() {
  const serverConfigs = useAtomValue(environmentServerConfigsAtom);
  const stopThreadSession = useAtomCommand(threadEnvironment.stopSession, { reportFailure: false });

  const releaseThreadToCli = useCallback(
    async (thread: EnvironmentThreadShell) => {
      const nativeSession = thread.session?.nativeSession;
      if (!nativeSession?.cli) return;
      if (nativeSession.ownership === "t3") {
        const released = await stopThreadSession({
          environmentId: thread.environmentId,
          input: { threadId: thread.id, releaseToCli: true },
        });
        if (released._tag === "Failure") return;
      }
      const os = serverConfigs.get(thread.environmentId)?.environment.platform.os;
      const platform = os === "windows" ? "win32" : os === "darwin" ? "darwin" : "linux";
      copyTextWithHaptic(formatNativeSessionCliCommand(nativeSession.cli, platform), {
        target: "CLI resume command",
      });
    },
    [serverConfigs, stopThreadSession],
  );

  const copyNativeSessionId = useCallback((thread: EnvironmentThreadShell) => {
    const nativeId = thread.session?.nativeSession?.id;
    if (nativeId) copyTextWithHaptic(nativeId, { target: "native session ID" });
  }, []);

  return { releaseThreadToCli, copyNativeSessionId } as const;
}
