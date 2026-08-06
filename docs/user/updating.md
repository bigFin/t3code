# Keeping T3 Code in Sync

The T3 Code web or desktop app and the server it connects to work best when they use the same
version. If they do not match, T3 Code identifies which side is older and shows the appropriate
update guidance.

## Where to Find the Update

You may see the warning in either of these places:

- above the message box in the current conversation
- **Settings** → **Connections**, beside the affected connection

Dismissing the conversation warning only hides that reminder for those two versions. It does not
update the server, and the version difference remains visible in Connections.

## Before You Update

Updating restarts the server, so the connection disappears briefly. On Linux and macOS, active
Codex turns continue in an independent provider host and T3 reattaches after the restart. Terminal
commands and other providers can still be interrupted, so let those finish first.

The update does not remove saved threads, settings, or project files. Restarting or evicting the
machine itself still stops its running processes.

## Restart the Desktop App

Use **Settings** → **About** → **Restart T3 Code**, or run **Restart T3 Code** from the command
palette. This uses the desktop app's graceful shutdown and relaunch flow rather than requiring a
process kill or a hidden application menu.

The desktop app and its local backend disconnect briefly. Remote environments reconnect
automatically. Active Codex turns on Linux and macOS continue independently, but let local terminal
commands and work from other providers finish first.

## Choose the Action You See

If the client is older, update and relaunch T3 Code on the device showing the warning. T3 Code does
not downgrade a newer remote server to match an older client.

| Action                     | What to do                                                                                                                                                                  |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Update server**          | Available for the T3 Code Linux background service. Select the button and leave T3 Code open while it prepares, tests, restarts, and reconnects.                            |
| **Update the desktop app** | Open the T3 Code desktop app on the machine that runs the server and install the app update there. Reopen it if needed.                                                     |
| **Copy update command**    | Copy the command, open a terminal on the server machine, stop the current T3 Code server, and relaunch it with the copied command and any startup options you normally use. |

Server update actions only appear when the server is older than the client. The available action
depends on how that server was started. T3 Code does not update connected
servers silently in the background.

An older background-service launcher may ask you to run the exact
`npx t3@<version> service update` command on the server machine. That one local update installs the
rollback support needed for later remote updates, including versions that change the database.

After selecting **Update server**, the warning becomes a three-step progress rail:
**Download**, **Install**, and **Resume**. The same progress appears in the conversation and in
Connections, so navigating between them does not lose the update. A failed step remains visible
with its error and an option to retry.

**Copy update command** gives you `npx t3@<client-version>`, which relaunches the server directly
at the matching version. Add whatever startup options you normally use.

If the server instead runs as the T3 Code background service, update the service on the host and
pin the same version:

```sh
npx t3@<client-version> service update
```

`service update` installs the version of the CLI that invoked it, so `npx t3@latest service update`
only resolves the skew when your client happens to be on the latest release. The exact version from
the warning always works.

See [Running T3 Code in the Background](./background-service.md) for install, status, and removal
commands.

## After the Update

Keep the web or desktop app open while the server restarts. The update completes only after the
service launcher reports that exact update committed and the replacement server is ready to accept
commands. A rollback is reported immediately instead of waiting for a generic reconnect timeout.

If a step fails:

1. Retry the offered action once.
2. Make sure you updated the machine named in the warning, not only the device you are using.
3. For a command-line server, relaunch it with `npx t3@<client-version>`, replacing
   `<client-version>` with the client version shown in the warning.

For remote connection setup and access troubleshooting, see [Remote Access](./remote-access.md).
