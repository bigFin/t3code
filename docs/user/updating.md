# Updating T3 Code

The T3 Code web or desktop app and the server it connects to work best when they use the same
version. If they do not match, T3 Code identifies which side is older and shows the appropriate
update guidance.
The app you use and the server running your agents can be on different machines.
When a server is behind your web or desktop app, an update notice appears in the
conversation and **Settings → Connections**. Update the machine named in that
notice.

## Before you update

Server updates restart the connection and can interrupt active agents and
terminal commands. Saved threads, settings, and project files remain.

**Settings → General → Continue threads after restarts** is off by default.
Enable it to resume supported active threads after an update, crash, or machine
restart. Changes are saved to connected environments that support this setting;
update older servers first. If a supported environment was offline or has a
different value, use **Apply to all** in Settings after it connects.
T3 Code must start again on that machine;
the setting does not enable automatic startup. Terminal commands may still be
interrupted, and threads without saved provider resume state need a new message.
If you previously enabled continuation for updates, enable this setting once
to allow recovery without a connected client.

## Update a connected server

The offered action depends on how the server runs:

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

| Action                     | What to do                                                                                                                                                                                      |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Update server**          | Keep the client open while it installs and reconnects. Supported background services update remotely. For a desktop-hosted server, this also closes and relaunches the desktop app on the host. |
| **Update the desktop app** | Update the desktop app on the machine running the server, then reopen it if needed.                                                                                                             |
| **Copy update command**    | Stop the command-line server on its host and relaunch with the copied command, keeping your usual startup options.                                                                              |

Server update actions only appear when the server is older than the client. T3 Code does not update connected
servers silently in the background.

After selecting **Update**, the notice becomes a live status line: **Downloading…** while the new
version is fetched and verified, then **Restarting…** while the server restarts into it. The same
status appears in the conversation and in Connections, so navigating between them does not lose the
update. A failure remains visible with its error and an option to retry.

On the host, run:

```sh
t3 update <client-version>
```

Replace `<client-version>` with the version shown in the notice. The command
asks before restarting the background service; if you decline, run
`t3 service restart` when you are ready. For a server you started by hand,
stop it and start it again afterwards with your usual options such as `--host`
or `--tailscale-serve`.

If you run the server with `npx` rather than an installed `t3`, there is
nothing to update on the host: stop the server and relaunch it as
`npx t3@<client-version>` with the same subcommand and options.

## If an update fails

Keep the client open until it reconnects or reports a failure. A failed service
update can roll back to the previous version. If the update still fails:

1. Retry the offered action once.
2. Check that you updated the server's machine, not only the device you are using.
3. For a command-line server, stop it and relaunch the exact version shown in the notice.

## Mobile updates

Install App Store or Google Play releases as usual. The mobile app can also
download updates in the background and apply them when you next leave the app.
It saves drafts and queued messages before restarting. If you keep the app open
for a long time, it may ask to install immediately; choosing **Later** leaves the
update queued for the next suitable moment.
