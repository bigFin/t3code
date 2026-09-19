# Running T3 Code in the background

On Linux and macOS, T3 Code can run as a service for your user so you do not need
to keep a terminal open.

## Manage the service

Install the `t3` CLI first ([Install T3 Code](./install.md#command-line)), then
run these commands on the machine that will host T3 Code:

| Task                            | Command                |
| ------------------------------- | ---------------------- |
| Install and start               | `t3 service install`   |
| Inspect status and log location | `t3 service status`    |
| Move to a newer release         | `t3 update`            |
| Restart                         | `t3 service restart`   |
| Stop and remove from startup    | `t3 service uninstall` |

Uninstalling the service leaves your projects, threads, and settings intact.
Running `t3 service install` again repairs a service that `t3 service status`
reports as broken.

`t3 update` downloads the newest release on your channel and switches `t3`
and the service to it. Restarting interrupts running agent turns, terminals,
and remote clients, so it asks first; answer no and the service keeps running
the old version until you run `t3 service restart`. Pass `--yes` from a
script. A server you started by hand is left running; stop and start it again
to pick up the new version. Wait for any remote update already in progress
before updating; to match a remote client's version, follow
[Updating T3 Code](./updating.md).

Pass an exact version (`t3 update 0.0.42`) to pin one, `--channel nightly` to
switch trains, or `--allow-downgrade` to move backwards. `preview` is a
maintainers' test train: its builds can be broken and are never offered as
updates, so the installer and `t3 update` ask for confirmation before
installing one.

`t3 uninstall` removes the background service, the `t3` launcher, and the
downloaded versions after showing you the list and asking once. Your projects,
threads, and settings under `~/.t3/userdata` are kept. Pass `--yes` from a
script.

## Platform support

Linux needs systemd user services. Setup enables lingering so T3 Code starts at
boot and keeps running after logout. If this needs administrator permission,
setup prints a recovery command before changing the service.

macOS starts the service when you log in and stops it when you log out. Keep the
Mac logged in and awake for unattended remote access. Installing over SSH while
nobody is logged in at the Mac's screen can fail at the final start step; the
service is still installed and will start at the next login.

Windows background services are not supported.

T3 Connect can offer service installation during setup, but the two are managed
separately. Signing out of T3 Connect does not stop or uninstall the service.

## Troubleshooting

Start with `t3 service status` on the host. It prints the log path and, on Linux,
checks whether the installed service is running, enabled, and allowed to survive
logout.

If it stops when your SSH session closes, check for `linger-disabled`. An
administrator can enable lingering with:

```sh
sudo loginctl enable-linger "$(id -un)"
```

Over SSH, allow sudo to prompt:

```sh
ssh -t your-server 'sudo loginctl enable-linger "$(id -un)"'
```

Then retry service setup as your normal user. Run only the `loginctl` command
with sudo; running T3 Code as root creates a separate installation and Connect
identity. Without administrator access, run `t3 serve` in a terminal and keep
that session open.

| Status problem                          | Next step                                                                                                                      |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `linger-unavailable`                    | Run `loginctl show-user "$(id -un)" --property=Linger` and check that systemd-logind is available.                             |
| `user-manager-unavailable`              | Run `systemctl --user status` in a login session for the service user; check your distribution's systemd user-session support. |
| `service-disabled` or `service-stopped` | Read the log and `systemctl --user status t3code.service`, then use the repair command printed by T3 Code.                     |
| `restart-pending`                       | A newer version is installed but the service still runs the previous one. Run `t3 service restart`.                            |

On macOS, check **System Settings → General → Login Items** if the service no
longer starts at login. If agent work cannot access Desktop, Documents, or
Downloads, it may need Full Disk Access for the `t3` executable listed in
`ProgramArguments` in
`~/Library/LaunchAgents/com.t3tools.t3code.service.plist`.

```sh
npx t3@latest service uninstall
```

Updating restarts T3 Code briefly. Active Codex turns continue in their independent provider host,
but terminal commands and other providers can still be interrupted. Let active agent work and
terminal commands finish first. If a remote update is already in progress, wait for it to finish
before retrying a local update.

The service runs a small stable launcher. Exact T3 Code versions are installed separately, so a
failed remote candidate can return to the previous version without rewriting the service
definition. The launcher snapshots the database before a remote candidate starts, so database
updates roll back with the server version. An older launcher may require one local
`service update` before this is available.

## Platform Support

**Linux** uses a systemd user unit at `~/.config/systemd/user/t3code.service`. The service starts
when the machine boots and keeps running after you log out (lingering is enabled during install).

**macOS** uses a launch agent at `~/Library/LaunchAgents/com.t3tools.t3code.service.plist`. It
starts when you log in, not when the Mac boots, and it stops when you log out; macOS has no
equivalent of Linux lingering for user agents. For a Mac that should stay reachable unattended,
turn on automatic login (System Settings → Users & Groups; unavailable while FileVault is on) and
keep the Mac from sleeping.

A few more macOS notes:

- Installing over SSH needs someone logged in at the Mac's screen to start the agent right away.
  Without that, the install command reports an error at the final start step, but the agent is
  fully installed and starts at the next login.
- macOS may show privacy prompts for protected folders such as Desktop, Documents, or Downloads,
  attributed to a bare `node` process, or deny access without a prompt. If agent work fails to
  read those folders, grant Full Disk Access to the node binary listed in the launch agent's
  `ProgramArguments`.
- The agent appears under System Settings → General → Login Items. If it was switched off there,
  or disabled with `launchctl disable`, macOS will not start it at login until you switch it back
  on.

**Windows** is not supported yet.

## Using It with T3 Connect

T3 Connect may offer to install the service during setup so the host stays reachable in the
background. This is only an onboarding shortcut: the service and T3 Connect are managed separately.

Signing out of T3 Connect does not remove the service. Use `t3 service uninstall` when you no longer
want T3 Code to start in the background.
For failures after signing in to T3 Connect, see
[connection troubleshooting](./remote-access.md#t3-connect-troubleshooting).
