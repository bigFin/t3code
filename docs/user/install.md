# Install T3 Code

T3 Code runs coding agents on your computer and lets you control them from its
desktop, web, or mobile app. Set up the machine where the agents will work first.

## Requirements

Command-line use, SSH hosts, and WSL backends need Node.js 22.16+ (22.x), 23.11+
(23.x), or 24.10 and later. The native desktop app includes its server runtime.

You need an installed, authenticated provider before starting a thread. You can
launch T3 Code and configure providers afterwards.

## Run without installing

```bash
npx t3@latest
```

This starts the server and opens the local web app. Run
`npx t3@latest --help` for command-line options.

## Desktop app

Download a release from [GitHub Releases](https://github.com/pingdotgg/t3code/releases),
or use a package manager:

| Platform           | Install                         |
| ------------------ | ------------------------------- |
| Windows            | `winget install T3Tools.T3Code` |
| macOS              | `brew install --cask t3-code`   |
| Arch Linux         | `yay -S t3code-bin`             |
| Arch Linux nightly | `yay -S t3code-nightly-bin`     |

### Windows Subsystem for Linux

Choose a WSL distro in **Settings → Connections** to run agents and projects
there. Install Node.js and provider CLIs inside that distro. T3 Code installs its
matching server runtime there automatically; the first launch after an app
update can take longer.

### Open a project from a terminal

With the desktop app already running on the same machine:

```bash
npx t3 app
```

This opens a new thread for the current directory, adding the project if needed.
Pass a path, such as `npx t3 app ../my-project`, to open another directory. It requires
the desktop app, so a standalone server or an SSH session is not enough. If the
command cannot reach the app, start or update the desktop app and try again.

## Mobile app

Install T3 Code from the
[App Store](https://apps.apple.com/us/app/t3-code-remote-claude-more/id6787819824) or
[Google Play](https://play.google.com/store/apps/details?id=com.t3tools.t3code).
The phone connects to a server on another machine. Follow
[remote access](./remote-access.md) to link it through T3 Connect or a pairing URL.

## Providers

Open **Settings → Providers** in the web or desktop app, select the environment,
and enable the provider you want. Installation, login, and configuration belong
to that environment's machine, even when you connect from a phone or another
computer.

| Provider   | CLI                                                   | Default binary | Log in with             |
| ---------- | ----------------------------------------------------- | -------------- | ----------------------- |
| Codex      | [Codex CLI](https://developers.openai.com/codex/cli)  | `codex`        | `codex login`           |
| Claude     | [Claude Code](https://claude.com/product/claude-code) | `claude`       | `claude auth login`     |
| Cursor     | [Cursor CLI](https://cursor.com/cli)                  | `cursor-agent` | `agent login`           |
| Grok Build | [Grok Build CLI](https://x.ai/cli)                    | `grok`         | `grok login`            |
| Oh My Pi   | [Oh My Pi](https://omp.sh)                            | `omp`          | `omp auth-broker login` |
| OpenCode   | [OpenCode](https://opencode.ai)                       | `opencode`     | `opencode auth login`   |
| Pi         | [Pi](https://pi.dev)                                  | `pi`           | Run `pi`, then `/login` |
| Antigravity | Managed in T3 Code                                | —              | Sign in with Google      |

Provider CLIs must be on the server's `PATH`. If T3 Code cannot find one, set its
**Binary path** in provider settings, especially when using a version manager.
Cursor's executable is `cursor-agent`, although its login command is
`agent login`. Antigravity can use its managed runtime without a `PATH` entry.

When a provider CLI is behind its latest release, its provider card shows the
available version. **Update now** appears only when T3 Code can tell which
installer owns the CLI (its own update command, Homebrew, or a global npm, pnpm,
bun, or Vite+ install) and runs that installer. Otherwise update the CLI the same
way you installed it. Homebrew installs compare against the version Homebrew
offers, which can trail the npm release by a few hours.

Add another provider instance for a separate account or configuration. Each
instance can have its own environment variables, such as API keys or a custom
base URL. Mark secret values as sensitive; after saving, T3 Code does not display
their original values.

For provider-specific setup and accounts, see [Codex](./providers-codex.md),
[Claude](./providers-claude.md), [OpenCode](./providers-opencode.md), and
[Antigravity](./providers-antigravity.md).

Codex, Oh My Pi, OpenCode, and Pi report their model catalogs to T3 Code at runtime. Models added
through a Codex catalog, an Oh My Pi or OpenCode provider, or Pi's model configuration appear in
the model picker without being copied into T3 Code settings.

Pi and Oh My Pi sessions are process-bound. T3 Code stores their session file and can resume the
conversation after a server restart, but an active turn does not continue while the T3 Code server
is offline.

T3 Code also discovers existing Pi and Oh My Pi transcripts from their configured session
directory (by default `~/.pi/agent/sessions` and `~/.omp/agent/sessions`). Imported conversations
remain stopped until you resume them, so T3 Code never competes with an active CLI session.

## Next steps

- [Working with threads](./thread-sidebar.md): start tasks and organize parallel work.
- [Permission modes](./permission-modes.md): choose when agents ask before acting.
- [Remote access](./remote-access.md): connect from another device.
- [Running in the background](./background-service.md): keep a Linux or macOS host available.
- [Updating T3 Code](./updating.md): update the app and connected servers.
