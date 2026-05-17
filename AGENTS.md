# opencode-bot

Telegram bot for managing OpenCode sessions and projects.

## System Configuration

- **sudo**: No password required
- **Service**: systemd service named `opencode-bot.service`
- **Restart**: `sudo systemctl restart opencode-bot.service`

## Architecture

### Components

1. **telegram_bot.py** - Main bot with Telegram interface
2. **bot_server.py** - HTTP server (port 13002) for API endpoints
3. **project_manager.py** - Multi-project/session management with reply tracking
4. **opencode_server.py** - OpenCode server management (port 4096)
5. **opencode_client.py** - HTTP + SSE client for OpenCode API

### Ports

| Service | Port | Default |
|---------|------|---------|
| Bot Server | BOT_PORT | 13002 |
| OpenCode | OPENCODE_PORT | 4096 |

## Commands

| Command | Description |
|---------|-------------|
| `/start` | Status and main menu |
| `/open` | File explorer to open projects |
| `/projects` | List opened projects |
| `/close` | Close active project |
| `/sessions` | Manage sessions |
| `/models` | Change model |
| `/status` | Task progress |
| `/esc` | Cancel task |
| `/restart` | Restart bot |

## Features

### Multi-Project Support
- Multiple projects with different workspaces
- Each project has its own model and sessions
- Switch between projects via `/projects`

### Reply Tracking
- Reply to bot messages to continue conversation
- Automatically detects project/session from reply

### Status Feedback
- Shows project name, session title, context %
- Tools used, files modified
- Real-time via SSE events

## Environment Variables

```env
TELEGRAM_BOT_TOKEN=<token>
TELEGRAM_ADMIN_ID=<id>
BOT_PORT=13002
OPENCODE_PORT=4096
DEFAULT_WORKSPACE=~/.proyectos
```
