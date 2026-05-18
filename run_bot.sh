#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$SCRIPT_DIR/.venv"
PYTHON="$VENV/bin/python"

# Create venv if needed
if [ ! -f "$PYTHON" ]; then
    echo "Creating virtualenv..."
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install -q --upgrade pip
    "$VENV/bin/pip" install -q -r "$SCRIPT_DIR/requirements.txt"
fi

# Load .env if present
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
fi

OPENCODE_HOST="${OPENCODE_HOST:-localhost}"
OPENCODE_PORT="${OPENCODE_PORT:-4096}"
echo "Starting OpenCode Bot (OpenCode at ${OPENCODE_HOST}:${OPENCODE_PORT})"

exec "$PYTHON" "$SCRIPT_DIR/src/telegram_bot.py"
