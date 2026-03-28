#!/bin/bash
# run_bot.sh — Arranca el bot con OpenCode backend
# El bot gestiona el opencode server internamente (watchdog)
# Uso: ./run_bot.sh

set -e

BOT_DIR="/home/valle/proyectos/opencode-bot"
PYTHON="$BOT_DIR/.venv/bin/python"

cd "$BOT_DIR"
source .venv/bin/activate

# Verificar que opencode esté instalado
if ! command -v opencode &>/dev/null; then
    echo "⚠️  ADVERTENCIA: 'opencode' no encontrado en PATH"
    echo "   Instala con: npm install -g opencode-ai"
    echo "   O asegúrate de que esté en PATH"
    echo ""
fi

echo "🚀 Arrancando OpenCode Bot (OpenCode backend en puerto 13001)..."
exec "$PYTHON" src/telegram_bot.py
