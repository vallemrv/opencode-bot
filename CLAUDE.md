# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Qué es

Bot de Telegram (operador único) que controla remotamente un **servidor OpenCode** vía su API HTTP + SSE. El bot no gestiona el proceso de OpenCode: asume que `opencode serve --port 4096` ya está corriendo y solo se conecta a él. Toda la verdad sobre proyectos y sesiones vive en OpenCode; el bot solo persiste cuál es la sesión activa.

## Comandos de desarrollo

```bash
# Ejecutar localmente (crea venv e instala deps la primera vez)
./run_bot.sh
# o manualmente:
python3 src/telegram_bot.py        # requiere .venv activado y .env presente

# Lint (única herramienta de dev declarada)
ruff check src/

# Producción: corre como systemd service
sudo systemctl restart opencode-bot.service
journalctl -u opencode-bot.service -f
```

No hay tests. Cambios se commitean directo a `main` y se pushean (rama única, ver AGENTS.md). El comando `/restart` del bot hace `git pull` + `systemctl restart` sobre la copia desplegada, así que para que un cambio llegue a producción debe estar pusheado.

`.env` requerido (ver `.env.example`): `TELEGRAM_BOT_TOKEN`, `TELEGRAM_ADMIN_ID`, `OPENCODE_HOST`, `OPENCODE_PORT`, `DEFAULT_WORKSPACE`, `XAI_API_KEY` (opcional, transcripción).

## Arquitectura

Cinco módulos en `src/`:

- **`telegram_bot.py`** (~3200 líneas) — Único entrypoint. `main()` registra todos los handlers y arranca `app.run_polling()`. Contiene todos los comandos, callbacks inline, el procesamiento de eventos SSE y el renderizado de estado en vivo.
- **`opencode_client.py`** — Cliente async de la API de OpenCode. Una `aiohttp.ClientSession` reutilizada para HTTP; el SSE usa su propia sesión efímera con reconexión y backoff exponencial.
- **`db.py`** — SQLite mínimo (`bot.db`). **Solo** una tabla `active_session` (singleton, `id=1`) con `session_id` + `directory`. Nada más se persiste.
- **`transcription.py`** — STT de notas de voz vía API de X.AI (Grok). Devuelve `None` si `XAI_API_KEY` no está configurada.
- **`md2tgv2.py`** — Conversor Markdown → Telegram MarkdownV2. `convert()` para texto completo, `_escape()` para escapar literales que se interpolan en plantillas MarkdownV2.

### Conceptos clave para no romper nada

**Scoping por `directory`.** Casi toda llamada a la API de OpenCode lleva un query param `?directory=<path>`. Un "proyecto" en este bot ES un directorio. Al añadir métodos al cliente, propaga `directory` igual que los existentes.

**`list_sessions()` lee el SQLite de OpenCode directamente.** La API de OpenCode solo matchea directorios exactos, pero las sesiones pueden vivir en subdirectorios de un worktree. Por eso `_get_session_directories_from_db()` lee `~/.local/share/opencode/opencode.db` para descubrir todos los directorios con sesiones y luego consulta la API por cada uno. Si esto falla hay un fallback a una llamada `/session` pelada.

**SSE global, una sola conexión.** `event_stream()` consume `/global/event` (eventos de TODOS los proyectos/sesiones). `sse_listener()` en `telegram_bot.py` enruta cada evento al proyecto correcto usando `properties.sessionID` + `directory`. El listener arranca vía `job_queue.run_once` tras el polling y se cancela en `post_shutdown`.

**Prompts son fire-and-forget.** `send_message_async()` POSTea a `/prompt_async` y devuelve 204 inmediatamente. La respuesta del LLM llega *exclusivamente* por SSE, que actualiza en vivo un "status message" en Telegram y al terminar (`session.idle`) lo reemplaza por la respuesta final. No esperes respuesta síncrona de un prompt.

### Estado en memoria (`bot_data`)

Casi todo el estado de runtime vive en `application.bot_data` (no en DB). Los más importantes:

- `statuses[session_id]` — Todo el estado del status message en vivo (msg_id, tools vistas, ficheros editados, tokens, texto acumulado, etc.). Ver AGENTS.md para el shape completo.
- `msg_to_session` — `OrderedDict` (límite 200) que mapea message_id del bot → `{session_id, directory}`. Es lo que permite que **responder (reply) a un mensaje del bot** enrute el siguiente prompt a esa sesión concreta. Registrado por `_track_msg()`; resuelto por `_resolve_target()` (prioridad: reply > sesión activa).
- `ks` — "Key store": comprime strings largos en claves int para meterlos en `callback_data` (Telegram limita a 64 bytes). `_key()`/`_val()`. Imprescindible para callbacks que llevan paths o IDs largos.
- `models_cache` (TTL 300s), `queues`, `pending_model`, `pending_perms`, `pending_questions`, `send_target`, `child_to_parent`.

### Convenciones de Telegram UI

- **Todos los handlers están protegidos por `@admin_only`** (compara contra `TELEGRAM_ADMIN_ID`). Bot de un solo operador.
- Callbacks se registran con `CallbackQueryHandler(fn, pattern=r"^prefijo:")`. Cada prefijo (`ob:`, `os:`, `perm:`, `qans:`, etc.) tiene su `cb_*`. La tabla completa prefijo→función está en AGENTS.md.
- **Dos parse modes conviven.** Mucho texto usa `parse_mode="Markdown"` (simple). El render de respuestas/preguntas usa `MarkdownV2` y DEBE pasar por `md2tgv2.convert()` o `md2tgv2._escape()` — olvidarlo rompe el envío con errores de parseo de Telegram.
- Respuestas largas se trocean en chunks; si superan el límite se mandan como ficheros `.md`.

## Documentación adicional

`AGENTS.md` contiene la referencia exhaustiva: shape completo de `statuses`, tabla de eventos SSE procesados, tabla de todos los callbacks, y diagramas de flujo de `/open`, `/close` y `/send`. Consúltalo antes de tocar esos flujos.
