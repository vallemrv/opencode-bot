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

- **`telegram_bot.py`** (~3500 líneas) — Único entrypoint. `main()` registra todos los handlers y arranca `app.run_polling()`. Contiene todos los comandos, callbacks inline, el procesamiento de eventos SSE y el renderizado de estado en vivo.
- **`opencode_client.py`** — Cliente async de la API de OpenCode. Una `aiohttp.ClientSession` reutilizada para HTTP; el SSE usa su propia sesión efímera con reconexión y backoff exponencial.
- **`db.py`** — SQLite mínimo (`bot.db`). **Solo** una tabla `active_session` (singleton, `id=1`) con `session_id` + `directory`. Nada más se persiste.
- **`transcription.py`** — STT de notas de voz vía API de X.AI (Grok). Devuelve `None` si `XAI_API_KEY` no está configurada.
- **`md2tgv2.py`** — Conversor Markdown → Telegram MarkdownV2. `convert()` para texto completo, `_escape()` para escapar literales que se interpolan en plantillas MarkdownV2.

### Conceptos clave para no romper nada

**Scoping por `directory`.** Casi toda llamada a la API de OpenCode lleva un query param `?directory=<path>`. Un "proyecto" en este bot ES un directorio. Al añadir métodos al cliente, propaga `directory` igual que los existentes.

**`list_sessions()` lee el SQLite de OpenCode directamente.** La API de OpenCode solo matchea directorios exactos, pero las sesiones pueden vivir en subdirectorios de un worktree. Por eso `_get_session_directories_from_db()` lee `~/.local/share/opencode/opencode.db` para descubrir todos los directorios con sesiones y luego consulta la API por cada uno. Si esto falla hay un fallback a una llamada `/session` pelada.

**SSE global, una sola conexión.** `event_stream()` consume `/global/event` (eventos de TODOS los proyectos/sesiones). `sse_listener()` en `telegram_bot.py` enruta cada evento al proyecto correcto usando `properties.sessionID` + `directory`. El listener arranca vía `job_queue.run_once` tras el polling y se cancela en `post_shutdown`.

**Prompts son fire-and-forget.** `send_message_async()` POSTea a `/prompt_async` y devuelve 204 inmediatamente. La respuesta del LLM llega *exclusivamente* por SSE, que actualiza en vivo un "status message" en Telegram y al terminar (`session.idle`) lo reemplaza por la respuesta final. No esperes respuesta síncrona de un prompt.

**Cola de prompts.** Si el usuario envía un mensaje mientras la sesión está ocupada, el prompt se encola en `bot_data["queues"][session_id]` (un `deque`). Cuando llega `session.idle`, `_finish_status()` llama a `_drain_queue()`, que extrae el siguiente item y lo envía automáticamente. El `pending_model` se consume en ese momento (no persiste en la cola).

**`BOT_DIR` excluido de listados.** El directorio raíz del bot (`Path(__file__).parent.parent`) está en `BOT_DIR` y se filtra explícitamente en `/sessions` y `/close` para que no aparezca como proyecto OpenCode.

**Dos directorios temporales distintos:**
- `TMP_SESSION_DIR = ~/.local/share/opencode-bot/tmp` — workspace que crea el comando `/tmp`; es un directorio de proyecto real en OpenCode. **Ruta persistente a propósito**: cuando vivía en `/tmp` (se vacía en cada reboot), OpenCode podía bootstrapear su file picker para una carpeta inexistente y dejar la instancia de ese directorio rota en memoria (`"Invalid path"`), abortando todos los prompts a ~3ms. No volver a moverlo a `/tmp`.
- `TMP_DIR = Path("/tmp/opencode-bot-media")` — solo para descargas temporales de audio antes de moverlas al cwd del proyecto.

### Estado en memoria (`bot_data`)

Casi todo el estado de runtime vive en `application.bot_data` (no en DB). Los más importantes:

- `statuses[session_id]` — Todo el estado del status message en vivo (msg_id, tools vistas, ficheros editados, tokens, texto acumulado, etc.). Ver AGENTS.md para el shape completo.
- `msg_to_session` — `OrderedDict` (límite 200) que mapea message_id del bot → `{session_id, directory}`. Es lo que permite que **responder (reply) a un mensaje del bot** enrute el siguiente prompt a esa sesión concreta. Registrado por `_track_msg()`; resuelto por `_resolve_target()` (prioridad: reply > sesión activa).
- `ks` — "Key store": comprime strings largos en claves int para meterlos en `callback_data` (Telegram limita a 64 bytes). `_key()`/`_val()`. Imprescindible para callbacks que llevan paths o IDs largos.
- `queues[session_id]` — `deque` de prompts pendientes para una sesión ocupada. Se drena automáticamente al llegar `session.idle`.
- `session_variant[session_id]` — Variante de esfuerzo de razonamiento elegida con `/effort` (p.ej. `"low"`, `"medium"`, `"high"`, `"max"`). Se aplica a cada prompt de esa sesión hasta que se cambie. Se limpia al activar o crear una nueva sesión.
- `pending_model[session_id]` — Override de modelo `{providerID, modelID}` elegido con `/models`; se consume (pop) en el siguiente prompt enviado.
- `models_cache` (TTL 300s), `pending_perms`, `pending_questions`, `send_target`, `child_to_parent`.

### Handlers de mensajes no-comando

Además de los comandos, el bot registra tres handlers de mensajes:

- **Texto libre / voz** (`handle_message`) — Envía prompt a la sesión resuelta por `_resolve_target()`. Si la sesión está ocupada, encola en `queues`.
- **Archivos (documentos, fotos, vídeos)** (`handle_file_upload`) — Descarga el archivo directamente al `directory` de la sesión activa. Requiere sesión activa; no envía prompt.
- **Audio / notas de voz** (`handle_audio_upload`) — Descarga a `TMP_DIR`, transcribe con X.AI STT, mueve el archivo al cwd si hay sesión activa, y envía la transcripción como prompt. Si la transcripción falla o no hay `XAI_API_KEY`, avisa al usuario.

### Convenciones de Telegram UI

- **Todos los handlers están protegidos por `@admin_only`** (compara contra `TELEGRAM_ADMIN_ID`). Bot de un solo operador.
- Callbacks se registran con `CallbackQueryHandler(fn, pattern=r"^prefijo:")`. Cada prefijo (`ob:`, `os:`, `perm:`, `qans:`, etc.) tiene su `cb_*`. La tabla completa prefijo→función está en AGENTS.md.
- **Dos parse modes conviven.** Mucho texto usa `parse_mode="Markdown"` (simple). El render de respuestas/preguntas usa `MarkdownV2` y DEBE pasar por `md2tgv2.convert()` o `md2tgv2._escape()` — olvidarlo rompe el envío con errores de parseo de Telegram.
- Respuestas largas se trocean en chunks; si superan el límite se mandan como ficheros `.md`.

## Documentación adicional

`AGENTS.md` contiene la referencia exhaustiva: shape completo de `statuses`, tabla de eventos SSE procesados, tabla de todos los callbacks, y diagramas de flujo de `/open`, `/close` y `/send`. Consúltalo antes de tocar esos flujos.
