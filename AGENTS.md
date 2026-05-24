# opencode-bot

Bot de Telegram para controlar el servidor OpenCode remotamente.

## Sistema

- **sudo**: Sin contraseña
- **Repo**: `git@github.com:vallemrv/opencode-bot.git` (privado)
- **Servicio**: `opencode-bot.service` (systemd)
- **Reiniciar**: `sudo systemctl restart opencode-bot.service`
- **Logs**: `journalctl -u opencode-bot.service -f`

## Estructura del proyecto

```
src/
  telegram_bot.py      — Bot principal (único punto de entrada)
  opencode_client.py   — Cliente HTTP + SSE para la API de OpenCode
  db.py                — SQLite: active_session
  transcription.py     — Transcripción de audio vía X.AI STT API
  md2tgv2.py           — Conversor Markdown → Telegram MarkdownV2
```

## Base de datos (SQLite)

### Tablas

- **active_session**: Sesión actualmente activa
  - `id INTEGER PRIMARY KEY CHECK (id = 1)`
  - `session_id TEXT NOT NULL`
  - `directory TEXT NOT NULL`

### Funciones principales (`db.py`)

- `get_active()` → `dict | None` — Devuelve `{session_id, directory}` o None
- `set_active(session_id, directory)` — Establece sesión activa
- `clear_active()` — Limpia sesión activa

## Variables de entorno (`.env`)

```env
TELEGRAM_BOT_TOKEN=<token>
TELEGRAM_ADMIN_ID=<id>
OPENCODE_HOST=localhost
OPENCODE_PORT=4096
DEFAULT_WORKSPACE=~/proyectos
XAI_API_KEY=<key>   # Opcional, para transcripción de audio
```

## OpenCode Server

El servidor OpenCode debe estar corriendo en `OPENCODE_HOST:OPENCODE_PORT`:

```bash
opencode serve --port 4096
```

El bot **no gestiona** el proceso de OpenCode, solo se conecta a él vía HTTP y SSE.

## Comandos del bot

| Comando | Descripción |
|---------|-------------|
| `/start` | Estado completo: sesión activa, proyecto, modelo, estado del server |
| `/open` | Browser de carpetas → elige proyecto → session picker o model picker → crea sesión |
| `/close` | Cierra proyecto: borra sesiones de OpenCode y limpia sesión activa |
| `/sessions` | Gestiona sesiones del proyecto activo |
| `/models` | Cambia el modelo de la sesión activa |
| `/projects` | Lista todos los proyectos con sesiones en OpenCode |
| `/send` | Envía prompt a un proyecto específico (sin cambiar sesión activa) |
| `/esc` | Cancela la tarea en curso (abort) |

Cualquier texto libre (o audio) envía un prompt a la sesión activa. Los replies a mensajes del bot se envían a la sesión que generó ese mensaje.

## Flujo principal

### /open

```
/open
  └─ Browser de carpetas (paginado)
       └─ "✅ Open here"
            ├─ Si el proyecto tiene sesiones → session picker:
            │    • Activar sesión existente
            │    • Borrar sesión
            │    • "➕ Nueva sesión" → model picker → crear sesión
            │
            └─ Si no tiene sesiones → model picker → crear sesión
```

### /close

```
/close
  └─ Lista proyectos con sesiones
       ├─ "Borrar sesiones en OpenCode" → elimina todas las sesiones del proyecto
       ├─ "Solo quitar sesión activa del bot" → solo limpia la sesión activa
       └─ "Cerrar todo del server" → elimina TODAS las sesiones de TODOS los proyectos
```

### /send

```
/send
  └─ Lista proyectos con sesiones
       └─ Elige proyecto
            └─ Elige sesión (o crear nueva)
                 └─ Escribe prompt → envía sin cambiar sesión activa
```

### Texto libre → prompt

```
Texto
  └─ Envía prompt a sesión activa (prompt_async → 204 OK)
       └─ Muestra mensaje de estado INMEDIATO
            └─ SSE actualiza el mensaje en tiempo real
            └─ Al finalizar (session.status idle):
                 └─ Elimina status message
                 └─ Muestra respuesta completa
```

## SSE Events que procesa el bot

| Evento | Acción |
|--------|--------|
| `session.status` | Actualiza status (idle/busy/retry), crea/elimina mensaje de estado |
| `session.idle` | Finaliza, muestra respuesta completa |
| `session.error` | Muestra error, limpia estado |
| `message.part.updated` | Actualiza texto, reasoning, herramientas |
| `message.part.delta` | Streaming incremental de texto |
| `message.updated` | Cuenta mensajes, tokens |
| `permission.updated` | Muestra diálogo de permiso en Telegram |
| `permission.replied` | Limpia diálogo de permiso |
| `question.asked` | Muestra preguntas inline del LLM |
| `question.replied` | Limpia preguntas inline |

## Estado en `bot_data["statuses"]`

```python
{
  session_id: {
    "msg_id": int,           # ID del mensaje de estado en Telegram
    "directory": str,        # Directorio del proyecto
    "model": str,            # Modelo actual
    "session_title": str,    # Título de la sesión
    "state": str,            # busy | thinking | idle | error
    "tool": str | None,      # herramienta actual
    "tools_seen": [str],     # todas las herramientas llamadas
    "files_edited": set(),   # ficheros modificados
    "message_count": int,    # mensajes del assistant
    "last_text": str | None, # último fragmento de texto
    "final_text": str | None, # texto final acumulado
    "reasoning_text": str | None, # texto de razonamiento
    "start_time": float,     # timestamp inicio
    "last_update_time": float, # última actualización del mensaje
    "tokens_input": int,     # tokens input
    "tokens_output": int,    # tokens output
  }
}
```

## Otros estados en `bot_data`

- `bot_data["ks"]` — Key store para callback_data largos
- `bot_data["models_cache"]` — Cache de modelos (TTL 5 min)
- `bot_data["msg_to_session"]` — Mapea message_id → {session_id, directory}
- `bot_data["queues"]` — Cola de prompts pendientes por sesión
- `bot_data["pending_model"]` — Modelo pendiente para próximo prompt
- `bot_data["pending_perms"]` — Permisos pendientes de respuesta
- `bot_data["pending_questions"]` — Preguntas del LLM pendientes
- `bot_data["send_target"]` — Destino para /send
- `bot_data["child_to_parent"]` — Mapeo de sesiones hijas a padres

## Callbacks registrados

| Pattern | Función | Descripción |
|---------|---------|-------------|
| `ob:` | `cb_ob` | Navegar carpetas |
| `mkdir:` | `cb_mkdir` | Crear nueva carpeta |
| `os:` | `cb_os` | Abrir carpeta → session picker o model picker |
| `prov:` | `cb_prov` | Selector de proveedores |
| `provmodel:` | `cb_provmodel` | Modelo elegido → crear sesión o actualizar modelo |
| `newsess:` | `cb_newsess` | Nueva sesión |
| `actsess:` | `cb_actsess` | Activar sesión |
| `delsess:` | `cb_delsess` | Borrar sesión |
| `delconfirm:` | `cb_delconfirm` | Confirmar borrado de sesión con hijos |
| `closedir:` | `cb_closedir` | Elegir qué hacer al cerrar proyecto |
| `closedel:` | `cb_closedel` | Borrar sesiones del proyecto en OpenCode |
| `closebot:` | `cb_closebot` | Solo limpiar sesión activa del bot |
| `closeall:` | `cb_closeall` | Cerrar todo del server |
| `sda:` | `cb_sda` | Set Default Active |
| `abort:` | `cb_abort` | Cancelar tarea |
| `cancel:` | `cb_cancel` | Cancelar operación UI |
| `perm:` | `cb_perm` | Responder permiso |
| `perminput:` | `cb_perminput` | Input personalizado para permiso |
| `permabort:` | `cb_permabort` | Cancelar tarea desde permiso |
| `qans:` | `cb_qans` | Responder pregunta del LLM |
| `qcustom:` | `cb_qcustom` | Respuesta personalizada a pregunta |
| `qreject:` | `cb_qreject` | Rechazar pregunta |
| `qsendnow:` | `cb_qsendnow` | Enviar respuesta inmediata |
| `sendpick:` | `cb_sendpick` | Elegir proyecto para /send |
| `sendsess:` | `cb_sendsess` | Elegir sesión para /send |
| `sendnewsess:` | `cb_sendnewsess` | Nueva sesión para /send |
| `sesspick:` | `cb_sesspick` | Elegir sesión en /sessions |
| `modpick:` | `cb_modpick` | Elegir sesión en /models |
| `modsess:` | `cb_modsess` | Sesión elegida en /models |
| `modprov:` | `cb_modprov` | Proveedor elegido en /models |
| `setmodel:` | `cb_setmodel` | Establecer modelo |