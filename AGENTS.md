# opencode-bot

Bot de Telegram para controlar el servidor OpenCode remotamente.

## Sistema

- **sudo**: Sin contraseña
- **Servicio**: `opencode-bot.service` (systemd)
- **Reiniciar**: `sudo systemctl restart opencode-bot.service`
- **Logs**: `journalctl -u opencode-bot.service -f`

## Estructura del proyecto

```
src/
  telegram_bot.py      — Bot principal (único punto de entrada)
  opencode_client.py   — Cliente HTTP + SSE para la API de OpenCode
  db.py                — SQLite: open_cwds, sessions, active_session
```

## Base de datos (SQLite)

### Tablas

- **open_cwds**: Lista de proyectos abiertos (cwds)
  - `cwd TEXT PRIMARY KEY`

- **sessions**: Sesiones de OpenCode (solo de cwds abiertos)
  - `session_id TEXT PRIMARY KEY`
  - `cwd TEXT NOT NULL` (FK → open_cwds)
  - `title TEXT`
  - `model TEXT`
  - `status TEXT` (idle/busy/error)
  - `created_at INTEGER`
  - `updated_at INTEGER`

- **active_session**: Sesión actualmente activa
  - `id INTEGER PRIMARY KEY CHECK (id = 1)`
  - `session_id TEXT NOT NULL`

### Funciones principales (`db.py`)

- `open_cwd(cwd)` — Marca un proyecto como abierto
- `close_cwd(cwd)` — Cierra proyecto y borra todas sus sesiones
- `is_cwd_open(cwd)` — Verifica si un proyecto está abierto
- `add_session(sid, cwd, title, model)` — Guarda sesión
- `get_sessions_by_cwd(cwd)` — Lista sesiones de un proyecto
- `sync_sessions_from_opencode(cwd, sessions)` — Sincroniza sesiones de OpenCode

## Variables de entorno (`.env`)

```env
TELEGRAM_BOT_TOKEN=<token>
TELEGRAM_ADMIN_ID=<id>
OPENCODE_HOST=127.0.0.1
OPENCODE_PORT=4096
DEFAULT_WORKSPACE=~/proyectos
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
| `/open`  | Browser de carpetas → elige proyecto → (si abierto: session picker) → (si nuevo: model picker) → crea sesión |
| `/close` | Cierra proyecto: elimina de open_cwds y todas sus sesiones (BD + OpenCode) |
| `/sessions` | Gestiona sesiones del proyecto activo |
| `/models` | Cambia el modelo de la sesión activa |
| `/esc` | Cancela la tarea en curso (abort) |

Cualquier texto libre envía un prompt a la sesión activa.

## Flujo principal

### /open (Nuevo proyecto)

```
/open
  └─ Browser de carpetas (paginado)
       └─ "✅ Open here" (cwd no está en open_cwds)
            └─ Selector de proveedores
                 └─ Selector de modelos
                      └─ Crea sesión en OpenCode
                           └─ db.open_cwd(cwd)
                           └─ db.add_session(sid, cwd, title, model)
                           └─ db.set_active(sid)
                           └─ Listo para prompts
```

### /open (Proyecto ya abierto)

```
/open
  └─ Browser de carpetas
       └─ "✅ Open here" (cwd está en open_cwds)
            └─ Sincroniza sesiones desde OpenCode
            └─ Session picker:
                 • "➕ Nueva sesión" → model picker → crear sesión
                 • Activar sesión existente
                 • Borrar sesión
                 • "❌ Cerrar proyecto" → close_cwd
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

Solo procesa eventos de sesiones que están en `open_cwds`:

| Evento | Acción |
|--------|--------|
| `session.created` | Si cwd está abierto → guarda sesión en BD |
| `session.updated` | Actualiza título, modelo en BD |
| `session.deleted` | Elimina sesión de BD |
| `session.status` | Actualiza status (idle/busy/error) |
| `session.idle` | Muestra respuesta final |
| `session.error` | Muestra error, limpia estado |
| `session.next.text.*` | Streaming de texto |
| `session.next.reasoning.*` | Thinking/razonamiento |
| `session.next.tool.called` | Herramienta activa |

## Estado en `bot_data["statuses"]`

```python
{
  session_id: {
    "msg_id": int,          # ID del mensaje de estado en Telegram
    "state": str,           # busy | thinking | idle | error
    "tool": str | None,     # herramienta actual
    "tools_seen": [str],    # todas las herramientas llamadas
    "files_edited": set(),  # ficheros modificados
    "message_count": int,   # mensajes del assistant
    "last_text": str | None # último fragmento de texto
    "final_text": str | None # texto final acumulado
    "reasoning_text": str | None # texto de razonamiento
    "start_time": float,    # timestamp inicio
    "tokens_input": int,    # tokens input
    "tokens_output": int,   # tokens output
  }
}
```

## Callbacks registrados

| Pattern | Función | Descripción |
|---------|---------|-------------|
| `ob:` | `cb_ob` | Navegar carpetas |
| `os:` | `cb_os` | Abrir carpeta → session picker o model picker |
| `pickprov:` | `cb_pickprov` | Selector modelos del proveedor |
| `pickmodel:` | `cb_pickmodel` | Modelo elegido → crear sesión |
| `newsess:` | `cb_newsess` | Nueva sesión para cwd abierto |
| `actsess:` | `cb_actsess` | Activar sesión |
| `delsess:` | `cb_delsess` | Borrar sesión |
| `closecwd:` | `cb_closecwd` | Cerrar proyecto |
| `modelprov:` | `cb_modelprov` | Selector modelos (/models) |
| `modelset:` | `cb_modelset` | Establecer modelo |
| `abort:` | `cb_abort` | Cancelar tarea |
| `cancel:` | `cb_cancel` | Cancelar operación UI |
