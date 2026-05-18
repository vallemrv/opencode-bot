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
  db.py                — SQLite local: sesión activa (session_id, cwd, model)
```

## Variables de entorno (`.env`)

```env
TELEGRAM_BOT_TOKEN=<token>
TELEGRAM_ADMIN_ID=<id>
OPENCODE_HOST=127.0.0.1
OPENCODE_PORT=4096
DEFAULT_WORKSPACE=~/proyectos
```

> No hay BOT_PORT ni bot_server. El bot solo se conecta al servidor OpenCode.

## OpenCode Server

El servidor OpenCode debe estar corriendo en `OPENCODE_HOST:OPENCODE_PORT` (por defecto `127.0.0.1:4096`).

```bash
opencode serve --port 4096
```

El bot **no gestiona** el proceso de OpenCode, solo se conecta a él vía HTTP y SSE.

## Comandos del bot

| Comando | Descripción |
|---------|-------------|
| `/start` | Estado completo: sesión activa, proyecto, modelo, estado del server |
| `/open`  | Browser de carpetas → elige proveedor → elige modelo → crea sesión |
| `/close` | Cierra (elimina) todas las sesiones de un proyecto |
| `/sessions` | Gestiona sesiones del proyecto activo (pagindas, solo del cwd actual) |
| `/models` | Cambia el modelo de la sesión activa |
| `/esc` | Cancela la tarea en curso (abort) |

Cualquier texto libre envía un prompt a la sesión activa.

## Flujo principal

```
/open
  └─ Browser de carpetas (paginado, 8 por página)
       └─ "✅ Open here"
            └─ Selector de proveedores (un botón por proveedor)
                 └─ Selector de modelos del proveedor
                      └─ Crea sesión → activa → listo para prompts

Texto libre
  └─ Envía prompt a sesión activa (prompt_async → 204 OK)
       └─ Muestra mensaje de estado INMEDIATO con:
            • Estado (BUSY / THINKING)
            • Proyecto y modelo
            • Herramienta en uso
            • Fragmento de texto en streaming
            • Botón ❌ Cancelar
       └─ SSE actualiza el mensaje en tiempo real
       └─ Al finalizar (session.status idle): elimina status, muestra respuesta completa
            • Si hay pregunta → muestra botón "❌ Cancelar / ignorar"
```

## SSE Events que procesa el bot

| Evento | Acción |
|--------|--------|
| `session.status` (idle) | Muestra respuesta final y cierra estado |
| `session.status` (busy) | Actualiza estado |
| `tool.invocation` / `session.next.tool.called` | Muestra herramienta activa |
| `message.part.delta` | Muestra fragmento de texto en streaming |
| `message.part.updated` | Actualiza tipo (reasoning = thinking) |
| `message.updated` | Incrementa contador de mensajes |
| `session.idle` | Fallback: muestra respuesta final |
| `session.error` | Muestra error y limpia estado |

## Estado en `bot_data["status"]`

```python
{
  "session_id": str,
  "msg_id": int,          # ID del mensaje de estado en Telegram
  "state": str,           # busy | thinking | idle | error
  "tool": str | None,     # herramienta actual
  "tools_seen": [str],    # todas las herramientas llamadas
  "files_edited": set(),  # ficheros modificados
  "message_count": int,   # mensajes del assistant
  "last_text": str | None # último fragmento de texto (streaming)
}
```

## DB local (`db.py`)

Solo guarda **una sesión activa** a la vez:
- `session_id`, `cwd`, `model` (formato `providerID/modelID`)
- `db.get_active()`, `db.set_active(sid, cwd, model)`, `db.clear_active()`

## Callbacks registrados

| Pattern | Función | Descripción |
|---------|---------|-------------|
| `ob:` | `cb_ob` | Navegar carpetas |
| `os:` | `cb_os` | Abrir carpeta → selector proveedores |
| `pickprov:` | `cb_pickprov` | Selector modelos del proveedor |
| `pickmodel:` | `cb_pickmodel` | Modelo elegido → crear sesión |
| `modelprov:` | `cb_modelprov` | Selector modelos (/models) |
| `modelset:` | `cb_modelset` | Establecer modelo en sesión activa |
| `cl:` | `cb_cl` | Cerrar proyecto |
| `sn:` | `cb_sn` | Nueva sesión |
| `sa:` | `cb_sa` | Activar sesión |
| `sd:` | `cb_sd` | Borrar sesión |
| `sda:` | `cb_sda` | Borrar todas las sesiones |
| `sesspage:` | `cb_sesspage` | Paginación de sesiones |
| `abort:` | `cb_abort` | Cancelar tarea (abort_session) |
| `cancel:` | `cb_cancel` | Cancelar operación de UI |
