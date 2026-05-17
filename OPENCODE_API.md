# OpenCode Server API Documentation

## Overview

OpenCode usa una arquitectura **client/server**:
- El servidor expone una API HTTP con OpenAPI 3.1 spec
- El TUI, desktop app, IDE plugins son clientes que se conectan al server
- Permite múltiples clientes conectados al mismo server

## Server

### Comando
```bash
opencode serve [--port 4096] [--hostname 127.0.0.1] [--mdns]
```

### Puerto Default
- **4096** ( configurable con `--port`)

### OpenAPI Spec
- Disponible en: `http://localhost:4096/doc`
- Swagger UI para explorar endpoints

### Authentication
```bash
OPENCODE_SERVER_PASSWORD=secret opencode serve
```

## SSE (Server-Sent Events)

### Endpoint Principal
```
GET /event
```

Stream de eventos en tiempo real. Primer evento: `server.connected`

### Event Types

| Type | Description |
|------|-------------|
| `server.connected` | Conexión establecida |
| `session.status` | Estado de sesión (busy/idle) |
| `session.error` | Error en sesión |
| `message.updated` | Mensaje actualizado |
| `message.part.updated` | Parte de mensaje actualizada |
| `message.part.delta` | Delta/streaming de texto |
| `tool.invocation` | Tool being called |

### Estructura de Eventos
```json
{
  "type": "session.status",
  "properties": {
    "sessionID": "ses_xxx",
    "status": {
      "type": "busy"
    }
  }
}
```

## Sessions API

### List Sessions
```
GET /session
```
Response: `Session[]`

### Create Session
```
POST /session
Body: { title?: string, parentID?: string }
```

### Get Session Status
```
GET /session/status
```
Response: `{ [sessionID]: SessionStatus }`

### Session Object
```typescript
interface Session {
  id: string
  title: string
  projectID: string
  directory: string
  time: {
    created: number
    updated: number
  }
}
```

## Messages API

### Send Message
```
POST /session/:id/message
Body: {
  parts: [{ type: "text", text: "..." }],
  model?: { providerID, modelID },
  agent?: string
}
```

### Get Messages
```
GET /session/:id/message
```
Response: `{ info: Message, parts: Part[] }[]`

### Message Parts Types

| Part Type | Description |
|-----------|-------------|
| `text` | Texto del mensaje |
| `reasoning` | Pensamiento/thinking del modelo |
| `tool-invocation` | Tool being called |
| `tool-result` | Resultado del tool |
| `step-finish` | Paso completado (incluye tokens) |
| `error` | Error |

### Tokens en step-finish
```json
{
  "type": "step-finish",
  "tokens": {
    "input": 100,
    "output": 500,
    "total": 600,
    "cache": { read: 50, write: 20 }
  }
}
```

## Context Percentage

El contexto se calcula desde los tokens:
- Cada modelo tiene un **context window** máximo
- `input tokens` / `context window` = % usado
- Auto-compact cuando llega a 95%

### Model Context Windows (approx)
| Model | Context Window |
|-------|----------------|
| Claude 3.5 Sonnet | 200K |
| Claude 4 | 200K |
| GPT-4o | 128K |
| Gemini 2.5 | 1M |
| Qwen 3.5 | 128K |

## Status Feedback Best Practice

Mostrar en heartbeat:
1. **Project name** - Nombre del folder/workspace
2. **Session title** - Truncado (max 20 chars)
3. **Status** - busy/idle
4. **Context %** - tokens / context_window
5. **Last action** - tool o reasoning

### Ejemplo de Status Message
```
⏳ mi-proyecto | session_xxx... (25%)
🔧 edit: file.py
💬 Thinking about the implementation...
```

## SDK Usage (TypeScript)

```typescript
import { createOpencode } from "@opencode-ai/sdk"

const { client } = await createOpencode()

// List sessions
const sessions = await client.session.list()

// Create session
const session = await client.session.create({ body: { title: "My Session" } })

// Send message
const result = await client.session.prompt({
  path: { id: session.id },
  body: {
    parts: [{ type: "text", text: "Hello!" }],
    model: { providerID: "anthropic", modelID: "claude-3-5-sonnet" }
  }
})

// Subscribe to events
const events = await client.event.subscribe()
for await (const event of events.stream) {
  console.log(event.type, event.properties)
}
```

## Python Client Implementation

Ver `src/opencode_client.py` para implementación Python:
- `get_session_status(session_id)` - Estado de una sesión (busy/idle)
- `get_all_session_status()` - Estado de todas las sesiones
- `is_session_busy(session_id)` - Check si sesión está procesando
- `get_messages(session_id)` - Obtener mensajes
- `create_session(title)` - Crear sesión
- `send_message(session_id, payload)` - Enviar mensaje (blocking)
- `send_message_async(session_id, payload)` - Enviar mensaje (async, no wait)
- `abort_session(session_id)` - Cancelar sesión en curso
- `stream_session_events(session_id)` - SSE stream

## Message Queue

OpenCode maneja internamente la cola de mensajes por sesión:
- Si sesión está "busy", mensajes se encolan automáticamente
- No necesitas cola local en el cliente
- Usa `/session/status` para saber si puedes enviar

## Referencias

- [Server Docs](https://opencode.ai/docs/server)
- [SDK Docs](https://opencode.ai/docs/sdk)
- [GitHub](https://github.com/anomalyco/opencode)