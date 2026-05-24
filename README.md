# OpenCode Bot

Bot de Telegram para controlar el servidor OpenCode remotamente.

![Telegram](https://img.shields.io/badge/Telegram-Bot-blue?logo=telegram)
![Python](https://img.shields.io/badge/Python-3.11+-green?logo=python)
![OpenCode](https://img.shields.io/badge/OpenCode-Server-purple)

## Características

- Control remoto de OpenCode desde Telegram
- Streaming en tiempo real vía SSE
- Gestión de múltiples proyectos y sesiones
- Transcripción de audio (X.AI STT)
- Respuestas largas enviadas como archivos Markdown
- Permisos inline con aprobación/denegación
- Preguntas del LLM con respuestas inline

## Comandos

| Comando | Descripción |
|---------|-------------|
| `/start` | Estado: sesión activa, proyecto, modelo |
| `/open` | Browser de carpetas → crear/activar sesión |
| `/close` | Cerrar proyecto y borrar sesiones |
| `/sessions` | Gestiona sesiones del proyecto activo |
| `/models` | Cambiar modelo de la sesión activa |
| `/projects` | Lista proyectos con sesiones |
| `/send` | Enviar prompt a otro proyecto |
| `/esc` | Cancelar tarea en curso |

También puedes responder a mensajes del bot para continuar conversaciones específicas.

## Requisitos

- OpenCode Server corriendo en `localhost:4096`
- Python 3.11+
- Token de Telegram Bot
- Token X.AI (opcional, para transcripción de audio)

## Instalación

```bash
# Clonar
git clone git@github.com:vallemrv/opencode-bot.git
cd opencode-bot

# Crear venv e instalar dependencias
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Configurar
cp .env.example .env
# Editar .env con tus tokens

# Ejecutar
python3 src/telegram_bot.py
```

## Configuración

Edita `.env`:

```env
TELEGRAM_BOT_TOKEN=your_token
TELEGRAM_ADMIN_ID=your_user_id
OPENCODE_HOST=localhost
OPENCODE_PORT=4096
DEFAULT_WORKSPACE=/home/user/projects
XAI_API_KEY=your_xai_key  # opcional
```

## Systemd (opcional)

```bash
# Copiar service example
sudo cp opencode-bot.service.example /etc/systemd/system/opencode-bot.service
# Editar paths según tu configuración
sudo systemctl daemon-reload
sudo systemctl enable opencode-bot
sudo systemctl start opencode-bot
```

## Estructura

```
src/
  telegram_bot.py      — Bot principal
  opencode_client.py   — Cliente HTTP + SSE
  db.py                — SQLite (sesión activa)
  transcription.py     — Transcripción audio
  md2tgv2.py           — Markdown → Telegram
```

## Flujo

1. `/open` → selecciona proyecto → crea/activa sesión
2. Envía texto/audio → OpenCode procesa
3. SSE streaming → respuesta en tiempo real
4. Respuestas largas → archivo `respuesta.md`

## Seguridad

- Solo el `ADMIN_ID` puede usar el bot
- `.env` y tokens nunca se commitean
- Repo privado recomendado

## Licencia

MIT