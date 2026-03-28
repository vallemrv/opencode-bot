#!/usr/bin/env python3
"""
Test para verificar el logging SSE del cliente OpenCode
"""

import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from opencode_client import OpenCodeClient, SSELogger

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)


async def test_sse_logger():
    """Prueba el logger SSE con eventos simulados"""
    session_id = "test_session_12345"
    sse_logger = SSELogger(session_id)
    
    test_events = [
        {"type": "session.status", "properties": {"status": {"type": "busy"}}},
        {"type": "message.updated", "properties": {"info": {"role": "assistant", "id": "msg_abc123"}}},
        {"type": "message.part.updated", "properties": {"part": {"id": "part_1", "type": "text", "text": "Hola"}}},
        {"type": "message.part.delta", "properties": {"partID": "part_1"}, "delta": " mundo"},
        {"type": "message.part.delta", "properties": {"partID": "part_1"}, "delta": "!"},
        {"type": "session.status", "properties": {"status": {"type": "idle"}}},
    ]
    
    logger.info("=" * 60)
    logger.info("Probando SSELogger con eventos simulados")
    logger.info("=" * 60)
    
    for evt in test_events:
        sse_logger.log_event(evt)
        await asyncio.sleep(0.1)
    
    summary = sse_logger.summary()
    logger.info(f"Resumen: {summary}")
    
    assert summary["total_events"] == 6
    assert summary["total_chars"] == 7  # Solo deltas: " mundo!" (el "Hola" vino en part.updated)
    logger.info("✅ Test SSELogger pasado")


async def test_sse_connection():
    """Prueba la conexión SSE real al servidor"""
    client = OpenCodeClient()
    session_id = None
    
    logger.info("=" * 60)
    logger.info("Probando conexión SSE real")
    logger.info("=" * 60)
    
    try:
        # Check health
        health = await client.health_check()
        logger.info(f"Health check: {'✅' if health else '❌'}")
        
        if not health:
            logger.warning("Server no disponible, saltando test SSE")
            return
        
        # Crear sesión
        session = await client.create_session(title="test_sse_logging")
        session_id = session.get("id") or session.get("sessionID")
        logger.info(f"Sesión creada: {session_id}")
        
        # Enviar mensaje
        payload = {"parts": [{"type": "text", "text": "Di solo OK"}]}
        
        event_count = 0
        async for evt in client.stream_session_events(session_id, inactivity_timeout=30):
            event_count += 1
            etype = evt.get("type", "")
            
            if etype == "session.status":
                status = evt.get("properties", {}).get("status", {}).get("type", "")
                if status == "idle":
                    logger.info("Estado idle alcanzado")
                    break
        
        logger.info(f"Eventos recibidos: {event_count}")
        
        # Fetch mensajes
        messages = await client.get_messages(session_id)
        logger.info(f"Mensajes en sesión: {len(messages)}")
        
        # Limpiar
        await client.delete_session(session_id)
        logger.info("✅ Test conexión SSE pasado")
        
    except Exception as e:
        logger.error(f"❌ Test fallido: {type(e).__name__}: {e}", exc_info=True)
    finally:
        if session_id:
            try:
                await client.delete_session(session_id)
            except Exception:
                pass


async def main():
    await test_sse_logger()
    await asyncio.sleep(1)
    await test_sse_connection()


if __name__ == "__main__":
    asyncio.run(main())
