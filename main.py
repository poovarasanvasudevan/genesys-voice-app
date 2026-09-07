import asyncio
import http
import json
import logging
import uuid
from datetime import datetime
from typing import List

import websockets

try:
    from websockets.asyncio.server import ServerConnection as _ServerConnection
    from websockets.http11 import Request as _WsRequest
except Exception:
    _ServerConnection = None
    _WsRequest = None

from audio_hook_server import AudioHookServer
from config import DEBUG, GENESYS_API_KEY, GENESYS_PATH, HOST, LOG_FILE, PORT, logger
from utils import format_json


async def validate_request(path_or_connection, headers_or_request=None):
    log_buffer: List[str] = []

    def _buffer(message: str):
        log_buffer.append(message)

    def _flush():
        for entry in log_buffer:
            logger.info(entry)
        log_buffer.clear()

    connection = None
    request = None

    if _ServerConnection and isinstance(path_or_connection, _ServerConnection):
        connection = path_or_connection
    if _WsRequest and isinstance(headers_or_request, _WsRequest):
        request = headers_or_request
    elif connection and _WsRequest and isinstance(getattr(connection, "request", None), _WsRequest):
        request = connection.request

    if request:
        path_value = request.path
        header_source = request.headers
    else:
        path_value = path_or_connection
        header_source = headers_or_request

    if isinstance(path_value, str):
        request_path = path_value
    else:
        request_path = getattr(path_value, "path", None) or str(path_value)

    def build_header_map(source):
        if source is None:
            return {}
        pairs = None
        raw_items = getattr(source, "raw_items", None)
        if callable(raw_items):
            pairs = list(raw_items())
        else:
            items_fn = getattr(source, "items", None)
            if callable(items_fn):
                pairs = list(items_fn())
        if pairs is None:
            try:
                pairs = list(source)
            except Exception:
                pairs = []
        return {str(k).lower(): str(v) for k, v in pairs}

    header_keys = build_header_map(header_source)
    upgrade_header = header_keys.get("upgrade", "").lower()

    def _build_response(status: http.HTTPStatus, text: str):
        if connection and hasattr(connection, "respond"):
            return connection.respond(status, text)
        return status, [], text.encode()

    # Platform health checks (Heroku/DO/K8s) hit these without WebSocket upgrade.
    health_paths = {"/", "", "/health", "/healthz", "/ready"}
    if request_path in health_paths and upgrade_header != "websocket":
        return _build_response(http.HTTPStatus.OK, "OK\n")

    _buffer(f"[HTTP] path={request_path}")
    _flush()

    if not request_path.startswith(GENESYS_PATH):
        # Non-WS probes to unknown paths still get a clean HTTP response when possible.
        if upgrade_header != "websocket":
            return _build_response(http.HTTPStatus.OK, "OK\n")
        return _build_response(http.HTTPStatus.NOT_FOUND, "Invalid path\n")

    incoming_api_key = header_keys.get("x-api-key")
    if not incoming_api_key:
        return _build_response(http.HTTPStatus.UNAUTHORIZED, "Missing x-api-key\n")
    if incoming_api_key != GENESYS_API_KEY:
        return _build_response(http.HTTPStatus.UNAUTHORIZED, "Invalid API Key\n")

    required = [
        "audiohook-organization-id",
        "audiohook-correlation-id",
        "audiohook-session-id",
        "upgrade",
        "sec-websocket-version",
        "sec-websocket-key",
    ]
    missing = [h for h in required if h not in header_keys]
    if missing:
        return _build_response(
            http.HTTPStatus.BAD_REQUEST,
            f"Missing required headers: {', '.join(missing)}\n",
        )

    if header_keys.get("upgrade", "").lower() != "websocket":
        return _build_response(http.HTTPStatus.BAD_REQUEST, "WebSocket upgrade required\n")
    if header_keys.get("sec-websocket-version") != "13":
        return _build_response(http.HTTPStatus.BAD_REQUEST, "WebSocket version 13 required\n")

    logger.info("[HTTP] AudioHook validation passed")
    return None


async def handle_genesys_connection(websocket):
    connection_id = str(uuid.uuid4())[:8]
    logger.info(f"[WS-{connection_id}] connected from {websocket.remote_address}")
    session = AudioHookServer(websocket)

    try:
        while session.running:
            try:
                msg = await websocket.recv()
                if isinstance(msg, bytes):
                    await session.handle_audio_frame(msg)
                else:
                    try:
                        data = json.loads(msg)
                        if DEBUG == "true":
                            logger.debug(f"[WS-{connection_id}] ←\n{format_json(data)}")
                        await session.handle_message(data)
                    except json.JSONDecodeError as exc:
                        logger.error(f"[WS-{connection_id}] bad JSON: {exc}")
                        # Keep the call up; ignore malformed control frames.
            except websockets.ConnectionClosed as exc:
                logger.info(f"[WS-{connection_id}] closed code={exc.code} reason={exc.reason}")
                break
            except Exception as exc:
                logger.error(f"[WS-{connection_id}] loop error: {exc}", exc_info=True)
                break
    finally:
        if session.openai_client:
            try:
                await session.openai_client.close()
            except Exception:
                pass
        logger.info(f"[WS-{connection_id}] handler finished")


async def main():
    startup = f"""
{'=' * 72}
Genesys ↔ OpenAI Realtime Audio Connector
Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Listen:  ws://{HOST}:{PORT}{GENESYS_PATH}
Log:     {LOG_FILE}
{'=' * 72}
"""
    logger.info(startup)

    if DEBUG != "true":
        logging.getLogger("websockets").setLevel(logging.INFO)

    async with websockets.serve(
        handle_genesys_connection,
        HOST,
        PORT,
        process_request=validate_request,
        max_size=64000,
        # Genesys uses application-level ping/pong; disable WS-level pings.
        ping_interval=None,
        ping_timeout=None,
    ):
        logger.info(f"Listening for AudioHook on ws://{HOST}:{PORT}{GENESYS_PATH}")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    except Exception as exc:
        logger.critical(f"Fatal: {exc}", exc_info=True)
