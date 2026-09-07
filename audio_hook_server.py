import asyncio
import json
import time
import uuid
from collections import deque

import websockets

from config import (
    AI_MODEL,
    AI_VOICE,
    AUDIO_BUFFER_WARN_HIGH,
    AUDIO_BUFFER_WARN_MEDIUM,
    DEFAULT_AGENT_NAME,
    DEFAULT_COMPANY_NAME,
    GENESYS_BINARY_BURST_LIMIT,
    GENESYS_BINARY_RATE_LIMIT,
    GENESYS_MSG_BURST_LIMIT,
    GENESYS_MSG_RATE_LIMIT,
    GENESYS_PCMU_FRAME_SIZE,
    MAX_AUDIO_BUFFER_FRAMES,
    RATE_LIMIT_MAX_RETRIES,
    logger,
)
from openai_client import OpenAIRealtimeClient
from rate_limiter import RateLimiter
from utils import format_json, parse_iso8601_duration


class AudioHookServer:
    """Genesys AudioHook session bridged to OpenAI Realtime (PCMU 8 kHz)."""

    def __init__(self, websocket):
        self.session_id = str(uuid.uuid4())
        self.ws = websocket
        self.client_seq = 0
        self.server_seq = 0
        self.openai_client: OpenAIRealtimeClient | None = None
        self.running = True
        self.in_backoff = False
        self.start_time = time.time()
        self.logger = logger.getChild(f"AudioHook_{self.session_id[:8]}")

        self.audio_frames_sent = 0
        self.audio_frames_received = 0
        self.rate_limit_retries = 0

        self.message_limiter = RateLimiter(GENESYS_MSG_RATE_LIMIT, GENESYS_MSG_BURST_LIMIT)
        self.binary_limiter = RateLimiter(GENESYS_BINARY_RATE_LIMIT, GENESYS_BINARY_BURST_LIMIT)

        self.audio_buffer: deque[bytes] = deque(maxlen=MAX_AUDIO_BUFFER_FRAMES)
        self._pcmu_reassembly = bytearray()
        self.audio_process_task: asyncio.Task | None = None
        self._disconnecting = False

        self.session_outcome = {
            "escalation_required": False,
            "escalation_reason": "",
            "summary": "",
        }

        self.logger.info("Session created")

    async def start_audio_processing(self):
        if self.audio_process_task is None:
            self.audio_process_task = asyncio.create_task(self._process_audio_buffer())

    async def stop_audio_processing(self):
        if self.audio_process_task:
            self.audio_process_task.cancel()
            try:
                await self.audio_process_task
            except asyncio.CancelledError:
                pass
            self.audio_process_task = None

    async def _process_audio_buffer(self):
        try:
            while self.running:
                if self.audio_buffer and not self.in_backoff:
                    if await self.binary_limiter.acquire():
                        frame = self.audio_buffer.popleft()
                        try:
                            await self.ws.send(frame)
                            self.audio_frames_sent += 1
                        except websockets.ConnectionClosed:
                            self.logger.warning("Genesys closed while sending audio")
                            self.running = False
                            break
                    else:
                        await asyncio.sleep(0.01)
                else:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            self.logger.debug("Audio pump cancelled")
        except Exception as exc:
            self.logger.error(f"Audio pump error: {exc}", exc_info=True)

    async def handle_message(self, msg: dict):
        msg_type = msg.get("type")
        self.client_seq = msg.get("seq", self.client_seq)

        if self.in_backoff and msg_type not in ("error", "ping", "close"):
            return

        if msg_type == "open":
            await self.handle_open(msg)
        elif msg_type == "ping":
            await self.handle_ping(msg)
        elif msg_type == "close":
            await self.handle_close(msg)
        elif msg_type == "error":
            await self.handle_error(msg)
        elif msg_type in ("update", "resume", "pause"):
            self.logger.debug(f"Ignoring {msg_type}")
        else:
            self.logger.debug(f"Unknown message type: {msg_type}")

    async def handle_open(self, msg: dict):
        self.session_id = msg["id"]
        params = msg.get("parameters") or {}

        is_probe = (
            params.get("conversationId") == "00000000-0000-0000-0000-000000000000"
            and (params.get("participant") or {}).get("id")
            == "00000000-0000-0000-0000-000000000000"
        )
        if is_probe:
            self.logger.info("Probe connection")
            await self._send_json(
                {
                    "version": "2",
                    "type": "opened",
                    "seq": self.server_seq + 1,
                    "clientseq": self.client_seq,
                    "id": self.session_id,
                    "parameters": {"startPaused": False, "media": []},
                }
            )
            self.server_seq += 1
            return

        chosen = None
        for media in params.get("media") or []:
            if media.get("format") == "PCMU" and media.get("rate") == 8000:
                chosen = media
                break

        if not chosen:
            await self._send_json(
                {
                    "version": "2",
                    "type": "disconnect",
                    "seq": self.server_seq + 1,
                    "clientseq": self.client_seq,
                    "id": self.session_id,
                    "parameters": {"reason": "error", "info": "No supported PCMU/8000 media"},
                }
            )
            self.server_seq += 1
            self.running = False
            return

        await self._send_json(
            {
                "version": "2",
                "type": "opened",
                "seq": self.server_seq + 1,
                "clientseq": self.client_seq,
                "id": self.session_id,
                "parameters": {"startPaused": False, "media": [chosen]},
            }
        )
        self.server_seq += 1
        self.logger.info(f"Opened with media={chosen}")

        input_vars = params.get("inputVariables") or {}
        voice = input_vars.get("AI_VOICE") or input_vars.get("OPENAI_VOICE") or AI_VOICE
        instructions = (
            input_vars.get("AI_SYSTEM_PROMPT")
            or input_vars.get("OPENAI_SYSTEM_PROMPT")
            or "You are a helpful phone assistant."
        )
        model = input_vars.get("AI_MODEL") or input_vars.get("OPENAI_MODEL") or AI_MODEL
        temperature = input_vars.get("AI_TEMPERATURE") or input_vars.get("OPENAI_TEMPERATURE")
        language = input_vars.get("LANGUAGE")
        customer_data = input_vars.get("CUSTOMER_DATA")
        agent_name = input_vars.get("AGENT_NAME", DEFAULT_AGENT_NAME)
        company_name = next(
            (v for k, v in input_vars.items() if k.strip() == "COMPANY_NAME"),
            DEFAULT_COMPANY_NAME,
        )
        success_prompt = input_vars.get("SUCCESS_PROMPT")
        escalation_prompt = input_vars.get("ESCALATION_PROMPT")

        try:
            client = OpenAIRealtimeClient(
                self.session_id,
                on_speech_started_callback=self.handle_speech_started,
            )
            client.language = language
            client.customer_data = customer_data
            client.success_prompt = success_prompt
            client.escalation_prompt = escalation_prompt
            client.on_end_call_request = self._on_end_call_request
            client.on_handoff_request = self._on_handoff_request

            await client.connect(
                instructions=instructions,
                voice=voice,
                temperature=temperature,
                model=model,
                agent_name=agent_name,
                company_name=company_name,
                greet=True,
            )
            self.openai_client = client

            def on_audio(pcmu: bytes):
                # Keep ordering: enqueue framed audio on the event loop.
                asyncio.create_task(self.handle_openai_audio(pcmu))

            await self.start_audio_processing()
            await client.start_receiving(on_audio)
        except Exception as exc:
            self.logger.error(f"OpenAI connect failed: {exc}", exc_info=True)
            await self.disconnect_session(reason="error", info=str(exc))

    async def handle_speech_started(self):
        """Barge-in: stop local playback immediately and notify Genesys."""
        self.audio_buffer.clear()
        self._pcmu_reassembly.clear()

        await self._send_json(
            {
                "version": "2",
                "type": "event",
                "seq": self.server_seq + 1,
                "clientseq": self.client_seq,
                "id": self.session_id,
                "parameters": {"entities": [{"type": "barge_in", "data": {}}]},
            }
        )
        self.server_seq += 1

        if self.openai_client:
            await self.openai_client.cancel_response()

    async def handle_openai_audio(self, pcmu_8k: bytes):
        if not self.running or self._disconnecting:
            return

        # Reassemble arbitrary OpenAI deltas into fixed 1600-byte Genesys frames.
        self._pcmu_reassembly.extend(pcmu_8k)
        while len(self._pcmu_reassembly) >= GENESYS_PCMU_FRAME_SIZE:
            frame = bytes(self._pcmu_reassembly[:GENESYS_PCMU_FRAME_SIZE])
            del self._pcmu_reassembly[:GENESYS_PCMU_FRAME_SIZE]
            await self._enqueue_frame(frame)

    async def _enqueue_frame(self, frame: bytes):
        if len(self.audio_buffer) >= MAX_AUDIO_BUFFER_FRAMES:
            self.logger.error("Audio buffer full — dropping frame to protect Genesys session")
            return

        self.audio_buffer.append(frame)
        usage = len(self.audio_buffer) / MAX_AUDIO_BUFFER_FRAMES
        if usage >= AUDIO_BUFFER_WARN_HIGH:
            self.logger.warning(
                f"Audio buffer HIGH {len(self.audio_buffer)}/{MAX_AUDIO_BUFFER_FRAMES}"
            )
        elif usage >= AUDIO_BUFFER_WARN_MEDIUM:
            self.logger.info(
                f"Audio buffer elevated {len(self.audio_buffer)}/{MAX_AUDIO_BUFFER_FRAMES}"
            )

    async def handle_audio_frame(self, frame_bytes: bytes):
        if not self.openai_client or not self.openai_client.running:
            return
        self.audio_frames_received += 1
        await self.openai_client.send_audio(frame_bytes)

    async def handle_ping(self, msg: dict):
        try:
            await asyncio.wait_for(
                self._send_json(
                    {
                        "version": "2",
                        "type": "pong",
                        "seq": self.server_seq + 1,
                        "clientseq": self.client_seq,
                        "id": self.session_id,
                        "parameters": {},
                    }
                ),
                timeout=1.0,
            )
            self.server_seq += 1
        except asyncio.TimeoutError:
            self.logger.error("pong send timed out")

    async def handle_error(self, msg: dict):
        """Handle Genesys errors without killing the main session loop."""
        params = msg.get("parameters") or {}
        code = params.get("code")
        if code != 429:
            self.logger.error(f"Genesys error: {format_json(msg)}")
            return

        self.rate_limit_retries += 1
        if self.rate_limit_retries > RATE_LIMIT_MAX_RETRIES:
            await self.disconnect_session(reason="error", info="Genesys rate limit retries exceeded")
            return

        retry_after = None
        if "retryAfter" in params:
            try:
                retry_after = parse_iso8601_duration(params["retryAfter"])
            except ValueError:
                retry_after = None
        delay = retry_after if retry_after is not None else min(3 * self.rate_limit_retries, 27)

        # CRITICAL: do NOT set self.running = False here — that exits the recv loop permanently.
        self.in_backoff = True
        self.logger.warning(
            f"Genesys 429 — backoff {delay}s (attempt {self.rate_limit_retries}/{RATE_LIMIT_MAX_RETRIES})"
        )
        await asyncio.sleep(delay)
        self.in_backoff = False
        self.logger.info("Genesys rate-limit backoff complete; session continues")

    async def handle_close(self, msg: dict):
        self.logger.info(f"Genesys close: {msg.get('parameters', {}).get('reason')}")
        summary = None
        if self.openai_client:
            summary = await self.openai_client.request_summary()
            if summary:
                self.session_outcome["summary"] = summary

        try:
            await asyncio.wait_for(
                self._send_json(
                    {
                        "version": "2",
                        "type": "closed",
                        "seq": self.server_seq + 1,
                        "clientseq": self.client_seq,
                        "id": self.session_id,
                        "parameters": {"summary": summary},
                    }
                ),
                timeout=4.0,
            )
            self.server_seq += 1
        except asyncio.TimeoutError:
            self.logger.error("closed send timed out")

        await self._cleanup()
        self.running = False

    async def _on_end_call_request(self, reason: str, info: str):
        self.session_outcome.update({"escalation_required": False, "escalation_reason": ""})
        await self.disconnect_session(reason=reason or "completed", info=info or "")

    async def _on_handoff_request(self, reason: str, info: str):
        escalation = info or reason or "Customer requested escalation"
        self.session_outcome.update(
            {"escalation_required": True, "escalation_reason": escalation}
        )
        await self.disconnect_session(reason="completed", info=escalation)

    async def disconnect_session(self, reason: str = "completed", info: str = ""):
        if self._disconnecting:
            return
        self._disconnecting = True
        self.logger.info(f"Disconnecting reason={reason} info={info}")

        # Drain farewell audio (framed) before tearing down.
        wait_start = time.time()
        while self.audio_buffer and (time.time() - wait_start) < 15.0:
            await asyncio.sleep(0.05)

        # Flush any leftover reassembly bytes as a padded final frame.
        if self._pcmu_reassembly and self.running:
            pad = GENESYS_PCMU_FRAME_SIZE - len(self._pcmu_reassembly)
            if pad < GENESYS_PCMU_FRAME_SIZE:
                frame = bytes(self._pcmu_reassembly) + bytes([0xFF] * pad)
                self._pcmu_reassembly.clear()
                await self._enqueue_frame(frame)
                drain_start = time.time()
                while self.audio_buffer and (time.time() - drain_start) < 5.0:
                    await asyncio.sleep(0.05)

        summary = self.session_outcome.get("summary") or ""
        tokens = {}
        if self.openai_client:
            if not summary:
                summary = (await self.openai_client.request_summary()) or ""
            cum = self.openai_client.cumulative_tokens
            tokens = {
                "TOTAL_INPUT_TEXT_TOKENS": str(cum.get("input_text_tokens", 0)),
                "TOTAL_INPUT_CACHED_TEXT_TOKENS": str(cum.get("input_cached_text_tokens", 0)),
                "TOTAL_INPUT_AUDIO_TOKENS": str(cum.get("input_audio_tokens", 0)),
                "TOTAL_INPUT_CACHED_AUDIO_TOKENS": str(cum.get("input_cached_audio_tokens", 0)),
                "TOTAL_OUTPUT_TEXT_TOKENS": str(cum.get("output_text_tokens", 0)),
                "TOTAL_OUTPUT_AUDIO_TOKENS": str(cum.get("output_audio_tokens", 0)),
            }

        output_vars = {
            "CONVERSATION_SUMMARY": summary,
            "COMPLETION_SUMMARY": summary,
            "CONVERSATION_DURATION": str(time.time() - self.start_time),
            "ESCALATION_REQUIRED": "true" if self.session_outcome.get("escalation_required") else "false",
            "ESCALATION_REASON": self.session_outcome.get("escalation_reason", ""),
            **tokens,
        }

        try:
            await asyncio.wait_for(
                self._send_json(
                    {
                        "version": "2",
                        "type": "disconnect",
                        "seq": self.server_seq + 1,
                        "clientseq": self.client_seq,
                        "id": self.session_id,
                        "parameters": {
                            "reason": reason,
                            "info": info,
                            "outputVariables": output_vars,
                        },
                    }
                ),
                timeout=5.0,
            )
            self.server_seq += 1
        except Exception as exc:
            self.logger.error(f"Failed to send disconnect: {exc}")

        await self._cleanup()
        self.running = False

    async def _cleanup(self):
        await self.stop_audio_processing()
        self.audio_buffer.clear()
        self._pcmu_reassembly.clear()
        if self.openai_client:
            try:
                await self.openai_client.close()
            except Exception:
                pass
            self.openai_client = None
        duration = time.time() - self.start_time
        self.logger.info(
            f"Cleanup done duration={duration:.1f}s "
            f"sent={self.audio_frames_sent} recv={self.audio_frames_received}"
        )

    async def _send_json(self, msg: dict):
        try:
            if not await self.message_limiter.acquire():
                self.logger.warning(f"JSON rate limited, dropping type={msg.get('type')}")
                return
            await self.ws.send(json.dumps(msg))
        except websockets.ConnectionClosed:
            self.logger.warning("Genesys closed while sending JSON")
            self.running = False
