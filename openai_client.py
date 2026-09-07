import asyncio
import base64
import json
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional

import websockets

from config import (
    AI_MODEL,
    DEBUG,
    DEFAULT_TEMPERATURE,
    ENDING_SUMMARY_PROMPT,
    OPENAI_API_KEY,
    OPENAI_VAD_EAGERNESS,
    OPENAI_VAD_TYPE,
    logger,
)
from utils import create_final_system_prompt, format_json, get_websocket_connect_kwargs, is_websocket_open


CALL_CONTROL_GUIDANCE = """[CALL CONTROL]
Use end_conversation_successfully when the caller clearly confirms they are done.
Use end_conversation_with_escalation when the caller asks for a human or the task is blocked.
Do not call either tool for brief silence, hesitation, or mild frustration.
After invoking a call-control tool, give a short spoken farewell."""


def _call_control_tools() -> List[Dict[str, Any]]:
    return [
        {
            "type": "function",
            "name": "end_conversation_successfully",
            "description": (
                "End the phone call only after the caller clearly confirms they are finished. "
                "Provide a short summary of what was accomplished."
            ),
            "parameters": {
                "type": "object",
                "strict": True,
                "properties": {
                    "summary": {
                        "type": "string",
                        "description": "One-sentence summary of what was accomplished.",
                    }
                },
                "required": ["summary"],
                "additionalProperties": False,
            },
        },
        {
            "type": "function",
            "name": "end_conversation_with_escalation",
            "description": (
                "End the call and request transfer to a human when the caller asks for a person "
                "or the task cannot be completed."
            ),
            "parameters": {
                "type": "object",
                "strict": True,
                "properties": {
                    "reason": {
                        "type": "string",
                        "description": "Why escalation is needed.",
                    }
                },
                "required": ["reason"],
                "additionalProperties": False,
            },
        },
    ]


class OpenAIRealtimeClient:
    """OpenAI Realtime bridge. VAD owns turn-taking; no manual commit/response.create on speech stop."""

    def __init__(
        self,
        session_id: str,
        on_speech_started_callback: Optional[Callable[[], Awaitable[None]]] = None,
    ):
        self.session_id = session_id
        self.logger = logger.getChild(f"OpenAIClient_{session_id}")
        self.on_speech_started_callback = on_speech_started_callback

        self.ws = None
        self.running = False
        self.read_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self.start_time = time.time()

        self.voice = "sage"
        self.model = AI_MODEL
        self.temperature = DEFAULT_TEMPERATURE
        self.final_instructions = ""
        self.language = None
        self.customer_data = None
        self.success_prompt = None
        self.escalation_prompt = None

        self.on_end_call_request: Optional[Callable[[str, str], Awaitable[None]]] = None
        self.on_handoff_request: Optional[Callable[[str, str], Awaitable[None]]] = None
        self._await_disconnect_on_done = False
        self._disconnect_context: Optional[dict] = None
        self._summary_future: Optional[asyncio.Future] = None
        self._response_in_progress = False

        self.cumulative_tokens = {
            "input_text_tokens": 0,
            "input_cached_text_tokens": 0,
            "input_audio_tokens": 0,
            "input_cached_audio_tokens": 0,
            "output_text_tokens": 0,
            "output_audio_tokens": 0,
        }

    def _realtime_url(self) -> str:
        return f"wss://api.openai.com/v1/realtime?model={self.model}"

    async def connect(
        self,
        instructions: Optional[str] = None,
        voice: Optional[str] = None,
        temperature: Optional[Any] = None,
        model: Optional[str] = None,
        agent_name: Optional[str] = None,
        company_name: Optional[str] = None,
        greet: bool = True,
    ):
        self.model = model or AI_MODEL
        self.voice = (voice or "sage").strip() or "sage"
        try:
            self.temperature = float(temperature) if temperature is not None else DEFAULT_TEMPERATURE
            if not (0.6 <= self.temperature <= 1.2):
                self.temperature = DEFAULT_TEMPERATURE
        except (TypeError, ValueError):
            self.temperature = DEFAULT_TEMPERATURE

        self.final_instructions = create_final_system_prompt(
            instructions,
            language=self.language,
            customer_data=self.customer_data,
            agent_name=agent_name,
            company_name=company_name,
        )
        instructions_text = f"{self.final_instructions}\n\n{CALL_CONTROL_GUIDANCE}"

        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        url = self._realtime_url()
        self.logger.info(f"Connecting to OpenAI Realtime model={self.model}")

        connect_kwargs = get_websocket_connect_kwargs(
            url,
            headers,
            max_size=2**23,
            compression=None,
            max_queue=64,
            ping_interval=20,
            ping_timeout=20,
        )
        self.ws = await asyncio.wait_for(websockets.connect(**connect_kwargs), timeout=15.0)
        self.running = True

        created = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=15.0))
        if created.get("type") == "error":
            await self.close()
            raise RuntimeError(f"OpenAI connect error: {created}")
        if created.get("type") != "session.created":
            await self.close()
            raise RuntimeError(f"Expected session.created, got {created.get('type')}")

        turn_detection: Dict[str, Any] = {
            "type": OPENAI_VAD_TYPE,
            "create_response": True,
            "interrupt_response": True,
        }
        if OPENAI_VAD_TYPE == "semantic_vad":
            turn_detection["eagerness"] = OPENAI_VAD_EAGERNESS
        elif OPENAI_VAD_TYPE == "server_vad":
            turn_detection.update(
                {
                    "threshold": 0.5,
                    "prefix_padding_ms": 300,
                    "silence_duration_ms": 600,
                }
            )

        session_update = {
            "type": "session.update",
            "session": {
                "type": "realtime",
                "model": self.model,
                "instructions": instructions_text,
                "output_modalities": ["audio"],
                "tools": _call_control_tools(),
                "tool_choice": "auto",
                "audio": {
                    "input": {
                        "format": {"type": "audio/pcmu"},
                        "turn_detection": turn_detection,
                    },
                    "output": {
                        "format": {"type": "audio/pcmu"},
                        "voice": self.voice,
                    },
                },
            },
        }
        await self._safe_send(json.dumps(session_update))

        updated = False
        deadline = time.time() + 15.0
        while time.time() < deadline:
            ev = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=15.0))
            et = ev.get("type")
            if et == "session.updated":
                updated = True
                break
            if et == "error":
                await self.close()
                raise RuntimeError(f"OpenAI session.update error: {ev}")
            self.logger.debug(f"Ignoring pre-update event: {et}")

        if not updated:
            await self.close()
            raise RuntimeError("OpenAI session.update not confirmed")

        self.logger.info(
            f"OpenAI session ready voice={self.voice} vad={OPENAI_VAD_TYPE} "
            f"create_response=true interrupt_response=true"
        )

        # Optional greeting so the caller hears the agent immediately.
        if greet:
            await self._safe_send(
                json.dumps(
                    {
                        "type": "response.create",
                        "response": {
                            "instructions": (
                                "Greet the caller briefly, introduce yourself, and ask how you can help."
                            )
                        },
                    }
                )
            )

    async def _safe_send(self, message: str):
        async with self._lock:
            if not (self.ws and self.running and is_websocket_open(self.ws)):
                return
            try:
                if DEBUG == "true":
                    try:
                        self.logger.debug(f"→ OpenAI {json.loads(message).get('type')}")
                    except json.JSONDecodeError:
                        pass
                await self.ws.send(message)
            except Exception as exc:
                self.logger.error(f"_safe_send failed: {exc}")
                raise

    async def send_audio(self, pcmu_8k: bytes):
        if not self.running or not is_websocket_open(self.ws):
            return
        encoded = base64.b64encode(pcmu_8k).decode("utf-8")
        await self._safe_send(
            json.dumps({"type": "input_audio_buffer.append", "audio": encoded})
        )

    async def cancel_response(self):
        """Cancel active model speech when the caller barges in."""
        try:
            await self._safe_send(json.dumps({"type": "response.cancel"}))
        except Exception as exc:
            self.logger.debug(f"response.cancel ignored: {exc}")

    async def start_receiving(self, on_audio_callback: Callable[[bytes], None]):
        if not self.running or not is_websocket_open(self.ws):
            self.logger.warning("Cannot start receiving — OpenAI socket not open")
            return

        async def _read_loop():
            try:
                while self.running and is_websocket_open(self.ws):
                    raw = await self.ws.recv()
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    await self._handle_event(msg, on_audio_callback)
            except websockets.exceptions.ConnectionClosed as exc:
                self.logger.info(f"OpenAI websocket closed: code={exc.code} reason={exc.reason}")
            except Exception as exc:
                self.logger.error(f"OpenAI read loop error: {exc}", exc_info=True)
            finally:
                # Keep Genesys alive; mark OpenAI leg down so audio is dropped cleanly.
                self.running = False

        self.read_task = asyncio.create_task(_read_loop())

    async def _handle_event(self, msg: dict, on_audio_callback: Callable[[bytes], None]):
        ev_type = msg.get("type", "")

        if ev_type in ("response.output_audio.delta", "response.audio.delta"):
            delta_b64 = msg.get("delta", "")
            if delta_b64:
                on_audio_callback(base64.b64decode(delta_b64))
            return

        if ev_type == "input_audio_buffer.speech_started":
            self.logger.info("Caller speech started (VAD)")
            if self.on_speech_started_callback:
                await self.on_speech_started_callback()
            # interrupt_response is enabled server-side; still cancel for safety.
            if self._response_in_progress:
                await self.cancel_response()
            return

        if ev_type == "input_audio_buffer.speech_stopped":
            # VAD with create_response=true already commits + creates the response.
            # Do NOT also send commit/response.create — that caused mid-call failures.
            self.logger.info("Caller speech stopped (VAD) — waiting for auto response")
            return

        if ev_type == "response.created":
            self._response_in_progress = True
            return

        if ev_type == "response.done":
            self._response_in_progress = False
            await self._on_response_done(msg)
            return

        if ev_type == "error":
            await self._on_error(msg)
            return

        if DEBUG == "true":
            self.logger.debug(f"← OpenAI {ev_type}")

    async def _on_response_done(self, msg: dict):
        response_obj = msg.get("response", {}) or {}
        out = response_obj.get("output", []) or response_obj.get("content", []) or []

        # Token accounting
        try:
            usage = response_obj.get("usage", {}) or {}
            input_details = usage.get("input_token_details", {}) or {}
            cached_details = input_details.get("cached_tokens_details", {}) or {}
            output_details = usage.get("output_token_details", {}) or {}
            self.cumulative_tokens["input_text_tokens"] += input_details.get("text_tokens", 0)
            self.cumulative_tokens["input_cached_text_tokens"] += cached_details.get("text_tokens", 0)
            self.cumulative_tokens["input_audio_tokens"] += input_details.get("audio_tokens", 0)
            self.cumulative_tokens["input_cached_audio_tokens"] += cached_details.get(
                "audio_tokens", 0
            )
            self.cumulative_tokens["output_text_tokens"] += output_details.get("text_tokens", 0)
            self.cumulative_tokens["output_audio_tokens"] += output_details.get("audio_tokens", 0)
        except Exception as exc:
            self.logger.debug(f"Token parse skipped: {exc}")

        meta = response_obj.get("metadata") or {}
        if meta.get("type") == "ending_analysis" and self._summary_future and not self._summary_future.done():
            self._summary_future.set_result(msg)
            return

        for item in out:
            item_type = item.get("type")
            if item_type not in ("function_call", "tool_call", "function", "tool"):
                continue
            name = item.get("name") or (item.get("function") or {}).get("name")
            call_id = item.get("call_id") or item.get("id")
            args_raw = (
                item.get("arguments")
                or item.get("input")
                or (item.get("function") or {}).get("arguments")
            )
            try:
                args = json.loads(args_raw) if isinstance(args_raw, str) else (args_raw or {})
            except json.JSONDecodeError:
                args = {}
            await self._handle_function_call(name, call_id, args)

        if self._await_disconnect_on_done and self._disconnect_context:
            ctx = self._disconnect_context
            self._await_disconnect_on_done = False
            self._disconnect_context = None
            try:
                if ctx.get("action") == "end_conversation_successfully" and self.on_end_call_request:
                    await self.on_end_call_request(ctx.get("reason", "completed"), ctx.get("info", ""))
                elif ctx.get("action") == "end_conversation_with_escalation":
                    if self.on_handoff_request:
                        await self.on_handoff_request("transfer", ctx.get("info", ""))
                    elif self.on_end_call_request:
                        await self.on_end_call_request("transfer", ctx.get("info", ""))
            except Exception as exc:
                self.logger.error(f"Disconnect callback failed: {exc}", exc_info=True)

    async def _on_error(self, msg: dict):
        err = msg.get("error") if isinstance(msg.get("error"), dict) else {}
        code = err.get("code") or msg.get("code")
        message = err.get("message") or msg.get("message") or "unknown"

        # Benign / recoverable — never tear down the call.
        if code in (
            "input_audio_buffer_commit_empty",
            "conversation_already_has_active_response",
            "response_cancel_not_active",
            "cancellation_failed",
        ):
            self.logger.debug(f"OpenAI benign error ignored: {code} — {message}")
            if code == "conversation_already_has_active_response":
                self._response_in_progress = True
            return

        self.logger.error(f"OpenAI error code={code} message={message}\n{format_json(msg)}")
        # Do not close the socket on transient errors; keep the call alive.

    async def _handle_function_call(self, name: Optional[str], call_id: Optional[str], args: dict):
        if not name or not call_id:
            return

        closing_instruction = None
        output_payload: Dict[str, Any] = {}

        if name in ("end_call", "end_conversation_successfully"):
            summary = (args or {}).get("summary") or "Caller confirmed they were finished."
            output_payload = {"result": "ok", "action": "end_conversation_successfully", "summary": summary}
            self._disconnect_context = {
                "action": "end_conversation_successfully",
                "reason": "completed",
                "info": summary,
            }
            self._await_disconnect_on_done = True
            if self.success_prompt:
                closing_instruction = f'Say exactly this to the caller: "{self.success_prompt}"'
            else:
                closing_instruction = "Confirm the request is complete and thank the caller briefly."
        elif name in ("handoff_to_human", "end_conversation_with_escalation"):
            reason = (args or {}).get("reason") or "Caller requested a human agent"
            output_payload = {"result": "ok", "action": "end_conversation_with_escalation", "reason": reason}
            self._disconnect_context = {
                "action": "end_conversation_with_escalation",
                "reason": "transfer",
                "info": reason,
            }
            self._await_disconnect_on_done = True
            if self.escalation_prompt:
                closing_instruction = f'Say exactly this to the caller: "{self.escalation_prompt}"'
            else:
                closing_instruction = "Tell the caller a live agent will take over shortly."
        else:
            output_payload = {"result": "error", "error": f"Unknown function: {name}"}

        await self._safe_send(
            json.dumps(
                {
                    "type": "conversation.item.create",
                    "item": {
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": json.dumps(output_payload),
                    },
                }
            )
        )

        if closing_instruction:
            await self._safe_send(
                json.dumps(
                    {
                        "type": "response.create",
                        "response": {
                            "conversation": "none",
                            "output_modalities": ["audio"],
                            "instructions": closing_instruction,
                            "metadata": {"type": "final_farewell"},
                        },
                    }
                )
            )

    async def request_summary(self) -> Optional[str]:
        if not self.running or not is_websocket_open(self.ws):
            return None
        loop = asyncio.get_running_loop()
        self._summary_future = loop.create_future()
        try:
            await self._safe_send(
                json.dumps(
                    {
                        "type": "response.create",
                        "response": {
                            "conversation": "none",
                            "output_modalities": ["text"],
                            "metadata": {"type": "ending_analysis"},
                            "instructions": ENDING_SUMMARY_PROMPT,
                        },
                    }
                )
            )
            data = await asyncio.wait_for(self._summary_future, timeout=10.0)
            output = (data.get("response") or {}).get("output") or []
            for item in output:
                if item.get("type") == "message":
                    for content in item.get("content") or []:
                        if content.get("type") in ("text", "output_text") and content.get("text"):
                            return content["text"].strip()
                        if content.get("type") == "output_text" and content.get("text"):
                            return content["text"].strip()
                text = item.get("text")
                if text:
                    return str(text).strip()
            return None
        except Exception as exc:
            self.logger.warning(f"Summary generation failed: {exc}")
            return None
        finally:
            self._summary_future = None

    async def close(self):
        self.running = False
        if self.read_task:
            self.read_task.cancel()
            try:
                await self.read_task
            except (asyncio.CancelledError, Exception):
                pass
            self.read_task = None
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None
        self.logger.info(f"OpenAI closed after {time.time() - self.start_time:.1f}s")
