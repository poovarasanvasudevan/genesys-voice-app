import inspect
import json
import re
from typing import Any, Optional

import websockets

from config import LANGUAGE_SYSTEM_PROMPT, MASTER_SYSTEM_PROMPT, logger


def is_websocket_open(ws) -> bool:
    if ws is None:
        return False
    try:
        from websockets.protocol import State

        if hasattr(ws, "state"):
            return ws.state == State.OPEN
    except (ImportError, AttributeError):
        pass
    if hasattr(ws, "open"):
        return bool(ws.open)
    return False


def get_websocket_connect_kwargs(url: str, headers: dict, **other_kwargs) -> dict:
    kwargs: dict[str, Any] = {"uri": url}
    kwargs.update(other_kwargs)
    try:
        param_names = list(inspect.signature(websockets.connect).parameters.keys())
        if "extra_headers" in param_names:
            kwargs["extra_headers"] = headers
        elif "additional_headers" in param_names:
            kwargs["additional_headers"] = headers
        else:
            kwargs["extra_headers"] = headers
    except Exception as exc:
        logger.warning(f"websockets header param detect failed: {exc}")
        kwargs["extra_headers"] = headers
    return kwargs


def format_json(obj: Any) -> str:
    return json.dumps(obj, indent=2)


def create_final_system_prompt(
    admin_prompt: Optional[str],
    language: Optional[str] = None,
    customer_data: Optional[str] = None,
    agent_name: Optional[str] = None,
    company_name: Optional[str] = None,
) -> str:
    base_prompt = (
        LANGUAGE_SYSTEM_PROMPT.format(language=language) if language else MASTER_SYSTEM_PROMPT
    )
    admin_prompt = admin_prompt or "You are a helpful phone assistant."

    if agent_name:
        admin_prompt = admin_prompt.replace("[AGENT_NAME]", agent_name)
    if company_name:
        admin_prompt = admin_prompt.replace("[COMPANY_NAME]", company_name)
        admin_prompt = admin_prompt.replace("Our Company", company_name)

    customer_instructions = ""
    if customer_data:
        try:
            data_dict = {}
            for pair in customer_data.split(";"):
                pair = pair.strip()
                if ":" in pair:
                    key, value = pair.split(":", 1)
                    data_dict[key.strip()] = value.strip()
            if data_dict:
                lines = "\n".join(f"{k}: {v}" for k, v in data_dict.items())
                customer_instructions = (
                    "\n\n[CUSTOMER DATA]\n"
                    f"{lines}\n"
                    "Use this only when relevant to personalize the call."
                )
        except Exception as exc:
            logger.warning(f"Error parsing customer data: {exc}")

    return f"""[TIER 1 - MASTER INSTRUCTIONS]
{base_prompt}

[TIER 2 - ADMIN INSTRUCTIONS]
{admin_prompt}{customer_instructions}

[TOOL USAGE]
- Call end_conversation_successfully only after the caller clearly confirms they are finished.
- Call end_conversation_with_escalation only when the caller asks for a human or the task cannot continue.
- Keep talking and listening until one of those tools is appropriate. Never hang up on silence alone.
"""


def parse_iso8601_duration(duration_str: str) -> float:
    match = re.match(
        r"P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?",
        duration_str,
    )
    if not match:
        raise ValueError(f"Invalid ISO 8601 duration format: {duration_str}")
    days, hours, minutes, seconds = match.groups()
    total = 0.0
    if days:
        total += int(days) * 86400
    if hours:
        total += int(hours) * 3600
    if minutes:
        total += int(minutes) * 60
    if seconds:
        total += float(seconds)
    return total
