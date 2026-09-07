# Genesys ↔ OpenAI Realtime Audio Connector

Clean rebuild of the Genesys AudioHook bridge that streams caller audio to the **OpenAI Realtime API** and plays synthesized audio back on the call.

## What this fixes vs the old project

| Problem in old connector | Fix here |
|--------------------------|----------|
| `semantic_vad` **plus** manual `commit` / `response.create` on `speech_stopped` | VAD owns turns (`create_response: true`). No manual commit on speech stop. |
| OpenAI audio deltas sent unframed to Genesys | Reassemble into fixed **1600-byte PCMU** frames |
| Genesys `429` set `running=False` and killed the recv loop | Backoff via `in_backoff` only; session stays alive |
| Barge-in did not clear outbound audio | Clear buffer + Genesys `barge_in` + `response.cancel` |
| Prompt ended calls on silence / mild frustration | End only on clear goodbye or human request |
| Gemini / MCP / Data Actions complexity | OpenAI Realtime only |

## Setup

```bash
cd genesys-voice-app
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env with GENESYS_API_KEY and OPENAI_API_KEY
python main.py
```

Genesys Audio Connector URL: `wss://<host>:8080/audiohook`  
Header: `x-api-key: <GENESYS_API_KEY>`

## Architect input variables (optional)

| Variable | Purpose |
|----------|---------|
| `AI_SYSTEM_PROMPT` | Agent instructions |
| `AI_VOICE` / `AI_MODEL` | Voice / model override |
| `LANGUAGE` | Force response language |
| `CUSTOMER_DATA` | `key:value;key:value` personalization |
| `SUCCESS_PROMPT` / `ESCALATION_PROMPT` | Exact farewell lines |

## Output variables on disconnect

`CONVERSATION_SUMMARY`, `COMPLETION_SUMMARY`, `ESCALATION_REQUIRED`, `ESCALATION_REASON`, token counters.

## Protocol notes

- Media: **PCMU @ 8000 Hz**
- Keepalive: Genesys `ping` → server `pong` (WebSocket pings disabled)
- OpenAI: `wss://api.openai.com/v1/realtime?model=...` with PCMU in/out
- Health: `GET /` (and `/health`) returns `200 OK` for platform probes

### Benign deploy logs

Load balancers sometimes open a TCP socket and close it without an HTTP request. That produces websockets `InvalidMessage` / `EOFError` traces. Those are **not** Genesys call failures and are filtered from logs. Real AudioHook traffic always sends a proper WebSocket upgrade to `/audiohook`.
