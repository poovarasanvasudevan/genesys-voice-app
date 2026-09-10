# Project Overview (Genesys + OpenAI Realtime)

## What This Project Is

This service is a real-time audio bridge between:

- **Genesys Cloud AudioHook** (phone call audio stream)
- **OpenAI Realtime API** (AI conversation engine)

It receives caller audio from Genesys, sends it to OpenAI, receives AI-generated audio back, and streams that audio to Genesys so the caller hears the assistant.

## Why This Exists

Genesys and OpenAI do not connect directly out-of-the-box for this use case.  
This app acts as middleware that handles:

- protocol compatibility
- authentication and validation
- media framing and rate limits
- call lifecycle (open, keepalive, close/disconnect)
- barge-in behavior and final call summary

## Core Runtime Components

- `main.py`  
  Starts the WebSocket server, validates incoming requests, and routes each call session.

- `audio_hook_server.py`  
  Implements Genesys AudioHook session behavior and bridge logic.

- `openai_client.py`  
  Connects to OpenAI Realtime and manages turn-taking, tool calls, audio events, and summaries.

- `config.py`  
  Environment settings, model defaults, VAD mode, logging, limits.

- `rate_limiter.py`  
  Prevents sending messages/audio faster than safe limits.

- `utils.py`  
  Prompt construction, duration parsing, and websocket compatibility helpers.

## End-to-End Call Flow

1. Genesys opens `wss://<host>:8080/audiohook` with required headers.
2. Server validates path, API key, and handshake headers.
3. Genesys sends `open`; server replies `opened` with accepted media (`PCMU`, `8000`).
4. Server opens OpenAI Realtime session using configured model/voice.
5. Caller audio frames flow to OpenAI.
6. OpenAI audio deltas flow back; server reassembles into fixed `1600-byte` PCMU frames.
7. Frames are streamed to Genesys at controlled rate.
8. If caller interrupts AI speech, server emits barge-in event and cancels response.
9. On close/disconnect, server requests a short AI summary and returns output variables to Genesys.

## Audio and Protocol Constraints

- Codec: **PCMU**
- Sample rate: **8000 Hz**
- Outbound frame size to Genesys: **1600 bytes** (~200 ms/frame)
- Keepalive: Genesys `ping` -> server `pong`
- App-level rate limiting for both JSON and binary frames

## Input Variables This Project Supports

These can be passed from Architect flow configuration:

- `AI_SYSTEM_PROMPT`
- `AI_VOICE` / `OPENAI_VOICE`
- `AI_MODEL` / `OPENAI_MODEL`
- `AI_TEMPERATURE` / `OPENAI_TEMPERATURE`
- `LANGUAGE`
- `CUSTOMER_DATA` (`key:value;key:value`)
- `AGENT_NAME`
- `COMPANY_NAME`
- `SUCCESS_PROMPT`
- `ESCALATION_PROMPT`

## Output Variables Returned on Disconnect

- `CONVERSATION_SUMMARY`
- `COMPLETION_SUMMARY`
- `CONVERSATION_DURATION`
- `ESCALATION_REQUIRED`
- `ESCALATION_REASON`
- token counters (`TOTAL_INPUT_*`, `TOTAL_OUTPUT_*`)

## Conversation Control Policy

The assistant can trigger function calls to:

- end call successfully
- end call with escalation to a human

Design intent:

- do **not** end call on silence alone
- end only when user confirms completion or requests human handoff

