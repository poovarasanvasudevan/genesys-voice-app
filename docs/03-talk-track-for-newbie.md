# Talk Track: Explain This Project To A Newbie

Use this as speaker notes for demos or onboarding.

## 1-Minute Explanation

"This project is a bridge between Genesys Cloud phone calls and OpenAI Realtime AI.  
Genesys streams call audio to this service over WebSocket.  
The service sends that audio to OpenAI, gets AI speech back, and streams it to the caller in near real-time."

## Key Concepts To Explain

- **AudioHook protocol**: How Genesys exchanges open/close/events/audio with external services.
- **Realtime AI loop**: caller audio in -> model reasoning -> synthesized audio out.
- **Media constraints**: this implementation uses `PCMU 8k`, framed as `1600-byte` packets.
- **Turn-taking**: OpenAI VAD handles when AI should respond.
- **Barge-in**: if caller interrupts, assistant audio is canceled quickly.
- **Safe ending**: call ends only on clear completion/escalation signals.

## Suggested Whiteboard Flow

1. Caller speaks in PSTN/VoIP call.
2. Genesys forks/streams audio to AudioHook endpoint.
3. `main.py` validates request and starts session.
4. `audio_hook_server.py` relays audio and control messages.
5. `openai_client.py` runs Realtime session with instructions/tools.
6. AI audio returns, is framed/rate-limited, and sent back to Genesys.
7. Session ends with summary + outcome variables.

## What Is Configurable Per Call

From Architect input variables:

- assistant system prompt
- model and voice
- response language
- customer personalization data
- exact farewell message for success/escalation

## Practical Demo Script (5-7 minutes)

1. Start service locally and show startup logs.
2. Place a test call from a flow configured to stream audio.
3. Ask a simple question and confirm voice response.
4. Interrupt assistant while it is speaking (show barge-in behavior).
5. Ask for a human transfer phrase (show escalation path).
6. End call politely and inspect output variables/summary.

## Common Questions Newbies Ask

- **Why not connect Genesys directly to OpenAI?**  
  Because this bridge handles protocol translation, framing, reliability, and call controls.

- **Why fixed 1600-byte frames?**  
  Genesys-side streaming behavior is most stable with consistent frame sizing/rate.

- **Why use VAD with create_response?**  
  It avoids double-trigger bugs and simplifies turn management.

- **Where is business logic?**  
  In prompts, per-call input variables, and post-call output handling in Architect.

## Short "What To Learn Next" Path

1. Understand AudioHook message lifecycle.
2. Learn Architect variable mapping and flow actions.
3. Study OpenAI Realtime event types.
4. Practice debugging with real call traces/logs.

