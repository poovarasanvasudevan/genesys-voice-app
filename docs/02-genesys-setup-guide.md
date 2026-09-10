# Genesys Cloud Setup Guide For This Project

This guide maps Genesys settings to what this repository expects.

## 1) Prepare This Service

1. Copy `.env.example` to `.env`
2. Set:
   - `GENESYS_API_KEY` (must match integration credential API key)
   - `OPENAI_API_KEY`
3. Start server:
   - `python main.py`

Default listener:

- `ws://0.0.0.0:8080/audiohook` (local bind)

For Genesys Cloud production integration, expose via TLS endpoint:

- `wss://<public-domain>/audiohook`

## 2) Configure Genesys AudioHook/Audio Connector

In Genesys integration setup:

- Base URI / Connection URI: `wss://<public-domain>`
- Path used by this app: `/audiohook`
- Credential API key: same value as `.env` `GENESYS_API_KEY`

Notes:

- Genesys requires secure WebSocket (`wss://`).
- This app validates required AudioHook headers and rejects invalid requests.

## 3) Architect Flow Behavior

In Architect, use the relevant audio action for your scenario:

- Audio Monitoring action (for AudioHook Monitor based flows)
- Call Audio Connector action (for Audio Connector based flows)

Then pass optional input variables from Architect to personalize behavior:

- prompt, model, voice, language, customer context, closing prompts

## 4) How Open/Media Negotiation Works

When Genesys sends `open`, this server:

- checks available `media`
- accepts only `PCMU` at `8000 Hz`
- responds with `opened` and selected media

If expected media is missing, it disconnects with error info.

## 5) Keepalive, Errors, and Recovery

- Genesys `ping` messages are answered with `pong`.
- On Genesys `429` rate-limit errors, app performs controlled backoff and resumes.
- Session is intentionally kept alive during recoverable throttling.

## 6) Barge-In (Caller Interrupts Assistant)

When caller speech starts mid-assistant output:

- local audio queue is cleared
- `barge_in` event is sent to Genesys
- active OpenAI response is canceled

This prevents overlapping speech and improves call quality.

## 7) Disconnect and Reporting

On close/disconnect:

- app generates a short AI conversation summary
- output variables are returned to Genesys
- escalation status/reason and token usage are included

## 8) Common Troubleshooting

- **401 Unauthorized**: API key mismatch between Genesys and `.env`.
- **400 Missing headers**: request did not follow AudioHook protocol.
- **No audio heard**: check media format/rate (`PCMU`, `8000`) and TLS exposure.
- **Unexpected hangup**: verify call-control prompt and function-call behavior.
- **Load balancer probe errors in logs**: some probe errors are benign and filtered.

