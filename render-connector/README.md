# MaritimeScope Hybrid AIS Bridge v10

This Render connector combines **Open Waters aiscast** and **AISStream** at the same time. Both server-side WebSocket streams feed one shared vessel cache. Vessels are merged by MMSI, duplicate sources are combined, and the newest valid position wins.

## Render settings
- Root Directory: `render-connector`
- Build Command: `npm install`
- Start Command: `node server.js`
- Plan: Free

Recommended environment variables:
- `AIS_PROVIDER=hybrid` (informational)
- `OPENWATERS_API_KEY=` optional; blank uses Open Waters anonymous read limits
- `AISSTREAM_API_KEY=...` required for AISStream
- `BRIDGE_TOKEN=...` recommended
- `AIS_BOXES=[[[0,105],[10,115]],[[5,115],[15,125]]]` for a Southeast Asia test region
- `ALLOW_GLOBAL_BOXES=false` during initial testing
- `MAX_CACHE=5000`
- `AIS_PUSH_INGEST=false` because InfinityFree can block server-to-server POST requests

Legacy ingest variables (`APP_URL`, `AIS_INGEST_KEY`, `AIS_INGEST_PATH`, retry settings) can remain unset while pull mode is used.

## How the hybrid merge works
1. Open Waters connects to `wss://ais.openwaters.io/v1/stream`.
2. AISStream connects to `wss://stream.aisstream.io/v0/stream`.
3. Both subscribe to the same configured bounding boxes.
4. Incoming messages are normalized into the MaritimeScope vessel format.
5. MMSI is used as the primary deduplication key.
6. If both providers report the same vessel, the newest valid position is kept and both sources are recorded.
7. If one provider disconnects, the other continues supplying data.

Open Waters can run anonymously, while AISStream requires its server-side API key. Do not put either API key in the PHP website or browser code.

## Diagnostics
- `/health` shows both provider connections, message counts, cache size and subscription state.
- `/diagnostics` shows provider configuration without exposing secrets.
- `/vessels` returns the merged vessel cache.

## Important
The current default boxes are deliberately limited to Southeast Asia so the first deployment is safer and easier to diagnose. After confirming both streams work, the boxes can be expanded or rotated. Open Waters anonymous access has documented area/message limits, so do not immediately request a huge worldwide box.
