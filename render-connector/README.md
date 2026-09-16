# MaritimeScope — Pelyr OPEN-AIS Render Bridge v10.7

This connector uses **Pelyr OPEN-AIS as the only AIS provider** for MaritimeScope. It keeps the Pelyr WebSocket live stream connected and periodically enriches the cache through the Pelyr HTTPS API. The browser never receives the Pelyr key.

## Render environment
- `AIS_PROVIDER=pelyr`
- `PELYR_API_KEY=<your Pelyr key>`
- `BRIDGE_TOKEN=<optional shared secret>`
- `GLOBAL_STREAM=true
AIS_BOXES=[[[5,115],[10,120]]]`
- `ALLOW_GLOBAL_BOXES=true`
- `MAX_CACHE=5000`
- `AIS_PUSH_INGEST=false`
- `PELYR_START_DELAY_MS=5000`
- `PELYR_429_COOLDOWN_MS=120000`
- `PELYR_API_REFRESH_MS=90000`

## Data flow
Pelyr OPEN-AIS WebSocket → Render bridge cache → MaritimeScope PHP/API

Pelyr HTTPS `/v1/vessels/{mmsi}` → on-demand rich-detail lookup; global positions come from the WebSocket stream

## Health
`/health` reports Pelyr connection, subscription, message count, heartbeat, API refresh status and errors.

## WebSocket authentication
The connector sends the Pelyr key server-side as `Authorization: Bearer <key>` during the WebSocket handshake. The key is never sent to the browser or included in the WebSocket URL.


## v10.9 storage change
AIS vessel positions/details are no longer pushed to or read from MySQL. The Render bridge keeps only its current in-memory Pelyr working set; the PHP pages read that working set over HTTPS. MySQL remains available for user accounts, saved items, recent views, and other application data.


## Global stream

Version 12 uses the native Pelyr v1 stream with no `bbox`, which Pelyr documents as worldwide. This avoids the old fixed 5°×5° box. Global coverage is sampled/uneven, and the service documents roughly one position per vessel per minute for global coverage. The bridge keeps AIS data in Render memory only; it does not write AIS positions to MySQL.
