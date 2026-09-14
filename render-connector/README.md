# MaritimeScope Open Waters aiscast Bridge v9

This Render connector keeps one server-side WebSocket connection to Open Waters aiscast and forwards normalized vessel updates to the InfinityFree PHP site through `vessel-sync.php`. It supports both anonymous Open Waters access and an optional personal token.

## Render settings
- Root Directory: `render-connector`
- Build Command: `npm install`
- Start Command: `node server.js`
- Plan: Free

Recommended environment:
- `AIS_PROVIDER=openwaters`
- `OPENWATERS_API_KEY=` optional; leave blank for anonymous access
- `APP_URL=https://your-infinityfree-domain`
- `AIS_INGEST_KEY=...` matching PHP
- `AIS_INGEST_PATH=/vessel-sync.php`
- `AIS_BOXES=[[[5,115],[10,120]]]` for the safe test area
- `ALLOW_GLOBAL_BOXES=false` during testing

## Authentication behavior
Open Waters v1 supports anonymous reads. v8 therefore does **not** send an empty `key` parameter. If `OPENWATERS_API_KEY` is present, v8 uses that key. If Open Waters rejects the token as invalid, v8 automatically retries anonymously instead of getting stuck in a token-rejection loop.

## Reconnect behavior
The connector uses exponential backoff with jitter and one outbound connection. Open Waters has documented per-address connection limits, so avoid running multiple Render services for the same bridge. `snapshot:true` is requested on each subscription so the cache can rebuild after reconnects.

## Diagnostics
`/health` shows provider, connection state, message count, cache size, ingest state, reconnect state and the last close/error. `/diagnostics` additionally reports whether Open Waters is operating in token or anonymous mode and whether it had to fall back from a rejected token. Secrets are never returned.

## InfinityFree
The PHP endpoint is `vessel-sync.php`. The bridge sends small batches with retries and `Connection: close`. Failed batches remain queued for the next interval.
