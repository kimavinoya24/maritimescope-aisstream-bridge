# MaritimeScope AISStream Bridge v4

V3 is a deliberately conservative diagnostic build. It creates **one** outbound AISStream WebSocket connection and starts with a small Asia-Pacific test bounding box. Global boxes are ignored unless `ALLOW_GLOBAL_BOXES=true`.

## Render
Build: `npm install`
Start: `node server.js`
Plan: Free

Required environment variables:
- `AISSTREAM_API_KEY`
- `BRIDGE_TOKEN`
- `APP_URL`
- `AIS_INGEST_KEY`

Optional:
- `AIS_INGEST_PATH=/vessel-sync.php`
- `AIS_BOXES=[[[0,100],[30,150]]]`
- `ALLOW_GLOBAL_BOXES=false`
- `MAX_CACHE=5000`

## Test order
1. Deploy one instance only.
2. Open `/health`.
3. Open `/diagnostics`.
4. Do not repeatedly restart the service.
5. A successful AIS connection should show `subscriptionConfirmed: true` and `totalMessages > 0`.
6. Only after that set `ALLOW_GLOBAL_BOXES=true` and configure larger boxes if desired.

## Diagnostic fields
`/diagnostics` reports the AIS handshake HTTP status, selected response headers (including Retry-After), response body snippet, connection attempts, subscription confirmation, and sanitized API-key fingerprint. It never returns the actual API key or secrets.


## InfinityFree ingest reliability (v4)
The bridge sends small batches (20 vessels by default) with `Connection: close`, a 20-second timeout, and up to 3 sequential retries. This avoids sending a large 400–500 vessel payload to shared hosting at once and preserves newer AIS updates while an older batch is being uploaded. Optional environment variables: `AIS_INGEST_BATCH_SIZE`, `AIS_INGEST_INTERVAL_MS`, `AIS_INGEST_RETRIES`, `AIS_INGEST_RETRY_DELAY_MS`.


## v6 diagnostic ingest
The bridge reports version 7.0.0 from `/health` and `/diagnostics`. Ingest diagnostics include HTTP status, duration, request ID, response body/headers (truncated), batch number, and retry information. Failed batches remain queued for retry. InfinityFree `vessel-sync.php` writes non-secret request diagnostics to `aisfeed_debug.log`.


## v7 Open Waters mode
The connector now defaults to Open Waters aiscast using the native v1 stream. Anonymous access is intentionally limited to a small test box (5-10 N, 115-120 E) because Open Waters documents an anonymous 100 square-degree area cap. A personal token can raise limits but still has documented area/connection limits. Set `AIS_PROVIDER=aisstream` to use the old AISStream-compatible provider.
