# MaritimeScope AISStream Render Bridge v2

This service keeps the AISStream WebSocket server-side and exposes sanitized HTTPS JSON endpoints to MaritimeScope.

## Important 429 behavior

AISStream currently limits subscribed connections per account and open connections per originating IP. HTTP 429 during the WebSocket handshake is therefore treated as a connection/rate-limit condition. The bridge does NOT reconnect rapidly: it uses exponential backoff with jitter and a maximum delay of 15 minutes. A successful SubscriptionConfirmation resets the backoff.

## First deployment

Start with the Asia-Pacific box in `AIS_BOXES`:
`[[[0,100],[30,150]]]`

Only expand coverage after `/health` shows:
- `aisConnected: true`
- `subscriptionConfirmed: true`
- `totalMessages > 0`
- `cacheSize > 0`

## Render

Build: `npm install`
Start: `node server.js`
Health check: `/health`

Required environment variables:
- `AISSTREAM_API_KEY`
- `BRIDGE_TOKEN`
- `APP_URL`
- `AIS_INGEST_KEY`
- `AIS_INGEST_PATH=/aisfeed.php`
- `AIS_BOXES`

The AISStream key is never sent to the browser.
