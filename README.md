# MaritimeScope AISStream Bridge

This small Node.js service is the server-side connector between MaritimeScope on XAMPP/InfinityFree and AISStream.

## Render settings
- Runtime: Node
- Build command: `npm install`
- Start command: `node server.js`
- Plan: Free
- Health check: `/health`

## Environment variables
- `AISSTREAM_API_KEY`: your AISStream API key
- `BRIDGE_TOKEN`: secret shared with the PHP site
- `APP_URL`: your MaritimeScope HTTPS root URL, for example `https://example.rf.gd`
- `AIS_INGEST_KEY`: second secret shared with `aisfeed.php`
- `AIS_INGEST_PATH`: `/aisfeed.php`
- `AIS_BOXES`: JSON array of AISStream bounding boxes

Do not put the AISStream API key in the PHP website or browser JavaScript.
