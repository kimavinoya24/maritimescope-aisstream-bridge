# MaritimeScope Hybrid AIS Bridge v10.1

This connector runs Open Waters aiscast and AISStream independently and merges vessel state by MMSI.

## Render environment

Set:

- `AIS_PROVIDER=hybrid`
- `AISSTREAM_API_KEY=<your AISStream key>`
- `OPENWATERS_API_KEY=` (blank is valid; anonymous Open Waters read is supported)
- `BRIDGE_TOKEN=<same secret configured in PHP config/config.php>`
- `AIS_BOXES=[[[0,105],[10,115]],[[5,115],[15,125]]]`
- `ALLOW_GLOBAL_BOXES=false`
- `MAX_CACHE=5000`
- `AIS_PUSH_INGEST=false`

The two providers are deliberately started 10 seconds apart. AISStream HTTP 429 responses honor `Retry-After` when supplied and otherwise use exponential backoff with jitter. A failed AISStream connection does not stop Open Waters.

## Verification

Open `/health`. The response must say `version: 10.1.0` and expose separate `providers.openwaters` and `providers.aisstream` objects.

Open `/diagnostics` for the configured-boxes and API-key flags. Do not paste the actual API key into chat.
