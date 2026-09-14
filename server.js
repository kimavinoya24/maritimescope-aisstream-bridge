const express = require('express');
const https = require('https');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT || 10000);
const AIS_KEY = String(process.env.AISSTREAM_API_KEY || '').trim();
const BRIDGE_TOKEN = String(process.env.BRIDGE_TOKEN || '').trim();
const APP_URL = String(process.env.APP_URL || '').replace(/\/$/, '');
const INGEST_KEY = String(process.env.AIS_INGEST_KEY || '').trim();
const INGEST_PATH = String(process.env.AIS_INGEST_PATH || '/aisfeed.php');
const MAX_CACHE = Number(process.env.MAX_CACHE || 10000);

// Start with one manageable Asia-Pacific test region. Set AIS_BOXES in Render
// later to expand coverage after the stream is confirmed working.
const DEFAULT_BOXES = [[[0,100],[30,150]]];

let socket = null;
let reconnectTimer = null;
let reconnectAttempt = 0;
let lastMessageAt = null;
let connectedAt = null;
let disconnectedAt = null;
let subscriptionConfirmed = false;
let compressionEnabled = null;
let totalMessages = 0;
let totalVessels = 0;
let lastError = '';
let lastClose = null;
let nextReconnectAt = null;
let pendingFlushTimer = null;

const cache = new Map();
const pending = new Map();

function parseBoxes() {
  try {
    const value = JSON.parse(process.env.AIS_BOXES || JSON.stringify(DEFAULT_BOXES));
    if (Array.isArray(value) && value.length) return value;
  } catch (e) {
    lastError = 'Invalid AIS_BOXES; using Asia-Pacific default';
  }
  return DEFAULT_BOXES;
}
const BOXES = parseBoxes();

function cleanString(v, max = 255) {
  if (v === undefined || v === null) return null;
  const s = String(v).replace(/\s+/g, ' ').trim();
  return s ? s.slice(0, max) : null;
}
function num(v) {
  if (v === undefined || v === null || v === '' || !Number.isFinite(Number(v))) return null;
  return Number(v);
}
function messageObject(d) { return d?.Message?.[d?.MessageType] || {}; }
function mmsiOf(d) {
  const raw = d?.MetaData?.MMSI ?? d?.MetaData?.UserID ?? d?.Message?.MMSI ?? messageObject(d)?.UserID ?? '';
  return String(raw).replace(/\D/g, '');
}
function etaText(eta) {
  if (!eta) return null;
  if (typeof eta === 'string') return eta;
  if (typeof eta === 'object') {
    const m = eta.Month ?? eta.month, day = eta.Day ?? eta.day;
    const h = eta.Hour ?? eta.hour, min = eta.Minute ?? eta.minute;
    if ([m, day, h, min].some(x => x !== undefined && x !== null)) {
      return `${m ?? '--'}-${day ?? '--'} ${h ?? '--'}:${min ?? '--'}`;
    }
  }
  return String(eta);
}

function normalize(d) {
  const type = String(d?.MessageType || '');
  const o = messageObject(d);
  const meta = d?.MetaData || {};
  const mmsi = mmsiOf(d);
  if (!/^\d{9}$/.test(mmsi)) return null;

  const existing = cache.get(mmsi) || { mmsi };
  const lat = num(meta.Latitude ?? o.Latitude ?? existing.lat);
  const lon = num(meta.Longitude ?? o.Longitude ?? existing.lon);
  const vessel = {
    ...existing,
    mmsi,
    ship_name: cleanString(meta.ShipName ?? o.Name ?? o.ShipName ?? existing.ship_name, 120),
    imo: cleanString(o.ImoNumber ?? o.IMO ?? existing.imo, 30),
    callsign: cleanString(o.CallSign ?? o.Callsign ?? existing.callsign, 30),
    ship_type: num(o.Type ?? o.ShipType ?? existing.ship_type),
    lat,
    lon,
    sog: num(o.Sog ?? existing.sog),
    cog: num(o.Cog ?? existing.cog),
    heading: num(o.TrueHeading ?? o.Heading ?? existing.heading),
    nav_status: cleanString(o.NavigationalStatus ?? existing.nav_status, 80),
    destination: cleanString(o.Destination ?? existing.destination, 120),
    eta: etaText(o.Eta ?? existing.eta),
    draught: num(o.Draught ?? existing.draught),
    last_seen: new Date().toISOString(),
    source: 'AISStream',
    message_type: type
  };

  cache.set(mmsi, vessel);
  totalVessels = cache.size;
  pending.set(mmsi, vessel);

  if (cache.size > MAX_CACHE) {
    const oldest = cache.keys().next().value;
    if (oldest) cache.delete(oldest);
  }
  return vessel;
}

function authorized(req, res) {
  if (!BRIDGE_TOKEN) return true;
  const supplied = String(req.get('x-bridge-token') || req.query.token || '');
  if (supplied !== BRIDGE_TOKEN) {
    res.status(403).json({ ok: false, error: 'forbidden' });
    return false;
  }
  return true;
}

function postJson(urlString, body, headers = {}) {
  return new Promise((resolve, reject) => {
    let u;
    try { u = new URL(urlString); } catch (e) { reject(e); return; }
    const payload = Buffer.from(JSON.stringify(body));
    const req = https.request({
      hostname: u.hostname,
      port: u.port || 443,
      path: u.pathname + u.search,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': payload.length, ...headers }
    }, res => {
      let out = '';
      res.on('data', c => out += c);
      res.on('end', () => resolve({ status: res.statusCode || 0, body: out }));
    });
    req.on('error', reject);
    req.setTimeout(15000, () => req.destroy(new Error('ingest timeout')));
    req.write(payload);
    req.end();
  });
}

async function flushPending() {
  if (!APP_URL || !INGEST_KEY || pending.size === 0) return;
  const rows = Array.from(pending.values());
  pending.clear();
  try {
    const result = await postJson(APP_URL + INGEST_PATH, { vessels: rows }, { 'X-AIS-Ingest-Key': INGEST_KEY });
    if (result.status < 200 || result.status >= 300) {
      lastError = `InfinityFree ingest HTTP ${result.status}`;
      for (const row of rows) pending.set(row.mmsi, row);
    }
  } catch (e) {
    lastError = `InfinityFree ingest: ${e.message}`;
    for (const row of rows) pending.set(row.mmsi, row);
  }
}
setInterval(flushPending, 3000);

function backoffDelay(attempt) {
  // 429 deserves a longer pause. Jitter prevents synchronized reconnect storms.
  const base = Math.min(15 * 60 * 1000, 15000 * Math.pow(2, Math.min(attempt, 6)));
  return Math.round(base * (0.75 + Math.random() * 0.5));
}

function scheduleReconnect(reason = '') {
  if (reconnectTimer) return;
  reconnectAttempt += 1;
  const delay = backoffDelay(reconnectAttempt);
  nextReconnectAt = new Date(Date.now() + delay).toISOString();
  if (reason) lastError = reason;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    nextReconnectAt = null;
    connectAIS();
  }, delay);
}

function connectAIS() {
  if (!AIS_KEY) {
    lastError = 'AISSTREAM_API_KEY is not configured';
    return;
  }
  if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return;
  subscriptionConfirmed = false;
  compressionEnabled = null;
  disconnectedAt = null;

  try {
    const ws = new WebSocket('wss://stream.aisstream.io/v0/stream', {
      perMessageDeflate: true,
      handshakeTimeout: 15000,
      maxPayload: 10 * 1024 * 1024
    });
    socket = ws;

    ws.on('open', () => {
      connectedAt = new Date().toISOString();
      lastError = '';
      const subscription = {
        APIKey: AIS_KEY,
        BoundingBoxes: BOXES,
        FilterMessageTypes: ['PositionReport', 'ShipStaticData', 'StaticDataReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport']
      };
      // AISStream requires a complete subscription within 3 seconds.
      ws.send(JSON.stringify(subscription));
    });

    ws.on('message', data => {
      lastMessageAt = new Date().toISOString();
      totalMessages += 1;
      try {
        // ws may deliver Buffer for binary frames; Buffer.toString() decodes UTF-8.
        const d = JSON.parse(Buffer.isBuffer(data) ? data.toString('utf8') : String(data));
        if (d.MessageType === 'SubscriptionConfirmation') {
          subscriptionConfirmed = true;
          compressionEnabled = d?.Message?.CompressionEnabled ?? null;
          reconnectAttempt = 0;
          lastError = '';
          return;
        }
        normalize(d);
      } catch (e) {
        lastError = 'AIS JSON: ' + e.message;
      }
    });

    ws.on('error', e => {
      const msg = e && e.message ? e.message : String(e);
      lastError = 'AIS WebSocket: ' + msg;
      // A 429 is returned during the HTTP handshake and is followed by close.
      // Do not reconnect immediately; scheduleReconnect uses long exponential backoff.
    });

    ws.on('close', (code, reasonBuffer) => {
      const reason = Buffer.isBuffer(reasonBuffer) ? reasonBuffer.toString('utf8') : String(reasonBuffer || '');
      lastClose = { code, reason, at: new Date().toISOString() };
      disconnectedAt = lastClose.at;
      socket = null;
      if (!subscriptionConfirmed && /429/.test(lastError)) {
        scheduleReconnect('AIS WebSocket: HTTP 429 rate/connection limit; backing off before retry');
      } else {
        scheduleReconnect(lastError || `AIS WebSocket closed (${code})`);
      }
    });
  } catch (e) {
    socket = null;
    lastError = 'AIS WebSocket: ' + e.message;
    scheduleReconnect(lastError);
  }
}

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '2mb' }));

app.get('/', (req, res) => res.json({
  ok: true,
  name: 'MaritimeScope AISStream Bridge',
  service: 'AISStream',
  mode: 'server-side WebSocket bridge',
  boxes: BOXES,
  endpoints: ['/health', '/vessels', '/vessel', '/search']
}));

app.get('/health', (req, res) => res.json({
  ok: true,
  aisConnected: !!socket && socket.readyState === WebSocket.OPEN,
  subscriptionConfirmed,
  compressionEnabled,
  lastMessageAt,
  connectedAt,
  disconnectedAt,
  totalMessages,
  cacheSize: cache.size,
  reconnectAttempt,
  nextReconnectAt,
  lastClose,
  lastError,
  boxes: BOXES
}));

function matches(v, q) {
  if (!q) return true;
  const x = q.toLowerCase();
  return [v.mmsi, v.ship_name, v.imo, v.callsign, v.destination].some(z => String(z ?? '').toLowerCase().includes(x));
}

function inBbox(v, b) {
  if (!b || v.lat === null || v.lon === null) return true;
  const south = Number(b.south), north = Number(b.north), west = Number(b.west), east = Number(b.east);
  if (![south, north, west, east].every(Number.isFinite)) return true;
  if (v.lat < Math.min(south, north) || v.lat > Math.max(south, north)) return false;
  if (west <= east) return v.lon >= west && v.lon <= east;
  return v.lon >= west || v.lon <= east;
}

app.get('/vessels', (req, res) => {
  if (!authorized(req, res)) return;
  const q = cleanString(req.query.q, 120) || '';
  const limit = Math.max(1, Math.min(500, Number(req.query.limit) || 250));
  const rows = Array.from(cache.values())
    .filter(v => matches(v, q) && inBbox(v, req.query))
    .sort((a, b) => String(b.last_seen).localeCompare(String(a.last_seen)))
    .slice(0, limit);
  res.json({ ok: true, source: 'AISStream via MaritimeScope bridge', vessels: rows, cacheSize: cache.size });
});

app.get('/search', (req, res) => {
  if (!authorized(req, res)) return;
  const q = cleanString(req.query.q, 120) || '';
  const limit = Math.max(1, Math.min(50, Number(req.query.limit) || 10));
  const rows = Array.from(cache.values()).filter(v => matches(v, q))
    .sort((a, b) => String(b.last_seen).localeCompare(String(a.last_seen))).slice(0, limit);
  res.json({ ok: true, vessels: rows });
});

app.get('/vessel', (req, res) => {
  if (!authorized(req, res)) return;
  const mmsi = String(req.query.mmsi || '').replace(/\D/g, '');
  const vessel = cache.get(mmsi);
  if (!/^\d{9}$/.test(mmsi) || !vessel) return res.status(404).json({ ok: false, error: 'Vessel not currently in bridge cache' });
  res.json({ ok: true, vessel });
});

app.get('/stats', (req, res) => {
  if (!authorized(req, res)) return;
  res.json({ ok: true, cacheSize: cache.size, totalMessages, lastMessageAt, subscriptionConfirmed });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`MaritimeScope AIS bridge listening on ${PORT}`);
  console.log(`AIS boxes: ${JSON.stringify(BOXES)}`);
  // Delay initial connect slightly so Render can finish bringing the service up.
  setTimeout(connectAIS, 1500);
});
