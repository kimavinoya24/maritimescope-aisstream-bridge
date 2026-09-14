const express = require('express');
const https = require('https');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT || 10000);
const AIS_KEY = String(process.env.AISSTREAM_API_KEY || '').trim();
const BRIDGE_TOKEN = String(process.env.BRIDGE_TOKEN || '').trim();
const APP_URL = String(process.env.APP_URL || '').replace(/\/$/, '');
const INGEST_KEY = String(process.env.AIS_INGEST_KEY || '').trim();
const INGEST_PATH = String(process.env.AIS_INGEST_PATH || '/aisfeed.php');
const MAX_CACHE = 25000;
const cache = new Map();
const pending = new Map();
let socket = null;
let reconnectTimer = null;
let reconnectDelay = 2000;
let lastMessageAt = null;
let connectedAt = null;
let subscriptionConfirmed = false;
let totalMessages = 0;
let lastError = '';

function parseBoxes() {
  const fallback = [
    [[70,-180],[-70,-60]],
    [[70,-60],[-70,20]],
    [[70,20],[-70,100]],
    [[70,100],[-70,180]]
  ];
  try {
    const v = JSON.parse(process.env.AIS_BOXES || JSON.stringify(fallback));
    if (Array.isArray(v) && v.length) return v;
  } catch (_) {}
  return fallback;
}
const BOXES = parseBoxes();

function cleanString(v, max=255) {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s ? s.slice(0,max) : null;
}
function num(v) { return v === undefined || v === null || v === '' || !Number.isFinite(Number(v)) ? null : Number(v); }
function mmsiOf(d) { return String(d?.MetaData?.MMSI ?? d?.MetaData?.UserID ?? d?.Message?.MMSI ?? '').replace(/\D/g,''); }
function messageObject(d) { return d?.Message?.[d?.MessageType] || {}; }
function etaText(eta) {
  if (!eta) return null;
  if (typeof eta === 'string') return eta;
  if (typeof eta === 'object') {
    const m = eta.Month ?? eta.month, day = eta.Day ?? eta.day, h = eta.Hour ?? eta.hour, min = eta.Minute ?? eta.minute;
    if ([m,day,h,min].some(x => x !== undefined && x !== null)) return `${m ?? '--'}-${day ?? '--'} ${h ?? '--'}:${min ?? '--'}`;
  }
  return String(eta);
}
function normalize(d) {
  const type = String(d?.MessageType || '');
  const o = messageObject(d);
  const meta = d?.MetaData || {};
  const mmsi = mmsiOf(d);
  if (!/^\d{9}$/.test(mmsi)) return null;
  let existing = cache.get(mmsi) || {mmsi};
  const lat = num(meta.Latitude ?? o.Latitude ?? existing.lat);
  const lon = num(meta.Longitude ?? o.Longitude ?? existing.lon);
  const shipName = cleanString(meta.ShipName ?? o.Name ?? o.ShipName ?? existing.ship_name,120);
  const imo = cleanString(o.ImoNumber ?? o.IMO ?? existing.imo,30);
  const callsign = cleanString(o.CallSign ?? o.Callsign ?? existing.callsign,30);
  const destination = cleanString(o.Destination ?? existing.destination,120);
  const eta = etaText(o.Eta ?? existing.eta);
  const v = {
    ...existing,
    mmsi,
    ship_name: shipName,
    imo,
    callsign,
    ship_type: num(o.Type ?? o.ShipType ?? existing.ship_type),
    lat, lon,
    sog: num(o.Sog ?? existing.sog),
    cog: num(o.Cog ?? existing.cog),
    heading: num(o.TrueHeading ?? o.Heading ?? existing.heading),
    nav_status: cleanString(o.NavigationalStatus ?? existing.nav_status,80),
    destination,
    eta,
    draught: num(o.Draught ?? existing.draught),
    last_seen: new Date().toISOString(),
    source: 'AISStream'
  };
  cache.set(mmsi, v);
  if (cache.size > MAX_CACHE) {
    const oldest = cache.keys().next().value;
    if (oldest) cache.delete(oldest);
  }
  pending.set(mmsi, v);
  return v;
}
function inBbox(v,b) {
  if (!b || v.lat === null || v.lon === null) return true;
  const south=Number(b.south), north=Number(b.north), west=Number(b.west), east=Number(b.east);
  if (![south,north,west,east].every(Number.isFinite)) return true;
  if (v.lat < Math.min(south,north) || v.lat > Math.max(south,north)) return false;
  if (west <= east) return v.lon >= west && v.lon <= east;
  return v.lon >= west || v.lon <= east;
}
function matches(v,q) {
  if (!q) return true;
  const x = q.toLowerCase();
  return [v.mmsi,v.ship_name,v.imo,v.callsign,v.destination].some(z => String(z ?? '').toLowerCase().includes(x));
}
function authorized(req,res) {
  if (!BRIDGE_TOKEN) return true;
  const got = String(req.get('x-bridge-token') || req.query.token || '');
  if (!got || got !== BRIDGE_TOKEN) { res.status(403).json({ok:false,error:'forbidden'}); return false; }
  return true;
}
function postJson(urlString, body, headers={}) {
  return new Promise((resolve,reject)=>{
    let u; try { u = new URL(urlString); } catch(e) { return reject(e); }
    const data = Buffer.from(JSON.stringify(body));
    const req = https.request({hostname:u.hostname,port:443,path:u.pathname+u.search,method:'POST',headers:{'Content-Type':'application/json','Content-Length':data.length,...headers}}, r=>{
      let out=''; r.on('data',c=>out+=c); r.on('end',()=>resolve({status:r.statusCode,body:out}));
    });
    req.on('error',reject); req.setTimeout(15000,()=>req.destroy(new Error('ingest timeout'))); req.write(data); req.end();
  });
}
async function flushPending() {
  if (!APP_URL || !INGEST_KEY || pending.size === 0) return;
  const rows = Array.from(pending.values());
  pending.clear();
  try {
    const r = await postJson(APP_URL + INGEST_PATH, {vessels: rows}, {'X-AIS-Ingest-Key':INGEST_KEY});
    if (r.status < 200 || r.status >= 300) lastError = `InfinityFree ingest HTTP ${r.status}`;
  } catch(e) { lastError = `InfinityFree ingest: ${e.message}`; }
}
setInterval(flushPending, 2000);

function connectAIS() {
  if (!AIS_KEY) { lastError='AISSTREAM_API_KEY is not configured'; return; }
  if (socket && [WebSocket.OPEN,WebSocket.CONNECTING].includes(socket.readyState)) return;
  subscriptionConfirmed=false;
  try {
    socket = new WebSocket('wss://stream.aisstream.io/v0/stream', {perMessageDeflate:true});
    socket.on('open', ()=>{
      connectedAt=new Date().toISOString(); lastError='';
      socket.send(JSON.stringify({APIKey:AIS_KEY,BoundingBoxes:BOXES,FilterMessageTypes:['PositionReport','ShipStaticData','StaticDataReport','StandardClassBPositionReport','ExtendedClassBPositionReport']}));
    });
    socket.on('message', data=>{
      lastMessageAt=new Date().toISOString(); totalMessages++;
      try {
        const d=JSON.parse(data.toString());
        if (d.MessageType === 'SubscriptionConfirmation') { subscriptionConfirmed=true; return; }
        normalize(d);
      } catch(e) { lastError='AIS JSON: '+e.message; }
    });
    socket.on('error', e=>{ lastError='AIS WebSocket: '+e.message; });
    socket.on('close', ()=>{
      socket=null;
      if (reconnectTimer) return;
      reconnectTimer=setTimeout(()=>{reconnectTimer=null; reconnectDelay=Math.min(reconnectDelay*2,60000); connectAIS();}, reconnectDelay);
    });
    reconnectDelay=2000;
  } catch(e) { lastError=e.message; }
}
connectAIS();

const app=express();
app.disable('x-powered-by'); app.use(express.json({limit:'2mb'}));
app.get('/',(req,res)=>res.json({ok:true,name:'MaritimeScope AISStream Bridge',service:'AISStream',endpoints:['/health','/vessels','/vessel','/search']}));
app.get('/health',(req,res)=>res.json({ok:true,aisConnected:!!socket&&socket.readyState===WebSocket.OPEN,subscriptionConfirmed,lastMessageAt,connectedAt,totalMessages,cacheSize:cache.size,lastError}));
app.get('/vessels',(req,res)=>{
  if(!authorized(req,res)) return;
  const q=cleanString(req.query.q,120)||''; const limit=Math.max(1,Math.min(500,Number(req.query.limit)||250));
  const rows=Array.from(cache.values()).filter(v=>matches(v,q)&&inBbox(v,req.query)).sort((a,b)=>String(b.last_seen).localeCompare(String(a.last_seen))).slice(0,limit);
  res.json({ok:true,source:'AISStream via MaritimeScope bridge',vessels:rows,cacheSize:cache.size});
});
app.get('/search',(req,res)=>{
  if(!authorized(req,res)) return;
  const q=cleanString(req.query.q,120)||''; const limit=Math.max(1,Math.min(50,Number(req.query.limit)||10));
  const rows=Array.from(cache.values()).filter(v=>matches(v,q)).sort((a,b)=>String(b.last_seen).localeCompare(String(a.last_seen))).slice(0,limit);
  res.json({ok:true,vessels:rows});
});
app.get('/vessel',(req,res)=>{
  if(!authorized(req,res)) return;
  const m=String(req.query.mmsi||'').replace(/\D/g,''); const v=cache.get(m);
  if(!/^\d{9}$/.test(m) || !v) return res.status(404).json({ok:false,error:'Vessel not currently in bridge cache'});
  res.json({ok:true,vessel:v});
});
app.listen(PORT,'0.0.0.0',()=>console.log(`MaritimeScope AIS bridge listening on ${PORT}`));
