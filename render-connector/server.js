const express = require('express');
const https = require('https');
const WebSocket = require('ws');
const crypto = require('crypto');

const PORT = Number(process.env.PORT || 10000);
const AIS_PROVIDER = String(process.env.AIS_PROVIDER || 'openwaters').trim().toLowerCase();
const OPENWATERS_API_KEY = String(process.env.OPENWATERS_API_KEY || '').trim();
const AISSTREAM_API_KEY = String(process.env.AISSTREAM_API_KEY || '').trim();
const AIS_KEY = AIS_PROVIDER === 'openwaters' ? OPENWATERS_API_KEY : AISSTREAM_API_KEY;
const BRIDGE_TOKEN = String(process.env.BRIDGE_TOKEN || '').trim();
const APP_URL = String(process.env.APP_URL || '').replace(/\/$/, '');
const INGEST_KEY = String(process.env.AIS_INGEST_KEY || '').trim();
const INGEST_PATH = String(process.env.AIS_INGEST_PATH || '/vessel-sync.php');
const MAX_CACHE = Math.max(100, Number(process.env.MAX_CACHE || 5000));
const ALLOW_GLOBAL_BOXES = String(process.env.ALLOW_GLOBAL_BOXES || 'false').toLowerCase() === 'true';
const TEST_BOXES = [[[5,115],[10,120]]];

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
let lastHandshake = null;
let nextReconnectAt = null;
let pendingFlushTimer = null;
let lastSubscriptionAt = null;
let successfulSubscriptions = 0;
let totalConnectAttempts = 0;
let lastConnectStartedAt = null;
let openWatersAnonymousFallback = false;
let openWatersAuthFallbackAt = null;
let ingestInProgress = false;
let ingestSuccesses = 0;
let ingestFailures = 0;
let ingestRowsStored = 0;
let lastIngestAt = null;
let lastIngestStatus = null;
let lastIngestError = null;
let lastIngestDetail = null;
let ingestBatchNumber = 0;
const INGEST_BATCH_SIZE = Math.max(1, Math.min(50, Number(process.env.AIS_INGEST_BATCH_SIZE || 20)));
const INGEST_INTERVAL_MS = Math.max(3000, Number(process.env.AIS_INGEST_INTERVAL_MS || 5000));
const INGEST_RETRIES = Math.max(1, Math.min(5, Number(process.env.AIS_INGEST_RETRIES || 3)));
const INGEST_RETRY_DELAY_MS = Math.max(1000, Number(process.env.AIS_INGEST_RETRY_DELAY_MS || 2500));
const INGEST_DEBUG = String(process.env.AIS_INGEST_DEBUG || 'true').toLowerCase() === 'true';

const cache = new Map();
const pending = new Map();

function parseBoxes() {
  if (!ALLOW_GLOBAL_BOXES) return TEST_BOXES;
  try {
    const value = JSON.parse(process.env.AIS_BOXES || JSON.stringify(TEST_BOXES));
    if (Array.isArray(value) && value.length) return value;
  } catch (_) {
    lastError = 'Invalid AIS_BOXES; using safe Asia-Pacific test box';
  }
  return TEST_BOXES;
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
    if ([m, day, h, min].some(x => x !== undefined && x !== null)) return `${m ?? '--'}-${day ?? '--'} ${h ?? '--'}:${min ?? '--'}`;
  }
  return String(eta);
}
function normalize(d) {
  const isOpenWaters = AIS_PROVIDER === 'openwaters';
  const type = String(d?.MessageType || d?.msg_type || '');
  const o = isOpenWaters ? d : messageObject(d);
  const meta = isOpenWaters ? d : (d?.MetaData || {});
  const mmsi = isOpenWaters ? String(d?.mmsi ?? '') : mmsiOf(d);
  if (!/^\d{9}$/.test(mmsi)) return null;
  const existing = cache.get(mmsi) || { mmsi };
  const vessel = {
    ...existing, mmsi,
    ship_name: cleanString(isOpenWaters ? (o.name ?? existing.ship_name) : (meta.ShipName ?? o.Name ?? o.ShipName ?? existing.ship_name), 120),
    imo: cleanString(isOpenWaters ? (o.imo ?? existing.imo) : (o.ImoNumber ?? o.IMO ?? existing.imo), 30),
    callsign: cleanString(isOpenWaters ? (o.callsign ?? existing.callsign) : (o.CallSign ?? o.Callsign ?? existing.callsign), 30),
    ship_type: num(isOpenWaters ? (o.type ?? existing.ship_type) : (o.Type ?? o.ShipType ?? existing.ship_type)),
    lat: num(isOpenWaters ? (o.lat ?? existing.lat) : (meta.Latitude ?? o.Latitude ?? existing.lat)),
    lon: num(isOpenWaters ? (o.lon ?? existing.lon) : (meta.Longitude ?? o.Longitude ?? existing.lon)),
    sog: num(isOpenWaters ? (o.sog ?? existing.sog) : (o.Sog ?? existing.sog)), cog: num(isOpenWaters ? (o.cog ?? existing.cog) : (o.Cog ?? existing.cog)),
    heading: num(isOpenWaters ? (o.heading ?? existing.heading) : (o.TrueHeading ?? o.Heading ?? existing.heading)),
    nav_status: cleanString(isOpenWaters ? (o.nav_status ?? existing.nav_status) : (o.NavigationalStatus ?? existing.nav_status), 80),
    destination: cleanString(isOpenWaters ? (o.destination ?? existing.destination) : (o.Destination ?? existing.destination), 120),
    eta: etaText(isOpenWaters ? (o.eta ?? existing.eta) : (o.Eta ?? existing.eta)), draught: num(isOpenWaters ? (o.draught ?? existing.draught) : (o.Draught ?? existing.draught)),
    last_seen: new Date().toISOString(), source: isOpenWaters ? 'Open Waters aiscast' : 'AISStream', message_type: type
  };
  cache.set(mmsi, vessel); totalVessels = cache.size; pending.set(mmsi, vessel);
  while (cache.size > MAX_CACHE) { const oldest = cache.keys().next().value; if (oldest) cache.delete(oldest); else break; }
  return vessel;
}
function authorized(req, res) {
  if (!BRIDGE_TOKEN) return true;
  const supplied = String(req.get('x-bridge-token') || req.query.token || '');
  if (supplied !== BRIDGE_TOKEN) { res.status(403).json({ ok:false, error:'forbidden' }); return false; }
  return true;
}
function postJson(urlString, body, headers = {}) {
  return new Promise((resolve, reject) => {
    let u; try { u = new URL(urlString); } catch (e) { reject(e); return; }
    const payload = Buffer.from(JSON.stringify(body));
    const started = Date.now();
    const requestId = crypto.randomBytes(6).toString('hex');
    const req = https.request({
      hostname:u.hostname, port:u.port||443, path:u.pathname+u.search, method:'POST',
      agent:false,
      headers:{
        'Content-Type':'application/json; charset=utf-8',
        'Content-Length':payload.length,
        'Accept':'application/json',
        'User-Agent':'MaritimeScope-AIS-Bridge/6.0',
        'Connection':'close',
        'X-AIS-Request-ID':requestId,
        ...headers
      }
    }, res => {
      let out=''; res.setEncoding('utf8'); res.on('data', c=>{ if(out.length<4096) out+=c; });
      res.on('end',()=>resolve({status:res.statusCode||0,body:out,headers:res.headers,durationMs:Date.now()-started,requestId}));
    });
    req.on('error', e => {
      e.requestId=requestId; e.durationMs=Date.now()-started; reject(e);
    });
    req.setTimeout(20000,()=>{ const e=new Error('ingest timeout'); e.requestId=requestId; e.durationMs=Date.now()-started; req.destroy(e); });
    req.end(payload);
  });
}
function sleep(ms){ return new Promise(resolve=>setTimeout(resolve,ms)); }
async function sendIngestBatch(rows) {
  let lastFailure='';
  const batchNumber=++ingestBatchNumber;
  for(let attempt=1; attempt<=INGEST_RETRIES; attempt++){
    try{
      const result=await postJson(APP_URL + INGEST_PATH, {vessels:rows}, {'X-AIS-Ingest-Key':INGEST_KEY});
      const detail={batchNumber,attempt,batchSize:rows.length,status:result.status,durationMs:result.durationMs,requestId:result.requestId,responseBody:INGEST_DEBUG?result.body.slice(0,1000):undefined,responseHeaders:INGEST_DEBUG?result.headers:undefined,endpoint:APP_URL+INGEST_PATH,at:new Date().toISOString()};
      lastIngestDetail=detail;
      if(result.status>=200 && result.status<300){
        ingestSuccesses++; ingestRowsStored += rows.length; lastIngestAt=detail.at; lastIngestStatus=result.status; lastIngestError=null;
        return true;
      }
      lastFailure=`HTTP ${result.status}${result.body ? ': '+result.body.slice(0,500) : ''}`;
    }catch(e){
      lastFailure=e?.message || String(e);
      lastIngestDetail={batchNumber,attempt,batchSize:rows.length,status:null,durationMs:e?.durationMs??null,requestId:e?.requestId??null,responseBody:null,responseHeaders:null,endpoint:APP_URL+INGEST_PATH,error:lastFailure,at:new Date().toISOString()};
    }
    if(attempt<INGEST_RETRIES) await sleep(INGEST_RETRY_DELAY_MS * attempt);
  }
  ingestFailures++; lastIngestAt=new Date().toISOString(); lastIngestStatus=null; lastIngestError=lastFailure; lastError=`InfinityFree ingest failed after ${INGEST_RETRIES} attempts: ${lastFailure}`;
  return false;
}
async function flushPending() {
  if (ingestInProgress || !APP_URL || !INGEST_KEY || pending.size === 0) return;
  ingestInProgress=true;
  try {
    const rows = Array.from(pending.values()).slice(0, INGEST_BATCH_SIZE);
    const ok = await sendIngestBatch(rows);
    for (const row of rows) {
      if (ok && pending.get(row.mmsi) === row) pending.delete(row.mmsi);
      // On failure, keep the row queued so the next interval retries it.
    }
  } finally { ingestInProgress=false; }
}
setInterval(flushPending, INGEST_INTERVAL_MS);

function backoffDelay(attempt) {
  const base = Math.min(15*60*1000, 30000 * Math.pow(2, Math.min(Math.max(attempt-1,0), 5)));
  return Math.round(base * (0.8 + Math.random()*0.4));
}
function scheduleReconnect(reason='') {
  if (reconnectTimer) return;
  reconnectAttempt += 1;
  const delay = backoffDelay(reconnectAttempt);
  nextReconnectAt = new Date(Date.now()+delay).toISOString();
  if (reason) lastError = reason;
  reconnectTimer = setTimeout(()=>{ reconnectTimer=null; nextReconnectAt=null; connectAIS(); }, delay);
}
function safeHeaderSubset(headers) {
  const out={}; for (const [k,v] of Object.entries(headers||{})) { if (/^(retry-after|server|date|content-type|connection|cf-|x-)/i.test(k)) out[k]=v; }
  return out;
}
function connectAIS() {
  if (AIS_PROVIDER === 'openwaters') return connectOpenWaters();
  if (!AIS_KEY) { lastError='AISSTREAM_API_KEY is not configured'; return; }
  if (socket && (socket.readyState===WebSocket.OPEN || socket.readyState===WebSocket.CONNECTING)) return;
  totalConnectAttempts += 1; lastConnectStartedAt = new Date().toISOString();
  subscriptionConfirmed=false; compressionEnabled=null; disconnectedAt=null;
  lastHandshake={status:null,headers:{},bodySnippet:null,at:lastConnectStartedAt};
  try {
    const ws = new WebSocket('wss://stream.aisstream.io/v0/stream', {perMessageDeflate:true, handshakeTimeout:15000, maxPayload:10*1024*1024});
    socket=ws;
    ws.on('open',()=>{
      connectedAt=new Date().toISOString(); lastSubscriptionAt=new Date().toISOString();
      const subscription={APIKey:AIS_KEY, BoundingBoxes:BOXES, FilterMessageTypes:['PositionReport','ShipStaticData','StaticDataReport','StandardClassBPositionReport','ExtendedClassBPositionReport']};
      try { ws.send(JSON.stringify(subscription)); } catch(e) { lastError='Subscription send: '+e.message; }
    });
    ws.on('unexpected-response',handleUnexpectedResponse);
    ws.on('message',data=>{
      lastMessageAt=new Date().toISOString(); totalMessages += 1;
      try { const d=JSON.parse(Buffer.isBuffer(data)?data.toString('utf8'):String(data));
        if(d.MessageType==='SubscriptionConfirmation'){ subscriptionConfirmed=true; compressionEnabled=d?.Message?.CompressionEnabled ?? null; reconnectAttempt=0; lastError=''; successfulSubscriptions += 1; return; }
        normalize(d);
      } catch(e){ lastError='AIS JSON: '+e.message; }
    });
    ws.on('error',e=>{ lastError='AIS WebSocket: '+(e?.message||String(e)); });
    ws.on('close',handleClose);
  } catch(e) { socket=null; lastError='AIS WebSocket: '+e.message; scheduleReconnect(lastError); }
}

function handleUnexpectedResponse(request,response){
  const chunks=[]; response.on('data',c=>{ if(Buffer.concat(chunks).length<4096) chunks.push(Buffer.from(c)); });
  response.on('end',()=>{
    const body=Buffer.concat(chunks).toString('utf8').slice(0,4096);
    lastHandshake={status:response.statusCode,headers:safeHeaderSubset(response.headers),bodySnippet:body,at:new Date().toISOString()};
    const retryAfter=response.headers['retry-after'];
    lastError=`AIS WebSocket handshake HTTP ${response.statusCode}${retryAfter ? `; Retry-After=${retryAfter}` : ''}`;
  });
}
function handleClose(code,reasonBuffer){
  const reason=Buffer.isBuffer(reasonBuffer)?reasonBuffer.toString('utf8'):String(reasonBuffer||'');
  lastClose={code,reason,at:new Date().toISOString()}; disconnectedAt=lastClose.at; socket=null;
  const retryAfter=lastHandshake?.headers?.['retry-after'];
  const combined=(reason+' '+(lastError||'')).trim();
  if(AIS_PROVIDER==='openwaters' && AIS_KEY && /invalid token|bad token|unauthorized|401|token/i.test(combined)){
    openWatersAnonymousFallback=true;
    openWatersAuthFallbackAt=new Date().toISOString();
    lastError='Open Waters token rejected; retrying anonymously';
  }
  const why=lastHandshake?.status===429 ? `Open Waters: HTTP 429 rate/connection limit${retryAfter ? `; Retry-After=${retryAfter}`:''}` : (lastError || `AIS WebSocket closed (${code})`);
  scheduleReconnect(why);
}
function connectOpenWaters(){
  if (socket && (socket.readyState===WebSocket.OPEN || socket.readyState===WebSocket.CONNECTING)) return;
  totalConnectAttempts += 1; lastConnectStartedAt=new Date().toISOString(); subscriptionConfirmed=false; compressionEnabled=null; disconnectedAt=null;
  lastHandshake={status:null,headers:{},bodySnippet:null,at:lastConnectStartedAt};
  const usingToken = !!AIS_KEY && !openWatersAnonymousFallback;
  const url = 'wss://ais.openwaters.io/v1/stream' + (usingToken ? `?key=${encodeURIComponent(AIS_KEY)}` : '');
  try {
    const ws=new WebSocket(url,{perMessageDeflate:true,handshakeTimeout:15000,maxPayload:10*1024*1024}); socket=ws;
    ws.on('open',()=>{
      connectedAt=new Date().toISOString(); lastSubscriptionAt=new Date().toISOString();
      const sub={type:'subscribe',bbox:BOXES.map(b=>[b[0][0],b[0][1],b[1][0],b[1][1]]),snapshot:true};
      try{ws.send(JSON.stringify(sub));}catch(e){lastError='Open Waters subscribe send: '+e.message;}
    });
    ws.on('unexpected-response',handleUnexpectedResponse);
    ws.on('message',data=>{
      try{
        const d=JSON.parse(Buffer.isBuffer(data)?data.toString('utf8'):String(data));
        if(d.type==='welcome'){ compressionEnabled=true; return; }
        if(d.type==='ack'){ subscriptionConfirmed=true; reconnectAttempt=0; lastError=''; successfulSubscriptions+=1; return; }
        if(d.type==='error'){
          lastError='Open Waters: '+String(d.error||'stream error');
          if (/token|auth|key/i.test(String(d.error||'')) && AIS_KEY) { openWatersAnonymousFallback=true; openWatersAuthFallbackAt=new Date().toISOString(); }
          return;
        }
        if(d.type==='event'){
          lastMessageAt=new Date().toISOString(); totalMessages+=1; normalize(d);
        }
      }catch(e){lastError='Open Waters JSON: '+e.message;}
    });
    ws.on('error',e=>{lastError='Open Waters WebSocket: '+(e?.message||String(e));});
    ws.on('close',handleClose);
  }catch(e){socket=null;lastError='Open Waters WebSocket: '+e.message;scheduleReconnect(lastError);}
}

const app=express(); app.disable('x-powered-by'); app.use(express.json({limit:'2mb'}));
app.get('/',(req,res)=>res.json({ok:true,version:'8.0.0',name:'MaritimeScope AIS Bridge v8',service:AIS_PROVIDER==='openwaters'?'Open Waters aiscast':'AISStream',mode:'single-connection-diagnostic-with-resilient-ingest',testMode:!ALLOW_GLOBAL_BOXES,boxes:BOXES,endpoints:['/health','/diagnostics','/vessels','/vessel','/search']}));
app.get('/health',(req,res)=>res.json({ok:true,version:'8.0.0',ingest:{inProgress:ingestInProgress,batchSize:INGEST_BATCH_SIZE,intervalMs:INGEST_INTERVAL_MS,retries:INGEST_RETRIES,pending:pending.size,successes:ingestSuccesses,failures:ingestFailures,rowsStored:ingestRowsStored,lastIngestAt,lastIngestStatus,lastIngestError,lastIngestDetail},aisConnected:!!socket&&socket.readyState===WebSocket.OPEN,subscriptionConfirmed,compressionEnabled,lastMessageAt,connectedAt,disconnectedAt,totalMessages,cacheSize:cache.size,reconnectAttempt,nextReconnectAt,lastClose,lastError,boxes:BOXES,testMode:!ALLOW_GLOBAL_BOXES}));
app.get('/diagnostics',(req,res)=>res.json({ok:true,version:'8.0.0',time:new Date().toISOString(),node:process.version,platform:process.platform,instance:{renderService:process.env.RENDER_SERVICE_NAME||null,renderInstance:process.env.RENDER_INSTANCE_ID||null},config:{provider:AIS_PROVIDER,apiKeyConfigured:!!AIS_KEY,apiKeyFingerprint:AIS_KEY?crypto.createHash('sha256').update(AIS_KEY).digest('hex').slice(0,12):null,bridgeTokenConfigured:!!BRIDGE_TOKEN,appUrlConfigured:!!APP_URL,ingestKeyConfigured:!!INGEST_KEY,allowGlobalBoxes:ALLOW_GLOBAL_BOXES,boxes:BOXES,maxCache:MAX_CACHE,ingestDebug:INGEST_DEBUG},connection:{mode:AIS_PROVIDER==='openwaters' ? (openWatersAnonymousFallback || !AIS_KEY ? 'anonymous' : 'token') : 'token',anonymousFallback:openWatersAnonymousFallback,anonymousFallbackAt:openWatersAuthFallbackAt,readyState:socket?socket.readyState:null,aisConnected:!!socket&&socket.readyState===WebSocket.OPEN,totalConnectAttempts,lastConnectStartedAt,connectedAt,disconnectedAt,subscriptionConfirmed,successfulSubscriptions,compressionEnabled,lastSubscriptionAt,reconnectAttempt,nextReconnectAt,lastClose,lastError,lastHandshake},data:{totalMessages,cacheSize:cache.size,lastMessageAt,pendingIngest:pending.size,ingest:{inProgress:ingestInProgress,batchSize:INGEST_BATCH_SIZE,intervalMs:INGEST_INTERVAL_MS,retries:INGEST_RETRIES,successes:ingestSuccesses,failures:ingestFailures,rowsStored:ingestRowsStored,lastIngestAt,lastIngestStatus,lastIngestError,lastIngestDetail}}}));
function matches(v,q){if(!q)return true;const x=q.toLowerCase();return [v.mmsi,v.ship_name,v.imo,v.callsign,v.destination].some(z=>String(z??'').toLowerCase().includes(x));}
app.get('/vessels',(req,res)=>{if(!authorized(req,res))return;const q=cleanString(req.query.q,120)||'';const limit=Math.max(1,Math.min(500,Number(req.query.limit)||250));const rows=Array.from(cache.values()).filter(v=>matches(v,q)).sort((a,b)=>String(b.last_seen).localeCompare(String(a.last_seen))).slice(0,limit);res.json({ok:true,source:AIS_PROVIDER==='openwaters' ? 'Open Waters aiscast via MaritimeScope bridge' : 'AISStream via MaritimeScope bridge',vessels:rows,cacheSize:cache.size});});
app.get('/search',(req,res)=>{if(!authorized(req,res))return;const q=cleanString(req.query.q,120)||'';const limit=Math.max(1,Math.min(50,Number(req.query.limit)||10));const rows=Array.from(cache.values()).filter(v=>matches(v,q)).sort((a,b)=>String(b.last_seen).localeCompare(String(a.last_seen))).slice(0,limit);res.json({ok:true,vessels:rows});});
app.get('/vessel',(req,res)=>{if(!authorized(req,res))return;const mmsi=String(req.query.mmsi||'').replace(/\D/g,'');const vessel=cache.get(mmsi);if(!/^\d{9}$/.test(mmsi)||!vessel)return res.status(404).json({ok:false,error:'Vessel not currently in bridge cache'});res.json({ok:true,vessel});});
app.get('/stats',(req,res)=>{if(!authorized(req,res))return;res.json({ok:true,cacheSize:cache.size,totalMessages,lastMessageAt,subscriptionConfirmed,pendingIngest:pending.size});});
app.listen(PORT,'0.0.0.0',()=>{console.log(`MaritimeScope AIS bridge v8 listening on ${PORT}`);console.log(`SAFE TEST MODE: ${!ALLOW_GLOBAL_BOXES}; boxes: ${JSON.stringify(BOXES)}`);setTimeout(connectAIS,2000);});
