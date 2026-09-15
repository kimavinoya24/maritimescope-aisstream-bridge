const express = require('express');
const WebSocket = require('ws');
const crypto = require('crypto');

const PORT = Number(process.env.PORT || 10000);
const OPENWATERS_API_KEY = String(process.env.OPENWATERS_API_KEY || '').trim();
const AISSTREAM_API_KEY = String(process.env.AISSTREAM_API_KEY || '').trim();
const BRIDGE_TOKEN = String(process.env.BRIDGE_TOKEN || '').trim();
const APP_URL = String(process.env.APP_URL || '').replace(/\/$/, '');
const INGEST_KEY = String(process.env.AIS_INGEST_KEY || '').trim();
const INGEST_PATH = String(process.env.AIS_INGEST_PATH || '/vessel-sync.php');
const MAX_CACHE = Math.max(100, Number(process.env.MAX_CACHE || 5000));
const ALLOW_GLOBAL_BOXES = String(process.env.ALLOW_GLOBAL_BOXES || 'false').toLowerCase() === 'true';
const PUSH_INGEST = String(process.env.AIS_PUSH_INGEST || 'false').toLowerCase() === 'true';
const INGEST_BATCH_SIZE = Math.max(1, Math.min(50, Number(process.env.AIS_INGEST_BATCH_SIZE || 20)));
const INGEST_INTERVAL_MS = Math.max(3000, Number(process.env.AIS_INGEST_INTERVAL_MS || 5000));
const INGEST_RETRIES = Math.max(1, Math.min(5, Number(process.env.AIS_INGEST_RETRIES || 3)));
const INGEST_RETRY_DELAY_MS = Math.max(1000, Number(process.env.AIS_INGEST_RETRY_DELAY_MS || 2500));
const INGEST_DEBUG = String(process.env.AIS_INGEST_DEBUG || 'true').toLowerCase() === 'true';
const TEST_BOXES = [[[0,105],[10,115]],[[5,115],[15,125]]];

function parseBoxes() {
  try {
    const value = JSON.parse(process.env.AIS_BOXES || JSON.stringify(TEST_BOXES));
    if (Array.isArray(value) && value.length) return value;
  } catch (_) {}
  return TEST_BOXES;
}
const BOXES = parseBoxes();

const cache = new Map();
const pending = new Map();
const streams = {
  openwaters: { socket:null, connected:false, reconnectTimer:null, reconnectAttempt:0, nextReconnectAt:null, totalConnectAttempts:0, lastConnectStartedAt:null, connectedAt:null, disconnectedAt:null, lastMessageAt:null, totalMessages:0, subscriptionConfirmed:false, compressionEnabled:null, lastError:'', lastClose:null, lastHandshake:null, successfulSubscriptions:0, anonymousFallback:false },
  aisstream:  { socket:null, connected:false, reconnectTimer:null, reconnectAttempt:0, nextReconnectAt:null, totalConnectAttempts:0, lastConnectStartedAt:null, connectedAt:null, disconnectedAt:null, lastMessageAt:null, totalMessages:0, subscriptionConfirmed:false, compressionEnabled:null, lastError:'', lastClose:null, lastHandshake:null, successfulSubscriptions:0 }
};
let totalMessages = 0;
let lastError = '';
let ingestInProgress = false, ingestSuccesses = 0, ingestFailures = 0, ingestRowsStored = 0;
let lastIngestAt=null, lastIngestStatus=null, lastIngestError=null, lastIngestDetail=null, ingestBatchNumber=0;

function cleanString(v,max=255){ if(v===undefined||v===null)return null; const s=String(v).replace(/\s+/g,' ').trim(); return s?s.slice(0,max):null; }
function num(v){ if(v===undefined||v===null||v===''||!Number.isFinite(Number(v)))return null; return Number(v); }
function msgObj(d){ return d?.Message?.[d?.MessageType] || {}; }
function mmsiOfAISStream(d){ const raw=d?.MetaData?.MMSI ?? d?.MetaData?.UserID ?? d?.Message?.MMSI ?? msgObj(d)?.UserID ?? ''; return String(raw).replace(/\D/g,''); }
function etaText(eta){ if(!eta)return null; if(typeof eta==='string')return eta; if(typeof eta==='object'){const m=eta.Month??eta.month,day=eta.Day??eta.day,h=eta.Hour??eta.hour,min=eta.Minute??eta.minute;if([m,day,h,min].some(x=>x!==undefined&&x!==null))return `${m??'--'}-${day??'--'} ${h??'--'}:${min??'--'}`;} return String(eta); }
function validIso(v){ const t=Date.parse(v||''); return Number.isFinite(t)?new Date(t).toISOString():null; }

function normalizeOpenWaters(d){
  if(d?.type!=='event') return null;
  const o=d||{}; const m=String(o.mmsi??'').replace(/\D/g,'');
  if(!/^\d{9}$/.test(m)) return null;
  const p=o||{};
  return {mmsi:m, ship_name:cleanString(p.name,120), imo:cleanString(p.imo,30), callsign:cleanString(p.callsign,30), ship_type:num(p.type), lat:num(p.lat), lon:num(p.lon), sog:num(p.sog), cog:num(p.cog), heading:num(p.heading), nav_status:cleanString(p.nav_status,80), destination:cleanString(p.destination,120), eta:etaText(p.eta), draught:num(p.draught), event_time:validIso(p.time), source:'Open Waters aiscast', message_type:cleanString(p.msg_type,60)};
}
function normalizeAISStream(d){
  if(d?.MessageType==='SubscriptionConfirmation') return null;
  const o=msgObj(d), meta=d?.MetaData||{}, m=mmsiOfAISStream(d);
  if(!/^\d{9}$/.test(m)) return null;
  const eventTime=validIso(meta?.TimeUTC || meta?.time || d?.TimeUTC || d?.time);
  return {mmsi:m, ship_name:cleanString(meta.ShipName ?? o.Name ?? o.ShipName,120), imo:cleanString(o.ImoNumber ?? o.IMO,30), callsign:cleanString(o.CallSign ?? o.Callsign,30), ship_type:num(o.Type ?? o.ShipType), lat:num(meta.Latitude ?? o.Latitude), lon:num(meta.Longitude ?? o.Longitude), sog:num(o.Sog), cog:num(o.Cog), heading:num(o.TrueHeading ?? o.Heading), nav_status:cleanString(o.NavigationalStatus,80), destination:cleanString(o.Destination,120), eta:etaText(o.Eta), draught:num(o.Draught), event_time:eventTime, source:'AISStream', message_type:cleanString(d.MessageType,60)};
}

function mergeVessel(v){
  const old=cache.get(v.mmsi)||{mmsi:v.mmsi};
  const oldTime=Date.parse(old.source_updated_at||old.last_seen||'')||0;
  const newTime=Date.parse(v.event_time||'')||Date.now();
  const hasPosition=v.lat!==null && v.lon!==null && v.lat>=-90 && v.lat<=90 && v.lon>=-180 && v.lon<=180;
  const vessel={...old,mmsi:v.mmsi};
  for(const k of ['ship_name','imo','callsign','ship_type','sog','cog','heading','nav_status','destination','eta','draught','message_type']) if(v[k]!==null&&v[k]!==undefined) vessel[k]=v[k];
  if(hasPosition && (newTime>=oldTime || old.lat===null || old.lon===null)){ vessel.lat=v.lat; vessel.lon=v.lon; vessel.sog=v.sog??vessel.sog; vessel.cog=v.cog??vessel.cog; vessel.heading=v.heading??vessel.heading; vessel.source=v.source; vessel.source_updated_at=v.event_time||new Date().toISOString(); }
  vessel.sources=Array.from(new Set([...(old.sources||[]),v.source]));
  vessel.last_seen=new Date(Math.max(oldTime,newTime,Date.now())).toISOString();
  if(!vessel.source) vessel.source=v.source;
  cache.set(v.mmsi,vessel); pending.set(v.mmsi,vessel);
  while(cache.size>MAX_CACHE){ const first=cache.keys().next().value; if(first)cache.delete(first); else break; }
  return vessel;
}
function consume(source,d){
  const v=source==='openwaters'?normalizeOpenWaters(d):normalizeAISStream(d); if(!v)return null; return mergeVessel(v);
}
function authorized(req,res){ if(!BRIDGE_TOKEN)return true; const supplied=String(req.get('x-bridge-token')||req.query.token||''); if(supplied!==BRIDGE_TOKEN){res.status(403).json({ok:false,error:'forbidden'});return false;} return true; }
function backoffDelay(attempt){ const base=Math.min(15*60*1000,30000*Math.pow(2,Math.min(Math.max(attempt-1,0),5))); return Math.round(base*(0.8+Math.random()*0.4)); }
function retryAfterMs(response){ const v=response?.headers?.['retry-after']; if(!v)return 0; const n=Number(v); if(Number.isFinite(n))return Math.max(0,Math.min(15*60*1000,n*1000)); const t=Date.parse(v); return Number.isFinite(t)?Math.max(0,Math.min(15*60*1000,t-Date.now())):0; }
function safeHeaderSubset(headers){const out={};for(const[k,v]of Object.entries(headers||{}))if(/^(retry-after|server|date|content-type|connection|cf-|x-)/i.test(k))out[k]=v;return out;}
function scheduleReconnect(source,reason=''){const s=streams[source];if(s.reconnectTimer)return;s.reconnectAttempt++;const delay=backoffDelay(s.reconnectAttempt);s.nextReconnectAt=new Date(Date.now()+delay).toISOString();if(reason){s.lastError=reason;lastError=`${source}: ${reason}`;}s.reconnectTimer=setTimeout(()=>{s.reconnectTimer=null;s.nextReconnectAt=null;connectSource(source);},delay);}
function handleUnexpectedResponse(source,response){const s=streams[source],chunks=[];response.on('data',c=>{if(Buffer.concat(chunks).length<4096)chunks.push(Buffer.from(c));});response.on('end',()=>{const body=Buffer.concat(chunks).toString('utf8').slice(0,4096);s.lastHandshake={status:response.statusCode,headers:safeHeaderSubset(response.headers),bodySnippet:body,at:new Date().toISOString()};const ra=retryAfterMs(response);s.lastError=`HTTP ${response.statusCode}${response.headers['retry-after']?`; Retry-After=${response.headers['retry-after']}`:''}`;if(ra>0&&!s.reconnectTimer){s.reconnectAttempt++;s.nextReconnectAt=new Date(Date.now()+ra).toISOString();s.reconnectTimer=setTimeout(()=>{s.reconnectTimer=null;s.nextReconnectAt=null;connectSource(source);},ra);}else scheduleReconnect(source,s.lastError);});}
function handleClose(source,code,reasonBuffer){const s=streams[source],reason=Buffer.isBuffer(reasonBuffer)?reasonBuffer.toString('utf8'):String(reasonBuffer||'');s.lastClose={code,reason,at:new Date().toISOString()};s.disconnectedAt=s.lastClose.at;s.connected=false;s.socket=null;const combined=(reason+' '+(s.lastError||'')).toLowerCase();if(source==='openwaters'&&OPENWATERS_API_KEY&&!s.anonymousFallback&&/invalid token|bad token|unauthorized|401|api key|token/.test(combined)){s.anonymousFallback=true;s.lastError='Open Waters token rejected; retrying anonymously';}scheduleReconnect(source,s.lastError||`WebSocket closed (${code})`);}
function openSocket(source,url,onOpen,onMessage){const s=streams[source];s.totalConnectAttempts++;s.lastConnectStartedAt=new Date().toISOString();s.subscriptionConfirmed=false;s.compressionEnabled=null;s.lastHandshake={status:null,headers:{},bodySnippet:null,at:s.lastConnectStartedAt};try{const ws=new WebSocket(url,{perMessageDeflate:true,handshakeTimeout:15000,maxPayload:10*1024*1024});s.socket=ws;ws.on('open',()=>{s.connected=true;s.connectedAt=new Date().toISOString();s.lastError='';onOpen(ws,s);});ws.on('unexpected-response',(_,res)=>handleUnexpectedResponse(source,res));ws.on('message',data=>{s.lastMessageAt=new Date().toISOString();s.totalMessages++;totalMessages++;try{onMessage(JSON.parse(Buffer.isBuffer(data)?data.toString('utf8'):String(data)),s);}catch(e){s.lastError='JSON: '+e.message;}});ws.on('error',e=>{s.lastError=e?.message||String(e);lastError=`${source}: ${s.lastError}`;});ws.on('close',(c,r)=>handleClose(source,c,r));}catch(e){s.socket=null;s.connected=false;s.lastError=e.message;scheduleReconnect(source,e.message);}}
function connectOpenWaters(){const s=streams.openwaters;if(s.socket&&(s.socket.readyState===WebSocket.OPEN||s.socket.readyState===WebSocket.CONNECTING))return;const usingToken=!!OPENWATERS_API_KEY&&!s.anonymousFallback;const url='wss://ais.openwaters.io/v1/stream'+(usingToken?`?key=${encodeURIComponent(OPENWATERS_API_KEY)}`:'');openSocket('openwaters',url,(ws,state)=>{const sub={type:'subscribe',bbox:BOXES.map(b=>[b[0][0],b[0][1],b[1][0],b[1][1]]),snapshot:true};ws.send(JSON.stringify(sub));state.lastSubscriptionAt=new Date().toISOString();},(d,state)=>{if(d.type==='welcome'){state.compressionEnabled=true;return;}if(d.type==='ack'){state.subscriptionConfirmed=true;state.reconnectAttempt=0;state.successfulSubscriptions++;return;}if(d.type==='error'){state.lastError='Open Waters: '+String(d.error||'stream error');if(OPENWATERS_API_KEY&&/token|auth|key/i.test(String(d.error||'')))state.anonymousFallback=true;return;}if(d.type==='event')consume('openwaters',d);});}
function connectAISStream(){const s=streams.aisstream;if(!AISSTREAM_API_KEY){s.lastError='AISSTREAM_API_KEY is not configured';return;}if(s.socket&&(s.socket.readyState===WebSocket.OPEN||s.socket.readyState===WebSocket.CONNECTING))return;openSocket('aisstream','wss://stream.aisstream.io/v0/stream',(ws,state)=>{ws.send(JSON.stringify({APIKey:AISSTREAM_API_KEY,BoundingBoxes:BOXES,FilterMessageTypes:['PositionReport','ShipStaticData']}));state.lastSubscriptionAt=new Date().toISOString();},(d,state)=>{if(d.MessageType==='SubscriptionConfirmation'){state.subscriptionConfirmed=true;state.compressionEnabled=d?.Message?.CompressionEnabled??null;state.reconnectAttempt=0;state.successfulSubscriptions++;return;}consume('aisstream',d);});}
function connectSource(source){if(source==='openwaters')connectOpenWaters();else connectAISStream();}

function postJson(urlString,body,headers={}){return new Promise((resolve,reject)=>{let u;try{u=new URL(urlString);}catch(e){reject(e);return;}const payload=Buffer.from(JSON.stringify(body));const req=require('https').request({hostname:u.hostname,port:u.port||443,path:u.pathname+u.search,method:'POST',agent:false,headers:{'Content-Type':'application/json; charset=utf-8','Content-Length':payload.length,'Accept':'application/json','User-Agent':'MaritimeScope-AIS-Hybrid-Bridge/10.0','Connection':'close','X-AIS-Request-ID':crypto.randomBytes(6).toString('hex'),...headers}},res=>{let out='';res.setEncoding('utf8');res.on('data',c=>{if(out.length<4096)out+=c;});res.on('end',()=>resolve({status:res.statusCode||0,body:out,headers:res.headers}));});req.on('error',reject);req.setTimeout(20000,()=>req.destroy(new Error('ingest timeout')));req.end(payload);});}
function sleep(ms){return new Promise(r=>setTimeout(r,ms));}
async function sendIngestBatch(rows){let failure='';const batch=++ingestBatchNumber;for(let attempt=1;attempt<=INGEST_RETRIES;attempt++){try{const r=await postJson(APP_URL+INGEST_PATH,{vessels:rows},{'X-AIS-Ingest-Key':INGEST_KEY});lastIngestDetail={batchNumber:batch,attempt,batchSize:rows.length,status:r.status,responseBody:INGEST_DEBUG?r.body.slice(0,1000):undefined,endpoint:APP_URL+INGEST_PATH,at:new Date().toISOString()};if(r.status>=200&&r.status<300){ingestSuccesses++;ingestRowsStored+=rows.length;lastIngestAt=lastIngestDetail.at;lastIngestStatus=r.status;lastIngestError=null;return true;}failure=`HTTP ${r.status}${r.body?': '+r.body.slice(0,500):''}`;}catch(e){failure=e.message||String(e);lastIngestDetail={batchNumber:batch,attempt,batchSize:rows.length,status:null,error:failure,endpoint:APP_URL+INGEST_PATH,at:new Date().toISOString()};}if(attempt<INGEST_RETRIES)await sleep(INGEST_RETRY_DELAY_MS*attempt);}ingestFailures++;lastIngestAt=new Date().toISOString();lastIngestStatus=null;lastIngestError=failure;return false;}
async function flushPending(){if(ingestInProgress||!PUSH_INGEST||!APP_URL||!INGEST_KEY||!pending.size)return;ingestInProgress=true;try{const rows=Array.from(pending.values()).slice(0,INGEST_BATCH_SIZE),ok=await sendIngestBatch(rows);if(ok)for(const row of rows)if(pending.get(row.mmsi)===row)pending.delete(row.mmsi);}finally{ingestInProgress=false;}}
if(PUSH_INGEST)setInterval(flushPending,INGEST_INTERVAL_MS);

const app=express();app.disable('x-powered-by');app.use(express.json({limit:'2mb'}));
function streamHealth(s){return {connected:s.connected,subscriptionConfirmed:s.subscriptionConfirmed,totalMessages:s.totalMessages,lastMessageAt:s.lastMessageAt,connectedAt:s.connectedAt,disconnectedAt:s.disconnectedAt,reconnectAttempt:s.reconnectAttempt,nextReconnectAt:s.nextReconnectAt,lastClose:s.lastClose,lastError:s.lastError,successfulSubscriptions:s.successfulSubscriptions};}
app.get('/',(req,res)=>res.json({ok:true,version:'10.1.0',name:'MaritimeScope Hybrid AIS Bridge',providers:['Open Waters aiscast','AISStream'],mode:PUSH_INGEST?'hybrid-push-ingest':'hybrid-pull',testMode:!ALLOW_GLOBAL_BOXES,boxes:BOXES,endpoints:['/health','/diagnostics','/vessels','/vessel','/search']}));
app.get('/health',(req,res)=>res.json({ok:true,version:'10.1.0',providers:{openwaters:streamHealth(streams.openwaters),aisstream:streamHealth(streams.aisstream)},ingest:{enabled:PUSH_INGEST,pending:pending.size,successes:ingestSuccesses,failures:ingestFailures,rowsStored:ingestRowsStored,lastIngestAt,lastIngestStatus,lastIngestError,lastIngestDetail},totalMessages,cacheSize:cache.size,lastError,boxes:BOXES,testMode:!ALLOW_GLOBAL_BOXES}));
app.get('/diagnostics',(req,res)=>res.json({ok:true,version:'10.1.0',time:new Date().toISOString(),config:{openWatersKeyConfigured:!!OPENWATERS_API_KEY,aisStreamKeyConfigured:!!AISSTREAM_API_KEY,bridgeTokenConfigured:!!BRIDGE_TOKEN,pushIngestEnabled:PUSH_INGEST,allowGlobalBoxes:ALLOW_GLOBAL_BOXES,boxes:BOXES,maxCache:MAX_CACHE},providers:{openwaters:{...streamHealth(streams.openwaters),mode:streams.openwaters.anonymousFallback||!OPENWATERS_API_KEY?'anonymous':'token'},aisstream:{...streamHealth(streams.aisstream),mode:'token'}},data:{totalMessages,cacheSize:cache.size,pendingIngest:pending.size}}));
function matches(v,q){if(!q)return true;const x=q.toLowerCase();return [v.mmsi,v.ship_name,v.imo,v.callsign,v.destination].some(z=>String(z??'').toLowerCase().includes(x));}
app.get('/vessels',(req,res)=>{if(!authorized(req,res))return;const q=cleanString(req.query.q,120)||'',limit=Math.max(1,Math.min(500,Number(req.query.limit)||250));const rows=Array.from(cache.values()).filter(v=>matches(v,q)).sort((a,b)=>String(b.last_seen).localeCompare(String(a.last_seen))).slice(0,limit);res.json({ok:true,source:'Open Waters aiscast + AISStream via MaritimeScope hybrid bridge',providers:['Open Waters aiscast','AISStream'],vessels:rows,cacheSize:cache.size});});
app.get('/search',(req,res)=>{if(!authorized(req,res))return;const q=cleanString(req.query.q,120)||'',limit=Math.max(1,Math.min(50,Number(req.query.limit)||10));res.json({ok:true,vessels:Array.from(cache.values()).filter(v=>matches(v,q)).sort((a,b)=>String(b.last_seen).localeCompare(String(a.last_seen))).slice(0,limit)});});
app.get('/vessel',(req,res)=>{if(!authorized(req,res))return;const m=String(req.query.mmsi||'').replace(/\D/g,'');const v=cache.get(m);if(!/^\d{9}$/.test(m)||!v)return res.status(404).json({ok:false,error:'Vessel not currently in bridge cache'});res.json({ok:true,vessel:v});});
app.get('/stats',(req,res)=>{if(!authorized(req,res))return;res.json({ok:true,cacheSize:cache.size,totalMessages,openwaters:streamHealth(streams.openwaters),aisstream:streamHealth(streams.aisstream)});});
app.listen(PORT,'0.0.0.0',()=>{console.log(`MaritimeScope Hybrid AIS bridge v10.1 listening on ${PORT}`);console.log(`Open Waters: ${!!OPENWATERS_API_KEY?'token':'anonymous'} | AISStream: ${!!AISSTREAM_API_KEY?'configured':'NOT configured'}`);console.log(`Boxes: ${JSON.stringify(BOXES)}`);setTimeout(()=>{connectOpenWaters();},2000); setTimeout(()=>{connectAISStream();},12000);});
