const express = require('express');
const WebSocket = require('ws');
const crypto = require('crypto');

const PORT = Number(process.env.PORT || 10000);
const PELYR_API_KEY = String(process.env.PELYR_API_KEY || '').trim();
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
const TEST_BOXES = [[[5,115],[10,120]]];
const PELYR_START_DELAY_MS = Math.max(1000, Number(process.env.PELYR_START_DELAY_MS || 5000));
const PELYR_429_COOLDOWN_MS = Math.max(30000, Number(process.env.PELYR_429_COOLDOWN_MS || 120000));
const PELYR_API_REFRESH_MS = Math.max(60000, Number(process.env.PELYR_API_REFRESH_MS || 90000));

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
  pelyr:  { socket:null, connected:false, reconnectTimer:null, reconnectAttempt:0, nextReconnectAt:null, totalConnectAttempts:0, lastConnectStartedAt:null, connectedAt:null, disconnectedAt:null, lastMessageAt:null, totalMessages:0, subscriptionConfirmed:false, compressionEnabled:null, lastError:'', lastClose:null, lastHandshake:null, successfulSubscriptions:0, state:'idle', cooldownUntil:null, lastHeartbeat:null, dropped:0, lastApiRefreshAt:null, lastApiStatus:null, lastApiError:null }
};
let totalMessages = 0;
let lastError = '';
let ingestInProgress = false, ingestSuccesses = 0, ingestFailures = 0, ingestRowsStored = 0;
let lastIngestAt=null, lastIngestStatus=null, lastIngestError=null, lastIngestDetail=null, ingestBatchNumber=0;

function cleanString(v,max=255){ if(v===undefined||v===null)return null; const s=String(v).replace(/\s+/g,' ').trim(); return s?s.slice(0,max):null; }
function num(v){ if(v===undefined||v===null||v===''||!Number.isFinite(Number(v)))return null; return Number(v); }
function msgObj(d){ return d?.Message?.[d?.MessageType] || {}; }
function etaText(eta){ if(!eta)return null; if(typeof eta==='string')return eta; if(typeof eta==='object'){const m=eta.Month??eta.month,day=eta.Day??eta.day,h=eta.Hour??eta.hour,min=eta.Minute??eta.minute;if([m,day,h,min].some(x=>x!==undefined&&x!==null))return `${m??'--'}-${day??'--'} ${h??'--'}:${min??'--'}`;} return String(eta); }
function validIso(v){ const t=Date.parse(v||''); return Number.isFinite(t)?new Date(t).toISOString():null; }

function normalizePelyr(d){
  if(d?.type!=='position') return null;
  const o=d.data||{}, m=String(o.mmsi??'').replace(/\D/g,'');
  if(!/^\d{9}$/.test(m)) return null;
  const eventTime=validIso(o.rx_ts||o.ingest_ts);
  return {mmsi:m, ship_name:cleanString(o.shipname,120), imo:cleanString(o.imo,30), callsign:cleanString(o.callsign,30), ship_type:num(o.shiptype), lat:num(o.lat), lon:num(o.lon), sog:num(o.sog), cog:num(o.cog), heading:num(o.heading), nav_status:num(o.nav_status), destination:cleanString(o.destination,120), eta:etaText(o.eta), draught:num(o.draught), length:num(o.length), beam:num(o.beam), dim_a:num(o.dim_a), dim_b:num(o.dim_b), dim_c:num(o.dim_c), dim_d:num(o.dim_d), event_time:eventTime, source:'Pelyr OPEN-AIS', message_type:String(o.msg_type??'')};
}

function normalizePelyrApi(v){
  if(!v || v.mmsi===undefined || !v.position) return null;
  const m=String(v.mmsi).replace(/\D/g,''); if(!/^\d{9}$/.test(m)) return null;
  const p=v.position||{}, st=v.static||{};
  return {mmsi:m, ship_name:cleanString(st.name,120), imo:cleanString(st.imo,30), callsign:cleanString(st.callsign,30), ship_type:num(st.type), lat:num(p.lat), lon:num(p.lon), sog:num(p.sog), cog:num(p.cog), heading:num(p.heading), nav_status:num(p.nav_status), destination:cleanString(st.dest,120), eta:etaText(st.eta), draught:num(st.draught), length:num(st.length), beam:num(st.beam), dim_a:num(st.dim_a), dim_b:num(st.dim_b), dim_c:num(st.dim_c), dim_d:num(st.dim_d), event_time:validIso(st.ts||p.ts||st.ts), source:'Pelyr HTTPS API', message_type:String(p.msg_type??'')};
}

function mergeVessel(v){
  const old=cache.get(v.mmsi)||{mmsi:v.mmsi};
  const oldTime=Date.parse(old.position_updated_at||old.source_updated_at||old.last_seen||'')||0;
  const newTime=Date.parse(v.event_time||'')||Date.now();
  const hasPosition=v.lat!==null && v.lon!==null && v.lat>=-90 && v.lat<=90 && v.lon>=-180 && v.lon<=180;
  const vessel={...old,mmsi:v.mmsi};
  const isPelyr=String(v.source||'').startsWith('Pelyr');
  const detailFields=['ship_name','imo','callsign','ship_type','nav_status','destination','eta','draught'];

  // Pelyr is the sole authoritative AIS source.
  for(const k of detailFields){
    if(v[k]===null||v[k]===undefined||v[k]==='') continue;
    const existing=vessel[k];
    const existingSource=vessel.field_sources?.[k];
    if(isPelyr || existing===null || existing===undefined || existing==='' || !String(existingSource||'').startsWith('Pelyr')){
      vessel[k]=v[k];
      vessel.field_sources={...(vessel.field_sources||{}),[k]:v.source};
      vessel.field_updated_at={...(vessel.field_updated_at||{}),[k]:v.event_time||new Date().toISOString()};
    }
  }

  // Position/motion data should follow the freshest valid report, regardless of provider.
  if(hasPosition && (newTime>=oldTime || old.lat===null || old.lon===null)){
    vessel.lat=v.lat; vessel.lon=v.lon;
    for(const k of ['sog','cog','heading']) if(v[k]!==null&&v[k]!==undefined) vessel[k]=v[k];
    vessel.position_source=v.source;
    vessel.position_updated_at=v.event_time||new Date().toISOString();
    vessel.source=v.source;
    vessel.source_updated_at=v.event_time||new Date().toISOString();
  }

  vessel.sources=Array.from(new Set([...(old.sources||[]),v.source]));
  vessel.last_seen=new Date(Math.max(oldTime,newTime,Date.now())).toISOString();
  vessel.primary_detail_source=Object.values(vessel.field_sources||{}).some(x=>String(x).startsWith('Pelyr'))?'Pelyr OPEN-AIS':(vessel.sources||[])[0]||v.source;
  vessel.updated_by=v.source;
  cache.set(v.mmsi,vessel); pending.set(v.mmsi,vessel);
  while(cache.size>MAX_CACHE){ const first=cache.keys().next().value; if(first)cache.delete(first); else break; }
  return vessel;
}
function consume(source,d){
  const v=normalizePelyr(d); if(!v)return null; return mergeVessel(v);
}
function authorized(req,res){ if(!BRIDGE_TOKEN)return true; const supplied=String(req.get('x-bridge-token')||req.query.token||''); if(supplied!==BRIDGE_TOKEN){res.status(403).json({ok:false,error:'forbidden'});return false;} return true; }
function backoffDelay(attempt){ const base=Math.min(15*60*1000,30000*Math.pow(2,Math.min(Math.max(attempt-1,0),5))); return Math.round(base*(0.8+Math.random()*0.4)); }
function retryAfterMs(response){ const v=response?.headers?.['retry-after']; if(!v)return 0; const n=Number(v); if(Number.isFinite(n))return Math.max(0,Math.min(15*60*1000,n*1000)); const t=Date.parse(v); return Number.isFinite(t)?Math.max(0,Math.min(15*60*1000,t-Date.now())):0; }
function safeHeaderSubset(headers){const out={};for(const[k,v]of Object.entries(headers||{}))if(/^(retry-after|server|date|content-type|connection|cf-|x-)/i.test(k))out[k]=v;return out;}
function scheduleReconnect(source,reason=''){const s=streams[source];if(s.reconnectTimer)return;s.reconnectAttempt++;const delay=backoffDelay(s.reconnectAttempt);s.nextReconnectAt=new Date(Date.now()+delay).toISOString();if(reason){s.lastError=reason;lastError=`${source}: ${reason}`;}s.reconnectTimer=setTimeout(()=>{s.reconnectTimer=null;s.nextReconnectAt=null;connectPelyr();},delay);}
function handleUnexpectedResponse(source,response){const s=streams[source],chunks=[];s.connected=false;s.socket=null;response.on('data',c=>{if(Buffer.concat(chunks).length<4096)chunks.push(Buffer.from(c));});response.on('end',()=>{const body=Buffer.concat(chunks).toString('utf8').slice(0,4096);s.lastHandshake={status:response.statusCode,headers:safeHeaderSubset(response.headers),bodySnippet:body,at:new Date().toISOString()};const ra=response.statusCode===429?Math.max(PELYR_429_COOLDOWN_MS,retryAfterMs(response)):retryAfterMs(response);s.lastError=`HTTP ${response.statusCode}${response.headers['retry-after']?`; Retry-After=${response.headers['retry-after']}`:''}`;
if(source==='pelyr'){s.state=response.statusCode===429?'rate_limited':'handshake_rejected';s.cooldownUntil=ra?new Date(Date.now()+ra).toISOString():null;lastError=`pelyr: ${s.lastError}`;}if(ra>0&&!s.reconnectTimer){s.reconnectAttempt++;s.nextReconnectAt=new Date(Date.now()+ra).toISOString();s.reconnectTimer=setTimeout(()=>{s.reconnectTimer=null;s.nextReconnectAt=null;connectPelyr();},ra);}else scheduleReconnect(source,s.lastError);});}
function handleClose(source,code,reasonBuffer){const s=streams[source],reason=Buffer.isBuffer(reasonBuffer)?reasonBuffer.toString('utf8'):String(reasonBuffer||'');s.lastClose={code,reason,at:new Date().toISOString()};s.disconnectedAt=s.lastClose.at;s.connected=false;s.socket=null;if(source==='pelyr'&&s.state!=='rate_limited')s.state='closed';const combined=(reason+' '+(s.lastError||'')).toLowerCase();scheduleReconnect(source,s.lastError||`WebSocket closed (${code})`);}
function openSocket(source,url,onOpen,onMessage){const s=streams[source];if(source==='pelyr'&&s.cooldownUntil&&Date.parse(s.cooldownUntil)>Date.now())return;s.totalConnectAttempts++;s.lastConnectStartedAt=new Date().toISOString();if(source==='pelyr'){s.state='connecting';s.cooldownUntil=null;}s.subscriptionConfirmed=false;s.compressionEnabled=null;s.lastHandshake={status:null,headers:{},bodySnippet:null,at:s.lastConnectStartedAt};try{const ws=new WebSocket(url,{perMessageDeflate:true,handshakeTimeout:15000,maxPayload:10*1024*1024});s.socket=ws;ws.on('open',()=>{s.connected=true;if(source==='pelyr')s.state='connected';s.connectedAt=new Date().toISOString();s.lastError='';onOpen(ws,s);});ws.on('unexpected-response',(_,res)=>handleUnexpectedResponse(source,res));ws.on('message',data=>{s.lastMessageAt=new Date().toISOString();s.totalMessages++;totalMessages++;try{onMessage(JSON.parse(Buffer.isBuffer(data)?data.toString('utf8'):String(data)),s);}catch(e){s.lastError='JSON: '+e.message;}});ws.on('error',e=>{s.lastError=e?.message||String(e);if(source==='pelyr')s.state='error';lastError=`${source}: ${s.lastError}`;});ws.on('close',(c,r)=>handleClose(source,c,r));}catch(e){s.socket=null;s.connected=false;s.lastError=e.message;scheduleReconnect(source,e.message);}}
function connectPelyr(){
  const s=streams.pelyr;
  if(!PELYR_API_KEY){s.lastError='PELYR_API_KEY is not configured';s.state='not_configured';return;}
  if(s.socket&&(s.socket.readyState===WebSocket.OPEN||s.socket.readyState===WebSocket.CONNECTING))return;
  if(s.cooldownUntil&&Date.parse(s.cooldownUntil)>Date.now())return;
  const url='wss://stream.pelyr.com/v1/stream';
  openSocket('pelyr',url,(ws,state)=>{
    const sub={type:'subscribe',id:'live',bbox:BOXES.map(b=>({west:b[0][1],south:b[0][0],east:b[1][1],north:b[1][0]})),fields:'full',msg_types:[1,2,3,18,19]};
    ws.send(JSON.stringify(sub));
    state.lastSubscriptionAt=new Date().toISOString();
  },(d,state)=>{
    if(d.type==='welcome'){state.compressionEnabled=false;state.welcomeSources=d.sources||[];return;}
    if(d.type==='subscribed'){state.state='subscribed';state.subscriptionConfirmed=true;state.reconnectAttempt=0;state.successfulSubscriptions++;state.effective=d.effective||null;state.subscriptionNotes=d.notes||[];return;}
    if(d.type==='heartbeat'){state.lastHeartbeat={at:new Date().toISOString(),dropped:Number(d.dropped||0),feed:d.feed||null};state.dropped=Number(d.dropped||0);return;}
    if(d.type==='error'){state.lastError=`Pelyr: ${String(d.message||d.code||'stream error')}`;if(d.fatal){state.state='fatal';}return;}
    if(d.type==='position')consume('pelyr',d);
  });
}

function httpsGetJson(urlString, headers={}){return new Promise((resolve,reject)=>{let u;try{u=new URL(urlString);}catch(e){reject(e);return;}const req=require('https').request({hostname:u.hostname,port:u.port||443,path:u.pathname+u.search,method:'GET',headers:{'Accept':'application/json','User-Agent':'MaritimeScope-Pelyr-Bridge/10.6','Connection':'close',...headers}},res=>{let out='';res.setEncoding('utf8');res.on('data',c=>{if(out.length<2*1024*1024)out+=c;});res.on('end',()=>{let body=null;try{body=out?JSON.parse(out):null;}catch(e){}resolve({status:res.statusCode||0,body,raw:out,headers:res.headers});});});req.on('error',reject);req.setTimeout(20000,()=>req.destroy(new Error('Pelyr API timeout')));req.end();});}

async function refreshPelyrApi(){
  if(!PELYR_API_KEY)return;
  const s=streams.pelyr;
  for(const b of BOXES){
    const bbox=`${b[0][1]},${b[0][0]},${b[1][1]},${b[1][0]}`;
    try{
      const r=await httpsGetJson(`https://api.pelyr.com/v1/vessels?bbox=${encodeURIComponent(bbox)}&max=15000`,{'Authorization':`Bearer ${PELYR_API_KEY}`});
      s.lastApiRefreshAt=new Date().toISOString();s.lastApiStatus=r.status;
      if(r.status===429){s.lastApiError=`HTTP 429${r.headers['retry-after']?` Retry-After=${r.headers['retry-after']}`:''}`;continue;}
      if(r.status<200||r.status>=300){s.lastApiError=r.body?.error?.code||`HTTP ${r.status}`;continue;}
      s.lastApiError=null;
      for(const item of (r.body?.vessels||[])){const v=normalizePelyrApi(item);if(v)mergeVessel(v);}
    }catch(e){s.lastApiRefreshAt=new Date().toISOString();s.lastApiStatus=null;s.lastApiError=e.message||String(e);}
  }
}

function connectSource(){connectPelyr();}


function postJson(urlString,body,headers={}){return new Promise((resolve,reject)=>{let u;try{u=new URL(urlString);}catch(e){reject(e);return;}const payload=Buffer.from(JSON.stringify(body));const req=require('https').request({hostname:u.hostname,port:u.port||443,path:u.pathname+u.search,method:'POST',agent:false,headers:{'Content-Type':'application/json; charset=utf-8','Content-Length':payload.length,'Accept':'application/json','User-Agent':'MaritimeScope-Pelyr-Bridge/10.6','Connection':'close','X-AIS-Request-ID':crypto.randomBytes(6).toString('hex'),...headers}},res=>{let out='';res.setEncoding('utf8');res.on('data',c=>{if(out.length<4096)out+=c;});res.on('end',()=>resolve({status:res.statusCode||0,body:out,headers:res.headers}));});req.on('error',reject);req.setTimeout(20000,()=>req.destroy(new Error('ingest timeout')));req.end(payload);});}
function sleep(ms){return new Promise(r=>setTimeout(r,ms));}
async function sendIngestBatch(rows){let failure='';const batch=++ingestBatchNumber;for(let attempt=1;attempt<=INGEST_RETRIES;attempt++){try{const r=await postJson(APP_URL+INGEST_PATH,{vessels:rows},{'X-AIS-Ingest-Key':INGEST_KEY});lastIngestDetail={batchNumber:batch,attempt,batchSize:rows.length,status:r.status,responseBody:INGEST_DEBUG?r.body.slice(0,1000):undefined,endpoint:APP_URL+INGEST_PATH,at:new Date().toISOString()};if(r.status>=200&&r.status<300){ingestSuccesses++;ingestRowsStored+=rows.length;lastIngestAt=lastIngestDetail.at;lastIngestStatus=r.status;lastIngestError=null;return true;}failure=`HTTP ${r.status}${r.body?': '+r.body.slice(0,500):''}`;}catch(e){failure=e.message||String(e);lastIngestDetail={batchNumber:batch,attempt,batchSize:rows.length,status:null,error:failure,endpoint:APP_URL+INGEST_PATH,at:new Date().toISOString()};}if(attempt<INGEST_RETRIES)await sleep(INGEST_RETRY_DELAY_MS*attempt);}ingestFailures++;lastIngestAt=new Date().toISOString();lastIngestStatus=null;lastIngestError=failure;return false;}
async function flushPending(){if(ingestInProgress||!PUSH_INGEST||!APP_URL||!INGEST_KEY||!pending.size)return;ingestInProgress=true;try{const rows=Array.from(pending.values()).slice(0,INGEST_BATCH_SIZE),ok=await sendIngestBatch(rows);if(ok)for(const row of rows)if(pending.get(row.mmsi)===row)pending.delete(row.mmsi);}finally{ingestInProgress=false;}}
if(PUSH_INGEST)setInterval(flushPending,INGEST_INTERVAL_MS);

const app=express();app.disable('x-powered-by');app.use(express.json({limit:'2mb'}));
function streamHealth(s){return {connected:s.connected,subscriptionConfirmed:s.subscriptionConfirmed,totalMessages:s.totalMessages,lastMessageAt:s.lastMessageAt,connectedAt:s.connectedAt,disconnectedAt:s.disconnectedAt,reconnectAttempt:s.reconnectAttempt,nextReconnectAt:s.nextReconnectAt,lastClose:s.lastClose,lastError:s.lastError,successfulSubscriptions:s.successfulSubscriptions,state:s.state||undefined,cooldownUntil:s.cooldownUntil||undefined,lastHandshake:s.lastHandshake};}
app.get('/',(req,res)=>res.json({ok:true,version:'10.6.0',name:'MaritimeScope Pelyr AIS Bridge',providers:['Pelyr OPEN-AIS'],mode:PUSH_INGEST?'pelyr-push-ingest':'pelyr-pull',testMode:!ALLOW_GLOBAL_BOXES,boxes:BOXES,endpoints:['/health','/diagnostics','/vessels','/vessel','/search']}));
app.get('/health',(req,res)=>res.json({ok:true,version:'10.6.0',providers:{pelyr:streamHealth(streams.pelyr)},ingest:{enabled:PUSH_INGEST,pending:pending.size,successes:ingestSuccesses,failures:ingestFailures,rowsStored:ingestRowsStored,lastIngestAt,lastIngestStatus,lastIngestError,lastIngestDetail},totalMessages,cacheSize:cache.size,lastError,boxes:BOXES,testMode:!ALLOW_GLOBAL_BOXES}));
app.get('/diagnostics',(req,res)=>res.json({ok:true,version:'10.6.0',time:new Date().toISOString(),config:{pelyrKeyConfigured:!!PELYR_API_KEY,bridgeTokenConfigured:!!BRIDGE_TOKEN,pushIngestEnabled:PUSH_INGEST,allowGlobalBoxes:ALLOW_GLOBAL_BOXES,boxes:BOXES,maxCache:MAX_CACHE},providers:{pelyr:{...streamHealth(streams.pelyr),mode:'token',httpsApi:!!PELYR_API_KEY}},data:{totalMessages,cacheSize:cache.size,pendingIngest:pending.size}}));
function matches(v,q){if(!q)return true;const x=q.toLowerCase();return [v.mmsi,v.ship_name,v.imo,v.callsign,v.destination].some(z=>String(z??'').toLowerCase().includes(x));}
app.get('/vessels',(req,res)=>{if(!authorized(req,res))return;const q=cleanString(req.query.q,120)||'',limit=Math.max(1,Math.min(500,Number(req.query.limit)||250));const rows=Array.from(cache.values()).filter(v=>matches(v,q)).sort((a,b)=>String(b.last_seen).localeCompare(String(a.last_seen))).slice(0,limit);res.json({ok:true,source:'Pelyr OPEN-AIS via MaritimeScope Render bridge',providers:['Pelyr OPEN-AIS'],vessels:rows,cacheSize:cache.size});});
app.get('/search',(req,res)=>{if(!authorized(req,res))return;const q=cleanString(req.query.q,120)||'',limit=Math.max(1,Math.min(50,Number(req.query.limit)||10));res.json({ok:true,vessels:Array.from(cache.values()).filter(v=>matches(v,q)).sort((a,b)=>String(b.last_seen).localeCompare(String(a.last_seen))).slice(0,limit)});});
app.get('/vessel',(req,res)=>{if(!authorized(req,res))return;const m=String(req.query.mmsi||'').replace(/\D/g,'');const v=cache.get(m);if(!/^\d{9}$/.test(m)||!v)return res.status(404).json({ok:false,error:'Vessel not currently in bridge cache'});res.json({ok:true,vessel:v});});
app.get('/stats',(req,res)=>{if(!authorized(req,res))return;res.json({ok:true,cacheSize:cache.size,totalMessages,pelyr:streamHealth(streams.pelyr)});});
app.listen(PORT,'0.0.0.0',()=>{console.log(`MaritimeScope Pelyr AIS bridge v10.6 listening on ${PORT}`);console.log(`Pelyr: ${!!PELYR_API_KEY?'configured':'NOT configured'}`);console.log(`Boxes: ${JSON.stringify(BOXES)}`);setTimeout(()=>{connectPelyr();},PELYR_START_DELAY_MS); setInterval(()=>{refreshPelyrApi();},PELYR_API_REFRESH_MS); setTimeout(()=>{refreshPelyrApi();},PELYR_START_DELAY_MS+10000);});
