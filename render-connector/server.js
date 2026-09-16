const express = require('express');
const WebSocket = require('ws');
const crypto = require('crypto');

const PORT = Number(process.env.PORT || 10000);
const PELYR_API_KEY = String(process.env.PELYR_API_KEY || '').trim();
const BRIDGE_TOKEN = String(process.env.BRIDGE_TOKEN || '').trim();

const MAX_CACHE = Math.max(100, Number(process.env.MAX_CACHE || 5000));
const ALLOW_GLOBAL_BOXES = String(process.env.ALLOW_GLOBAL_BOXES || 'false').toLowerCase() === 'true';

const TEST_BOXES = [[[5,115],[10,120]]];
const PELYR_START_DELAY_MS = Math.max(1000, Number(process.env.PELYR_START_DELAY_MS || 5000));
const PELYR_429_COOLDOWN_MS = Math.max(30000, Number(process.env.PELYR_429_COOLDOWN_MS || 120000));
const PELYR_API_REFRESH_MS = Math.max(60000, Number(process.env.PELYR_API_REFRESH_MS || 60000));

function parseBoxes() {
  try {
    const value = JSON.parse(process.env.AIS_BOXES || JSON.stringify(TEST_BOXES));
    if (Array.isArray(value) && value.length) return value;
  } catch (_) {}
  return TEST_BOXES;
}
const BOXES = parseBoxes();

const cache = new Map();
const streams = {
  pelyr:  { socket:null, connected:false, reconnectTimer:null, reconnectAttempt:0, nextReconnectAt:null, totalConnectAttempts:0, lastConnectStartedAt:null, connectedAt:null, disconnectedAt:null, lastMessageAt:null, totalMessages:0, subscriptionConfirmed:false, compressionEnabled:null, lastError:'', lastClose:null, lastHandshake:null, successfulSubscriptions:0, state:'idle', cooldownUntil:null, lastHeartbeat:null, dropped:0, lastApiRefreshAt:null, lastApiStatus:null, lastApiError:null }
};
let totalMessages = 0;
let lastError = '';


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
  return {mmsi:m, ship_name:cleanString(o.shipname,120), imo:cleanString(o.imo,30), callsign:cleanString(o.callsign,30), ship_type:num(o.shiptype), lat:num(o.lat), lon:num(o.lon), sog:num(o.sog), cog:num(o.cog), heading:num(o.heading), nav_status:num(o.nav_status), rot:num(o.rot), pos_accuracy:o.pos_accuracy??null, destination:cleanString(o.destination,120), eta:etaText(o.eta), draught:num(o.draught), length:num(o.length), beam:num(o.beam), dim_a:num(o.dim_a), dim_b:num(o.dim_b), dim_c:num(o.dim_c), dim_d:num(o.dim_d), event_time:eventTime, source:'Pelyr OPEN-AIS', message_type:String(o.msg_type??'')};
}
function normalizePelyrApi(v){
  if(!v || v.mmsi===undefined || !v.position) return null;
  const m=String(v.mmsi).replace(/\D/g,''); if(!/^\d{9}$/.test(m)) return null;
  const p=v.position||{}, st=v.static||{};
  return {mmsi:m, ship_name:cleanString(st.name,120), imo:cleanString(st.imo,30), callsign:cleanString(st.callsign,30), ship_type:num(st.type), lat:num(p.lat), lon:num(p.lon), sog:num(p.sog), cog:num(p.cog), heading:num(p.heading), nav_status:num(p.nav_status), rot:num(p.rot), pos_accuracy:p.pos_accuracy??null, destination:cleanString(st.dest,120), eta:etaText(st.eta), draught:num(st.draught), length:num(st.length), beam:num(st.beam), dim_a:num(st.dim_a), dim_b:num(st.dim_b), dim_c:num(st.dim_c), dim_d:num(st.dim_d), plausibility:cleanString(p.plausibility,30), flags:Array.isArray(p.flags)?p.flags:[], position_license:cleanString(p.license,30), static_license:cleanString(st.license,30), event_time:validIso(p.ts||st.ts), source:'Pelyr HTTPS API', message_type:String(p.msg_type??'')};
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
function openSocket(source,url,onOpen,onMessage){const s=streams[source];if(source==='pelyr'&&s.cooldownUntil&&Date.parse(s.cooldownUntil)>Date.now())return;s.totalConnectAttempts++;s.lastConnectStartedAt=new Date().toISOString();if(source==='pelyr'){s.state='connecting';s.cooldownUntil=null;}s.subscriptionConfirmed=false;s.compressionEnabled=null;s.lastHandshake={status:null,headers:{},bodySnippet:null,at:s.lastConnectStartedAt};try{const wsOptions={perMessageDeflate:true,handshakeTimeout:15000,maxPayload:10*1024*1024};if(source==='pelyr'){wsOptions.headers={Authorization:`Bearer ${PELYR_API_KEY}`,'User-Agent':'MaritimeScope-Pelyr-Bridge/11.0'};}const ws=new WebSocket(url,wsOptions);s.socket=ws;ws.on('open',()=>{s.connected=true;if(source==='pelyr')s.state='connected';s.connectedAt=new Date().toISOString();s.lastError='';onOpen(ws,s);});ws.on('unexpected-response',(_,res)=>handleUnexpectedResponse(source,res));ws.on('message',data=>{s.lastMessageAt=new Date().toISOString();s.totalMessages++;totalMessages++;try{onMessage(JSON.parse(Buffer.isBuffer(data)?data.toString('utf8'):String(data)),s);}catch(e){s.lastError='JSON: '+e.message;}});ws.on('error',e=>{s.lastError=e?.message||String(e);if(source==='pelyr')s.state='error';lastError=`${source}: ${s.lastError}`;});ws.on('close',(c,r)=>handleClose(source,c,r));}catch(e){s.socket=null;s.connected=false;s.lastError=e.message;scheduleReconnect(source,e.message);}}
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

function httpsGetJson(urlString, headers={}){return new Promise((resolve,reject)=>{let u;try{u=new URL(urlString);}catch(e){reject(e);return;}const req=require('https').request({hostname:u.hostname,port:u.port||443,path:u.pathname+u.search,method:'GET',headers:{'Accept':'application/json','User-Agent':'MaritimeScope-Pelyr-Bridge/11.0','Connection':'close',...headers}},res=>{let out='';res.setEncoding('utf8');res.on('data',c=>{if(out.length<2*1024*1024)out+=c;});res.on('end',()=>{let body=null;try{body=out?JSON.parse(out):null;}catch(e){}resolve({status:res.statusCode||0,body,raw:out,headers:res.headers});});});req.on('error',reject);req.setTimeout(20000,()=>req.destroy(new Error('Pelyr API timeout')));req.end();});}

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

async function fetchPelyrVessel(mmsi){
  if(!PELYR_API_KEY) return {status:0,body:null,headers:{}};
  return httpsGetJson(`https://api.pelyr.com/v1/vessels/${encodeURIComponent(mmsi)}`,{'Authorization':`Bearer ${PELYR_API_KEY}`});
}

const app=express();app.disable('x-powered-by');app.use(express.json({limit:'2mb'}));
function streamHealth(s){return {connected:s.connected,subscriptionConfirmed:s.subscriptionConfirmed,totalMessages:s.totalMessages,lastMessageAt:s.lastMessageAt,connectedAt:s.connectedAt,disconnectedAt:s.disconnectedAt,reconnectAttempt:s.reconnectAttempt,nextReconnectAt:s.nextReconnectAt,lastClose:s.lastClose,lastError:s.lastError,successfulSubscriptions:s.successfulSubscriptions,state:s.state||undefined,cooldownUntil:s.cooldownUntil||undefined,lastHandshake:s.lastHandshake};}
app.get('/',(req,res)=>res.json({ok:true,version:'11.0.0',name:'MaritimeScope Pelyr AIS Bridge',providers:['Pelyr OPEN-AIS'],mode:'pelyr-live-memory',testMode:!ALLOW_GLOBAL_BOXES,boxes:BOXES,endpoints:['/health','/diagnostics','/vessels','/vessel','/search']}));
app.get('/health',(req,res)=>res.json({ok:true,version:'11.0.0',providers:{pelyr:streamHealth(streams.pelyr)},totalMessages,cacheSize:cache.size,lastError,boxes:BOXES,testMode:!ALLOW_GLOBAL_BOXES,aisStorage:'Render in-memory only; MySQL AIS cache disabled'}));
app.get('/diagnostics',(req,res)=>res.json({ok:true,version:'11.0.0',time:new Date().toISOString(),config:{pelyrKeyConfigured:!!PELYR_API_KEY,bridgeTokenConfigured:!!BRIDGE_TOKEN,allowGlobalBoxes:ALLOW_GLOBAL_BOXES,boxes:BOXES,maxCache:MAX_CACHE},providers:{pelyr:{...streamHealth(streams.pelyr),mode:'token',httpsApi:!!PELYR_API_KEY}},data:{totalMessages,cacheSize:cache.size}}));
function matches(v,q){if(!q)return true;const x=q.toLowerCase();return [v.mmsi,v.ship_name,v.imo,v.callsign,v.destination].some(z=>String(z??'').toLowerCase().includes(x));}
app.get('/vessels',(req,res)=>{if(!authorized(req,res))return;const q=cleanString(req.query.q,120)||'',limit=Math.max(1,Math.min(500,Number(req.query.limit)||250));const rows=Array.from(cache.values()).filter(v=>matches(v,q)).sort((a,b)=>String(b.last_seen).localeCompare(String(a.last_seen))).slice(0,limit);res.set('Cache-Control','no-store');res.json({ok:true,source:'Pelyr OPEN-AIS live working set',providers:['Pelyr OPEN-AIS'],vessels:rows,cacheSize:cache.size,storage:'memory-only'});});
app.get('/search',(req,res)=>{if(!authorized(req,res))return;const q=cleanString(req.query.q,120)||'',limit=Math.max(1,Math.min(50,Number(req.query.limit)||10));res.set('Cache-Control','no-store');res.json({ok:true,vessels:Array.from(cache.values()).filter(v=>matches(v,q)).sort((a,b)=>String(b.last_seen).localeCompare(String(a.last_seen))).slice(0,limit),storage:'memory-only'});});
app.get('/vessel',async(req,res)=>{
  if(!authorized(req,res))return;
  const m=String(req.query.mmsi||'').replace(/\D/g,'');
  if(!/^\d{9}$/.test(m))return res.status(422).json({ok:false,error:'invalid_mmsi'});
  try{
    const r=await fetchPelyrVessel(m);
    if(r.status===200){
      const v=normalizePelyrApi(r.body);
      if(v){
        const merged=mergeVessel(v);
        res.set('Cache-Control','no-store');
        return res.json({ok:true,vessel:merged,source:'Pelyr OPEN-AIS HTTPS /v1/vessels/{mmsi}',fresh:true});
      }
    }
    if(r.status===404)return res.status(404).json({ok:false,error:'Vessel not heard by Pelyr for over an hour'});
    if(r.status===429)return res.status(429).json({ok:false,error:'Pelyr rate limit reached',retryAfter:r.headers?.['retry-after']||null});
    const cached=cache.get(m);
    if(cached)return res.json({ok:true,vessel:cached,source:'Pelyr live working set',fresh:false,warning:`Pelyr HTTP ${r.status}`});
    return res.status(r.status||502).json({ok:false,error:r.body?.error?.code||`Pelyr HTTP ${r.status}`});
  }catch(e){
    const cached=cache.get(m);
    if(cached)return res.json({ok:true,vessel:cached,source:'Pelyr live working set',fresh:false,warning:e.message||String(e)});
    return res.status(502).json({ok:false,error:e.message||String(e)});
  }
});
app.get('/stats',(req,res)=>{if(!authorized(req,res))return;res.json({ok:true,cacheSize:cache.size,totalMessages,pelyr:streamHealth(streams.pelyr)});});
app.listen(PORT,'0.0.0.0',()=>{console.log(`MaritimeScope Pelyr AIS bridge v10.9 listening on ${PORT}`);console.log(`Pelyr: ${!!PELYR_API_KEY?'configured':'NOT configured'}`);console.log(`Boxes: ${JSON.stringify(BOXES)}`);setTimeout(()=>{connectPelyr();},PELYR_START_DELAY_MS); setInterval(()=>{refreshPelyrApi();},PELYR_API_REFRESH_MS); setTimeout(()=>{refreshPelyrApi();},PELYR_START_DELAY_MS+10000);});
