'use strict';
/* ==========================================================================================
   CoWatch v6 - transfer + smart buffer engine
   Sender (host):  reads the file in 1 MB blocks, SHA-256 per block, sends framed chunks on request.
   Receiver (guest): decides WHAT it needs (priority list), asks for it (REQ), verifies, stores blocks
   in IndexedDB; the service worker serves them to <video> through HTTP Range.
   Frame (raw data channel):  [1][blockIdx u32][offset u32][payload]  data
                              [2][blockIdx u32][sha256 32B]            hash of the block (sent first)
   ========================================================================================== */
const BS=1048576,HDR=9,TAILB=16,HIGH=4*1024*1024;
const HASH_OK=!!(window.crypto&&crypto.subtle&&crypto.subtle.digest);
const MAXK=Math.max(1,Math.min(4,(navigator.hardwareConcurrency||2)>2?4:2,(navigator.deviceMemory||4)<=2?2:4));
const baseBufSec=()=>window.__BASE_BUF_SEC||900;           // 15 minutes (tests may shrink it for short clips)
let swOK=false,wake=null,tStart=0,fileConns={},tx=null,fileReady=null;
const READ_HELP='تعذّر قراءة ملف الفيلم من تخزين تليفونك. غالباً الملف مختار من Google Drive أو Photos أو «الأخيرة»، أو اتنقل/اتعدّل بعد اختياره. انسخه لذاكرة التليفون (Downloads) واختاره من «الملفات» تاني. النقل هيكمّل من اللي وصل.';
/* ---------- service worker + IndexedDB ---------- */
async function initSW(){
 try{
  if(!('serviceWorker' in navigator)||!window.isSecureContext||!window.indexedDB)return;
  navigator.serviceWorker.addEventListener('message',onSwMsg);
  await navigator.serviceWorker.register('cowatch-sw.js');await navigator.serviceWorker.ready;
  if(!navigator.serviceWorker.controller)await new Promise(r=>{navigator.serviceWorker.addEventListener('controllerchange',r,{once:true});setTimeout(r,1500)});
  if(!navigator.serviceWorker.controller){ // rare first-visit race: reload once (lobby only) so the page becomes controlled
   let done=false;try{done=sessionStorage.getItem('cwsw')==='1';sessionStorage.setItem('cwsw','1')}catch{done=true}
   if(!done&&!joined){location.reload();await new Promise(()=>{})}
  }else{try{sessionStorage.removeItem('cwsw')}catch{}}
  swOK=!!navigator.serviceWorker.controller;
 }catch(e){swOK=false}
}
const swInit=initSW();
let _db=null;
function idb(){return _db||(_db=new Promise((res,rej)=>{const q=indexedDB.open('cowatch',1);q.onupgradeneeded=()=>{q.result.createObjectStore('meta');q.result.createObjectStore('blk')};q.onsuccess=()=>res(q.result);q.onerror=()=>{_db=null;rej(q.error)}}))}
const idbPut=(st,k,v)=>idb().then(d=>new Promise((res,rej)=>{const t=d.transaction(st,'readwrite');t.objectStore(st).put(v,k);t.oncomplete=res;t.onerror=()=>rej(t.error);t.onabort=()=>rej(t.error)}));
const idbClear=()=>idb().then(d=>new Promise((res,rej)=>{const t=d.transaction(['meta','blk'],'readwrite');t.objectStore('meta').clear();t.objectStore('blk').clear();t.oncomplete=res;t.onerror=()=>rej(t.error)}));
const idbMeta=()=>idb().then(d=>new Promise((res,rej)=>{const q=d.transaction('meta').objectStore('meta').openCursor();q.onsuccess=()=>{const c=q.result;res(c?Object.assign({id:c.key},c.value):null)};q.onerror=()=>rej(q.error)}));
const idbKeys=id=>idb().then(d=>new Promise((res,rej)=>{const q=d.transaction('blk').objectStore('blk').getAllKeys(IDBKeyRange.bound(id+':',id+':\uffff'));q.onsuccess=()=>res(q.result);q.onerror=()=>rej(q.error)}));
/* ---------- small helpers ---------- */
async function keepAwake(on){try{if(on&&navigator.wakeLock&&!wake)wake=await navigator.wakeLock.request('screen');else if(!on&&wake){await wake.release();wake=null}}catch{}}
const fmt=s=>{if(!Number.isFinite(s))return '--:--';s=Math.max(0,Math.round(s));const h=Math.floor(s/3600),m=Math.floor(s%3600/60);return (h?h+':'+String(m).padStart(2,'0'):m)+':'+String(s%60).padStart(2,'0')};
function spin(on,txt){$('spinner').style.display=on?'flex':'none';if(txt)$('spinText').textContent=txt}
function xferInfo(done,total,label,speed){const sp=speed||done/((Date.now()-tStart)/1000||1),eta=(total-done)/(sp||1);progress(label+' '+Math.round(done/total*100)+'% — '+(sp/1048576).toFixed(1)+' MB/s — متبقي '+fmt(eta),done/total)}
function blockOrder(size){const N=Math.ceil(size/BS),T=Math.min(N,TAILB),o=[];for(let i=N-T;i<N;i++)o.push(i);for(let i=0;i<N-T;i++)o.push(i);return o}
const blockLen=(size,n)=>Math.min(BS,size-n*BS);
/* ==========================================================================================
   SENDER (host)
   ========================================================================================== */
async function readBlock(file,n){
 const o=n*BS,end=Math.min(o+BS,file.size);let last;
 for(let a=0;a<3;a++){try{return await file.slice(o,end).arrayBuffer()}catch(e){last=e;await sleep(250*(a+1))}}
 try{const out=new Uint8Array(end-o);for(let p=o;p<end;p+=65536){out.set(new Uint8Array(await file.slice(p,Math.min(p+65536,end)).arrayBuffer()),p-o)}return out.buffer}catch(e){last=e}
 const err=new Error((last&&last.name)||'READ');err.code='READ';throw err;
}
function regFileConn(inc){
 const t=inc.metadata.tid,i=inc.metadata.idx||0;
 if(!tx)for(const k in fileConns)if(k!==t){Object.values(fileConns[k]).forEach(c=>{try{c.close()}catch{}});delete fileConns[k]}
 (fileConns[t]=fileConns[t]||{})[i]=inc;
}
function dropFileConns(){Object.values(fileConns).forEach(m=>Object.values(m).forEach(c=>{try{c.close()}catch{}}));fileConns={}}
function txEnd(){if(tx&&tx.finish){const f=tx.finish;tx.finish=null;f()}}
function txAbort(msg){if(!tx)return;tx.abort=true;if(msg)sys(msg);txEnd()}
function txFail(e){if(!tx||tx.abort)return;tx.abort=true;sys(e&&e.code==='READ'?READ_HELP:'فشل الإرسال: '+((e&&e.message)||e));send({type:'TX_ERR',code:e&&e.code});txEnd()}
async function sendFile(file){
 if(sending){sys('يوجد إرسال جارٍ بالفعل.');return}if(!conn||!conn.open)return;
 sending=true;fileReady=null;
 try{
  send({type:'FILE_META',name:file.name,size:file.size,mime:file.type,hash:HASH_OK});
  const rd=await new Promise((res,rej)=>{fileReady=res;setTimeout(()=>rej(new Error('الطرف الآخر لم يجهز لاستقبال الملف')),25000)});
  fileReady=null;if(rd.abort)throw new Error('الطرف الآخر أوقف استقبال الملف (مساحة أو خطأ).');
  const have=rd.have||'',N=Math.ceil(file.size/BS);let total=0,skipped=0;
  for(let n=0;n<N;n++){if(have[n]==='1')skipped++;else total+=blockLen(file.size,n)}
  const myTx={file,tid:rd.tid,abort:false,q:{},run:{},sent:0,total,lastUi:0,cs:{},hash:HASH_OK&&rd.hash!==false,t0:Date.now(),finish:null};
  tx=myTx;tStart=Date.now();keepAwake(true);
  if(skipped)sys('صاحبك عنده '+skipped+' جزء من الفيلم من قبل، هيكمّل من عنده.');
  if(total===0){sys('صاحبك عنده الفيلم كله. جاهز.');}else sys('بيتبعت الفيلم لصاحبك. استنى رسالة «جاهز» وبعدها اضغط تشغيل، والباقي هيتحمل أثناء المشاهدة.');
  await new Promise(res=>{myTx.finish=res;if(total===0)res()});
 }catch(e){sys('فشل الإرسال: '+((e&&e.message)||e))}
 tx=null;keepAwake(false);sending=false;
}
function hostReq(d){
 if(!tx||tx.abort||d.tid!==tx.tid||!Array.isArray(d.blocks))return;
 const j=d.j;tx.q[j]=d.replace?d.blocks.slice():(tx.q[j]||[]).concat(d.blocks);
 if(!tx.run[j])runWorker(tx,j);
}
async function runWorker(T,j){
 T.run[j]=true;
 try{
  while(T===tx&&!T.abort&&T.q[j]&&T.q[j].length){
   const n=T.q[j].shift(),fc=(fileConns[T.tid]||{})[j];
   if(!fc||!fc.open){T.q[j]=[];break}               // that channel is gone: guest re-requests elsewhere
   if(n<0||n*BS>=T.file.size)continue;
   await sendBlock(T,fc,j,n);
  }
 }catch(e){if(T===tx)txFail(e)}
 T.run[j]=false;
}
async function sendBlock(T,fc,j,n){
 const len=blockLen(T.file.size,n),buf=await readBlock(T.file,n),dc=()=>fc.open&&fc.dataChannel;
 if(T.hash){const h=new Uint8Array(await crypto.subtle.digest('SHA-256',buf)),f=new Uint8Array(5+32),dv=new DataView(f.buffer);f[0]=2;dv.setUint32(1,n);f.set(h,5);if(dc())fc.send(f)}
 const cs=T.cs[j]||(T.cs[j]={t:Date.now(),b:0,chunk:32768});
 for(let p=0;p<len;p+=cs.chunk){
  while(dc()&&(dc().bufferedAmount>HIGH||(fc.bufferSize||0)>64)&&!T.abort)await sleep(5);
  if(T.abort||!dc())return;
  const m=Math.min(cs.chunk,len-p),f=new Uint8Array(HDR+m),dv=new DataView(f.buffer);
  f[0]=1;dv.setUint32(1,n);dv.setUint32(5,p);f.set(new Uint8Array(buf,p,m),HDR);
  if(window.__corrupt&&Math.random()<window.__corrupt)f[HDR+3]^=0xFF;          // test hook only
  fc.send(f);
 }
 T.sent+=len;cs.b+=len;
 const now=Date.now();
 if(now-cs.t>2000){const rate=cs.b/((now-cs.t)/1000);let c=rate>6e6?65536:rate<4e5?16384:32768;const mm=fc.peerConnection&&fc.peerConnection.sctp&&fc.peerConnection.sctp.maxMessageSize;if(mm&&c+HDR>mm)c=Math.max(16384,mm-HDR);cs.chunk=c;cs.t=now;cs.b=0}
 if(now-T.lastUi>500){T.lastUi=now;xferInfo(Math.min(T.sent,T.total),Math.max(T.total,1),'إرسال')}
}
/* ==========================================================================================
   RECEIVER (guest)
   ========================================================================================== */
function closeConns(r){(r.conns||[]).forEach(c=>{try{c.pc.close()}catch{}});r.conns=[]}
async function startRx(d){
 await swInit;
 if(rx){rx.dead=true;clearInterval(rx.timer);closeConns(rx)}
 if(!swOK||!window.indexedDB){sys('التشغيل أثناء التحميل يحتاج HTTPS + Service Worker (ملف cowatch-sw.js بجانب الصفحة). افتح الرابط على Chrome.');send({type:'FILE_ABORT'});return}
 try{if(navigator.storage&&navigator.storage.persist)navigator.storage.persist()}catch{}
 const r={name:d.name,size:d.size,mime:d.mime||'video/mp4',N:Math.ceil(d.size/BS),dead:false,gate:true,started:false,meta:false,finished:false,force:false,
  tid:'t'+Date.now()+Math.random().toString(36).slice(2,6),wire:0,lastWire:0,lastT:Date.now(),speed:0,haveBytes:0,written:0,head:0,pendingW:0,q:Promise.resolve(),
  conns:[],nextIdx:0,need:null,pos:null,lastBase:null,lowMode:false,ahead:0,targetAhead:baseBufSec(),dur:0,badBlocks:{},recon:0,readySent:false,
  tune:{phase:'idle',next:Date.now()+4000,lock:0,base:0,added:null},hashWanted:!!d.hash&&HASH_OK,lastUi:0};
 r.have=new Uint8Array(r.N);
 try{
  const m=await idbMeta();
  if(m&&m.name===d.name&&m.size===d.size){r.id=m.id;(await idbKeys(r.id)).forEach(k=>{const n=parseInt(String(k).split(':')[1],10);if(n>=0&&n<r.N)r.have[n]=1})}
  else{await idbClear();r.id='m'+Date.now();await idbPut('meta',r.id,{name:d.name,size:d.size,mime:r.mime,bs:BS})}
 }catch(e){sys('تعذّر فتح تخزين المتصفح (IndexedDB): '+((e&&e.message)||e));send({type:'FILE_ABORT'});return}
 for(let n=0;n<r.N;n++)if(r.have[n]){r.written++;r.haveBytes+=blockLen(d.size,n)}
 while(r.head<r.N&&r.have[r.head])r.head++;
 if(r.written>0&&r.written<r.N)sys('هيكمّل التحميل من اللي وصل قبل كده ('+Math.round(r.haveBytes/d.size*100)+'%).');
 try{const e=await navigator.storage.estimate(),need=(r.N-r.written)*BS*1.05;if(e&&e.quota&&e.quota-e.usage<need){sys('مساحة التخزين على جهازك لا تكفي: المتاح '+((e.quota-e.usage)/1e9).toFixed(1)+' جيجا والمطلوب '+(need/1e9).toFixed(1)+' جيجا. فرّغ مساحة وأعيدوا الإرسال (هيكمّل من اللي وصل).');send({type:'FILE_ABORT'});return}}catch{}
 rx=r;window.__lastRx=r;keepAwake(true);tStart=Date.now();markLocalBuf(true);spin(true,'جارٍ تحميل بداية الفيلم...');
 r.timer=setInterval(()=>tick(r),500);
 if(r.written===r.N){send({type:'FILE_READY',tid:r.tid,have:Array.from(r.have).join('')});doneProg(r);return}
 openConn(r);
 setTimeout(()=>{if(!r.dead&&!r.readySent)abortRx(r,'تعذّر فتح قنوات نقل الملف. تأكد من اتصال الاتنين وأعيدوا الإرسال.')},9000);
}
function openConn(r){
 const idx=r.nextIdx++,pc=peer.connect(PEER_PREFIX+roomCode,{serialization:'raw',reliable:true,metadata:{kind:'file',tid:r.tid,idx}});
 const c={idx,pc,open:false,out:[],asm:new Map(),hash:{}};r.conns.push(c);
 pc.on('open',()=>{c.open=true;r.recon=0;if(!r.readySent){r.readySent=true;send({type:'FILE_READY',tid:r.tid,hash:r.hashWanted,have:Array.from(r.have).join('')})}refill(r,false)});
 pc.on('data',b=>onFrame(r,c,b));
 pc.on('close',()=>{c.open=false;r.conns=r.conns.filter(x=>x!==c);connLost(r)});
 pc.on('error',()=>{});
 return c;
}
function connLost(r){
 if(r.dead||r.finished||r.conns.some(x=>x.open))return;
 if(!conn||!conn.open)return;                       // main link is down too: the rejoin flow restarts the transfer
 if(++r.recon>6){abortRx(r,'تعذّر إعادة فتح قنوات النقل. أعيدوا الإرسال وهيكمّل من اللي وصل.',true);return}
 setTimeout(()=>{if(!r.dead&&!r.finished&&conn&&conn.open&&!r.conns.some(x=>x.open))openConn(r)},800*r.recon);
}
function closeConn(r,c){try{c.pc.close()}catch{}r.conns=r.conns.filter(x=>x!==c)}
function abortRx(r,m,quiet){if(r.dead)return;r.dead=true;clearInterval(r.timer);if(rx===r)rx=null;if(m)sys(m);spin(false);$('startNowBtn').classList.add('hidden');keepAwake(false);markLocalBuf(false);if(!quiet)send({type:'FILE_ABORT'});closeConns(r)}
/* ---------- receiving frames ---------- */
function onFrame(r,c,buf){
 if(r.dead||r!==rx||!buf)return;
 const u=buf instanceof ArrayBuffer?new Uint8Array(buf):ArrayBuffer.isView(buf)?new Uint8Array(buf.buffer,buf.byteOffset,buf.byteLength):null;
 if(!u||u.length<5)return;
 const dv=new DataView(u.buffer,u.byteOffset,u.byteLength),t=u[0],n=dv.getUint32(1);
 if(n>=r.N)return;
 r.wire+=u.length;
 if(t===2){if(u.length>=37)c.hash[n]=u.slice(5,37);return}
 if(t!==1||u.length<=HDR||r.have[n])return;                      // duplicates of stored blocks are ignored
 const off=dv.getUint32(5),len=blockLen(r.size,n),pl=u.length-HDR;
 if(off+pl>len)return;                                            // malformed
 let a=c.asm.get(n);if(!a||off===0){a={buf:new Uint8Array(len),fill:0,cov:new Set()};c.asm.set(n,a)}
 if(a.cov.has(off))return;                                        // duplicate frame
 a.cov.add(off);a.buf.set(u.subarray(HDR),off);a.fill+=pl;        // offsets make order irrelevant
 if(a.fill>=len){c.asm.delete(n);blockDone(r,c,n,a.buf)}
}
async function blockDone(r,c,n,buf){
 const exp=c.hash[n];delete c.hash[n];
 if(exp&&HASH_OK){
  const h=new Uint8Array(await crypto.subtle.digest('SHA-256',buf));let ok=h.length===exp.length;for(let i=0;ok&&i<h.length;i++)if(h[i]!==exp[i])ok=false;
  if(!ok){r.badBlocks[n]=(r.badBlocks[n]||0)+1;r.badTotal=(r.badTotal||0)+1;c.out=c.out.filter(x=>x!==n);
   if(r.badBlocks[n]>4){abortRx(r,'تلف متكرر في جزء من الملف. أعيدوا الإرسال.');return}
   if(r.badTotal===1)sys('اكتُشف جزء تالف أثناء النقل وهيتبعت تاني تلقائياً.');
   return}
 }
 if(r.dead||r.have[n])return;
 r.pendingW++;
 r.q=r.q.then(()=>idbPut('blk',r.id+':'+n,buf.buffer)).then(()=>{
  r.pendingW--;if(r.dead||r.have[n])return;
  r.have[n]=1;r.written++;r.haveBytes+=buf.length;while(r.head<r.N&&r.have[r.head])r.head++;
  r.conns.forEach(x=>{x.out=x.out.filter(y=>y!==n)});if(r.need===n)r.need=null;
  if(r.written===r.N)doneProg(r);
 }).catch(e=>{r.pendingW--;abortRx(r,'فشل الحفظ على التخزين: '+((e&&(e.message||e.name))||e)+'. تأكد من وجود مساحة كافية، وأعيدوا الإرسال وهيكمّل من اللي وصل.')});
}
function doneProg(r){if(r.finished)return;r.finished=true;send({type:'FILE_DONE',tid:r.tid});closeConns(r);progress('اكتمل التحميل ✓',1);keepAwake(false);sys('اكتمل تحميل الفيلم بالكامل.');if(!r.gate&&rx===r)rx=null}
/* ---------- what do we need next? (priority) ---------- */
function tailOK(r){for(let n=r.N-Math.min(r.N,TAILB);n<r.N;n++)if(!r.have[n])return false;return true}
function wantList(r,excl,max){
 const out=[],N=r.N,have=r.have,add=n=>{if(!have[n]&&!excl.has(n)){out.push(n)}return out.length>=max};
 if(!r.started){                                   // phase A: moov may be at the end -> tail first, then the head
  for(let n=N-Math.min(N,TAILB);n<N;n++)if(add(n))return out;
  for(let n=0;n<N;n++)if(add(n))return out;return out;
 }
 const base=r.need!=null?r.need:r.pos!=null?r.pos:r.head;
 for(let n=base;n<N;n++)if(add(n))return out;
 for(let n=0;n<base;n++)if(add(n))return out;
 return out;
}
function refill(r,replace){
 if(r.dead||r.finished)return;
 const open=r.conns.filter(c=>c.open);if(!open.length)return;
 if(!replace&&r.pendingW>24)return;               // storage can't keep up: don't pile more in RAM
 const act=r.lowMode?open.slice(0,1):open,O=Math.max(3,Math.min(10,Math.ceil((r.speed/Math.max(1,act.length))*0.6/BS)+2)),O2=r.lowMode?2:O;
 if(replace)open.forEach(c=>{c.out=[]});
 const excl=new Set();r.conns.forEach(c=>{c.out.forEach(n=>excl.add(n));c.asm.forEach((a,n)=>excl.add(n))});
 const room=act.map(c=>Math.max(0,O2-c.out.length)),want=room.reduce((a,b)=>a+b,0);
 const list=want?wantList(r,excl,want):[],take=act.map(()=>[]);let i=0,guard=0;
 while(i<list.length&&guard++<1000){let placed=false;for(let k=0;k<act.length&&i<list.length;k++){if(take[k].length<room[k]){take[k].push(list[i++]);placed=true}}if(!placed)break}
 open.forEach(c=>{const k=act.indexOf(c),blocks=k>=0?take[k]:[];if(blocks.length||replace){send({type:'REQ',tid:r.tid,j:c.idx,blocks,replace:!!replace});c.out.push(...blocks)}});
}
function onNeed(r,n){if(r.dead||r.have[n]||n>=r.N)return;if(r.need!==n){r.need=n;refill(r,true)}}
function onPos(r,n){if(r.dead||n>=r.N)return;const jump=r.pos==null||Math.abs(n-r.pos)>12;r.pos=n;if(jump&&r.started)refill(r,true)}
function onSwMsg(e){const d=e.data,r=rx;if(!d||!r||d.id!==r.id)return;if(d.type==='NEED')onNeed(r,d.block);else if(d.type==='POS')onPos(r,d.block)}
/* ---------- buffer maths (shared with the sync/rebuffer logic) ---------- */
function bytePosOf(r){const d=r.dur;return d>0?Math.min(r.size-1,Math.floor(video.currentTime/d*r.size)):0}
function aheadSecOf(r){
 if(!r||!(r.dur>0))return Infinity;
 const b=r.size/r.dur,p=bytePosOf(r);let n=Math.floor(p/BS);if(!r.have[n])return 0;
 let k=n;while(k<r.N&&r.have[k])k++;return Math.max(0,Math.min(r.size,k*BS)-p)/b;
}
/* ---------- the 500 ms brain ---------- */
function autotune(r,now){
 const t=r.tune;if(now<t.next||r.lowMode||r.finished||r.N-r.written<MAXK*6)return;
 const open=r.conns.filter(c=>c.open).length;
 if(t.phase==='idle'){
  if(open>=1&&open<MAXK&&now>t.lock&&r.speed>0){t.base=r.speed;t.added=openConn(r);t.phase='probe';t.next=now+7000}else t.next=now+2000;
 }else{                                           // more channels are kept ONLY if they measurably help
  if(r.speed>t.base*1.15){t.phase='idle';t.next=now+4000}
  else{if(t.added)closeConn(r,t.added);t.added=null;t.lock=now+120000;t.phase='idle';t.next=now+8000}
 }
}
function tick(r){
 if(r.dead){clearInterval(r.timer);return}
 if(r.finished&&!r.gate){clearInterval(r.timer);return}
 const now=Date.now(),dt=Math.max(0.05,(now-r.lastT)/1000),inst=(r.wire-r.lastWire)/dt;r.lastWire=r.wire;r.lastT=now;r.speed=r.speed?r.speed*0.75+inst*0.25:inst;
 if(r.meta&&Number.isFinite(video.duration)&&video.duration>0)r.dur=video.duration;
 const b=r.dur>0?r.size/r.dur:0;
 if(b){                                           // adaptive read-ahead (slow link: bigger target; fast + full: calmer)
  r.ahead=aheadSecOf(r);r.targetAhead=r.speed<1.5*b?2*baseBufSec():baseBufSec();
  if(!r.lowMode&&r.started&&!r.gate&&r.speed>4*b&&r.ahead>=r.targetAhead&&!r.finished)r.lowMode=true;
  else if(r.lowMode&&(r.ahead<0.7*r.targetAhead||r.finished))r.lowMode=false;
 }
 autotune(r,now);
 refill(r,false);
 if(r.gate){
  if(!r.started&&r.head>=Math.min(r.N,24)&&tailOK(r)){r.started=true;startProgPlay(r)}
  if(r.started&&r.meta&&r.dur>0){
   const have=Math.min(r.size,r.head*BS);
   let need=Math.min(r.size,baseBufSec()*b);                                  // the first 15 minutes of film
   need=Math.max(need,Math.min(r.size,r.size-0.85*r.speed*r.dur));          // slow link: enough head start to never stall
   $('startNowBtn').classList.remove('hidden');
   spin(true,'جارٍ تجهيز بداية الفيلم ('+fmt(Math.min(have,need)/b)+' من '+fmt(need/b)+') — '+(r.speed/1048576).toFixed(1)+' MB/s');
   if(have>=need||r.force||r.finished)openGate(r);
  }else spin(true,'جارٍ تحميل بداية الفيلم... '+Math.round(r.haveBytes/r.size*100)+'%');
 }
 if(now-r.lastUi>900){
  r.lastUi=now;const left=r.size-r.haveBytes;
  progress((r.finished?'اكتمل ✓':'تحميل '+Math.round(r.haveBytes/r.size*100)+'% — '+(r.speed/1048576).toFixed(1)+' MB/s — متبقي '+fmt(left/(r.speed||1)))+(r.lowMode?' (هادئ)':''),r.haveBytes/r.size);
  $('bufInfo').textContent='المخزن: '+(r.dur>0?fmt(Math.min(r.ahead,9e4)):'--')+' | قنوات: '+r.conns.filter(c=>c.open).length+(r.lowMode?' (توفير)':'');
 }
}
function startProgPlay(r){
 stopHls();video.srcObject=null;hidePlayers();video.style.display='block';video.controls=true;video.preload='auto';activeMode='file';
 video.addEventListener('loadedmetadata',()=>{r.meta=true},{once:true});
 video.src=new URL('cw-stream?id='+r.id,location.href).href;
 $('modeBadge').textContent='الوضع: ملف — '+r.name;$('qualityBadge').textContent='الجودة: أصلية 100%';
}
function openGate(r){
 r.gate=false;$('startNowBtn').classList.add('hidden');spin(false);if(r.finished&&rx===r)rx=null;
 send({type:'RX_READY'});sys('جاهز للمشاهدة. باقي الفيلم بيتحمل أثناء التشغيل.');markLocalBuf(false);
 if(pendingSync){const s=pendingSync;pendingSync=null;applySync(s)}else send({type:'SYNC_REQUEST'});
}
$('startNowBtn').onclick=()=>{if(rx&&rx.gate)rx.force=true};
