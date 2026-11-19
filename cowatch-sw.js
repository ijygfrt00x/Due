/* CoWatch service worker: serves a movie that is still being downloaded (HTTP Range) from IndexedDB blocks. */
'use strict';
let dbp=null;
function db(){return dbp||(dbp=new Promise((res,rej)=>{const q=indexedDB.open('cowatch',1);q.onupgradeneeded=()=>{q.result.createObjectStore('meta');q.result.createObjectStore('blk')};q.onsuccess=()=>res(q.result);q.onerror=()=>{dbp=null;rej(q.error)}}))}
const get=(st,k)=>db().then(d=>new Promise((res,rej)=>{const q=d.transaction(st).objectStore(st).get(k);q.onsuccess=()=>res(q.result);q.onerror=()=>rej(q.error)}));
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
self.addEventListener('install',()=>self.skipWaiting());
self.addEventListener('activate',e=>e.waitUntil(self.clients.claim()));
self.addEventListener('fetch',e=>{
  const u=new URL(e.request.url);
  if(!u.pathname.endsWith('/cw-stream'))return;
  const p=serve(e.request,u).catch(()=>new Response('error',{status:500}));
  e.respondWith(p);e.waitUntil(p);
});
async function serve(req,u){
  const id=u.searchParams.get('id'),m=await get('meta',id);
  if(!m)return new Response('not found',{status:404});
  const size=m.size,bs=m.bs;
  let start=0,end=size-1;
  const h=req.headers.get('range'),mm=h&&/bytes=(\d*)-(\d*)/.exec(h);
  if(mm){
    if(mm[1]===''){start=Math.max(0,size-parseInt(mm[2],10))}
    else{start=parseInt(mm[1],10);if(mm[2]!=='')end=Math.min(parseInt(mm[2],10),size-1)}
  }
  if(start>=size||start>end)return new Response(null,{status:416,headers:{'Content-Range':'bytes */'+size}});
  const first=Math.floor(start/bs),t0=Date.now();
  let blk=await get('blk',id+':'+first);
  while(!blk){                       // block not downloaded yet: hold the request (player shows buffering)
    if(req.signal&&req.signal.aborted)return new Response(null,{status:499});
    if(Date.now()-t0>270000)return new Response('timeout',{status:504});
    await sleep(200);blk=await get('blk',id+':'+first);
  }
  const lastWanted=Math.min(Math.floor(end/bs),first+7),blocks=[new Uint8Array(blk)];
  let last=first;
  for(let n=first+1;n<=lastWanted;n++){            // only what is already available, so we never stall mid-response
    const b=await get('blk',id+':'+n);if(!b)break;blocks.push(new Uint8Array(b));last=n;
  }
  const lastByte=Math.min(end,(last+1)*bs-1,size-1),len=lastByte-start+1,out=new Uint8Array(len);
  let pos=0,off=start-first*bs;
  for(const b of blocks){const take=Math.min(b.length-off,len-pos);if(take<=0)break;out.set(b.subarray(off,off+take),pos);pos+=take;off=0}
  return new Response(out,{status:206,headers:{
    'Content-Type':m.mime||'video/mp4','Accept-Ranges':'bytes','Content-Length':String(len),
    'Content-Range':'bytes '+start+'-'+lastByte+'/'+size,'Cache-Control':'no-store'}});
}
