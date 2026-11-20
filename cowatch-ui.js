'use strict';
/* ==========================================================================================
   CoWatch v6 - shared state, UI helpers, synchronisation + buffering coordination, media loading
   ========================================================================================== */
const PEER_PREFIX='cowatch-v6-room-';
const PEER_OPTS={config:{iceServers:[{urls:'stun:stun.l.google.com:19302'},{urls:'stun:stun.cloudflare.com:3478'},{urls:'turn:openrelay.metered.ca:80',username:'openrelayproject',credential:'openrelayproject'},{urls:'turn:openrelay.metered.ca:443?transport=tcp',username:'openrelayproject',credential:'openrelayproject'}]}};
let peer=null,conn=null,mediaCall=null,isHost=false,roomCode='',myName='',activeMode='html5',mediaUrl='',extUrl='',ytPlayer=null,ytReady=false,pendingYt=null,hls=null,objUrl=null,localFile=null,rx=null,pendingSync=null,rtt=0,ytLast=0,sending=false,joined=false;
const $=id=>document.getElementById(id),video=$('player'),ytBox=$('ytPlayerContainer'),frame=$('externalFrame'),overlay=$('autoplayOverlay'),messages=$('messages');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const params=new URLSearchParams(location.search),inviteCode=(params.get('room')||'').toUpperCase().replace(/[^A-Z0-9]/g,'').slice(0,6);
$('displayName').value='مشاهد-'+Math.floor(100+Math.random()*899);if(inviteCode.length===6)$('roomCodeInput').value=inviteCode;
if(location.protocol==='file:')setTimeout(()=>showError('تنبيه: لازم ترفع الملفات على استضافة HTTPS (Netlify أو GitHub Pages) عشان التشغيل أثناء التحميل ورابط الدعوة يشتغلوا.'),0);
if(!window.Peer)setTimeout(()=>showError('تعذر تحميل مكتبة PeerJS. تأكد من الإنترنت وأن الموقع غير محجوب.'),0);
const isMedia=()=>activeMode==='html5'||activeMode==='file';
/* ---------- YouTube ---------- */
window.onYouTubeIframeAPIReady=()=>{ytPlayer=new YT.Player('ytPlayer',{width:'100%',height:'100%',playerVars:{playsinline:1,rel:0},events:{onReady:()=>{ytReady=true;if(pendingYt){ytPlayer.loadVideoById(pendingYt);pendingYt=null}},onStateChange:e=>{if(activeMode!=='youtube'||ytLock)return;if(e.data===1){wantPlay=true;sendSync()}else if(e.data===2){wantPlay=false;sendSync()}}}})};
let ytLock=false,ytLockT=0;const setYtLock=()=>{ytLock=true;clearTimeout(ytLockT);ytLockT=setTimeout(()=>ytLock=false,900)};
function ytId(u){const m=u.match(/(?:youtube\.com\/(?:watch\?(?:.*&)?v=|embed\/|shorts\/)|youtu\.be\/)([A-Za-z0-9_-]{11})/);return m?m[1]:null}
function validHttps(raw){try{const u=new URL(raw.trim());return u.protocol==='https:'?u.href:null}catch{return null}}
/* ---------- UI helpers ---------- */
function showError(t){$('lobbyError').textContent=t;$('lobbyError').style.display='block'}
function appendMessage(s,t,type){const d=document.createElement('div');d.className='msg '+type;if(type!=='sys'){const b=document.createElement('b');b.textContent=s;b.style.cssText='display:block;font-size:11px';d.appendChild(b)}d.appendChild(document.createTextNode(t));messages.appendChild(d);messages.scrollTop=messages.scrollHeight}
const sys=t=>appendMessage('',t,'sys');
const send=d=>{try{if(conn&&conn.open)conn.send(d)}catch(e){console.warn(e)}};
function setView(m){$('videoToolbar').style.display=m==='video'?'flex':'none';$('externalToolbar').style.display=m==='external'?'flex':'none';$('externalNote').style.display=m==='external'?'block':'none';document.querySelectorAll('.tab').forEach(b=>b.classList.toggle('active',b.dataset.mode===m))}
document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>setView(b.dataset.mode));
function stopHls(){if(hls){hls.destroy();hls=null}}
function hidePlayers(){video.pause();video.style.display='none';ytBox.style.display='none';frame.style.display='none';overlay.style.display='none';$('streamControls').classList.add('hidden')}
function progress(txt,p){$('transfer').classList.remove('hidden');$('transferText').textContent=txt;$('transferBar').style.width=Math.round(Math.min(1,p)*100)+'%';if(p>=1)setTimeout(()=>{if(!rx&&!tx)$('transfer').classList.add('hidden')},2500)}
/* ==========================================================================================
   Synchronisation
   - every SYNC carries a sequence number (stale / looping messages are dropped)
   - remote actions mark the next matching media event to be ignored (no echo loops)
   - drift: <0.15 s ignored, 0.15..1 s corrected by a gentle playback-rate nudge, >1 s by a seek
   - buffering: if either side stalls (or is still preparing) the other pauses; both resume together
   ========================================================================================== */
let wantPlay=false,localBuf=false,peerBuf=false,pausedByBuf=false,mySeq=0,peerSeq=0,rebuf=false,waitTimer=0;const rttS=[];const skipE={};
const median=a=>{const s=[...a].sort((x,y)=>x-y);return s.length?s[s>>1]:0};
function getState(){if(activeMode==='youtube'&&ytReady)return{t:ytPlayer.getCurrentTime()||0,p:ytPlayer.getPlayerState()===1||(wantPlay&&(localBuf||peerBuf))};return{t:video.currentTime||0,p:!video.paused||(pausedByBuf&&wantPlay)}}
function sendSync(){if(!isMedia()&&activeMode!=='youtube')return;send({type:'SYNC',...getState(),mode:activeMode,n:++mySeq})}
function onPong(t){const x=Math.min(Date.now()-t,3000);rttS.push(x);if(rttS.length>7)rttS.shift();rtt=median(rttS)}
function updateBufUI(){$('syncBadge').textContent=peerBuf?'المزامنة: بننتظر صاحبك ⏳':localBuf?'المزامنة: جارٍ التحميل ⏳':'المزامنة: متصلة ✓'}
function markLocalBuf(on){if(localBuf===on)return;localBuf=on;send({type:'BUF',on});if(on)autoPause();else maybeResume();updateBufUI()}
function onPeerBuf(on){if(peerBuf===on)return;peerBuf=on;if(on)autoPause();else maybeResume();updateBufUI()}
function autoPause(){
 if(activeMode==='youtube'){if(ytReady&&ytPlayer.getPlayerState()===1){setYtLock();wantPlay=true;pausedByBuf=true;ytPlayer.pauseVideo()}return}
 if(!isMedia())return;
 if(!video.paused){skipE.pause=Date.now()+1500;wantPlay=true;pausedByBuf=true;video.pause()}
}
function maybeResume(){
 if(localBuf||peerBuf)return;
 if(!(pausedByBuf||wantPlay))return;
 const d=Math.min(rtt/2,400);        // the side that finished last waits half an RTT so both start together
 setTimeout(()=>{
  if(localBuf||peerBuf)return;pausedByBuf=false;
  if(wantPlay){
   if(activeMode==='youtube'){if(ytReady){setYtLock();ytPlayer.playVideo()}}
   else if(isMedia()&&video.paused){skipE.play=Date.now()+1500;video.play().catch(()=>overlay.style.display='flex')}
  }
  if(isHost)setTimeout(sendSync,700);
 },d);
}
function applySync(s){
 if(s.n!=null){if(s.n<=peerSeq&&peerSeq-s.n<50)return;peerSeq=s.n}
 if(s.mode&&s.mode!==activeMode)return;
 wantPlay=!!s.p;
 const target=s.t+(s.p?rtt/2000:0);
 if(activeMode==='youtube'){
  if(!ytReady)return;setYtLock();
  if(Math.abs(ytPlayer.getCurrentTime()-target)>1)ytPlayer.seekTo(target,true);
  const st=ytPlayer.getPlayerState();
  if(s.p&&!(localBuf||peerBuf)&&st!==1)ytPlayer.playVideo();if(!s.p&&st===1)ytPlayer.pauseVideo();return;
 }
 if(!isMedia())return;
 if((rx&&rx.gate)||video.readyState<1){pendingSync=s;return}
 const dlt=target-video.currentTime,big=Math.abs(dlt)>1,hold=localBuf||peerBuf,needPlay=s.p&&video.paused&&!hold,needPause=!s.p&&!video.paused;
 const T=Date.now()+1500;if(big)skipE.seeked=T;if(needPlay)skipE.play=T;if(needPause)skipE.pause=T;
 if(big)video.currentTime=target;
 if(!big&&s.p&&!video.paused&&Math.abs(dlt)>0.15)video.playbackRate=1+Math.max(-0.08,Math.min(0.08,dlt*0.1));else video.playbackRate=1;
 if(needPlay)video.play().catch(()=>overlay.style.display='flex');
 if(needPause){video.pause();video.playbackRate=1;pausedByBuf=false}
 updateBufUI();
}
['play','pause','seeked'].forEach(ev=>video.addEventListener(ev,()=>{
 const t=skipE[ev];if(t&&Date.now()<t){skipE[ev]=0;return}
 if(!isMedia())return;
 if(ev==='play'){wantPlay=true;if(localBuf||peerBuf){skipE.pause=Date.now()+1500;video.pause();sendSync();return}}   // somebody is still buffering: wait for them
 if(ev==='pause'){wantPlay=false;pausedByBuf=false}
 sendSync();
}));
/* rebuffer policy: if playback starves, pause BOTH sides until enough data is ahead, then resume together */
function stallStart(){if(rebuf||!isMedia())return;rebuf=true;spin(true,'جارٍ تحميل الجزء المطلوب...');markLocalBuf(true)}
function stallEnd(){if(!rebuf)return;rebuf=false;spin(false);markLocalBuf(false)}
video.addEventListener('waiting',()=>{if(!isMedia()||(rx&&rx.gate))return;clearTimeout(waitTimer);waitTimer=setTimeout(()=>{if(video.readyState<3)stallStart()},700)});
['playing','canplay'].forEach(ev=>video.addEventListener(ev,()=>{clearTimeout(waitTimer);if(rx&&rx.gate)return;if(!rx||rx.finished)stallEnd();else if(!rebuf)spin(false)}));
video.addEventListener('seeking',()=>{if(rx&&!rx.gate&&rx.dur>0&&!rx.have[Math.min(rx.N-1,Math.floor(video.currentTime/rx.dur*rx.size/BS))])spin(true,'الجزء ده لسه ما اتحملش... بنجيبه الأول')});
video.addEventListener('loadedmetadata',()=>{if(pendingSync&&!(rx&&rx.gate)){const s=pendingSync;pendingSync=null;applySync(s)}});
setInterval(()=>{                                 // 500 ms: rebuffer exit check
 if(!rebuf)return;
 const r=rx;
 if(!r||r.finished){if(video.readyState>=3)stallEnd();return}
 const b=r.size/r.dur,need=r.speed>=b?12:Math.min(60,20+(b/Math.max(r.speed,1))*5);
 if(r.have[Math.min(r.N-1,Math.floor(bytePosOf(r)/BS))]&&aheadSecOf(r)>=need&&video.readyState>=2)stallEnd();
},500);
setInterval(()=>{
 if(!conn||!conn.open)return;
 send({type:'PING',t:Date.now()});
 if(isHost&&getState().p&&!(localBuf||peerBuf))sendSync();
 if(activeMode==='youtube'&&ytReady&&!ytLock){const c=ytPlayer.getCurrentTime();if(Math.abs(c-ytLast-2.5)>2&&ytLast)sendSync();ytLast=c}
},2500);
/* ---------- Media loading ---------- */
function resetForNewSource(){if(rx)abortRx(rx,'',true);if(tx)txAbort('تم تغيير مصدر الفيديو.');rebuf=false;wantPlay=false;pausedByBuf=false;pendingSync=null;video.playbackRate=1}
function loadMediaSource(url,broadcast=false){
 const safe=validHttps(url);if(!safe){alert('استخدم رابط HTTPS صحيحاً.');return}
 resetForNewSource();
 const id=ytId(safe);stopHls();video.srcObject=null;video.controls=true;frame.src='about:blank';hidePlayers();mediaUrl=safe;activeMode=id?'youtube':'html5';
 if(id){$('modeBadge').textContent='الوضع: YouTube';ytBox.style.display='block';if(ytReady)ytPlayer.loadVideoById(id,0);else pendingYt=id}
 else{const m3u8=/\.m3u8(\?|$)/i.test(safe);$('modeBadge').textContent=m3u8?'الوضع: HLS':'الوضع: HTML5';video.style.display='block';
  if(m3u8&&window.Hls&&Hls.isSupported()){hls=new Hls({enableWorker:true,maxBufferLength:60,maxMaxBufferLength:600});hls.on(Hls.Events.ERROR,(e,d)=>{if(d.fatal){sys('خطأ في تشغيل البث: '+d.type);if(d.type===Hls.ErrorTypes.NETWORK_ERROR)hls.startLoad();else if(d.type===Hls.ErrorTypes.MEDIA_ERROR)hls.recoverMediaError()}});hls.loadSource(safe);hls.attachMedia(video)}
  else video.src=safe}
 $('qualityBadge').textContent='الجودة: حسب المصدر';$('videoUrlInput').value=safe;$('bufInfo').textContent='';
 if(broadcast)send({type:'MEDIA_LOAD',url:safe,sender:myName});
}
video.addEventListener('error',()=>{if(isMedia()&&video.src)sys('تعذر تشغيل الفيديو (الرابط غير مباشر أو محجوب CORS أو الصيغة غير مدعومة).')});
function openExternal(url,broadcast=false){
 const safe=validHttps(url);if(!safe){alert('أدخل رابط HTTPS صحيحاً.');return}
 resetForNewSource();
 activeMode='external';extUrl=safe;stopHls();video.srcObject=null;if(ytReady)ytPlayer.stopVideo();hidePlayers();
 if($('sandboxChk').checked)frame.setAttribute('sandbox','allow-scripts allow-same-origin allow-forms allow-presentation');else frame.removeAttribute('sandbox');
 frame.style.display='block';frame.src=safe;$('externalUrlInput').value=safe;$('modeBadge').textContent='الوضع: موقع خارجي';setView('external');
 if(broadcast)send({type:'EXTERNAL_NAV',url:safe,sender:myName});
}
$('sandboxChk').onchange=()=>{if(activeMode==='external'&&extUrl)openExternal(extUrl)};
function playFile(file){stopHls();if(objUrl)URL.revokeObjectURL(objUrl);objUrl=URL.createObjectURL(file);video.srcObject=null;activeMode='file';hidePlayers();video.style.display='block';video.controls=true;video.src=objUrl;$('modeBadge').textContent='الوضع: ملف — '+(file.name||'');$('qualityBadge').textContent='الجودة: أصلية 100%'}
