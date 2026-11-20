'use strict';
/* ==========================================================================================
   CoWatch v6 - rooms, connection (with auto-rejoin), message routing, live streaming, UI events
   ========================================================================================== */
let leaving=false,rejoinTimer=0,rejoinTries=0;
const REJOIN_MAX=25;
function getCid(){let cid;try{cid=localStorage.getItem('cw-cid');if(!cid){cid=Math.random().toString(36).slice(2)+Date.now().toString(36);localStorage.setItem('cw-cid',cid)}}catch{cid=window.__cid||(window.__cid=Math.random().toString(36).slice(2))}return cid}
/* ---------- Live streaming (re-encoded in real time: lower fidelity than file mode) ---------- */
function tuneSender(call){const pc=call&&call.peerConnection;if(!pc)return;pc.getSenders().forEach(s=>{if(!s.track)return;try{const p=s.getParameters();if(!p.encodings||!p.encodings.length)p.encodings=[{}];if(s.track.kind==='video'){p.encodings[0].maxBitrate=30e6;p.encodings[0].maxFramerate=60;p.encodings[0].scaleResolutionDownBy=1;p.degradationPreference='maintain-resolution'}else{p.encodings[0].maxBitrate=192000}s.setParameters(p).catch(()=>{})}catch(e){}})}
function boostSdp(sdp){try{return sdp.replace(/m=video[\s\S]*?(?=\r?\nm=|$)/,sec=>sec.replace(/(c=IN[^\r\n]*\r?\n)/,'$1b=AS:30000\r\n').replace(/a=fmtp:(\d+) ([^\r\n]*)/g,(m,pt,pr)=>/apt=/.test(pr)?m:'a=fmtp:'+pt+' '+pr+';x-google-min-bitrate=8000;x-google-start-bitrate=15000;x-google-max-bitrate=30000'))}catch(e){return sdp}}
function startStream(stream,label){
 if(!peer||!conn||!conn.open){sys('وصّل الطرف الآخر أولاً.');return}
 try{stream.getVideoTracks().forEach(t=>{try{t.contentHint='motion'}catch{}});if(mediaCall)mediaCall.close();mediaCall=peer.call(conn.peer,stream,{metadata:{label},sdpTransform:boostSdp});mediaCall.on('close',()=>sys('انتهى البث.'));setTimeout(()=>tuneSender(mediaCall),800);setTimeout(()=>tuneSender(mediaCall),3000);send({type:'STREAM_STARTED',label,sender:myName});$('qualityBadge').textContent='الجودة: حتى 30 Mbps / 60fps حسب الشبكة';sys('بدأ البث المباشر.')}catch(e){sys('تعذر بدء البث: '+e.message)}
}
$('liveFileInput').onchange=e=>{const f=e.target.files[0];e.target.value='';if(!f)return;if(!isHost){sys('للمضيف فقط.');return}if(!f.type.startsWith('video/')){sys('الملف ليس فيديو.');return}resetForNewSource();playFile(f);activeMode='stream';$('modeBadge').textContent='الوضع: بث مباشر — '+f.name;video.onloadedmetadata=async()=>{video.onloadedmetadata=null;try{await video.play();const s=video.captureStream?video.captureStream():video.mozCaptureStream?video.mozCaptureStream():null;if(!s){sys('captureStream غير مدعوم؛ استخدم «إرسال ملف».');return}startStream(s,f.name)}catch(err){sys('تعذر التشغيل: '+err.message)}}};
$('screenShareBtn').onclick=async()=>{if(!isHost){sys('المضيف فقط يبث الشاشة.');return}if(!navigator.mediaDevices||!navigator.mediaDevices.getDisplayMedia){sys('مشاركة الشاشة غير مدعومة (غير متاحة في أغلب متصفحات الموبايل).');return}try{const s=await navigator.mediaDevices.getDisplayMedia({video:{frameRate:60},audio:true});resetForNewSource();activeMode='stream';hidePlayers();video.style.display='block';video.srcObject=s;video.muted=true;video.controls=false;await video.play().catch(()=>{});startStream(s,'مشاركة الشاشة');s.getVideoTracks()[0].addEventListener('ended',()=>{if(mediaCall)mediaCall.close();sys('انتهت مشاركة الشاشة.')})}catch(e){if(e.name!=='NotAllowedError'&&e.name!=='AbortError')sys('تعذرت المشاركة: '+e.message)}};
function onCall(call){call.answer(undefined,{sdpTransform:boostSdp});mediaCall=call;call.on('stream',r=>{resetForNewSource();stopHls();activeMode='stream';hidePlayers();video.style.display='block';video.controls=false;video.muted=false;video.srcObject=r;video.play().catch(()=>overlay.style.display='flex');$('streamControls').classList.remove('hidden');$('modeBadge').textContent='الوضع: استقبال بث مباشر';$('qualityBadge').textContent='الجودة: حسب شبكة المرسل';sys('بدأ استقبال البث.')});call.on('close',()=>{video.srcObject=null;$('streamControls').classList.add('hidden');sys('انتهى بث الطرف الآخر.')});call.on('error',e=>sys('خطأ في البث: '+e.message))}
$('streamControls').onclick=e=>{const b=e.target.closest('button[data-cmd]');if(!b)return;if(isHost)hostCmd(b.dataset.cmd);else send({type:'STREAM_COMMAND',command:b.dataset.cmd})};
function hostCmd(c){if(c==='TOGGLE'){video.paused?video.play().catch(()=>{}):video.pause()}else if(c==='BACK')video.currentTime=Math.max(0,video.currentTime-10);else if(c==='FORWARD'&&Number.isFinite(video.duration))video.currentTime=Math.min(video.duration,video.currentTime+10)}
/* ---------- Connection ---------- */
function enterRoom(code){roomCode=code;joined=true;$('lobby').style.display='none';$('room').style.display='flex';$('roomBadge').textContent='غرفة: '+code;history.replaceState({},'',location.pathname+'?room='+code)}
function onData(d){
 if(!d||typeof d!=='object'||d instanceof ArrayBuffer||ArrayBuffer.isView(d))return;
 switch(d.type){
 case 'ROOM_FULL':alert('الغرفة ممتلئة (شخصان فقط).');location.href=location.pathname;break;
 case 'HELLO':peerSeq=0;peerBuf=false;$('peerStatus').textContent='متصل مع: '+d.guestName+' (2/2)';sys(d.guestName+' انضم.');send({type:'INIT',hostName:myName,mode:activeMode,url:mediaUrl,externalUrl:extUrl,hasFile:!!localFile});
  if(localFile&&activeMode==='file'){let n=0;const t=setInterval(()=>{if(!sending||++n>30){clearInterval(t);if(!sending)sendFile(localFile)}},300)}else setTimeout(sendSync,1500);break;
 case 'INIT':peerSeq=0;$('peerStatus').textContent='متصل مع: '+d.hostName+' (2/2)';sys('تم الاتصال بالمضيف.');if(d.mode==='external'&&d.externalUrl)openExternal(d.externalUrl);else if(d.mode==='youtube'||d.mode==='html5'){if(d.url)loadMediaSource(d.url)}break;
 case 'MEDIA_LOAD':if(d.url){loadMediaSource(d.url);sys(d.sender+' اختار فيديو.')}break;
 case 'EXTERNAL_NAV':if(d.url)openExternal(d.url);break;
 case 'FILE_NOTICE':sys('المضيف سيرسل ملفاً، انتظر تجهيز البداية.');break;
 case 'FILE_META':startRx(d);break;
 case 'FILE_READY':if(fileReady)fileReady(d);break;
 case 'REQ':hostReq(d);break;
 case 'FILE_DONE':if(tx&&d.tid===tx.tid){progress('تم إرسال الملف ✓',1);sys('صاحبك استلم الفيلم كله.');txEnd()}break;
 case 'FILE_ABORT':if(tx)txAbort('الطرف الآخر أوقف استقبال الملف (مساحة أو خطأ).');else if(sending&&fileReady){const f=fileReady;fileReady=null;f({abort:true})}break;
 case 'TX_ERR':if(rx&&!rx.finished)sys('المضيف مش قادر يقرا الملف دلوقتي. هيكمّل لما يختاره تاني.');break;
 case 'RX_READY':sys('صاحبك جاهز ✓ اضغط تشغيل.');setTimeout(sendSync,300);break;
 case 'BUF':onPeerBuf(!!d.on);break;
 case 'STREAM_STARTED':sys(d.sender+' بدأ بثاً مباشراً.');break;
 case 'SYNC':applySync(d);break;
 case 'PING':send({type:'PONG',t:d.t});break;
 case 'PONG':onPong(d.t);break;
 case 'STREAM_COMMAND':if(isHost&&activeMode==='stream')hostCmd(d.command);break;
 case 'CHAT':appendMessage(d.sender,String(d.text).slice(0,500),'peer');break;
 case 'SYNC_REQUEST':if(isHost)sendSync();break;
 }
}
function bindConn(c){
 c.on('open',()=>{$('connDot').classList.add('online');rejoinTries=0;clearTimeout(rejoinTimer);rejoinTimer=0;if(!isHost)send({type:'HELLO',guestName:myName})});
 c.on('data',onData);
 c.on('close',()=>{
  if(conn&&conn!==c)return;
  $('connDot').classList.remove('online');$('peerStatus').textContent='انقطع اتصال الطرف الآخر';sys('انقطع اتصال الطرف الآخر.');
  if(conn===c)conn=null;peerBuf=false;updateBufUI();
  if(isHost){txAbort();dropFileConns()}               // the receiver is gone: stop sending, it resumes from its stored blocks when it returns
  if(!isHost&&!leaving)autoRejoin();
 });
 c.on('error',e=>sys('خطأ اتصال: '+(e.type||e.message)));
}
function autoRejoin(){
 if(isHost||leaving||!joined||rejoinTimer)return;
 sys('بنحاول نرجع للغرفة تلقائياً...');
 const attempt=()=>{
  rejoinTimer=0;if(leaving||(conn&&conn.open))return;
  if(++rejoinTries>REJOIN_MAX){sys('تعذر الرجوع للغرفة. اضغط خروج وادخل تاني بنفس الرمز.');return}
  try{if(!peer||peer.destroyed)return;if(peer.disconnected)peer.reconnect()}catch{}
  const c=peer.connect(PEER_PREFIX+roomCode,{reliable:true,metadata:{cid:getCid()}});conn=c;bindConn(c);
  rejoinTimer=setTimeout(()=>{rejoinTimer=0;if(!(conn&&conn.open)){try{c.close()}catch{}conn=null;attempt()}},6000);
 };
 rejoinTimer=setTimeout(attempt,1500);
}
const makeCode=()=>{const ch='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';return Array.from({length:6},()=>ch[Math.floor(Math.random()*ch.length)]).join('')};
function createRoom(attempt=0){
 if(!window.Peer){showError('تعذر تحميل مكتبة PeerJS. تأكد من الإنترنت.');return}
 const b=$('createBtn');b.disabled=true;myName=$('displayName').value.trim()||'المضيف';isHost=true;
 const code=makeCode();peer=new Peer(PEER_PREFIX+code,PEER_OPTS);
 peer.on('call',onCall);
 peer.on('open',()=>{enterRoom(code);sys('تم إنشاء الغرفة. شارك الرابط مع صديقك.')});
 peer.on('connection',inc=>{
  if(inc.metadata&&inc.metadata.kind==='file'){regFileConn(inc);return}
  if(conn&&conn.open){
   const same=inc.metadata&&conn.metadata&&inc.metadata.cid&&inc.metadata.cid===conn.metadata.cid;
   if(!same){inc.on('open',()=>{inc.send({type:'ROOM_FULL'});setTimeout(()=>inc.close(),400)});return}
   const old=conn;conn=inc;bindConn(inc);txAbort();dropFileConns();try{old.close()}catch{}sys('صاحبك رجع للغرفة.');return;
  }
  conn=inc;bindConn(inc);
 });
 peer.on('disconnected',()=>{if(peer&&!peer.destroyed)peer.reconnect()});
 peer.on('error',e=>{if(e.type==='unavailable-id'&&attempt<3&&!joined){peer.destroy();createRoom(attempt+1);return}b.disabled=false;if(!joined)showError('تعذر إنشاء الغرفة: '+e.type);else sys('خطأ: '+e.type)});
}
function joinRoom(){
 if(!window.Peer){showError('تعذر تحميل مكتبة PeerJS. تأكد من الإنترنت.');return}
 const code=$('roomCodeInput').value.trim().toUpperCase();if(!/^[A-Z0-9]{6}$/.test(code)){showError('رمز الغرفة يجب أن يتكون من 6 أحرف أو أرقام.');return}
 const b=$('joinBtn');b.disabled=true;myName=$('displayName').value.trim()||'الضيف';isHost=false;roomCode=code;
 peer=new Peer(undefined,PEER_OPTS);peer.on('call',onCall);
 const fail=m=>{b.disabled=false;showError(m);if(peer&&!peer.destroyed)peer.destroy()};
 peer.on('open',()=>{
  const c=peer.connect(PEER_PREFIX+code,{reliable:true,metadata:{cid:getCid()}});conn=c;bindConn(c);
  const t=setTimeout(()=>{if(!joined)fail('انتهت مهلة الاتصال. تأكد أن المضيف فتح الغرفة.')},12000);
  c.on('open',()=>{clearTimeout(t);if(!joined)enterRoom(code)});
 });
 peer.on('disconnected',()=>{if(peer&&!peer.destroyed)peer.reconnect()});
 peer.on('error',e=>{if(!joined)fail(e.type==='peer-unavailable'?'الغرفة غير موجودة. تأكد من الرمز.':'تعذر الاتصال: '+e.type);else if(e.type!=='peer-unavailable')sys('خطأ: '+e.type)});
}
/* ---------- UI events ---------- */
$('createBtn').onclick=()=>createRoom();$('joinBtn').onclick=joinRoom;
$('loadUrlBtn').onclick=()=>{if(!isHost){sys('المضيف فقط يختار المصدر.');return}loadMediaSource($('videoUrlInput').value,true)};
$('openExternalBtn').onclick=()=>{if(!isHost){sys('المضيف فقط يغيّر الصفحة.');return}openExternal($('externalUrlInput').value,true)};
$('openNewTabBtn').onclick=()=>{const u=validHttps($('externalUrlInput').value||extUrl);if(!u){alert('أدخل رابط HTTPS أولاً.');return}window.open(u,'_blank','noopener,noreferrer')};
$('localFileInput').onchange=e=>{
 const f=e.target.files[0];e.target.value='';if(!f)return;
 if(!isHost){sys('الإرسال للمضيف فقط.');return}
 if(!f.type.startsWith('video/')&&!/\.(mkv|mp4|webm|mov|m4v|avi)$/i.test(f.name)){sys('الملف ليس فيديو.');return}
 resetForNewSource();localFile=f;playFile(f);send({type:'FILE_NOTICE'});
 if(conn&&conn.open)sendFile(f);else sys('سيُرسل الملف تلقائياً عند انضمام صديقك.');
};
$('syncNowBtn').onclick=()=>{if(activeMode==='external'){if(isHost)send({type:'EXTERNAL_NAV',url:extUrl,sender:myName});sys('أُعيد إرسال الرابط. المزامنة داخل الموقع الخارجي غير ممكنة.')}else if(isHost)sendSync();else send({type:'SYNC_REQUEST'})};
$('shareLinkBtn').onclick=async()=>{const u=location.protocol==='file:'?roomCode:location.origin+location.pathname+'?room='+roomCode;try{if(navigator.share)await navigator.share({title:'CoWatch Live',url:u});else{await navigator.clipboard.writeText(u);sys('تم نسخ رابط الغرفة.')}}catch{prompt('انسخ رابط الغرفة:',u)}};
$('leaveBtn').onclick=()=>{leaving=true;if(mediaCall)mediaCall.close();if(peer)peer.destroy();location.href=location.pathname};
$('chatForm').onsubmit=e=>{e.preventDefault();const i=$('chatInput'),t=i.value.trim();if(!t)return;appendMessage(myName,t,'me');send({type:'CHAT',sender:myName,text:t});i.value=''};
$('unlockBtn').onclick=()=>{overlay.style.display='none';video.play().catch(()=>{});send({type:'SYNC_REQUEST'});if(activeMode==='youtube'&&ytReady)ytPlayer.playVideo()};
/* ---------- tab / network lifecycle ---------- */
window.addEventListener('offline',()=>{if(joined)sys('الإنترنت اتقطع. النقل هيكمّل تلقائياً من اللي وصل لما يرجع.')});
window.addEventListener('online',()=>{if(!joined)return;sys('الإنترنت رجع.');try{if(peer&&!peer.destroyed&&peer.disconnected)peer.reconnect()}catch{}if(!isHost&&!(conn&&conn.open)){clearTimeout(rejoinTimer);rejoinTimer=0;rejoinTries=0;autoRejoin()}});
document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible'&&(rx||tx)){keepAwake(true);send({type:'PING',t:Date.now()})}});
window.addEventListener('pagehide',()=>{if(peer&&!peer.destroyed&&!leaving){try{peer.destroy()}catch{}}});
