'use strict';
// No credentials, raw telemetry, or external notification destinations.
class ObservatoryNotifications {
 constructor(){this.seen=new Set();this.unread=new Set();this.acknowledged=new Set();this.archived=new Set();this.storageLoaded=false;this.onChange=null;this.ready=false;this.mutedUntil=0;this.audio=null;this.sound=false;this.desktop=false;this.failures=0;this.disconnected=false;this.toastTimer=null;}
 id(e){return [e.id,e.phase,e.at].join('|');}
 status(message){document.getElementById('notification-status').textContent=message;}
 loadState(){if(this.storageLoaded)return;this.storageLoaded=true;this.storageKey='nodeverse-alert-review-v1:'+document.getElementById('environment').textContent;try{const saved=JSON.parse(localStorage.getItem(this.storageKey)||'{}');this.acknowledged=new Set((Array.isArray(saved.read)?saved.read:[]).filter(x=>typeof x==='string').slice(-100));this.archived=new Set((Array.isArray(saved.archived)?saved.archived:[]).filter(x=>this.acknowledged.has(x)).slice(-100));}catch{/* storage disabled: keep review state in memory */}}
 saveState(){try{localStorage.setItem(this.storageKey,JSON.stringify({read:[...this.acknowledged].slice(-100),archived:[...this.archived].slice(-100)}));}catch{this.status('브라우저 저장을 사용할 수 없어 확인 상태는 현재 탭에서만 유지됩니다.');}}
 changed(){this.saveState();this.draw();if(this.onChange)this.onChange();}
 setRead(key,read){if(read){this.acknowledged.add(key);this.unread.delete(key);}else{this.acknowledged.delete(key);this.archived.delete(key);this.unread.add(key);}this.changed();}
 archiveRead(){for(const key of this.acknowledged)this.archived.add(key);this.changed();}
 toggleArchive(key){if(this.archived.has(key))this.archived.delete(key);else if(this.acknowledged.has(key))this.archived.add(key);this.changed();}
 draw(){setObservatoryBadge('incidents',this.unread.size,'경고 이력 · 이 브라우저의 미확인 알림 수');document.getElementById('unread-count').textContent=String(this.unread.size);document.getElementById('notification-mute').textContent=this.mutedUntil>Date.now()?'알림 다시 켜기':'15분 알림 끄기';}
 toast(message){const el=document.getElementById('toast');el.textContent=message;el.classList.remove('hidden');clearTimeout(this.toastTimer);this.toastTimer=setTimeout(()=>el.classList.add('hidden'),8000);}
 async enableSound(){
  if(this.sound){this.sound=false;this.status('소리 알림을 껐습니다.');return;}
  try{const Audio=window.AudioContext||window.webkitAudioContext;if(!Audio)throw Error();this.audio=this.audio||new Audio();await this.audio.resume();this.sound=true;this.status('소리 알림을 켰습니다.');}catch{this.status('이 브라우저에서 소리를 사용할 수 없습니다.');}
 }
 beep(){if(!this.sound||!this.audio||this.audio.state!=='running')return;const osc=this.audio.createOscillator(),gain=this.audio.createGain(),t=this.audio.currentTime;osc.frequency.value=660;gain.gain.setValueAtTime(.08,t);gain.gain.exponentialRampToValueAtTime(.001,t+.3);osc.connect(gain);gain.connect(this.audio.destination);osc.start(t);osc.stop(t+.3);}
 async enableDesktop(){
  if(this.desktop){this.desktop=false;this.status('브라우저 알림을 껐습니다.');return;}
  if(!window.isSecureContext||!('Notification'in window)){this.status('브라우저 알림은 HTTPS 또는 localhost의 지원 브라우저에서 사용할 수 있습니다. 화면 알림은 계속 동작합니다.');return;}
  try{const result=await Notification.requestPermission();this.desktop=result==='granted';this.status(this.desktop?'브라우저 알림을 켰습니다. 페이지가 열려 있는 동안 동작합니다.':'브라우저 알림 권한이 허용되지 않았습니다. 화면 알림은 계속 동작합니다.');}catch{this.status('브라우저 알림을 활성화할 수 없습니다.');}
 }
 notify(events,test=false){
  if(!events.length)return;
  if(!test&&this.mutedUntil>Date.now())return;
  const first=events.at(-1),phase=first.phase==='resolved'?'복구':first.phase==='changed'?'등급 변경':'경고';
  const message=`${test?'[알림 시험] ':''}${phase} · ${first.title}${events.length>1?` 외 ${events.length-1}건`:''}`;
  this.toast(message);this.beep();
  if(this.desktop&&'Notification'in window&&Notification.permission==='granted')try{const note=new Notification('NODEVERSE 파이프라인',{body:message,tag:'nodeverse-pipeline',silent:true});setTimeout(()=>note.close(),10000);}catch{this.status('브라우저 알림을 표시하지 못했습니다. 화면 알림을 확인하세요.');}
 }
 ingest(events,persistent){
  this.loadState();
  const fresh=[];
  for(const e of [...events].reverse()){const id=this.id(e);if(this.seen.has(id))continue;this.seen.add(id);if(!this.acknowledged.has(id))this.unread.add(id);if(this.ready)fresh.push(e);}
  this.ready=true;
  // Keep client bookkeeping bounded like the server's most recent 100 events.
  const keep=new Set(events.map(e=>this.id(e)));this.seen=keep;this.unread=new Set([...this.unread].filter(id=>keep.has(id)));
  this.acknowledged=new Set([...this.acknowledged].filter(id=>keep.has(id)));this.archived=new Set([...this.archived].filter(id=>keep.has(id)));
  document.getElementById('notification-storage').textContent=persistent===false?'이력 저장 실패 · 현재 화면에서만 유지':'최근 발생·복구 100건 · 관측소 재시작 후 복원';
  this.draw();this.notify(fresh);
 }
 connection(ok){if(ok){this.failures=0;if(this.disconnected){this.disconnected=false;this.notify([{phase:'resolved',title:'관측 화면 연결 복구'}]);}}else if(++this.failures===3){this.disconnected=true;this.notify([{phase:'opened',title:'관측 API 연결 실패가 3회 연속 발생했습니다'}]);}}
 bind(){
  const $=id=>document.getElementById(id);
  $('notification-sound').onclick=async()=>{await this.enableSound();$('notification-sound').setAttribute('aria-pressed',String(this.sound));$('notification-sound').textContent=this.sound?'소리 끄기':'소리 켜기';};
  $('notification-desktop').onclick=async()=>{await this.enableDesktop();$('notification-desktop').setAttribute('aria-pressed',String(this.desktop));$('notification-desktop').textContent=this.desktop?'브라우저 알림 끄기':'브라우저 알림 켜기';};
  $('notification-mute').onclick=()=>{this.mutedUntil=this.mutedUntil>Date.now()?0:Date.now()+15*60000;this.status(this.mutedUntil?'팝업·소리·브라우저 알림을 15분간 끕니다. 경고 수집과 이력은 계속됩니다.':'알림을 다시 켰습니다.');this.draw();};
  $('notification-read').onclick=()=>{for(const key of this.unread)this.acknowledged.add(key);this.unread.clear();this.changed();this.status('현재 알림을 모두 확인했습니다. 현재 문제는 해결 전까지 계속 표시합니다.');};
  $('notification-test').onclick=()=>{this.notify([{phase:'opened',title:'시험 경고 → 실제 파이프라인에는 영향 없음'}],true);this.status('화면과 활성화한 채널로 시험 알림을 보냈습니다. 실제 경고 이력에는 저장하지 않습니다.');};
 }
}
