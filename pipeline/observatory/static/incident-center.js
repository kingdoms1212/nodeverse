'use strict';
const queueGuides={
 waiting_context:{title:'회원·선행 기록 연결 대기',action:'회원 UUID 연결과 세션 → 문제 실행 → 풀이 시도의 선행 기록을 확인하세요. 선행 기록이 저장된 뒤 작업자가 다시 처리하는지 관측하세요.'},
 parked:{title:'재시도 보관',action:'적재 작업자 로그에서 실패 원인을 확인하고 연결·권한 문제를 해결한 뒤 승인된 재처리 도구를 사용하세요.'},
 blocked:{title:'적재 중단',action:'ID 충돌·스키마 불일치를 조사하세요. 같은 ID의 다른 내용을 덮어쓰지 말고 원본과 매핑 규칙을 수정한 뒤 재처리를 검토하세요.'},
 quarantined:{title:'격리 기록',action:'수신 로그 화면에서 격리 사유를 확인하세요. 중복 ID의 내용 충돌·필수 필드·참조·시간 규약을 수정한 뒤 새 시험을 진행하세요.'}
};
function incidentGuide(event,snapshot){
 const id=event.id,counts=snapshot.queue?.counts;
 let target='#overview',label='서비스 상태',action='갱신 시각과 서비스 응답을 확인하세요.',evidence='현재 관측 데이터로 확인하세요.';
 if(/^(stopped|stalled|slow):/.test(id)){
  const name=id.split(':').slice(1).join(':'),q=(snapshot.queries||[]).find(q=>q.name===name);
  target='#query-'+name;label=q?.label||name;action='해당 쿼리의 활성 상태·최근 배치 시각·입력 건수·배치 시간을 확인하고 Spark 로그에서 원인을 조사하세요. 입력 0건의 대기 상태는 장애로 판단하지 않습니다.';
  evidence=q?`쿼리 ${name} · ${q.active?'실행 중':'중단'} · 배치 #${q.batch_id??'미확인'} · 입력 ${q.input_rows??'미확인'}건 · 배치 시간 ${q.batch_ms??'미확인'}ms`:'해당 쿼리의 현재 관측값이 없습니다.';
 }else if(id==='context-age'||id==='queue-age'||queueGuides[id]){
  target='#queue';label='이벤트 처리 상태';
  const state=id==='context-age'?'waiting_context':id;if(queueGuides[state])target='#state-'+state;
  action=queueGuides[state]?.action||'처리 가능한 대기열의 증가 여부와 작업자 상태를 확인하세요. 연결과 적재 오류를 해결한 뒤 완료 건수가 증가하는지 점검하세요.';
  evidence=counts?(state in counts?`${queueGuides[state]?.title||state}: ${counts[state]}건`:`처리 대기 ${counts.pending||0} · 처리 중 ${counts.processing||0} · 재시도 ${counts.retrying||0}건`):'DB 관측 불가 · 현재 건수를 확인할 수 없습니다.';
 }else if(id==='progress'){
  target='#queries';label='Spark 진행 신호';action='Spark 컨테이너 실행 상태와 진행 파일 갱신 경로를 확인하세요. 관측 파일 오류와 실제 쿼리 중단을 구분하세요.';evidence=`진행 신호 상태: ${snapshot.progress?.state||'미확인'}`;
 }else if(id==='remote'||id==='local-mongo'||id==='collector'){
  label=id==='remote'?'원격 MongoDB':id==='local-mongo'?'로컬 MongoDB':'수집기';
  action='해당 서비스 실행 상태, 네트워크·포트, 접속 권한과 준비 응답을 점검하세요. 연결 복구 후 저장 완료 상태도 별도로 확인하세요.';
  evidence=`현재 응답: ${(id==='remote'?snapshot.remote:id==='local-mongo'?snapshot.mongo:snapshot.collector)?.state||'미확인'}`;
 }
 return {target,label,action,evidence};
}
class IncidentCenter{
 constructor(notifications){this.notifications=notifications;this.snapshot=null;this.filter='unread';this.selected=null;this.fingerprint='';}
 update(snapshot){this.snapshot=snapshot;this.render();}
 element(tag,text,className){const el=document.createElement(tag);if(text!=null)el.textContent=text;if(className)el.className=className;return el;}
 button(text,action){const b=this.element('button',text,'button ghost');b.type='button';b.onclick=action;return b;}
 select(key){this.selected=key;this.render(true);document.querySelector('.incident-detail')?.scrollIntoView({behavior:'smooth',block:'nearest'});}
 open(filter='unread'){this.filter=filter;this.render(true);document.getElementById('incidents').scrollIntoView({behavior:'smooth',block:'start'});}
 async records(host,state){
  host.textContent='점검 기록을 조회하고 있습니다…';
  try{const result=await request('/api/attention?state='+encodeURIComponent(state));if(!host.isConnected)return;host.replaceChildren();
   if(result.state!=='ok'){host.textContent='점검 목록 조회용 인덱스가 준비되지 않았습니다. 관리자에게 확인하세요.';return;}
   host.append(this.element('p',`최대 20건의 인덱스 표본입니다. 최신순·전체 사유 분포가 아닙니다. ${result.more?'추가 기록이 있습니다.':''} 참조값은 원본 ID를 숨긴 표시용 지문입니다.`,'detail-note'));
   for(const row of result.records){const line=this.element('div',null,'attention-record');line.append(this.element('strong',`${row.reference} · ${row.reason_label}`),this.element('p',`사유 코드: ${row.reason_code} · 시도 ${row.attempts??'미확인'}회 · 원격 원본 ${row.archive_stored===true?'저장 확인':row.archive_stored===false?'미확인':'정보 없음'}`),this.element('small',`대기열 접수 ${row.queued_at?new Date(row.queued_at).toLocaleString('ko-KR'):'미확인'} · 갱신 ${row.updated_at?new Date(row.updated_at).toLocaleString('ko-KR'):'미확인'}`));host.append(line);}
   if(!result.records.length)host.append(this.element('p','해당 상태의 기록이 없습니다. 다음 관측에서 건수가 갱신되는지 확인하세요.'));
  }catch{if(host.isConnected){host.replaceChildren(this.element('p','기록을 불러오지 못했습니다. 접속 상태·키를 확인한 뒤 다시 시도하세요.'),this.button('기록 다시 조회',()=>this.records(host,state)));}}
 }
 render(force=false){
  const s=this.snapshot;if(!s)return;
  const n=this.notifications,events=s.incidents||[],active=new Set((s.alerts||[]).map(a=>a.id));
  const fingerprint=JSON.stringify([events,s.alerts,s.queue?.counts,s.progress?.state,[...n.unread],[...n.archived],this.filter,this.selected]);
  if(!force&&fingerprint===this.fingerprint)return;this.fingerprint=fingerprint;
  const host=document.getElementById('incident-list');host.replaceChildren();
  const filters=this.element('div',null,'incident-filters');
  for(const [value,label]of [['unread','미확인'],['active','현재 문제'],['all','전체 이력'],['archived','숨긴 이력']]){const b=this.button(label,()=>{this.filter=value;this.selected=null;this.render(true);});b.setAttribute('aria-pressed',String(this.filter===value));filters.append(b);}
  filters.append(this.button('확인한 이력 숨기기',()=>{n.archiveRead();this.render(true);}));host.append(filters);
  const groups=this.element('div',null,'attention-groups');
  for(const [state,guide]of Object.entries(queueGuides)){const count=s.queue?.counts?.[state];if(count===0)continue;const b=this.button(`${guide.title} ${count==null?'?':count}건`,()=>this.select('queue:'+state));b.classList.add('attention-item');b.setAttribute('aria-pressed',String(this.selected==='queue:'+state));groups.append(b);}
  if(groups.childElementCount){const label=this.element('p','데이터 점검 대상 · 아래 항목을 누르면 원인 점검·이동 안내가 열립니다. 알림 확인과 별개로 해결 전까지 유지됩니다.','footnote');host.append(label,groups);}
  const rows=this.filter==='active'?(s.alerts||[]).map(a=>events.find(e=>e.id===a.id&&e.phase!=='resolved')||{...a,phase:'opened',at:a.first_seen}):events;
  const visible=rows.filter(e=>this.filter==='active'||(this.filter==='archived'?n.archived.has(n.id(e)):!n.archived.has(n.id(e))&&(this.filter!=='unread'||n.unread.has(n.id(e)))));
  const list=this.element('div',null,'incident-rows');
  for(const e of visible){const key=n.id(e),isActive=active.has(e.id),read=!n.unread.has(key),row=this.element('div',null,`incident ${isActive?e.level:'resolved'} ${read?'is-read':''}`);
   const open=this.button(`${e.phase==='resolved'?'복구':e.phase==='changed'?'등급 변경':'발생'} · ${e.title}`,()=>this.select(key));open.classList.add('incident-open');open.setAttribute('aria-expanded',String(this.selected===key));
   const summary=this.element('div',null,'incident-summary');summary.append(open,this.element('small',`${new Date(e.at).toLocaleString('ko-KR')} · ${isActive?'현재 문제 지속':'현재 활성 아님'} · ${read?'확인함':'미확인'}`));
   const actions=this.element('div',null,'incident-actions');actions.append(this.button(read?'확인 취소':'확인',()=>{n.setRead(key,!read);this.render(true);}));
   row.append(summary,actions);list.append(row);
  }
  if(!visible.length)list.append(this.element('p',this.filter==='unread'?'미확인 알림이 없습니다. 현재 문제·데이터 점검 대상은 위 항목에서 계속 확인할 수 있습니다.':'이 조건에 해당하는 알림이 없습니다.','empty-cell'));
  host.append(list);
  if(this.selected){
   const isQueue=this.selected.startsWith('queue:'),state=this.selected.slice(6),e=isQueue?{id:state,title:queueGuides[state]?.title,detail:'데이터 처리 상태 점검',phase:'current'}:events.find(e=>n.id(e)===this.selected)||rows.find(e=>n.id(e)===this.selected);
   if(e){const guide=incidentGuide(e,s),detail=this.element('section',null,'incident-detail');detail.setAttribute('aria-label','선택한 알림 상세');
    detail.append(this.element('h3',e.title),this.element('p',e.detail),this.element('strong',`발생 위치: ${guide.label}`),this.element('p',`현재 근거: ${guide.evidence}`),this.element('p',`권장 조치: ${guide.action}`),this.element('p','현재 근거는 최신 관측값입니다. 과거 발생 당시의 원본 데이터와 다를 수 있습니다. 확인 처리는 장애 해결이나 데이터 수정·삭제를 실행하지 않습니다.','detail-note'));
    const a=this.element('a',`${guide.label}에서 확인 →`,'button');a.href=guide.target;detail.append(a);
    if(isQueue&&state==='quarantined'){const link=this.element('a','수신 로그·격리 사유 조회 →','button ghost');link.href=`http://${location.hostname}:8010/incoming`;link.target='_blank';link.rel='noopener noreferrer';detail.append(link);}
    if(!isQueue){const key=n.id(e);detail.append(this.button(n.unread.has(key)?'이 알림 확인':'확인 취소',()=>{n.setRead(key,n.unread.has(key));this.render(true);}));if(!n.unread.has(key))detail.append(this.button(n.archived.has(key)?'이력 다시 표시':'이력 숨기기',()=>{n.toggleArchive(key);this.render(true);}));}
    if(isQueue){const records=this.element('div',null,'attention-records');detail.append(this.element('h4','처리 상태·사유 표본'),records);this.records(records,state);}
    detail.append(this.button('상세 닫기',()=>{this.selected=null;this.render(true);}));host.append(detail);
   }
  }
 }
}
