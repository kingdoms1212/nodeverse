'use strict';
function observatoryMenuCounts(snapshot){
 const alerts=snapshot.alerts||[];
 const queryIssues=new Set(alerts.filter(a=>/^(stopped|stalled|slow):/.test(a.id)).map(a=>a.id.split(':').slice(1).join(':')));
 const counts=snapshot.queue?.counts;
 return {
  overview:alerts.length,
  queries:snapshot.progress?.state==='ok'?queryIssues.size:null,
  queue:counts?['waiting_context','parked','blocked','quarantined'].reduce((sum,k)=>sum+(counts[k]||0),0):null
 };
}
function setObservatoryBadge(name,count,description){
 const badge=document.getElementById(`nav-${name}-count`);
 if(!badge)return;
 badge.textContent=count==null?'?':count>99?'99+':String(count);
 badge.classList.toggle('hidden',count===0);
 const link=badge.closest('a');
 link.title=`${description}: ${count==null?'확인 불가':`${count}개`}`;
 link.setAttribute('aria-label',link.title);
}
function renderObservatoryMenu(snapshot){
 const counts=observatoryMenuCounts(snapshot);
 setObservatoryBadge('overview',counts.overview,'실시간 개요 · 활성 경고');
 setObservatoryBadge('queries',counts.queries,'쿼리 성능 · 중단·지연 쿼리 수');
 setObservatoryBadge('queue',counts.queue,'이벤트 상태 · 조건 대기·재시도 보관·적재 중단·격리 기록 수');
 document.querySelector('nav[aria-label="주요 메뉴"]').classList.remove('badges-stale');
}
