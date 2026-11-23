/* Keep recorded phase passes visible without treating archival as a paid reset. */
(function(){
'use strict';
if(window.NP_DASHBOARD_ACCOUNT_STATE_RELEASE)return;
window.NP_DASHBOARD_ACCOUNT_STATE_RELEASE='dashboard-account-state-20261123-v1';
if(typeof npIsResetArchive==='function'){
 const resetArchive=npIsResetArchive;
 window.npIsResetArchive=function(a){
  if(!a)return resetArchive(a);
  const status=String(a.account_status||'').trim().toLowerCase();
  const recordedPhasePass=['archived_phase1','archived_phase2','phase1_passed','phase2_passed'].includes(status)&&a.phase_pass_recorded!==false&&!a.breached_at&&!a.recorded_breach;
  // A pass is archived to stop trading; archived_at alone is not reset evidence.
  // Preserve every other reset/replacement marker and the original stored row.
  return resetArchive(recordedPhasePass?{...a,archived_at:null}:a);
 };
}
if(typeof npProgressionSourceConsumedForever==='function'){
 const consumed=npProgressionSourceConsumedForever;
 window.npProgressionSourceConsumedForever=function(a){
  let journey=a?.challenge_journey||a?.journey_stages||a?.lifecycle_journey;
  if(typeof journey==='string'){try{journey=JSON.parse(journey)}catch{journey=null}}
  if(a&&!a.next_stage&&Array.isArray(journey)){
   const stages=journey.map(x=>String(x).toLowerCase().replace(/\s+/g,''));
   const index=stages.indexOf(String(a.stage||'phase1').toLowerCase().replace(/\s+/g,''));
   const next=index>=0?stages[index+1]:null;
   if(['phase2','funded'].includes(next))return consumed({...a,next_stage:next});
  }
  return consumed(a);
 };
}
if(typeof mt5ActiveFilter==='function'){
 const activeFilter=mt5ActiveFilter;
 window.mt5ActiveFilter=function(rows){
  const seen=new Set();
  return activeFilter(rows).filter(a=>{
   const status=String(a?.account_status||a?.status||'').trim().toLowerCase();
   if(!String(a?.mt5_login||'').trim()||/profit_cap|profit_protected|payout_pending|payment_processing/.test(status))return false;
   if(a.breached_at||a.recorded_breach||a.archived_at)return false;
   const key=String(a.mt5_server||'')+':'+String(a.mt5_login);if(seen.has(key))return false;seen.add(key);return true;
  });
 };
}
})();
