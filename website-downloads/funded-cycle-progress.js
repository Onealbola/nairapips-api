/* FUNDED_CYCLE_PROGRESS_START */
function npCycleProgressState(){
  const key=String(SESSION.token||'')+'|'+String(SESSION.admin?.id||'');
  if(!window.NP_CYCLE_PROGRESS || window.NP_CYCLE_PROGRESS.key!==key) window.NP_CYCLE_PROGRESS={key,data:null,error:'',loadedAt:0,pending:null};
  return window.NP_CYCLE_PROGRESS;
}
function npCycleFresh(){const s=npCycleProgressState();return !!s.data&&!s.error&&Date.now()-s.loadedAt<45000;}
function npCyclePurchase(id){return npCycleProgressState().data?.purchases?.find(p=>String(p.purchase_id)===String(id));}
function npCycleCounter(p){
  if(!p?.verified) return '<span class="tag tag-red">VERIFICATION REQUIRED</span>';
  return `<b style="color:${p.completed?'#fca5a5':'#ffe08a'}">${Number(p.paid)} of ${Number(p.limit)} paid</b><div class="text-xs">${Number(p.remaining)} remaining${p.reserved?' · '+Number(p.reserved)+' pending / approved':''}</div>${p.completed?'<span class="tag tag-red">COMPLETED — NO FURTHER PAYOUT</span>':''}`;
}
function npCyclePurchaseCell(p){
  return `<span data-cycle-purchase="${esc(String(p.id))}" data-cycle-limited="${Number(p.payout_cycles)>0?'1':'0'}">${npCycleCellContent(p.id,Number(p.payout_cycles)>0)}</span>`;
}
function npCycleCellContent(id,limited=false){
  const p=npCyclePurchase(id);
  if(p && npCycleFresh())return npCycleCounter(p);
  if(!npCycleFresh())return '<span class="text-xs text-gray-400">Checking cycle records…</span>';
  return limited?'<span class="tag tag-red">VERIFICATION REQUIRED</span>':'<span class="text-xs text-gray-500">—</span>';
}
function npCyclePayoutCell(p){
  const info=npCycleProgressState().data?.payouts?.[String(p.id)];
  const content=!npCycleFresh()?'Checking cycle records…':info?npCycleCounter(npCyclePurchase(info.purchase_id)):'—';
  return `<span data-cycle-payout="${esc(String(p.id))}">${content}</span>`;
}
function npCycleActionAttrs(id,kind='payment'){return `data-cycle-action="${esc(String(id))}" data-cycle-kind="${kind}" disabled title="Checking cycle records"`;}
function npCyclePanel(){return '<div id="np-cycle-progress-panel" class="card tab-content mb-4" style="border:1px solid #b69b3655"><b>FUNDED PAYOUT CYCLE PROGRESS</b><p class="text-sm text-gray-400">Checking recorded paid cycles…</p></div>';}
function npCycleApply(){
  if(!['purchases','payouts'].includes(SESSION.currentMod))return;
  const s=npCycleProgressState(),fresh=npCycleFresh(),panel=document.getElementById('np-cycle-progress-panel');
  if(panel){
    const warning=s.error?'Verification unavailable. Do not authorize payment until refreshed.':s.data&&!fresh?'Progress is stale. Refresh before authorizing payment.':'';
    panel.innerHTML=`<div style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap"><div><b>FUNDED PAYOUT CYCLE PROGRESS</b><div class="text-xs text-gray-400">Lifetime counts per purchase. Pending and approved requests are not paid cycles.</div></div><button class="btn btn-sm btn-gold" onclick="npCycleRefresh(true)" ${s.pending?'disabled':''}>Refresh cycles</button></div>${warning?`<p style="color:#fca5a5;font-weight:800">${esc(warning)}</p>`:''}`;
    if(s.data){
      panel.innerHTML+=`<div class="text-xs text-gray-400" style="margin:10px 0">${Number(s.data.counts.tracked)} capped purchases · ${Number(s.data.counts.completed)} completed · Updated ${esc(new Date(s.data.generated_at).toLocaleTimeString())}${!fresh?' · PREVIOUS SNAPSHOT':''}</div><div style="overflow-x:auto"><table class="compact"><thead><tr><th>Trader / Plan</th><th>Cycles paid</th><th>Remaining</th><th>Pending / Approved</th><th>Status</th></tr></thead><tbody>${s.data.purchases.map(p=>`<tr><td><b>${esc(p.customer)}</b><div class="text-xs text-gray-400">${esc(p.plan)} · ${esc(String(p.purchase_id).slice(0,8))}</div></td><td>${p.verified?Number(p.paid)+' of '+Number(p.limit):'Not verified'}</td><td>${p.verified?Number(p.remaining):'—'}</td><td>${p.verified?Number(p.reserved):'—'}</td><td>${!p.verified?'<span class="tag tag-red">VERIFICATION REQUIRED</span>':p.completed?'<span class="tag tag-red">COMPLETED — NO FURTHER PAYOUT</span>':p.status==='awaiting_funded'?'<span class="tag tag-yellow">AWAITING FUNDED</span>':'<span class="tag tag-green">CYCLES REMAIN</span>'}</td></tr>`).join('')||'<tr><td colspan="5">No capped purchases.</td></tr>'}</tbody></table></div>`;
    }else panel.innerHTML+='<p class="text-sm text-gray-400">'+(s.pending?'Checking recorded paid cycles…':'Cycle totals are unavailable.')+'</p>';
  }
  document.querySelectorAll('[data-cycle-purchase]').forEach(el=>el.innerHTML=npCycleCellContent(el.dataset.cyclePurchase,el.dataset.cycleLimited==='1'));
  document.querySelectorAll('[data-cycle-payout]').forEach(el=>{
    const info=s.data?.payouts?.[el.dataset.cyclePayout];
    el.innerHTML=!fresh?'<span class="tag tag-yellow">CYCLE VERIFICATION REQUIRED</span>':info?npCycleCounter(npCyclePurchase(info.purchase_id)):'<span class="text-xs text-gray-500">—</span>';
  });
  document.querySelectorAll('[data-cycle-action]').forEach(el=>{
    const info=s.data?.payouts?.[el.dataset.cycleAction];
    const purchase=info?npCyclePurchase(info.purchase_id):null;
    const blocked=el.dataset.cycleKind==='renewal' ? !!purchase&&(!purchase.verified||purchase.completed) : !!info?.blocked;
    el.disabled=!fresh||blocked;
    el.title=!fresh?'Cycle verification required before this action':blocked?(purchase?.completed?'COMPLETED — no further payout':info?.reason||'Cycle records require review'):'Backend verifies cycles and permitted amount again';
  });
}
async function npCycleRefresh(force=false){
  const s=npCycleProgressState();
  if(s.pending)return s.pending;
  if(!force&&npCycleFresh()){npCycleApply();return true;}
  s.pending=(async()=>{
    try{
      const r=await api('/admin/funded_cycle_progress?_t='+Date.now(),'GET');
      if(npCycleProgressState()!==s)return false;
      if(!r?.success||!Array.isArray(r.data?.purchases)||!r.data?.payouts)throw Error('Invalid cycle progress response');
      s.data=r.data;s.loadedAt=Date.now();s.error='';return true;
    }catch(e){if(npCycleProgressState()===s)s.error=e.message||'Cycle verification unavailable';return false;}
    finally{if(npCycleProgressState()===s){s.pending=null;npCycleApply();}}
  })();
  npCycleApply();return s.pending;
}
function npCycleStart(){
  npCycleRefresh(false);
  if(!window.NP_CYCLE_PROGRESS_TIMER)window.NP_CYCLE_PROGRESS_TIMER=setInterval(()=>{
    if(!document.hidden&&['purchases','payouts'].includes(SESSION.currentMod))npCycleRefresh(true);
  },30000);
}
async function npCyclePreflight(id){
  if(!await npCycleRefresh(true))throw Error('Cycle progress could not be verified. Refresh before authorizing payment.');
  const info=npCycleProgressState().data.payouts[String(id)];
  if(info?.blocked)throw Error(info.reason);
}
/* FUNDED_CYCLE_PROGRESS_END */
