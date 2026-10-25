/* PURCHASE_ALERT_START: new arrivals since last seen; old pending payments do not blink. */
function npPurchaseNeedsReview(row){
 const status=String(row.status||'').trim().toLowerCase(),payment=String(row.payment_status||'').trim().toLowerCase();
 const terminal=['rejected','declined','cancelled','canceled','refunded','failed','expired'];
 if(row.mark_as_test===true||terminal.includes(status)||terminal.includes(payment))return false;
 if(['approved','paid'].includes(payment)||['approved','paid','completed','approved_active','assigned','approved_waiting_mt5'].includes(status))return false;
 return ['pending','pending_review','awaiting_review','review','payment_pending','submitted','awaiting_verification','under_review','awaiting_approval','requested'].includes(payment||status);
}
function npPurchaseAlertState(){
 const key=String(SESSION.token||'')+'|'+String(SESSION.admin?.id||'');
 if(!window.NP_PURCHASE_ALERT||NP_PURCHASE_ALERT.key!==key){
  const storageKey='np-new-purchases-v1:'+String(SESSION.admin?.id||SESSION.admin?.username||'admin');
  let seen=null;try{seen=localStorage.getItem(storageKey);if(seen&&!Number.isFinite(Date.parse(seen)))seen=null;}catch(e){}
  window.NP_PURCHASE_ALERT={key,storageKey,seen,count:null,ids:[],observed:null,error:false,pending:false,loadedAt:0};
 }
 return window.NP_PURCHASE_ALERT;
}
function npPurchaseAlertSave(s,cursor){
 s.seen=cursor;try{localStorage.setItem(s.storageKey,cursor);}catch(e){}
}
function npPurchaseAlertBadge(){
 const s=npPurchaseAlertState();
 if(!s.loadedAt&&!s.pending&&!s.error&&SESSION.token)setTimeout(npPurchaseAlertRefresh,0);
 const stale=s.error||!s.loadedAt||Date.now()-s.loadedAt>65000,count=stale?null:s.count;
 return `<span data-purchase-alert-badge class="${count>0?'np-purchase-blink':''}" style="margin-left:auto;min-width:20px;padding:2px 6px;border-radius:999px;background:${count>0?'#ef4444':'#555'};color:white;font-size:11px;font-weight:950;" role="status" aria-live="polite" title="${count===null?'New purchase check unavailable':count===0?'Purchase alerts active · no unseen purchases':count+' new purchase'+(count===1?'':'s')}">${count===null?'…':count}</span>`;
}
function npPurchaseAlertApply(){
 document.querySelectorAll('[data-purchase-alert-slot]').forEach(el=>el.innerHTML=npPurchaseAlertBadge());
 const banner=document.getElementById('np-purchase-review-alert');if(!banner)return;
 const s=npPurchaseAlertState(),stale=s.error||!s.loadedAt||Date.now()-s.loadedAt>65000;
 banner.hidden=!stale&&s.count===0;
 banner.innerHTML=stale?'New purchase check unavailable. Refresh purchases to check arrivals.':`<strong class="np-purchase-blink">● ${s.count} new purchase${s.count===1?'':'s'} received</strong> <button class="btn btn-sm btn-gold" type="button" onclick="npOpenNewPurchases()">View new purchases</button>`;
}
function npPurchaseArrivalsViewed(){
 const s=npPurchaseAlertState();
 if(SESSION.currentMod!=='purchases'||!s.loadedAt||s.error||!s.observed||!s.ids.length)return;
 // Never acknowledge a purchase until its actual row is loaded and visible.
 const shown=new Set(purchaseVisibleRows().map(p=>String(p.id)));
 if(!s.ids.every(id=>shown.has(id)))return;
 npPurchaseAlertSave(s,s.observed);s.count=0;s.ids=[];npPurchaseAlertApply();
}
async function npPurchaseAlertRefresh(){
 const s=npPurchaseAlertState();if(!SESSION.token||s.pending||document.hidden)return;
 s.pending=true;
 try{
  const r=await api('/admin/purchase_alert?'+new URLSearchParams(s.seen?{since:s.seen}:{}),'GET');
  if(npPurchaseAlertState()!==s)return;
  if(!r.success||r.mode!=='new_arrivals'||!Number.isSafeInteger(r.new_count)||r.new_count<0||!Array.isArray(r.new_ids)||r.new_ids.length!==r.new_count||!Number.isFinite(Date.parse(r.observed_at)))throw Error('Invalid new-purchase count');
  if(!s.seen)npPurchaseAlertSave(s,r.observed_at);
  s.count=r.new_count;s.ids=r.new_ids;s.observed=r.observed_at;s.loadedAt=Date.now();s.error=false;
 }catch(e){if(npPurchaseAlertState()===s)s.error=true;}
 finally{if(npPurchaseAlertState()===s){s.pending=false;npPurchaseAlertApply();}}
}
function npOpenNewPurchases(){
 window.PURCHASE_FILTER_STATUS='all';window.PURCHASE_FILTER_RANGE='all';window.PURCHASE_FILTER_Q='';
 setMod('purchases');
 npRefreshPurchaseOperationsData(true);
}
function npOpenPurchaseReviews(){
 window.PURCHASE_FILTER_STATUS='needs_review';window.PURCHASE_FILTER_RANGE='all';window.PURCHASE_FILTER_Q='';
 setMod('purchases');
}
setInterval(npPurchaseAlertRefresh,30000);
window.addEventListener('focus',npPurchaseAlertRefresh);
/* PURCHASE_ALERT_END */
