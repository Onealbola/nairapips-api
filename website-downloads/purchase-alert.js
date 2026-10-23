/* PURCHASE_ALERT_START: exact unreviewed payments; no financial writes. */
function npPurchaseNeedsReview(row){
 const status=String(row.status||'').trim().toLowerCase(),payment=String(row.payment_status||'').trim().toLowerCase();
 const terminal=['rejected','declined','cancelled','canceled','refunded','failed','expired'];
 if(row.mark_as_test===true||terminal.includes(status)||terminal.includes(payment))return false;
 if(['approved','paid'].includes(payment)||['approved','paid','completed','approved_active','assigned','approved_waiting_mt5'].includes(status))return false;
 return ['pending','pending_review','awaiting_review','review','payment_pending','submitted','awaiting_verification','under_review','awaiting_approval','requested'].includes(payment||status);
}
function npPurchaseAlertState(){
 const key=String(SESSION.token||'')+'|'+String(SESSION.admin?.id||'');
 if(!window.NP_PURCHASE_ALERT||NP_PURCHASE_ALERT.key!==key)window.NP_PURCHASE_ALERT={key,count:null,error:false,pending:false,loadedAt:0};
 return window.NP_PURCHASE_ALERT;
}
function npPurchaseAlertBadge(){
 const s=npPurchaseAlertState();
 if(!s.loadedAt&&!s.pending&&!s.error&&SESSION.token)setTimeout(npPurchaseAlertRefresh,0);
 const stale=s.error||!s.loadedAt||Date.now()-s.loadedAt>65000;
 const count=stale?null:s.count;
 return `<span data-purchase-alert-badge class="${count>0?'np-purchase-blink':''}" style="margin-left:auto;min-width:20px;padding:2px 6px;border-radius:999px;background:${count>0?'#ef4444':'#555'};color:white;font-size:11px;font-weight:950;${count===0?'display:none':''}" title="${count===null?'Purchase review count unavailable':count+' purchase payments waiting for review'}">${count===null?'…':count}</span>`;
}
function npPurchaseAlertApply(){
 document.querySelectorAll('[data-purchase-alert-slot]').forEach(el=>el.innerHTML=npPurchaseAlertBadge());
 const banner=document.getElementById('np-purchase-review-alert');if(!banner)return;
 const s=npPurchaseAlertState(),stale=s.error||!s.loadedAt||Date.now()-s.loadedAt>65000;
 banner.hidden=!stale&&s.count===0;
 banner.innerHTML=stale?'Purchase alert unavailable. Refresh purchases to check payment reviews.':`<strong class="np-purchase-blink">● ${s.count} purchase payment${s.count===1?'':'s'} waiting for review</strong> <button class="btn btn-sm btn-gold" type="button" onclick="npOpenPurchaseReviews()">Review purchases</button>`;
}
async function npPurchaseAlertRefresh(){
 const s=npPurchaseAlertState();if(!SESSION.token||s.pending||document.hidden)return;
 s.pending=true;
 try{
  const r=await api('/admin/purchase_alert?_t='+Date.now(),'GET');
  if(npPurchaseAlertState()!==s)return;
  if(!r.success||!Number.isSafeInteger(r.pending)||r.pending<0)throw Error('Invalid count');
  s.count=r.pending;s.loadedAt=Date.now();s.error=false;
 }catch(e){if(npPurchaseAlertState()===s)s.error=true;}
 finally{if(npPurchaseAlertState()===s){s.pending=false;npPurchaseAlertApply();}}
}
function npOpenPurchaseReviews(){
 window.PURCHASE_FILTER_STATUS='needs_review';window.PURCHASE_FILTER_RANGE='all';window.PURCHASE_FILTER_Q='';
 setMod('purchases');
}
setInterval(npPurchaseAlertRefresh,30000);
window.addEventListener('focus',npPurchaseAlertRefresh);
/* PURCHASE_ALERT_END */
