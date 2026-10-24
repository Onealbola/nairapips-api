/* Trader notices use verified purchase history, never today's plan default. */
(function(){
'use strict';
let busy=false;
const text=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function markup(row){
 const label=text(row.plan)+' · Purchase '+text(row.purchase_id.slice(0,8));
 if(!row.verified)return `<article><strong>${label}</strong><p>Your payout cycle records need checking. Please contact support before requesting another payout.</p></article>`;
 const legacy=row.limit==null||row.limit===0;
 const title=row.completed?'Payout cycles completed':row.remaining===1?'Your final payout cycle remains':'Your payout cycles';
 const message=legacy?`${row.paid} payout${row.paid===1?'':'s'} received · ${row.limit===0?'Unlimited cycles':'Your existing purchase terms apply; no cycle limit is recorded.'}`:row.completed?'All payouts included in this purchase have been received. No further payout is available under this purchase.':`${row.paid} of ${row.limit} payouts received · ${row.remaining} remaining`;
 const pending=row.reserved?`<p>${row.reserved} payout request${row.reserved===1?' is':'s are'} pending or processing. These count as received only after payment.</p>`:'';
 return `<article style="padding:10px 0"><strong style="color:${row.completed?'#ffb3a7':'var(--gold,#ffd85a)'}">${title}</strong><div style="font-size:12px;opacity:.8;margin:5px 0">${label}</div><p>${message}</p>${pending}${row.status==='awaiting_funded'?'<p>These cycles start when your account becomes funded.</p>':''}</article>`;
}
async function load(){
 const nodes=[...document.querySelectorAll('[data-trader-cycle-notice]')];
 if(!nodes.length||busy)return;
 const token=SESSION.auth_token,owner=String(SESSION.trader?.id||SESSION.data?.trader?.id||'');
 if(!token||!owner){nodes.forEach(n=>n.textContent='Sign in to view your payout cycle status.');return;}
 busy=true;const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),15000);
 try{
  const r=await fetch(API+'/trader/funded_cycle_progress',{headers:{Authorization:'Bearer '+token},cache:'no-store',signal:controller.signal});
  const result=await r.json();if(!r.ok||!result.success)throw Error('unavailable');
  if(SESSION.auth_token!==token||String(SESSION.trader?.id||SESSION.data?.trader?.id||'')!==owner)return;
  const rows=result.data.purchases;
  nodes.filter(n=>n.isConnected).forEach(n=>{n.hidden=!rows.length;n.innerHTML=rows.map(markup).join('');});
 }catch(e){nodes.filter(n=>n.isConnected).forEach(n=>{n.hidden=false;n.textContent='Payout cycle status is temporarily unavailable. Please refresh or contact support.';});}
 finally{clearTimeout(timer);busy=false;}
}
const card=()=>'<section data-trader-cycle-notice class="card" role="status" aria-live="polite" style="border:1px solid var(--gold,#ffd85a);padding:16px;margin:12px 0;border-radius:14px">Checking your payout cycles…</section>';
for(const key of ['renderDashboard','renderPayouts']){
 const core=window[key];if(typeof core!=='function')continue;
 window[key]=function(...args){const html=core.apply(this,args);if(typeof html!=='string')return html;setTimeout(load,0);return card()+html;};
}
setInterval(()=>{if(!document.hidden)load();},30000);
window.addEventListener('focus',load);
window.NAIRAPIPS_TRADER_CYCLE_NOTICE_RELEASE='trader-cycle-notice-2026-10-23';
})();
