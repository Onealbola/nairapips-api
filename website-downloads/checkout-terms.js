/* Current checkout terms, including configured caps and optional paid resets. */
(function(){
'use strict';
if(window.NP_CHECKOUT_TERMS_RELEASE||typeof npBuyerLoad!=='function')return;
window.NP_CHECKOUT_TERMS_RELEASE='checkout-terms-20261123-v1';
window.npBuyerLoad=async function(planId,token){
 const state=NP_BUYER_CHECKOUT,element=state?.element;
 const current=()=>token===NP_BUYER_CHECKOUT_SEQ&&state===NP_BUYER_CHECKOUT&&element&&element===document.getElementById('pur-cycles');
 try{
  const result=await safeFetchJson(API+'/plan_purchase_options?plan_id='+encodeURIComponent(planId),{},15000,false);
  if(!current())return;
  if(!result?.success||!result.data||String(result.data.id)!==String(planId))throw Error(result?.error||'Current package terms could not be verified');
  npBuyerRender(result.data);
  const p=result.data,w=p.withdrawal_rules,parts=[];
  if(p.managed_progress){
   parts.push('Continuing eligible payouts included',Number(p.payout_split)+'% trader share');
   if(w&&typeof w.profit_cap_enabled==='boolean')parts.push(w.profit_cap_enabled?Number(w.profit_cap)+'% gross profit cap per cycle':'No gross profit cap');
   const paid=Object.values(p.reset_stage_prices||{}).some(v=>Number(v)>0)||Number(p.reset_fee)>0||Number(p.funded_reset_fee)>0;
   parts.push(paid?'Paid resets available':'No resets');
  }
  const status=document.getElementById('pur-price-status');if(status)status.textContent=parts.join(' · ');
 }catch(error){
  if(!current())return;
  state.ready=false;
  for(const id of ['pur-file','pur-submit']){const el=document.getElementById(id);if(el)el.disabled=true;}
  const status=document.getElementById('pur-price-status');if(status)status.textContent=(error.message||'Current package terms are unavailable')+'. Close checkout and reopen to retry. If already paid, do not pay again.';
 }
};
})();
