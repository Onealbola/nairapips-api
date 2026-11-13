/* Account-bound recovery offers; backend independently verifies every quote/write. */
(function(){
'use strict';
if(window.NPRecoveryOfferScope==='20261113-v1')return;
window.NPRecoveryOfferScope='20261113-v1';
const session=()=>typeof SESSION!=='undefined'?SESSION:null;
const saved=()=>{try{return JSON.parse(sessionStorage.getItem('np_pending_private_offer')||'null')}catch(_){return null}};
function clear(){const s=session();if(s){s.pendingPrivateOfferId='';s.pendingPrivateOfferCode='';s.pendingPrivateOfferScope=null}try{sessionStorage.removeItem('np_pending_private_offer')}catch(_){}}
function pending(){const s=session(),old=saved();return s?.pendingPrivateOfferScope||s?.offers?.find(o=>String(o.id)===String(s.pendingPrivateOfferId||old?.id))?.recovery_scope||old?.scope||null}
if(typeof window.purchasePlan==='function'){
 const core=window.purchasePlan;
 window.purchasePlan=function(id,code,...rest){
  const scope=pending(),s=session(),captured=String(code||s?.pendingPrivateOfferCode||saved()?.code||'');
  if(scope&&String(id)!==String(scope.eligible_plan_id)){
   // A normal catalogue selection keeps its normal price; never carry a private
   // recovery code across products through persistent SESSION state.
   if(code&&String(code)===String(s?.pendingPrivateOfferCode||saved()?.code||'')){toast('This offer applies only to '+scope.plan_name+' for the linked breached account.','err');return}
   clear();return core.call(this,id,'',...rest);
  }
  if(captured&&!scope){
   const offer=s?.offers?.find(o=>String(o.offer_code||o.promo_code||'')===captured);
   if(s?.pendingPrivateOfferId||saved()?.id){
    clear();
    if(code){toast('Refresh Offers to verify the linked account before using this discount.','err');return}
    return core.call(this,id,'',...rest);
   }
   if(offer&&!offer.message_only){clear();toast('Refresh Offers to verify the linked account before using this discount.','err');return}
  }
  return core.call(this,id,code,...rest);
 };
}
window.npClaimPrivateOffer=async function(id){
 try{
  const s=session(),t=s?.data?.trader||s?.trader||{};
  const result=await safeFetchJson(API+'/claim_private_offer',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({offer_id:id,trader_id:t.id})});
  const claim=result?.data,scope=claim?.recovery_scope;
  if(result?.success===false||!scope?.eligible_plan_id)throw Error(result?.error||'The eligible account could not be verified.');
  await loadTraderModuleData('purchase');
  const plan=(s.plansList||[]).find(p=>String(p.id)===String(scope.eligible_plan_id));
  if(!plan)throw Error('The account linked to this offer is currently unavailable. This offer cannot be transferred to another plan.');
  s.pendingPrivateOfferId=String(id);s.pendingPrivateOfferCode=String(claim.offer_code||claim.promo_code||'');s.pendingPrivateOfferScope=scope;
  try{sessionStorage.setItem('np_pending_private_offer',JSON.stringify({id,code:s.pendingPrivateOfferCode,scope,at:Date.now()}))}catch(_){}
  purchasePlan(plan.id,s.pendingPrivateOfferCode);
 }catch(error){toast(error.message||'Offer verification failed. Please retry.','err')}
};
})();
