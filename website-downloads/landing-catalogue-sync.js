/* All offer pages read the same owner-managed public catalogue. */
(function(){
'use strict';
if(window.NPLandingCatalogue)return;
const base='https://nairapips-api.onrender.com';
const state={products:[],pending:false,last:0,confirmed:false};window.NPLandingCatalogue=state;
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const money=v=>'₦'+Number(v||0).toLocaleString('en-NG',{maximumFractionDigits:2});
const row=(name,value)=>'<div class="challenge-plan-row"><span>'+esc(name)+'</span><b>'+esc(value)+'</b></div>';
function card(p,i){
 const x=p.plan,two=Number(x.phase2_target)>0,progress=p.family==='progress';
 const resets=Number(x.reset_fee)>0||x.second_life_enabled===true;
 return '<div class="challenge-preview-card reveal active" data-public-product="'+esc(p.id)+'"><span class="badge">'+(two?'2-PHASE':'1-PHASE')+'</span><div class="challenge-plan-size">'+money(x.account_size)+'</div><h3 style="font-size:24px;margin-bottom:8px">'+esc(p.name)+'</h3><div class="challenge-plan-fee">Fee: '+money(x.fee)+'</div><div class="challenge-plan-table">'+row('Phase 1 Target',x.phase1_target+'%')+(two?row('Phase 2 Target',x.phase2_target+'%'):row('After Phase 1','FUNDED'))+row('Drawdown Type',x.drawdown_type||'STATIC')+row('Max Drawdown',x.max_drawdown+'%')+row('Daily Drawdown',Number(x.daily_drawdown)>0?x.daily_drawdown+'%':'None')+row('Payout',x.payout_frequency||'Daily')+row('Payout Split',x.payout_split+'%')+row('Account Reset',resets?'Available':'Not available')+(progress?row('Gross profit cap per cycle',p.package.profit_cap+'%'):'' )+row('Funded payouts',x.payout_cycles===0?'Continuing eligible payouts':(x.payout_cycles??3)+' paid cycles')+'</div><button data-landing-product="'+esc(p.id)+'" type="button" class="btn btn-primary" style="width:100%" '+(p.purchases_open?'':'disabled')+'>'+(p.purchases_open?'Start This Challenge':'Purchases paused')+'</button></div>';
}
function paint(){
 const s=document.getElementById('npStandardPlansGrid'),p=document.getElementById('npPremiumPlansGrid');if(!s||!p)return false;
 const products=state.products;window.NP_REAL_PLANS=products.map(p=>({...p.plan,raw:p.plan,catalog_product_id:p.id,purchases_open:p.purchases_open}));
 for(const [grid,premium] of [[s,false],[p,true]]){const rows=products.filter(p=>(p.family==='premium')===premium);grid.innerHTML=rows.length?rows.map(card).join(''):'<div class="np-market-empty">'+(state.confirmed?'No packages are currently shown.':'Verifying current offers…')+'</div>';}
 return true;
}
async function refresh(force=false){
 if(state.pending||(!force&&Date.now()-state.last<10000))return;
 state.pending=true;state.last=Date.now();
 try{const r=await fetch(base+'/trader/challenge_catalog',{cache:'no-store',signal:AbortSignal.timeout(18000)});const d=await r.json();if(!r.ok||!d.success||!Array.isArray(d.products))throw Error('Catalogue unavailable');state.products=d.products;state.confirmed=true;paint();}
 catch(e){state.products=[];state.confirmed=false;paint();for(const id of ['npStandardPlansGrid','npPremiumPlansGrid']){const el=document.getElementById(id);if(el)el.innerHTML='<div class="np-market-empty">Current offer terms could not be verified. Please refresh to retry.</div>';}}
 finally{state.pending=false;}
}
// Retire the old local-storage snapshot renderer so delayed responses cannot
// restore stale prices or re-enable an owner-paused purchase button.
window.npRenderRealPlans=function(){return paint()};window.loadLandingPlans=()=>refresh(true);
window.npStartRealPlan=function(i){const p=state.products[i];if(!p?.purchases_open)return;start(p)};
function start(p){try{localStorage.setItem('nairapips_selected_plan',JSON.stringify(p.plan));localStorage.setItem('nairapipsAuthMode','register')}catch(e){}location.href='/dashboard/?mode=register'}
document.addEventListener('click',event=>{const b=event.target.closest('[data-landing-product]');if(!b)return;const p=state.products.find(p=>p.id===b.dataset.landingProduct);if(p?.purchases_open)start(p)});
window.npRefreshLandingCatalogue=refresh;
window.addEventListener('focus',()=>refresh(true));document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh(true)});setInterval(()=>{if(!document.hidden)refresh()},30000);
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',()=>refresh(true));else refresh(true);
})();
