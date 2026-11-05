/* Saved Business Settings -> trader catalogue. No private underwriting fields. */
(function(){
'use strict';
const state={data:null,last:0,pending:false,error:'',sequence:0,needsPaint:false};
const money=v=>'₦'+Number(v).toLocaleString('en-NG',{maximumFractionDigits:2}),e=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function current(){return ['plans','purchase','packages'].includes(SESSION.currentTab)}
function productCard(product){
 const p=product.family==='progress'?product.package:product.plan,progress=product.family==='progress';
 const cap=progress?p.capital:p.account_size,price=progress?p.price:p.fee,share=progress?p.profit_share:p.payout_split;
 const route=progress?[p.phase1_target,p.phase2_target]:String(p.challenge_journey||'').includes('phase2')||String(p.challenge_journey||'').includes('two')||Number(p.phase2_target)>0?[p.phase1_target??10,p.phase2_target??8]:[p.phase1_target??10];
 const dd=progress?p.drawdown:p.max_drawdown,cycles=progress?'Continuing eligible payouts':p.payout_cycles===0?'Continuing eligible payouts':(p.payout_cycles??3)+' paid funded cycles';
 return '<article class="card" style="margin:0;min-width:0"><div class="badge">'+e(({standard:'Standard',premium:'Premium',progress:'Build your package'})[product.family]||'Challenge')+'</div><h3 style="margin:12px 0">'+e(product.name)+'</h3><div style="font-size:27px;font-weight:900;color:var(--gold)">'+money(cap)+'</div><p style="font-weight:800">'+(progress&&!product.purchases_open?'Proposed fee: ':'Base fee: ')+money(price)+'</p><p>'+route.length+' phase'+(route.length===1?'':'s')+' · targets '+route.map(x=>e(x)+'%').join(' / ')+'<br>'+e(dd)+'% maximum drawdown · '+money(Number(cap)*Number(dd)/100)+' loss allowance<br>'+e(share)+'% trader share<br>'+e(cycles)+(progress?'<br>'+e(p.profit_cap)+'% gross profit cap per cycle<br>'+(p.daily_drawdown?e(p.daily_drawdown)+'% daily drawdown rule':'No separate daily drawdown rule'):'')+'</p>'+(progress&&!product.purchases_open?'<p class="text-sm">Coming soon. These are proposed terms; payment is not available.</p>':'')+(!progress?'<p>Phase reset: '+(Number(p.reset_fee)>0?money(p.reset_fee):'Not available')+'<br>Funded reset: '+(p.funded_reset_fee==null?'Automatic price, confirmed at request':Number(p.funded_reset_fee)>0?money(p.funded_reset_fee):'Not available')+'</p>':'')+(!progress&&Array.isArray(p.cycle_options)?'<details><summary>Payout-cycle options</summary>'+p.cycle_options.filter(x=>x.available).map(x=>'<div>'+e(x.cycles===0?'Continuing':x.cycles+' cycles')+' · '+money(x.fee)+'</div>').join('')+'</details>':'')+'<button type="button" class="btn btn-gold" data-catalog-product="'+e(product.id)+'" '+(product.purchases_open?'':'disabled')+'>'+(product.purchases_open?'Choose package':progress?'Coming soon':'Purchases paused')+'</button></article>';
}
window.npSharedCatalogueHTML=function(family='all'){
 if(!state.pending&&Date.now()-state.last>10000)queueMicrotask(()=>refresh());
 if(!state.data)return '<section class="card"><h3>Challenge catalogue</h3><p>'+(state.error?'Current prices could not be confirmed. Please retry.':'Loading current prices and package availability…')+'</p><button class="btn" onclick="npRefreshSharedCatalogue(true)">Retry catalogue</button></section>';
 const rows=state.data.products.filter(p=>family==='all'||p.family===family);
 if(!rows.length)return '<section class="card"><p>No '+(family==='progress'?'new packages':'challenge packages')+' are visible right now.</p></section>';
 return '<section data-shared-catalogue><h2>'+(family==='progress'?'Build your package':'Challenge packages')+'</h2><p>Current prices and benefits from NairaPips. Availability refreshes automatically. Existing purchases retain their purchased terms.</p><div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,250px),1fr));gap:16px">'+rows.map(productCard).join('')+'</div></section>';
};
function repaint(){if(current()&&typeof renderApp==='function')renderApp()}
async function refresh(force=false){
 if(state.pending||(!force&&Date.now()-state.last<10000)||!current()||!SESSION.trader)return;
 state.pending=true;state.last=Date.now();const sequence=++state.sequence;
 try{
  const r=await fetch(API+'/trader/challenge_catalog',{cache:'no-store',signal:AbortSignal.timeout(18000)});const data=await r.json();
  if(!r.ok||!data.success||!Array.isArray(data.products))throw Error('Catalogue unavailable');
  if(sequence!==state.sequence)return;
  const changed=JSON.stringify(state.data?.products)!==JSON.stringify(data.products)||state.error;
  state.data=data;state.error='';state.needsPaint=changed||state.needsPaint;
  if(state.needsPaint&&!document.querySelector('.modal-bg')){state.needsPaint=false;repaint();}
 }catch(err){state.data=null;state.error='Catalogue unavailable';state.needsPaint=true;if(!document.querySelector('.modal-bg')){state.needsPaint=false;repaint()}}
 finally{state.pending=false}
}
window.npRefreshSharedCatalogue=refresh;
document.addEventListener('click',event=>{const button=event.target.closest('[data-catalog-product]');if(!button)return;const p=state.data?.products.find(x=>x.id===button.dataset.catalogProduct);if(!p?.purchases_open)return;
 if(p.family==='progress'){toast('This package requires its verified purchase flow. Please refresh the catalogue.','err');return}
 // Use only the selected, current server plan when opening the existing signed
 // price/payment flow. That flow checks price again, and the API gates submission.
 SESSION.plansList=(SESSION.plansList||[]).filter(x=>String(x.id)!==p.plan_id&&String(x.id)!==p.plan.id);SESSION.plansList.push({...p.plan,status:'active'});purchasePlan(p.plan.id);
});
const oldBuilder=window.npRenderPackageBuilderV222;
if(typeof oldBuilder==='function')window.npRenderPackageBuilderV222=function(){return window.npSharedCatalogueHTML('progress')+'<details style="margin-top:24px"><summary>Other custom-package tools and existing orders</summary>'+oldBuilder()+'</details>'};
window.addEventListener('focus',()=>refresh(true));document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh(true)});
setInterval(()=>{if(!document.hidden)refresh()},30000);
window.NPSharedCatalogueState=state;queueMicrotask(()=>{if(current()&&SESSION.trader)refresh(true)});
})();
