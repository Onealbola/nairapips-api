/* Saved Business Settings -> trader catalogue. No private underwriting fields. */
(function(){
'use strict';
const reused=!!window.NPSharedCatalogueState;
const state=window.NPSharedCatalogueState||{data:null,last:0,pending:false,error:'',sequence:0,needsPaint:false};
const money=v=>'₦'+Number(v).toLocaleString('en-NG',{maximumFractionDigits:2}),e=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function current(){return ['plans','purchase','packages'].includes(SESSION.currentTab)}
const css=`.np-offer-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,310px),1fr));gap:18px}.np-offer{display:flex;flex-direction:column;min-width:0;margin:0!important;padding:20px!important}.np-offer-head{display:flex;align-items:center;justify-content:space-between;gap:8px}.np-offer-state{font-size:11px;font-weight:800;color:var(--text-mute,#aaa)}.np-offer-state.open{color:var(--green,#63d998)}.np-offer h3{margin:14px 0 6px;font-size:18px}.np-offer-capital{font-size:28px;font-weight:900;color:var(--gold,#e9c760);line-height:1.2}.np-offer-price{display:flex;justify-content:space-between;gap:10px;padding:15px 0;font-size:15px}.np-offer dl{margin:0 0 18px}.np-offer-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:10px;padding:8px 0;border-top:1px solid rgba(255,255,255,.09);font-size:13px;align-items:center}.np-offer dt{color:var(--text-mute,#aaa)}.np-offer dd{margin:0;font-weight:800;text-align:right;overflow-wrap:anywhere}.np-offer button{margin-top:auto;width:100%}.np-offer button:disabled{opacity:.5;cursor:not-allowed}.np-offer-title{margin:0 0 18px}`;
let stylesheet=document.getElementById('np-offer-card-style');if(!stylesheet){stylesheet=document.createElement('style');stylesheet.id='np-offer-card-style';document.head.appendChild(stylesheet)}stylesheet.textContent=css;
const field=(label,value)=>'<div class="np-offer-row"><dt>'+e(label)+'</dt><dd>'+e(value)+'</dd></div>';
function productCard(product){
 const progress=product.family==='progress',p=progress?product.package:product.plan;
 const cap=progress?p.capital:p.account_size,price=progress?p.price:p.fee,share=progress?p.profit_share:p.payout_split;
 const two=progress||Number(p.phase2_target)>0||String(p.challenge_journey||'').includes('phase2');
 const dd=progress?p.drawdown:p.max_drawdown,daily=Number(p.daily_drawdown||0);
 const cycles=progress||p.payout_cycles===0?'Continuing':String(p.payout_cycles??3);
 const phaseReset=progress?'None':Number(p.reset_fee)>0?money(p.reset_fee):'None';
 const fundedReset=progress?'None':p.funded_reset_fee==null?'At request':Number(p.funded_reset_fee)>0?money(p.funded_reset_fee):'None';
 return '<article class="card np-offer" data-catalogue-card="'+e(product.id)+'"><div class="np-offer-head"><span class="badge">'+e(({standard:'STANDARD',premium:'PREMIUM',progress:'2-PHASE'})[product.family]||'CHALLENGE')+'</span><span class="np-offer-state '+(product.purchases_open?'open':'')+'">'+(product.purchases_open?'Open':'Paused')+'</span></div><h3>'+e(progress?p.name:product.name)+'</h3><div class="np-offer-capital">'+money(cap)+'</div><div class="np-offer-price"><span>Price</span><strong>'+money(price)+'</strong></div><dl>'+field('Phases',two?'2':'1')+field('Phase 1 target',(p.phase1_target??10)+'%')+(two?field('Phase 2 target',(p.phase2_target??8)+'%'):'')+field('Max drawdown',dd+'%')+field('Max loss',money(Number(cap)*Number(dd)/100))+field('Daily drawdown',daily?daily+'%':'None')+field('Profit share',share+'%')+(progress?field('Gross cap / cycle',p.profit_cap+'%'):'')+field('Payout cycles',cycles)+field('Phase reset',phaseReset)+field('Funded reset',fundedReset)+'</dl><button type="button" class="btn btn-gold" data-catalog-product="'+e(product.id)+'" '+(product.purchases_open?'':'disabled')+'>'+(product.purchases_open?'Choose package':'Paused')+'</button></article>';
}
window.npSharedCatalogueHTML=function(family='all'){
 if(!state.pending&&Date.now()-state.last>10000)queueMicrotask(()=>refresh());
 if(!state.data)return '<section class="card"><h3>Challenge catalogue</h3><p>'+(state.error?'Current prices could not be confirmed. Please retry.':'Loading current prices and package availability…')+'</p><button class="btn" onclick="npRefreshSharedCatalogue(true)">Retry catalogue</button></section>';
 const rows=state.data.products.filter(p=>family==='all'||p.family===family);
 if(rows.length&&window.NPPlanSelector)return window.NPPlanSelector.render(rows,{key:'trader-'+family,context:'trader'});
 if(!rows.length)return '<section class="card"><p>No '+(family==='progress'?'new packages':'challenge packages')+' are visible right now.</p></section>';
 return '<section data-shared-catalogue><h2 class="np-offer-title">'+(family==='progress'?'Build your package':'Challenge packages')+'</h2><div class="np-offer-grid">'+rows.map(productCard).join('')+'</div></section>';
};
function repaint(){if(current()&&typeof renderApp==='function')renderApp()}
if(reused){if(!document.querySelector('.modal-bg'))repaint();return;}
async function refresh(force=false){
 if(state.pending||(!force&&Date.now()-state.last<10000)||!current()||!(SESSION.trader||SESSION.data?.trader))return;
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

 // Use only the selected, current server plan when opening the existing signed
 // price/payment flow. That flow checks price again, and the API gates submission.
 SESSION.plansList=(SESSION.plansList||[]).filter(x=>String(x.id)!==p.plan_id&&String(x.id)!==p.plan.id);SESSION.plansList.push({...p.plan,status:'active'});purchasePlan(p.plan.id);
});
const oldBuilder=window.npRenderPackageBuilderV222;
if(typeof oldBuilder==='function')window.npRenderPackageBuilderV222=function(){return window.npSharedCatalogueHTML('progress')+'<details style="margin-top:24px"><summary>Other custom-package tools and existing orders</summary>'+oldBuilder()+'</details>'};
window.addEventListener('focus',()=>refresh(true));document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh(true)});
setInterval(()=>{if(!document.hidden)refresh()},30000);
window.NPSharedCatalogueState=state;queueMicrotask(()=>{if(current()&&(SESSION.trader||SESSION.data?.trader))refresh(true)});
})();
