/* New package client. Legacy products and accounts remain separate. */
(function(){
'use strict';
const labels={capital:'Account size',phases:'Evaluation phases',drawdown:'Maximum drawdown %',drawdown_type:'Drawdown type',phase1_target:'Phase 1 target %',phase2_target:'Phase 2 target %',profit_share:'Your profit share %',profit_cap:'Gross profit cap per funded cycle %',payout_cycles:'Paid funded cycles',phase_resets:'Included phase resets',funded_resets:'Included funded resets'};
const facilityOrder=['capital','phases','drawdown','drawdown_type','phase1_target','phase2_target','profit_share','profit_cap','payout_cycles','phase_resets','funded_resets'];
const state={price:null,priceSequence:0,owner:'',catalog:null,choices:{},quote:null,contracts:[],historyError:'',historyPage:1,hasMore:false,sequence:0,historySequence:0,pending:null,receipt:null,loading:false,busy:false,operation:null,lastLoad:0};
const e=x=>esc(String(x??'')), money=x=>'₦'+Number(x).toLocaleString('en-NG',{minimumFractionDigits:2,maximumFractionDigits:2});
const owner=()=>String(SESSION.trader?.id||SESSION.data?.trader?.id||'');
const key=()=>`np-package-checkout-v1:${state.owner}`;
function status(text){const n=document.getElementById('np-package-status');if(n)n.textContent=text;}
let priceTimer=null;
const pretty=(key,value)=>key==='capital'?money(value):key==='phases'?(value==='one'?'One phase':'Two phases'):key==='drawdown_type'?(value==='static'?'Static':'Trailing'):key==='profit_cap'&&value==='none'?'No cap':key==='payout_cycles'&&value==='unlimited'?'Unlimited':['drawdown','phase1_target','phase2_target','profit_share','profit_cap'].includes(key)?value+'%':value;
function describePrice(){return state.price?'Base '+money(state.price.base)+(state.price.additions.some(a=>Number(a.percent)>0||Number(a.fixed)>0)?' · '+state.price.additions.filter(a=>Number(a.percent)>0||Number(a.fixed)>0).map(a=>labels[a.facility]+': '+(a.price_type==='fixed'?money(a.fixed):a.percent+'% of base')).join(' · '):' · No paid additions'):'';}
function updatePrice(){
 const n=document.getElementById('np-package-live-price'),detail=document.getElementById('np-package-price-note');
 if(n)n.textContent=state.price?money(state.price.total):'—';
 if(detail)detail.textContent=state.price?'Price based on your selected benefits. Confirm your package before payment.':'Choose your benefits to calculate the current price.';
 const breakdown=document.getElementById('np-package-price-breakdown');if(breakdown)breakdown.textContent=describePrice();
 const mobile=document.getElementById('np-package-mobile-price');if(mobile)mobile.textContent=state.price?money(state.price.total):'—';
}
function schedulePrice(){
 clearTimeout(priceTimer);const sequence=++state.priceSequence,id=state.owner;
 if(!state.catalog?.available||state.pending)return;
 state.price=null;updatePrice();
 priceTimer=setTimeout(async()=>{
  const selection=JSON.stringify(state.choices);
  try{const r=await send('/trader/package_price','POST',{choices:state.choices});
   if(id!==owner()||sequence!==state.priceSequence||selection!==JSON.stringify(state.choices))return;
   if(!r.price||!Number.isFinite(r.price.total)||r.price.total<=0)throw Error('Price was not confirmed');
   state.price=r.price;updatePrice();
  }catch(err){if(id===owner()&&sequence===state.priceSequence)status(err.message);}
 },220);
}
function repaint(){if(SESSION.currentTab==='packages')renderApp();}
async function send(path,method='GET',body){
 const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),20000);
 try{
  const headers={Authorization:'Bearer '+SESSION.auth_token};if(body!==undefined)headers['Content-Type']='application/json';
  const r=await fetch(API+path,{method,headers,body:body===undefined?undefined:JSON.stringify(body),signal:controller.signal,cache:'no-store'});
  let data;try{data=await r.json();}catch(_){throw Error('The service response was not confirmed.');}
  if(!r.ok||data.success===false){const err=Error(data.error||'Package request failed');err.purchaseStatus=data.purchase_status;throw err;}
  return data;
 }finally{clearTimeout(timer);}
}
function retainPending(){try{if(state.pending)sessionStorage.setItem(key(),JSON.stringify({quote:state.pending.quote,checkout_key:state.pending.checkout_key}));else sessionStorage.removeItem(key());}catch(_){}}
function syncOwner(){
 const id=owner();if(!id)return false;if(id===state.owner)return true;
 clearTimeout(priceTimer);
 Object.assign(state,{price:null,priceSequence:state.priceSequence+1,owner:id,catalog:null,choices:{},quote:null,contracts:[],historyError:'',historyPage:1,hasMore:false,sequence:state.sequence+1,historySequence:state.historySequence+1,pending:null,receipt:null,loading:false,busy:false,operation:null,lastLoad:0});
 try{const saved=JSON.parse(sessionStorage.getItem(key())||'null');if(saved?.quote?.document?.trader_id===id&&saved.checkout_key)state.pending=saved;}catch(_){}
 return true;
}
window.npPackageVisibleV222=()=>!!(state.catalog?.available||state.contracts.length||state.pending);
window.npPackageMaybeLoadV222=async function(force=false){
 if(!syncOwner()||state.loading||(!force&&Date.now()-state.lastLoad<30000))return;
 const id=state.owner;state.loading=true;state.lastLoad=Date.now();
 try{
  const result=await send('/trader/package_catalog');if(owner()!==id||state.owner!==id)return;
  if(typeof result.available!=='boolean'||(result.available&&(!result.options||typeof result.options!=='object')))throw Error('Package availability returned an invalid response.');
  state.catalog=result;
  if(result.available){
   for(const [k,rows] of Object.entries(result.options))if(!rows.some(r=>r.value===state.choices[k]))state.choices[k]=rows[0]?.value||'';
   if(state.choices.phases==='one')state.choices.phase2_target=null;
  }
  await loadHistory();
 }catch(err){if(owner()===id){state.catalog={available:false};state.historyError=err.message;}}
 finally{if(state.owner===id){state.loading=false;if(SESSION.currentTab==='packages'||state.catalog?.available||state.contracts.length)renderApp();schedulePrice();}}
};
async function loadHistory(){
 const id=state.owner,page=state.historyPage,sequence=++state.historySequence;
 try{
  const r=await send('/trader/package_contracts?page='+page);
  if(owner()!==id||sequence!==state.historySequence)return;
  if(!Array.isArray(r.contracts))throw Error('Contract history returned an invalid response.');
  state.contracts=r.contracts;state.hasMore=!!r.has_more;state.historyError='';
  if(state.pending&&state.contracts.some(c=>c.purchase_id===state.pending.quote.id)){state.receipt={purchase_id:state.pending.quote.id};state.pending=null;retainPending();}
 }catch(err){if(owner()===id&&sequence===state.historySequence)state.historyError=err.message;}
}
function terms(doc){
 if(!doc)return '';
 const rows=[['Capital',money(doc.capital)],['Stages',doc.stages.join(' → ')],...doc.stages.map(s=>[s==='funded'?'Funded DD':s+' target / DD',s==='funded'?doc.stage_rules[s].drawdown_percent+'% '+doc.stage_rules[s].drawdown_type:doc.stage_rules[s].target_percent+'% / '+doc.stage_rules[s].drawdown_percent+'% '+doc.stage_rules[s].drawdown_type]),['Trader profit share',doc.profit_share_percent+'%'],['Gross profit cap',doc.gross_profit_cap_percent===null?'No cap':doc.gross_profit_cap_percent+'%'],['Paid cycles',doc.payout_cycles===null?'Unlimited':doc.payout_cycles],['Included phase resets',doc.phase_resets],['Included funded resets',doc.funded_resets],['Purchase price',money(doc.purchase_price)]];
 return '<dl>'+rows.map(([k,v])=>`<div style="display:flex;justify-content:space-between;gap:12px;margin:8px 0;flex-wrap:wrap"><dt>${e(k)}</dt><dd style="font-weight:700">${e(v)}</dd></div>`).join('')+'</dl><p>Evaluation targets use equity and require no open positions before passing. Paid funded cycles use fresh accounts. An old paid or breached account stays retired.</p>';
}
window.npRenderPackageBuilderV222=function(){
 syncOwner();const pending=state.pending, q=pending?.quote||state.quote;
 const banks=(SESSION.paymentAccounts||[]).filter(b=>String(b.status||'active').toLowerCase()==='active');
 return `<div class="card"><h2>Build Your Package</h2><p>Choose your benefits and review the price before payment. Your purchase keeps its agreed rules. Account delivery follows payment verification and matching account availability.</p>
 ${state.receipt?`<p role="status">Purchase ${e(state.receipt.purchase_id)} is recorded. Open Purchase Challenge history for its current approval and assignment status.</p>`:''}
 ${pending?'<p role="alert">A submission needs confirmation. Retry the same purchase or check its history before making another purchase. After refreshing, select the payment proof file again.</p>':''}
 ${state.catalog?.available&&!pending?`<div class="builder-layout"><section class="card"><h3>Choose your benefits</h3><div class="grid">${Object.entries(state.catalog.options).sort(([a],[b])=>facilityOrder.indexOf(a)-facilityOrder.indexOf(b)).map(([k,rows])=>`<label id="np-package-choice-wrap-${k}" style="${k==='phase2_target'&&state.choices.phases==='one'?'display:none':''}">${e(labels[k])}<select class="input" id="np-package-choice-${k}" onchange="npPackageChoiceChangedV222()">${rows.map(r=>`<option value="${e(r.value)}" ${state.choices[k]===r.value?'selected':''}>${e(pretty(k,r.value))}</option>`).join('')}</select></label>`).join('')}</div></section><aside class="card summary-card"><h3>Your package</h3><div id="np-package-live-price" class="price" aria-live="polite">${state.price?e(money(state.price.total)):'—'}</div><p id="np-package-price-note">Price based on your selected benefits.</p><details><summary>Price breakdown</summary><p id="np-package-price-breakdown">${e(describePrice())}</p></details><dl>${Object.entries(state.choices).sort(([a],[b])=>facilityOrder.indexOf(a)-facilityOrder.indexOf(b)).filter(([k])=>k!=='phase2_target'||state.choices.phases==='two').map(([k,v])=>`<div class="summary-line"><dt>${e(labels[k])}</dt><dd>${e(pretty(k,v))}</dd></div>`).join('')}</dl><button class="btn btn-gold" id="np-package-quote-button" ${state.busy?'disabled':''} onclick="npRequestPackageQuoteV222(this)">Review package and payment</button></aside></div><div class="mobile-summary"><div><small>Your package price</small><strong id="np-package-mobile-price">${state.price?e(money(state.price.total)):'—'}</strong></div><button class="secondary" onclick="document.querySelector('.summary-card').scrollIntoView()">View summary</button></div>`:(!pending?'<p>New package purchases are currently closed. Your purchased contracts remain available below.</p>':'')}
 ${q?`<div class="card" style="margin-top:16px"><h3>Review your exact package</h3>${terms(q.document)}<p>Catalog version ${e(q.catalog_revision)} · Quote expires ${e(new Date(q.expires_at).toLocaleString())}</p>
 ${banks.map(b=>`<p>${e(b.bank_name)} · ${e(b.account_name)} · ${e(b.account_number)}</p>`).join('')}
 <label style="display:block;margin:12px 0"><input id="np-package-accepted" type="checkbox"> I accept these rules and the purchase price.</label>
 <label>Payment proof (JPG, PNG, WEBP or PDF, up to 10MB)<input class="input" type="file" id="np-package-proof" accept="image/jpeg,image/png,image/webp,application/pdf"></label>
 <button class="btn btn-gold" id="np-package-submit-button" ${state.busy?'disabled':''} onclick="npSubmitPackageV222(this)">${pending?'Retry same purchase':'Submit payment proof'}</button></div>`:''}
 <p id="np-package-status" role="status"></p></div>
 <div class="card" style="margin-top:16px"><h3>Your purchased package contracts</h3>
 ${state.historyError?`<p>${e(state.historyError)}</p><button class="btn" onclick="npRefreshPackageHistoryV222()">Retry history</button>`:state.contracts.map(c=>`<details><summary>Purchase ${e(c.purchase_id)} · ${e(new Date(c.created_at).toLocaleDateString())}</summary>${terms(c.document)}<p>Rules version ${e(c.rules_version)} · Contract ${e(c.id)}</p></details>`).join('')||'<p>No purchased package contracts on this page.</p>'}
 <div style="display:flex;gap:12px;margin-top:12px"><button class="btn" ${state.historyPage<=1?'disabled':''} onclick="npPackageHistoryPageV222(-1)">Previous</button><span>Page ${state.historyPage}</span><button class="btn" ${!state.hasMore?'disabled':''} onclick="npPackageHistoryPageV222(1)">Next</button></div></div>`;
};
window.npPackageChoiceChangedV222=function(){
 if(state.pending||state.operation==='checkout')return;
 for(const k of Object.keys(state.catalog.options)){const n=document.getElementById('np-package-choice-'+k);if(n)state.choices[k]=n.value;}
 if(state.choices.phases==='one')state.choices.phase2_target=null;
 else if(!state.choices.phase2_target)state.choices.phase2_target=state.catalog.options.phase2_target?.[0]?.value||'';
 state.quote=null;state.sequence++;state.price=null;repaint();schedulePrice();
};
window.npRequestPackageQuoteV222=async function(button){
 if(state.pending||state.busy)return;clearTimeout(priceTimer);state.priceSequence++;state.busy=true;state.operation='quote';button.disabled=true;
 const id=state.owner,sequence=++state.sequence,selection=JSON.stringify(state.choices);status('Saving your package quote…');
 try{const r=await send('/trader/package_quote','POST',{choices:state.choices});if(owner()!==id||sequence!==state.sequence||selection!==JSON.stringify(state.choices))return;if(!r.quote?.id||r.quote.document?.trader_id!==id||!Array.isArray(r.quote.document?.stages))throw Error('The saved quote could not be verified.');state.quote=r.quote;state.price=r.quote.price_breakdown;repaint();}
 catch(err){if(owner()===id&&sequence===state.sequence)status(err.message);}
 finally{if(state.owner===id){state.busy=false;state.operation=null;button.disabled=false;for(const buttonId of ['np-package-quote-button','np-package-submit-button']){const current=document.getElementById(buttonId);if(current)current.disabled=false;}}}
};
async function upload(file){
 const data=new FormData();data.append('file',file,file.name);data.append('folder','challenge-purchases');data.append('trader_id',state.owner);
 const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),30000);
 try{
  const r=await fetch(API+'/upload_payment_proof',{method:'POST',headers:{Authorization:'Bearer '+SESSION.auth_token},body:data,signal:controller.signal,cache:'no-store'});
  const d=await r.json();if(!r.ok||d.success===false)throw Error(d.error||'Payment proof upload failed');
  const url=d.url||d.file_url||d.payment_proof_url||d.data?.url||d.data?.file_url||d.data?.payment_proof_url;
  if(!url)throw Error('Upload did not return a payment proof URL');return url;
 }finally{clearTimeout(timer);}
}
window.npSubmitPackageV222=async function(button){
 if(state.busy)return;const q=state.pending?.quote||state.quote,id=state.owner;
 if(!q||!document.getElementById('np-package-accepted')?.checked){status('Review and accept the package rules first.');return;}
 const file=document.getElementById('np-package-proof')?.files?.[0];
 if(!file||file.size>10*1024*1024||!['image/jpeg','image/png','image/webp','application/pdf'].includes(file.type)){status('Choose a JPG, PNG, WEBP or PDF payment proof up to 10MB.');return;}
 state.busy=true;state.operation='checkout';button.disabled=true;
 document.querySelectorAll('[id^="np-package-choice-"]').forEach(n=>{if(n.tagName==='SELECT')n.disabled=true;});
 try{
  const proof=await upload(file);if(owner()!==id)return;
  if(!state.pending){state.pending={quote:q,checkout_key:crypto.randomUUID()};retainPending();}
  const p=state.pending;status('Submitting payment proof…');
  const result=await send('/trader/package_checkout','POST',{quote_id:p.quote.id,checkout_key:p.checkout_key,payment_proof_url:proof,accepted_rules_hash:p.quote.rules_hash});
  if(owner()!==id)return;state.receipt=result;state.pending=null;state.quote=null;retainPending();await loadHistory();repaint();
 }catch(err){if(owner()===id){if(err.purchaseStatus==='rejected'){state.pending=null;state.quote=null;retainPending();}repaint();status(err.message);}}
 finally{if(state.owner===id){state.busy=false;state.operation=null;button.disabled=false;for(const id of ['np-package-submit-button','np-package-quote-button']){const n=document.getElementById(id);if(n)n.disabled=false;}document.querySelectorAll('[id^="np-package-choice-"]').forEach(n=>{if(n.tagName==='SELECT')n.disabled=false;});}}
};
window.npRefreshPackageHistoryV222=async function(){await loadHistory();repaint();};
window.npPackageHistoryPageV222=async function(delta){state.historyPage=Math.max(1,state.historyPage+delta);await loadHistory();repaint();};
})();
