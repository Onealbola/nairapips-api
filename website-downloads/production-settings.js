/* Owner controls: real API persistence; no writes to issued account rules. */
(function(){
'use strict';
let controls=null, catalog=[], packages=null, loadSequence=0;
const e=x=>esc(String(x??''));
const journey=r=>String(r.challenge_journey||'').includes('two')||String(r.challenge_journey||'').includes('phase2')?'two_phase':'one_phase';
const message=(id,text)=>{const n=document.getElementById(id);if(n)n.textContent=text;};
const input=(id,value,extra='')=>`<input class="input" id="${id}" value="${e(value)}" ${extra}>`;
const facilityOrder=['capital','phases','drawdown','drawdown_type','phase1_target','phase2_target','profit_share','profit_cap','payout_cycles','phase_resets','funded_resets'];
const choiceRow=(key,r,i)=>`<div class="grid md:grid-cols-4 gap-2" style="margin:12px 0;padding:12px;border:1px solid rgba(247,200,67,.15);border-radius:12px"><label>Benefit value${input(`np-package-${key}-${i}-value`,r.value,['phases','drawdown_type'].includes(key)?'readonly':'')}</label>${key==='capital'?'':`<label>Charge in<select class="input" id="np-package-${key}-${i}-price-type"><option value="percent" ${r.price_type!=='fixed'?'selected':''}>% of base price</option><option value="fixed" ${r.price_type==='fixed'?'selected':''}>₦ amount</option></select></label>`}<label>${key==='capital'?'Base price ₦':'Benefit charge'}${input(`np-package-${key}-${i}-price`,r.price??'','type="number" min="0" step="0.01"')}</label><label style="display:flex;align-items:center;gap:10px"><input type="checkbox" id="np-package-${key}-${i}-enabled" ${r.enabled?'checked':''}> Active</label></div>`;
window.npLoadProductionSettings=async function(){
  const node=document.getElementById('np-production-settings');if(!node)return;
  const sequence=++loadSequence;
  node.innerHTML='<h3>Production controls</h3><p>Loading saved settings…</p>';
  try{
    const results=await Promise.all([api('/admin/production_controls'),api('/admin/production_catalog'),api('/admin/package_configuration')]);
    if(sequence!==loadSequence||document.getElementById('np-production-settings')!==node)return;
    const [c,p,f]=results;controls=c;catalog=p.plans;packages=f;
    node.innerHTML=`<h3>Business settings</h3><p>Set prices and benefits below. Save changes whenever you need; completed purchases retain their agreed terms.</p><details><summary>Service availability</summary><h3>Service release controls</h3><p>Tick to accept new requests. Unticking keeps history, issued account protection and existing settlement obligations.</p>
      <div>${c.facilities.map(r=>`<label style="display:block;margin:12px 0"><input type="checkbox" id="np-control-${e(r.key)}" ${c.data.features[r.key]?'checked':''}> ${e(r.label)}</label>`).join('')}</div>
      <button class="btn btn-gold" onclick="npSaveProductionControls(this)">Save service switches</button>
      <p id="np-control-message" role="status"></p>
      <p>Saved revision ${e(c.data.revision)} · Changes reach the server within ${e(c.propagation_seconds)} seconds.</p>
      <p>Always protected: ${c.protected.map(e).join(' · ')}.</p>
      </details><details><summary>Existing products: edit, pause or reopen</summary><h3>Products: available now or held for later</h3>
      <p>Inactive products stay in production and can be opened for new sales later. Existing customers retain their purchased terms. Create a new version to change the commercial terms of a product already sold.</p>
      <div>${catalog.map((r,i)=>`<div class="card" style="margin:12px 0"><h4>${e(r.name)} · ${e(r.account_size)}</h4>
        <label><input type="checkbox" id="np-plan-active-${i}" ${r.status==='active'?'checked':''}> Open for new purchases</label>
        <div class="grid md:grid-cols-3 gap-2" style="margin-top:12px">
          ${[['name','Product name'],['fee','Purchase fee ₦'],['account_size','Account size ₦'],['phase1_target','Phase 1 target %'],['phase2_target','Phase 2 target % (0 for one phase)'],['max_drawdown','Maximum drawdown %'],['payout_split','Trader share % (60 or 70)'],['reset_fee','Phase reset fee ₦']].map(([key,label])=>`<label>${label}${input(`np-plan-${i}-${key}`,r[key]??'',key==='name'?'maxlength="100"':'type="number" min="0" step="0.01"')}</label>`).join('')}
          <label>Evaluation route<select class="input" id="np-plan-${i}-challenge_journey"><option value="one_phase" ${journey(r)==='one_phase'?'selected':''}>One phase</option><option value="two_phase" ${journey(r)==='two_phase'?'selected':''}>Two phases</option></select></label>
        </div><p>Drawdown: ${e(r.drawdown_type||'STATIC')}. Current funded reset pricing remains twice the challenge fee.</p>
        <button class="btn btn-gold" onclick="npSaveProductionPlan(${i},this)">Save product</button>
        <button class="btn" onclick="npCloneProductionPlan(${i},this)">Create new inactive version</button>
        <p id="np-plan-message-${i}" role="status"></p></div>`).join('')}</div>
      </details><hr style="margin:24px 0"><h3 id="np-package-pricing-section">Package prices & benefits</h3>
      <p>Every choice has its own editable price and active status. Enter 0 for an included benefit. Leave future choices inactive. Saving changes future quotes and preserves existing purchase terms.</p>
      <p>${e(f.price_rule)}</p>
      <label>Minimum purchase price ₦ ${input('np-package-minimum',f.data.minimum_price??'','type="number" min="0.01" step="0.01"')}</label>
      ${Object.entries(f.data.options).sort(([a],[b])=>facilityOrder.indexOf(a)-facilityOrder.indexOf(b)).map(([key,rows])=>`<div class="card" style="margin:12px 0"><h4>${e(f.labels[key])}</h4><div id="np-package-rows-${key}">${rows.map((r,i)=>choiceRow(key,r,i)).join('')}</div>${['phases','drawdown_type'].includes(key)?'':`<button class="btn" onclick="npAddPackageOption('${key}')">Add choice</button>`}</div>`).join('')}
      <label><input id="np-package-release" type="checkbox" ${f.release_ready||f.data.enabled?'':'disabled'} ${f.data.enabled?'checked':''}> Release new package checkout</label>
      <p>Release is locked until: ${f.release_blockers.map(e).join('; ')}. Saved prices and choices are real configuration; customers cannot purchase an unenforced contract.</p>
      <button class="btn btn-gold" onclick="npSavePackageConfiguration(this)">Save prices and benefits</button>
      <p id="np-package-message" role="status"></p>
      <h4>Check a package price using saved settings</h4>
      <div class="grid md:grid-cols-3 gap-2">${Object.entries(f.data.options).sort(([a],[b])=>facilityOrder.indexOf(a)-facilityOrder.indexOf(b)).map(([key,rows])=>`<label>${e(f.labels[key])}<select class="input" id="np-package-preview-${key}">${rows.filter(r=>r.enabled).map(r=>`<option value="${e(r.value)}">${e(r.value)}</option>`).join('')}</select></label>`).join('')}</div>
      <button class="btn" onclick="npPreviewPackageQuote(this)">Calculate saved price</button>
      <p id="np-package-quote" role="status"></p>
      <hr style="margin:24px 0"><h3>Package accounts awaiting delivery</h3><p>Approved package orders keep their purchased rules while waiting for matching verified stock.</p><div id="np-package-deliveries"></div>`;
    refreshPackagePreview();
    npLoadPackageDeliveries(1);
  }catch(err){node.innerHTML=`<h3>Production settings unavailable</h3><p>${e(err.message)}</p><button class="btn" onclick="npLoadProductionSettings()">Retry</button>`;}
};
window.npSaveProductionControls=async function(button){
  button.disabled=true;message('np-control-message','Saving…');
  try{const flags={};for(const r of controls.facilities)flags[r.key]=document.getElementById('np-control-'+r.key).checked;
    const result=await api('/admin/production_controls','POST',{revision:controls.data.revision,features:flags});controls=result;message('np-control-message','Saved and confirmed.');
  }catch(err){message('np-control-message',err.message);}finally{button.disabled=false;}
};
window.npSaveProductionPlan=async function(i,button){
  button.disabled=true;message('np-plan-message-'+i,'Saving…');
  try{const row=catalog[i],changes={},numeric=['fee','account_size','phase1_target','phase2_target','max_drawdown','payout_split','reset_fee'];
    for(const key of ['name',...numeric,'challenge_journey']){const n=document.getElementById(`np-plan-${i}-${key}`);const baseline=key==='challenge_journey'?journey(row):String(row[key]??'');if(n.value===baseline)continue;if(numeric.includes(key)){if(!n.value.trim())throw Error('Set '+key);const value=Number(n.value);if(!Number.isFinite(value))throw Error('Enter a valid '+key);if(baseline!==''&&Number(baseline)===value)continue;changes[key]=value;}else{changes[key]=n.value;}}
    const status=document.getElementById('np-plan-active-'+i).checked?'active':'inactive';if(status!==row.status)changes.status=status;
    if(!Object.keys(changes).length){message('np-plan-message-'+i,'No changes to save.');return;}
    const result=await api('/admin/production_catalog','POST',{id:row.id,updated_at:row.updated_at,changes});catalog[i]=result.plan;message('np-plan-message-'+i,'Saved. Existing purchase rules were not rewritten.');
  }catch(err){message('np-plan-message-'+i,err.message);}finally{button.disabled=false;}
};
window.npCloneProductionPlan=async function(i,button){
  const name=prompt('Name for the new inactive product version',catalog[i].name+' — new version');if(!name)return;
  button.disabled=true;
  try{await api('/admin/production_catalog/clone','POST',{id:catalog[i].id,name});await npLoadProductionSettings();}
  catch(err){message('np-plan-message-'+i,err.message);}finally{button.disabled=false;}
};
window.npAddPackageOption=function(key){
  const value=prompt('New '+(packages.labels[key]||key)+' choice');if(!value)return;
  const rows=packages.data.options[key];if(rows.length>=30){message('np-package-message','Each facility supports up to 30 choices.');return;}
  const i=rows.length,row={value:value.trim(),enabled:false,price:null};rows.push(row);
  document.getElementById('np-package-rows-'+key).insertAdjacentHTML('beforeend',choiceRow(key,row,i));
};
function refreshPackagePreview(){
  const missing=[];
  for(const [key,rows] of Object.entries(packages.data.options)){
    const select=document.getElementById('np-package-preview-'+key);if(!select)continue;
    const previous=select.value,active=rows.filter(r=>r.enabled);
    select.innerHTML=active.length?active.map(r=>`<option value="${e(r.value)}">${e(r.value)}</option>`).join(''):'<option value="">No active choices — enable a benefit above</option>';
    select.disabled=!active.length;
    if(active.some(r=>r.value===previous))select.value=previous;
    if(!active.length&&!(key==='phase2_target'&&!packages.data.options.phases.some(r=>r.enabled&&r.value==='two')))missing.push(packages.labels[key]||key);
  }
  message('np-package-quote',missing.length?'To calculate a package, enable and save at least one choice for: '+missing.join(', ')+'.':'Choose your saved benefits, then calculate the price.');
}
window.npSavePackageConfiguration=async function(button){
  button.disabled=true;message('np-package-message','Saving…');
  try{const data=JSON.parse(JSON.stringify(packages.data));
    for(const [key,rows] of Object.entries(data.options))for(let i=0;i<rows.length;i++){rows[i].value=document.getElementById(`np-package-${key}-${i}-value`).value.trim();rows[i].enabled=document.getElementById(`np-package-${key}-${i}-enabled`).checked;const value=document.getElementById(`np-package-${key}-${i}-price`).value;rows[i].price=value===''?null:Number(value);if(key!=='capital')rows[i].price_type=document.getElementById(`np-package-${key}-${i}-price-type`).value;}
    const minimum=document.getElementById('np-package-minimum').value;
    const result=await api('/admin/package_configuration','POST',{revision:data.revision,enabled:document.getElementById('np-package-release').checked,minimum_price:minimum===''?null:Number(minimum),options:data.options});packages=result;refreshPackagePreview();message('np-package-message','Saved and confirmed. Price-check choices are updated below.');
  }catch(err){message('np-package-message',err.message);}finally{button.disabled=false;}
};
window.npPreviewPackageQuote=async function(button){
  button.disabled=true;message('np-package-quote','Calculating…');
  try{const choices={};for(const key of Object.keys(packages.data.options))choices[key]=document.getElementById('np-package-preview-'+key).value;if(choices.phases==='one')choices.phase2_target=null;
    const r=await api('/admin/package_configuration/quote','POST',choices);message('np-package-quote','Saved catalog version '+r.data.catalog_revision+': ₦'+r.data.total.toLocaleString()+'. '+(packages.data.enabled?'Sales are open.':'Sales remain closed.'));
  }catch(err){message('np-package-quote',err.message);}finally{button.disabled=false;}
};
let deliverySequence=0,deliveryPage=1;
window.npLoadPackageDeliveries=async function(page=1){
  const node=document.getElementById('np-package-deliveries');if(!node)return;
  const sequence=++deliverySequence;node.textContent='Loading package deliveries…';
  try{
    const result=await api('/admin/package_fulfillment?page='+page);
    if(sequence!==deliverySequence||!document.getElementById('np-package-deliveries'))return;
    if(!Array.isArray(result.entitlements))throw Error('Package delivery response is invalid');
    deliveryPage=page;
    node.innerHTML=result.entitlements.length?result.entitlements.map(r=>`<div class="card" style="margin:10px 0;overflow-wrap:anywhere"><p>Contract ${e(r.contract_id)}</p><p>${e(r.kind)} → ${e(r.target_stage)} · Waiting for matching verified stock</p><button class="btn" data-entitlement="${e(r.id)}" onclick="npRetryPackageDelivery(this)">Retry package delivery</button><p role="status" class="np-delivery-result"></p></div>`).join(''):'<p>No package accounts are awaiting delivery on this page.</p>';
    node.insertAdjacentHTML('beforeend',`<div style="display:flex;gap:12px;align-items:center"><button class="btn" ${page===1?'disabled':''} onclick="npLoadPackageDeliveries(${page-1})">Previous</button><span>Page ${e(page)}</span><button class="btn" ${result.has_more?'':'disabled'} onclick="npLoadPackageDeliveries(${page+1})">Next</button></div>`);
  }catch(err){if(sequence===deliverySequence)node.innerHTML=`<p>${e(err.message)}</p><button class="btn" onclick="npLoadPackageDeliveries(${page})">Reload package deliveries</button>`;}
};
window.npRetryPackageDelivery=async function(button){
  const id=button.dataset.entitlement,node=button.parentElement.querySelector('.np-delivery-result');
  if(!/^[a-f0-9-]{36}$/i.test(id)){node.textContent='Invalid delivery identity. Reload the list.';return;}
  button.disabled=true;node.textContent='Checking the same delivery entitlement…';
  try{
    const result=await api('/admin/package_fulfillment/retry','POST',{entitlement_id:id});
    if(result.data?.waiting){node.textContent='Still waiting for matching verified stock. No account was assigned.';}
    else if(result.data?.account_id){node.textContent='Account delivery is recorded. Refresh the trader account list.';button.textContent='Delivery recorded';return;}
    else throw Error('Delivery was not confirmed. Reload before retrying.');
  }catch(err){node.textContent=err.message;}finally{if(button.textContent!=='Delivery recorded')button.disabled=false;}
};
// Package approval must bypass the old browser-side MT5 picker. The server
// selects verified stock from the immutable package terms, not a displayed plan.
const packageApprovalBusy=new Set();
const oldApproveAndAssign=window.approveAndAssign,oldApprovePurchase=window.approvePurchase;
const packagePurchase=id=>(typeof SESSION!=='undefined'?(SESSION.purchases||[]):[]).find(p=>String(p.id)===String(id)&&p.package_contract_id);
window.npApprovePackagePurchase=async function(id){
  if(packageApprovalBusy.has(id))return;
  packageApprovalBusy.add(id);
  try{
    const result=await api('/approve_challenge_purchase','POST',{id});
    if(result.data?.waiting){toast('Payment approved. Waiting for matching verified package stock.','warn');}
    else if(result.data?.account_id){toast('Package account delivery is recorded.','ok');}
    else throw Error('Package delivery was not confirmed. Refresh this purchase before retrying.');
    document.querySelector('.modal-bg')?.remove();
    if(typeof npRefreshPurchaseOperationsData==='function')await npRefreshPurchaseOperationsData(true);
  }catch(err){toast(err.message,'err');}
  finally{packageApprovalBusy.delete(id);}
};
if(typeof oldApproveAndAssign==='function')window.approveAndAssign=function(id,...args){
  if(packagePurchase(id))return npApprovePackagePurchase(id);
  return oldApproveAndAssign.call(this,id,...args);
};
if(typeof oldApprovePurchase==='function')window.approvePurchase=function(id,...args){
  const purchase=packagePurchase(id);
  if(!purchase)return oldApprovePurchase.call(this,id,...args);
  if(!/^[a-f0-9-]{36}$/i.test(String(id)))return toast('Invalid purchase identity','err');
  npShowResetModalSafe(`<h2>Approve package payment</h2><p>${e(purchase.plan_name||'Build Your Package')} · ₦${e(purchase.fee)}</p><p>Approval uses the rules saved with this purchase. Delivery waits if matching verified stock is unavailable.</p><button class="btn btn-gold" onclick="npApprovePackagePurchase('${id}')">Approve package payment</button><button class="btn" onclick="this.closest('.modal-bg').remove()">Cancel</button>`);
};
})();
