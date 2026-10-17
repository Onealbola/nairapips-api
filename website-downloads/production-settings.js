/* Owner controls: real API persistence; no writes to issued account rules. */
(function(){
'use strict';
let controls=null, catalog=[], packages=null;
const e=x=>esc(String(x??''));
const journey=r=>String(r.challenge_journey||'').includes('two')||String(r.challenge_journey||'').includes('phase2')?'two_phase':'one_phase';
const message=(id,text)=>{const n=document.getElementById(id);if(n)n.textContent=text;};
const input=(id,value,extra='')=>`<input class="input" id="${id}" value="${e(value)}" ${extra}>`;
const choiceRow=(key,r,i)=>`<label style="display:flex;flex-wrap:wrap;gap:12px;align-items:center;margin:8px 0"><input type="checkbox" id="np-package-${key}-${i}-enabled" ${r.enabled?'checked':''}>${input(`np-package-${key}-${i}-value`,r.value,'aria-label="Option value"')}${input(`np-package-${key}-${i}-price`,r.price??'','type="number" min="0" step="0.01" aria-label="Option price"')}<span>${key==='capital'?'₦ base price':'% addition to base'}</span></label>`;
window.npLoadProductionSettings=async function(){
  const node=document.getElementById('np-production-settings');if(!node)return;
  node.innerHTML='<h3>Production controls</h3><p>Loading saved settings…</p>';
  try{
    const results=await Promise.all([api('/admin/production_controls'),api('/admin/production_catalog'),api('/admin/package_configuration')]);
    if(!document.getElementById('np-production-settings'))return;
    const [c,p,f]=results;controls=c;catalog=p.plans;packages=f;
    node.innerHTML=`<h3>Service release controls</h3><p>Tick to accept new requests. Unticking keeps history, issued account protection and existing settlement obligations.</p>
      <div>${c.facilities.map(r=>`<label style="display:block;margin:12px 0"><input type="checkbox" id="np-control-${e(r.key)}" ${c.data.features[r.key]?'checked':''}> ${e(r.label)}</label>`).join('')}</div>
      <button class="btn btn-gold" onclick="npSaveProductionControls(this)">Save service switches</button>
      <p id="np-control-message" role="status"></p>
      <p>Saved revision ${e(c.data.revision)} · Changes reach the server within ${e(c.propagation_seconds)} seconds.</p>
      <p>Always protected: ${c.protected.map(e).join(' · ')}.</p>
      <hr style="margin:24px 0"><h3>Products: available now or held for later</h3>
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
      <hr style="margin:24px 0"><h3>New package product settings</h3>
      <p>These settings belong to the new product only. Prices start empty and choices start off until you configure them. Saving does not change any old product or issued account.</p>
      <p>${e(f.price_rule)}</p>
      <label>Minimum purchase price ₦ ${input('np-package-minimum',f.data.minimum_price??'','type="number" min="0.01" step="0.01"')}</label>
      ${Object.entries(f.data.options).map(([key,rows])=>`<div class="card" style="margin:12px 0"><h4>${e(f.labels[key])}</h4><div id="np-package-rows-${key}">${rows.map((r,i)=>choiceRow(key,r,i)).join('')}</div>${['phases','drawdown_type'].includes(key)?'':`<button class="btn" onclick="npAddPackageOption('${key}')">Add choice</button>`}</div>`).join('')}
      <label><input type="checkbox" disabled ${f.data.enabled?'checked':''}> Release new package checkout</label>
      <p>Release is locked until: ${f.release_blockers.map(e).join('; ')}. Saved prices and choices are real configuration; customers cannot purchase an unenforced contract.</p>
      <button class="btn btn-gold" onclick="npSavePackageConfiguration(this)">Save new package settings</button>
      <p id="np-package-message" role="status"></p>
      <h4>Check a package price using saved settings</h4>
      <div class="grid md:grid-cols-3 gap-2">${Object.entries(f.data.options).map(([key,rows])=>`<label>${e(f.labels[key])}<select class="input" id="np-package-preview-${key}">${rows.filter(r=>r.enabled).map(r=>`<option value="${e(r.value)}">${e(r.value)}</option>`).join('')}</select></label>`).join('')}</div>
      <button class="btn" onclick="npPreviewPackageQuote(this)">Calculate saved price</button>
      <p id="np-package-quote" role="status"></p>`;
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
window.npSavePackageConfiguration=async function(button){
  button.disabled=true;message('np-package-message','Saving…');
  try{const data=JSON.parse(JSON.stringify(packages.data));
    for(const [key,rows] of Object.entries(data.options))for(let i=0;i<rows.length;i++){rows[i].value=document.getElementById(`np-package-${key}-${i}-value`).value.trim();rows[i].enabled=document.getElementById(`np-package-${key}-${i}-enabled`).checked;const value=document.getElementById(`np-package-${key}-${i}-price`).value;rows[i].price=value===''?null:Number(value);}
    const minimum=document.getElementById('np-package-minimum').value;
    const result=await api('/admin/package_configuration','POST',{revision:data.revision,enabled:false,minimum_price:minimum===''?null:Number(minimum),options:data.options});packages=result;message('np-package-message','Saved and confirmed. Reload settings to refresh the price-check choices.');
  }catch(err){message('np-package-message',err.message);}finally{button.disabled=false;}
};
window.npPreviewPackageQuote=async function(button){
  button.disabled=true;message('np-package-quote','Calculating…');
  try{const choices={};for(const key of Object.keys(packages.data.options))choices[key]=document.getElementById('np-package-preview-'+key).value;if(choices.phases==='one')choices.phase2_target=null;
    const r=await api('/admin/package_configuration/quote','POST',choices);message('np-package-quote','Saved catalog version '+r.data.catalog_revision+': ₦'+r.data.total.toLocaleString()+'. Checkout remains unreleased.');
  }catch(err){message('np-package-quote',err.message);}finally{button.disabled=false;}
};
})();
