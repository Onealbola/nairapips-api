const {chromium}=require('playwright');
const fs=require('fs'),assert=require('assert');
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 try{
 const page=await browser.newPage({viewport:{width:1280,height:960}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.setContent('<div id="np-production-settings"></div>');
 await page.evaluate(()=>{
  window.esc=x=>String(x).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  window.savedCalls=[];window.SESSION={purchases:[{id:'22222222-2222-4222-8222-222222222222',package_contract_id:'fixture-contract'}]};window.toasts=[];window.toast=(message)=>toasts.push(message);window.approveAndAssign=()=>{throw Error('Legacy picker must not run for a package')};
  const plan={id:'old',updated_at:'v1',name:'Old product <safe>',status:'inactive',account_size:500000,fee:20000,phase1_target:10,phase2_target:0,max_drawdown:20,payout_split:60,reset_fee:null,challenge_journey:'phase1,funded',drawdown_type:'STATIC'};
  const controls={success:true,data:{revision:0,features:{payout_requests:true}},facilities:[{key:'payout_requests',label:'New payout requests'}],protected:['DD police'],propagation_seconds:3};
  const options={capital:[{value:'500000',enabled:false,price:null}],phases:[{value:'one',enabled:false,price:null}],phase2_target:[{value:'8',enabled:false,price:null}]};
  const packages={success:true,data:{revision:0,enabled:false,minimum_price:null,options},labels:{capital:'Account size',phases:'Phases',phase2_target:'Phase 2'},price_rule:'Server tariff',release_blockers:['Verified monitoring']};
  window.api=async(path,method,body)=>{
    if(method==='POST'){
      savedCalls.push({path,body});
      if(path==='/approve_challenge_purchase')return {success:true,data:{waiting:true}};
      if(path==='/admin/production_controls'){controls.data={...body,revision:body.revision+1};return structuredClone(controls);}
      if(path==='/admin/production_catalog'){Object.assign(plan,body.changes,{updated_at:'v2'});return {success:true,plan:structuredClone(plan)};}
      if(path==='/admin/premium_cycle_prices')return {success:true,data:{...body,revision:body.revision+1}};
      if(path==='/admin/standard_cycle_prices')return {success:true,data:{...body,revision:body.revision+1}};
      if(path==='/admin/package_configuration'){packages.data={...body,revision:body.revision+1};return structuredClone(packages);}
      if(path==='/admin/package_fulfillment/retry')return {success:true,data:{entitlement_id:body.entitlement_id,waiting:true}};
      if(path.endsWith('/quote'))return {success:true,data:{total:25000,catalog_revision:1}};
    }
    if(path.startsWith('/admin/package_fulfillment?page='))return {success:true,entitlements:[{id:'11111111-1111-4111-8111-111111111111',contract_id:'fixture-contract',kind:'first_account',target_stage:'phase1'}],has_more:false};
    if(path==='/admin/production_controls')return structuredClone(controls);
    if(path==='/admin/production_catalog')return {success:true,plans:[structuredClone(plan),{...structuredClone(plan),id:'premium',name:'Premium fixture',product_category:'premium',payout_cycles:5}]};
    if(path==='/admin/package_configuration')return structuredClone(packages);
    if(path==='/admin/premium_cycle_prices')return {success:true,data:{revision:0,prices:{'3':18000,'4':28000,'5':18000,'0':58000}}};
    if(path==='/admin/standard_cycle_prices')return {success:true,data:{revision:0,prices:{'3':5000,'4':10000,'5':15000,'0':25000}}};
    throw Error('Unexpected request '+path);
  };
 });
 await page.addScriptTag({content:fs.readFileSync(__dirname+'/../website-downloads/production-settings.js','utf8')});
 await page.evaluate(()=>npLoadProductionSettings());
 await page.locator('details').evaluateAll(nodes=>nodes.forEach(n=>n.open=true));
 assert.equal(await page.locator('h4').first().textContent(),'Old product <safe> · 500000');
 assert.equal(await page.locator('#np-plan-active-0').isChecked(),false);
 assert.equal(await page.locator('#np-plan-0-reset-fee-mode').inputValue(),'none');
 assert.equal(await page.locator('#np-plan-0-funded-reset-fee-mode').inputValue(),'auto');
 await page.locator('#np-standard-cycle-4').fill('11000');
 await page.locator('#np-standard-cycle-5').fill('6000');
 await page.getByRole('button',{name:'Save cycle extra costs',exact:true}).click();
 await page.waitForFunction(()=>document.getElementById('np-cycle-price-message').textContent.startsWith('Saved.'));
 assert.deepEqual((await page.evaluate(()=>savedCalls.pop())).body.prices,{'3':5000,'4':11000,'5':6000,'0':25000});
 assert.equal(await page.locator('#np-premium-cycle-5').inputValue(),'18000');
 assert((await page.locator('#np-plan-1-price-preview').innerText()).includes('38,000'));
 await page.getByRole('button',{name:'Premium plans (1)',exact:true}).click();
 assert.equal(await page.locator('[data-production-market="premium"]:visible').count(),1);
 assert.equal(await page.locator('[data-production-market="standard"]:visible').count(),0);
 await page.getByRole('button',{name:'All plans',exact:true}).click();
 await page.locator('#np-premium-cycle-5').fill('19000');
 await page.getByRole('button',{name:'Save Premium cycle extra costs',exact:true}).click();
 await page.waitForFunction(()=>document.getElementById('np-premium-cycle-price-message').textContent.startsWith('Saved.'));
 const premiumSave=await page.evaluate(()=>savedCalls.pop());assert.equal(premiumSave.path,'/admin/premium_cycle_prices');assert.equal(premiumSave.body.prices['5'],19000);
 assert((await page.locator('#np-plan-1-price-preview').innerText()).includes('39,000'));
 await page.locator('#np-plan-active-0').check();
 await page.getByRole('button',{name:'Save product',exact:true}).first().click();
 await page.waitForFunction(()=>document.getElementById('np-plan-message-0').textContent.startsWith('Saved.'));
 let calls=await page.evaluate(()=>savedCalls);assert.deepEqual(calls[0].body.changes,{status:'active'});
 await page.locator('#np-control-payout_requests').uncheck();
 await page.getByRole('button',{name:'Save service switches',exact:true}).click();
 await page.waitForFunction(()=>document.getElementById('np-control-message').textContent==='Saved and confirmed.');
 assert.equal((await page.evaluate(()=>savedCalls.at(-1))).body.features.payout_requests,false);
 await page.locator('#np-package-minimum').fill('5000');
 await page.locator('#np-package-capital-0-price').fill('25000');
 await page.locator('#np-package-capital-0-enabled').check();
 page.once('dialog',dialog=>dialog.accept('3000000'));
 await page.locator('#np-package-rows-capital').locator('..').getByRole('button',{name:'Add choice'}).click();
 await page.locator('#np-package-capital-1-price').fill('120000');
 await page.locator('#np-package-capital-1-enabled').check();
 await page.getByRole('button',{name:'Save prices and benefits',exact:true}).click();
 await page.waitForFunction(()=>document.getElementById('np-package-message').textContent.startsWith('Saved and confirmed.'));
 calls=await page.evaluate(()=>savedCalls);const saved=calls.at(-1).body;
 assert.equal(await page.locator('#np-package-preview-capital option').count(),2);
 assert.equal(await page.locator('#np-package-preview-phases').isDisabled(),true);
 assert((await page.locator('#np-package-preview-phases').textContent()).includes('No active choices'));
 assert.equal(saved.enabled,false);assert.equal(saved.options.capital[0].price,25000);assert.equal(saved.options.capital[0].enabled,true);assert.equal(saved.options.capital[1].value,'3000000');assert.equal(saved.options.capital[1].price,120000);
 await page.getByRole('button',{name:'Retry package delivery',exact:true}).click();
 await page.waitForFunction(()=>document.querySelector('.np-delivery-result').textContent.startsWith('Still waiting'));
 assert.deepEqual((await page.evaluate(()=>savedCalls.at(-1))).body,{entitlement_id:'11111111-1111-4111-8111-111111111111'});
 await page.evaluate(()=>approveAndAssign('22222222-2222-4222-8222-222222222222'));
 assert.deepEqual((await page.evaluate(()=>savedCalls.at(-1))).body,{id:'22222222-2222-4222-8222-222222222222'});
 assert((await page.evaluate(()=>toasts.at(-1))).includes('Waiting for matching verified'));
 await page.locator('#np-plan-0-funded-reset-fee-mode').selectOption('none');
 await page.locator('#np-plan-0-reset-fee-mode').selectOption('custom');
 await page.locator('#np-plan-0-reset-fee').fill('7500');
 await page.getByRole('button',{name:'Save product',exact:true}).first().click();
 await page.waitForFunction(()=>savedCalls.at(-1)?.body?.changes?.funded_reset_fee===0);
 assert.equal((await page.evaluate(()=>savedCalls.at(-1))).body.changes.reset_fee,7500);
 await page.evaluate(()=>{const base=window.api;window.goodSettingsApi=base;window.api=(path,...args)=>path==='/admin/package_configuration'?Promise.reject(Error('Package service unavailable')):base(path,...args)});
 await page.evaluate(()=>npLoadProductionSettings());
 assert.equal(await page.locator('#np-standard-cycle-5').count(),1);assert.equal(await page.locator('#np-premium-cycle-5').count(),1);
 await page.evaluate(()=>{window.api=window.goodSettingsApi});await page.evaluate(()=>npLoadProductionSettings());await page.locator('details').evaluateAll(nodes=>nodes.forEach(n=>n.open=true));
 await page.evaluate(()=>{window.api=async()=>{throw Error('Network unavailable')}});
 await page.getByRole('button',{name:'Save service switches',exact:true}).click();
 await page.waitForFunction(()=>document.getElementById('np-control-message').textContent==='Network unavailable');
 assert.equal(await page.getByRole('button',{name:'Save service switches',exact:true}).isDisabled(),false);
 await page.setViewportSize({width:390,height:844});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false,'Settings should fit mobile');assert.deepEqual(errors,[]);
 console.log('Browser passed: inactive product retained/reopened, only changed fields sent, switches persist, real price payload, escaped names, failed save remains retryable.');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
