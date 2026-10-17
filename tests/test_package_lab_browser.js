// Run with the installed Playwright environment; fixture never touches production.
const {chromium}=require('playwright');
const {spawn}=require('child_process');
const path=require('path'),assert=require('assert');
(async()=>{
 const server=spawn('/workspace/.venvs/nairapips-api/bin/python',[path.join(__dirname,'package_lab_fixture.py')],{stdio:['ignore','pipe','pipe']});
 let browser;
 try{
  const port=await new Promise((resolve,reject)=>{
   const timer=setTimeout(()=>reject(Error('Local fixture did not start')),10000);
   server.stdout.on('data',chunk=>{const match=String(chunk).match(/READY:(\d+)/);if(match){clearTimeout(timer);resolve(match[1]);}});
   server.on('exit',()=>{clearTimeout(timer);reject(Error('Local fixture exited'));});
  });
  browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
  const page=await browser.newPage({viewport:{width:1280,height:960}}),errors=[];
  page.on('pageerror',err=>errors.push(err.message));
  await page.goto('http://127.0.0.1:'+port+'/package-lab');
  await page.locator('#username').fill('fixture-owner');await page.locator('#password').fill('fixture-password');
  await page.getByRole('button',{name:'Sign in',exact:true}).click();
  await page.waitForFunction(()=>!document.getElementById('work').hidden);
  assert.equal(await page.locator('#password').inputValue(),'');
  await page.locator('#sample').click();
  await page.waitForFunction(()=>!document.getElementById('results').hidden);
  assert.equal(await page.locator('#save').isDisabled(),true,'Example tariff cannot be saved');
  assert.equal(await page.locator('#live-price').textContent(),'₦25,000');
  await page.locator('[data-choice="phase_resets"]').selectOption('1');
  await page.waitForFunction(()=>liveQuote?.quote.selection.phase_resets==='1');
  assert.equal(await page.locator('#live-price').textContent(),'₦26,500','Adding a benefit must update the server price without starting a test');
  assert.equal(await page.locator('#results').isVisible(),false,'Changing choices must not silently restart a journey');
  let releaseOld,markOldRequested;
  const oldRequested=new Promise(resolve=>markOldRequested=resolve);
  await page.route('**/admin/package_lab/rehearse',async route=>{
   const body=route.request().postDataJSON();
   if(body.actions.length===0&&body.choices.phase_resets==='2'){
    const response=await route.fetch();markOldRequested();
    await new Promise(resolve=>releaseOld=resolve);await route.fulfill({response});
   }else await route.continue();
  });
  await page.locator('[data-choice="phase_resets"]').selectOption('2');await oldRequested;
  await page.locator('[data-choice="phase_resets"]').selectOption('0');
  await page.waitForFunction(()=>liveQuote?.quote.selection.phase_resets==='0');
  const oldResponse=page.waitForResponse(response=>{try{return response.url().includes('/admin/package_lab/rehearse')&&response.request().postDataJSON().choices.phase_resets==='2';}catch{return false;}});
  releaseOld();await oldResponse;await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
  assert.equal(await page.locator('#live-price').textContent(),'₦25,000','Delayed older quotes must not replace the current price');
  await page.unroute('**/admin/package_lab/rehearse');
  await page.locator('[data-preset="starter"]').click();
  await page.waitForFunction(()=>liveQuote?.quote.selection.capital==='100000');
  assert.equal(await page.locator('#live-price').textContent(),'₦5,000');
  await page.locator('[data-preset="flexible"]').click();
  await page.waitForFunction(()=>liveQuote?.quote.selection.capital==='250000');
  await page.screenshot({path:'/workspace/scratch/package-card-desktop.png',fullPage:true});
  await page.setViewportSize({width:390,height:844});
  assert.equal(await page.locator('#mobile-price').isVisible(),true);
  assert.equal(await page.locator('#mobile-price').textContent(),'₦25,000');
  await page.screenshot({path:'/workspace/scratch/package-card-mobile-preview.png',fullPage:true});
  await page.setViewportSize({width:1280,height:960});
  const selections={capital:'500000',drawdown:'10',phase1_target:'10',profit_share:'70',payout_cycles:'3',phase_resets:'1',funded_resets:'2'};
  for(const [key,value] of Object.entries(selections))await page.locator('[data-choice="'+key+'"]').selectOption(value);
  await page.locator('#start').click();await page.waitForFunction(()=>!document.getElementById('results').hidden);
  await page.locator('#pass').click();await page.waitForFunction(()=>result?.state.stage==='funded');
  await page.locator('#payout').click();await page.waitForFunction(()=>result?.usage.paid_cycles===1);
  await page.locator('#payout').click();await page.waitForFunction(()=>result?.usage.paid_cycles===2);
  assert.equal(await page.evaluate(()=>result.total_test_payout),'35000.00');
  assert.equal(await page.evaluate(()=>result.usage.funded_resets),0);
  for(let i=1;i<=2;i++){
   await page.locator('#breach').click();await page.waitForFunction(()=>result.state.decision==='breached');
   await page.locator('#reset').click();await page.waitForFunction(n=>result.usage.funded_resets===n,i);
  }
  await page.locator('#breach').click();await page.waitForFunction(()=>result.state.decision==='breached');
  await page.locator('#reset').click();await page.waitForFunction(()=>document.getElementById('message').textContent.includes('No included funded resets'));
  let writes=await (await page.request.get('http://127.0.0.1:'+port+'/fixture-writes')).json();
  assert.deepEqual(writes.tables,[],'Rehearsals must not write to any table');
  await page.locator('#load').click();await page.waitForFunction(()=>document.getElementById('mode').textContent.startsWith('Saved tariff'));
  await page.locator('#show-owner').check();
  await page.locator('.owner-pricing > summary').click();
  await page.locator('.choice details').evaluateAll(nodes=>nodes.forEach(node=>node.open=true));
  await page.locator('#minimum').fill('5000');
  for(const key of await page.locator('[data-price]').evaluateAll(nodes=>nodes.map(n=>n.dataset.price))){
   if(key==='phase2_target')continue;
   await page.locator('[data-price="'+key+'"]').fill(key==='capital'?'25000':'0');
   await page.locator('[data-enabled="'+key+'"]').check();
  }
  await page.locator('#save').click();await page.waitForFunction(()=>document.getElementById('message').textContent.startsWith('Prices and choices saved'));
  writes=await (await page.request.get('http://127.0.0.1:'+port+'/fixture-writes')).json();
  assert(writes.tables.every(name=>name==='business_settings'));
  assert.equal(JSON.parse(writes.config_rows[0].value).enabled,false,'Saving tariff must not launch the product');
  await page.locator('#start').click();await page.waitForFunction(()=>!document.getElementById('results').hidden);
  assert.equal(await page.evaluate(()=>result.quote.total),25000);
  await page.setViewportSize({width:390,height:844});
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false,'Mobile layout overflows');
  await page.screenshot({path:'/workspace/scratch/package-card-mobile.png',fullPage:true});
  assert.deepEqual(errors,[]);
  await page.setViewportSize({width:1280,height:960});
  await page.locator('#option-editors details').evaluateAll(nodes=>nodes.forEach(n=>n.open=true));
  let field=page.locator('[data-option-key="capital"][data-option-index="0"][data-option-field="value"]');
  await field.fill('125000');await field.dispatchEvent('change');
  await page.locator('#option-editors details').evaluateAll(nodes=>nodes.forEach(n=>n.open=true));
  field=page.locator('[data-option-key="capital"][data-option-index="0"][data-option-field="price"]');
  await field.fill('7000');await field.dispatchEvent('change');
  await page.locator('#option-editors details').evaluateAll(nodes=>nodes.forEach(n=>n.open=true));
  await page.locator('[data-option-key="capital"][data-option-index="0"][data-option-field="enabled"]').check();
  await page.locator('#save').click();await page.waitForFunction(()=>saved.options.capital[0].value==='125000');
  await page.locator('#load').click();await page.waitForFunction(()=>config.options.capital[0].price===7000&&document.getElementById('message').textContent.startsWith('Saved settings loaded'));
  assert.equal(await page.evaluate(()=>config.options.capital[0].enabled),true);
  await page.locator('#option-editors details').evaluateAll(nodes=>nodes.forEach(n=>n.open=true));
  await page.locator('[data-option-key="capital"][data-option-index="0"][data-option-field="enabled"]').uncheck();
  await page.locator('#save').click();await page.waitForFunction(()=>saved.options.capital[0].enabled===false);
  await page.locator('#load').click();await page.waitForFunction(()=>config.options.capital[0].enabled===false);
  assert.equal(await page.evaluate(()=>config.options.capital[0].value),'125000','Inactive choices must remain saved');
  await page.locator('#logout').click();assert.equal(await page.locator('#login').isVisible(),true);
  assert.equal(await page.evaluate(()=>localStorage.length+sessionStorage.length),0,'No credentials should persist');
  console.log('Owner lab browser passed: live benefit prices, stale quote rejection, card presets, real rule evaluation, payouts, resets, saved inactive tariff, mobile and logout.');
 }finally{if(browser)await browser.close();server.kill('SIGTERM');}
})().catch(err=>{console.error(err);process.exitCode=1;});
