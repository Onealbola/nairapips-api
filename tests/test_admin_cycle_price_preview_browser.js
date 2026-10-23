const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 try{for(const file of ['admin_clean.html','admin_clean_cpanel.html']){
  const page=await browser.newPage();await page.setContent('<main id="content"></main><div id="modal"></div>');
  await page.evaluate(()=>{
   window.esc=x=>String(x??'');window.pct=x=>x+'%';window.money=n=>new Intl.NumberFormat('en-NG',{style:'currency',currency:'NGN',maximumFractionDigits:0}).format(n);
   window.npShowResetModalSafe=html=>document.getElementById('modal').innerHTML=html;window.toast=()=>{};window.npPlanRouteChanged=()=>{};
   window.SESSION={plans:[{id:'s',name:'Standard fixture',product_category:'standard',fee:20000,payout_cycles:null,status:'active',funded_reset_fee:null},{id:'p',name:'Premium fixture',product_category:'premium',fee:40000,payout_cycles:5,status:'active',funded_reset_fee:null}]};
   window.api=async path=>({success:true,data:{revision:1,prices:{'3':path.includes('premium')?18000:5000,'4':24000,'5':path.includes('premium')?18000:5000,'0':54000}}});
  });
  const html=fs.readFileSync(__dirname+'/../website-downloads/'+file,'utf8');
  const a=html.search(/window\.npResetControl\s*=/),b=html.indexOf('window.npPlanRouteChanged',a);await page.addScriptTag({content:html.slice(a,b)});
  const c=html.indexOf('window.editPlan'),d=html.indexOf('window.savePlan',c);await page.addScriptTag({content:html.slice(c,d)});
  const x=html.search(/function renderPlans\(\)\s*\{/);await page.addScriptTag({content:html.slice(x,a)});
  await page.evaluate(()=>renderPlans());
  await page.waitForFunction(()=>document.querySelector('[data-plan-price-id="p"]').textContent.includes('58,000'));
  assert((await page.locator('[data-plan-price-id="s"]').innerText()).includes('20,000'));
  await page.evaluate(()=>editPlan('s'));await page.waitForFunction(()=>document.getElementById('epl-cycle-price-preview').textContent.includes('4 cycles included'));
  assert.equal(await page.locator('#epl-cycles').inputValue(),'');
  await page.evaluate(()=>editPlan('p'));await page.waitForFunction(()=>document.getElementById('epl-cycle-price-preview').textContent.includes('58,000'));
  assert((await page.locator('#epl-cycle-price-preview').innerText()).includes('18,000'));
  await page.locator('#epl-fee').fill('50000');assert((await page.locator('#epl-cycle-price-preview').innerText()).includes('68,000'));
  assert.equal(await page.locator('#epl-funded-reset-fee').inputValue(),'100000');
  await page.locator('#epl-category').selectOption('standard');await page.waitForFunction(()=>document.getElementById('epl-cycle-price-preview').textContent.includes('55,000'));
  await page.close();
 }console.log('PASS: full and compact Plans lists/editors show saved market extras, recalculate totals, preserve base reset pricing and keep legacy defaults at four.');}
 finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1)});
