const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 try{
  const page=await browser.newPage();await page.setContent('<main id="modal"></main>');
  await page.evaluate(()=>{
   window.npShowResetModalSafe=html=>document.getElementById('modal').innerHTML=html;
   window.npSyncFundedResetDoubleV152=()=>{};window.npPlanRouteChanged=()=>{};
   window.esc=x=>String(x??'');window.toast=()=>{};window.audit=async()=>{};window.reload=()=>{};
   window.calls=[];window.api=async(path,method,body)=>{calls.push({path,body});return {success:true}};
  });
  const html=fs.readFileSync(__dirname+'/../website-downloads/'+(process.env.NP_PANEL_FILE||'admin_clean.html'),'utf8');
  await page.addScriptTag({content:html.slice(html.search(/window\.npResetControl\s*=/),html.indexOf('window.npPlanRouteChanged'))});
  const start=html.search(/window\.showCreatePlan\s*=/),end=html.indexOf('window.editPlan',start);
  await page.addScriptTag({content:html.slice(start,end)});
  for(const value of ['1','2','3','4','5','0']){
   await page.evaluate(()=>showCreatePlan());
   assert.equal(await page.locator('#pl-cycles').inputValue(),'3');
   assert.equal(await page.locator('#pl-cycles option').count(),6);
   await page.locator('#pl-cycles').selectOption(value);await page.locator('#pl-name').fill('Cycle fixture');
   await page.evaluate(()=>createPlan());
   const last=await page.evaluate(()=>calls.at(-1));assert.equal(last.path,'/create_challenge_plan');assert.equal(last.body.payout_cycles,Number(value));assert.equal(last.body.status,'inactive');assert.equal(last.body.reset_fee,0);assert.equal(last.body.funded_reset_fee,null);
  }
  await page.evaluate(()=>showCreatePlan());
  await page.locator('#pl-name').fill('Custom reset fixture');
  await page.locator('#pl-fee').fill('40000');
  await page.locator('#pl-funded-reset-fee-mode').selectOption('custom');
  await page.locator('#pl-funded-reset-fee').fill('25000');
  await page.locator('#pl-reset-fee-mode').selectOption('custom');
  await page.locator('#pl-reset-fee').fill('6000');
  await page.locator('#pl-fee').fill('50000');
  assert.equal(await page.locator('#pl-funded-reset-fee').inputValue(),'25000');
  await page.evaluate(()=>createPlan());
  assert.equal((await page.evaluate(()=>calls.at(-1))).body.funded_reset_fee,25000);
  assert.equal((await page.evaluate(()=>calls.at(-1))).body.reset_fee,6000);
  await page.locator('#pl-funded-reset-fee-mode').selectOption('none');
  await page.locator('#pl-reset-fee-mode').selectOption('none');
  await page.evaluate(()=>createPlan());
  assert.equal((await page.evaluate(()=>calls.at(-1))).body.funded_reset_fee,0);
  assert.equal((await page.evaluate(()=>calls.at(-1))).body.reset_fee,0);
  console.log('Passed: actual new-plan card defaults to three, offers 1–5/unlimited, and submits each exact limit without releasing sales.');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1)});
