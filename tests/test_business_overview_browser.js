const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
const fixture={release:'business-overview-truth-2026-10-22',generated_at:'2026-10-22T10:00:00Z',period:'today',counts:{customers:3,open_accounts:1,phase1_accounts:1,work_waiting:1,purchase_reviews:1,approved_receipts:1},amounts:{approved_receipts:40000,paid_payouts:0,approved_payouts:0},unknown_amounts:{},groups:{purchase_reviews:['purchase:p'],approved_receipts:['purchase:p']},records:{'purchase:p':{id:'p',kind:'purchase',trader_id:'t',customer:'<img src=x onerror=alert(1)>',reference:'Standard',status:'pending',occurred_at:'2026-10-22T09:00:00Z',amount:40000}},reasons:{purchase_reviews:{'purchase:p':'Submitted payment proof, not yet collected money'}},oldest_waiting:{purchase_reviews:'2026-10-22T09:00:00Z'}};
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 try{for(const file of ['business-overview.js','admin_clean_cpanel.html']){
  const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.setContent('<main id="content"></main>');
  await page.evaluate(fixture=>{
   window.SESSION={token:'fixture',admin:{id:'owner'},currentMod:'overview'};
   window.renderOverview=()=>{};window.npOpenTrader360=()=>{};
   window.esc=x=>String(x).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
   window.fail=false;window.calls=[];
   window.api=async path=>{calls.push(path);if(fail)throw Error('Offline fixture');return {success:true,data:{...structuredClone(fixture),period:new URL('https://fixture.invalid'+path).searchParams.get('period')}};};
  },fixture);
  let code=fs.readFileSync(__dirname+'/../website-downloads/'+file,'utf8');
  if(file.endsWith('.html'))code=code.split('<!-- BUSINESS_OVERVIEW_TRUTH_V1 -->')[1].match(/<script>([\s\S]*?)<\/script>/)[1];
  await page.addScriptTag({content:code});await page.evaluate(()=>renderOverview());
  await page.waitForFunction(()=>document.querySelector('[data-overview-metric="customers"] .kpi-value')?.textContent==='3');
  const text=await page.locator('#content').innerText();
  assert(!text.includes('CRITICAL'));assert(!text.includes('stageOf()'));assert(text.includes('Recorded money'));assert(text.includes('Oldest:'));
  await page.locator('[data-overview-metric="purchase_reviews"]').click();
  assert.equal(await page.locator('#business-overview-record-table tbody tr').count(),1);
  assert((await page.locator('#business-overview-record-table').innerText()).includes('Submitted payment proof'));
  assert.equal(await page.locator('#business-overview-records img').count(),0);
  await page.getByLabel('Search overview records').fill('nothing-matches');
  assert((await page.locator('#business-overview-record-table').innerText()).includes('0 matching records'));
  await page.getByLabel('Money reporting period').selectOption('7d');
  await page.waitForFunction(()=>document.getElementById('business-overview-period')?.value==='7d'&&!document.getElementById('business-overview-period').disabled);
  await page.evaluate(()=>{fail=true});await page.getByRole('button',{name:'↻ Refresh',exact:true}).click();
  await page.waitForFunction(()=>document.getElementById('content').innerText.includes('previous snapshot retained'));
  assert.equal(await page.locator('[data-overview-metric="customers"] .kpi-value').innerText(),'3');
  await page.evaluate(()=>{SESSION.currentMod='accounts'});
  const before=await page.evaluate(()=>calls.length);await page.evaluate(()=>npRefreshBusinessOverview(false));
  assert.equal(await page.evaluate(()=>calls.length),before);
  await page.evaluate(()=>{SESSION.token='another-session';SESSION.currentMod='overview';renderOverview()});
  await page.waitForFunction(()=>document.getElementById('content').innerText.includes('Totals are unavailable'));
  assert.equal(await page.locator('.kpi-value').count(),0);
  assert.deepEqual(errors,[]);
  await page.close();
 }console.log('PASS: real and compact overview show source counts, exact drilldowns, safe escaping, periods, stale data and session isolation.');}
 finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1)});
