const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});try{
 const page=await browser.newPage();await page.clock.install({time:new Date('2026-10-24T06:30:00Z')});await page.setContent('<main id="content"></main>');
 await page.evaluate(()=>{
  window.SESSION={token:'fixture',currentMod:'purchases'};window.PURCHASE_FILTER_RANGE='month';window.NP_PAYOUT_INTEL={range:'30d'};window.saved={};window.count=0;window.failure=false;
  window.api=async(path,method)=>{if(path.includes('finance_source')){count++;if(failure)throw Error('Unavailable');return {success:true,purchases:[],payouts:[],generated_at:'2026-10-24T06:30:00Z'}};return {success:true}};
  window.renderPurchases=()=>document.getElementById('content').innerHTML='<h1>Purchases</h1>';window.renderPayouts=()=>document.getElementById('content').innerHTML='<h1>Payouts</h1>';window.revenueModule=async()=>document.getElementById('content').innerHTML='<h1>Revenue</h1>';window.saveRevenueDateFilters=f=>saved=f;window.revenueDateFilters=()=>saved;
 });
 const html=fs.readFileSync('website-downloads/admin_clean.html','utf8'),a=html.lastIndexOf('<script>'),b=html.indexOf('</script>',a);
 await page.addScriptTag({content:fs.readFileSync('website-downloads/finance-rules.js','utf8')});await page.addScriptTag({content:html.slice(a+8,b)});
 await page.evaluate(()=>renderPurchases());assert.equal(await page.evaluate(()=>count),1);
 await page.evaluate(()=>{SESSION.currentMod='payouts';return renderPayouts()});assert.equal(await page.evaluate(()=>NP_PAYOUT_INTEL.range),'month');assert.equal(await page.evaluate(()=>count),1);
 await page.evaluate(()=>{SESSION.currentMod='revenue';return revenueModule()});assert.deepEqual(await page.evaluate(()=>saved),{from_date:'2026-10-01',to_date:'2026-10-24'});
 await page.evaluate(()=>{saveRevenueDateFilters({from_date:'2026-10-03',to_date:'2026-10-20'});return revenueModule()});
 await page.evaluate(()=>{SESSION.currentMod='payouts';return renderPayouts()});assert.equal(await page.evaluate(()=>NP_PAYOUT_INTEL.from),'2026-10-03T00:00');assert.equal(await page.evaluate(()=>NP_PAYOUT_INTEL.to),'2026-10-20T23:59');
 await page.evaluate(()=>{SESSION.currentMod='purchases';return renderPurchases()});assert.equal(await page.evaluate(()=>PURCHASE_CUSTOM_FROM),'2026-10-03');
 await page.evaluate(()=>{SESSION.currentMod='revenue';return revenueModule()});
 await page.evaluate(()=>api('/mark_payout_paid','POST',{}));assert.equal(await page.evaluate(()=>npFinanceSourceState().at),0);
 await page.evaluate(()=>{failure=true;return revenueModule()});assert((await page.locator('#content').innerText()).includes('Unavailable'));assert(!(await page.locator('#content').innerText()).includes('₦0'));
 console.log('PASS: shared period carries between modules, complete source reused, payment changes invalidate totals and unavailable source never shows zero.');
 }finally{await browser.close()}})().catch(e=>{console.error(e);process.exit(1)});
