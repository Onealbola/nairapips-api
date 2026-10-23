const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});try{
 for(const file of ['admin_clean.html','admin_clean_cpanel.html']){
 const page=await browser.newPage({viewport:{width:390,height:844}});await page.setContent('<nav><span data-purchase-alert-slot></span></nav><div id="np-purchase-review-alert"></div>');
 await page.evaluate(()=>{window.SESSION={token:'fixture',admin:{id:'owner'}};window.count=2;window.fail=false;window.api=async()=>{if(fail)throw Error('offline');return {success:true,pending:count}};window.setMod=m=>window.opened=m;});
 const html=fs.readFileSync('website-downloads/'+file,'utf8');const a=html.indexOf('function npPurchaseNeedsReview('),b=html.indexOf('function purchaseStatus(',a);assert(a>=0&&b>a);await page.addScriptTag({content:html.slice(a,b)});
 await page.evaluate(()=>npPurchaseAlertRefresh());assert.equal(await page.locator('[data-purchase-alert-badge]').textContent(),'2');assert((await page.locator('[data-purchase-alert-badge]').getAttribute('class')).includes('np-purchase-blink'));
 await page.getByRole('button',{name:'Review purchases',exact:true}).click();assert.equal(await page.evaluate(()=>opened),'purchases');assert.equal(await page.evaluate(()=>PURCHASE_FILTER_STATUS),'needs_review');assert.equal(await page.evaluate(()=>PURCHASE_FILTER_RANGE),'all');
 await page.evaluate(()=>{count=0;return npPurchaseAlertRefresh()});assert(await page.locator('[data-purchase-alert-badge]').isHidden());assert(await page.locator('#np-purchase-review-alert').isHidden());
 await page.evaluate(()=>{count=3;return npPurchaseAlertRefresh()});assert.equal(await page.locator('[data-purchase-alert-badge]').textContent(),'3');
 await page.evaluate(()=>{fail=true;return npPurchaseAlertRefresh()});assert.equal(await page.locator('[data-purchase-alert-badge]').textContent(),'…');assert(!(await page.locator('[data-purchase-alert-badge]').getAttribute('class')).includes('np-purchase-blink'));
 assert((await page.locator('#np-purchase-review-alert').innerText()).includes('unavailable'));
 await page.evaluate(()=>{SESSION.token='other';npPurchaseAlertApply()});assert.equal(await page.locator('[data-purchase-alert-badge]').textContent(),'…');
 await page.close();
 }console.log('PASS: desktop/mobile alert slots, pending pulse, review queue, zero clears, polling updates, unavailable count and session isolation.');
 }finally{await browser.close()}})().catch(e=>{console.error(e);process.exit(1)});
