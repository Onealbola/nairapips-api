const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});try{
 for(const file of ['admin_clean.html','admin_clean_cpanel.html']){
 const page=await browser.newPage({viewport:{width:390,height:844}});await page.goto('about:blank');await page.setContent('<nav><span data-purchase-alert-slot></span></nav><div id="np-purchase-review-alert"></div>');
 await page.evaluate(()=>{
  window.SESSION={token:'fixture',admin:{id:'owner'},currentMod:'overview'};window.ids=[];window.fail=false;window.calls=[];window.cursor='2026-10-23T20:00:00Z';window.visible=[];
  window.store={};Object.defineProperty(window,'localStorage',{value:{getItem:k=>store[k]||null,setItem:(k,v)=>store[k]=v}});
  window.api=async(url)=>{calls.push(url);if(fail)throw Error('offline');return {success:true,mode:'new_arrivals',new_count:ids.length,new_ids:[...ids],observed_at:cursor}};
  window.setMod=m=>{SESSION.currentMod=m};window.npRefreshPurchaseOperationsData=()=>{};window.purchaseVisibleRows=()=>visible;
 });
 const html=fs.readFileSync('website-downloads/'+file,'utf8');const a=html.indexOf('function npPurchaseNeedsReview('),b=html.indexOf('function purchaseStatus(',a);await page.addScriptTag({content:html.slice(a,b)});
 await page.evaluate(()=>npPurchaseAlertRefresh());assert(await page.locator('[data-purchase-alert-badge]').isHidden());assert.equal(await page.evaluate(()=>NP_PURCHASE_ALERT.seen),'2026-10-23T20:00:00Z');
 await page.evaluate(()=>{ids=['new'];cursor='2026-10-23T20:01:00Z';return npPurchaseAlertRefresh()});assert.equal(await page.locator('[data-purchase-alert-badge]').textContent(),'1');assert((await page.locator('[data-purchase-alert-badge]').getAttribute('class')).includes('np-purchase-blink'));
 await page.getByRole('button',{name:'View new purchases',exact:true}).click();assert.equal(await page.evaluate(()=>SESSION.currentMod),'purchases');assert.equal(await page.evaluate(()=>PURCHASE_FILTER_STATUS),'all');
 await page.evaluate(()=>npPurchaseArrivalsViewed());assert.equal(await page.locator('[data-purchase-alert-badge]').textContent(),'1');
 await page.evaluate(()=>{visible=[{id:'new',status:'pending'}];npPurchaseArrivalsViewed()});assert(await page.locator('[data-purchase-alert-badge]').isHidden());assert.equal(await page.evaluate(()=>NP_PURCHASE_ALERT.seen),'2026-10-23T20:01:00Z');
 await page.evaluate(()=>{ids=[];window.NP_PURCHASE_ALERT=null;return npPurchaseAlertRefresh()});assert(await page.locator('[data-purchase-alert-badge]').isHidden());assert((await page.evaluate(()=>calls.at(-1))).includes('since=2026-10-23T20%3A01%3A00Z'));
 await page.evaluate(()=>{fail=true;return npPurchaseAlertRefresh()});assert.equal(await page.locator('[data-purchase-alert-badge]').textContent(),'…');
 await page.close();
 }console.log('PASS: old pending does not blink; new arrivals pulse; only displayed arrivals clear; checkpoint survives refresh; unavailability stays honest.');
 }finally{await browser.close()}})().catch(e=>{console.error(e);process.exit(1)});
