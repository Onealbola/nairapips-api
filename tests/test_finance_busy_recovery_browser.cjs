const {chromium}=require('playwright'),fs=require('fs'),assert=require('node:assert/strict');
(async()=>{const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});try{
 for(const file of ['admin_clean.html','admin_clean_cpanel.html']){
 const page=await browser.newPage();await page.clock.install();await page.setContent('<main id="content"></main>');
 await page.evaluate(()=>{
 window.SESSION={token:'fixture',currentMod:'purchases'};window.PURCHASE_FILTER_RANGE='all';window.NP_PAYOUT_INTEL={range:'all'};window.calls=0;window.fail=true;window.source={at:0,data:null,pending:null};
 window.npFinanceSourceState=()=>source;
 window.npFinanceLoad=async()=>{calls++;if(fail)throw Error('The server is handling other requests. This request has not started. Please try again shortly.');source.data={generated_at:new Date().toISOString()};source.at=Date.now();};
 window.api=async()=>({success:true});window.renderPurchases=()=>document.getElementById('content').innerHTML='<h1>Complete Purchases</h1>';window.renderPayouts=()=>{};window.revenueModule=()=>{};
 });
 const html=fs.readFileSync('website-downloads/'+file,'utf8');const marker=html.indexOf('const apiCore=window.api');const start=html.lastIndexOf('(function()',marker);const end=html.indexOf('</script>',marker);await page.addScriptTag({content:html.slice(start,end)});
 await page.evaluate(()=>renderPurchases());assert.match(await page.locator('#content').innerText(),/Retrying automatically/);assert.equal(await page.locator('button').innerText(),'Try again');
 await page.evaluate(()=>{fail=false});await page.clock.fastForward(10000);await page.waitForFunction(()=>document.getElementById('content').innerText.includes('Complete Purchases'));assert.equal(await page.evaluate(()=>calls),2);
 // A pending retry cannot reopen Purchases over another module.
 await page.evaluate(()=>{source.at=0;fail=true;return renderPurchases()});await page.evaluate(()=>{SESSION.currentMod='accounts';document.getElementById('content').innerHTML='<h1>Accounts</h1>'});await page.clock.fastForward(10000);assert.equal(await page.locator('#content').innerText(),'Accounts');assert.equal(await page.evaluate(()=>calls),3);
 // The manual button retries only the read and restores a complete module.
 await page.evaluate(()=>{SESSION.currentMod='purchases';return renderPurchases()});await page.evaluate(()=>{fail=false});await page.getByRole('button',{name:'Try again'}).click();await page.waitForFunction(()=>document.getElementById('content').innerText.includes('Complete Purchases'));
 await page.close();
 }console.log('PASS: both Admin pages recover from busy financial reads automatically and manually, without overriding navigation or showing partial totals.');
}finally{await browser.close()}})().catch(e=>{console.error(e);process.exitCode=1});
