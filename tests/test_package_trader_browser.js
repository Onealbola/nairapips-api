const {chromium}=require('playwright');const assert=require('assert'),fs=require('fs');
(async()=>{
const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage({viewport:{width:1280,height:960}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.route('https://fixture.invalid/**',route=>route.fulfill({contentType:'text/html',body:'<style>body{font-family:sans-serif;margin:12px}.card{padding:12px;max-width:100%;box-sizing:border-box}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}input,select{max-width:100%;box-sizing:border-box}dd{margin:0}</style><main id="package-root"></main>'}));
 await page.goto('https://fixture.invalid/');
 await page.evaluate(()=>{
  window.SESSION={trader:{id:'11111111-1111-4111-8111-111111111111'},auth_token:'fixture-token',currentTab:'packages',paymentAccounts:[]};window.API='https://fixture.invalid';
  window.esc=x=>String(x).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  window.renderApp=()=>{document.getElementById('package-root').innerHTML=npRenderPackageBuilderV222();};
  window.requests=[];window.available=true;window.fixtureContracts=[];window.delayQuote=false;window.checkoutFails=true;let quoteNumber=0,uploads=0;
  const options={capital:[{value:'500000'},{value:'250000'}],phases:[{value:'one'}],drawdown:[{value:'10'}],drawdown_type:[{value:'static'}],phase1_target:[{value:'10'}],phase2_target:[{value:'8'}],profit_share:[{value:'70'}],profit_cap:[{value:'none'}],payout_cycles:[{value:'3'}],phase_resets:[{value:'0'}],funded_resets:[{value:'0'}]};
  window.fetch=async(url,opts={})=>{
   const body=typeof opts.body==='string'?JSON.parse(opts.body):null;requests.push({url,method:opts.method||'GET',body});
   const reply=data=>({ok:true,json:async()=>data});
   if(url.endsWith('/trader/package_catalog'))return reply({success:true,available,options:available?options:{}});
   if(url.includes('/trader/package_contracts'))return reply({success:true,contracts:fixtureContracts,page:1,has_more:false});
   if(url.endsWith('/trader/package_quote')){
    const c=body.choices,rule={target_percent:'10',drawdown_percent:'10',drawdown_type:'static'};
    const doc={contract_id:'22222222-2222-4222-8222-222222222222',trader_id:SESSION.trader.id,capital:c.capital,stages:['phase1','funded'],stage_rules:{phase1:rule,funded:{...rule,target_percent:null}},profit_share_percent:'70',gross_profit_cap_percent:null,payout_cycles:3,phase_resets:0,funded_resets:0,purchase_price:'10000.00'};
    const q={id:'quote-'+(++quoteNumber),catalog_revision:1,rules_hash:'fixture-hash',document:doc,expires_at:'2026-10-18T10:00:00Z'};window.lastQuote=q;
    if(delayQuote)await new Promise(resolve=>window.releaseQuote=resolve);
    return reply({success:true,quote:q});
   }
   if(url.endsWith('/upload_payment_proof'))return reply({success:true,url:'https://proof.invalid/upload-'+(++uploads)});
   if(url.endsWith('/trader/package_checkout')){
    if(checkoutFails)throw new TypeError('Checkout response lost. Check history or retry the same purchase.');
    fixtureContracts=[{id:lastQuote.document.contract_id,purchase_id:body.quote_id,rules_version:'NP_PACKAGE_RULES_1',document:lastQuote.document,created_at:'2026-10-17T10:00:00Z'}];
    return reply({success:true,purchase_id:body.quote_id,contract_id:lastQuote.document.contract_id,replayed:true});
   }
   throw Error('Unexpected fixture request '+url);
  };
 });
 await page.addScriptTag({content:fs.readFileSync(__dirname+'/../website-downloads/package-trader.js','utf8')});
 await page.evaluate(()=>npPackageMaybeLoadV222(true));
 assert.equal(await page.evaluate(()=>npPackageVisibleV222()),true);
 // A selector change while an older quote is pending must invalidate its response.
 await page.evaluate(()=>window.delayQuote=true);
 await page.getByRole('button',{name:'Get saved package price'}).click();
 await page.waitForFunction(()=>typeof releaseQuote==='function');
 await page.locator('#np-package-choice-capital').selectOption('250000');
 await page.evaluate(()=>{delayQuote=false;releaseQuote();});
 await page.waitForFunction(()=>!document.getElementById('np-package-quote-button').disabled);
 assert.equal(await page.getByText('Review your exact package',{exact:true}).count(),0);
 await page.getByRole('button',{name:'Get saved package price'}).click();
 await page.getByText('Review your exact package',{exact:true}).waitFor();
 const file={name:'proof.png',mimeType:'image/png',buffer:Buffer.from('isolated fixture file')};
 await page.locator('#np-package-accepted').check();await page.locator('#np-package-proof').setInputFiles(file);
 await page.getByRole('button',{name:'Submit payment proof',exact:true}).click();
 await page.waitForFunction(()=>document.getElementById('np-package-status').textContent.includes('response lost'));
 let checkout=await page.evaluate(()=>requests.filter(r=>r.url.endsWith('/trader/package_checkout')));
 assert.equal(checkout.length,1,'An uncertain mutation must not auto-retry');
 const firstKey=checkout[0].body.checkout_key,quoteId=checkout[0].body.quote_id;
 const saved=await page.evaluate(()=>sessionStorage.getItem('np-package-checkout-v1:'+SESSION.trader.id));
 assert(saved.includes(firstKey));assert(!saved.includes('https://proof.invalid'),'Payment proof URLs must not be stored in browser drafts');
 assert.equal(await page.getByRole('button',{name:'Retry same purchase',exact:true}).isDisabled(),false);
 await page.evaluate(()=>window.checkoutFails=false);
 await page.locator('#np-package-accepted').check();await page.locator('#np-package-proof').setInputFiles(file);
 await page.getByRole('button',{name:'Retry same purchase',exact:true}).click();
 await page.getByText(/is recorded\. Open Purchase Challenge history/).waitFor();
 checkout=await page.evaluate(()=>requests.filter(r=>r.url.endsWith('/trader/package_checkout')));
 assert.equal(checkout.length,2);assert.equal(checkout[1].body.checkout_key,firstKey);assert.equal(checkout[1].body.quote_id,quoteId);
 assert.equal(await page.evaluate(()=>sessionStorage.getItem('np-package-checkout-v1:'+SESSION.trader.id)),null);
 await page.evaluate(()=>{available=false;return npPackageMaybeLoadV222(true);});
 assert.equal(await page.getByRole('button',{name:'Get saved package price'}).count(),0);
 assert.equal(await page.locator('details').count(),1,'Pausing sales must not hide a purchased contract');
 await page.setViewportSize({width:390,height:844});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
 assert.deepEqual(errors,[]);
 console.log('Package browser passed: stale quote rejected, saved contract terms, single uncertain submit, stable retry key, no proof URL storage, preserved history after pause, mobile layout.');
}finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
