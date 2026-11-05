const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert'),cp=require('child_process');
const products=JSON.parse(cp.execFileSync('python',['-c',`
import json
from package_plan_matrix import defaults
from progress_plan_bridge import plan_row,commercial_plan
rows=[]
for p in defaults()['config']['plans']:
 for x in p['packages']:
  rows.append(dict(id='progress:'+p['id']+':'+x['key'],family='progress',name=p['name']+' · '+x['name'],purchases_open=False,plan=commercial_plan(plan_row(p,x,True)),package={**x,'capital':p['capital']}))
for family,name,cap,fee,share in [('standard','Standard',500000,20000,60),('premium','Premium',1000000,40000,70)]:
 rows.append(dict(id='plan:'+family,family=family,name=name,purchases_open=True,plan=dict(id=family,name=name,account_size=cap,fee=fee,phase1_target=10,phase2_target=0,max_drawdown=20,payout_split=share,payout_cycles=3,reset_fee=0,funded_reset_fee=0,drawdown_type='STATIC')))
print(json.dumps(rows))
`],{encoding:'utf8'}));
(async()=>{const b=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});try{
 const ctx=await b.newContext();let fail=false;const errors=[],requests=[];
 await ctx.route('https://api.test/**',r=>{requests.push(r.request().url());return r.fulfill({status:fail?503:200,json:fail?{success:false}:{success:true,products}})});
 const page=await ctx.newPage({viewport:{width:1440,height:1000}});page.on('pageerror',e=>errors.push(e.message));await page.setContent('<main id="content"></main>');
 await page.evaluate(()=>{window.API='https://api.test';window.SESSION={currentTab:'plans',trader:{id:'fixture'},plansList:[],paymentAccounts:[]};window.buys=[];window.purchasePlan=id=>buys.push({id,plan:SESSION.plansList.find(x=>x.id===id)});window.toast=()=>{};window.renderApp=()=>document.getElementById('content').innerHTML=npSharedCatalogueHTML()});
 const common=fs.readFileSync('website-downloads/account-plan-selector.js','utf8'),trader=fs.readFileSync('website-downloads/trader-catalog-sync.js','utf8');await page.addScriptTag({content:common+'\n'+trader});await page.locator('.nps-summary').waitFor();
 assert.equal(await page.locator('[data-nps-action="family"][aria-selected="true"]').getAttribute('data-nps-value'),'standard');
 await page.locator('[data-catalog-product="plan:standard"]').click();assert.equal((await page.evaluate(()=>buys)).length,1);assert.equal((await page.evaluate(()=>buys[0].plan)).fee,20000);
 await page.locator('[data-nps-action="family"][data-nps-value="standard"]').focus();await page.keyboard.press('Home');assert.equal(await page.locator('[data-nps-action="family"][aria-selected="true"]').getAttribute('data-nps-value'),'progress');assert.equal(await page.locator('.nps-sizes button').count(),3);assert.equal(await page.locator('.nps-variants button').count(),4);assert(await page.locator('.nps-buy').isDisabled());
 await page.locator('[data-nps-action="capital"][data-nps-value="1000000"]').click();await page.locator('[data-nps-action="product"][data-nps-value="progress:ngn-1000000:room"]').click();assert((await page.locator('.nps-price').innerText()).includes('28,000'));
 await page.locator('[data-nps-action="unit"][data-nps-value="ngn"]').click();const maxdd=page.locator('.nps-rule').filter({has:page.locator('dt',{hasText:'Max drawdown'})});assert((await maxdd.innerText()).includes('100,000'));
 await page.locator('[data-nps-action="detail"][data-nps-value="rewards"]').click();assert((await page.locator('.nps-rules').innerText()).includes('Continuing'));assert((await page.locator('.nps-rules').innerText()).includes('150,000'));
 await page.locator('[data-nps-action="compare"]').click();assert.equal(await page.locator('.nps-comparison th').count(),5);assert((await page.locator('.nps-comparison').innerText()).includes('24,000'));
 const room=products.find(p=>p.id==='progress:ngn-1000000:room');room.plan.fee=room.package.price=29000;room.plan.payout_split=room.package.profit_share=55;
 await page.evaluate(()=>npRefreshSharedCatalogue(true));assert((await page.locator('.nps-price').innerText()).includes('29,000'));assert((await page.locator('.nps-rules').innerText()).includes('55%'));assert.equal(await page.locator('.nps-summary').getAttribute('data-nps-selected'),room.id);
 await page.addScriptTag({content:common+'\n'+trader});assert.equal(await page.locator('#np-plan-selector-style').count(),1);assert.equal(await page.locator('.nps-summary').count(),1);
 room.purchases_open=true;await page.evaluate(()=>npRefreshSharedCatalogue(true));await page.locator('.nps-buy').click();const buys=await page.evaluate(()=>buys);assert.equal(buys.length,2);assert.equal(buys[1].plan.fee,29000);assert.equal(buys[1].plan.payout_cycles,0);assert.equal(buys[1].plan.phase1_target,10);assert.equal(buys[1].plan.max_drawdown,10);assert.equal(buys[1].plan.payout_split,55);assert.equal(buys[1].plan.id,room.plan.id);
 room.purchases_open=false;await page.evaluate(()=>npRefreshSharedCatalogue(true));assert(await page.locator('.nps-buy').isDisabled());
 room.package.name='X'.repeat(60);await page.evaluate(()=>npRefreshSharedCatalogue(true));await page.setViewportSize({width:390,height:844});assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
 await page.evaluate(()=>{SESSION.currentTab='purchase';renderApp()});assert.equal(await page.locator('.nps-summary').getAttribute('data-nps-selected'),room.id);
 fail=true;await page.evaluate(()=>npRefreshSharedCatalogue(true));assert.equal(await page.locator('.nps-buy').count(),0);assert((await page.locator('body').innerText()).includes('could not be confirmed'));fail=false;await page.evaluate(()=>npRefreshSharedCatalogue(true));await page.locator('.nps-summary').waitFor();
 assert.deepEqual(errors,[]);assert(requests.every(url=>url.endsWith('/trader/challenge_catalog')));
 console.log('PASS: model/size/package selection, exact checkout identity and price, %/NGN rules, comparison, refresh selection retention, pause, reused renderer, mobile, both trader tabs, and fail-closed reads.');
 }finally{await b.close()}})().catch(e=>{console.error(e);process.exit(1)});
