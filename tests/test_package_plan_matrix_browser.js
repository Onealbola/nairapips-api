const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert'),cp=require('child_process'),path=require('path');
const root=path.resolve(__dirname,'..');
// Use the actual connected backend response, so editor/API contract drift fails here.
const generated=JSON.parse(cp.execFileSync('python',['-c',`
import json
from flask import Flask
from types import SimpleNamespace
from package_plan_matrix import defaults,register_plan_matrix
app=Flask(__name__)
app.config['NP_PROGRESS_PLAN_BRIDGE']=object()
register_plan_matrix(app,SimpleNamespace(read=lambda **kw:(defaults(),None)),lambda:({'id':'owner','role':'owner'},None),lambda *a:None)
print(json.dumps({'response':app.test_client().get('/admin/package_studio/plan_matrix').json,'view_html':app.test_client().get('/admin/package_studio/plan_matrix/view').get_data(as_text=True)}))
`],{cwd:root,encoding:'utf8'}));
const fixture=generated.response;const initial=fixture.data;
assert.equal(fixture.production_effect,'future_offers_only');assert.equal(fixture.purchase_controls_connected,true);
(async()=>{const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});try{
 let doc=structuredClone(initial),writes=0,fail=false;const errors=[],requests=[];
 const context=await browser.newContext();
 await context.route('https://matrix.test/**',async route=>{const r=route.request(),url=new URL(r.url()),p=url.pathname;requests.push(p);
  if(p==='/package-studio')return route.fulfill({contentType:'text/html',body:fs.readFileSync(path.join(root,'package_studio_assets/login.html'),'utf8')});
  if(p==='/staff_login')return route.fulfill({json:{success:true,token:'fixture-token'}});
  if(r.headers().authorization!=='Bearer fixture-token')return route.fulfill({status:401,json:{success:false,error:'Sign in required'}});
  if(p==='/admin/package_studio/plan_matrix/view')return route.fulfill({contentType:'text/html',body:generated.view_html});
  if(p==='/admin/package_studio/plan_matrix'){
   if(fail)return route.fulfill({status:503,json:{success:false,error:'Fixture database unavailable'}});
   if(r.method()==='POST'){const b=r.postDataJSON();if(b.revision!==doc.revision)return route.fulfill({status:409,json:{success:false,error:'Another admin saved a newer version'}});doc={...doc,revision:doc.revision+1,config:b.config};writes++}
   return route.fulfill({json:{...fixture,data:doc}})
  }throw Error('Unexpected route '+p);
 });
 async function open(){const p=await context.newPage();p.on('pageerror',e=>errors.push(e.message));await p.goto('https://matrix.test/package-studio');await p.fill('#username','owner');await p.fill('#password','fixture');await p.click('#submit');await p.locator('#status').filter({hasText:'Loaded saved revision'}).waitFor();return p;}
 const p=await open();assert.equal(await p.locator('#overview tr').count(),3);assert.equal(await p.locator('#packages [data-package]').count(),4);assert((await p.locator('#overview').textContent()).includes('₦64,000'));
 await p.selectOption('[data-package="basic"] [data-field="drawdown_type"]','TRAILING');assert((await p.locator('#preview').innerText()).includes('TRAILING'));
 await p.locator('[data-package="basic"] [data-field="daily_drawdown_enabled"]').check();await p.fill('[data-package="basic"] [data-field="daily_drawdown"]','2.75');
 await p.locator('[data-package="basic"] [data-field="payout_schedule_enabled"]').check();await p.selectOption('[data-package="basic"] [data-field="payout_frequency"]','Weekly');
 await p.locator('[data-package="basic"] [data-field="minimum_withdrawal_enabled"]').check();await p.fill('[data-package="basic"] [data-field="minimum_withdrawal"]','10000');await p.locator('[data-package="basic"] [data-field="minimum_profit_enabled"]').check();await p.fill('[data-package="basic"] [data-field="minimum_profit_percent"]','5');
 await p.locator('[data-package="basic"] [data-field="phase1_reset_enabled"]').check();await p.fill('[data-package="basic"] [data-field="phase1_reset_fee"]','12500');
 await p.locator('[data-package="basic"] [data-field="funded_reset_enabled"]').check();await p.fill('[data-package="basic"] [data-field="funded_reset_fee"]','25000');
 assert((await p.locator('#preview').innerText()).includes('2.75%'));await p.locator('#preview [data-nps-action="detail"][data-nps-value="rewards"]').click();assert((await p.locator('#preview').innerText()).includes('Weekly'));assert((await p.locator('#preview').innerText()).includes('₦10,000'));assert((await p.locator('#preview').innerText()).includes('Minimum profit target'));assert((await p.locator('#preview').innerText()).includes('12,500'));await p.locator('#preview [data-nps-action="detail"][data-nps-value="account"]').click();

 await p.fill('[data-package="basic"] [data-field="price"]','11000');await p.fill('[data-package="basic"] [data-field="profit_share"]','55');await p.selectOption('[data-package="basic"] [data-field="pair"]','8/5');await p.locator('[data-package-tab="both"]').click();await p.locator('[data-package="both"] [data-field="enabled"]').uncheck();await p.click('#save');await p.locator('#status').filter({hasText:'saved revision 1'}).waitFor();assert.equal(doc.config.plans[0].packages[0].price,11000);assert.equal(doc.config.plans[0].packages[0].profit_share,55);assert.equal(doc.config.plans[0].packages[0].phase2_target,5);assert.equal(doc.config.plans[0].packages[3].enabled,false);
 await p.click('#add');await p.fill('#new-name','N5 million');await p.fill('#new-capital','5000000');await p.fill('#new-price','100000');await p.click('#add-form button[type="submit"]');assert.equal(await p.locator('#overview tr').count(),4);assert.equal(await p.locator('#plan-fields [data-field="enabled"]').isChecked(),false);assert((await p.locator('#preview .nps-price').innerText()).includes('₦160,000'));assert(await p.locator('#preview .nps-buy').isDisabled());await p.click('#save');await p.locator('#status').filter({hasText:'saved revision 2'}).waitFor();
 const second=await open();assert.equal(await second.locator('[data-package="basic"] [data-field="minimum_withdrawal"]').inputValue(),'10000');assert.equal(await second.locator('[data-package="basic"] [data-field="minimum_profit_percent"]').inputValue(),'5');assert.equal(await second.locator('[data-package="basic"] [data-field="daily_drawdown"]').inputValue(),'2.75');assert(await second.locator('[data-package="basic"] [data-field="daily_drawdown_enabled"]').isChecked());assert.equal(await second.locator('[data-package="basic"] [data-field="payout_frequency"]').inputValue(),'Weekly');assert.equal(await second.locator('[data-package="basic"] [data-field="phase1_reset_fee"]').inputValue(),'12500');assert.equal(await second.locator('[data-package="basic"] [data-field="drawdown_type"]').inputValue(),'TRAILING');assert.equal(await second.locator('#overview tr').count(),4);assert.equal(await second.locator('[data-package="basic"] [data-field="price"]').inputValue(),'11000');assert.equal(await second.locator('[data-package="both"] [data-field="enabled"]').isChecked(),false);
 await second.selectOption('#plan','ngn-2000000');assert.equal(await second.locator('[data-package="basic"] [data-field="price"]').inputValue(),'40000');await second.fill('[data-package="basic"] [data-field="profit_share"]','70');assert.equal(await second.locator('#limits').count(),0);assert(!(await second.locator('#overview').textContent()).toLowerCase().includes('blocked by limits'));
 doc.revision++;await second.click('#save');await second.locator('#status').filter({hasText:'Not saved:'}).waitFor();assert.equal(writes,2);assert.equal(await second.locator('[data-package="basic"] [data-field="profit_share"]').inputValue(),'70');
 await p.setViewportSize({width:390,height:844});assert(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
 await p.click('#reload');await p.locator('#status').filter({hasText:'Loaded saved revision 3'}).waitFor();
 // Model-specific fields and independent prices survive a full saved reload.
 await p.selectOption('#model','one_phase');assert((await p.locator('#plan-fields').innerText()).includes('No 1-Phase accounts yet'));await p.click('#add');await p.fill('#new-name','One Phase');await p.fill('#new-capital','500000');await p.fill('#new-price','22000');await p.click('#add-form button[type="submit"]');assert.equal(await p.locator('[data-package="basic"] [data-field="phase1_target"]').inputValue(),'10');assert.equal(await p.locator('[data-package="basic"] [data-field="pair"]').count(),0);await p.locator('[data-package-tab="basic"]').click();await p.fill('[data-package="basic"] [data-field="phase1_target"]','12');
 await p.selectOption('#model','instant');await p.click('#add');await p.fill('#new-name','Instant');await p.fill('#new-capital','500000');await p.fill('#new-price','65000');await p.click('#add-form button[type="submit"]');assert.equal(await p.locator('[data-package="basic"] [data-field="phase1_target"]').count(),0);assert((await p.locator('#preview .nps-rules').innerText()).includes('Instant Funded'));assert(!(await p.locator('#preview .nps-rules').innerText()).includes('Phase 1 target'));
 await p.click('#save');await p.locator('#status').filter({hasText:'saved revision 4'}).waitFor();assert.equal(doc.config.plans.find(x=>x.model==='one_phase').packages[0].phase1_target,12);assert.equal(doc.config.plans.find(x=>x.model==='instant').packages[0].phase1_target,0);assert.equal(doc.config.plans.find(x=>x.model==='instant').enabled,false);
 await p.click('#reload');await p.locator('#status').filter({hasText:'Loaded saved revision 4'}).waitFor();assert.equal(await p.locator('#model').inputValue(),'instant');assert((await p.locator('#preview .nps-price').innerText()).includes('65,000'));assert(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
 fail=true;const failed=await context.newPage();await failed.goto('https://matrix.test/package-studio');await failed.fill('#username','owner');await failed.fill('#password','fixture');await failed.click('#submit');await failed.locator('#loading-note').filter({hasText:'Could not load'}).waitFor();assert(await failed.locator('#save').isDisabled());
 assert.deepEqual(errors,[]);assert(!requests.some(p=>/checkout|create_challenge|assign|payout/.test(p)));console.log('PASS: four-package matrix, all prices, editable terms, persisted switches and prices, new account size, cross-session reload, independent package rules, conflict retention, mobile, fail-closed loading and no checkout requests.');
}finally{await browser.close()}})().catch(e=>{console.error(e);process.exit(1)});
