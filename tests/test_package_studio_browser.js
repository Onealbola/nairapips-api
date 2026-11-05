const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});try{
 const root='/workspace/nairapips-api/package_studio_assets/';
 if(!fs.existsSync(root+'studio.html')){console.log('Private owner artifact required for hosted-browser checks; it is deliberately excluded from public source.');return;}
 let doc={revision:0,mode:'draft',checkout_enabled:false,config:JSON.parse(fs.readFileSync(root+'defaults.json')),updated_at:null};
 let saves=0,failLoad=false;const errors=[],requests=[];
 const context=await browser.newContext();
 await context.route('https://studio.test/**',async route=>{
  const r=route.request(),path=new URL(r.url()).pathname;requests.push(path);
  if(path==='/package-studio')return route.fulfill({contentType:'text/html',body:fs.readFileSync(root+'login.html','utf8')});
  if(path==='/staff_login')return route.fulfill({json:{success:true,token:'fixture-owner-token'}});
  if(r.headers().authorization!=='Bearer fixture-owner-token')return route.fulfill({status:401,json:{success:false,error:'Sign in required'}});
  if(path==='/admin/package_studio/view')return route.fulfill({contentType:'text/html',body:fs.readFileSync(root+'studio.html','utf8')});
  if(path==='/admin/package_studio/draft'){
   if(failLoad)return route.fulfill({status:503,json:{success:false,error:'Settings unavailable'}});
   if(r.method()==='POST'){
    const body=r.postDataJSON();if(body.revision!==doc.revision)return route.fulfill({status:409,json:{success:false,error:'Another admin saved a newer draft'}});
    saves++;doc={...doc,revision:doc.revision+1,config:body.config};
   }
   return route.fulfill({json:{success:true,data:doc,checkout_enabled:false}});
  }
  throw Error('Unexpected request '+path);
 });
 async function signIn(){const p=await context.newPage();p.on('pageerror',e=>errors.push(e.message));await p.goto('https://studio.test/package-studio?legacy=1');assert(!(await p.content()).includes('maxPayoutToFee'));await p.fill('#username','owner');await p.fill('#password','fixture-password');await p.click('#submit');await p.locator('#server-status').filter({hasText:'Loaded admin draft'}).waitFor();return p;}
 const p=await signIn();assert.equal(await p.locator('#total').innerText(),'₦10,000');
 await p.locator('[data-tab="settings"]').click();await p.fill('[data-base="500000"]','12000');await p.locator('#apply-settings').click();await p.locator('#server-save').click();await p.locator('#server-status').filter({hasText:'Confirmed:'}).waitFor();assert.equal(doc.config.base['500000'],12000);assert.equal(saves,1);
 await p.selectOption('#availability-cap','500000');await p.locator('[data-availability="profitCap:30"]').uncheck();await p.locator('#server-save').click();await p.locator('#server-status').filter({hasText:'revision 2 saved'}).waitFor();
 const second=await signIn();assert.equal(await second.locator('#total').innerText(),'₦12,000');assert(await second.locator('#profitCap option[value="30"]').evaluate(e=>e.disabled));
 // A stale browser cannot replace another admin's saved settings.
 doc.revision++;await p.locator('#server-save').click();await p.locator('#server-status').filter({hasText:'Not confirmed:'}).waitFor();assert.equal(saves,2);assert.equal(doc.checkout_enabled,false);
 await second.setViewportSize({width:390,height:844});assert(await second.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
 assert(!requests.some(p=>/checkout|create_challenge|payout|assign/.test(p)));
 failLoad=true;const failed=await context.newPage();await failed.goto('https://studio.test/package-studio?legacy=1');await failed.fill('#username','owner');await failed.fill('#password','fixture-password');await failed.click('#submit');await failed.locator('body').filter({hasText:'Cannot load your admin draft'}).waitFor();assert(await failed.locator('#server-save').isDisabled());
 assert.deepEqual(errors,[]);console.log('PASS: owner sign-in; protected draft load; saved price and switches across sessions; stale-save conflict; mobile; no checkout calls; failed load cannot save defaults.');
}finally{await browser.close()}})().catch(e=>{console.error(e);process.exit(1)});
