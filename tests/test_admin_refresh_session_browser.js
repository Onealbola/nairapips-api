// Full HTML boot: later modules must load before restoring an existing session.
const {chromium}=require('playwright');
const fs=require('fs'),path=require('path'),assert=require('assert');
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 try{
  for(const file of ['admin_clean.html','admin_clean_cpanel.html']){
   const html=fs.readFileSync(path.join(__dirname,'../website-downloads',file),'utf8');
   for(const recoveryOnly of [false,true]){
    const page=await browser.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
    await page.route('**/*',route=>{
     const url=new URL(route.request().url());
     if(url.hostname==='panel.test')return route.fulfill({contentType:'text/html',body:url.pathname==='/setup'?'<!doctype html><title>Setup</title>':html});
     return route.fulfill({status:503,contentType:'application/json',body:JSON.stringify({success:false,error:'Temporary outage'})});
    });
    await page.goto('https://panel.test/setup');
    await page.evaluate(recoveryOnly=>{
     const payload=JSON.stringify({admin:{id:'fixture',name:'Owner',role:'super_admin'},token:'fixture-token',saved_at:Date.now()});
     localStorage.setItem('nairapips_admin_session_recovery',payload);
     if(!recoveryOnly)sessionStorage.setItem('nairapips_admin_session',payload);
    },recoveryOnly);
    await page.goto('https://panel.test/admin');
    const cdp=await page.context().newCDPSession(page);await cdp.send('Network.setCacheDisabled',{cacheDisabled:true});
    for(let i=0;i<3;i++){
     await page.waitForFunction(()=>document.querySelector('.app')&&!document.querySelector('#li-user'));
     assert.equal(await page.evaluate(()=>JSON.parse(sessionStorage.getItem('nairapips_admin_session')).token),'fixture-token');
     if(i<2)await page.reload({waitUntil:'load'});
    }
    assert.deepEqual(errors,[]);
    await page.evaluate(()=>logout());
    assert.equal(await page.locator('#li-user').count(),1);
    assert.equal(await page.evaluate(()=>sessionStorage.getItem('nairapips_admin_session')||localStorage.getItem('nairapips_admin_session_recovery')),null);
    await page.close();
   }
  }
  console.log('PASS: both admin files retain sessions across cache-disabled reloads, storage recovery and backend outages; explicit sign-out clears both stores.');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1)});
