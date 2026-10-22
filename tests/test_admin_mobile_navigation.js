const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
try{for(const file of ['admin_clean.html','admin_clean_cpanel.html']){
 const page=await browser.newPage({viewport:{width:390,height:844}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
 const url='https://nairapips-api.onrender.com/control-panel';
 await page.route('**/*',r=>r.request().url()===url?r.fulfill({status:200,contentType:'text/html',body:fs.readFileSync(__dirname+'/../website-downloads/'+file,'utf8')}):r.fulfill({status:401,contentType:'application/json',body:JSON.stringify({success:false,error:'Authentication required'})}));
 await page.goto(url,{waitUntil:'domcontentloaded'});await page.getByRole('button',{name:'Sign in',exact:true}).waitFor();
 await page.evaluate(()=>{
  SESSION.admin={id:'fixture',role:'super_admin',permissions:'all',name:'Fixture'};SESSION.currentMod='overview';SESSION.tickets=[];
  window.hits=[];window.setMod=name=>{if(!npStaffCanAccess(name))throw Error('Permission bypass');SESSION.currentMod=name;hits.push(name);document.getElementById('content').textContent=name;};
  window.openNairaPipsBlackBook=()=>hits.push('blackBook');window.logout=()=>{hits.push('logout');renderLogin();};window.reload=()=>hits.push('reload');
  renderApp();
 });
 const modules=await page.locator('#np-admin-sidebar .nav[data-mod]').evaluateAll(ns=>ns.filter(n=>n.style.display!=='none').map(n=>n.dataset.mod));
 assert(modules.length>=29);assert(modules.includes('settings'));assert(modules.includes('phaseAssign'));assert(modules.includes('salesOutcomes'));
 for(const module of modules){
  await page.getByRole('button',{name:'All modules',exact:true}).click();
  assert(await page.locator('#np-admin-sidebar').isVisible());
  await page.locator('#np-admin-sidebar .nav[data-mod="'+module+'"]').click();
  assert.equal(await page.evaluate(()=>hits[hits.length-1]),module);assert(!await page.locator('#np-admin-sidebar').isVisible());
 }
 await page.getByRole('button',{name:'All modules',exact:true}).click();await page.getByLabel('Find an admin module').fill('business');
 assert(await page.locator('#np-admin-sidebar [data-mod="settings"]').isVisible());assert(!await page.locator('#np-admin-sidebar [data-mod="payouts"]').isVisible());
 await page.keyboard.press('Escape');assert(!await page.locator('#np-admin-sidebar').isVisible());assert.equal(await page.evaluate(()=>document.body.style.overflow),'');
 await page.getByRole('button',{name:'All modules',exact:true}).click();
 await page.locator('#np-admin-sidebar').getByRole('button',{name:'Sign out',exact:true}).focus();await page.keyboard.press('Tab');assert.equal(await page.evaluate(()=>document.activeElement.id),'np-admin-menu-close');
 await page.keyboard.press('Shift+Tab');assert.equal(await page.locator('#np-admin-sidebar').getByRole('button',{name:'Sign out',exact:true}).evaluate(n=>n===document.activeElement),true);
 await page.locator('#np-admin-menu-backdrop').click({position:{x:388,y:100}});assert(!await page.locator('#np-admin-sidebar').isVisible());
 await page.getByRole('button',{name:'All modules',exact:true}).click();await page.getByLabel('Find an admin module').fill('settings');await page.setViewportSize({width:1280,height:900});
 await page.waitForFunction(()=>!document.querySelector('#np-admin-sidebar').classList.contains('np-admin-menu-open'));
 assert(await page.locator('#np-admin-sidebar').isVisible());assert(await page.locator('#np-admin-sidebar [data-mod="payouts"]').isVisible());
 await page.setViewportSize({width:320,height:640});await page.getByRole('button',{name:'All modules',exact:true}).click();
 assert(await page.locator('#np-admin-sidebar [data-mod="settings"]').isVisible());
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
 await page.getByRole('button',{name:'Close modules menu',exact:true}).filter({hasText:'×'}).click();
 // Same staff permissions as desktop; searching cannot reveal restricted modules.
 await page.evaluate(()=>{SESSION.admin={id:'limited',role:'staff',permissions:{overview:'view',purchases:'view',payouts:'view'}};SESSION.currentMod='overview';renderApp();});
 await page.getByRole('button',{name:'All modules',exact:true}).click();
 assert(await page.locator('#np-admin-sidebar [data-mod="payouts"]').isVisible());assert(!await page.locator('#np-admin-sidebar [data-mod="settings"]').isVisible());
 await page.getByLabel('Find an admin module').fill('settings');assert(!await page.locator('#np-admin-sidebar [data-mod="settings"]').isVisible());
 await page.getByLabel('Find an admin module').fill('');await page.locator('#np-admin-sidebar').getByRole('button',{name:'Sign out',exact:true}).click();
 assert.equal(await page.evaluate(()=>document.body.style.overflow),'');await page.getByRole('button',{name:'Sign in',exact:true}).waitFor();
 assert.deepEqual(errors,[]);await page.close();
}console.log('PASS: full and compact admin expose every authorized module on phones, search, dismiss, trap focus, resize, sign out and preserve staff permissions.');}
finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1)});
