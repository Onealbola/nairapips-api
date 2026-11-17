const {chromium}=require('playwright');
const fs=require('fs'),assert=require('assert');

async function openPanel(browser,file){
 const page=await browser.newPage({viewport:{width:390,height:844}});
 const errors=[];page.on('pageerror',e=>errors.push(e.message));
 const url='https://mobile.fixture.invalid/';
 await page.route('**/*',route=>{
  const req=route.request();
  if(req.url()===url)return route.fulfill({contentType:'text/html',body:fs.readFileSync('website-downloads/'+file,'utf8')});
  if(req.resourceType()==='script')return route.fulfill({contentType:'application/javascript',body:''});
  if(req.resourceType()==='stylesheet')return route.fulfill({contentType:'text/css',body:''});
  return route.fulfill({contentType:'application/json',body:JSON.stringify({success:false,data:[],error:'Offline mobile test'})});
 });
 await page.goto(url,{waitUntil:'domcontentloaded'});
 return {page,errors};
}
async function fits(page,label){
 await page.waitForTimeout(60);
 const result=await page.evaluate(()=>({width:innerWidth,scroll:document.documentElement.scrollWidth,overflow:[...document.querySelectorAll('main *')].filter(n=>{const r=n.getBoundingClientRect();return r.width&&r.right>innerWidth+1&&!n.closest('.np-table-scroll');}).slice(0,5).map(n=>n.className)}));
 assert(result.scroll<=result.width+1,label+': '+JSON.stringify(result));
}
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 try{
  const {page,errors}=await openPanel(browser,'trader_clean.html');
  await page.evaluate(()=>{
   SESSION.trader={id:'mobile-fixture',name:'Amina Adewale',email:'amina@example.invalid'};
   const account={id:'mobile-account',trader_id:'mobile-fixture',stage:'phase1',account_status:'assigned_active',status:'active',mt5_login:'123456789',mt5_server:'Demo-MT5-Server',account_size:500000,start_balance:500000,current_balance:500000,current_equity:500000,created_at:'2026-11-17T08:00:00Z'};
   SESSION.data={trader:SESSION.trader,accounts:[account],current_account:account};SESSION.allAccounts=[account];SESSION.selectedAccountId=account.id;
   SESSION.currentTab='accounts';startPolling=()=>{};
   // Block test-only background refreshes; use real panel rendering and routing.
   loadTraderModuleData=async()=>{};npFastDashboardRefreshV143=async()=>{};
   renderApp();
  });
  await page.locator('.np-trader-dock').waitFor();
  for(const width of [320,360,390,430,768,900]){
   await page.setViewportSize({width,height:844});
   for(const tab of ['dashboard','mt5vault','accounts','trades','progress','plans','purchase','payouts','offers','announcements','timeline','support','profile','packages']){
    await page.evaluate(tab=>{SESSION.currentTab=tab;renderApp();},tab);
    await fits(page,'Trader '+tab+' at '+width);
   }
  }
  await page.setViewportSize({width:390,height:844});
  await page.evaluate(()=>{SESSION.currentTab='accounts';renderApp();});
  await page.getByRole('button',{name:'Menu',exact:true}).click();
  assert(await page.getByRole('dialog',{name:'Trader menu'}).isVisible());
  assert(await page.locator('main').evaluate(n=>n.inert));
  await page.getByLabel('Find a trader tool').fill('temporary password does not match');
  assert(await page.getByText('No tools match your search.').isVisible());
  await page.getByLabel('Find a trader tool').fill('support');
  assert.equal(await page.locator('.sidebar .nav-item:visible').count(),1);
  await page.keyboard.press('Escape');
  assert(!await page.locator('.sidebar').isVisible());
  assert.equal(await page.evaluate(()=>document.body.style.overflow),'');
  assert(await page.getByRole('button',{name:'Menu',exact:true}).evaluate(n=>n===document.activeElement));
  await page.getByRole('button',{name:'Menu',exact:true}).click();
  await page.getByLabel('Find a trader tool').fill('support');
  await page.locator('.sidebar .nav-item:visible').click();
  await page.waitForFunction(()=>document.querySelector('main').dataset.module==='support');
  assert(!await page.locator('.sidebar').isVisible());
  assert(!await page.locator('main').evaluate(n=>n.inert));
  await page.locator('[data-np-go="payouts"]').click();
  await page.waitForFunction(()=>document.querySelector('main').dataset.module==='payouts');
  assert.equal(await page.locator('[data-np-go="payouts"]').getAttribute('aria-current'),'page');
  await page.getByRole('button',{name:'Menu',exact:true}).click();
  await page.locator('.sidebar .btn-signout').focus();await page.keyboard.press('Tab');
  assert(await page.locator('.sidebar').getByRole('button',{name:'Close trader menu',exact:true}).evaluate(n=>n===document.activeElement));
  await page.keyboard.press('Shift+Tab');assert(await page.locator('.sidebar .btn-signout').evaluate(n=>n===document.activeElement));
  await page.setViewportSize({width:1024,height:800});await page.waitForTimeout(60);
  assert(await page.locator('.sidebar').isVisible());assert(!await page.locator('.np-trader-dock').isVisible());
  assert(!await page.locator('main').evaluate(n=>n.inert));await fits(page,'Tablet');
  await page.setViewportSize({width:1440,height:900});await fits(page,'Desktop');
  assert.deepEqual(errors,[]);await page.close();

  for(const file of ['admin_clean.html','admin_clean_cpanel.html']){
   const {page,errors}=await openPanel(browser,file);
   await page.evaluate(()=>{
    SESSION.admin={id:'fixture',role:'super_admin',permissions:'all',name:'Mobile Administrator'};
    const account={id:'admin-mobile-account',trader_id:'admin-mobile-trader',stage:'phase1',account_status:'assigned_active',status:'active',mt5_login:'987654321',account_size:500000,start_balance:500000,current_balance:500000,current_equity:500000,created_at:'2026-11-17T08:00:00Z'};
    SESSION.traders=[{id:'admin-mobile-trader',name:'Amina Adewale',email:'amina@example.invalid',current_account:account,stage:'phase1',created_at:'2026-11-17T08:00:00Z'}];SESSION.traderAccounts=[account];
    renderApp();
   });
   await page.locator('#content').waitFor();
   const modules=await page.locator('#np-admin-sidebar .nav[data-mod]').evaluateAll(nodes=>nodes.map(n=>n.dataset.mod));
   for(const width of [320,360,390,430,768,900,1024,1440]){
    await page.setViewportSize({width,height:900});
    for(const mod of modules){
     await page.evaluate(mod=>setMod(mod),mod);await fits(page,file+' '+mod+' at '+width);
    }
   }
   await page.setViewportSize({width:390,height:844});
   // Exercise real table semantics and late content in both report and dialog.
   await page.evaluate(()=>{
    document.querySelector('#content').innerHTML='<section class="card"><h2>Sales report</h2><table><thead><tr><th>Trader</th><th>Account</th><th>Amount</th><th>Status</th></tr></thead><tbody><tr><td>Amina</td><td>123456789</td><td>₦500,000</td><td>Funded</td></tr></tbody></table></section>';
   });
   await page.locator('.np-table-scroll').waitFor();await fits(page,'Admin table');
   assert.equal(await page.locator('table thead th').count(),4);
   assert(await page.locator('.np-table-hint').isVisible());
   await page.locator('.np-table-scroll').evaluate(n=>n.scrollLeft=n.scrollWidth);
   assert(await page.locator('.np-table-scroll').evaluate(n=>n.scrollLeft>0));
   await page.evaluate(()=>{
    const modal=document.createElement('div');modal.className='modal-bg';modal.innerHTML='<div class="modal-card"><h2>Confirm request</h2><input aria-label="Request note"><button class="btn">Confirm</button><div id="late-modal-table"></div></div>';document.body.append(modal);
   });
   await page.evaluate(()=>document.querySelector('#late-modal-table').innerHTML='<table><tr><th>A</th><th>B</th><th>C</th></tr><tr><td>One</td><td>Two</td><td>Three</td></tr></table>');
   await page.locator('.modal-card .np-table-scroll').waitFor();
   const box=await page.locator('.modal-card').boundingBox();assert(box.width<=390&&box.x>=0);
   assert.equal(await page.getByLabel('Request note').evaluate(n=>getComputedStyle(n).fontSize),'16px');
   assert.deepEqual(errors,[]);await page.close();
  }
  console.log('PASS: trader/admin screens fit 320–1440px; real trader routing, searchable menu, focus trap, dismissal, resize, inert cleanup, report scrolling, late dialogs and 16px form controls work without page errors.');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1);});
