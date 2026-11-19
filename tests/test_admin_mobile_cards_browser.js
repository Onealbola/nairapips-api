const {chromium}=require('playwright');
const fs=require('fs'),assert=require('assert');

async function openPanel(browser,file){
 const page=await browser.newPage({viewport:{width:390,height:844},isMobile:true,hasTouch:true});
 const errors=[];page.on('pageerror',e=>errors.push(e.message));
 const url='https://mobile.fixture.invalid/';
 await page.route('**/*',route=>{
  const req=route.request();
  if(req.url()===url)return route.fulfill({contentType:'text/html',body:fs.readFileSync('website-downloads/'+file,'utf8')});
  if(req.resourceType()==='script'&&new URL(req.url()).pathname.startsWith('/admin-assets/')){const asset=new URL(req.url()).pathname.slice(1);assert(!asset.includes('..'));return route.fulfill({contentType:'application/javascript',body:require('child_process').execFileSync('unzip',['-p','website-downloads/nairapips-admin-mobile.zip',asset])});}
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
 const {page,errors}=await openPanel(browser,'admin_clean_small.html');
 await page.evaluate(()=>{
  SESSION.admin={id:'fixture',role:'super_admin',permissions:'all',name:'Mobile Administrator'};
  const account={id:'admin-mobile-account',trader_id:'admin-mobile-trader',stage:'funded',account_status:'assigned_active',status:'active',mt5_login:'987654321',account_size:50000000,start_balance:50000000,current_balance:50123456.78,current_equity:50123456.78,created_at:'2026-11-17T08:00:00Z'};
  SESSION.traders=[{id:'admin-mobile-trader',name:'Amina Adewale Abiodun Joseph',email:'longcustomeraddress@example.invalid',current_account:account,stage:'funded',created_at:'2026-11-17T08:00:00Z'}];SESSION.traderAccounts=[account];renderApp();
 });
 await page.locator('#content').waitFor();
 const modules=await page.locator('#np-admin-sidebar .nav[data-mod]').evaluateAll(nodes=>nodes.map(n=>n.dataset.mod));
 for(const width of [320,390,430,768]){
  await page.setViewportSize({width,height:844});
  for(const mod of modules){
   await page.evaluate(mod=>setMod(mod),mod);await page.waitForTimeout(80);
   const overflow=await page.evaluate(()=>[...document.querySelectorAll('#content,#content *')].filter(n=>{const r=n.getBoundingClientRect();return r.width&&r.right>innerWidth+1&&!n.closest('.np-table-scroll');}).slice(0,6).map(n=>({cls:n.className,width:n.getBoundingClientRect().width,right:n.getBoundingClientRect().right})));
   assert.equal(overflow.length,0,mod+' at '+width+': '+JSON.stringify(overflow));
  }
 }
 const cdp=await page.context().newCDPSession(page);
 for(const mod of modules.slice(0,3)){
  await page.setViewportSize({width:390,height:844});await page.evaluate(()=>window.scrollTo(0,0));await page.waitForTimeout(500);await page.locator('.np-admin-menu-trigger').tap();await page.locator(`#np-admin-sidebar .nav[data-mod="${mod}"]`).tap();await page.waitForTimeout(100);assert(!await page.locator('.app>main').evaluate(n=>n.inert),'Admin menu left content inert');
  await page.evaluate(()=>{const tail=document.createElement('div');tail.style.height='1800px';document.querySelector('#content').append(tail);window.scrollTo(0,0);});
  await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x:195,y:650}]});
  for(let y=620;y>=250;y-=30){await cdp.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{x:195,y}]});await page.waitForTimeout(20);}
  await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});await page.waitForTimeout(150);
  assert(await page.evaluate(()=>scrollY)>100,'Touch scroll blocked in '+mod);
 }
 assert.deepEqual(errors,[]);console.log('PASS: populated admin modules fit phones/tablet; real finger scrolling works.');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1);});
