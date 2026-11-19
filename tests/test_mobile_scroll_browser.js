const {chromium}=require('playwright');
const fs=require('fs'),assert=require('assert');

async function openPanel(browser,file){
 const page=await browser.newPage({viewport:{width:390,height:844},isMobile:true,hasTouch:true,deviceScaleFactor:2});
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

  const cdp=await page.context().newCDPSession(page);
  for(const tab of ['dashboard','accounts','plans','profile']){
   if(tab==='dashboard'){await page.evaluate(()=>{SESSION.currentTab='dashboard';renderApp();});}
   else{await page.evaluate(()=>window.scrollTo(0,0));await page.waitForTimeout(500);await page.getByRole('button',{name:'Menu',exact:true}).tap();await page.locator(`.sidebar .nav-item[onclick="setTab('${tab}')"]`).tap();await page.waitForFunction(tab=>document.querySelector('main')?.dataset.module===tab,tab);}
   assert(!await page.locator('main').evaluate(n=>n.inert),'Menu left module inert');
   await page.waitForTimeout(100);
   await page.evaluate(()=>{const tail=document.createElement('div');tail.style.height='1800px';tail.dataset.scrollFixture='true';document.querySelector('main').append(tail);window.scrollTo(0,0);});
   await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x:195,y:650}]});
   for(let y=620;y>=250;y-=30){await cdp.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{x:195,y}]});await page.waitForTimeout(20);}
   await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});
   await page.waitForTimeout(150);
   const state=await page.evaluate(()=>({scroll:scrollY,body:getComputedStyle(document.body).overscrollBehaviorY,overflow:getComputedStyle(document.body).overflowY}));
   console.log(tab,state);assert(state.scroll>100,'Touch scroll blocked on '+tab+': '+JSON.stringify(state));
  }
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1);});
