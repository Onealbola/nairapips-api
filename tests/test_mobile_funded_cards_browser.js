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
   SESSION.trader={id:'mobile-fixture',name:'Amina Adewale Abiodun Joseph',email:'amina@example.invalid'};
   const account={id:'mobile-account',trader_id:'mobile-fixture',stage:'funded',account_status:'assigned_active',status:'active',mt5_login:'123456789',mt5_server:'Demo-MT5-Server',account_size:2000000,start_balance:2000000,current_balance:2000000,current_equity:2000000,created_at:'2026-11-17T08:00:00Z'};
   SESSION.data={trader:SESSION.trader,accounts:[account],current_account:account};SESSION.allAccounts=[account];SESSION.selectedAccountId=account.id;
   SESSION.currentTab='accounts';startPolling=()=>{};
   // Block test-only background refreshes; use real panel rendering and routing.
   loadTraderModuleData=async()=>{};npFastDashboardRefreshV143=async()=>{};
   renderApp();
  });
  await page.locator('.np-trader-dock').waitFor();


  await page.evaluate(()=>{SESSION.previousResetActions=[{account_id:'previous-account',mt5_login:'123456780',stage:'phase1',code:'PAYMENT_REQUIRED',account_size:1500000}];SESSION.previousResetRelease='V70_BOOTSTRAP_PREVIOUS_RESET_AUTHORITY_2026_09_18';SESSION.currentTab='dashboard';renderApp();});
  await page.locator('.np-funded-elite').waitFor();
  for(const width of [320,360,390,412,430,768]){
   await page.setViewportSize({width,height:844});await page.waitForTimeout(150);
   const result=await page.evaluate(()=>({width:innerWidth,overflow:[...document.querySelectorAll('main,main *')].filter(n=>{const r=n.getBoundingClientRect();return r.width&&r.right>innerWidth+1&&!n.closest('.np-table-scroll');}).slice(0,12).map(n=>({tag:n.tagName,cls:n.className,right:n.getBoundingClientRect().right,width:n.getBoundingClientRect().width}))}));
   await page.locator('.np-fe-passport').scrollIntoViewIfNeeded();
   const passport=await page.locator('.np-fe-passport').boundingBox();assert(passport.x>=0&&passport.x+passport.width<=width+1,'Passport must fit in full');
   console.log(JSON.stringify(result));assert.equal(result.overflow.length,0,'Funded cards clipped at '+width);
  }
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1);});
