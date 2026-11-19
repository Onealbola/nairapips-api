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


  await page.evaluate(()=>{
   const fresh=SESSION.allAccounts[0];
   const held=(id,login)=>({id,trader_id:'mobile-fixture',mt5_login:login,stage:'phase2',account_status:'archived',mt5_master_password:'RECALLED-SECRET',archive_reason:'[NP_MANAGEMENT_HOLD:ACTIVE] [NP_MH_ACCOUNT:'+id+']'});
   const a=held('held-one','477761875'),b=held('held-two','486213087');
   const history={id:'real-history',trader_id:'mobile-fixture',stage:'phase1',account_status:'archived_phase1',mt5_login:'123450000',passed_at:'2026-11-16T08:00:00Z'};
   SESSION.allAccounts=[a,b,fresh,history];SESSION.data.all_accounts=[a,b,fresh,history];SESSION.data.accounts=[a,b,fresh,history];SESSION.data.current_account=fresh;
   SESSION.data.trader.current_account_id=a.id;SESSION.data.trader.mt5_master_password='RECALLED-SECRET';SESSION.selectedAccountId=a.id;
  });
  await page.addScriptTag({content:fs.readFileSync('website-downloads/account-reporting.js','utf8')});
  for(const tab of ['mt5vault','accounts','dashboard']){
   await page.evaluate(tab=>{SESSION.currentTab=tab;renderApp();},tab);await page.waitForTimeout(100);
   const text=await page.locator('body').innerText();assert(!text.includes('477761875'));assert(!text.includes('486213087'));
   assert(!(await page.content()).includes('RECALLED-SECRET'),'Recalled credentials remain in DOM');
  }
  const ids=await page.evaluate(()=>SESSION.allAccounts.map(a=>a.id));assert.deepEqual(ids,['mobile-account','real-history']);
  console.log('PASS: recalled accounts and credentials disappear from Vault, Accounts and Dashboard; fresh delivery and legitimate pass history remain.');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1);});
