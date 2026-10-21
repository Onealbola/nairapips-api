const {chromium}=require('playwright'),fs=require('fs'),path=require('path'),assert=require('assert');
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',args:['--no-sandbox']});
 try{for(const file of ['admin_clean.html','admin_clean_cpanel.html']){
  const page=await browser.newPage({timezoneId:'America/New_York'});
  await page.clock.install({time:new Date('2026-10-21T23:30:00Z')});
  const observed='2026-10-21T23:29:30Z';
  const base={id:'11111111-1111-4111-8111-111111111111',trader_id:'owner',mt5_login:'123',stage:'funded',account_status:'assigned_active',account_size:500000,start_balance:500000,created_at:'2026-10-21T23:15:00Z',assigned_at:'2026-10-21T23:15:00Z',current_balance:520000,current_equity:525000,live_data_status:'live',last_sync_at:observed,live_monitor_update_at:observed,latest_monitoring_snapshot:{observed_at:observed},dd_limit_percent:20};
  await page.route('**/*',route=>{
   if(route.request().url().startsWith('https://panel.test/'))return route.fulfill({contentType:'text/html',body:fs.readFileSync(path.join(__dirname,'../website-downloads',file),'utf8')});
   if(route.request().url().includes('/admin/account_state'))return route.fulfill({contentType:'application/json',body:JSON.stringify({success:true,accounts:[{...base,current_equity:530000}]})});
   return route.fulfill({status:503,contentType:'application/json',body:'{"success":false,"error":"Offline fixture"}'});
  });
  await page.goto('https://panel.test/admin');
  await page.evaluate(({base,observed})=>{
   SESSION.admin={role:'super_admin'};SESSION.token='fixture';SESSION.traders=[{id:'owner',name:'Live trader'}];
   SESSION.traderAccounts=[base,{...base,id:'22222222-2222-4222-8222-222222222222',mt5_login:'456',live_data_status:'missing',latest_monitoring_snapshot:{},last_sync_at:null,live_monitor_update_at:null}];
   SESSION.currentMod='funded';document.getElementById('root').innerHTML='<div id="content"></div>';ACCT_DATE_FILTER='all';renderFunded();
  },{base,observed});
  for(const label of ['Today','Yesterday','This Week','Last 2 Weeks','This Month','Choose Dates'])assert.equal(await page.getByRole('button',{name:label,exact:true}).count(),1);
  assert((await page.locator('#content').innerText()).includes('₦525,000'));
  assert((await page.locator('#content').innerText()).includes('Awaiting update'));
  const missing=page.locator('tbody tr').filter({hasText:'456'});assert.equal(await missing.locator('td').nth(3).innerText(),'—');
  // Nigeria is already October 22 even though the browser's local day is October 21.
  assert.equal(await page.evaluate(base=>admAccountMatchesDate(base,'today'),base),true);
  assert.equal(await page.evaluate(base=>admComputedAbsoluteDd({...base,current_equity:0}),base),100);
  assert.equal(await page.evaluate(base=>admCurrentAccountRows([base,{...base,id:'old-duplicate',archived_at:'2026-10-01T00:00:00Z',created_at:'2026-10-01T00:00:00Z'}]).length,base),1);
  assert.equal(await page.evaluate(base=>admAccountMatchesDate({...base,assigned_at:'2026-10-21T12:00:00Z',created_at:'2026-10-21T12:00:00Z'},'yesterday'),base),true);
  await page.getByRole('button',{name:'Last 2 Weeks',exact:true}).click();
  await page.locator('input[type=date]').nth(0).fill('2026-10-22');await page.locator('input[type=date]').nth(1).fill('2026-10-22');
  assert.equal(await page.locator('tbody tr').count(),2);
  await page.getByRole('button',{name:'Refresh live data',exact:true}).click();
  await page.waitForFunction(()=>document.getElementById('content').innerText.includes('₦530,000'));
  await page.evaluate(()=>{SESSION.currentMod='accounts';ACCT_FILTER='funded';renderAccounts();});
  assert((await page.locator('tbody').innerText()).includes('₦530,000'));
  assert((await page.locator('tbody').innerText()).includes('Live'));
  await page.close();
 }console.log('PASS: both full panels show broker values, missing telemetry, Nigerian date boundaries, two-week/custom filters and fresh refresh results.');}
 finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1)});
