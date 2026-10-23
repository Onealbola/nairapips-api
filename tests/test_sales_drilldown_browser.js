const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 try{for(const file of ['admin_clean.html','admin_clean_cpanel.html']){
  const page=await browser.newPage({viewport:{width:390,height:844}});
  await page.setContent('<div id="np-sales-result"></div><input id="np-sales-from" value="03/10/2026"><input id="np-sales-to" value="23/10/2026">');
  await page.evaluate(()=>{
   window.money=v=>'₦'+v;window.SESSION={};window.esc=String;
   const base={sale_date:'2026-10-20',trader_id:'owner',trader_name:'Same Trader <safe>',email:'fixture@example.invalid',plan:'Legend',account_size:500000,mt5_login:'old-login',breached:false,passed:false,funded:false,funded_breached:false,collected_payout:false,paid_then_breached_closed:false,assigned:true,paid_payout_count:0,payout_amount:0,still_phase1:false,still_funded:false,outcome:'no_recorded_result',outcome_label:'Challenge',current_accounts:[]};
   window.rows=[{...base,purchase_id:'first',still_phase1:true,current_position:'phase1',current_accounts:[{mt5_login:'phase-one-login',stage:'phase1',status:'assigned_active'}]},
    {...base,purchase_id:'second',still_funded:true,funded:true,passed:true,current_position:'funded',outcome:'funded_no_payout',current_accounts:[{mt5_login:'funded-login',stage:'funded',status:'profit_protected'}]},
    {...base,purchase_id:'outside',sale_date:'2026-09-01',still_funded:true,funded:true,current_position:'funded',outcome:'funded_no_payout'}];
   window.api=async()=>({success:true,from:'2026-10-03',to:'2026-10-23',rows:structuredClone(rows)});
  });
  const html=fs.readFileSync('website-downloads/'+file,'utf8'),a=html.search(/let npSalesReportSequence\s*=/),b=html.indexOf('function npCycleProgressState(',a);
  await page.addScriptTag({content:html.slice(a,b)});await page.evaluate(()=>loadSalesOutcomes());
  await page.locator('[data-sales-metric="still_phase1"]').click();
  let details=page.locator('#np-sales-details');assert(await details.isVisible());
  assert((await details.innerText()).includes('Still in Phase 1 · 1 purchase'));
  assert((await details.innerText()).includes('Same Trader <safe>'));assert((await details.innerText()).includes('phase-one-login'));assert(!(await details.innerText()).includes('funded-login'));assert.equal(await details.locator('safe').count(),0);
  await page.locator('[data-sales-metric="still_funded"]').focus();await page.keyboard.press('Enter');
  assert((await details.innerText()).includes('Still Funded · 1 purchase'));assert((await details.innerText()).includes('funded-login'));assert(!(await details.innerText()).includes('phase-one-login'));
  assert.equal(await details.locator('tbody tr').count(),1);
  await details.getByRole('button',{name:'Close list'}).click();assert(await details.isHidden());
  await page.locator('[data-sales-metric="sold"]').click();assert.equal(await details.locator('tbody tr').count(),2);
  await page.locator('[data-sales-metric="breached"]').click();assert((await details.innerText()).includes('No matching purchases'));
  await page.evaluate(()=>npSalesDatesChanged());assert.equal(await page.locator('#np-sales-details').count(),0);
  await page.close();
 }console.log('PASS: mobile/keyboard card drilldowns show names and exact current MT5s, retain cohort, separate repeat purchases, escape names, handle zero counts and clear on date change.');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exit(1)});
