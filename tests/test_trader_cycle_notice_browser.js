const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 try{
  const page=await browser.newPage({viewport:{width:390,height:844}});
  await page.setContent('<main></main>');
  await page.evaluate(()=>{
   window.API='https://fixture.invalid';window.SESSION={auth_token:'fixture-token',trader:{id:'owner'}};
   window.renderDashboard=()=>'<h1>Dashboard</h1>';window.renderPayouts=()=>'<h1>Payouts</h1>';
   window.rows=[{purchase_id:'abc12345-rest',plan:'Legend <safe>',limit:3,paid:1,reserved:1,remaining:2,completed:false,verified:true,status:'active'}];
   window.fail=false;window.calls=[];window.tick=null;window.setInterval=f=>{tick=f;return 1};
   window.fetch=async(url,opts)=>{calls.push({url,opts});return {ok:!fail,json:async()=>({success:!fail,data:{purchases:rows}})}};
  });
  const html=fs.readFileSync(__dirname+'/../website-downloads/trader_clean.html','utf8'),source=fs.readFileSync(__dirname+'/../website-downloads/trader-cycle-notice.js','utf8');
  assert(html.includes(source));await page.addScriptTag({content:source});
  await page.evaluate(()=>document.querySelector('main').innerHTML=renderDashboard());
  await page.getByText('1 of 3 payouts received · 2 remaining',{exact:true}).waitFor();
  assert(await page.getByText('Legend <safe> · Purchase abc12345',{exact:true}).count());
  assert(await page.getByText('1 payout request is pending or processing.',{exact:false}).count());
  assert.equal(await page.evaluate(()=>calls[0].opts.headers.Authorization),'Bearer fixture-token');
  await page.evaluate(()=>{rows[0].paid=2;rows[0].remaining=1;tick()});
  await page.getByText('Your final payout cycle remains',{exact:true}).waitFor();
  await page.evaluate(()=>{rows[0].final_cycle_active=true;tick()});
  await page.getByText('Your final funded account',{exact:true}).waitFor();
  assert((await page.locator('main').innerText()).includes('No further funded account or payout is included.'));
  await page.evaluate(()=>{rows[0].final_cycle_active=false;rows[0].paid=3;rows[0].remaining=0;rows[0].reserved=0;rows[0].completed=true;document.querySelector('main').innerHTML=renderPayouts()});
  await page.getByText('Payout cycles completed',{exact:true}).waitFor();
  assert(await page.getByText('No further payout is available under this purchase.',{exact:false}).count());
  await page.evaluate(()=>{rows[0].limit=null;rows[0].paid=2;rows[0].remaining=null;rows[0].completed=false;tick()});
  await page.getByText('2 payouts received',{exact:false}).waitFor();
  assert(!(await page.locator('main').innerText()).includes('of null'));
  await page.evaluate(()=>{fail=true;tick()});
  await page.getByText('Payout cycle status is temporarily unavailable.',{exact:false}).waitFor();
  assert.equal(await page.getByText('Payout cycles completed',{exact:true}).count(),0);
  await page.evaluate(()=>{fail=false;rows=[];tick()});await page.waitForFunction(()=>document.querySelector('[data-trader-cycle-notice]').hidden);
  console.log('PASS: mobile dashboard/payout notices, paid vs pending, final warning, completion, escaped names, auth, legacy absence and failure clearing.');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exit(1)});
