const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 try{
  const page=await browser.newPage();await page.setContent('<main id="cards"></main><div id="modal"></div>');
  await page.evaluate(()=>{
   window.esc=x=>String(x??'');window.fmt={money:n=>new Intl.NumberFormat('en-NG',{style:'currency',currency:'NGN',maximumFractionDigits:0}).format(n)};
   window.showModal=html=>document.getElementById('modal').innerHTML=html;window.toast=()=>{};window.npCaptureReferralFromUrl=()=>'';
   window.SESSION={paymentAccounts:[],plansList:[{id:'s',name:'Standard fixture',product_category:'standard',status:'active',base_fee:20000,payout_cycle_extra:5000,fee:25000,payout_cycles:3,phase2_target:0},{id:'p',name:'Premium fixture',product_category:'premium',status:'active',base_fee:40000,payout_cycle_extra:18000,fee:58000,payout_cycles:3,phase2_target:0}]};
  });
  const html=fs.readFileSync(__dirname+'/../website-downloads/trader_clean.html','utf8');
  const a=html.indexOf('function npPlanRoute(p)'),b=html.indexOf('function renderPlans()',a);
  assert(a>=0&&b>a);await page.addScriptTag({content:html.slice(a,b)});
  const c=html.indexOf('function purchasePlan('),d=html.indexOf('let PROMO_QUOTE=',c);await page.addScriptTag({content:html.slice(c,d)});
  await page.evaluate(()=>document.getElementById('cards').innerHTML=npProductMarketBlockV156(SESSION.plansList,'standard')+npProductMarketBlockV156(SESSION.plansList,'premium'));
  const premium=await page.locator('.np-product-market.premium').innerText();assert(premium.includes('18,000'));assert(premium.includes('58,000'));assert(premium.includes('40,000'));
  await page.evaluate(()=>purchasePlan('p'));
  const checkout=await page.locator('#modal').innerText();assert(checkout.includes('18,000'));assert(checkout.includes('58,000'));assert(checkout.includes('40,000'));
  console.log('PASS: Standard and Premium cards and checkout show base fee, selected cycle extra and the server total.');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1)});
