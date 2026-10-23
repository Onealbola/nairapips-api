const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 try{
  const page=await browser.newPage();await page.setContent('<main id="cards"></main><div id="modal"></div>');
  await page.evaluate(()=>{
   window.esc=x=>String(x??'');window.fmt={money:n=>new Intl.NumberFormat('en-NG',{style:'currency',currency:'NGN',maximumFractionDigits:0}).format(n)};
   window.showModal=html=>document.getElementById('modal').innerHTML='<div class="modal-bg"><div class="modal-card">'+html+'</div></div>';
   window.messages=[];window.toast=(s)=>messages.push(s);window.npCaptureReferralFromUrl=()=>'';window.API='https://fixture.invalid';window.renderApp=()=>{};
   window.SESSION={paymentAccounts:[{bank_name:'Fixture bank',account_name:'Business',account_number:'123'}],data:{trader:{id:'fixture',email:'fixture@example.invalid'}},plansList:[{id:'s',name:'Standard fixture',product_category:'standard',status:'active',fee:20000,payout_cycles:2,phase2_target:0},{id:'p',name:'Premium fixture',product_category:'premium',status:'active',fee:40000,payout_cycles:2,phase2_target:0}]};
   window.rates={s:{3:5000,4:10000,5:5000,0:null},p:{3:18000,4:30000,5:18000,0:60000}};
   window.catalog=id=>{const p=SESSION.plansList.find(p=>p.id===id);return {...p,base_fee:p.fee,payout_cycle_extra:0,cycle_options:[1,2,3,4,5,0].map(c=>{const extra=[1,2,3].includes(c)?0:rates[id][c];return {cycles:c,extra,available:extra!==null,fee:extra===null?null:p.fee+extra};})};};
   SESSION.plansList=SESSION.plansList.map(p=>catalog(p.id));
   window.submitted=[];window.uploads=0;window.delayPromo=false;window.releasePromo=null;
   window.safeFetchJson=async(url,opts={})=>{
    if(url.includes('plan_purchase_options')){const u=new URL(url),p=catalog(u.searchParams.get('plan_id')),cycles=Number(u.searchParams.get('payout_cycles')??3),o=p.cycle_options.find(o=>o.cycles===cycles);return {success:true,data:{...p,fee:o.fee,payout_cycles:cycles,payout_cycle_extra:o.extra}};}
    const body=JSON.parse(opts.body||'{}');
    if(url.includes('quote_challenge_price')){
     const p=catalog(body.plan_id),o=p.cycle_options.find(o=>o.cycles===body.payout_cycles);
     if(delayPromo) {delayPromo=false;await new Promise(r=>releasePromo=r);}
     return {success:true,data:{valid:true,original_fee:o.fee,final_fee:o.fee*.9,discount_amount:o.fee*.1,discount_percent:10}};
    }
    if(url.includes('create_challenge_purchase')){submitted.push(body);return {success:true,data:{id:'sold',...body}};}
    throw Error('Unexpected fixture route '+url);
   };
   window.fetch=async(url)=>{if(!url.endsWith('/upload_payment_proof'))throw Error('Unexpected upload');uploads++;return {ok:true,text:async()=>JSON.stringify({url:'https://fixture.invalid/proof.png'})};};
   Object.defineProperty(window,'sessionStorage',{value:{getItem:()=>null,setItem:()=>{},removeItem:()=>{}}});
  });
  const html=fs.readFileSync(__dirname+'/../website-downloads/trader_clean.html','utf8');
  const a=html.indexOf('function npPlanRoute(p)'),b=html.indexOf('function renderPlans()',a);
  await page.addScriptTag({content:html.slice(a,b)});
  const c=html.indexOf('let NP_BUYER_CHECKOUT='),d=html.indexOf('/* ====== PAYOUTS',c);await page.addScriptTag({content:html.slice(c,d)});
  await page.evaluate(()=>document.getElementById('cards').innerHTML=npProductMarketBlockV156(SESSION.plansList,'standard')+npProductMarketBlockV156(SESSION.plansList,'premium'));
  const cards=await page.locator('#cards').innerText();
  assert(!cards.includes('18,000'));assert(!cards.includes('cycle'));assert(!cards.includes('Base '));
  await page.evaluate(()=>document.getElementById('cards').innerHTML=npProductMarketBlockV156(SESSION.plansList,'standard','purchase')+npProductMarketBlockV156(SESSION.plansList,'premium','purchase'));
  const purchaseCards=await page.locator('#cards').innerText();
  assert(!purchaseCards.includes('cycle'));assert(!purchaseCards.includes('Base '));
  const open=async id=>{await page.evaluate(id=>purchasePlan(id),id);await page.waitForFunction(()=>!document.getElementById('pur-submit').disabled);};
  await page.locator('.standard .np-poster-cta').first().click();
  await page.waitForFunction(()=>!document.getElementById('pur-submit').disabled);
  assert.equal(await page.locator('#pur-cycles').inputValue(),'3');
  await open('s');assert.equal(await page.locator('#pur-cycles').inputValue(),'3');
  assert((await page.locator('#pur-amount-display').innerText()).includes('20,000'));
  assert(!await page.locator('#pur-cycle-toggle').isChecked());assert(!await page.locator('#pur-cycles').isVisible());
  assert(await page.locator('#pur-cycles option[value="0"]').isDisabled());
  await page.locator('#pur-cycle-toggle').check();assert(await page.locator('#pur-cycles').isVisible());
  await page.selectOption('#pur-cycles','5');assert((await page.locator('#pur-extra-display').innerText()).includes('5,000'));assert((await page.locator('#pur-amount-display').innerText()).includes('25,000'));
  await page.locator('#pur-cycle-toggle').uncheck();
  assert.equal(await page.locator('#pur-cycles').inputValue(),'3');assert(!await page.locator('#pur-cycles').isVisible());
  assert((await page.locator('#pur-amount-display').innerText()).includes('20,000'));
  await page.locator('#pur-cycle-toggle').check();
  await page.selectOption('#pur-cycles','1');assert((await page.locator('#pur-amount-display').innerText()).includes('20,000'));
  await open('p');await page.locator('#pur-cycle-toggle').check();await page.selectOption('#pur-cycles','5');assert((await page.locator('#pur-amount-display').innerText()).includes('58,000'));
  await page.locator('#pur-promo').fill('TEN');await page.evaluate(()=>applyPromoQuote('p'));
  assert((await page.locator('#pur-amount-display').innerText()).includes('52,200'));
  // An old promo response must not overwrite a later cycle selection.
  await page.evaluate(()=>{delayPromo=true;applyPromoQuote('p');});
  await page.waitForFunction(()=>!!releasePromo);
  await page.selectOption('#pur-cycles','0');await page.waitForFunction(()=>document.getElementById('pur-amount-display').textContent.includes('90,000'));
  await page.evaluate(()=>releasePromo());await page.waitForTimeout(30);
  assert((await page.locator('#pur-amount-display').innerText()).includes('90,000'));
  await page.selectOption('#pur-cycles','5');await page.waitForFunction(()=>document.getElementById('pur-amount-display').textContent.includes('52,200'));
  await page.locator('#pur-file').setInputFiles({name:'proof.png',mimeType:'image/png',buffer:Buffer.from('fixture')});
  await page.evaluate(()=>submitPurchase('p'));
  const sold=await page.evaluate(()=>submitted[0]);assert.equal(sold.payout_cycles,5);assert.equal(sold.fee,58000);assert.equal(sold.final_fee,52200);
  assert.equal(await page.evaluate(()=>uploads),1);
  // A price change must stop submission before any proof upload.
  await open('p');await page.locator('#pur-cycle-toggle').check();await page.selectOption('#pur-cycles','5');
  await page.locator('#pur-file').setInputFiles({name:'proof.png',mimeType:'image/png',buffer:Buffer.from('fixture')});
  await page.evaluate(()=>{rates.p[5]=19000;});await page.evaluate(()=>submitPurchase('p'));
  assert.equal(await page.evaluate(()=>uploads),1);assert.equal(await page.evaluate(()=>submitted.length),1);
  assert((await page.locator('#pur-amount-display').innerText()).includes('59,000'));
  assert((await page.evaluate(()=>messages.join('\n'))).includes('price has changed'));
  console.log('PASS: buyer cycles default 3; separate market extras, free 1, unavailable unlimited, promo race, exact purchase payload, and stale-price upload protection.');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1)});
