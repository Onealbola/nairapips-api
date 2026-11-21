const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert'),{execFileSync}=require('child_process');
(async()=>{const fixture=JSON.parse(execFileSync('/workspace/.venvs/nairapips-api/bin/python',['-c',`import json
from types import SimpleNamespace as N
from unittest.mock import Mock
from flask import Flask
from first_purchase_promo import quote
db=Mock();app=Flask(__name__);out=[]
with app.test_request_context('/quote_challenge_price'):
 for eligible in (True,False):
  db.rpc.return_value.execute.return_value=N(data=eligible)
  out.append(quote({'code':'WELCOME20','plan_id':'plan','trader_id':'new'},20000,db,lambda tid:('new',None)))
print(json.dumps(out))`],{encoding:'utf8'}));
const html=fs.readFileSync('website-downloads/trader_clean.html','utf8');const start=html.indexOf('async function applyPromoQuote(planId){');const end=html.indexOf('\nasync function submitPurchase(planId)',start);assert(start>0&&end>start);
const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});try{
const page=await browser.newPage({viewport:{width:390,height:844}});await page.setContent('<input id="pur-promo" value="WELCOME20"><div id="promo-result"></div><div id="pur-fee-display"></div><div id="pur-discount-row"><span id="pur-discount-display"></span></div><div id="pur-amount-row"><span id="pur-amount-display"></span></div>');
await page.evaluate(data=>{window.fixture=data;window.eligible=true;window.PROMO_QUOTE_SEQ=0;window.PROMO_QUOTE=null;window.API='https://fixture.invalid';window.SESSION={trader:{id:'new'}};window.npBuyerSelected=()=>({id:'plan',fee:20000});window.esc=s=>String(s);window.fmt={money:n=>'NGN '+Number(n).toLocaleString('en-NG')};window.safeFetchJson=async()=>({success:true,data:fixture[eligible?0:1]});window.resetPurchaseQuoteDisplay=p=>{document.querySelector('#pur-discount-row').style.display='none';document.querySelector('#pur-amount-display').textContent=fmt.money(p.fee)}} ,fixture);
await page.addScriptTag({content:html.slice(start,end)});await page.evaluate(()=>applyPromoQuote('plan'));assert((await page.locator('#pur-discount-display').innerText()).includes('20%'));assert((await page.locator('#pur-amount-display').innerText()).includes('16,000'));assert((await page.locator('#promo-result').innerText()).includes('WELCOME20 applied'));
await page.evaluate(async()=>{eligible=false;await applyPromoQuote('plan')});assert((await page.locator('#pur-amount-display').innerText()).includes('20,000'));assert((await page.locator('#promo-result').innerText()).includes('no previous purchases'));assert.equal(await page.evaluate(()=>PROMO_QUOTE),null);
console.log('PASS: existing mobile checkout shows WELCOME20 20% / 16,000 and clears the discount when eligibility is denied.');
}finally{await browser.close()}})().catch(e=>{console.error(e);process.exit(1)});
