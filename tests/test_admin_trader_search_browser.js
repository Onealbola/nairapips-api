const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{
 const b=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 try{
  const p=await b.newPage();await p.setContent('<div id="content"><div class="trader-card" data-trader-search="abdullah olamide abass ibnabass103@gmail.com 81906199">Trader match</div><div class="trader-card" data-trader-search="other customer">Other trader</div></div><span id="np-trader-visible-count"></span>');
  await p.evaluate(()=>window.TF={q:''});
  const html=fs.readFileSync(__dirname+'/../website-downloads/admin_clean.html','utf8');const start=html.indexOf('let __npTraderSearchTimer = 0;'),end=html.indexOf('window.MT5_PLAN_FILTER',start);
  await p.addScriptTag({content:html.slice(start,end)});
  for(const q of ['Abass Abdullah','Abdullah Abass','ibnabass103@gmail.com','81906199','  ABASS   ABDULLAH ']){
   await p.evaluate(q=>npFastTraderSearch(q),q);await p.waitForTimeout(120);
   assert.equal(await p.locator('.trader-card:not([hidden])').count(),1,q);
   assert.equal(await p.locator('.trader-card').first().isVisible(),true,q);
  }
  await p.evaluate(()=>npFastTraderSearch('notfound@example.com'));await p.waitForTimeout(120);assert.equal(await p.locator('.trader-card:not([hidden])').count(),0);
  console.log('Passed: actual trader search matches reversed names, email and MT5; unrelated searches stay excluded.');
 }finally{await b.close();}
})().catch(e=>{console.error(e);process.exit(1)});
