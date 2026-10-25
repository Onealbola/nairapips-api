const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
try{for(const file of ['funded-cycle-progress.js','admin_clean_cpanel.html']){
 const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.setContent('<main id="content"></main>');
 await page.evaluate(()=>{
  window.SESSION={token:'fixture',admin:{id:'owner'},currentMod:'payouts'};
  window.esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  window.offline=false;window.messages=[];window.calls=[];window.toast=s=>messages.push(s);window.prompt=()=>'';window.audit=async()=>{};window.reload=()=>npCycleApply();
  window.fixture={counts:{tracked:2,completed:1},generated_at:new Date().toISOString(),purchases:[{purchase_id:'p1',customer:'<img src=x onerror=alert(1)>',plan:'Legend',limit:2,paid:1,reserved:1,remaining:1,verified:true,completed:false,status:'active'},{purchase_id:'p2',customer:'Finished',plan:'Pro',limit:2,paid:2,reserved:0,remaining:0,verified:true,completed:true,status:'completed'}],payouts:{first:{purchase_id:'p1',blocked:true,reason:'This account already has a paid payout'},next:{purchase_id:'p1',blocked:false,reason:null},excess:{purchase_id:'p2',blocked:true,reason:'COMPLETED — no further payout'}}};
  window.api=async(path,method)=>{
   calls.push([path,method]);if(offline)throw Error('Offline');
   if(path==='/mark_payout_paid'){
    fixture.purchases[0]={...fixture.purchases[0],paid:2,reserved:0,remaining:0,completed:true,final_cycle_active:false,status:'completed'};fixture.counts.completed=2;fixture.payouts.next={purchase_id:'p1',blocked:true,reason:'COMPLETED — no further payout'};return {success:true};
   }
   return {success:true,data:structuredClone(fixture)};
  };
 });
 let code=fs.readFileSync(__dirname+'/../website-downloads/'+file,'utf8');
 if(file.endsWith('.html')){const a=code.indexOf('function npCycleProgressState('),b=code.indexOf('function renderPurchases(',a);assert(a>=0&&b>a);code=code.slice(a,b);}
 await page.addScriptTag({content:code});
 await page.evaluate(()=>document.getElementById('content').innerHTML=npCyclePanel()+npCyclePurchaseCell({id:'p1',payout_cycles:2})+npCyclePayoutCell({id:'next'})+`<button id="pay" ${npCycleActionAttrs('next')}>Mark Paid</button><button id="completed" ${npCycleActionAttrs('excess')}>Mark Paid</button><button id="renew" ${npCycleActionAttrs('first','renewal')}>Retry</button><button id="renew-completed" ${npCycleActionAttrs('excess','renewal')}>Retry</button>`);
 await page.evaluate(()=>npCycleRefresh(true));
 assert((await page.locator('[data-cycle-purchase]').innerText()).includes('1 of 2 paid'));
 assert((await page.locator('[data-cycle-payout]').innerText()).includes('1 payout remaining'));
 assert(!await page.locator('#pay').isDisabled());assert(await page.locator('#completed').isDisabled());assert(!await page.locator('#renew').isDisabled());assert(await page.locator('#renew-completed').isDisabled());
 assert.equal(await page.locator('#np-cycle-progress-panel img').count(),0);
 await page.evaluate(()=>{fixture.purchases[0].final_cycle_active=true;fixture.payouts.first.renewal_blocked=true;return npCycleRefresh(true)});
 assert((await page.locator('[data-cycle-payout]').innerText()).includes('FINAL ACCOUNT · 1 payout left'));assert(await page.locator('#renew').isDisabled());assert(!await page.locator('#pay').isDisabled());
 // Use the actual financial handler: mark paid must refresh the report immediately.
 const html=fs.readFileSync(__dirname+'/../website-downloads/admin_clean.html','utf8'),a=html.indexOf('window.markPaid = async function(id){'),b=html.indexOf('\n};',a)+4;
 await page.addScriptTag({content:html.slice(a,b)});await page.evaluate(()=>markPaid('next'));
 assert((await page.locator('[data-cycle-purchase]').innerText()).includes('2 of 2 paid'));
 assert((await page.locator('[data-cycle-purchase]').innerText()).includes('COMPLETED'));
 assert(await page.locator('#pay').isDisabled());assert(await page.locator('#renew').isDisabled());
 const before=await page.evaluate(()=>calls.filter(c=>c[0]==='/mark_payout_paid').length);await page.evaluate(()=>markPaid('next'));
 assert.equal(await page.evaluate(()=>calls.filter(c=>c[0]==='/mark_payout_paid').length),before);
 await page.evaluate(()=>{offline=true;return npCycleRefresh(true);});
 assert((await page.locator('#np-cycle-progress-panel').innerText()).includes('Verification unavailable'));
 assert((await page.locator('#np-cycle-progress-panel').innerText()).includes('PREVIOUS SNAPSHOT'));
 assert(await page.locator('#pay').isDisabled());
 await page.evaluate(()=>{SESSION.token='different-user';npCycleApply();});
 assert(!(await page.locator('#np-cycle-progress-panel').innerText()).includes('Finished'));
 assert.deepEqual(errors,[]);await page.close();
}console.log('PASS: full/compact progress displays real counters, completion, reservations, safe renewal, immediate payment refresh, blocked repeat actions, stale records and session isolation.');}
finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1)});
