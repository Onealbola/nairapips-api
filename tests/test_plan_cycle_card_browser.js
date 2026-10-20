const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 try{
  const page=await browser.newPage();await page.setContent('<main id="modal"></main>');
  await page.evaluate(()=>{
   window.npShowResetModalSafe=html=>document.getElementById('modal').innerHTML=html;
   window.npSyncFundedResetDoubleV152=()=>{};window.npPlanRouteChanged=()=>{};
   window.esc=x=>String(x??'');window.toast=()=>{};window.audit=async()=>{};window.reload=()=>{};
   window.calls=[];window.api=async(path,method,body)=>{calls.push({path,body});return {success:true}};
  });
  const html=fs.readFileSync(__dirname+'/../website-downloads/admin_clean.html','utf8');
  const start=html.indexOf('window.showCreatePlan = function(){'),end=html.indexOf('window.editPlan = function(id){',start);
  await page.addScriptTag({content:html.slice(start,end)});
  for(const value of ['1','2','3','4','5','0']){
   await page.evaluate(()=>showCreatePlan());
   assert.equal(await page.locator('#pl-cycles').inputValue(),'2');
   assert.equal(await page.locator('#pl-cycles option').count(),6);
   await page.locator('#pl-cycles').selectOption(value);await page.locator('#pl-name').fill('Cycle fixture');
   await page.evaluate(()=>createPlan());
   const last=await page.evaluate(()=>calls.at(-1));assert.equal(last.path,'/create_challenge_plan');assert.equal(last.body.payout_cycles,Number(value));assert.equal(last.body.status,'inactive');
  }
  console.log('Passed: actual new-plan card defaults to two, offers 1–5/unlimited, and submits each exact limit without releasing sales.');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1)});
