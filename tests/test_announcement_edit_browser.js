const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});try{
 for(const file of ['admin_clean.html','admin_clean_cpanel.html']){
  const page=await browser.newPage();await page.setContent('<main id="content"></main>');
  await page.evaluate(()=>{
   window.original={id:'notice',title:'Old <safe>',subject:'Old <safe>',message:'Before <b>text</b>',created_at:'2026-10-01',read_by:['trader'],status:'active'};
   window.SESSION={announcements:[original]};window.calls=[];window.esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));window.toast=()=>{};
   window.modal=(title,html)=>{const el=document.createElement('section');el.className='modal-bg';el.innerHTML=html;document.body.append(el)};
   window.api=async(path,method,body)=>{calls.push({path,method,body});return method==='POST'?{success:true,announcement:{...original,title:body.title,subject:body.title,message:body.message}}:{success:true,id:'notice',version:'v1',title:original.title,message:original.message}};
  });
  const html=fs.readFileSync('website-downloads/'+file,'utf8');const a=html.indexOf('function renderAnnouncements(){'),b=html.indexOf('window.populateInlineAnnouncementTraders',a);assert(a>=0&&b>a);await page.addScriptTag({content:html.slice(a,b)});await page.evaluate(()=>renderAnnouncements());
  await page.getByRole('button',{name:'Edit',exact:true}).click();await page.locator('#np-announcement-edit-title').waitFor();assert.equal(await page.locator('#np-announcement-edit-message').inputValue(),'Before <b>text</b>');assert.equal(await page.locator('.modal-bg safe').count(),0);
  await page.locator('#np-announcement-edit-title').fill('Corrected');await page.locator('#np-announcement-edit-message').fill('Updated wording');await page.getByRole('button',{name:'Save changes',exact:true}).click();
  await page.waitForFunction(()=>SESSION.announcements[0].title==='Corrected');const result=await page.evaluate(()=>({rows:SESSION.announcements,calls}));assert.equal(result.rows.length,1);assert.equal(result.rows[0].id,'notice');assert.equal(result.rows[0].created_at,'2026-10-01');assert.deepEqual(result.rows[0].read_by,['trader']);assert(result.calls.every(x=>x.path.startsWith('/admin/edit_announcement')));await page.close();
 }
 const page=await browser.newPage();await page.setContent('<main></main>');await page.evaluate(()=>{
  window.SESSION={trader:{id:'trader'},auth_token:'token',currentTab:'announcements',announcements:[{id:'notice',title:'Old',message:'Before',created_at:'date'}],offers:[{id:'notice',title:'Old',message:'Before'}],targetedOffers:[]};window.API='https://fixture';window.renderCount=0;window.renderApp=()=>renderCount++;
  const store={'np_announcement_read_trader_notice':'1'};Object.defineProperty(window,'localStorage',{value:{getItem:k=>store[k],setItem:(k,v)=>store[k]=v}});
  window.safeFetchJson=async()=>[{id:'notice',title:'New',message:'After',created_at:'date'}];
 });
 const trader=fs.readFileSync('website-downloads/trader_clean.html','utf8');let a=trader.indexOf('const NP_ANNOUNCEMENT_CACHE_V159='),b=trader.indexOf('const NP_PAYOUT_CACHE_PREFIX_V161',a);await page.addScriptTag({content:trader.slice(a,b)});
 await page.evaluate(()=>npRefreshAnnouncementsV159({onlyChanges:true}));assert.equal(await page.evaluate(()=>SESSION.announcements[0].message),'After');assert.equal(await page.evaluate(()=>SESSION.offers[0].message),'After');assert.equal(await page.evaluate(()=>localStorage.getItem('np_announcement_read_trader_notice')),'1');await page.evaluate(()=>npHydrateAnnouncementCacheV159());assert.equal(await page.evaluate(()=>SESSION.announcements[0].message),'After');await page.evaluate(()=>npRefreshAnnouncementsV159({onlyChanges:true}));assert.equal(await page.evaluate(()=>renderCount),1);
 await page.close();console.log('PASS: edit existing announcement safely; same ID/date/read state, no create/send, cached private notices update, unchanged polls do not repaint.');
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1)});
