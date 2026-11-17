// Boot the exact cPanel ZIP, including its extracted admin scripts.
const {chromium}=require('playwright'),fs=require('fs'),path=require('path'),os=require('os'),assert=require('assert');
const {execFileSync}=require('child_process');
(async()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'np-mobile-archive-'));
 let browser;
 try{
  execFileSync('python',['-m','zipfile','-e','website-downloads/nairapips-mobile-update.zip',dir]);
  browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
  const page=await browser.newPage({viewport:{width:390,height:844}}),errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  await page.route('**/*',route=>{
   const url=new URL(route.request().url()),file=path.join(dir,url.pathname);
   if(url.hostname==='archive.fixture.invalid'&&fs.existsSync(file)&&fs.statSync(file).isFile())return route.fulfill({contentType:file.endsWith('.js')?'application/javascript':'text/html',body:fs.readFileSync(file)});
   return route.fulfill({status:401,contentType:'application/json',body:'{"success":false,"error":"Offline fixture"}'});
  });
  await page.goto('https://archive.fixture.invalid/admin/admin_clean.html',{waitUntil:'domcontentloaded'});
  await page.getByRole('button',{name:'Sign in',exact:true}).waitFor();
  await page.evaluate(()=>{SESSION.admin={id:'archive',role:'super_admin',permissions:'all',name:'Archive Test'};renderApp();});
  await page.getByRole('button',{name:'All modules',exact:true}).click();
  await page.getByLabel('Find an admin module').fill('business');
  assert(await page.locator('#np-admin-sidebar [data-mod="settings"]').isVisible());
  await page.keyboard.press('Escape');
  assert(!await page.locator('#np-admin-sidebar').isVisible());
  assert(await page.evaluate(()=>window.NP_MOBILE_PANELS==='2026-11-17'));
  assert.deepEqual(errors,[]);
  console.log('PASS: exact extracted cPanel ZIP loads all compact admin scripts, boots sign-in and runs mobile navigation without page errors.');
 }finally{if(browser)await browser.close();fs.rmSync(dir,{recursive:true,force:true});}
})().catch(e=>{console.error(e);process.exit(1);});
