const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{const b=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});try{
 const p=await b.newPage(),errors=[];p.on('pageerror',e=>errors.push(e.message));
 const url='https://nairapips-api.onrender.com/control-panel';
 await p.route('**/*',r=>r.request().url()===url?r.fulfill({status:200,contentType:'text/html',body:fs.readFileSync(__dirname+'/../website-downloads/admin_clean_cpanel.html','utf8')}):r.fulfill({status:401,contentType:'application/json',body:JSON.stringify({success:false,error:'Authentication required'})}));
 await p.goto(url,{waitUntil:'domcontentloaded'});await p.getByRole('button',{name:'Sign in',exact:true}).waitFor({state:'visible',timeout:10000});
 assert(await p.locator('input[type="password"]').isVisible());assert.deepEqual(errors,[]);
 console.log('PASS: complete delivered admin HTML boots at backend origin and shows authenticated sign-in without script errors.');
}finally{await b.close();}})().catch(e=>{console.error(e);process.exit(1)});
